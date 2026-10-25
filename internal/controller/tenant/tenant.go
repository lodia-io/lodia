// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package tenant is the Tenant controller.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
)

const (
	Name           = "tenant"
	FieldManager   = "brygga/" + Name
	managedByLabel = "app.kubernetes.io/managed-by"
	managedBy      = "brygga"
)

const (
	ReasonReconciling         = "Reconciling"
	ReasonWaitingForAPIServer = "WaitingForAPIServer"
	ReasonApplying            = "Applying"
	ReasonApplied             = "Applied"
	ReasonRolloutPending      = "RolloutPending"
	ReasonAvailable           = "Available"
)

// rolloutPeriod is how often the addons' rollout is read while it is not
// complete: their pods run on nodes, which no event reports.
const rolloutPeriod = time.Minute

type Reconciler struct {
	client.Client
	// Reader reads the Secrets the cache holds the metadata of only.
	Reader client.Reader
	// ApplyPeriod is how often the tenant gets every object again, so one
	// deleted or edited there comes back.
	ApplyPeriod time.Duration
	// Dial, when set, opens every connection to a tenant's API server; tests
	// set it, since envtest has no DNS for the tenant's Service.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&bryggav1alpha1.ControlPlane{}, builder.WithPredicates(inputsChanged())).
		Complete(r)
}

// The other controllers' conditions change often; a pass runs only for what
// the render or the wait reads.
func inputsChanged() predicate.Funcs {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		before, ok := e.ObjectOld.(*bryggav1alpha1.ControlPlane)
		after, ok2 := e.ObjectNew.(*bryggav1alpha1.ControlPlane)
		return !ok || !ok2 || inputs(before) != inputs(after)
	}}
}

func inputs(cp *bryggav1alpha1.ControlPlane) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d %t", cp.Generation, cp.DeletionTimestamp.IsZero())
	if t := cp.Status.Target; t != nil {
		fmt.Fprintf(&b, " target=%s", t.Version)
	}
	if e := cp.Status.Endpoint; e != nil {
		for _, hp := range []*bryggav1alpha1.HostPort{e.APIServer, e.Konnectivity} {
			if hp != nil {
				fmt.Fprintf(&b, " %s:%d", hp.Host, hp.Port)
			}
		}
	}
	if c := meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionAccessReady); c != nil {
		fmt.Fprintf(&b, " %s/%d", c.Status, c.ObservedGeneration)
	}
	return b.String()
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &bryggav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cp.DeletionTimestamp.IsZero() || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	applied, available := previous(cp, bryggav1alpha1.ConditionAddonsApplied), previous(cp, bryggav1alpha1.ConditionAddonsAvailable)
	if meta.FindStatusCondition(cp.Status.Conditions, applied.Type) == nil || meta.FindStatusCondition(cp.Status.Conditions, available.Type) == nil {
		if err := r.applyStatus(ctx, cp, applied, available); err != nil {
			return ctrl.Result{}, err
		}
	}
	access := meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionAccessReady)
	if access == nil || access.Status != metav1.ConditionTrue || access.ObservedGeneration != cp.Generation {
		waiting := newCondition(cp, applied.Type, metav1.ConditionFalse, ReasonWaitingForAPIServer, "AccessReady is not True for this generation")
		return ctrl.Result{}, r.applyStatus(ctx, cp, waiting, available)
	}
	clusterCA, kubeconfig, err := r.credentials(ctx, cp)
	if err != nil {
		return ctrl.Result{}, err
	}
	v, waitingFor, err := newValues(cp, clusterCA)
	if err != nil {
		return ctrl.Result{}, err
	}
	if waitingFor != "" {
		waiting := newCondition(cp, applied.Type, metav1.ConditionFalse, ReasonWaitingForAPIServer, waitingFor)
		return ctrl.Result{}, r.applyStatus(ctx, cp, waiting, available)
	}
	objects, err := render(v)
	if err != nil {
		return ctrl.Result{}, err
	}
	tenant, err := r.tenantClient(kubeconfig)
	if err != nil {
		return ctrl.Result{}, err
	}

	syncErr := applyInventory(ctx, tenant, objects)
	switch f, ok := errors.AsType[*failure](syncErr); {
	case syncErr == nil:
		applied = newCondition(cp, applied.Type, metav1.ConditionTrue, ReasonApplied, fmt.Sprintf("%d objects are applied and the inventory is pruned", len(objects)))
	case ok && f.verdict != "":
		applied = newCondition(cp, applied.Type, metav1.ConditionFalse, ReasonApplying, f.entry.String()+" "+f.verdict+"; the reason is in Brygga's log")
	case applied.Status != metav1.ConditionTrue:
		// No answer is no observation: a True stays, anything else says so.
		applied = newCondition(cp, applied.Type, metav1.ConditionFalse, ReasonApplying, "the tenant's API server does not answer; the error is in Brygga's log")
	}
	if syncErr == nil {
		if c, err := rollout(ctx, tenant, cp, v); err == nil {
			available = c
		} else {
			syncErr = err
		}
	}
	if err := r.applyStatus(ctx, cp, applied, available); err != nil || syncErr != nil {
		return ctrl.Result{}, errors.Join(syncErr, err)
	}
	if available.Status != metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: min(rolloutPeriod, r.ApplyPeriod)}, nil
	}
	return ctrl.Result{RequeueAfter: r.ApplyPeriod}, nil
}

func (r *Reconciler) credentials(ctx context.Context, cp *bryggav1alpha1.ControlPlane) ([]byte, []byte, error) {
	bundle, kubeconfig := &corev1.Secret{}, &corev1.Secret{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-ca-bundle"}, bundle); err != nil {
		return nil, nil, err
	}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-brygga-kubeconfig"}, kubeconfig); err != nil {
		return nil, nil, err
	}
	return bundle.Data["ca.crt"], kubeconfig.Data["kubeconfig"], nil
}

func (r *Reconciler) tenantClient(kubeconfig []byte) (client.Client, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	// The in-namespace Service is never behind a proxy from the environment.
	cfg.Proxy = http.ProxyURL(nil)
	cfg.Dial = r.Dial
	return client.New(cfg, client.Options{Scheme: clientgoscheme.Scheme})
}

func rollout(ctx context.Context, c client.Client, cp *bryggav1alpha1.ControlPlane, v *values) (metav1.Condition, error) {
	type workload struct {
		name string
		obj  client.Object
	}
	var workloads []workload
	if v.kubeProxy {
		workloads = append(workloads, workload{"kube-proxy", &appsv1.DaemonSet{}})
	}
	if v.coreDNS {
		workloads = append(workloads, workload{"coredns", &appsv1.Deployment{}})
	}
	if v.AgentDeployment {
		workloads = append(workloads, workload{"konnectivity-agent", &appsv1.Deployment{}})
	} else {
		workloads = append(workloads, workload{"konnectivity-agent", &appsv1.DaemonSet{}})
	}
	var names, pending []string
	for _, w := range workloads {
		names = append(names, w.name)
		if err := c.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: w.name}, w.obj); err != nil {
			return metav1.Condition{}, err
		}
		if want, available, done := rolledOut(w.obj); !done {
			pending = append(pending, fmt.Sprintf("%s %d of %d available", w.name, available, want))
		}
	}
	if len(pending) > 0 {
		slices.Sort(pending)
		return newCondition(cp, bryggav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, ReasonRolloutPending, strings.Join(pending, ", ")), nil
	}
	return newCondition(cp, bryggav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, ReasonAvailable, strings.Join(names, ", ")+" are rolled out"), nil
}

func rolledOut(obj client.Object) (int32, int32, bool) {
	switch w := obj.(type) {
	case *appsv1.DaemonSet:
		s := w.Status
		return s.DesiredNumberScheduled, s.NumberAvailable, s.ObservedGeneration >= w.Generation && s.UpdatedNumberScheduled == s.DesiredNumberScheduled && s.NumberAvailable == s.DesiredNumberScheduled
	case *appsv1.Deployment:
		want := int32(1)
		if w.Spec.Replicas != nil {
			want = *w.Spec.Replicas
		}
		s := w.Status
		return want, s.AvailableReplicas, s.ObservedGeneration >= w.Generation && s.UpdatedReplicas == want && s.AvailableReplicas == want
	}
	return 0, 0, false
}

// refused tells the tenant's answer from no answer: every 4xx is one but
// those that ask to retry.
func refused(err error) bool {
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}
	code := status.Status().Code
	return code >= 400 && code < 500 && !slices.Contains([]int32{http.StatusRequestTimeout, http.StatusConflict, http.StatusTooManyRequests}, code)
}

func previous(cp *bryggav1alpha1.ControlPlane, conditionType string) metav1.Condition {
	if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c != nil {
		return *c
	}
	return newCondition(cp, conditionType, metav1.ConditionUnknown, ReasonReconciling, "the first pass has not finished")
}

func newCondition(cp *bryggav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: message, ObservedGeneration: cp.Generation}
}

// applyStatus writes only on a change, and both conditions every time: an
// apply that leaves one out removes it.
func (r *Reconciler) applyStatus(ctx context.Context, cp *bryggav1alpha1.ControlPlane, conditions ...metav1.Condition) error {
	all := slices.Clone(cp.Status.Conditions)
	changed := false
	var applied []any
	for _, c := range conditions {
		prev := meta.FindStatusCondition(cp.Status.Conditions, c.Type)
		if prev == nil || prev.Status != c.Status || prev.Reason != c.Reason || prev.Message != c.Message || prev.ObservedGeneration != c.ObservedGeneration {
			changed = true
		}
		meta.SetStatusCondition(&all, c)
		u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(meta.FindStatusCondition(all, c.Type))
		if err != nil {
			return err
		}
		applied = append(applied, u)
	}
	if !changed {
		return nil
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": bryggav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     map[string]any{"conditions": applied},
	}}
	if err := r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		return err
	}
	cp.Status.Conditions = all
	return nil
}
