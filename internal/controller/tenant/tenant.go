// Copyright 2026 The Lodia Authors.
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
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/live"
	"go.lodia.io/lodia/internal/ownership"
	"go.lodia.io/lodia/internal/statusapply"
)

const (
	Name         = "tenant"
	FieldManager = "lodia/" + Name
)

// waitPeriod is how often a pass runs while it waits on what no event
// reports: the addons' pods on the nodes, and a Secret that is missing or that
// this ControlPlane does not control.
const waitPeriod = time.Minute

// deliveryTimeout and applyTimeout bound each request of a tenant client. They
// are on the client because a new client's first read maps the kind through
// discovery, which takes no context. Delivery's is short of the 30 s dial of a
// tenant that drops packets, and the two leave a pass on a tenant that answers
// nothing its status write inside its minute.
const (
	deliveryTimeout = 10 * time.Second
	applyTimeout    = 30 * time.Second
)

type Reconciler struct {
	client.Client
	// Reader reads the Secrets the cache holds the metadata of only, and
	// AddonsApplied and AddonsAvailable when Tenant waits for access or keeps
	// them.
	Reader client.Reader
	// ApplyPeriod is how often the tenant gets every object again, so one
	// deleted or edited there comes back.
	ApplyPeriod time.Duration
	// TunnelImage is lodia-tunnel's, which apiserver-proxy runs.
	TunnelImage string
	// Dial, when set, opens every connection to a tenant's API server; tests
	// set it, since envtest has no DNS for the tenant's Service.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.ControlPlane{}, builder.WithPredicates(inputsChanged())).
		Complete(r)
}

// The other controllers' conditions change often; a pass runs only for what
// the render or the wait reads.
func inputsChanged() predicate.Funcs {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		before, ok := e.ObjectOld.(*lodiav1alpha1.ControlPlane)
		after, ok2 := e.ObjectNew.(*lodiav1alpha1.ControlPlane)
		return !ok || !ok2 || inputs(before) != inputs(after)
	}}
}

func inputs(cp *lodiav1alpha1.ControlPlane) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d %t", cp.Generation, cp.DeletionTimestamp.IsZero())
	if t := cp.Status.Target; t != nil {
		fmt.Fprintf(&b, " target=%s", t.Version)
	}
	fmt.Fprintf(&b, " components=%q", reported(cp))
	if e := cp.Status.Endpoint; e != nil {
		fmt.Fprintf(&b, " %s:%d %s:%d %s:%d", e.APIServer.Host, e.APIServer.Port, e.Konnectivity.Host, e.Konnectivity.Port, e.InCluster.Host, e.InCluster.Port)
		if e.Tunnel != nil {
			fmt.Fprintf(&b, " %s:%d", e.Tunnel.Host, e.Tunnel.Port)
		}
	}
	if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAccessReady); c != nil {
		fmt.Fprintf(&b, " %s/%d", c.Status, c.ObservedGeneration)
	}
	return b.String()
}

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cp.DeletionTimestamp.IsZero() || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	if cp.Spec.Endpoint.Gateway != nil {
		r.deliverTunnelClient(ctx, cp)
	}
	if err := live.Addons(ctx, r.Reader, cp, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ConditionAddonsAvailable); err != nil {
		return ctrl.Result{}, err
	}
	applied, available := previous(cp, lodiav1alpha1.ConditionAddonsApplied), previous(cp, lodiav1alpha1.ConditionAddonsAvailable)
	if meta.FindStatusCondition(cp.Status.Conditions, applied.Type) == nil || meta.FindStatusCondition(cp.Status.Conditions, available.Type) == nil {
		if err := r.applyStatus(ctx, cp, cp.Status.Addons, applied, available); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !statusapply.TrueAt(cp, lodiav1alpha1.ConditionAccessReady) {
		if err := r.applyStatus(ctx, cp, cp.Status.Addons, waitForAccess(cp, applied), waitForAccess(cp, available)); err != nil {
			return ctrl.Result{}, err
		}
		if cp.Spec.Endpoint.Gateway != nil {
			return ctrl.Result{RequeueAfter: r.ApplyPeriod}, nil
		}
		return ctrl.Result{}, nil
	}
	clusterCA, kubeconfig, tunnelClient, unusable, err := r.credentials(ctx, cp)
	if err != nil {
		return ctrl.Result{}, err
	}
	if unusable != "" {
		if err := r.wait(ctx, cp, lodiav1alpha1.ReasonWaitingForAccess, unusable); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: min(waitPeriod, r.ApplyPeriod)}, nil
	}
	v, reason, waitingFor, err := newValues(cp, clusterCA)
	if err != nil {
		return ctrl.Result{}, err
	}
	if waitingFor != "" {
		return ctrl.Result{}, r.wait(ctx, cp, reason, waitingFor)
	}
	v.TunnelImage, v.tunnelCert, v.tunnelKey = r.TunnelImage, tunnelClient.Data[corev1.TLSCertKey], tunnelClient.Data[corev1.TLSPrivateKeyKey]
	objects, err := render(v)
	if err != nil {
		return ctrl.Result{}, err
	}
	tenant, err := r.tenantClient(kubeconfig, applyTimeout)
	if err != nil {
		return ctrl.Result{}, err
	}

	others, applyErr := applyInventory(ctx, tenant, objects)
	syncErr := applyErr
	var rolled metav1.Condition
	if applyErr == nil {
		rolled, syncErr = rollout(ctx, tenant, cp, objects, others)
	}
	switch reason, message, answered := outcome(applyErr); {
	case applyErr == nil && len(others) > 0:
		applied = newCondition(cp, applied.Type, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying, others[0].String()+" exists and is not Lodia's; delete it or disable the addon")
	case applyErr == nil:
		applied = newCondition(cp, applied.Type, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, fmt.Sprintf("%d objects are applied and the inventory is pruned", len(objects)))
	case answered || applied.Status != metav1.ConditionTrue:
		// No answer is no observation: a True stays, anything else says so.
		applied = newCondition(cp, applied.Type, metav1.ConditionFalse, reason, message)
	}
	addons := cp.Status.Addons
	if applyErr == nil && len(others) == 0 {
		rendered := ptr.Deref(addons, lodiav1alpha1.Rendered{})
		rendered.Version = v.Version
		addons = &rendered
	}
	if syncErr == nil {
		available = rolled
	}
	if syncErr != nil && available.Reason != lodiav1alpha1.ReasonAvailable && available.Reason != lodiav1alpha1.ReasonRolloutPending {
		// A rollout read before stays while none can be read; until one is,
		// what stopped the pass says why.
		reason, message, _ := outcome(syncErr)
		available = newCondition(cp, available.Type, metav1.ConditionFalse, reason, message)
	}
	if err := r.applyStatus(ctx, cp, addons, applied, available); err != nil || syncErr != nil {
		return ctrl.Result{}, errors.Join(syncErr, err)
	}
	if available.Status != metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: min(waitPeriod, r.ApplyPeriod)}, nil
	}
	return ctrl.Result{RequeueAfter: r.ApplyPeriod}, nil
}

// outcome is the reason and the message a pass that stopped on err reports,
// and whether the tenant answered.
func outcome(err error) (string, string, bool) {
	if f, ok := errors.AsType[*failure](err); ok && f.verdict != "" {
		return f.reason, f.entry.String() + " " + f.verdict + "; the reason is in Lodia's log", true
	}
	return lodiav1alpha1.ReasonApplying, "the tenant's API server does not answer; the error is in Lodia's log", false
}

// wait says what both conditions wait for: neither can be observed yet.
func (r *Reconciler) wait(ctx context.Context, cp *lodiav1alpha1.ControlPlane, reason, message string) error {
	return r.applyStatus(ctx, cp, cp.Status.Addons,
		newCondition(cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, reason, message),
		newCondition(cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, reason, message))
}

// waitForAccess leaves a condition at the earlier generation it describes:
// Tenant has not acted on this one.
func waitForAccess(cp *lodiav1alpha1.ControlPlane, c metav1.Condition) metav1.Condition {
	return statusapply.Wait(cp, &c, newCondition(cp, c.Type, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, "AccessReady is not True for this generation"))
}

type secretRead struct {
	secret lodiav1alpha1.Secret
	obj    *corev1.Secret
}

// credentials also returns, when one of the Secrets cannot be used, what to
// report about it; its content is not used.
func (r *Reconciler) credentials(ctx context.Context, cp *lodiav1alpha1.ControlPlane) ([]byte, []byte, *corev1.Secret, string, error) {
	bundle, kubeconfig, tunnelClient := &corev1.Secret{}, &corev1.Secret{}, &corev1.Secret{}
	reads := []secretRead{{lodiav1alpha1.CABundleSecret, bundle}, {lodiav1alpha1.LodiaKubeconfigSecret, kubeconfig}}
	if cp.Spec.Endpoint.Gateway != nil {
		reads = append(reads, secretRead{lodiav1alpha1.TunnelClientSecret, tunnelClient})
	}
	unusable, err := r.readSecrets(ctx, cp, reads)
	if err != nil || unusable != "" {
		return nil, nil, nil, unusable, err
	}
	return bundle.Data[lodiav1alpha1.CACertKey], kubeconfig.Data[lodiav1alpha1.KubeconfigKey], tunnelClient, "", nil
}

// readSecrets stops at the first Secret that is missing, that another owner
// keeps or that this ControlPlane does not control, and returns what to report
// about it. AccessReady does not cover the last: PKI grants access while it
// holds a Secret it has not adopted, and a restore adopts none.
func (r *Reconciler) readSecrets(ctx context.Context, cp *lodiav1alpha1.ControlPlane, reads []secretRead) (string, error) {
	for _, read := range reads {
		obj, name := read.obj, read.secret.Name(cp.Name)
		err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, obj)
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("Secret %s does not exist", name), nil
		}
		if err != nil {
			return "", err
		}
		if ref := ownership.ControlledElsewhere(obj, cp); ref != nil {
			return fmt.Sprintf("Secret %s is kept by %s %s with UID %s", obj.Name, ref.Kind, ref.Name, ref.UID), nil
		}
		if !metav1.IsControlledBy(obj, cp) {
			return fmt.Sprintf("Secret %s is not controlled by this ControlPlane", obj.Name), nil
		}
	}
	return "", nil
}

// deliverTunnelClient brings a renewed pair to the tenant before any condition
// is read: none proves the API server cannot take it, and an expired pair takes
// every pod's API. Failures stop nothing, and neither a Secret that does not
// exist yet, the inventory's to write, nor another owner's gets a write.
func (r *Reconciler) deliverTunnelClient(ctx context.Context, cp *lodiav1alpha1.ControlPlane) {
	kubeconfig, pair := &corev1.Secret{}, &corev1.Secret{}
	unusable, err := r.readSecrets(ctx, cp, []secretRead{{lodiav1alpha1.LodiaKubeconfigSecret, kubeconfig}, {lodiav1alpha1.TunnelClientSecret, pair}})
	if unusable != "" {
		return
	}
	if err == nil {
		err = r.applyTunnelClient(ctx, kubeconfig.Data[lodiav1alpha1.KubeconfigKey], pair)
	}
	if err != nil {
		logf.FromContext(ctx).Error(err, "deliver the tunnel's client certificate")
	}
}

func (r *Reconciler) applyTunnelClient(ctx context.Context, kubeconfig []byte, pair *corev1.Secret) error {
	tenant, err := r.tenantClient(kubeconfig, deliveryTimeout)
	if err != nil {
		return err
	}
	existing := &corev1.Secret{}
	switch err := tenant.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: apiserverProxyClientSecret}, existing); {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return err
	case !appliedByLodia(existing):
		return nil
	}
	return tenant.Apply(ctx, client.ApplyConfigurationFromUnstructured(apiserverProxyClient(pair.Data[corev1.TLSCertKey], pair.Data[corev1.TLSPrivateKeyKey])), client.FieldOwner(FieldManager), client.ForceOwnership)
}

func (r *Reconciler) tenantClient(kubeconfig []byte, timeout time.Duration) (client.Client, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	// The in-namespace Service is never behind a proxy from the environment.
	cfg.Proxy = http.ProxyURL(nil)
	cfg.Dial = r.Dial
	cfg.Timeout = timeout
	return client.New(cfg, client.Options{Scheme: clientgoscheme.Scheme})
}

// rollout reads the DaemonSets and Deployments of the render but another
// owner's.
func rollout(ctx context.Context, c client.Client, cp *lodiav1alpha1.ControlPlane, objects []*unstructured.Unstructured, others []entry) (metav1.Condition, error) {
	var names, pending []string
	for _, obj := range objects {
		e := entryOf(obj)
		var workload client.Object
		switch {
		case e.Group != appsv1.GroupName || contains(others, e):
			continue
		case e.Kind == "DaemonSet":
			workload = &appsv1.DaemonSet{}
		case e.Kind == "Deployment":
			workload = &appsv1.Deployment{}
		default:
			continue
		}
		names = append(names, e.Name)
		if err := c.Get(ctx, client.ObjectKey{Namespace: e.Namespace, Name: e.Name}, workload); err != nil {
			return metav1.Condition{}, failed(e, err)
		}
		if want, available, done := rolledOut(workload); !done {
			pending = append(pending, fmt.Sprintf("%s %d of %d available", e.Name, available, want))
		}
	}
	if len(pending) > 0 {
		slices.Sort(pending)
		return newCondition(cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending, strings.Join(pending, ", ")), nil
	}
	slices.Sort(names)
	return newCondition(cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable, strings.Join(names, ", ")+" are rolled out"), nil
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

func previous(cp *lodiav1alpha1.ControlPlane, conditionType string) metav1.Condition {
	if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c != nil {
		return *c
	}
	return statusapply.FirstPass(cp, conditionType)
}

func newCondition(cp *lodiav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: message, ObservedGeneration: cp.Generation}
}

// applyStatus writes only on a change, and everything Tenant owns every time:
// both conditions and addons, since an apply that leaves one out removes it.
// A path that changes no addons passes cp's, which Reconcile read live.
func (r *Reconciler) applyStatus(ctx context.Context, cp *lodiav1alpha1.ControlPlane, addons *lodiav1alpha1.Rendered, conditions ...metav1.Condition) error {
	return statusapply.ApplyInto(ctx, r.Client, r.Reader, FieldManager, cp, lodiav1alpha1.ControlPlaneStatus{Conditions: conditions, Addons: addons}, !equality.Semantic.DeepEqual(cp.Status.Addons, addons))
}
