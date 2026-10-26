// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package health is the Health controller. In phase 1 it probes each control
// plane's public endpoint.
package health

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	Name         = "health"
	FieldManager = "lodia/" + Name
	// FailuresBeforeFalse consecutive failed probes turn a True Reachable
	// False, so that one blip does not drop Ready.
	FailuresBeforeFalse = 3
)

type Reconciler struct {
	client.Client
	// Reader reads the CA bundle, whose data the cache does not hold.
	Reader client.Reader
	// Period is how often a public endpoint is probed, Timeout how long a
	// probe waits for an answer.
	Period, Timeout time.Duration

	mu       sync.Mutex
	failures map[types.NamespacedName]int
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.ControlPlane{}, builder.WithPredicates(predicate.Or[client.Object](predicate.GenerationChangedPredicate{}, endpointChanged()))).
		Complete(r)
}

// Between the periodic passes, only a change of the endpoint brings one, so
// the failures counted are probes a period apart.
func endpointChanged() predicate.Funcs {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		before, ok := e.ObjectOld.(*lodiav1alpha1.ControlPlane)
		after, ok2 := e.ObjectNew.(*lodiav1alpha1.ControlPlane)
		return !ok || !ok2 || endpoint(before) != endpoint(after)
	}}
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		if apierrors.IsNotFound(err) {
			r.count(req.NamespacedName, false)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cp.DeletionTimestamp.IsZero() || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	if meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReachable) == nil {
		if err := r.apply(ctx, cp, newCondition(cp, metav1.ConditionUnknown, lodiav1alpha1.ReasonReconciling, "the first probe has not finished")); err != nil {
			return ctrl.Result{}, err
		}
	}
	c, err := r.reachable(ctx, cp)
	if err != nil {
		return ctrl.Result{}, err
	}
	if c != nil {
		if err := r.apply(ctx, cp, c); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: r.Period}, nil
}

// reachable returns nil to keep the condition as it is: a True after fewer
// than FailuresBeforeFalse failed probes in a row.
func (r *Reconciler) reachable(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*metav1.Condition, error) {
	key := client.ObjectKeyFromObject(cp)
	url := endpoint(cp)
	if url == "" {
		r.count(key, false)
		return newCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint"), nil
	}
	name := lodiav1alpha1.CABundleSecret.Name(cp.Name)
	secret := &corev1.Secret{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			r.count(key, false)
			return newCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "waiting for PKI to write the CA bundle "+name), nil
		}
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(secret.Data[lodiav1alpha1.CACertKey]) {
		r.count(key, false)
		return newCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "the CA bundle "+name+" holds no certificate"), nil
	}
	code := r.probe(ctx, url, roots)
	if code == http.StatusOK {
		r.count(key, false)
		return newCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, url+" answered 200"), nil
	}
	failures := r.count(key, true)
	if previous := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReachable); previous != nil &&
		previous.Status == metav1.ConditionTrue && failures < FailuresBeforeFalse {
		return nil, nil
	}
	answer := url + " did not answer"
	if code != 0 {
		answer = fmt.Sprintf("%s answered %d", url, code)
	}
	return newCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed, answer), nil
}

// probe returns the status code the endpoint answered with, or 0 for none.
func (r *Reconciler) probe(ctx context.Context, url string, roots *x509.CertPool) int {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0
	}
	probe := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := probe.Do(req)
	if err != nil {
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// count counts a failed probe of a ControlPlane, or clears its count, and
// returns it. Every success clears it, so a ControlPlane recreated under the
// same name is True, and keeps a True on a failure, only after a success of
// its own.
func (r *Reconciler) count(key types.NamespacedName, failed bool) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !failed {
		delete(r.failures, key)
		return 0
	}
	if r.failures == nil {
		r.failures = map[types.NamespacedName]int{}
	}
	r.failures[key]++
	return r.failures[key]
}

func endpoint(cp *lodiav1alpha1.ControlPlane) string {
	if cp.Status.Endpoint == nil {
		return ""
	}
	api := cp.Status.Endpoint.APIServer
	return "https://" + net.JoinHostPort(api.Host, strconv.Itoa(int(api.Port))) + lodiav1alpha1.APIServerReadyzPath
}

func newCondition(cp *lodiav1alpha1.ControlPlane, status metav1.ConditionStatus, reason, message string) *metav1.Condition {
	return &metav1.Condition{Type: lodiav1alpha1.ConditionReachable, Status: status, Reason: reason, Message: message, ObservedGeneration: cp.Generation}
}

// apply writes Reachable only on a change, and leaves cp with what it wrote.
func (r *Reconciler) apply(ctx context.Context, cp *lodiav1alpha1.ControlPlane, c *metav1.Condition) error {
	if previous := meta.FindStatusCondition(cp.Status.Conditions, c.Type); previous != nil && previous.Status == c.Status &&
		previous.Reason == c.Reason && previous.Message == c.Message && previous.ObservedGeneration == c.ObservedGeneration {
		return nil
	}
	conditions := slices.Clone(cp.Status.Conditions)
	meta.SetStatusCondition(&conditions, *c)
	applied, err := runtime.DefaultUnstructuredConverter.ToUnstructured(meta.FindStatusCondition(conditions, c.Type))
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     map[string]any{"conditions": []any{applied}},
	}}
	if err := r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		return err
	}
	cp.Status.Conditions = conditions
	return nil
}
