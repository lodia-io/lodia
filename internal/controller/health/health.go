// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package health is the Health controller: it probes each control plane's
// public endpoint and reads its components' leader Leases inside the tenant.
package health

import (
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/ownership"
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
	// Reader reads the CA bundle and Lodia's kubeconfig, whose data the cache
	// does not hold.
	Reader client.Reader
	// Period is how often a control plane is probed and its leader Leases
	// read, Timeout how long a probe or a read waits for an answer.
	Period, Timeout time.Duration
	// Clock tells when a leader Lease was seen to change.
	Clock clock.PassiveClock

	mu       sync.Mutex
	failures map[types.NamespacedName]int
	renewals map[types.NamespacedName]map[string]renewal
	clients  map[types.NamespacedName]*clients
}

// clients reach one control plane: the probe while its CA bundle stays the
// same, the Leases while Lodia's kubeconfig does.
type clients struct {
	bundle     []byte
	probe      *http.Client
	kubeconfig []byte
	leases     coordinationv1client.LeaseInterface
	transport  *http.Client
}

// renewal is the version of a leader Lease Health saw last, and when it saw
// that version first.
type renewal struct {
	resourceVersion string
	at              time.Time
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.ControlPlane{}, builder.WithPredicates(predicate.Or[client.Object](predicate.GenerationChangedPredicate{}, inputsChanged()))).
		Complete(r)
}

// Between the periodic passes, only a change of the endpoint or of
// AccessReady brings one, so the failures counted are probes a period apart.
func inputsChanged() predicate.Funcs {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		before, ok := e.ObjectOld.(*lodiav1alpha1.ControlPlane)
		after, ok2 := e.ObjectNew.(*lodiav1alpha1.ControlPlane)
		return !ok || !ok2 || endpoint(before) != endpoint(after) || access(before) != access(after)
	}}
}

func access(cp *lodiav1alpha1.ControlPlane) string {
	c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAccessReady)
	if c == nil {
		return ""
	}
	return fmt.Sprintf("%s/%d", c.Status, c.ObservedGeneration)
}

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=list;watch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		if apierrors.IsNotFound(err) {
			r.count(req.NamespacedName, false)
			r.forget(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cp.DeletionTimestamp.IsZero() || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	reachable, leading := previous(cp, lodiav1alpha1.ConditionReachable), previous(cp, lodiav1alpha1.ConditionControllersLeading)
	if meta.FindStatusCondition(cp.Status.Conditions, reachable.Type) == nil || meta.FindStatusCondition(cp.Status.Conditions, leading.Type) == nil {
		if err := r.apply(ctx, cp, reachable, leading); err != nil {
			return ctrl.Result{}, err
		}
	}
	c, err := r.reachable(ctx, cp)
	if err != nil {
		return ctrl.Result{}, err
	}
	if c != nil {
		reachable = *c
	}
	if c, err = r.leading(ctx, cp); err != nil {
		return ctrl.Result{}, err
	}
	if c != nil {
		leading = *c
	}
	if err := r.apply(ctx, cp, reachable, leading); err != nil {
		return ctrl.Result{}, err
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
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint"), nil
	}
	name := lodiav1alpha1.CABundleSecret.Name(cp.Name)
	secret := &corev1.Secret{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			r.count(key, false)
			return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "waiting for PKI to write the CA bundle "+name), nil
		}
		return nil, err
	}
	prober := r.prober(key, secret.Data[lodiav1alpha1.CACertKey])
	if prober == nil {
		r.count(key, false)
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "the CA bundle "+name+" holds no certificate"), nil
	}
	code := r.probe(ctx, prober, url)
	if code == http.StatusOK {
		r.count(key, false)
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, url+" answered 200"), nil
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
	return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed, answer), nil
}

// leading returns nil to keep the condition as it is when a read of a Lease
// fails (docs/README.md, Health).
func (r *Reconciler) leading(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*metav1.Condition, error) {
	key := client.ObjectKeyFromObject(cp)
	waiting := func(message string) (*metav1.Condition, error) {
		r.forget(key)
		return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, message), nil
	}
	if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAccessReady); c == nil || c.Status != metav1.ConditionTrue || c.ObservedGeneration != cp.Generation {
		return waiting("AccessReady is not True for this generation")
	}
	name := lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name)
	secret := &corev1.Secret{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return waiting("waiting for PKI to write " + name)
		}
		return nil, err
	}
	if ref := ownership.ControlledElsewhere(secret, cp); ref != nil {
		return waiting(fmt.Sprintf("Secret %s is kept by %s %s with UID %s", name, ref.Kind, ref.Name, ref.UID))
	}
	tenant, err := r.leases(key, secret.Data[lodiav1alpha1.KubeconfigKey])
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	now := r.Clock.Now()
	reason, messages := "", []string(nil)
	for _, l := range []struct{ name, reason string }{
		{lodiav1alpha1.ControllerManagerLease, lodiav1alpha1.ReasonControllerManagerNotLeading},
		{lodiav1alpha1.SchedulerLease, lodiav1alpha1.ReasonSchedulerNotLeading},
	} {
		lease, err := tenant.Get(ctx, l.name, metav1.GetOptions{})
		var message string
		switch {
		case apierrors.IsNotFound(err):
			message = "Lease " + lodiav1alpha1.LeaderElectionNamespace + "/" + l.name + " does not exist"
		case err != nil:
			logf.FromContext(ctx).Error(err, "read a leader Lease", "lease", l.name)
			return nil, nil
		default:
			message = r.stale(key, lease, now)
		}
		if message != "" {
			reason = cmp.Or(reason, l.reason)
			messages = append(messages, message)
		}
	}
	if reason != "" {
		return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, reason, strings.Join(messages, "; ")), nil
	}
	return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading,
		"the leader Leases of kube-controller-manager and kube-scheduler are renewed"), nil
}

// stale says why lease does not lead, or nothing when it does.
func (r *Reconciler) stale(key types.NamespacedName, lease *coordinationv1.Lease, now time.Time) string {
	duration := time.Duration(ptr.Deref(lease.Spec.LeaseDurationSeconds, 0)) * time.Second
	if now.Sub(r.changed(key, lease, now)) <= duration+r.Period {
		return ""
	}
	renewed := "never"
	if t := lease.Spec.RenewTime; t != nil {
		renewed = t.UTC().Format(time.RFC3339)
	}
	if holder := ptr.Deref(lease.Spec.HolderIdentity, ""); holder != "" {
		return fmt.Sprintf("Lease %s/%s of %s was last renewed %s", lease.Namespace, lease.Name, holder, renewed)
	}
	return fmt.Sprintf("Lease %s/%s has no holder; it was last renewed %s", lease.Namespace, lease.Name, renewed)
}

// changed returns when Health first saw the lease's current version.
func (r *Reconciler) changed(key types.NamespacedName, lease *coordinationv1.Lease, now time.Time) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.renewals == nil {
		r.renewals = map[types.NamespacedName]map[string]renewal{}
	}
	if r.renewals[key] == nil {
		r.renewals[key] = map[string]renewal{}
	}
	seen, ok := r.renewals[key][lease.Name]
	if !ok || seen.resourceVersion != lease.ResourceVersion {
		seen = renewal{resourceVersion: lease.ResourceVersion, at: now}
		r.renewals[key][lease.Name] = seen
	}
	return seen.at
}

func (r *Reconciler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.renewals, key)
	if c := r.clients[key]; c != nil && c.transport != nil {
		utilnet.CloseIdleConnectionsFor(c.transport.Transport)
	}
	delete(r.clients, key)
}

func (r *Reconciler) clientsOf(key types.NamespacedName) *clients {
	if r.clients == nil {
		r.clients = map[types.NamespacedName]*clients{}
	}
	if r.clients[key] == nil {
		r.clients[key] = &clients{}
	}
	return r.clients[key]
}

// prober verifies the endpoint against bundle, or is nil when bundle holds no
// certificate.
func (r *Reconciler) prober(key types.NamespacedName, bundle []byte) *http.Client {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.clientsOf(key)
	if c.probe != nil && bytes.Equal(c.bundle, bundle) {
		return c.probe
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle) {
		return nil
	}
	c.bundle, c.probe = bundle, &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return c.probe
}

// leases reads the tenant's leader Leases as Lodia, through the in-namespace
// Service, which is never behind a proxy from the environment. The client of
// a previous kubeconfig closes its idle connections.
func (r *Reconciler) leases(key types.NamespacedName, kubeconfig []byte) (coordinationv1client.LeaseInterface, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.clientsOf(key)
	if c.leases != nil && bytes.Equal(c.kubeconfig, kubeconfig) {
		return c.leases, nil
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	cfg.Proxy = http.ProxyURL(nil)
	cfg.UserAgent = rest.DefaultKubernetesUserAgent()
	transport, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, err
	}
	leases, err := coordinationv1client.NewForConfigAndClient(cfg, transport)
	if err != nil {
		return nil, err
	}
	if c.transport != nil {
		utilnet.CloseIdleConnectionsFor(c.transport.Transport)
	}
	c.kubeconfig, c.leases, c.transport = kubeconfig, leases.Leases(lodiav1alpha1.LeaderElectionNamespace), transport
	return c.leases, nil
}

// probe returns the status code the endpoint answered with, or 0 for none.
func (r *Reconciler) probe(ctx context.Context, prober *http.Client, url string) int {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0
	}
	resp, err := prober.Do(req)
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

func newCondition(cp *lodiav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason, message string) *metav1.Condition {
	return &metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: message, ObservedGeneration: cp.Generation}
}

func previous(cp *lodiav1alpha1.ControlPlane, conditionType string) metav1.Condition {
	if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c != nil {
		return *c
	}
	return *newCondition(cp, conditionType, metav1.ConditionUnknown, lodiav1alpha1.ReasonReconciling, "the first pass has not finished")
}

// apply writes Reachable and ControllersLeading only on a change, and both
// every time, since an apply that leaves one out removes it; it leaves cp
// with what it wrote.
func (r *Reconciler) apply(ctx context.Context, cp *lodiav1alpha1.ControlPlane, conditions ...metav1.Condition) error {
	all := slices.Clone(cp.Status.Conditions)
	changed := false
	var applied []any
	for _, c := range conditions {
		if previous := meta.FindStatusCondition(cp.Status.Conditions, c.Type); previous == nil || previous.Status != c.Status ||
			previous.Reason != c.Reason || previous.Message != c.Message || previous.ObservedGeneration != c.ObservedGeneration {
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
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
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
