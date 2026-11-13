// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package health is the Health controller: it probes each control plane's
// public endpoint and its in-namespace Service, reads its components' leader
// Leases inside the tenant, and, in NodesReconciler, samples the tenant's nodes.
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

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/events"
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
	"go.lodia.io/lodia/internal/live"
	"go.lodia.io/lodia/internal/ownership"
	"go.lodia.io/lodia/internal/restore"
	"go.lodia.io/lodia/internal/statusapply"
)

const (
	Name         = "health"
	FieldManager = "lodia/" + Name

	// accessNotTrue is the message of a pass while AccessReady is not True:
	// Reconciler reads nothing in the tenant then, and NodesReconciler reads only
	// the controller manager's Lease, which it renews or releases if it holds it.
	accessNotTrue = "AccessReady is not True"

	// leaderSlack is how long past its duration a leader Lease may stay
	// unchanged and still lead. A standby takes over from a leader that died
	// without releasing up to 8.8 s after the duration: two polls of the 2 s
	// retry period Workloads renders, each up to 2.2 times as long
	// (https://github.com/kubernetes/client-go/blob/v0.37.1/tools/leaderelection/leaderelection.go#L259-L274).
	leaderSlack = 10 * time.Second
)

type Reconciler struct {
	client.Client
	// Reader reads the CA bundle and Lodia's kubeconfig, whose data the cache
	// does not hold, and a condition Health keeps.
	Reader client.Reader
	// Events records the transitions of the conditions it writes;
	// SetupWithManager sets it.
	Events events.EventRecorder
	// Period is how often a control plane is probed and its leader Leases
	// read, Timeout how long a probe or a read waits for an answer.
	Period, Timeout time.Duration
	// FailuresBeforeFalse consecutive failed probes turn a True Reachable
	// False, so that one blip does not drop Ready.
	FailuresBeforeFalse int
	// Clock tells when a leader Lease was seen to change.
	Clock clock.PassiveClock
	// DialContext, when set, dials the probes of the public endpoint and of
	// the in-namespace Service, whose name nothing outside a cluster resolves.
	// It is nil in production.
	DialContext func(ctx context.Context, network, address string) (net.Conn, error)
	// ProbeFailures counts every failed probe by its path, the label path;
	// nil counts nothing.
	ProbeFailures *prometheus.CounterVec

	mu       sync.Mutex
	failures map[types.NamespacedName]int
	renewals map[types.NamespacedName]map[string]renewal
	clients  map[types.NamespacedName]*tenantClients
}

// tenantClients are the clients of one tenant over one transport, and the
// kubeconfig they were built from.
type tenantClients struct {
	kubeconfig []byte
	tenant     *tenant
	transport  *http.Client
}

// prober holds what a control plane's probes verify and present: the client
// of the probes that dial the host of their URL, the CAs of the bundle, and the
// tunnel's client certificate, in gateway mode.
type prober struct {
	direct *http.Client
	roots  *x509.CertPool
	client *tls.Certificate
}

// renewal is the digest of the resourceVersion of a Lease Health saw last, and
// when it saw that version first.
type renewal struct {
	version uint64
	at      time.Time
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	r.Events = mgr.GetEventRecorder("lodia.io/" + Name)
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.ControlPlane{}, builder.WithPredicates(predicate.Or[client.Object](predicate.GenerationChangedPredicate{}, inputsChanged()))).
		Complete(r)
}

// Between the periodic passes, only a change of the endpoint or of whether
// AccessReady is True brings one, besides a new generation, so failed probes
// count a period apart unless such a change comes between them.
func inputsChanged() predicate.Funcs {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		before, ok := e.ObjectOld.(*lodiav1alpha1.ControlPlane)
		after, ok2 := e.ObjectNew.(*lodiav1alpha1.ControlPlane)
		return !ok || !ok2 || endpoint(before) != endpoint(after) || access(before) != access(after)
	}}
}

func access(cp *lodiav1alpha1.ControlPlane) bool {
	c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAccessReady)
	return c != nil && c.Status == metav1.ConditionTrue
}

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

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
	if meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReachable) == nil || meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionControllersLeading) == nil {
		if err := r.apply(ctx, cp, previous(cp, lodiav1alpha1.ConditionReachable), previous(cp, lodiav1alpha1.ConditionControllersLeading)); err != nil {
			return ctrl.Result{}, err
		}
	}
	reachable, failed, err := r.reachable(ctx, cp)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !failed {
		r.count(req.NamespacedName, false)
	}
	leading, err := r.leading(ctx, cp)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.apply(ctx, cp, ptr.Deref(reachable, previous(cp, lodiav1alpha1.ConditionReachable)), ptr.Deref(leading, previous(cp, lodiav1alpha1.ConditionControllersLeading))); err != nil {
		return ctrl.Result{}, err
	}
	if failed {
		r.count(req.NamespacedName, true)
	}
	return ctrl.Result{RequeueAfter: r.Period}, nil
}

// reachable returns nil to keep the condition as the API server holds it: a
// True after fewer than r.FailuresBeforeFalse failed probes in a row. It also
// says whether its probe failed: a success clears the count at once, and a
// failure counts only once the pass has applied its conditions.
func (r *Reconciler) reachable(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*metav1.Condition, bool, error) {
	key := client.ObjectKeyFromObject(cp)
	url := endpoint(cp)
	if url == "" {
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint"), false, nil
	}
	gateway := cp.Spec.Endpoint.Gateway != nil
	if gateway && cp.Status.Endpoint.Tunnel == nil {
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint.tunnel"), false, nil
	}
	name := lodiav1alpha1.CABundleSecret.Name(cp.Name)
	secret := &corev1.Secret{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "waiting for PKI to write the CA bundle "+name), false, nil
		}
		return nil, false, err
	}
	if ref := ownership.ControlledElsewhere(secret, cp); ref != nil {
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates,
			fmt.Sprintf("Secret %s is kept by %s %s with UID %s", name, ref.Kind, ref.Name, ref.UID)), false, nil
	}
	var pair *corev1.Secret
	if gateway {
		pair = &corev1.Secret{}
		pairName := lodiav1alpha1.TunnelClientSecret.Name(cp.Name)
		if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: pairName}, pair); err != nil {
			if apierrors.IsNotFound(err) {
				return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "waiting for PKI to write the tunnel's client certificate "+pairName), false, nil
			}
			return nil, false, err
		}
		if ref := ownership.ControlledElsewhere(pair, cp); ref != nil {
			return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates,
				fmt.Sprintf("Secret %s is kept by %s %s with UID %s", pairName, ref.Kind, ref.Name, ref.UID)), false, nil
		}
	}
	prober := r.prober(secret.Data[lodiav1alpha1.CACertKey], pair)
	if prober == nil {
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "the CA bundle "+name+" holds no certificate"), false, nil
	}
	if gateway && prober.client == nil {
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "the tunnel's client certificate "+pair.Name+" holds no certificate and key"), false, nil
	}
	reason, answer := r.probes(ctx, cp, prober, url)
	if reason == "" {
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, answer), false, nil
	}
	if r.failed(key)+1 < r.FailuresBeforeFalse {
		if err := live.Conditions(ctx, r.Reader, cp, lodiav1alpha1.ConditionReachable); err != nil {
			return nil, false, err
		}
		if previous := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReachable); previous != nil && previous.Status == metav1.ConditionTrue {
			return nil, true, nil
		}
	}
	return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, reason, answer), true, nil
}

// probes runs the control plane's probes at once, each under Timeout, so that
// a dead path holds a worker for one Timeout: the public endpoint, the
// in-namespace Service and, in gateway mode, the tunnel and konnectivity; it
// counts each that fails, but none that ended after the pass's context: the
// pass cut those. It returns the reason of a failed pass, empty when all
// passed, and the condition's message: what each path that failed answered,
// or what answered.
func (r *Reconciler) probes(ctx context.Context, cp *lodiav1alpha1.ControlPlane, p *prober, url string) (string, string) {
	gateway := cp.Spec.Endpoint.Gateway != nil
	internal := lodiav1alpha1.APIServerServiceURL(cp.Namespace, cp.Name) + lodiav1alpha1.APIServerReadyzPath
	var (
		apiCode, internalCode, tunnelCode       int
		apiErr, internalErr, tunnelErr          error
		konnErr                                 error
		apiCut, internalCut, tunnelCut, konnCut bool
		wg                                      sync.WaitGroup
	)
	wg.Go(func() {
		apiCode, apiErr = r.probe(ctx, p.direct, url)
		apiCut = ctx.Err() != nil
	})
	wg.Go(func() {
		internalCode, internalErr = r.probe(ctx, p.direct, internal)
		internalCut = ctx.Err() != nil
	})
	e := cp.Status.Endpoint
	if gateway {
		wg.Go(func() {
			tunnelCode, tunnelErr = r.probe(ctx, r.throughTunnel(p, *e.Tunnel), url)
			tunnelCut = ctx.Err() != nil
		})
		wg.Go(func() {
			konnErr = r.handshake(ctx, p, e.Konnectivity)
			konnCut = ctx.Err() != nil
		})
	}
	wg.Wait()
	log := logf.FromContext(ctx)
	var failed []string
	fail := func(path string, cut bool, answer string) {
		if r.ProbeFailures != nil && !cut {
			r.ProbeFailures.WithLabelValues(path).Inc()
		}
		failed = append(failed, answer)
	}
	reason := lodiav1alpha1.ReasonExternalProbeFailed
	if internalCode != http.StatusOK {
		log.Error(internalErr, "the probe of the in-namespace Service failed", "url", internal, "code", internalCode)
		reason = lodiav1alpha1.ReasonInternalProbeFailed
		fail(lodiav1alpha1.MetricProbeInternal, internalCut, lodiav1alpha1.MetricProbeInternal+": "+internal+" "+answered(internalCode))
	}
	if apiCode != http.StatusOK {
		log.Error(apiErr, "the probe of the public endpoint failed", "url", url, "code", apiCode)
		answer := url + " " + answered(apiCode)
		if gateway {
			answer = lodiav1alpha1.MetricProbeAPI + ": " + answer
		}
		fail(lodiav1alpha1.MetricProbeAPI, apiCut, answer)
	}
	if gateway && tunnelCode != http.StatusOK {
		log.Error(tunnelErr, "the probe through the tunnel failed", "tunnel", hostPort(*e.Tunnel), "code", tunnelCode)
		fail(lodiav1alpha1.MetricProbeTunnel, tunnelCut, lodiav1alpha1.MetricProbeTunnel+": "+url+" through "+hostPort(*e.Tunnel)+" "+answered(tunnelCode))
	}
	if gateway && konnErr != nil {
		log.Error(konnErr, "the TLS handshake with konnectivity failed", "address", hostPort(e.Konnectivity))
		fail(lodiav1alpha1.MetricProbeKonnectivity, konnCut, lodiav1alpha1.MetricProbeKonnectivity+": "+hostPort(e.Konnectivity)+" did not complete a TLS handshake")
	}
	if len(failed) > 0 {
		return reason, strings.Join(failed, "; ")
	}
	if !gateway {
		return "", url + " answered 200"
	}
	return "", url + " answered 200 at the public endpoint and through the tunnel " + hostPort(*e.Tunnel) + ", and " + hostPort(e.Konnectivity) + " completed a TLS handshake"
}

func answered(code int) string {
	if code == 0 {
		return "did not answer"
	}
	return fmt.Sprintf("answered %d", code)
}

// leading returns nil to keep the condition as the API server holds it when a
// read of a Lease fails (docs/README.md, Health).
func (r *Reconciler) leading(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*metav1.Condition, error) {
	key := client.ObjectKeyFromObject(cp)
	if !access(cp) {
		r.forget(key)
		return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, accessNotTrue), nil
	}
	kubeconfig, waiting, err := lodiaKubeconfig(ctx, r.Reader, cp)
	if err != nil {
		return nil, err
	}
	if waiting != "" {
		r.forget(key)
		return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, waiting), nil
	}
	tenant, err := r.tenant(key, kubeconfig)
	if err != nil {
		return nil, err
	}
	if restoring := cp.Spec.RestoreFrom; restoring != nil {
		if restoring.Release != lodiav1alpha1.RestoreReleased {
			return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease,
				"spec.restoreFrom.release is Held; once released, the controller manager acts on: "+r.actedOn(ctx, tenant)), nil
		}
		source, err := restore.Source(ctx, r.Client, cp)
		if err != nil {
			return nil, err
		}
		if source != nil {
			return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonSourceExists,
				fmt.Sprintf("ControlPlane %s (UID %s), the Backup's source, still exists in the namespace; delete it to release this one", source.Name, source.UID)), nil
		}
	}
	reading, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	now := r.Clock.Now()
	reason, messages, held := "", []string(nil), false
	for _, l := range []struct{ name, reason string }{
		{lodiav1alpha1.ControllerManagerLease, lodiav1alpha1.ReasonControllerManagerNotLeading},
		{lodiav1alpha1.SchedulerLease, lodiav1alpha1.ReasonSchedulerNotLeading},
	} {
		lease, err := tenant.getLease(reading, l.name)
		var message string
		switch {
		case apierrors.IsNotFound(err):
			message = "Lease " + lodiav1alpha1.LeaderElectionNamespace + "/" + l.name + " does not exist"
		case err != nil:
			logf.FromContext(ctx).Error(err, "read a leader Lease", "lease", l.name)
			return nil, live.Conditions(ctx, r.Reader, cp, lodiav1alpha1.ConditionControllersLeading)
		case l.name == lodiav1alpha1.ControllerManagerLease && ptr.Deref(lease.Spec.HolderIdentity, "") == lodiav1alpha1.HoldHolderIdentity:
			held = true
		default:
			message = r.stale(key, l.name, lease, now)
		}
		if message != "" {
			reason = cmp.Or(reason, l.reason)
			messages = append(messages, message)
		}
	}
	if reason != "" {
		return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, reason, strings.Join(messages, "; ")), nil
	}
	if held {
		return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionTrue, lodiav1alpha1.ReasonHeld,
			"Lodia holds the leader Lease of kube-controller-manager, and that of kube-scheduler is renewed"), nil
	}
	return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading,
		"the leader Leases of kube-controller-manager and kube-scheduler are renewed"), nil
}

// lodiaKubeconfig returns Lodia's kubeconfig for cp, or why it cannot be used
// yet: <cp>-lodia-kubeconfig is missing or kept by another owner.
func lodiaKubeconfig(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane) ([]byte, string, error) {
	name := lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name)
	secret := &corev1.Secret{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, "waiting for PKI to write " + name, nil
		}
		return nil, "", err
	}
	if ref := ownership.ControlledElsewhere(secret, cp); ref != nil {
		return nil, fmt.Sprintf("Secret %s is kept by %s %s with UID %s", name, ref.Kind, ref.Name, ref.UID), nil
	}
	return secret.Data[lodiav1alpha1.KubeconfigKey], "", nil
}

// listLimit is the page of each list actedOn reads; the kinds select by no
// field it needs, so it filters on the client and counts the first page.
const listLimit = 500

// actedOn counts, kind by kind, what a held restore's controller manager acts
// on outside the cluster once it runs, naming the first three by name; a kind
// that cannot be listed says so in its place. The lists run at once, so a
// tenant that does not answer holds a worker for one Timeout, not seven.
func (r *Reconciler) actedOn(ctx context.Context, t *tenant) string {
	type list struct {
		kind string
		read func(context.Context) (tally, error)
	}
	items := []struct {
		what  string
		lists []list
	}{
		{"LoadBalancer Services", []list{{"Services", func(ctx context.Context) (tally, error) {
			l, err := firstPage(ctx, t, "/api/v1/services", serviceOf)
			if err != nil {
				return tally{}, err
			}
			found := tally{more: l.Metadata.Continue != ""}
			for _, s := range l.Items.items {
				if s.Spec.Type == corev1.ServiceTypeLoadBalancer {
					found.add(s.Metadata.Namespace, s.Metadata.Name)
				}
			}
			return found, nil
		}}}},
		{"PersistentVolumes with reclaimPolicy Delete", []list{{"PersistentVolumes", func(ctx context.Context) (tally, error) {
			l, err := firstPage(ctx, t, "/api/v1/persistentvolumes", persistentVolumeOf)
			if err != nil {
				return tally{}, err
			}
			found := tally{more: l.Metadata.Continue != ""}
			for _, v := range l.Items.items {
				if v.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimDelete {
					found.add(v.Metadata.Name)
				}
			}
			return found, nil
		}}}},
		{"pending PersistentVolumeClaims", []list{{"PersistentVolumeClaims", func(ctx context.Context) (tally, error) {
			l, err := firstPage(ctx, t, "/api/v1/persistentvolumeclaims", persistentVolumeClaimOf)
			if err != nil {
				return tally{}, err
			}
			found := tally{more: l.Metadata.Continue != ""}
			for _, c := range l.Items.items {
				if c.Status.Phase == corev1.ClaimPending {
					found.add(c.Metadata.Namespace, c.Metadata.Name)
				}
			}
			return found, nil
		}}}},
		{"webhooks that call a URL or fail closed", []list{
			{"ValidatingWebhookConfigurations", func(ctx context.Context) (tally, error) {
				return outsideWebhooks(ctx, t, "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations")
			}},
			{"MutatingWebhookConfigurations", func(ctx context.Context) (tally, error) {
				return outsideWebhooks(ctx, t, "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations")
			}},
		}},
		{"APIServices served by a Service", []list{{"APIServices", func(ctx context.Context) (tally, error) {
			l, err := firstPage(ctx, t, "/apis/apiregistration.k8s.io/v1/apiservices", apiServiceOf)
			if err != nil {
				return tally{}, err
			}
			found := tally{more: l.Metadata.Continue != ""}
			for _, a := range l.Items.items {
				if a.Spec.Service {
					found.add(a.Metadata.Name)
				}
			}
			return found, nil
		}}}},
		{"CronJobs not suspended", []list{{"CronJobs", func(ctx context.Context) (tally, error) {
			l, err := firstPage(ctx, t, "/apis/batch/v1/cronjobs", cronJobOf)
			if err != nil {
				return tally{}, err
			}
			found := tally{more: l.Metadata.Continue != ""}
			for _, j := range l.Items.items {
				if !ptr.Deref(j.Spec.Suspend, false) {
					found.add(j.Metadata.Namespace, j.Metadata.Name)
				}
			}
			return found, nil
		}}}},
	}
	type answer struct {
		found tally
		err   error
	}
	results := make([][]answer, len(items))
	var wg sync.WaitGroup
	for i, item := range items {
		results[i] = make([]answer, len(item.lists))
		for j, l := range item.lists {
			wg.Go(func() {
				listing, cancel := context.WithTimeout(ctx, r.Timeout)
				defer cancel()
				found, err := l.read(listing)
				results[i][j] = answer{found, err}
			})
		}
	}
	wg.Wait()
	var out []string
	for i, item := range items {
		var found tally
		var failed []string
		for j, l := range item.lists {
			got := results[i][j]
			if got.err != nil {
				logf.FromContext(ctx).Error(got.err, "list in the tenant", "kind", l.kind)
				failed = append(failed, l.kind)
				continue
			}
			found.merge(got.found)
		}
		if len(failed) > 0 {
			out = append(out, strings.Join(failed, " and ")+" could not be listed")
			continue
		}
		out = append(out, counted(item.what, found))
	}
	return strings.Join(out, "; ")
}

// outsideWebhooks counts the webhooks of one kind of configuration that act
// outside the cluster, one configuration at a time, so that the list keeps no
// webhook; a configuration with more webhooks than maxWebhooks makes the list
// fail.
func outsideWebhooks(ctx context.Context, t *tenant, path string) (tally, error) {
	var found tally
	page, err := firstPage(ctx, t, path, func(data []byte) (struct{}, error) {
		c, err := webhookConfigurationOf(data)
		if err != nil {
			return struct{}{}, err
		}
		if c.Webhooks.over {
			return struct{}{}, errOverBound
		}
		for _, w := range c.Webhooks.items {
			if w.actsOutside() {
				found.add(c.Metadata.Name, w.Name)
			}
		}
		return struct{}{}, nil
	})
	if err != nil {
		return tally{}, err
	}
	found.more = page.Metadata.Continue != ""
	return found, nil
}

const shown = 3

// tally counts what a held restore's controller manager acts on in one kind,
// and keeps the shown names that sort first. A name with a part over maxName is
// counted and not kept, so that the message stays under the schema's bound
// (32,768 bytes) whatever the tenant answers; the kept names reuse their
// buffers, so that no name is built for one that does not qualify.
type tally struct {
	n     int
	names [][]byte
	more  bool
}

func (t *tally) add(parts ...string) {
	t.n++
	var buf [2*maxName + 1]byte
	name := buf[:0]
	for i, part := range parts {
		if len(part) > maxName {
			return
		}
		if i > 0 {
			name = append(name, '/')
		}
		name = append(name, part...)
	}
	t.keep(name)
}

func (t *tally) keep(name []byte) {
	at := 0
	for at < len(t.names) && bytes.Compare(t.names[at], name) < 0 {
		at++
	}
	if at == shown {
		return
	}
	var slot []byte
	if len(t.names) == shown {
		slot = t.names[shown-1][:0]
		t.names = t.names[:shown-1]
	} else {
		slot = make([]byte, 0, 2*maxName+1)
	}
	t.names = slices.Insert(t.names, at, append(slot, name...))
}

func (t *tally) merge(other tally) {
	t.n += other.n
	t.more = t.more || other.more
	for _, name := range other.names {
		t.keep(name)
	}
}

func counted(what string, t tally) string {
	n := strconv.Itoa(t.n)
	if t.more {
		n += " or more"
	}
	if len(t.names) == 0 {
		return n + " " + what
	}
	names := make([]string, len(t.names))
	for i, name := range t.names {
		names[i] = string(name)
	}
	quoted := strings.Join(names, ", ")
	if t.n > len(t.names) || t.more {
		quoted += "…"
	}
	return n + " " + what + " (" + quoted + ")"
}

// stale says why the leader Lease name does not lead, or nothing when it does.
func (r *Reconciler) stale(key types.NamespacedName, name string, lease *leaderLease, now time.Time) string {
	duration := time.Duration(ptr.Deref(lease.Spec.LeaseDurationSeconds, 0)) * time.Second
	if now.Sub(r.changed(key, name, lease, now)) <= duration+leaderSlack {
		return ""
	}
	renewed := "never"
	if t := lease.Spec.RenewTime; t != nil {
		renewed = t.UTC().Format(time.RFC3339)
	}
	if holder := ptr.Deref(lease.Spec.HolderIdentity, ""); holder != "" {
		return fmt.Sprintf("Lease %s/%s of %s was last renewed %s", lodiav1alpha1.LeaderElectionNamespace, name, quoted(holder), renewed)
	}
	return fmt.Sprintf("Lease %s/%s has no holder; it was last renewed %s", lodiav1alpha1.LeaderElectionNamespace, name, renewed)
}

// changed returns when Health first saw the current version of the leader Lease name.
func (r *Reconciler) changed(key types.NamespacedName, name string, lease *leaderLease, now time.Time) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.renewals == nil {
		r.renewals = map[types.NamespacedName]map[string]renewal{}
	}
	if r.renewals[key] == nil {
		r.renewals[key] = map[string]renewal{}
	}
	seen, ok := r.renewals[key][name]
	if version := versionOf(lease.Metadata.ResourceVersion); !ok || seen.version != version {
		seen = renewal{version: version, at: now}
		r.renewals[key][name] = seen
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

// prober verifies the endpoint against bundle, or is nil when bundle holds no
// certificate. Its client pair is pair's, and nil outside gateway mode or when
// pair holds no certificate and key.
func (r *Reconciler) prober(bundle []byte, pair *corev1.Secret) *prober {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle) {
		return nil
	}
	direct := &http.Transport{
		DialContext:         r.dial,
		TLSClientConfig:     &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout: r.Timeout,
		DisableKeepAlives:   true,
	}
	p := &prober{direct: &http.Client{Transport: direct, CheckRedirect: noRedirects}, roots: roots}
	if pair != nil {
		if parsed, err := tls.X509KeyPair(pair.Data[lodiav1alpha1.TLSCertKey], pair.Data[lodiav1alpha1.TLSPrivateKeyKey]); err == nil {
			p.client = &parsed
		}
	}
	return p
}

// dial is bounded by Timeout because net/http dials on a context detached from
// the request's, as throughTunnel says.
func (r *Reconciler) dial(ctx context.Context, network, address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	if r.DialContext != nil {
		return r.DialContext(ctx, network, address)
	}
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

func noRedirects(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

func (r *Reconciler) tenant(key types.NamespacedName, kubeconfig []byte) (*tenant, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.clients == nil {
		r.clients = map[types.NamespacedName]*tenantClients{}
	}
	if r.clients[key] == nil {
		r.clients[key] = &tenantClients{}
	}
	return r.clients[key].of(kubeconfig)
}

// of returns the clients that read inside the tenant as Lodia, through the
// in-namespace Service, which is never behind a proxy from the environment. The
// clients of a previous kubeconfig close their idle connections.
func (c *tenantClients) of(kubeconfig []byte) (*tenant, error) {
	if c.tenant != nil && bytes.Equal(c.kubeconfig, kubeconfig) {
		return c.tenant, nil
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	cfg.Proxy = http.ProxyURL(nil)
	cfg.UserAgent = rest.DefaultKubernetesUserAgent()
	// A QPS below zero turns client-go's limiter off. Its default, 5 requests a
	// second with a burst of 10, stops a list of the Nodes at about 34 pages
	// inside Timeout. Without it, since tenant.do sends a request once, a pass of
	// health-nodes sends at most maxPages requests for each of the two lists and
	// the hold's: a read of the controller manager's Lease, then up to
	// retry.DefaultRetry's five patches, each after the first preceded by a read
	// of the Lease again (5 patches and 5 reads when every patch conflicts); a
	// pass of health sends the reads of the two leader Leases, or one page of each
	// of a held restore's seven lists. capResponses cuts the headers and the body
	// of every answer; the tenant's API server limits what it serves itself
	// (https://kubernetes.io/docs/concepts/cluster-administration/flow-control/).
	cfg.QPS = -1
	cfg.Wrap(capResponses)
	cfg.AcceptContentTypes, cfg.ContentType = "application/json", "application/json"
	// client-go asks for a serializer, and DoRaw never uses it: no response is
	// decoded into the API's types.
	cfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	transport, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, err
	}
	api, err := rest.UnversionedRESTClientForConfigAndClient(cfg, transport)
	if err != nil {
		return nil, err
	}
	if c.transport != nil {
		utilnet.CloseIdleConnectionsFor(c.transport.Transport)
	}
	c.kubeconfig, c.transport, c.tenant = kubeconfig, transport, newTenant(api)
	return c.tenant, nil
}

// probe returns the status code the endpoint answered with, or 0 for none, and
// the error that says why.
func (r *Reconciler) probe(ctx context.Context, prober *http.Client, url string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := prober.Do(req)
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

// throughTunnel reaches the API server through the tunnel: the outer TLS to
// tunnel with the client certificate, and the inner TLS to the request's host
// verified against the bundle, as a pod's own connection through
// apiserver-proxy does.
// net/http dials and hands the inner TLS over on a context of its own, detached
// from the request's (https://github.com/golang/go/blob/go1.26.10/src/net/http/transport.go#L1524-L1529),
// so the dial and both handshakes need Timeout's bound here: without them a
// stalled Gateway would keep a goroutine for every probe.
func (r *Reconciler) throughTunnel(p *prober, tunnel lodiav1alpha1.HostPort) *http.Client {
	dialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: r.Timeout},
		Config:    &tls.Config{ServerName: tunnel.Host, RootCAs: p.roots, Certificates: []tls.Certificate{*p.client}, MinVersion: tls.VersionTLS13},
	}
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, "tcp", hostPort(tunnel))
			},
			TLSClientConfig:     &tls.Config{RootCAs: p.roots, MinVersion: tls.VersionTLS12},
			TLSHandshakeTimeout: r.Timeout,
			DisableKeepAlives:   true,
		},
		CheckRedirect: noRedirects,
	}
}

// handshake completes a TLS handshake with the konnectivity host, verified
// against the bundle.
func (r *Reconciler) handshake(ctx context.Context, p *prober, target lodiav1alpha1.HostPort) error {
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: r.Timeout}, Config: &tls.Config{ServerName: target.Host, RootCAs: p.roots, MinVersion: tls.VersionTLS12}}
	conn, err := dialer.DialContext(ctx, "tcp", hostPort(target))
	if err != nil {
		return err
	}
	return conn.Close()
}

// count counts a failed probe of a ControlPlane, or clears its count. Every
// success clears it, so a ControlPlane recreated under the same name is True,
// and keeps a True on a failure, only after a success of its own.
func (r *Reconciler) count(key types.NamespacedName, failed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !failed {
		delete(r.failures, key)
		return
	}
	if r.failures == nil {
		r.failures = map[types.NamespacedName]int{}
	}
	r.failures[key]++
}

func (r *Reconciler) failed(key types.NamespacedName) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failures[key]
}

func endpoint(cp *lodiav1alpha1.ControlPlane) string {
	if cp.Status.Endpoint == nil {
		return ""
	}
	return "https://" + hostPort(cp.Status.Endpoint.APIServer) + lodiav1alpha1.APIServerReadyzPath
}

func hostPort(address lodiav1alpha1.HostPort) string {
	return net.JoinHostPort(address.Host, strconv.Itoa(int(address.Port)))
}

func newCondition(cp *lodiav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason, message string) *metav1.Condition {
	return &metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: message, ObservedGeneration: cp.Generation}
}

// previous keeps the condition as it is but at cp's generation, since Health
// applies nothing from the spec.
func previous(cp *lodiav1alpha1.ControlPlane, conditionType string) metav1.Condition {
	if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c != nil {
		kept := *c
		kept.ObservedGeneration = cp.Generation
		return kept
	}
	return statusapply.FirstPass(cp, conditionType)
}

// apply writes Reachable and ControllersLeading only on a change, and both
// every time, since an apply that leaves one out removes it; it leaves cp
// with what it wrote.
func (r *Reconciler) apply(ctx context.Context, cp *lodiav1alpha1.ControlPlane, conditions ...metav1.Condition) error {
	return statusapply.Writer{Client: r.Client, Reader: r.Reader, Manager: FieldManager, Events: r.Events}.ApplyInto(ctx, cp, lodiav1alpha1.ControlPlaneStatus{Conditions: conditions}, false)
}
