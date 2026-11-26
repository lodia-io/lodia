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
	"golang.org/x/sync/semaphore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	"go.lodia.io/lodia/internal/contract"
	"go.lodia.io/lodia/internal/live"
	"go.lodia.io/lodia/internal/ownership"
	"go.lodia.io/lodia/internal/restore"
	"go.lodia.io/lodia/internal/statusapply"
	"go.lodia.io/lodia/internal/tenantclient"
)

const (
	Name         = "health"
	FieldManager = "lodia/" + Name

	// restoreReads is how many reads a pass has in flight at once, the seven
	// lists of a held restore.
	restoreReads = 7

	// heldRestores is how many passes over a held restore list at once, so that
	// the memory of their lists does not grow with the workers.
	heldRestores = 10

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
	// Reader reads a condition Health keeps from the API server, and a Secret
	// again when its copy makes a pass wait.
	Reader client.Reader
	// Secrets reads the CA bundle, the tunnel's client pair and Lodia's
	// kubeconfig, whose data the cache does not hold.
	Secrets client.Reader
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
	// DialContext, when set, opens every connection to the tenant: the probes,
	// the tunnel's and konnectivity's included, and the tenant clients', whose
	// names nothing outside a cluster resolves. It is nil in production.
	DialContext func(ctx context.Context, network, address string) (net.Conn, error)
	// ProbeFailures counts every failed probe by its path, the label path;
	// nil counts nothing.
	ProbeFailures *prometheus.CounterVec

	mu       sync.Mutex
	failures map[types.NamespacedName]int
	// rereads holds the ControlPlanes whose last pass found a condition to
	// write: the next reads both live, since the cache can lag that write, and
	// a pass that compared with a cache still holding the condition it
	// replaced would write nothing.
	rereads  map[types.NamespacedName]bool
	renewals map[types.NamespacedName]map[string]renewal
	clients  *tenantclient.Clients
	restores *semaphore.Weighted
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
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		if apierrors.IsNotFound(err) {
			r.count(req.NamespacedName, false)
			r.rereadNext(req.NamespacedName, false)
			r.forget(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cp.DeletionTimestamp.IsZero() || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	if r.reread(req.NamespacedName) {
		if err := live.Conditions(ctx, r.Reader, cp, lodiav1alpha1.ConditionReachable, lodiav1alpha1.ConditionControllersLeading); err != nil {
			return ctrl.Result{}, err
		}
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
	if failed == "" {
		r.count(req.NamespacedName, false)
	}
	leading, err := r.leading(ctx, cp, failed == lodiav1alpha1.ReasonInternalProbeFailed)
	if err != nil {
		return ctrl.Result{}, err
	}
	conditions := []metav1.Condition{ptr.Deref(reachable, previous(cp, lodiav1alpha1.ConditionReachable)), ptr.Deref(leading, previous(cp, lodiav1alpha1.ConditionControllersLeading))}
	r.rereadNext(req.NamespacedName, slices.ContainsFunc(conditions, func(c metav1.Condition) bool {
		return statusapply.Differs(meta.FindStatusCondition(cp.Status.Conditions, c.Type), c)
	}))
	if err := r.apply(ctx, cp, conditions...); err != nil {
		return ctrl.Result{}, err
	}
	if failed != "" {
		r.count(req.NamespacedName, true)
	}
	return ctrl.Result{RequeueAfter: r.Period}, nil
}

// reachable returns nil to keep the condition as the API server holds it: a
// True after fewer than r.FailuresBeforeFalse failed probes in a row. It also
// returns the reason of a failed probe, empty when none failed: a success
// clears the count at once, and a failure counts only once the pass has
// applied its conditions.
func (r *Reconciler) reachable(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*metav1.Condition, string, error) {
	key := client.ObjectKeyFromObject(cp)
	url := endpoint(cp)
	if url == "" {
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint"), "", nil
	}
	gateway := cp.Spec.Endpoint.Gateway != nil
	if gateway && cp.Status.Endpoint.Tunnel == nil {
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint.tunnel"), "", nil
	}
	prober, waiting, err := r.certificates(ctx, cp)
	if err != nil {
		return nil, "", err
	}
	if waiting != nil {
		return waiting, "", nil
	}
	reason, answer := r.probes(ctx, cp, prober, url)
	if reason == "" {
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, answer), "", nil
	}
	if r.failed(key)+1 < r.FailuresBeforeFalse {
		if err := live.Conditions(ctx, r.Reader, cp, lodiav1alpha1.ConditionReachable); err != nil {
			return nil, "", err
		}
		if previous := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReachable); previous != nil && previous.Status == metav1.ConditionTrue {
			return nil, reason, nil
		}
	}
	return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, reason, answer), reason, nil
}

// certificates reads the CA bundle and, in gateway mode, the tunnel's client
// pair, and returns the prober they make or the condition that says why the
// pass waits for them.
func (r *Reconciler) certificates(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*prober, *metav1.Condition, error) {
	var p *prober
	name := lodiav1alpha1.CABundleSecret.Name(cp.Name)
	waiting, err := r.accessSecret(ctx, cp, name, "waiting for PKI to write the CA bundle "+name, func(secret *corev1.Secret) string {
		if ref := ownership.ControlledElsewhere(secret, cp); ref != nil {
			return fmt.Sprintf("Secret %s is kept by %s %s with UID %s", name, ref.Kind, ref.Name, ref.UID)
		}
		if p = r.prober(secret.Data[lodiav1alpha1.CACertKey]); p == nil {
			return "the CA bundle " + name + " holds no certificate"
		}
		return ""
	})
	if waiting != nil || err != nil || cp.Spec.Endpoint.Gateway == nil {
		return p, waiting, err
	}
	pairName := lodiav1alpha1.TunnelClientSecret.Name(cp.Name)
	waiting, err = r.accessSecret(ctx, cp, pairName, "waiting for PKI to write the tunnel's client certificate "+pairName, func(secret *corev1.Secret) string {
		if ref := ownership.ControlledElsewhere(secret, cp); ref != nil {
			return fmt.Sprintf("Secret %s is kept by %s %s with UID %s", pairName, ref.Kind, ref.Name, ref.UID)
		}
		pair, err := tls.X509KeyPair(secret.Data[lodiav1alpha1.TLSCertKey], secret.Data[lodiav1alpha1.TLSPrivateKeyKey])
		if err != nil {
			return "the tunnel's client certificate " + pairName + " holds no certificate and key"
		}
		p.client = &pair
		return ""
	})
	return p, waiting, err
}

// accessSecret reads a Secret of the access through its copies and returns the
// condition that says why the pass waits for it, nil when it can be used. A copy
// that unusable rejects is read again from the API server and the pass decides
// on that: a wait is never decided on a copy.
func (r *Reconciler) accessSecret(ctx context.Context, cp *lodiav1alpha1.ControlPlane, name, missing string, unusable func(*corev1.Secret) string) (*metav1.Condition, error) {
	key := client.ObjectKey{Namespace: cp.Namespace, Name: name}
	secret := &corev1.Secret{}
	err := r.Secrets.Get(ctx, key, secret)
	message := ""
	if err == nil {
		if message = unusable(secret); message != "" {
			secret = &corev1.Secret{}
			if err = r.Reader.Get(ctx, key, secret); err == nil {
				message = unusable(secret)
			}
		}
	}
	if apierrors.IsNotFound(err) {
		message, err = missing, nil
	}
	if message == "" || err != nil {
		return nil, err
	}
	return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, message), nil
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
	internal := lodiav1alpha1.APIServerServiceURL(cp.Namespace, cp.Name) + contract.APIServerReadyzPath
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
// read of a Lease fails (docs/README.md, Health). When the in-namespace
// Service, the address of every read in the tenant, has just failed its probe,
// it decides only what reads nothing in the tenant and keeps the condition
// otherwise, so that a tenant that hangs holds a worker for one Timeout.
func (r *Reconciler) leading(ctx context.Context, cp *lodiav1alpha1.ControlPlane, unanswered bool) (*metav1.Condition, error) {
	key := client.ObjectKeyFromObject(cp)
	if !access(cp) {
		r.forget(key)
		return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, accessNotTrue), nil
	}
	kubeconfig, waiting, err := lodiaKubeconfig(ctx, r.Secrets, r.Reader, cp)
	if err != nil {
		return nil, err
	}
	if waiting != "" {
		r.forget(key)
		return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, waiting), nil
	}
	restoring := cp.Spec.RestoreFrom
	if restoring != nil && restoring.Release == lodiav1alpha1.RestoreReleased {
		source, err := restore.Source(ctx, r.Client, cp)
		if err != nil {
			return nil, err
		}
		if source != nil {
			return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonSourceExists,
				fmt.Sprintf("ControlPlane %s (UID %s), the Backup's source, still exists in the namespace; delete it to release this one", source.Name, source.UID)), nil
		}
	}
	if unanswered {
		return nil, live.Conditions(ctx, r.Reader, cp, lodiav1alpha1.ConditionControllersLeading)
	}
	tenant, err := r.tenant(key, kubeconfig)
	if err != nil {
		return nil, err
	}
	if restoring != nil && restoring.Release != lodiav1alpha1.RestoreReleased {
		return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease,
			"spec.restoreFrom.release is Pending; once released, the controller manager acts on: "+r.actedOn(ctx, tenant)), nil
	}
	reading, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	now := r.Clock.Now()
	reason, messages, held := "", []string(nil), false
	for _, l := range []struct{ name, reason string }{
		{contract.ControllerManagerLease, lodiav1alpha1.ReasonControllerManagerNotLeading},
		{contract.SchedulerLease, lodiav1alpha1.ReasonSchedulerNotLeading},
	} {
		lease, err := tenant.getLease(reading, l.name)
		var message string
		switch {
		case apierrors.IsNotFound(err):
			message = "Lease " + contract.LeaderElectionNamespace + "/" + l.name + " does not exist"
		case err != nil:
			logf.FromContext(ctx).Error(err, "read a leader Lease", "lease", l.name)
			return nil, live.Conditions(ctx, r.Reader, cp, lodiav1alpha1.ConditionControllersLeading)
		case l.name == contract.ControllerManagerLease && ptr.Deref(lease.Spec.HolderIdentity, "") == contract.HoldHolderIdentity:
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
// yet: <cp>-lodia-kubeconfig is missing or kept by another owner. A copy that
// shows it kept by another owner is read again through live, and the pass
// decides on that: a wait is never decided on a copy.
func lodiaKubeconfig(ctx context.Context, copies, live client.Reader, cp *lodiav1alpha1.ControlPlane) ([]byte, string, error) {
	name := lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name)
	key := client.ObjectKey{Namespace: cp.Namespace, Name: name}
	secret := &corev1.Secret{}
	err := copies.Get(ctx, key, secret)
	if err == nil && ownership.ControlledElsewhere(secret, cp) != nil {
		secret = &corev1.Secret{}
		err = live.Get(ctx, key, secret)
	}
	if err != nil {
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
			for _, s := range l.Items.Items {
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
			for _, v := range l.Items.Items {
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
			for _, c := range l.Items.Items {
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
			for _, a := range l.Items.Items {
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
			for _, j := range l.Items.Items {
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
	listing, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	results := make([][]answer, len(items))
	for i, item := range items {
		results[i] = make([]answer, len(item.lists))
	}
	if err := r.restoreLists().Acquire(listing, 1); err != nil {
		for i := range results {
			for j := range results[i] {
				results[i][j] = answer{err: err}
			}
		}
	} else {
		var wg sync.WaitGroup
		for i, item := range items {
			for j, l := range item.lists {
				wg.Go(func() {
					found, err := l.read(listing)
					results[i][j] = answer{found, err}
				})
			}
		}
		wg.Wait()
		r.restoreLists().Release(1)
	}
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
		if c.Webhooks.Over {
			return struct{}{}, errOverBound
		}
		for _, w := range c.Webhooks.Items {
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
// and keeps the shown names that sort first. A name with a part over tenantclient.MaxName is
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
	var buf [2*tenantclient.MaxName + 1]byte
	name := buf[:0]
	for i, part := range parts {
		if len(part) > tenantclient.MaxName {
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
		slot = make([]byte, 0, 2*tenantclient.MaxName+1)
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
		return fmt.Sprintf("Lease %s/%s of %s was last renewed %s", contract.LeaderElectionNamespace, name, quoted(holder), renewed)
	}
	return fmt.Sprintf("Lease %s/%s has no holder; it was last renewed %s", contract.LeaderElectionNamespace, name, renewed)
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
	if r.clients != nil {
		r.clients.Forget(key)
	}
}

func (r *Reconciler) restoreLists() *semaphore.Weighted {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.restores == nil {
		r.restores = semaphore.NewWeighted(heldRestores)
	}
	return r.restores
}

// prober verifies the endpoint against bundle, or is nil when bundle holds no
// certificate. The caller sets its client pair in gateway mode.
func (r *Reconciler) prober(bundle []byte) *prober {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle) {
		return nil
	}
	direct := &http.Transport{
		DialContext:            r.dial,
		TLSClientConfig:        &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:    r.Timeout,
		DisableKeepAlives:      true,
		MaxResponseHeaderBytes: tenantclient.MaxResponseHeaderBytes,
	}
	return &prober{direct: &http.Client{Transport: direct, CheckRedirect: noRedirects}, roots: roots}
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
	if r.clients == nil {
		r.clients = tenantclient.NewClients(tenantclient.Options{Connections: restoreReads, Timeout: r.Timeout, Dial: r.DialContext})
	}
	clients := r.clients
	r.mu.Unlock()
	client, err := clients.Of(key, kubeconfig)
	if err != nil {
		return nil, err
	}
	return newTenant(client), nil
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
	// net/http drains a body closed before its end to reuse its connection: a
	// probe reads no body and reuses no connection, so it asks to close.
	req.Close = true
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
// from the request's (https://github.com/golang/go/blob/go1.27.4/src/net/http/transport.go#L1591-L1596),
// so the dial and both handshakes need Timeout's bound here: without them a
// stalled Gateway would keep a goroutine for every probe.
func (r *Reconciler) throughTunnel(p *prober, tunnel lodiav1alpha1.HostPort) *http.Client {
	outer := &tls.Config{ServerName: tunnel.Host, RootCAs: p.roots, Certificates: []tls.Certificate{*p.client}, MinVersion: tls.VersionTLS13}
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return r.dialTLS(ctx, hostPort(tunnel), outer)
			},
			TLSClientConfig:        &tls.Config{RootCAs: p.roots, MinVersion: tls.VersionTLS12},
			TLSHandshakeTimeout:    r.Timeout,
			DisableKeepAlives:      true,
			MaxResponseHeaderBytes: tenantclient.MaxResponseHeaderBytes,
		},
		CheckRedirect: noRedirects,
	}
}

// handshake completes a TLS handshake with the konnectivity host, verified
// against the bundle.
func (r *Reconciler) handshake(ctx context.Context, p *prober, target lodiav1alpha1.HostPort) error {
	conn, err := r.dialTLS(ctx, hostPort(target), &tls.Config{ServerName: target.Host, RootCAs: p.roots, MinVersion: tls.VersionTLS12})
	if err != nil {
		return err
	}
	return conn.Close()
}

// dialTLS dials address and completes a TLS handshake with config, both
// within Timeout, as tls.Dialer does with a net.Dialer of that timeout.
func (r *Reconciler) dialTLS(ctx context.Context, address string, config *tls.Config) (*tls.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	raw, err := r.dial(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	conn := tls.Client(raw, config)
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return conn, nil
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

// rereadNext records whether key's next pass reads its conditions live.
func (r *Reconciler) rereadNext(key types.NamespacedName, reread bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !reread {
		delete(r.rereads, key)
		return
	}
	if r.rereads == nil {
		r.rereads = map[types.NamespacedName]bool{}
	}
	r.rereads[key] = true
}

// reread says whether key's pass reads its conditions live.
func (r *Reconciler) reread(key types.NamespacedName) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rereads[key]
}

func endpoint(cp *lodiav1alpha1.ControlPlane) string {
	if cp.Status.Endpoint == nil {
		return ""
	}
	return "https://" + hostPort(cp.Status.Endpoint.APIServer) + contract.APIServerReadyzPath
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
