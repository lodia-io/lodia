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

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	admissionregistrationv1client "k8s.io/client-go/kubernetes/typed/admissionregistration/v1"
	batchv1client "k8s.io/client-go/kubernetes/typed/batch/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	apiregistrationv1client "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/typed/apiregistration/v1"
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
)

const (
	Name         = "health"
	FieldManager = "lodia/" + Name
	// FailuresBeforeFalse consecutive failed probes turn a True Reachable
	// False, so that one blip does not drop Ready.
	FailuresBeforeFalse = 3

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

// clients reach one control plane: the prober while its CA bundle and the
// tunnel's client pair stay the same, the tenant while Lodia's kubeconfig
// does.
type clients struct {
	bundle     []byte
	clientCert []byte
	clientKey  []byte
	prober     *prober
	kubeconfig []byte
	tenant     *tenant
	transport  *http.Client
}

// prober holds what a control plane's probes verify and present: the client
// of the public endpoint, the CAs of the bundle, and the tunnel's client
// certificate, in gateway mode.
type prober struct {
	public *http.Client
	roots  *x509.CertPool
	client *tls.Certificate
}

// tenant reads inside a control plane as Lodia, every client over the same
// transport.
type tenant struct {
	leases       coordinationv1client.LeaseInterface
	core         corev1client.CoreV1Interface
	admission    admissionregistrationv1client.AdmissionregistrationV1Interface
	registration apiregistrationv1client.ApiregistrationV1Interface
	batch        batchv1client.BatchV1Interface
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
// True after fewer than FailuresBeforeFalse failed probes in a row. It also
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
	prober := r.prober(key, secret.Data[lodiav1alpha1.CACertKey], pair)
	if prober == nil {
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "the CA bundle "+name+" holds no certificate"), false, nil
	}
	if gateway && prober.client == nil {
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "the tunnel's client certificate "+pair.Name+" holds no certificate and key"), false, nil
	}
	passed, answer := r.probes(ctx, cp, prober, url)
	if passed {
		return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, answer), false, nil
	}
	if r.failed(key)+1 < FailuresBeforeFalse {
		if err := live.Conditions(ctx, r.Reader, cp, lodiav1alpha1.ConditionReachable); err != nil {
			return nil, false, err
		}
		if previous := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReachable); previous != nil && previous.Status == metav1.ConditionTrue {
			return nil, true, nil
		}
	}
	return newCondition(cp, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed, answer), true, nil
}

// probes runs the control plane's probes and says whether all passed, and,
// for the condition's message, what they answered: what each path that
// failed answered, or what the three did. In gateway mode the three run at
// once, each under Timeout, so a dead Gateway holds a worker for one Timeout,
// not three.
func (r *Reconciler) probes(ctx context.Context, cp *lodiav1alpha1.ControlPlane, p *prober, url string) (bool, string) {
	if cp.Spec.Endpoint.Gateway == nil {
		code, _ := r.probe(ctx, p.public, url)
		return code == http.StatusOK, url + " " + answered(code)
	}
	e := cp.Status.Endpoint
	var (
		apiCode, tunnelCode        int
		apiErr, tunnelErr, konnErr error
		wg                         sync.WaitGroup
	)
	wg.Go(func() { apiCode, apiErr = r.probe(ctx, p.public, url) })
	wg.Go(func() { tunnelCode, tunnelErr = r.probe(ctx, r.throughTunnel(p, *e.Tunnel), url) })
	wg.Go(func() { konnErr = r.handshake(ctx, p, e.Konnectivity) })
	wg.Wait()
	log := logf.FromContext(ctx)
	var failed []string
	if apiCode != http.StatusOK {
		log.Error(apiErr, "the probe of the public endpoint failed", "url", url, "code", apiCode)
		failed = append(failed, "api: "+url+" "+answered(apiCode))
	}
	if tunnelCode != http.StatusOK {
		log.Error(tunnelErr, "the probe through the tunnel failed", "tunnel", hostPort(*e.Tunnel), "code", tunnelCode)
		failed = append(failed, "tunnel: "+url+" through "+hostPort(*e.Tunnel)+" "+answered(tunnelCode))
	}
	if konnErr != nil {
		log.Error(konnErr, "the TLS handshake with konnectivity failed", "address", hostPort(e.Konnectivity))
		failed = append(failed, "konnectivity: "+hostPort(e.Konnectivity)+" did not complete a TLS handshake")
	}
	if len(failed) > 0 {
		return false, strings.Join(failed, "; ")
	}
	return true, url + " answered 200 at the public endpoint and through the tunnel " + hostPort(*e.Tunnel) + ", and " + hostPort(e.Konnectivity) + " completed a TLS handshake"
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
	waiting := func(message string) (*metav1.Condition, error) {
		r.forget(key)
		return newCondition(cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, message), nil
	}
	if !access(cp) {
		return waiting("AccessReady is not True")
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
	tenant, err := r.tenant(key, secret.Data[lodiav1alpha1.KubeconfigKey])
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
	reason, messages := "", []string(nil)
	for _, l := range []struct{ name, reason string }{
		{lodiav1alpha1.ControllerManagerLease, lodiav1alpha1.ReasonControllerManagerNotLeading},
		{lodiav1alpha1.SchedulerLease, lodiav1alpha1.ReasonSchedulerNotLeading},
	} {
		lease, err := tenant.leases.Get(reading, l.name, metav1.GetOptions{})
		var message string
		switch {
		case apierrors.IsNotFound(err):
			message = "Lease " + lodiav1alpha1.LeaderElectionNamespace + "/" + l.name + " does not exist"
		case err != nil:
			logf.FromContext(ctx).Error(err, "read a leader Lease", "lease", l.name)
			return nil, live.Conditions(ctx, r.Reader, cp, lodiav1alpha1.ConditionControllersLeading)
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

// listLimit is the page of each list actedOn reads; the kinds select by no
// field it needs, so it filters on the client and counts the first page.
const listLimit = 500

// actedOn counts, kind by kind, what a held restore's controller manager acts
// on outside the cluster once it runs, naming the first three by name; a kind
// that cannot be listed says so in its place. The lists run at once, so a
// tenant that does not answer holds a worker for one Timeout, not seven.
func (r *Reconciler) actedOn(ctx context.Context, t *tenant) string {
	page := metav1.ListOptions{Limit: listLimit}
	type list struct {
		kind string
		read func(context.Context) (names []string, more bool, err error)
	}
	items := []struct {
		what  string
		lists []list
	}{
		{"LoadBalancer Services", []list{{"Services", func(ctx context.Context) ([]string, bool, error) {
			l, err := t.core.Services("").List(ctx, page)
			if err != nil {
				return nil, false, err
			}
			var names []string
			for _, s := range l.Items {
				if s.Spec.Type == corev1.ServiceTypeLoadBalancer {
					names = append(names, s.Namespace+"/"+s.Name)
				}
			}
			return names, l.Continue != "", nil
		}}}},
		{"PersistentVolumes with reclaimPolicy Delete", []list{{"PersistentVolumes", func(ctx context.Context) ([]string, bool, error) {
			l, err := t.core.PersistentVolumes().List(ctx, page)
			if err != nil {
				return nil, false, err
			}
			var names []string
			for _, v := range l.Items {
				if v.Spec.PersistentVolumeReclaimPolicy == corev1.PersistentVolumeReclaimDelete {
					names = append(names, v.Name)
				}
			}
			return names, l.Continue != "", nil
		}}}},
		{"pending PersistentVolumeClaims", []list{{"PersistentVolumeClaims", func(ctx context.Context) ([]string, bool, error) {
			l, err := t.core.PersistentVolumeClaims("").List(ctx, page)
			if err != nil {
				return nil, false, err
			}
			var names []string
			for _, c := range l.Items {
				if c.Status.Phase == corev1.ClaimPending {
					names = append(names, c.Namespace+"/"+c.Name)
				}
			}
			return names, l.Continue != "", nil
		}}}},
		{"webhooks that call a URL or fail closed", []list{
			{"ValidatingWebhookConfigurations", func(ctx context.Context) ([]string, bool, error) {
				l, err := t.admission.ValidatingWebhookConfigurations().List(ctx, page)
				if err != nil {
					return nil, false, err
				}
				var names []string
				for _, c := range l.Items {
					for _, w := range c.Webhooks {
						if actsOutside(w.ClientConfig, w.FailurePolicy) {
							names = append(names, c.Name+"/"+w.Name)
						}
					}
				}
				return names, l.Continue != "", nil
			}},
			{"MutatingWebhookConfigurations", func(ctx context.Context) ([]string, bool, error) {
				l, err := t.admission.MutatingWebhookConfigurations().List(ctx, page)
				if err != nil {
					return nil, false, err
				}
				var names []string
				for _, c := range l.Items {
					for _, w := range c.Webhooks {
						if actsOutside(w.ClientConfig, w.FailurePolicy) {
							names = append(names, c.Name+"/"+w.Name)
						}
					}
				}
				return names, l.Continue != "", nil
			}},
		}},
		{"APIServices served by a Service", []list{{"APIServices", func(ctx context.Context) ([]string, bool, error) {
			l, err := t.registration.APIServices().List(ctx, page)
			if err != nil {
				return nil, false, err
			}
			var names []string
			for _, a := range l.Items {
				if a.Spec.Service != nil {
					names = append(names, a.Name)
				}
			}
			return names, l.Continue != "", nil
		}}}},
		{"CronJobs not suspended", []list{{"CronJobs", func(ctx context.Context) ([]string, bool, error) {
			l, err := t.batch.CronJobs("").List(ctx, page)
			if err != nil {
				return nil, false, err
			}
			var names []string
			for _, j := range l.Items {
				if !ptr.Deref(j.Spec.Suspend, false) {
					names = append(names, j.Namespace+"/"+j.Name)
				}
			}
			return names, l.Continue != "", nil
		}}}},
	}
	type listed struct {
		names []string
		more  bool
		err   error
	}
	results := make([][]listed, len(items))
	var wg sync.WaitGroup
	for i, item := range items {
		results[i] = make([]listed, len(item.lists))
		for j, l := range item.lists {
			wg.Go(func() {
				listing, cancel := context.WithTimeout(ctx, r.Timeout)
				defer cancel()
				names, more, err := l.read(listing)
				results[i][j] = listed{names, more, err}
			})
		}
	}
	wg.Wait()
	var out []string
	for i, item := range items {
		var names, failed []string
		more := false
		for j, l := range item.lists {
			got := results[i][j]
			if got.err != nil {
				logf.FromContext(ctx).Error(got.err, "list in the tenant", "kind", l.kind)
				failed = append(failed, l.kind)
				continue
			}
			names, more = append(names, got.names...), more || got.more
		}
		if len(failed) > 0 {
			out = append(out, strings.Join(failed, " and ")+" could not be listed")
			continue
		}
		out = append(out, counted(item.what, names, more))
	}
	return strings.Join(out, "; ")
}

// actsOutside says whether a webhook calls a URL, which may be outside the
// cluster, or fails closed, Fail being admissionregistration/v1's default
// (https://github.com/kubernetes/api/blob/v0.37.1/admissionregistration/v1/types.go#L814-L817).
func actsOutside(config admissionregistrationv1.WebhookClientConfig, policy *admissionregistrationv1.FailurePolicyType) bool {
	return config.URL != nil || ptr.Deref(policy, admissionregistrationv1.Fail) == admissionregistrationv1.Fail
}

func counted(what string, names []string, more bool) string {
	n := strconv.Itoa(len(names))
	if more {
		n += " or more"
	}
	if len(names) == 0 {
		return n + " " + what
	}
	slices.Sort(names)
	shown := strings.Join(names[:min(3, len(names))], ", ")
	if len(names) > 3 || more {
		shown += "…"
	}
	return n + " " + what + " (" + shown + ")"
}

// stale says why lease does not lead, or nothing when it does.
func (r *Reconciler) stale(key types.NamespacedName, lease *coordinationv1.Lease, now time.Time) string {
	duration := time.Duration(ptr.Deref(lease.Spec.LeaseDurationSeconds, 0)) * time.Second
	if now.Sub(r.changed(key, lease, now)) <= duration+leaderSlack {
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
// certificate. Its client pair is pair's, parsed once, and nil outside gateway
// mode or when pair holds no certificate and key.
func (r *Reconciler) prober(key types.NamespacedName, bundle []byte, pair *corev1.Secret) *prober {
	var clientCert, clientKey []byte
	if pair != nil {
		clientCert, clientKey = pair.Data[lodiav1alpha1.TLSCertKey], pair.Data[lodiav1alpha1.TLSPrivateKeyKey]
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.clientsOf(key)
	if c.prober != nil && bytes.Equal(c.bundle, bundle) && bytes.Equal(c.clientCert, clientCert) && bytes.Equal(c.clientKey, clientKey) {
		return c.prober
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle) {
		return nil
	}
	public := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, DisableKeepAlives: true}
	p := &prober{public: &http.Client{Transport: public, CheckRedirect: noRedirects}, roots: roots}
	if pair != nil {
		public.DialContext, public.TLSHandshakeTimeout = (&net.Dialer{Timeout: r.Timeout}).DialContext, r.Timeout
		if parsed, err := tls.X509KeyPair(clientCert, clientKey); err == nil {
			p.client = &parsed
		}
	}
	c.bundle, c.clientCert, c.clientKey, c.prober = bundle, clientCert, clientKey, p
	return c.prober
}

func noRedirects(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// tenant reads inside the tenant as Lodia, through the in-namespace Service,
// which is never behind a proxy from the environment. The clients of a
// previous kubeconfig close their idle connections.
func (r *Reconciler) tenant(key types.NamespacedName, kubeconfig []byte) (*tenant, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.clientsOf(key)
	if c.tenant != nil && bytes.Equal(c.kubeconfig, kubeconfig) {
		return c.tenant, nil
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
	coordination, err := coordinationv1client.NewForConfigAndClient(cfg, transport)
	if err != nil {
		return nil, err
	}
	core, err := corev1client.NewForConfigAndClient(cfg, transport)
	if err != nil {
		return nil, err
	}
	admission, err := admissionregistrationv1client.NewForConfigAndClient(cfg, transport)
	if err != nil {
		return nil, err
	}
	registration, err := apiregistrationv1client.NewForConfigAndClient(cfg, transport)
	if err != nil {
		return nil, err
	}
	batch, err := batchv1client.NewForConfigAndClient(cfg, transport)
	if err != nil {
		return nil, err
	}
	if c.transport != nil {
		utilnet.CloseIdleConnectionsFor(c.transport.Transport)
	}
	c.kubeconfig, c.transport = kubeconfig, transport
	c.tenant = &tenant{
		leases:       coordination.Leases(lodiav1alpha1.LeaderElectionNamespace),
		core:         core,
		admission:    admission,
		registration: registration,
		batch:        batch,
	}
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
