// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"cmp"
	"context"
	"errors"
	"hash/fnv"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/jsonscan"
	"go.lodia.io/lodia/internal/live"
	"go.lodia.io/lodia/internal/statusapply"
)

const (
	NodesName         = "health-nodes"
	NodesFieldManager = "lodia/" + NodesName

	// nodesEvery is how old the last list of Nodes gets before the next: they
	// are large objects, and the set the hold counts changes slowly.
	nodesEvery = 30 * time.Second

	// leasePage and nodePage are the limits of the pages of the two lists; a Node
	// is the larger object.
	leasePage = 500
	nodePage  = 100

	// unknownGrace is how long after the controller manager marked a node
	// Unknown it still counts.
	unknownGrace = 5 * time.Minute

	// maxKubeletVersion is status.nodes.oldestKubelet's schema bound.
	maxKubeletVersion = 64

	maxName = 253

	// maxContinue is the longest continue token Health sends back to a tenant:
	// client-go's request escapes and copies it several times.
	maxContinue = 4096
)

// NodesReconciler samples a tenant's nodes, reports status.nodes, and holds the
// controller manager's leader Lease while most of them are silent. It has a
// queue of its own, and touches only the in-namespace Service, so a public
// endpoint that does not answer, which holds a worker of Reconciler for
// Timeout, delays no sample (docs/decisions/controller-manager-hold.md).
type NodesReconciler struct {
	client.Client
	// Reader reads Lodia's kubeconfig, whose data the cache does not hold, and
	// the status NodesReconciler keeps.
	Reader client.Reader
	// Events records the transitions of the conditions it writes;
	// SetupWithManager sets it.
	Events events.EventRecorder
	// Period is how often a control plane is sampled, Timeout how long its
	// lists wait for an answer.
	Period, Timeout time.Duration
	// MaxNodes is the most Nodes, and the most Leases of kube-node-lease, of a
	// tenant that Lodia counts: a list that has read more stops, and the
	// tenant is not counted. It is positive.
	MaxNodes int
	// SilentAfter is how long a node's Lease may stay unchanged before the node
	// is silent, HoldFraction the share of the counted nodes that are silent,
	// in thousandths, at which the controller manager is held, and ReleaseAfter
	// how long the nodes have to stay below it before the hold ends.
	SilentAfter  time.Duration
	HoldFraction int
	ReleaseAfter time.Duration
	// Clock tells when a Lease was seen to change, and how old the last list of
	// Nodes is.
	Clock clock.PassiveClock
	// SampleLag observes how much later than its due time each pass starts;
	// nil observes nothing.
	SampleLag prometheus.Histogram

	mu      sync.Mutex
	samples map[types.NamespacedName]*sampling
	clients map[types.NamespacedName]*tenantClients
	// counts is what each control plane's last pass counted, for Counts, which
	// reads it under mu, since a pass writes its sampling without.
	counts map[types.NamespacedName]nodeCount
}

// nodeCount is what a pass counted of the control plane with uid: the silent
// nodes and those that count, as status.health has them.
type nodeCount struct {
	uid             types.UID
	silent, counted int
}

// sampling is what a pass remembers of one control plane; only that control
// plane's pass touches it. Its leases and nodes hold at most MaxNodes entries
// each, and each string in them is bounded here, since the tenant's
// administrator can have any server answer the lists
// (docs/review-checklist.md, item 10): leaseVersions and summarize keep no
// object whose name is over maxName; a Lease's resourceVersion is kept as a
// digest of 8 bytes; summarize keeps no kubelet version over maxKubeletVersion
// and no Ready status but the three a condition has.
type sampling struct {
	uid    types.UID
	leases map[string]renewal
	nodes  []node
	// nodesStop and leasesStop say why the last list of the Nodes, and the last
	// list of the node Leases that did not fail, ended before its end; they are
	// empty when it did not.
	nodesStop, leasesStop string
	listedAt              time.Time
	// restarted is when the clock of the node Leases last started.
	restarted time.Time
	// below is when a held pass the clock trusted first read the nodes below
	// the hold fraction, zero while the count towards the release is clear.
	below time.Time
	// leader is the version of the controller manager's Lease the pass saw
	// last, and when it saw that version first.
	leader renewal
	// due is when the next pass is due, the end of the last one plus the
	// requeue it returned; zero before the first pass and after one that could
	// not read the ControlPlane.
	due time.Time
	// count is what this pass counted, once the clock of the node Leases has
	// run SilentAfter and while the hold is on, else nil.
	count *nodeCount
}

// restart forgets the clock of the node Leases and the count towards the
// release: no list of the Leases stands behind this pass.
func (s *sampling) restart() {
	s.leases, s.below = nil, time.Time{}
}

// stopped says why a list of this control plane ended before its end: the
// tenant is then not counted.
func (s *sampling) stopped() string {
	return cmp.Or(s.leasesStop, s.nodesStop)
}

// observe starts the clock, when it was forgotten, and notes the Leases listed.
func (s *sampling) observe(leases []leaseVersion, now time.Time) {
	if s.leases == nil {
		s.restarted = now
	}
	s.leases = seen(s.leases, leases, now)
}

type node struct {
	name    string
	ready   corev1.ConditionStatus
	since   time.Time
	kubelet string
}

func (r *NodesReconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	r.Events = mgr.GetEventRecorder("lodia.io/" + NodesName)
	return ctrl.NewControllerManagedBy(mgr).
		Named(NodesName).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.ControlPlane{}, builder.WithPredicates(passInputs())).
		Complete(r)
}

func passInputs() predicate.Predicate {
	return predicate.Or[client.Object](predicate.GenerationChangedPredicate{}, accessChanged())
}

func accessChanged() predicate.Funcs {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		before, ok := e.ObjectOld.(*lodiav1alpha1.ControlPlane)
		after, ok2 := e.ObjectNew.(*lodiav1alpha1.ControlPlane)
		return !ok || !ok2 || access(before) != access(after)
	}}
}

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=pods,verbs=list
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *NodesReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	now := r.Clock.Now()
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		if apierrors.IsNotFound(err) {
			r.forget(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		logf.FromContext(ctx).Error(err, "read the ControlPlane")
		r.unread(req.NamespacedName)
		result, _ := r.again(now)
		return result, nil
	}
	if !cp.DeletionTimestamp.IsZero() || cp.Spec.Paused {
		r.forget(req.NamespacedName)
		return ctrl.Result{}, nil
	}
	s := r.sampling(req.NamespacedName, cp.UID)
	if !s.due.IsZero() && r.SampleLag != nil {
		r.SampleLag.Observe(max(now.Sub(s.due), 0).Seconds())
	}
	s.count = nil
	if err := r.sample(ctx, cp, s, now); err != nil {
		logf.FromContext(ctx).Error(err, "sample the tenant's nodes")
	}
	r.publish(req.NamespacedName, s)
	result, due := r.again(now)
	s.due = due
	return result, nil
}

// again requeues a Period after now, or in a second when the pass took longer,
// and returns when the next pass is due: controller-runtime adds the request
// once Reconcile returns. It returns no error on any path: controller-runtime
// does not requeue a non-positive RequeueAfter, and an error goes to a backoff
// that reaches 1,000 s (https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/internal/controller/controller.go#L498-L516).
func (r *NodesReconciler) again(now time.Time) (ctrl.Result, time.Time) {
	end := r.Clock.Now()
	after := max(r.Period-end.Sub(now), time.Second)
	return ctrl.Result{RequeueAfter: after}, end.Add(after)
}

// sample takes one pass over cp; an error only ends it, to be logged.
func (r *NodesReconciler) sample(ctx context.Context, cp *lodiav1alpha1.ControlPlane, s *sampling, now time.Time) error {
	key := client.ObjectKeyFromObject(cp)
	if meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionControllerManagerHeld) == nil {
		if err := live.Nodes(ctx, r.Reader, cp, lodiav1alpha1.ConditionControllerManagerHeld); err != nil {
			return err
		}
		if meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionControllerManagerHeld) == nil {
			if err := r.apply(ctx, cp, cp.Status.Nodes, cp.Status.Health, previous(cp, lodiav1alpha1.ConditionControllerManagerHeld)); err != nil {
				return err
			}
		}
	}
	kubeconfig, waiting, err := lodiaKubeconfig(ctx, r.Reader, cp)
	if err != nil {
		// No list stands behind this pass, so it clears the count; a failed read
		// of a management Secret says nothing about the kubelets, so the clock runs on.
		s.below = time.Time{}
		return err
	}
	if waiting != "" {
		s.restart()
		return r.report(ctx, cp, s.status(cp), nil, *held(cp, lodiav1alpha1.ReasonWaitingForAccess, waiting))
	}
	tenant, err := r.tenant(key, kubeconfig)
	if err != nil {
		s.restart()
		return err
	}
	return r.holdOrRelease(ctx, cp, tenant, s, now)
}

// list reads the node Leases and, when the last list of Nodes is old, the
// Nodes, at once under Timeout. A failed list of Nodes leaves the last good
// one.
func (r *NodesReconciler) list(ctx context.Context, tenant *tenant, s *sampling, now time.Time) ([]leaseVersion, error) {
	listing, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	var (
		leases    []leaseVersion
		leaseStop string
		leaseErr  error
		nodes     []node
		nodeStop  string
		nodeErr   error
		wg        sync.WaitGroup
	)
	listNodes := s.listedAt.IsZero() || now.Sub(s.listedAt) >= nodesEvery
	wg.Go(func() { leases, leaseStop, leaseErr = r.listLeases(listing, tenant) })
	if listNodes {
		wg.Go(func() { nodes, nodeStop, nodeErr = r.listNodes(listing, tenant) })
	}
	wg.Wait()
	if listNodes {
		if nodeErr != nil {
			logf.FromContext(ctx).Error(nodeErr, "list the tenant's Nodes")
		} else {
			s.nodes, s.nodesStop, s.listedAt = nodes, nodeStop, now
		}
	}
	if leaseErr != nil {
		return nil, leaseErr
	}
	s.leasesStop = leaseStop
	return leases, nil
}

func (r *NodesReconciler) listLeases(ctx context.Context, tenant *tenant) ([]leaseVersion, string, error) {
	leases, end, err := listPages(ctx, leasePage, r.MaxNodes, func(ctx context.Context, options metav1.ListOptions) ([]leaseVersion, string, listEnd, error) {
		page, err := tenant.leasePage(ctx, options)
		if err != nil {
			return nil, "", listEnded, err
		}
		end := listEnded
		if page.Items.over {
			end = pageTooLong
		}
		return leaseVersions(page.Items.items), page.Metadata.Continue, end, nil
	})
	stop, err := r.stop("Leases of "+corev1.NamespaceNodeLease, leasePage, end, err)
	return leases, stop, err
}

func (r *NodesReconciler) listNodes(ctx context.Context, tenant *tenant) ([]node, string, error) {
	nodes, end, err := listPages(ctx, nodePage, r.MaxNodes, func(ctx context.Context, options metav1.ListOptions) ([]node, string, listEnd, error) {
		page, err := tenant.nodePage(ctx, options)
		if err != nil {
			return nil, "", listEnded, err
		}
		nodes, end := summarize(page.Items.items)
		if end == listEnded && page.Items.over {
			end = pageTooLong
		}
		return nodes, page.Metadata.Continue, end, nil
	})
	stop, err := r.stop("Nodes", nodePage, end, err)
	return nodes, stop, err
}

type listEnd int

const (
	listEnded listEnd = iota
	tooManyItems
	tooManyPages
	pageTooLong
	nodeTooLarge
)

// maxPages is the most pages a list of most items reads: the pages that most
// items fill, and one more, since a tenant's API server fills every page, so by
// then the list has ended or holds more than most items.
func maxPages(size int64, most int) int {
	return (most+int(size)-1)/int(size) + 1
}

// listPages reads a list a page at a time and keeps its first most items. It
// asks for no page after one that left it with more than most items, nor after
// maxPages(size, most) pages, whatever they hold: the administrator of a tenant
// can serve the list from a server whose pages hold no item. The page function
// reads no token longer than maxContinue. It reduces a decoded page to
// what the pass keeps, so that no list holds more than one decoded page, and
// says whether the page ended the list early: it held more than size items, or
// an object of it was too large. It does not use pager.ListPager, whose
// EachListItem fetches the next page while it hands over the last, even
// unbuffered. Every page is a consistent read: the list sets no resourceVersion.
// A failure returns the items of the pages before it.
func listPages[T any](ctx context.Context, size int64, most int, page func(context.Context, metav1.ListOptions) ([]T, string, listEnd, error)) (kept []T, end listEnd, err error) {
	options := metav1.ListOptions{Limit: size}
	for read := 1; ; read++ {
		items, next, end, err := page(ctx, options)
		if err != nil {
			return kept, listEnded, err
		}
		kept = append(kept, items...)
		switch {
		case len(kept) > most:
			return kept[:most], tooManyItems, nil
		case end != listEnded:
			return kept, end, nil
		case next == "":
			return kept, listEnded, nil
		case read == maxPages(size, most):
			return kept, tooManyPages, nil
		}
		options.Continue = next
	}
}

// stop says why the list of what ended before its end, or nothing when it did
// not: it saw more than MaxNodes items or a page after maxPages, a page of more
// than size items or a Node of more than maxConditions conditions, a field
// longer than its bound, or a response of it was longer than maxResponseBytes.
// Another failure of the list is returned.
func (r *NodesReconciler) stop(what string, size int64, end listEnd, err error) (string, error) {
	var tooLong *jsonscan.TooLong
	switch {
	case errors.Is(err, errResponseTooLong):
		return "a response listing the " + what + " is longer than " + strconv.Itoa(maxResponseBytes) + " bytes", nil
	case errors.As(err, &tooLong):
		return tooLong.Field + " in the " + what + " is longer than " + strconv.Itoa(tooLong.Most) + " bytes", nil
	case err != nil:
		return "", err
	case end == tooManyItems:
		return "more than " + strconv.Itoa(r.MaxNodes) + " " + what + " (health.maxNodes)", nil
	case end == tooManyPages:
		return "more than " + strconv.Itoa(maxPages(size, r.MaxNodes)) + " pages of " + what + " (health.maxNodes)", nil
	case end == pageTooLong:
		return "a page of the " + what + " holds more than " + strconv.FormatInt(size, 10) + " items", nil
	case end == nodeTooLarge:
		return "a Node has more than " + strconv.Itoa(maxConditions) + " conditions", nil
	}
	return "", nil
}

type leaseVersion struct {
	name    string
	version uint64
}

// versionOf digests a resourceVersion: the clock only asks whether it changed,
// and a digest asks no format of it and takes 8 bytes.
func versionOf(resourceVersion string) uint64 {
	h := fnv.New64a()
	_, _ = io.WriteString(h, resourceVersion)
	return h.Sum64()
}

func leaseVersions(leases []leaseItem) []leaseVersion {
	out := make([]leaseVersion, 0, len(leases))
	for _, l := range leases {
		if len(l.Metadata.Name) <= maxName {
			out = append(out, leaseVersion{l.Metadata.Name, versionOf(l.Metadata.ResourceVersion)})
		}
	}
	return out
}

// seen keeps, for every Lease listed, when its version was first seen: now for
// a Lease new to the list or changed since the last one.
func seen(previous map[string]renewal, leases []leaseVersion, now time.Time) map[string]renewal {
	next := make(map[string]renewal, len(leases))
	for _, l := range leases {
		if p, ok := previous[l.name]; ok && p.version == l.version {
			next[l.name] = p
		} else {
			next[l.name] = renewal{version: l.version, at: now}
		}
	}
	return next
}

// summarize keeps the Nodes before the first that has more than maxConditions
// conditions, and says by nodeTooLarge that it stopped there.
func summarize(nodes []nodeObject) ([]node, listEnd) {
	out := make([]node, 0, len(nodes))
	for _, n := range nodes {
		if n.Status.Conditions.over {
			return out, nodeTooLarge
		}
		if len(n.Metadata.Name) > maxName {
			continue
		}
		summary := node{name: n.Metadata.Name}
		if len(n.Status.NodeInfo.KubeletVersion) <= maxKubeletVersion {
			summary.kubelet = n.Status.NodeInfo.KubeletVersion
		}
		for _, c := range n.Status.Conditions.items {
			if c.Type == corev1.NodeReady {
				summary.ready, summary.since = conditionStatus(c.Status), c.LastTransitionTime.Time
			}
		}
		out = append(out, summary)
	}
	return out, listEnded
}

// conditionStatus is status when it is one of the three a condition has, else
// none: any other counts as a node that is not Ready.
func conditionStatus(status corev1.ConditionStatus) corev1.ConditionStatus {
	switch status {
	case corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionUnknown:
		return status
	}
	return ""
}

// counts says whether a node's silence counts: its kubelet reports it Ready,
// or the controller manager marked it Unknown less than unknownGrace before
// began. A node its kubelet reports NotReady does not.
func (n node) counts(began time.Time) bool {
	return n.ready == corev1.ConditionTrue || n.ready == corev1.ConditionUnknown && began.Sub(n.since) < unknownGrace
}

// silence returns how many of the nodes that count have a Lease, and how many
// of those have it unchanged for SilentAfter; began is when the hold began, or
// now without one.
func (r *NodesReconciler) silence(s *sampling, now, began time.Time) (silent, counted int) {
	for _, n := range s.nodes {
		lease, ok := s.leases[n.name]
		if !ok || !n.counts(began) {
			continue
		}
		counted++
		if now.Sub(lease.at) >= r.SilentAfter {
			silent++
		}
	}
	return silent, counted
}

func held(cp *lodiav1alpha1.ControlPlane, reason, message string) *metav1.Condition {
	return newCondition(cp, lodiav1alpha1.ConditionControllerManagerHeld, metav1.ConditionFalse, reason, message)
}

func (s *sampling) status(cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.NodesStatus {
	if s.listedAt.IsZero() {
		return nil
	}
	running := ""
	if cp.Status.Kubernetes != nil {
		running = cp.Status.Kubernetes.Version
	}
	return nodesStatus(s.nodes, running, s.nodesStop != "")
}

// nodesStatus counts the nodes; a kubelet is outdated when its minor is below
// the minor of running, which counts none while it is absent or unparsable. A
// truncated list counts the nodes it read and tells nothing of the kubelets.
func nodesStatus(nodes []node, running string, truncated bool) *lodiav1alpha1.NodesStatus {
	status := &lodiav1alpha1.NodesStatus{Count: lodiav1alpha1.Count{Total: int32(len(nodes))}, Truncated: truncated}
	floor, _ := version.ParseSemantic(running)
	var oldest *version.Version
	for _, n := range nodes {
		if n.ready == corev1.ConditionTrue {
			status.Ready++
		}
		if truncated {
			continue
		}
		kubelet := kubeletVersion(n.kubelet)
		if kubelet == nil {
			continue
		}
		if oldest == nil || kubelet.LessThan(oldest) {
			oldest, status.OldestKubelet = kubelet, n.kubelet
		}
		if floor != nil && (kubelet.Major() < floor.Major() || kubelet.Major() == floor.Major() && kubelet.Minor() < floor.Minor()) {
			status.OutdatedKubelets++
		}
	}
	return status
}

func kubeletVersion(reported string) *version.Version {
	v, err := version.ParseSemantic(reported)
	if err != nil {
		return nil
	}
	return v
}

// report applies status.nodes and ControllerManagerHeld when either differs
// from the cache's, after a live read: the apply names the object by name, and
// a cache still holding a deleted predecessor must not leave its list on the
// successor, which carries it forward. An apply that leaves a field out
// removes it, so without a list of Nodes of this run it carries the status
// the API server holds.
func (r *NodesReconciler) report(ctx context.Context, cp *lodiav1alpha1.ControlPlane, nodes *lodiav1alpha1.NodesStatus, health *lodiav1alpha1.HealthStatus, c metav1.Condition) error {
	if !changes(cp, nodes, health, c) {
		return nil
	}
	if err := live.Nodes(ctx, r.Reader, cp, lodiav1alpha1.ConditionControllerManagerHeld); err != nil {
		return err
	}
	if nodes == nil {
		nodes = cp.Status.Nodes
	}
	if !changes(cp, nodes, health, c) {
		return nil
	}
	return r.apply(ctx, cp, nodes, health, c)
}

func changes(cp *lodiav1alpha1.ControlPlane, nodes *lodiav1alpha1.NodesStatus, health *lodiav1alpha1.HealthStatus, c metav1.Condition) bool {
	return statusapply.Differs(meta.FindStatusCondition(cp.Status.Conditions, c.Type), c) || nodes != nil && !equality.Semantic.DeepEqual(nodes, cp.Status.Nodes) || !equality.Semantic.DeepEqual(health, cp.Status.Health)
}

// apply writes every field each time, since an apply that leaves one out
// removes it; it leaves cp with what it wrote.
func (r *NodesReconciler) apply(ctx context.Context, cp *lodiav1alpha1.ControlPlane, nodes *lodiav1alpha1.NodesStatus, health *lodiav1alpha1.HealthStatus, c metav1.Condition) error {
	if err := (statusapply.Writer{Client: r.Client, Reader: r.Reader, Manager: NodesFieldManager, Events: r.Events}).ApplyInto(ctx, cp, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{c}, Nodes: nodes, Health: health}, true); err != nil {
		return err
	}
	cp.Status.Nodes, cp.Status.Health = nodes, health
	return nil
}

// sampling returns what is remembered of the control plane, or nothing of a
// predecessor under the same name.
func (r *NodesReconciler) sampling(key types.NamespacedName, uid types.UID) *sampling {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.samples == nil {
		r.samples = map[types.NamespacedName]*sampling{}
	}
	if s := r.samples[key]; s != nil && s.uid == uid {
		return s
	}
	r.samples[key] = &sampling{uid: uid}
	return r.samples[key]
}

func (r *NodesReconciler) tenant(key types.NamespacedName, kubeconfig []byte) (*tenant, error) {
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

// publish makes the pass's count the one Counts returns, or leaves none.
func (r *NodesReconciler) publish(key types.NamespacedName, s *sampling) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s.count == nil {
		delete(r.counts, key)
		return
	}
	if r.counts == nil {
		r.counts = map[types.NamespacedName]nodeCount{}
	}
	r.counts[key] = *s.count
}

// unread forgets the due time and the count of the control plane under key,
// whichever its UID: a pass that cannot read it has neither.
func (r *NodesReconciler) unread(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s := r.samples[key]; s != nil {
		s.due = time.Time{}
	}
	delete(r.counts, key)
}

// Counts returns the silent nodes and those that count of the control plane
// with key and uid, as its last pass counted them: nothing when that pass had
// no list of the node Leases or found the hold off, before the first list of
// the Nodes, or before their clock has run SilentAfter, when no node can be
// silent yet.
func (r *NodesReconciler) Counts(key types.NamespacedName, uid types.UID) (silent, counted int, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.counts[key]
	if !ok || c.uid != uid {
		return 0, 0, false
	}
	return c.silent, c.counted, true
}

func (r *NodesReconciler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.samples, key)
	delete(r.counts, key)
	if c := r.clients[key]; c != nil && c.transport != nil {
		utilnet.CloseIdleConnectionsFor(c.transport.Transport)
	}
	delete(r.clients, key)
}
