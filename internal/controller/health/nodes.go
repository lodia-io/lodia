// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/live"
	"go.lodia.io/lodia/internal/statusapply"
)

const (
	NodesName         = "health-nodes"
	NodesFieldManager = "lodia/" + NodesName

	// nodesEvery is how old the last list of Nodes gets before the next: they
	// are large objects, and the set the hold counts changes slowly.
	nodesEvery = 30 * time.Second

	// unknownGrace is how long after the controller manager marked a node
	// Unknown it still counts.
	unknownGrace = 5 * time.Minute

	// maxKubeletVersion is status.nodes.oldestKubelet's schema bound.
	maxKubeletVersion = 64
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
	// Period is how often a control plane is sampled, Timeout how long its
	// lists wait for an answer.
	Period, Timeout time.Duration
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

	mu      sync.Mutex
	samples map[types.NamespacedName]*sampling
	clients map[types.NamespacedName]*tenantClients
}

// sampling is what a pass remembers of one control plane; only that control
// plane's pass touches it.
type sampling struct {
	uid      types.UID
	leases   map[string]renewal
	nodes    []node
	listedAt time.Time
	// restarted is when the clock of the node Leases last started.
	restarted time.Time
	// below is when a held pass the clock trusted first read the nodes below
	// the hold fraction, zero while the count towards the release is clear.
	below time.Time
	// leader is the version of the controller manager's Lease the pass saw
	// last, and when it saw that version first.
	leader renewal
}

// restart forgets the clock of the node Leases and the count towards the
// release: no list of the Leases stands behind this pass.
func (s *sampling) restart() {
	s.leases, s.below = nil, time.Time{}
}

// observe starts the clock, when it was forgotten, and notes the Leases listed.
func (s *sampling) observe(leases []coordinationv1.Lease, now time.Time) {
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

func (r *NodesReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	now := r.Clock.Now()
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		if apierrors.IsNotFound(err) {
			r.forget(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		logf.FromContext(ctx).Error(err, "read the ControlPlane")
		return r.again(now), nil
	}
	if !cp.DeletionTimestamp.IsZero() || cp.Spec.Paused {
		r.forget(req.NamespacedName)
		return ctrl.Result{}, nil
	}
	if err := r.sample(ctx, cp, now); err != nil {
		logf.FromContext(ctx).Error(err, "sample the tenant's nodes")
	}
	return r.again(now), nil
}

// again requeues a Period after now, or in a second when the pass took longer.
// It returns no error on any path: controller-runtime does not requeue a
// non-positive RequeueAfter, and an error goes to a backoff that reaches 1,000
// s (https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/internal/controller/controller.go#L498-L516).
func (r *NodesReconciler) again(now time.Time) ctrl.Result {
	return ctrl.Result{RequeueAfter: max(r.Period-r.Clock.Since(now), time.Second)}
}

// sample takes one pass over cp; an error only ends it, to be logged.
func (r *NodesReconciler) sample(ctx context.Context, cp *lodiav1alpha1.ControlPlane, now time.Time) error {
	key := client.ObjectKeyFromObject(cp)
	s := r.sampling(key, cp.UID)
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
func (r *NodesReconciler) list(ctx context.Context, tenant *tenant, s *sampling, now time.Time) ([]coordinationv1.Lease, error) {
	listing, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	var (
		leases   *coordinationv1.LeaseList
		leaseErr error
		nodes    *corev1.NodeList
		nodeErr  error
		wg       sync.WaitGroup
	)
	listNodes := s.listedAt.IsZero() || now.Sub(s.listedAt) >= nodesEvery
	wg.Go(func() { leases, leaseErr = tenant.nodeLeases.List(listing, metav1.ListOptions{}) })
	if listNodes {
		wg.Go(func() { nodes, nodeErr = tenant.core.Nodes().List(listing, metav1.ListOptions{}) })
	}
	wg.Wait()
	if listNodes {
		if nodeErr != nil {
			logf.FromContext(ctx).Error(nodeErr, "list the tenant's Nodes")
		} else {
			s.nodes, s.listedAt = summarize(nodes.Items), now
		}
	}
	if leaseErr != nil {
		return nil, leaseErr
	}
	return leases.Items, nil
}

// seen keeps, for every Lease listed, when its version was first seen: now for
// a Lease new to the list or changed since the last one.
func seen(previous map[string]renewal, leases []coordinationv1.Lease, now time.Time) map[string]renewal {
	next := make(map[string]renewal, len(leases))
	for _, l := range leases {
		if p, ok := previous[l.Name]; ok && p.resourceVersion == l.ResourceVersion {
			next[l.Name] = p
		} else {
			next[l.Name] = renewal{resourceVersion: l.ResourceVersion, at: now}
		}
	}
	return next
}

func summarize(nodes []corev1.Node) []node {
	out := make([]node, 0, len(nodes))
	for _, n := range nodes {
		summary := node{name: n.Name, kubelet: n.Status.NodeInfo.KubeletVersion}
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeReady {
				summary.ready, summary.since = c.Status, c.LastTransitionTime.Time
			}
		}
		out = append(out, summary)
	}
	return out
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
	return nodesStatus(s.nodes, running)
}

// nodesStatus counts the nodes; a kubelet is outdated when its minor is below
// the minor of running, which counts none while it is absent or unparsable.
func nodesStatus(nodes []node, running string) *lodiav1alpha1.NodesStatus {
	status := &lodiav1alpha1.NodesStatus{Count: lodiav1alpha1.Count{Total: int32(len(nodes))}}
	floor, _ := version.ParseSemantic(running)
	var oldest *version.Version
	for _, n := range nodes {
		if n.ready == corev1.ConditionTrue {
			status.Ready++
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
	if len(reported) > maxKubeletVersion {
		return nil
	}
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
	if err := statusapply.Apply(ctx, r.Client, r.Reader, NodesFieldManager, cp, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{c}, Nodes: nodes, Health: health}, true); err != nil {
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

func (r *NodesReconciler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.samples, key)
	if c := r.clients[key]; c != nil && c.transport != nil {
		utilnet.CloseIdleConnectionsFor(c.transport.Transport)
	}
	delete(r.clients, key)
}
