// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package lifecycle is the Lifecycle controller.
package lifecycle

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/statusapply"
	"go.lodia.io/lodia/internal/versions"
)

const (
	Name         = "lifecycle"
	FieldManager = "lodia/" + Name
)

// readiness is what Ready is made of, in the order its reason is taken from.
// In gateway mode RoutesReady comes first: what stops the routes causes what
// the others report then, a wait for the endpoint or a failed probe.
func readiness(cp *lodiav1alpha1.ControlPlane) []string {
	conditions := []string{
		lodiav1alpha1.ConditionCertificatesReady,
		lodiav1alpha1.ConditionDatastoreReady,
		lodiav1alpha1.ConditionAPIServerReady,
		lodiav1alpha1.ConditionAccessReady,
		lodiav1alpha1.ConditionAddonsApplied,
		lodiav1alpha1.ConditionReachable,
		lodiav1alpha1.ConditionControllersLeading,
	}
	if cp.Spec.Endpoint.Gateway != nil {
		return append([]string{lodiav1alpha1.ConditionRoutesReady}, conditions...)
	}
	return conditions
}

type Reconciler struct {
	client.Client
	// Reader reads the ControlPlane as the API server holds it, before a write,
	// and the fleet before a slot is taken.
	Reader client.Reader
	Clock  clock.PassiveClock
	// Revision is the running operator revision, which Lifecycle grants by the
	// rules of docs/decisions/upgrades-and-maintenance-windows.md.
	Revision string
	// MaxConcurrentRollouts is how many tenants hold a slot of the fleet.
	MaxConcurrentRollouts int
	// Settling is how long a change Lodia started keeps its slot once it has
	// rolled out.
	Settling time.Duration

	once  sync.Once
	slots *semaphore.Weighted
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.ControlPlane{}).
		Watches(&lodiav1alpha1.Backup{}, handler.EnqueueRequestsFromMapFunc(backupsOwner)).
		Watches(&lodiav1alpha1.DatastoreClass{}, handler.EnqueueRequestsFromMapFunc(r.controlPlanesOn)).
		Complete(r)
}

func backupsOwner(_ context.Context, obj client.Object) []reconcile.Request {
	backup := obj.(*lodiav1alpha1.Backup)
	return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.ControlPlaneRef.Name}}}
}

// controlPlanesOn maps a DatastoreClass to the ControlPlanes whose Etcd runs
// it, as status.datastore.class names it: an edit of it is pending for them
// alone.
func (r *Reconciler) controlPlanesOn(ctx context.Context, class client.Object) []reconcile.Request {
	list := &lodiav1alpha1.ControlPlaneList{}
	if err := r.List(ctx, list); err != nil {
		logf.FromContext(ctx).Error(err, "list the ControlPlanes of a DatastoreClass", "class", class.GetName())
		return nil
	}
	var requests []reconcile.Request
	for i := range list.Items {
		if running := runningClass(&list.Items[i]); running != nil && running.Name == class.GetName() {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
		}
	}
	return requests
}

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch
// +kubebuilder:rbac:groups=lodia.io,resources=backups,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=lodia.io,resources=datastoreclasses,verbs=list;watch

// Reconcile decides from the cached object and, when that decision changes
// something, from the object as the API server holds it, comparing the write
// with that copy: a pass on a cache behind Lifecycle's own write would undo it
// (docs/decisions/independent-controllers.md). A decision that takes a slot
// of the fleet, counted from the cache, is made again under a semaphore from
// the fleet the API server holds, and written under it, so that two passes
// never take one slot.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cached := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cached); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	gathered, err := r.gather(ctx, cached, r.Client)
	if err != nil {
		return ctrl.Result{}, err
	}
	if d := decide(gathered); d.createBackup == "" && !d.differs(cached) {
		return ctrl.Result{RequeueAfter: d.requeue}, nil
	}
	current, err := statusapply.Live(ctx, r.Reader, cached)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gathered, err = r.gather(ctx, current, r.Client); err != nil {
		return ctrl.Result{}, err
	}
	d := decide(gathered)
	if d.takesSlot {
		slots := r.semaphore()
		if err := slots.Acquire(ctx, 1); err != nil {
			return ctrl.Result{}, err
		}
		defer slots.Release(1)
		if gathered, err = r.gather(ctx, current, r.Reader); err != nil {
			return ctrl.Result{}, err
		}
		d = decide(gathered)
	}
	if err := statusapply.Apply(ctx, r.Client, r.Reader, FieldManager, current, d.status, d.fieldsDiffer(current)); err != nil {
		return ctrl.Result{}, err
	}
	if d.createBackup != "" {
		err := client.IgnoreAlreadyExists(r.Create(ctx, &lodiav1alpha1.Backup{
			ObjectMeta: metav1.ObjectMeta{Name: d.createBackup, Namespace: current.Namespace},
			Spec:       lodiav1alpha1.BackupSpec{ControlPlaneRef: lodiav1alpha1.LocalReference{Name: current.Name}},
		}))
		if err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: d.requeue}, nil
}

func (r *Reconciler) semaphore() *semaphore.Weighted {
	r.once.Do(func() { r.slots = semaphore.NewWeighted(1) })
	return r.slots
}

// gather reads what a pass decides from: the Backup of the minor step and the
// DatastoreClass the Etcd runs from the cache, and, while a change is
// pending, the fleet through fleet.
func (r *Reconciler) gather(ctx context.Context, cp *lodiav1alpha1.ControlPlane, fleet client.Reader) (facts, error) {
	f := facts{cp: cp, now: r.Clock.Now(), revision: r.Revision, limit: r.MaxConcurrentRollouts, settling: r.Settling}
	if minor := backupMinor(cp); minor != "" {
		backup := &lodiav1alpha1.Backup{}
		switch err := r.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: lodiav1alpha1.PreUpgradeBackupName(cp.Name, minor, cp.UID)}, backup); {
		case err == nil:
			f.backup = backup
		case !apierrors.IsNotFound(err):
			return f, err
		}
	}
	if running := runningClass(cp); running != nil {
		class := &lodiav1alpha1.DatastoreClass{}
		switch err := r.Get(ctx, client.ObjectKey{Name: running.Name}, class); {
		case err == nil:
			f.class = lodiav1alpha1.DatastoreClassGeneration{UID: class.UID, Generation: class.Generation}
		case !apierrors.IsNotFound(err):
			return f, err
		}
	}
	if cp.DeletionTimestamp.IsZero() && !cp.Spec.Paused && f.pending(f.versionChange()).any() {
		list := &lodiav1alpha1.ControlPlaneList{}
		if err := fleet.List(ctx, list, client.UnsafeDisableDeepCopy); err != nil {
			return f, err
		}
		f.fleet = fleetOf(list.Items, cp, f.now)
	}
	return f, nil
}

// oldestReported is the oldest version the three components and the addons
// report, and empty until all four do.
func oldestReported(cp *lodiav1alpha1.ControlPlane) string {
	var reported []string
	if c := cp.Status.Components; c != nil {
		for _, component := range []*lodiav1alpha1.ComponentStatus{c.APIServer, c.ControllerManager, c.Scheduler} {
			if component != nil {
				reported = append(reported, component.Version)
			}
		}
	}
	if a := cp.Status.Addons; a != nil {
		reported = append(reported, a.Version)
	}
	if len(reported) < 4 {
		return ""
	}
	oldest, _ := versions.Oldest(reported...)
	return oldest
}

// rollingOut names what status.components and status.datastore.members count
// as not updated; a count of updated not reported yet is a spec its writer's
// controller has not observed, or a Deployment that is gone.
func rollingOut(cp *lodiav1alpha1.ControlPlane) []string {
	type rollout struct {
		name, unit string
		count      lodiav1alpha1.RolloutCount
	}
	var counts []rollout
	if c := cp.Status.Components; c != nil {
		for _, component := range []struct {
			name   string
			status *lodiav1alpha1.ComponentStatus
		}{{"kube-apiserver", c.APIServer}, {"kube-controller-manager", c.ControllerManager}, {"kube-scheduler", c.Scheduler}} {
			if component.status != nil {
				counts = append(counts, rollout{component.name, "replicas", component.status.RolloutCount})
			}
		}
	}
	if d := cp.Status.Datastore; d != nil {
		counts = append(counts, rollout{"etcd", "members", d.Members})
	}
	var rolling []string
	for _, c := range counts {
		switch {
		case c.count.Updated == nil:
			rolling = append(rolling, fmt.Sprintf("%s: its updated %s are not reported yet", c.name, c.unit))
		case *c.count.Updated < c.count.Total:
			rolling = append(rolling, fmt.Sprintf("%s: %d of %d %s updated", c.name, *c.count.Updated, c.count.Total, c.unit))
		}
	}
	return rolling
}

func lagging(cp *lodiav1alpha1.ControlPlane) []string {
	var lag []string
	for _, conditionType := range readiness(cp) {
		if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c != nil && c.ObservedGeneration < cp.Generation {
			lag = append(lag, fmt.Sprintf("%s at %d", conditionType, c.ObservedGeneration))
		}
	}
	return lag
}

func ready(cp *lodiav1alpha1.ControlPlane) *metav1.Condition {
	return readyOf(cp, readiness(cp))
}

func readyOf(cp *lodiav1alpha1.ControlPlane, inputs []string) *metav1.Condition {
	next := newCondition(cp, lodiav1alpha1.ConditionReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady,
		strings.Join(inputs[:len(inputs)-1], ", ")+" and "+inputs[len(inputs)-1]+" are True")
	for _, conditionType := range inputs {
		c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
		if c == nil {
			next = newCondition(cp, lodiav1alpha1.ConditionReady, metav1.ConditionUnknown, lodiav1alpha1.ReasonLatestGenerationNotObserved,
				fmt.Sprintf("%s is not reported for generation %d", conditionType, cp.Generation))
			break
		}
		if c.Status != metav1.ConditionTrue {
			message := conditionType
			if c.Message != "" {
				message += ": " + c.Message
			}
			next = newCondition(cp, lodiav1alpha1.ConditionReady, c.Status, c.Reason, message)
			break
		}
	}
	next.ObservedGeneration = oldest(cp)
	return next
}

// oldest is what Ready describes, so that Ready True at the object's
// generation means every change is taken up.
func oldest(cp *lodiav1alpha1.ControlPlane) int64 {
	generation := cp.Generation
	for _, conditionType := range readiness(cp) {
		c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
		if c == nil {
			return cp.Generation
		}
		generation = min(generation, c.ObservedGeneration)
	}
	return generation
}

func observed(cp *lodiav1alpha1.ControlPlane) bool {
	return !slices.ContainsFunc(readiness(cp), func(conditionType string) bool {
		c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
		return c == nil || c.ObservedGeneration != cp.Generation
	})
}

func newCondition(cp *lodiav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason, message string) *metav1.Condition {
	return &metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: message, ObservedGeneration: cp.Generation}
}
