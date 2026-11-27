// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package datastore is the Datastore controller.
package datastore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/contract"
	"go.lodia.io/lodia/internal/etcddruid"
	"go.lodia.io/lodia/internal/live"
	"go.lodia.io/lodia/internal/ownership"
	"go.lodia.io/lodia/internal/restore"
	"go.lodia.io/lodia/internal/statusapply"
)

const (
	Name         = "datastore"
	FieldManager = "lodia/" + Name
)

// maxRevision is the MaxLength of status.datastore.operatorRevision.
const maxRevision = 128

type Reconciler struct {
	client.Client
	// Reader reads the API server: the ControlPlane at the top of every pass,
	// what the label-filtered cache does not hold, and the Etcd a deletion
	// acts on.
	Reader client.Reader
	// Events records the transitions of the conditions it writes;
	// SetupWithManager sets it.
	Events events.EventRecorder
	// Druid runs the etcd of each ControlPlane.
	Druid *etcddruid.Engine
	// Revision is the running operator revision, which the Datastore
	// controller renders with once Lifecycle grants it in
	// status.target.operatorRevision.
	Revision string
	// OperatorNamespace is where the operator runs; the etcd's NetworkPolicy
	// admits its pods to the client port, which the backup reconciler dials.
	OperatorNamespace string
	// EtcdClient connects to the etcd of a ControlPlane; the caller closes it.
	EtcdClient func(ctx context.Context, etcd *etcddruid.Store) (*clientv3.Client, error)
	// Clock spaces the full snapshots a restore asks for again, keeps a
	// failed copy for a minute, and bounds the wait on an etcd-druid
	// reconcile in flight.
	Clock clock.PassiveClock
	// Latches hold what the sampler tells of a member's new data life, and
	// enqueue its ControlPlane.
	Latches *Latches
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	r.Events = mgr.GetEventRecorder("lodia.io/" + Name)
	certificates, err := predicate.LabelSelectorPredicate(metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: lodiav1alpha1.EtcdCertificatesLabel, Operator: metav1.LabelSelectorOpExists}},
	})
	if err != nil {
		return err
	}
	backupCopy := predicate.NewPredicateFuncs(func(secret client.Object) bool {
		cp, ok := secret.GetLabels()[lodiav1alpha1.ControlPlaneLabel]
		return ok && secret.GetName() == lodiav1alpha1.EtcdBackupSecret.Name(cp)
	})
	caBundle := predicate.NewPredicateFuncs(func(secret client.Object) bool {
		cp, ok := secret.GetLabels()[lodiav1alpha1.ControlPlaneLabel]
		return ok && secret.GetName() == lodiav1alpha1.CABundleSecret.Name(cp)
	})
	etcdPolicy := predicate.NewPredicateFuncs(func(policy client.Object) bool {
		cp, ok := policy.GetLabels()[lodiav1alpha1.ControlPlaneLabel]
		return ok && policy.GetName() == contract.EtcdName(cp)
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.ControlPlane{}).
		Owns(r.Druid.StoreObject(), builder.WithPredicates(r.Druid.StoreChanged())).
		Owns(&networkingv1.NetworkPolicy{}, builder.WithPredicates(etcdPolicy)).
		Owns(r.Druid.CopyTaskObject()).
		Owns(r.Druid.SnapshotTaskObject()).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(controlPlaneOf), builder.WithPredicates(predicate.Or(certificates, backupCopy, caBundle))).
		Watches(&lodiav1alpha1.DatastoreClass{}, handler.EnqueueRequestsFromMapFunc(r.controlPlanesUsing)).
		Watches(&lodiav1alpha1.Backup{}, handler.EnqueueRequestsFromMapFunc(restore.RestoresOf(r.Client))).
		WatchesRawSource(source.Channel(r.Latches.told, &handler.EnqueueRequestForObject{})).
		Complete(r)
}

func controlPlaneOf(_ context.Context, secret client.Object) []reconcile.Request {
	name, ok := secret.GetLabels()[lodiav1alpha1.ControlPlaneLabel]
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: secret.GetNamespace(), Name: name}}}
}

// The ControlPlanes that name no class are enqueued too: the default may have
// moved.
func (r *Reconciler) controlPlanesUsing(ctx context.Context, class client.Object) []reconcile.Request {
	controlPlanes := &lodiav1alpha1.ControlPlaneList{}
	if err := r.List(ctx, controlPlanes); err != nil {
		logf.FromContext(ctx).Error(err, "list the ControlPlanes of a DatastoreClass", "class", class.GetName())
		return nil
	}
	var requests []reconcile.Request
	for i := range controlPlanes.Items {
		cp := &controlPlanes.Items[i]
		if name := cp.Spec.Datastore.ClassName; name == "" || name == class.GetName() {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		}
	}
	return requests
}

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/finalizers,verbs=update
// +kubebuilder:rbac:groups=lodia.io,resources=datastoreclasses,verbs=list;watch
// +kubebuilder:rbac:groups=druid.gardener.cloud,resources=etcds,verbs=get;list;watch;create;patch;delete
// delete on secrets and networkpolicies serves no call: OwnerReferencesPermissionEnforcement refuses the ownerRef of an adoption on what the controller cannot delete.
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=list;patch;delete
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch
// get on etcd-druid's Deployment is a Role in its namespace, which the chart writes: no marker can name that namespace.

// Reconcile ends a pass whose status apply conflicts with a write since its
// read with a requeue a second later, and no error and no event: the next
// pass decides everything again from a new read, where retrying the apply
// would land what the old read decided. A conflict yields to the pass's other
// errors, which it returns, so that they are logged and counted.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		if apierrors.IsNotFound(err) {
			r.Druid.Forget(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	l, err := r.lock(ctx, cp)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	result, err := r.reconcile(ctx, cp, l)
	if errors.As(err, new(conflicted)) {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	return result, err
}

type conflicted struct{ error }

func (r *Reconciler) reconcile(ctx context.Context, cp *lodiav1alpha1.ControlPlane, l *lock) (ctrl.Result, error) {
	if !cp.DeletionTimestamp.IsZero() {
		requeueAfter, err := r.finalize(ctx, cp, l)
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}
	if !controllerutil.ContainsFinalizer(cp, lodiav1alpha1.DatastoreFinalizer) {
		err := r.setFinalizer(ctx, cp, true)
		switch {
		case apierrors.IsNotFound(err):
			return ctrl.Result{}, nil
		case apierrors.IsConflict(err):
			return ctrl.Result{RequeueAfter: time.Second}, nil
		case err != nil:
			return ctrl.Result{}, err
		}
		l.version = cp.ResourceVersion
	}
	if meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady) == nil {
		first := statusapply.FirstPass(cp, lodiav1alpha1.ConditionDatastoreReady)
		backup := first
		backup.Type = lodiav1alpha1.ConditionBackupReady
		if err := r.applyStatus(ctx, cp, l, cp.Status.Datastore, cp.Status.Restore, first, backup); err != nil {
			return ctrl.Result{}, err
		}
	}
	if granted(cp) == "" {
		return ctrl.Result{}, r.waitForTarget(ctx, cp, l)
	}
	out, err := r.reconcileEtcd(ctx, cp)
	if out == nil {
		return ctrl.Result{}, err
	}
	// The policy follows an Etcd of this ControlPlane's.
	if out.etcd != nil {
		blocked, policyErr := r.applyNetworkPolicy(ctx, cp, out.etcd, out.tried)
		if blocked == nil && policyErr != nil {
			return ctrl.Result{}, errors.Join(err, policyErr)
		}
		if blocked != nil {
			out.requeueAfter = cmp.Or(out.requeueAfter, time.Minute)
			out.condition = statusapply.Refusing(out.condition, *blocked)
			out.condition.ObservedGeneration = statusapply.Described(cp, out.condition.Type)
		}
		err = errors.Join(err, policyErr)
	}
	ready, backup := out.tried.on(out.condition), r.backupReady(out.etcd)
	if out.tried != nil {
		err = errors.Join(err, out.tried.refusals)
	}
	if r.held(cp) {
		ready.ObservedGeneration, backup.ObservedGeneration = statusapply.Described(cp, ready.Type), statusapply.Described(cp, backup.Type)
	}
	ready = r.memberRestored(cp, l, ready)
	if statusErr := r.applyStatus(ctx, cp, l, datastoreStatus(cp, out.etcd, out.rollout, out.applied), out.restore, ready, backup); statusErr != nil || err != nil {
		if err != nil && errors.As(statusErr, new(conflicted)) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	return ctrl.Result{RequeueAfter: out.requeueAfter}, nil
}

// lock is what every status apply of a pass carries: the resourceVersion of
// the pass's live read, or of the pass's own last write since, so that a
// write decided from a read conflicts once another write has landed since, a
// previous leader's included; and status.memberRestore, the one the read
// showed or the latch's record the pass writes.
type lock struct {
	key     types.NamespacedName
	version string
	record  *lodiav1alpha1.MemberRestoreStatus
	// writes is the latch whose record the pass writes, dropped once that
	// lands.
	writes *latch
}

// lock takes the latch the sampler holds for cp, then reads cp live: its two
// conditions, status.datastore, status.restore, status.memberRestore and
// Workloads' status.components, as the API server holds them whatever the
// cache shows, a previous leader's last write included. The first pass that
// takes a latch keeps the record that read showed as its base, and a pass
// writes the latch's record while the record is still the base; it drops the
// latch once the record has landed, and leaves any other record, which was
// written over the base, and so after the restore the latch tells, by a write
// that moved the annotation too.
func (r *Reconciler) lock(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*lock, error) {
	key := client.ObjectKeyFromObject(cp)
	taken, latched := r.Latches.take(key)
	version, err := live.Datastore(ctx, r.Reader, cp, lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ConditionBackupReady)
	if err != nil {
		return nil, fmt.Errorf("read ControlPlane %s: %w", cp.Name, err)
	}
	l := &lock{key: key, version: version, record: cp.Status.MemberRestore}
	if !latched || taken.uid != cp.UID {
		return l, nil
	}
	base := taken.base
	if !taken.based {
		base = cp.Status.MemberRestore
		r.Latches.setBase(key, taken, base)
	}
	record := taken.record(base)
	switch {
	case same(cp.Status.MemberRestore, base):
		l.record, l.writes = record, &taken
	case same(cp.Status.MemberRestore, record):
		r.Latches.drop(key, taken)
	}
	return l, nil
}

// memberRestored is DatastoreReady over status.memberRestore: True Ready
// becomes MemberRestored while the record is unreported or its roll is not
// done, the pass that first reports it writing reportedAt. The three reports
// come from lock's read: a next leader's cache can lack those of a roll the
// previous leader saw done, and would report MemberRestored again.
func (r *Reconciler) memberRestored(cp *lodiav1alpha1.ControlPlane, l *lock, ready metav1.Condition) metav1.Condition {
	record := l.record
	// @todo: end MemberRestored at its first report for a member restored from snapshots once etcd-backup-restore restores with RevisionBump and MarkCompacted; one that starts empty still waits for the roll (todo.md).
	if record == nil || ready.Status != metav1.ConditionTrue || ready.Reason != lodiav1alpha1.ReasonReady || record.ReportedAt != nil && rolled(cp, record.DetectedAt) {
		return ready
	}
	if record.ReportedAt == nil {
		reported := *record
		reported.ReportedAt = new(metav1.NewTime(r.Clock.Now()).Rfc3339Copy())
		l.record = &reported
	}
	ready.Reason, ready.Message = lodiav1alpha1.ReasonMemberRestored, restoredMessage(*record)
	return ready
}

func rolled(cp *lodiav1alpha1.ControlPlane, detectedAt metav1.Time) bool {
	c := cp.Status.Components
	if c == nil {
		return false
	}
	for _, component := range []*lodiav1alpha1.ComponentStatus{c.APIServer, c.ControllerManager, c.Scheduler} {
		if component == nil || component.MemberRestoreDetectedAt == nil || !component.MemberRestoreDetectedAt.Equal(&detectedAt) {
			return false
		}
	}
	return true
}

func restoredMessage(record lodiav1alpha1.MemberRestoreStatus) string {
	at := record.DetectedAt.UTC().Format(time.RFC3339)
	const roll = "Lodia rolls the API server, then the controller manager and the scheduler"
	if record.From == lodiav1alpha1.MemberRestoreFromEmpty {
		return fmt.Sprintf("member %s was found started empty at %s: every write is lost and the tenant's cluster starts again from nothing; %s", record.Member, at, roll)
	}
	return fmt.Sprintf("member %s was found restored from the store's snapshots at %s: the writes etcd acknowledged after the last delta snapshot are lost; %s", record.Member, at, roll)
}

// held says whether cp's writes wait for Lifecycle to grant the running
// operator revision.
func (r *Reconciler) held(cp *lodiav1alpha1.ControlPlane) bool {
	return granted(cp) != r.Revision
}

func granted(cp *lodiav1alpha1.ControlPlane) string {
	if cp.Status.Target == nil {
		return ""
	}
	return cp.Status.Target.OperatorRevision
}

// waitForTarget writes nothing but both conditions, waiting, until Lifecycle
// grants a first operator revision.
func (r *Reconciler) waitForTarget(ctx context.Context, cp *lodiav1alpha1.ControlPlane, l *lock) error {
	waiting := notReady(lodiav1alpha1.ReasonWaitingForTarget, "waiting for Lifecycle to set status.target.operatorRevision")
	backup := waiting
	backup.Type = lodiav1alpha1.ConditionBackupReady
	return r.applyStatus(ctx, cp, l, cp.Status.Datastore, cp.Status.Restore,
		statusapply.Wait(cp, meta.FindStatusCondition(cp.Status.Conditions, waiting.Type), waiting),
		statusapply.Wait(cp, meta.FindStatusCondition(cp.Status.Conditions, backup.Type), backup))
}

// outcome is what a pass reports. A nil outcome writes no status, and comes
// with the error that stopped the pass; a refusal has both.
type outcome struct {
	etcd    *etcddruid.Store
	rollout *etcddruid.Rollout
	// applied says that etcd is this pass's apply of it, not the cache's,
	// which can predate the class the last pass reported.
	applied bool
	// tried is what a held pass's dry run of the grant's writes found.
	tried *trial
	// restore is carried into every apply: what this pass found complete, or
	// what it read.
	restore      *lodiav1alpha1.RestoreStatus
	condition    metav1.Condition
	requeueAfter time.Duration
}

// pass is what a pass has read and decided so far.
type pass struct {
	cp           *lodiav1alpha1.ControlPlane
	current      *etcddruid.Store
	claims       []corev1.PersistentVolumeClaim
	class        *lodiav1alpha1.DatastoreClass
	secrets      []metav1.PartialObjectMetadata
	generation   int
	namespaceUID types.UID
	rollout      *etcddruid.Rollout
	check        restoreCheck
	members      int32
	rendered     *etcddruid.Store
	// copied says that rendered is the current Etcd's copy a held pass
	// applies, and keep that the Etcd keeps the fields its class gave it
	// while the class waits for its grant or is held for its credentials.
	copied, keep bool
	// tried is what the pass's dry run of the grant's writes found, while
	// DatastoreReady holds a refusal, and nil otherwise.
	tried *trial
	// held is the note on DatastoreReady of a class held or waiting for its
	// generation, of a quota capped, and of a request that waits for
	// etcd-druid. It and refusal, an apply of the credentials the API server
	// refused, reach whichever step the pass ends on.
	held    string
	heldFor time.Duration
	refusal error
	// heldReason is DatastoreReady's reason while it is True and the class is
	// held for its credentials copy: Lifecycle reads it as a cause that fails
	// a change, so that it grants no slot while the hold stands and fails the
	// change in a slot.
	heldReason string
}

func (r *Reconciler) reconcileEtcd(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*outcome, error) {
	p := &pass{cp: cp}
	if trying(cp) {
		p.tried = &trial{}
	}
	out, err := r.takeSteps(ctx, p)
	if out != nil {
		out.restore = cmp.Or(out.restore, cp.Status.Restore)
		out.condition.Message = note(out.condition.Message, p.held)
		out.tried = p.tried
	}
	return out, errors.Join(err, p.refusal)
}

// takeSteps stops at the first step that has something to report: a nil
// outcome and a nil error go on to the next.
func (r *Reconciler) takeSteps(ctx context.Context, p *pass) (*outcome, error) {
	if out, err := r.readInputs(ctx, p); out != nil || err != nil {
		return out, err
	}
	if out, err := r.holdClass(ctx, p); out != nil || err != nil {
		return out, err
	}
	if err := r.renderEtcd(ctx, p); err != nil {
		return nil, err
	}
	if out, err := r.gateFirstEtcd(ctx, p); out != nil || err != nil {
		return out, err
	}
	return r.applyEtcd(ctx, p)
}

func (r *Reconciler) readInputs(ctx context.Context, p *pass) (*outcome, error) {
	cp := p.cp
	current, err := r.Druid.Current(ctx, r.Client, r.Reader, cp)
	if err != nil {
		return nil, err
	}
	// An Etcd another ControlPlane or nobody controls, an orphan or a
	// predecessor's the garbage collector has not removed yet, is never
	// applied: the apply would take it over, and this ControlPlane would start
	// on another tenant's data.
	if current != nil && !current.ControlledBy(cp) {
		return &outcome{condition: notReady(lodiav1alpha1.ReasonVolumesExist, current.Foreign()), requeueAfter: time.Minute}, nil
	}
	p.current = current
	if current == nil {
		if p.claims, err = r.claims(ctx, cp); err != nil {
			return nil, err
		}
	}
	name, byClaims := className(cp, current, p.claims)
	class, unresolved, err := r.class(ctx, name, byClaims)
	if err != nil {
		return nil, err
	}
	// A recorded class that is gone stops only its own edits: the Etcd keeps
	// what the class gave it and still takes new certificate generations.
	// Claims are outside the cache, so deleting them brings no event.
	if class == nil && current == nil {
		var requeueAfter time.Duration
		if byClaims {
			requeueAfter = time.Minute
		}
		return &outcome{condition: unresolved, requeueAfter: requeueAfter}, nil
	}
	p.class = class
	if p.secrets, err = r.secrets(ctx, cp); err != nil {
		return nil, err
	}
	generation, found := newestCompleteGeneration(cp, p.secrets)
	if named, ok := r.Druid.NamedGeneration(cp.Name, current); ok && (!found || named > generation) {
		generation, found = named, true
	}
	if !found {
		var roles []string
		for _, role := range contract.EtcdCertificateRoles() {
			roles = append(roles, string(role))
		}
		message := fmt.Sprintf("waiting for a complete generation of the etcd certificates that this ControlPlane controls: the Secrets %s-etcd-{%s}-<g> labelled %s=<g>",
			cp.Name, strings.Join(roles, ","), lodiav1alpha1.EtcdCertificatesLabel)
		return &outcome{etcd: current, condition: notReady(lodiav1alpha1.ReasonWaitingForCertificates, message)}, nil
	}
	p.generation = generation
	if current == nil {
		if foreign := foreignClaims(cp, p.claims); len(foreign) > 0 {
			message := fmt.Sprintf("the claims %s of another ControlPlane stand where the etcd of %s would be created: delete them to start empty, or keep them, and this ControlPlane waits",
				strings.Join(foreign, ", "), cp.Name)
			return &outcome{condition: notReady(lodiav1alpha1.ReasonVolumesExist, message), requeueAfter: time.Minute}, nil
		}
	}
	return nil, nil
}

func (r *Reconciler) holdClass(ctx context.Context, p *pass) (*outcome, error) {
	class := p.class
	if class == nil {
		return nil, nil
	}
	// An Etcd takes an edit of its class, or a class created again under its
	// name, once Lifecycle grants the class's UID and generation, in the
	// window: a class is shared, and an edit rolls every member on it. A new
	// Etcd takes the class as it is.
	waits := p.current != nil && generationOf(class) != grantedClass(p.cp)
	if waits {
		p.keep = true
		if recorded, _ := p.current.RecordedClass(); recorded.DatastoreClassGeneration != generationOf(class) {
			generation := strconv.FormatInt(class.Generation, 10)
			if recorded.UID != class.UID {
				generation += " of UID " + string(class.UID)
			}
			p.held = fmt.Sprintf("DatastoreClass %s generation %s waits for status.target.datastoreClass", class.Name, generation)
		}
	}
	if class.Spec.Backup == nil {
		return nil, nil
	}
	moved, err := r.storeMoved(ctx, p, class, waits)
	if err != nil {
		return nil, err
	}
	// The grant's first pass copies the class's store unless the class waits
	// for its own grant.
	tried := p.tried
	if waits {
		tried = nil
	}
	blocked, err := r.copyCredentials(ctx, p.cp, class, p.secrets, moved, tried)
	if blocked == nil && err != nil {
		return nil, err
	}
	p.refusal = err
	if blocked != nil && p.current == nil {
		message := fmt.Sprintf("DatastoreClass %s cannot be applied yet: %s", class.Name, blocked.cause)
		return &outcome{condition: notReady(lodiav1alpha1.ReasonProvisioning, message), requeueAfter: time.Minute}, nil
	}
	// A class is held whole: applying the rest of an edit could add a store
	// whose copy cannot be made, and the certificate generations still move.
	if blocked != nil {
		p.held = note(p.held, fmt.Sprintf("DatastoreClass %s is held: %s", class.Name, blocked.cause))
		p.keep, p.heldFor, p.heldReason = true, time.Minute, blocked.reason
	}
	return nil, nil
}

// grantedClass is status.target.datastoreClass, the zero value while absent.
func grantedClass(cp *lodiav1alpha1.ControlPlane) lodiav1alpha1.DatastoreClassGeneration {
	if cp.Status.Target == nil || cp.Status.Target.DatastoreClass == nil {
		return lodiav1alpha1.DatastoreClassGeneration{}
	}
	return *cp.Status.Target.DatastoreClass
}

func generationOf(class *lodiav1alpha1.DatastoreClass) lodiav1alpha1.DatastoreClassGeneration {
	return lodiav1alpha1.DatastoreClassGeneration{UID: class.UID, Generation: class.Generation}
}

// storeMoved says whether a pass that keeps the Etcd's class, held by the
// operator revision or waiting for the class's generation, keeps an Etcd on a
// store other than the one class renders: the backup-restore sidecar mounts
// <cp>-etcd-backup, so the keys of a store must not arrive before the Etcd
// names that store. The Etcd is read from the API server: the copy lands
// before the kept apply, which a stale cache only makes conflict. The Etcd's
// store reaches the sidecar only once etcd-druid serves its request, so a move
// granted while that request waits for etcd-druid's Deployment brings the keys
// first, for as long as the wait lasts.
func (r *Reconciler) storeMoved(ctx context.Context, p *pass, class *lodiav1alpha1.DatastoreClass, waits bool) (bool, error) {
	if p.current == nil || !r.held(p.cp) && !waits {
		return false, nil
	}
	etcd, err := r.Druid.Reread(ctx, r.Reader, p.current)
	if err != nil {
		return false, err
	}
	if p.namespaceUID, err = r.namespaceUID(ctx, p.cp); err != nil {
		return false, err
	}
	return !etcd.OnStore(p.cp, class.Spec.Backup, p.namespaceUID), nil
}

func (r *Reconciler) namespaceUID(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (types.UID, error) {
	namespace := &corev1.Namespace{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Name: cp.Namespace}, namespace); err != nil {
		return "", fmt.Errorf("read Namespace %s: %w", cp.Namespace, err)
	}
	return namespace.UID, nil
}

func (r *Reconciler) renderEtcd(ctx context.Context, p *pass) error {
	cp := p.cp
	p.copied = p.current != nil && r.held(cp)
	class := p.class
	if p.keep {
		class = nil
	}
	var err error
	if !p.copied && class != nil && class.Spec.Backup != nil {
		if p.namespaceUID, err = r.namespaceUID(ctx, cp); err != nil {
			return err
		}
	}
	if p.rollout, err = r.Druid.Rollout(ctx, r.Reader, cp); err != nil {
		return err
	}
	if err := r.restoreMembers(ctx, p); err != nil {
		return err
	}
	if p.copied {
		p.rendered = r.Druid.Copy(cp, p.current, p.generation)
		p.held = note(p.held, p.current.KeptCap(p.class))
		return nil
	}
	var capped string
	p.rendered, capped = r.Druid.Render(cp, class, p.namespaceUID, p.generation, p.current, p.claims, p.rollout, p.members)
	p.rendered.Stamp(r.Revision)
	if p.keep {
		capped = p.current.KeptCap(p.class)
	}
	p.held = note(p.held, capped)
	return nil
}

func (r *Reconciler) applyEtcd(ctx context.Context, p *pass) (*outcome, error) {
	cp, rendered := p.cp, p.rendered
	etcd, err := r.Druid.Apply(ctx, r.Client, FieldManager, rendered)
	if err != nil {
		err = fmt.Errorf("apply Etcd %s: %w", rendered.Name(), err)
		if apistatus.Refused(err) && !replaced(err) {
			why, heldErr := r.whyRefused(ctx, rendered, err)
			condition, readErr := r.refusing(ctx, p, p.current, notReady(lodiav1alpha1.ReasonRefused, "the API server refuses "+sent(p, rendered)+why))
			if readErr != nil {
				return nil, errors.Join(err, heldErr, readErr)
			}
			return &outcome{etcd: p.current, restore: p.check.restored, condition: condition}, errors.Join(err, heldErr)
		}
		return nil, err
	}
	if p.copied && p.tried != nil {
		if err := r.tryEtcd(ctx, p, etcd); err != nil {
			return nil, err
		}
	}
	class, _ := etcd.Class()
	if err := r.labelClaims(ctx, cp, class); err != nil {
		return nil, err
	}
	requested, waiting, requestErr := r.Druid.RequestReconcile(ctx, r.Client, r.Reader, r.Clock, FieldManager, etcd)
	if requested == nil {
		return nil, requestErr
	}
	p.held = note(p.held, waiting)
	// The rollout read before the apply can predate a request the response
	// shows served.
	if p.rollout, err = r.Druid.Rollout(ctx, r.Reader, cp); err != nil {
		return nil, errors.Join(requestErr, err)
	}
	if requestErr != nil {
		condition, readErr := r.refusing(ctx, p, etcd, notReady(lodiav1alpha1.ReasonRefused, etcd.RequestRefused()))
		if readErr != nil {
			return nil, errors.Join(requestErr, readErr)
		}
		return &outcome{etcd: etcd, rollout: p.rollout, applied: true, restore: p.check.restored, condition: condition}, requestErr
	}
	etcd = requested
	absent, err := r.missingSecrets(ctx, cp, etcd, p.secrets)
	if err != nil {
		return nil, err
	}
	condition := p.check.reported(cp, etcd.Readiness(absent))
	if condition.Status == metav1.ConditionTrue && p.heldReason != "" {
		condition.Reason = p.heldReason
	}
	if !p.copied {
		condition.Message = note(condition.Message, etcd.MemberNote(p.members))
	}
	requeueAfter := p.heldFor
	if updated := etcd.UpdatedMembers(p.rollout); updated == nil || *updated < etcd.Members() || !etcd.Served() {
		requeueAfter = r.Druid.StatusSync
	}
	return &outcome{etcd: etcd, rollout: p.rollout, applied: true, restore: p.check.restored, condition: condition, requeueAfter: sooner(requeueAfter, p.check.requeueAfter)}, nil
}

// sent names the Etcd rendered as the pass sends it: the render on its
// certificate generation with its class's fields, or a held pass's copy.
func sent(p *pass, rendered *etcddruid.Store) string {
	if p.copied && rendered == p.rendered {
		return fmt.Sprintf("Etcd %s on certificate generation %d, the rest of its spec as stored", rendered.Name(), p.generation)
	}
	class, _ := rendered.Class()
	return fmt.Sprintf("Etcd %s on certificate generation %d with the fields of DatastoreClass %s generation %s", rendered.Name(), p.generation, class, rendered.ClassGeneration())
}

// whyRefused is what the API server's refusal of rendered, refused, says,
// after the refusal: the objects the apply empties while another manager
// holds them, and where the rest of the refusal is.
func (r *Reconciler) whyRefused(ctx context.Context, rendered *etcddruid.Store, refused error) (string, error) {
	held, explained, err := r.Druid.HeldEmpty(ctx, r.Reader, FieldManager, rendered, refused)
	var why string
	if len(held) > 0 {
		why = ": the apply empties " + strings.Join(held, ", and ") + "; the API server leaves an object emptied while another manager holds it null, or empty without the fields it requires, and the Etcd's schema refuses either"
	}
	switch {
	case len(held) == 0:
		why += "; the reason is in Lodia's log"
	case !explained:
		why += "; the rest of the refusal is in Lodia's log"
	}
	return why, err
}

// refusing is DatastoreReady while the refusal refused names stands: etcd's
// readiness, True under the refusal's reason, with the refusal after it, at
// the generation the condition describes, or refused itself while there is no
// Etcd.
func (r *Reconciler) refusing(ctx context.Context, p *pass, etcd *etcddruid.Store, refused metav1.Condition) (metav1.Condition, error) {
	if etcd == nil {
		return refused, nil
	}
	absent, err := r.missingSecrets(ctx, p.cp, etcd, p.secrets)
	if err != nil {
		return metav1.Condition{}, err
	}
	c := statusapply.Refusing(p.check.reported(p.cp, etcd.Readiness(absent)), refused)
	c.ObservedGeneration = statusapply.Described(p.cp, c.Type)
	return c, nil
}

// sooner is the earlier of two requeues, 0 being none.
func sooner(a, b time.Duration) time.Duration {
	if a == 0 || b != 0 && b < a {
		return b
	}
	return a
}

func note(message, held string) string {
	switch {
	case held == "":
		return message
	case message == "":
		return held
	}
	return message + "; " + held
}

// replaced says whether the API server refuses an apply for the UID it names,
// that of an Etcd another has replaced since the cache read it; the next pass
// reads the new one.
func replaced(err error) bool {
	var status apierrors.APIStatus
	if !errors.As(err, &status) || status.Status().Details == nil {
		return false
	}
	for _, cause := range status.Status().Details.Causes {
		if cause.Field == "metadata.uid" {
			return true
		}
	}
	return false
}

// A class is resolved once and recorded on the Etcd, so a default that moves
// changes no running etcd; this ControlPlane's claims record it too, so an
// Etcd created again over them keeps it.
func className(cp *lodiav1alpha1.ControlPlane, current *etcddruid.Store, claims []corev1.PersistentVolumeClaim) (string, bool) {
	if current != nil {
		if recorded, ok := current.Class(); ok {
			return recorded, false
		}
	} else if i := slices.IndexFunc(claims, func(claim corev1.PersistentVolumeClaim) bool {
		return claim.Labels[lodiav1alpha1.ControlPlaneUIDLabel] == string(cp.UID) && claim.Annotations[lodiav1alpha1.DatastoreClassAnnotation] != ""
	}); i >= 0 {
		return claims[i].Annotations[lodiav1alpha1.DatastoreClassAnnotation], true
	}
	return cp.Spec.Datastore.ClassName, false
}

func (r *Reconciler) class(ctx context.Context, name string, byClaims bool) (*lodiav1alpha1.DatastoreClass, metav1.Condition, error) {
	if name != "" {
		class := &lodiav1alpha1.DatastoreClass{}
		if err := r.Get(ctx, client.ObjectKey{Name: name}, class); err != nil {
			if apierrors.IsNotFound(err) && byClaims {
				return nil, notReady(lodiav1alpha1.ReasonDatastoreClassNotFound, "DatastoreClass "+name+", which the claims of this ControlPlane record, does not exist"), nil
			}
			if apierrors.IsNotFound(err) {
				return nil, notReady(lodiav1alpha1.ReasonDatastoreClassNotFound, "DatastoreClass "+name+" does not exist"), nil
			}
			return nil, metav1.Condition{}, err
		}
		return class, metav1.Condition{}, nil
	}
	classes := &lodiav1alpha1.DatastoreClassList{}
	if err := r.List(ctx, classes); err != nil {
		return nil, metav1.Condition{}, err
	}
	var defaults []*lodiav1alpha1.DatastoreClass
	for i := range classes.Items {
		if classes.Items[i].Annotations[lodiav1alpha1.DefaultClassAnnotation] == "true" {
			defaults = append(defaults, &classes.Items[i])
		}
	}
	switch len(defaults) {
	case 1:
		return defaults[0], metav1.Condition{}, nil
	case 0:
		return nil, notReady(lodiav1alpha1.ReasonDatastoreClassNotFound, "no DatastoreClass is annotated "+lodiav1alpha1.DefaultClassAnnotation+`: "true"`), nil
	default:
		names := make([]string, 0, len(defaults))
		for _, class := range defaults {
			names = append(names, class.Name)
		}
		slices.Sort(names)
		return nil, notReady(lodiav1alpha1.ReasonDatastoreClassAmbiguous, fmt.Sprintf("%d DatastoreClasses are annotated %s: %q, exactly one may be: %s",
			len(defaults), lodiav1alpha1.DefaultClassAnnotation, "true", strings.Join(names, ", "))), nil
	}
}

func (r *Reconciler) secrets(ctx context.Context, cp *lodiav1alpha1.ControlPlane) ([]metav1.PartialObjectMetadata, error) {
	secrets := &metav1.PartialObjectMetadataList{}
	secrets.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("SecretList"))
	if err := r.List(ctx, secrets, client.InNamespace(cp.Namespace), client.MatchingLabels{lodiav1alpha1.ControlPlaneLabel: cp.Name}); err != nil {
		return nil, err
	}
	return secrets.Items, nil
}

func (r *Reconciler) secret(ctx context.Context, cp *lodiav1alpha1.ControlPlane, name string, secrets []metav1.PartialObjectMetadata) (*metav1.PartialObjectMetadata, error) {
	if i := slices.IndexFunc(secrets, func(s metav1.PartialObjectMetadata) bool { return s.Name == name }); i >= 0 {
		return &secrets[i], nil
	}
	secret := &metav1.PartialObjectMetadata{}
	secret.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, secret); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return secret, nil
}

func (r *Reconciler) missingSecrets(ctx context.Context, cp *lodiav1alpha1.ControlPlane, etcd *etcddruid.Store, secrets []metav1.PartialObjectMetadata) ([]string, error) {
	var missing []string
	for _, name := range etcd.Secrets() {
		secret, err := r.secret(ctx, cp, name, secrets)
		if err != nil {
			return nil, err
		}
		if secret == nil {
			missing = append(missing, name)
		}
	}
	return missing, nil
}

func newestCompleteGeneration(cp *lodiav1alpha1.ControlPlane, secrets []metav1.PartialObjectMetadata) (int, bool) {
	labelled := make(map[string]string, len(secrets))
	for _, secret := range secrets {
		if generation, ok := secret.Labels[lodiav1alpha1.EtcdCertificatesLabel]; ok && secret.DeletionTimestamp.IsZero() && metav1.IsControlledBy(&secret, cp) {
			labelled[secret.Name] = generation
		}
	}
	newest, found := 0, false
	for _, label := range labelled {
		generation, err := strconv.Atoi(label)
		if err != nil || generation < 0 || (found && generation <= newest) {
			continue
		}
		complete := true
		for _, certificate := range contract.EtcdCertificateRoles() {
			if labelled[contract.EtcdCertificateSecretName(cp.Name, certificate, generation)] != label {
				complete = false
				break
			}
		}
		if complete {
			newest, found = generation, true
		}
	}
	return newest, found
}

// Claims are matched by the name the StatefulSet attaches them by, not by
// labels, which a claim made by hand lacks.
func (r *Reconciler) claims(ctx context.Context, cp *lodiav1alpha1.ControlPlane) ([]corev1.PersistentVolumeClaim, error) {
	list := &corev1.PersistentVolumeClaimList{}
	if err := r.Reader.List(ctx, list, client.InNamespace(cp.Namespace)); err != nil {
		return nil, err
	}
	var claims []corev1.PersistentVolumeClaim
	for _, claim := range list.Items {
		if _, ok := r.Druid.ClaimOrdinal(cp.Name, claim.Name); ok {
			claims = append(claims, claim)
		}
	}
	return claims, nil
}

func memberCount(cp *lodiav1alpha1.ControlPlane) int32 {
	if cp.Spec.HighAvailability != nil {
		return 3
	}
	return 1
}

// claimedMembers is how many members the claims held, at most limit: a
// member whose claim is gone still counts below the highest ordinal.
func (r *Reconciler) claimedMembers(cp string, claims []corev1.PersistentVolumeClaim, limit int32) int32 {
	held := 1
	for _, claim := range claims {
		if ordinal, ok := r.Druid.ClaimOrdinal(cp, claim.Name); ok {
			held = max(held, min(ordinal, int(limit)-1)+1)
		}
	}
	return int32(held)
}

// Starting an etcd on a claim without this ControlPlane's UID would be a
// restore of another tenant's data by name.
func foreignClaims(cp *lodiav1alpha1.ControlPlane, claims []corev1.PersistentVolumeClaim) []string {
	var foreign []string
	for _, claim := range claims {
		if claim.Labels[lodiav1alpha1.ControlPlaneUIDLabel] != string(cp.UID) {
			foreign = append(foreign, claim.Name)
		}
	}
	slices.Sort(foreign)
	return foreign
}

// The claims outlive the Etcd, so they carry the UID and the class their
// reclaim goes by.
func (r *Reconciler) labelClaims(ctx context.Context, cp *lodiav1alpha1.ControlPlane, class string) error {
	claims, err := r.claims(ctx, cp)
	if err != nil {
		return err
	}
	for i := range claims {
		claim := &claims[i]
		if _, labelled := claim.Labels[lodiav1alpha1.ControlPlaneUIDLabel]; labelled {
			continue
		}
		patch := client.MergeFrom(claim.DeepCopy())
		metav1.SetMetaDataLabel(&claim.ObjectMeta, lodiav1alpha1.ControlPlaneUIDLabel, string(cp.UID))
		metav1.SetMetaDataAnnotation(&claim.ObjectMeta, lodiav1alpha1.DatastoreClassAnnotation, class)
		if err := client.IgnoreNotFound(r.Patch(ctx, claim, patch, client.FieldOwner(FieldManager))); err != nil {
			return err
		}
	}
	return nil
}

// blocked is why a class's credentials copy cannot be made: the reason
// DatastoreReady gives it, and the cause.
type blocked struct {
	reason, cause string
}

// etcd-druid mounts the store's Secret from the Etcd's namespace, and the
// backup-restore sidecar reads the region from it. An existing copy is kept
// while moved says the Etcd is held on another store, and sent by dry run into
// tried when that is set. The first result says why the copy cannot be made,
// the second carries a refusal for the log.
func (r *Reconciler) copyCredentials(ctx context.Context, cp *lodiav1alpha1.ControlPlane, class *lodiav1alpha1.DatastoreClass, secrets []metav1.PartialObjectMetadata, moved bool, tried *trial) (*blocked, error) {
	store := class.Spec.Backup.Store
	source := &corev1.Secret{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: store.SecretRef.Namespace, Name: store.SecretRef.Name}, source); err != nil {
		if apierrors.IsNotFound(err) {
			return &blocked{lodiav1alpha1.ReasonSecretNotFound, fmt.Sprintf("its credentials Secret %s/%s is missing", store.SecretRef.Namespace, store.SecretRef.Name)}, nil
		}
		return nil, fmt.Errorf("read the credentials Secret %s/%s of DatastoreClass %s: %w", store.SecretRef.Namespace, store.SecretRef.Name, class.Name, err)
	}
	name := lodiav1alpha1.EtcdBackupSecret.Name(cp.Name)
	existing, err := r.secret(ctx, cp, name, secrets)
	if err != nil {
		return nil, err
	}
	data := maps.Clone(source.Data)
	if store.Region != "" {
		if data == nil {
			data = map[string][]byte{}
		}
		data["region"] = []byte(store.Region)
	}
	secret := corev1ac.Secret(name, cp.Namespace).
		WithLabels(map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name}).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(lodiav1alpha1.GroupVersion.String()).
			WithKind("ControlPlane").
			WithName(cp.Name).
			WithUID(cp.UID).
			WithController(true).
			WithBlockOwnerDeletion(true)).
		WithData(data)
	if existing != nil {
		if ref := ownership.ControlledElsewhere(existing, cp); ref != nil {
			return &blocked{lodiav1alpha1.ReasonObjectsControlledElsewhere, fmt.Sprintf("%s is controlled by %s %s with UID %s", name, ref.Kind, ref.Name, ref.UID)}, nil
		}
		if moved {
			if tried == nil {
				return nil, nil
			}
			return nil, tried.answer("Secret "+name, r.Apply(ctx, secret, client.FieldOwner(FieldManager), client.ForceOwnership, client.DryRunAll), inTheLog)
		}
	}
	if err := r.Apply(ctx, secret, client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		err = fmt.Errorf("apply Secret %s: %w", name, err)
		if apistatus.Refused(err) {
			return &blocked{lodiav1alpha1.ReasonRefused, fmt.Sprintf("the API server refuses %s; the reason is in Lodia's log", name)}, err
		}
		return nil, err
	}
	return nil, nil
}

func (r *Reconciler) finalize(ctx context.Context, cp *lodiav1alpha1.ControlPlane, l *lock) (time.Duration, error) {
	if !controllerutil.ContainsFinalizer(cp, lodiav1alpha1.DatastoreFinalizer) {
		return 0, nil
	}
	// Read from the API server: a deleted Etcd the cache still held would have
	// the claims of one standing under the name since labelled, and reclaimed.
	etcd, err := r.Druid.Get(ctx, r.Reader, cp)
	if err != nil {
		return 0, err
	}
	foreign := etcd != nil && !etcd.ControlledBy(cp)
	if foreign {
		etcd = nil
	}
	deleting := notReady(lodiav1alpha1.ReasonDeleting, "the ControlPlane is being deleted: the Etcd goes first, then its claims as the class says")
	if err := r.applyStatus(ctx, cp, l, datastoreStatus(cp, etcd, nil, false), cp.Status.Restore, deleting, r.backupReady(etcd)); err != nil {
		return 0, err
	}
	// A claim no pass has labelled yet can only be this Etcd's: VolumesExist
	// kept it from being created while anything else stood there.
	if etcd != nil {
		class, _ := etcd.Class()
		if err := r.labelClaims(ctx, cp, class); err != nil {
			return 0, err
		}
	}
	switch {
	case etcd != nil && !etcd.Deleting():
		logf.FromContext(ctx).Info("deleting the Etcd", "etcd", etcd.Name())
		return 0, client.IgnoreNotFound(r.Druid.Delete(ctx, r.Client, etcd))
	case etcd != nil:
		return 10 * time.Second, nil
	case foreign:
		logf.FromContext(ctx).Info("leaving the Etcd and the claims its StatefulSet uses alone, the ControlPlane does not control it", "etcd", contract.EtcdName(cp.Name))
	default:
		if err := r.reclaimClaims(ctx, cp); err != nil {
			return 0, err
		}
	}
	if err := r.setFinalizer(ctx, cp, false); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return 0, err
	}
	return 0, nil
}

func (r *Reconciler) reclaimClaims(ctx context.Context, cp *lodiav1alpha1.ControlPlane) error {
	log := logf.FromContext(ctx)
	claims, err := r.claims(ctx, cp)
	if err != nil {
		return err
	}
	for i := range claims {
		claim := &claims[i]
		if claim.Labels[lodiav1alpha1.ControlPlaneUIDLabel] != string(cp.UID) || !claim.DeletionTimestamp.IsZero() {
			continue
		}
		class := &lodiav1alpha1.DatastoreClass{}
		name := claim.Annotations[lodiav1alpha1.DatastoreClassAnnotation]
		if name == "" {
			log.Info("keeping the claim, it records no class", "claim", claim.Name)
			continue
		}
		err := r.Get(ctx, client.ObjectKey{Name: name}, class)
		switch {
		case apierrors.IsNotFound(err):
			log.Info("keeping the claim, its class is gone", "claim", claim.Name, "class", name)
			continue
		case err != nil:
			return err
		case class.Spec.Storage.ReclaimPolicy != "Delete":
			continue
		}
		log.Info("deleting the claim", "claim", claim.Name, "class", name)
		if err := client.IgnoreNotFound(r.Delete(ctx, claim, client.Preconditions{UID: &claim.UID})); err != nil {
			return err
		}
	}
	return nil
}

// The finalizer goes through a patch locked on the resourceVersion, never an
// apply, which would create a ControlPlane the cache still showed after its
// deletion. A conflict is a change since the read, which reconciles it again.
func (r *Reconciler) setFinalizer(ctx context.Context, cp *lodiav1alpha1.ControlPlane, held bool) error {
	patch := client.MergeFromWithOptions(cp.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if held {
		controllerutil.AddFinalizer(cp, lodiav1alpha1.DatastoreFinalizer)
	} else {
		controllerutil.RemoveFinalizer(cp, lodiav1alpha1.DatastoreFinalizer)
	}
	return r.Patch(ctx, cp, patch, client.FieldOwner(FieldManager))
}

// datastoreStatus says where the API server reaches the etcd, its endpoint,
// where its snapshots go, and the class and the operator revision it runs:
// those the Etcd records once RolledOut says so and its stamp fits the field,
// and until then those the API server held at the pass's start, since
// the field is written whole on every pass and the cache can lag this
// controller's own last write. While no revision is reported, or the Etcd
// applied records a class the report does not name, as after one was created
// again from another class, the class is the one that Etcd records, or no edit
// of it could become pending, the one that would let the etcd roll included.
// Only a person changes the name an Etcd records, and the report follows it
// at once, since the Etcd takes the class of that name; a UID or generation
// that moves under one name waits for RolledOut.
func datastoreStatus(cp *lodiav1alpha1.ControlPlane, etcd *etcddruid.Store, rollout *etcddruid.Rollout, applied bool) *lodiav1alpha1.DatastoreStatus {
	if etcd == nil {
		return nil
	}
	host, port := etcd.Endpoint()
	status := &lodiav1alpha1.DatastoreStatus{
		Endpoints: []lodiav1alpha1.HostPort{{Host: host, Port: port}},
		Members:   lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: etcd.Members(), Ready: etcd.ReadyMembers()}, Updated: etcd.UpdatedMembers(rollout)},
	}
	status.Snapshots, _ = snapshots(etcd)
	if stamp := etcd.Revision(); etcd.RolledOut(rollout) && len(stamp) <= maxRevision {
		status.Class, status.OperatorRevision = etcd.ClassStatus(), stamp
		return status
	}
	if previous := cp.Status.Datastore; previous != nil {
		status.Class, status.OperatorRevision = previous.Class, previous.OperatorRevision
	}
	if c := etcd.ClassStatus(); applied && (status.OperatorRevision == "" || c != nil && (status.Class == nil || status.Class.Name != c.Name)) {
		status.Class = c
	}
	return status
}

// snapshots is where the Etcd's store puts its snapshots, nil without a store
// or with one that status.datastore.snapshots cannot hold, which the message
// says.
func snapshots(etcd *etcddruid.Store) (*lodiav1alpha1.SnapshotLocation, string) {
	if etcd == nil {
		return nil, ""
	}
	location := etcd.Snapshots()
	if location == nil {
		return nil, ""
	}
	if why := unrecordable(*location); why != "" {
		return nil, fmt.Sprintf("Etcd %s names a backup store that status.datastore.snapshots cannot hold, and it is left out: %s", etcd.Name(), why)
	}
	return location, ""
}

// unrecordable says why a status cannot hold location, empty when it can: an
// edit of the Etcd's store by hand can take values its schema refuses.
func unrecordable(location lodiav1alpha1.SnapshotLocation) string {
	var why []string
	if !slices.Contains([]lodiav1alpha1.StoreProvider{"S3", "GCS", "ABS", "Swift"}, location.Provider) {
		why = append(why, "its provider is none of S3, GCS, ABS and Swift")
	}
	if n := utf8.RuneCountInString(location.Bucket); n < 1 || n > 63 {
		why = append(why, "its bucket is not 1 to 63 characters long")
	}
	if n := utf8.RuneCountInString(location.Prefix); n < 1 || n > 512 {
		why = append(why, "its prefix is not 1 to 512 characters long")
	}
	if utf8.RuneCountInString(location.Endpoint) > 512 {
		why = append(why, "its endpoint is longer than 512 characters")
	}
	return strings.Join(why, ", ")
}

// backupReady is etcd-druid's report of the Etcd's snapshots, but for a store
// that status.datastore.snapshots cannot hold.
func (r *Reconciler) backupReady(etcd *etcddruid.Store) metav1.Condition {
	if _, why := snapshots(etcd); why != "" {
		return metav1.Condition{Type: lodiav1alpha1.ConditionBackupReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonStoreUnsupported, Message: why}
	}
	return r.Druid.BackupReady(etcd)
}

// applyStatus writes DatastoreReady and BackupReady together: the conditions
// are a list keyed by type, and an apply that left one out would give up its
// ownership. The restore and the member restore are carried into every apply
// for the same reason, under l. Each condition is at cp's generation unless
// it names one.
func (r *Reconciler) applyStatus(ctx context.Context, cp *lodiav1alpha1.ControlPlane, l *lock, datastore *lodiav1alpha1.DatastoreStatus, restore *lodiav1alpha1.RestoreStatus, ready, backup metav1.Condition) error {
	for _, c := range []*metav1.Condition{&ready, &backup} {
		if c.ObservedGeneration == 0 {
			c.ObservedGeneration = cp.Generation
		}
	}
	changed := !equality.Semantic.DeepEqual(cp.Status.Datastore, datastore) || !equality.Semantic.DeepEqual(cp.Status.Restore, restore) || !equality.Semantic.DeepEqual(cp.Status.MemberRestore, l.record)
	desired := lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{ready, backup}, Datastore: datastore, Restore: restore, MemberRestore: l.record}
	version, err := statusapply.Writer{Client: r.Client, Manager: FieldManager, Events: r.Events}.ApplyVersion(ctx, cp, desired, changed, l.version)
	if apierrors.IsConflict(err) {
		return conflicted{err}
	}
	if err != nil || version == "" {
		return err
	}
	l.version = version
	if l.writes != nil {
		r.Latches.drop(l.key, *l.writes)
		l.writes = nil
	}
	return nil
}

func notReady(reason, message string) metav1.Condition {
	return metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionFalse, Reason: reason, Message: message}
}
