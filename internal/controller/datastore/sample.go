// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	"go.lodia.io/lodia/internal/etcddruid"
	"go.lodia.io/lodia/internal/live"
)

const (
	SampleName = "datastore-sample"

	sampleEvery = time.Minute

	// sampleTimeout bounds a pass, and a sample is served from its pass's
	// start plus sampleTimeout until the next one is, so each stands
	// sampleEvery whatever its pass took: an alert that holds for a minute
	// then fires on two samples and never on one
	// (docs/decisions/metrics-are-part-of-the-api.md).
	sampleTimeout = 5 * time.Second

	// etcdCallsTime is what a pass keeps of its deadline for MemberList and
	// the Status calls: a client that comes later keeps no sample, so a slow
	// management API server never makes a healthy etcd leaderless.
	etcdCallsTime = time.Second
)

// EtcdSample is what a pass read from the members of a ControlPlane's etcd.
type EtcdSample struct {
	// HasLeader says whether a quorum of the voting members answered naming
	// one leader.
	HasLeader bool
	// DBSize is the largest dbSize among the members that answered, the size
	// etcd checks its quota against; nil when none did.
	DBSize *int64
	// Alarms are the alarms armed in the etcd, as the members that answered
	// list them.
	Alarms []string
}

// SampleReconciler samples the etcd of every ControlPlane whose Etcd is
// formed, once a minute, for Lodia's metrics, and tells the reconciler of a
// member that began a new data life; it writes nothing.
type SampleReconciler struct {
	// Cache reads the ControlPlanes and their Etcds.
	Cache client.Reader
	// Reader reads status.memberRestore from the API server, which a new
	// leader's cache can lack.
	Reader client.Reader
	// Druid runs the etcd of each ControlPlane.
	Druid *etcddruid.Engine
	// EtcdClient connects to the etcd of a ControlPlane; the caller closes it.
	EtcdClient func(ctx context.Context, etcd *etcddruid.Store) (*clientv3.Client, error)
	// Clock times the passes and serves their samples.
	Clock clock.PassiveClock
	// Latches take what the sampler tells, for the reconciler.
	Latches *Latches

	mu    sync.Mutex
	kept  map[types.NamespacedName]*kept
	lives map[types.NamespacedName]*lives
}

// lives is what the rule keeps of a ControlPlane's members, apart from the
// samples: each member's last answer, by name.
type lives struct {
	uid     types.UID
	answers map[string]answer
}

// answer is a member's data life as the second of a pass's two Status calls,
// B, read it: its cluster ID, the term of its last applied entry, and bounds
// of its offset, the revision less the raft index. The lower bound takes B's
// committed index, which Status reads after the revision; the upper bound the
// applied index of the call before, A, which etcd reads before it
// (etcd@v3.5.27 server/etcdserver/api/v3rpc/maintenance.go#L225-L238,
// header.go#L39-L49): a revision read with a later applied index can stand
// one above the offset.
type answer struct {
	cluster, term uint64
	lower, upper  int64
}

// newLife says whether got is of another data life than kept. Within one
// life etcd keeps the cluster ID it bootstrapped with, which it reads back
// from its write-ahead log at each restart
// (etcd@v3.5.27 server/etcdserver/server.go#L413-L474,
// server/etcdserver/raft.go#L535-L549),
// reports the term of the last entry it applied, which never falls
// (server/etcdserver/server.go#L2251-L2252), and raises the revision at most
// once per entry (server/mvcc/kvstore_txn.go#L113-L120), so the offset never
// rises; a restore from snapshots and an empty start break one of the three.
func newLife(kept, got answer) bool {
	return got.cluster != kept.cluster || got.term < kept.term || got.lower > kept.upper
}

// kept is what a ControlPlane's passes left: the newest sample and the one
// before, each with the time it is served from.
type kept struct {
	uid              types.UID
	latest, previous *timed
}

type timed struct {
	from   time.Time
	sample EtcdSample
}

// SetupWithManager passes on a ControlPlane's creation and deletion alone:
// every other pass comes sampleEvery after the last one started, so a status
// write or an Etcd's change adds no call to the etcd.
func (r *SampleReconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(SampleName).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: sampleTimeout}).
		For(&lodiav1alpha1.ControlPlane{}, builder.WithPredicates(predicate.Funcs{UpdateFunc: func(event.UpdateEvent) bool { return false }})).
		Complete(r)
}

// Reconcile returns no error on any path: beside an error controller-runtime
// drops RequeueAfter and requeues by a backoff from 5 ms to 1000 s
// (https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/internal/controller/controller.go#L486-L497,
// https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/controller/controller.go#L270-L273).
//
// The rule's answers and the latch go only with the ControlPlane, its UID or
// its deletion: a pass that reads no answer says nothing of the members'
// data. A latch no reconciler pass has taken is sent again at every pass.
func (r *SampleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	start := r.Clock.Now()
	again := func() (ctrl.Result, error) {
		return ctrl.Result{RequeueAfter: sampleEvery - r.Clock.Since(start)}, nil
	}
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Cache.Get(ctx, req.NamespacedName, cp); err != nil {
		r.forget(req.NamespacedName)
		if apierrors.IsNotFound(err) {
			r.end(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		logf.FromContext(ctx).Error(err, "read the ControlPlane")
		return again()
	}
	if !cp.DeletionTimestamp.IsZero() {
		r.forget(req.NamespacedName)
		r.end(req.NamespacedName)
		return again()
	}
	r.pass(ctx, cp, start)
	if r.Latches.untaken(req.NamespacedName, cp.UID) {
		r.Latches.send(ctx, cp)
	}
	return again()
}

func (r *SampleReconciler) pass(ctx context.Context, cp *lodiav1alpha1.ControlPlane, start time.Time) {
	key := client.ObjectKeyFromObject(cp)
	etcd, err := r.sampled(ctx, cp)
	if err != nil {
		logf.FromContext(ctx).Error(err, "read the Etcd")
	}
	if etcd == nil {
		r.forget(key)
		return
	}
	sample, listed, answers, ok := r.sample(ctx, etcd)
	if !ok {
		r.forget(key)
		return
	}
	r.keep(key, cp.UID, start, sample)
	if listed != nil {
		r.tell(ctx, cp, start, listed, answers)
	}
}

// sampled is the Etcd of cp to sample: cp's own and formed, of a ControlPlane
// that is not paused; nil otherwise. An etcd that never had a ready member is
// on its way, a new one or a restore's, and has no leader to page about.
func (r *SampleReconciler) sampled(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*etcddruid.Store, error) {
	if ptr.Deref(cp.Spec.Paused, false) {
		return nil, nil
	}
	etcd, err := r.Druid.Get(ctx, r.Cache, cp)
	if err != nil || etcd == nil {
		return nil, err
	}
	if !etcd.ControlledBy(cp) || !etcd.Formed() {
		return nil, nil
	}
	return etcd, nil
}

// sample lists etcd's members and reads each one's Status at once, and is
// false when there is no client to do so with in time: a failed or slow read
// of the client certificate says nothing about the etcd. It also returns the
// names listed, nil when the list failed, and the rule's answer of each
// member that answered.
//
// The list is serializable, which a member answers from its own state
// (etcd@v3.5.27 server/etcdserver/api/v3rpc/member.go#L90-L98), so an etcd
// without a leader still gives its size and alarms until its members leave
// the client Service, as etcddruid.Store.Endpoint says; MemberList then fails
// or waits out the pass. The alarms come from each
// member's Status, not from AlarmList, which goes through raft
// (etcd@v3.5.27 server/etcdserver/v3_server.go#L491-L497).
func (r *SampleReconciler) sample(ctx context.Context, etcd *etcddruid.Store) (EtcdSample, []string, map[string]answer, bool) {
	log := logf.FromContext(ctx).WithValues("etcd", etcd.Name())
	c, err := r.EtcdClient(ctx, etcd)
	if err != nil {
		log.Error(err, "connect to the etcd")
		return EtcdSample{}, nil, nil, false
	}
	defer func() { _ = c.Close() }()
	deadline, _ := ctx.Deadline()
	if left := time.Until(deadline); left < etcdCallsTime {
		log.Error(fmt.Errorf("%s of the pass left, below the %s its calls to the etcd need", left, etcdCallsTime), "connect to the etcd")
		return EtcdSample{}, nil, nil, false
	}
	list, err := c.MemberList(ctx, clientv3.WithSerializable())
	if err != nil {
		log.Error(err, "list the etcd's members")
		return EtcdSample{}, nil, nil, true
	}
	listed := make([]string, len(list.Members))
	statuses := make([]*clientv3.StatusResponse, len(list.Members))
	read := make([]answer, len(list.Members))
	errs := make([]error, len(list.Members))
	var wg sync.WaitGroup
	for i, member := range list.Members {
		listed[i] = member.GetName()
		// A member added and not started yet has no client URL.
		if len(member.GetClientURLs()) == 0 {
			continue
		}
		wg.Go(func() {
			if statuses[i], read[i], errs[i] = status(ctx, c, member.GetClientURLs()[0]); errs[i] != nil {
				errs[i] = fmt.Errorf("member %s at %s: %w", member.GetName(), member.GetClientURLs()[0], errs[i])
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		log.Error(err, "read the members' Status")
	}
	answers := map[string]answer{}
	for i, s := range statuses {
		if s != nil {
			answers[listed[i]] = read[i]
		}
	}
	return tally(list.Members, statuses), listed, answers, true
}

// status calls Status twice on one connection to url, A then B, through the
// client's own wrappers, which keep Client.Status's call options and retries,
// WaitForReady among them, and returns B with the rule's answer.
// Client.Status would dial a connection per call
// (etcd@v3.7.4 client/v3/maintenance.go#L114-L132, #L204-L215).
func status(ctx context.Context, c *clientv3.Client, url string) (*clientv3.StatusResponse, answer, error) {
	conn, err := c.Dial(url)
	if err != nil {
		return nil, answer{}, err
	}
	defer func() { _ = conn.Close() }()
	m := clientv3.NewMaintenanceFromMaintenanceClient(clientv3.RetryMaintenanceClient(c, conn), c)
	a, err := m.Status(ctx, url)
	if err != nil {
		return nil, answer{}, err
	}
	b, err := m.Status(ctx, url)
	if err != nil {
		return nil, answer{}, err
	}
	revision := b.Header.GetRevision()
	return b, answer{cluster: b.Header.GetClusterId(), term: b.RaftTerm, lower: revision - int64(b.RaftIndex), upper: revision - int64(a.RaftAppliedIndex)}, nil
}

// tell compares each answer with the one kept for its member's name and
// latches the first member, by name, whose data life is new. A member with no
// kept answer is new when it runs under the cluster ID of a restore from
// snapshots while the newest life known is not one: the latch's, else the
// record's, read from the API server after the latch was looked for, since
// the reconciler drops a latch once its record has landed, else a restore
// exactly when spec.restoreFrom is set. Only a restore from snapshots runs
// under that ID, so after a bootstrap or an empty start such an answer is a
// restore since, and after a restore it is that restore's member or a
// learner of it. When that read fails those members keep no answer and the
// next pass reads again.
//
// A name the answered list no longer lists loses its answer, and a fire
// replaces every answer with the pass's own: every member of an etcd that
// began a new life joins it from the one that fired.
func (r *SampleReconciler) tell(ctx context.Context, cp *lodiav1alpha1.ControlPlane, start time.Time, listed []string, answers map[string]answer) {
	key := client.ObjectKeyFromObject(cp)
	kept := r.answers(key, cp.UID)
	maps.DeleteFunc(kept, func(name string, _ answer) bool { return !slices.Contains(listed, name) })
	newest, known := r.Latches.newest(key, cp.UID)
	if !known && slices.ContainsFunc(slices.Collect(maps.Keys(answers)), func(name string) bool {
		_, ok := kept[name]
		return !ok && etcddruid.RestoredFromSnapshots(answers[name].cluster)
	}) {
		newest, known = r.newest(ctx, cp)
	}
	var fired []string
	got := map[string]answer{}
	for name, a := range answers {
		previous, ok := kept[name]
		switch {
		case ok:
			if newLife(previous, a) {
				fired = append(fired, name)
			}
		case etcddruid.RestoredFromSnapshots(a.cluster):
			if !known {
				continue
			}
			if newest != lodiav1alpha1.MemberRestoreFromSnapshots {
				fired = append(fired, name)
			}
		}
		got[name] = a
	}
	if len(fired) == 0 {
		maps.Copy(kept, got)
		r.store(key, cp.UID, kept)
		return
	}
	r.store(key, cp.UID, got)
	member := slices.Min(fired)
	from := lodiav1alpha1.MemberRestoreFromEmpty
	if etcddruid.RestoredFromSnapshots(answers[member].cluster) {
		from = lodiav1alpha1.MemberRestoreFromSnapshots
	}
	r.Latches.tell(key, cp.UID, lodiav1alpha1.MemberRestoreStatus{Member: member, DetectedAt: metav1.NewTime(start).Rfc3339Copy(), From: from})
}

// newest is the newest data life the API server records for cp, false when
// the read fails.
func (r *SampleReconciler) newest(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (lodiav1alpha1.MemberRestoreFrom, bool) {
	current, err := live.Object(ctx, r.Reader, cp)
	if err != nil {
		logf.FromContext(ctx).Error(err, "read the ControlPlane's memberRestore")
		return "", false
	}
	if record := current.Status.MemberRestore; record != (lodiav1alpha1.MemberRestoreStatus{}) {
		return record.From, true
	}
	if cp.Spec.RestoreFrom != (lodiav1alpha1.RestoreFrom{}) {
		return lodiav1alpha1.MemberRestoreFromSnapshots, true
	}
	return "", true
}

// tally takes the members' answers: a member names no leader with 0, a
// learner's leader counts for nothing, and each member lists every alarm
// armed in the etcd among its errors as the alarm's text, memberID:<ID>
// alarm:<type> (etcd@v3.5.27 server/etcdserver/api/v3rpc/maintenance.go#L239-L244).
func tally(members []*etcdserverpb.Member, answers []*clientv3.StatusResponse) EtcdSample {
	var sample EtcdSample
	voting := 0
	for _, member := range members {
		if !member.GetIsLearner() {
			voting++
		}
	}
	named := map[uint64]int{}
	var fields []string
	for _, answer := range answers {
		if answer == nil {
			continue
		}
		if sample.DBSize == nil || answer.DbSize > *sample.DBSize {
			sample.DBSize = &answer.DbSize
		}
		if answer.Leader != 0 && !answer.IsLearner {
			named[answer.Leader]++
		}
		for _, e := range answer.Errors {
			fields = append(fields, strings.Fields(e)...)
		}
	}
	for _, n := range named {
		sample.HasLeader = sample.HasLeader || n > voting/2
	}
	for _, alarm := range []string{lodiav1alpha1.MetricAlarmCorrupt, lodiav1alpha1.MetricAlarmNoSpace} {
		if slices.Contains(fields, "alarm:"+alarm) {
			sample.Alarms = append(sample.Alarms, alarm)
		}
	}
	return sample
}

func (r *SampleReconciler) keep(key types.NamespacedName, uid types.UID, start time.Time, sample EtcdSample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.kept == nil {
		r.kept = map[types.NamespacedName]*kept{}
	}
	k := r.kept[key]
	if k == nil || k.uid != uid {
		k = &kept{uid: uid}
		r.kept[key] = k
	}
	k.previous, k.latest = k.latest, &timed{from: start.Add(sampleTimeout), sample: sample}
}

func (r *SampleReconciler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.kept, key)
}

// answers is a copy of the answers kept for the ControlPlane key names with
// UID uid; those of another UID go, with its latch.
func (r *SampleReconciler) answers(key types.NamespacedName, uid types.UID) map[string]answer {
	r.mu.Lock()
	defer r.mu.Unlock()
	l := r.lives[key]
	if l == nil || l.uid != uid {
		if l != nil {
			r.Latches.forget(key)
		}
		return map[string]answer{}
	}
	return maps.Clone(l.answers)
}

func (r *SampleReconciler) store(key types.NamespacedName, uid types.UID, answers map[string]answer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lives == nil {
		r.lives = map[types.NamespacedName]*lives{}
	}
	r.lives[key] = &lives{uid: uid, answers: answers}
}

// end drops what the rule kept of a ControlPlane gone or being deleted, and
// its latch.
func (r *SampleReconciler) end(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.lives, key)
	r.Latches.forget(key)
}

// Sample is the sample of the etcd of the ControlPlane key names, with UID
// uid, that is served now, and false while there is none.
func (r *SampleReconciler) Sample(key types.NamespacedName, uid types.UID) (EtcdSample, bool) {
	now := r.Clock.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	k := r.kept[key]
	if k == nil || k.uid != uid {
		return EtcdSample{}, false
	}
	for _, s := range []*timed{k.latest, k.previous} {
		if s != nil && !now.Before(s.from) {
			return s.sample, true
		}
	}
	return EtcdSample{}, false
}
