// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
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

// SampleReconciler samples the etcd of every dedicated ControlPlane whose
// Etcd is formed, once a minute, for Lodia's metrics; it writes nothing.
type SampleReconciler struct {
	// Cache reads the ControlPlanes and their Etcds.
	Cache client.Reader
	// EtcdClient connects to the etcd of an Etcd; the caller closes it.
	EtcdClient func(ctx context.Context, etcd *druidv1alpha1.Etcd) (*clientv3.Client, error)
	// Clock times the passes and serves their samples.
	Clock clock.PassiveClock

	mu   sync.Mutex
	kept map[types.NamespacedName]*kept
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
func (r *SampleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	start := r.Clock.Now()
	again := func() (ctrl.Result, error) {
		return ctrl.Result{RequeueAfter: sampleEvery - r.Clock.Since(start)}, nil
	}
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Cache.Get(ctx, req.NamespacedName, cp); err != nil {
		r.forget(req.NamespacedName)
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logf.FromContext(ctx).Error(err, "read the ControlPlane")
		return again()
	}
	etcd, err := r.sampled(ctx, cp)
	if err != nil {
		logf.FromContext(ctx).Error(err, "read the Etcd")
	}
	if etcd == nil {
		r.forget(req.NamespacedName)
		return again()
	}
	sample, ok := r.sample(ctx, etcd)
	if !ok {
		r.forget(req.NamespacedName)
		return again()
	}
	r.keep(req.NamespacedName, cp.UID, start, sample)
	return again()
}

// sampled is the Etcd of cp to sample: cp's own and formed, of a dedicated
// etcd neither paused nor being deleted; nil otherwise. An etcd that never
// had a ready member is on its way, a new one or a restore's, and has no
// leader to page about.
func (r *SampleReconciler) sampled(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*druidv1alpha1.Etcd, error) {
	if !cp.DeletionTimestamp.IsZero() || cp.Spec.Paused || cp.Spec.Datastore.Dedicated == nil {
		return nil, nil
	}
	etcd := &druidv1alpha1.Etcd{}
	if err := r.Cache.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: lodiav1alpha1.EtcdName(cp.Name)}, etcd); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(etcd, cp) || !formed(etcd) {
		return nil, nil
	}
	return etcd, nil
}

// sample lists etcd's members and reads each one's Status at once, and is
// false when there is no client to do so with in time: a failed or slow read
// of the client certificate says nothing about the etcd.
//
// The list is serializable, which a member answers from its own state
// (etcd@v3.5.27 server/etcdserver/api/v3rpc/member.go#L90-L98), so an etcd
// without a leader still gives its size and alarms. The alarms come from each
// member's Status, not from AlarmList, which goes through raft
// (etcd@v3.5.27 server/etcdserver/v3_server.go#L491-L497).
func (r *SampleReconciler) sample(ctx context.Context, etcd *druidv1alpha1.Etcd) (EtcdSample, bool) {
	log := logf.FromContext(ctx).WithValues("etcd", etcd.Name)
	c, err := r.EtcdClient(ctx, etcd)
	if err != nil {
		log.Error(err, "connect to the etcd")
		return EtcdSample{}, false
	}
	defer func() { _ = c.Close() }()
	deadline, _ := ctx.Deadline()
	if left := time.Until(deadline); left < etcdCallsTime {
		log.Error(fmt.Errorf("%s of the pass left, below the %s its calls to the etcd need", left, etcdCallsTime), "connect to the etcd")
		return EtcdSample{}, false
	}
	list, err := c.MemberList(ctx, clientv3.WithSerializable())
	if err != nil {
		log.Error(err, "list the etcd's members")
		return EtcdSample{}, true
	}
	answers := make([]*clientv3.StatusResponse, len(list.Members))
	errs := make([]error, len(list.Members))
	var wg sync.WaitGroup
	for i, member := range list.Members {
		// A member added and not started yet has no client URL.
		if len(member.GetClientURLs()) == 0 {
			continue
		}
		wg.Go(func() {
			if answers[i], errs[i] = c.Status(ctx, member.GetClientURLs()[0]); errs[i] != nil {
				errs[i] = fmt.Errorf("member %s at %s: %w", member.GetName(), member.GetClientURLs()[0], errs[i])
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		log.Error(err, "read the members' Status")
	}
	return tally(list.Members, answers), true
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
