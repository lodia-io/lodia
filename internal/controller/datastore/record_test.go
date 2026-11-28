// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
)

var restoredAt = time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)

func toldAt(member string, at time.Time, from lodiav1alpha1.MemberRestoreFrom) lodiav1alpha1.MemberRestoreStatus {
	return lodiav1alpha1.MemberRestoreStatus{Member: member, DetectedAt: metav1.NewTime(at), From: from}
}

func leftAlone(t *testing.T) string {
	t.Helper()
	ns := namespace(t)
	managerReads.leave(ns)
	return ns
}

func recorder(c client.WithWatch, reader client.Reader, latches *datastore.Latches) *datastore.Reconciler {
	return &datastore.Reconciler{Client: c, Reader: reader, Druid: newDruid(), OperatorNamespace: operatorNamespace, Revision: revision, Clock: clock.RealClock{}, Latches: latches}
}

func pass(t *testing.T, r *datastore.Reconciler, cp *lodiav1alpha1.ControlPlane) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
}

func passOver(t *testing.T, r *datastore.Reconciler, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	if result, err := pass(t, r, cp); err != nil || result.RequeueAfter != 0 {
		t.Fatalf("the pass returned %+v, %v, want neither a requeue nor an error", result, err)
	}
}

func waiting(t *testing.T, ns, name string, r *datastore.Reconciler) *lodiav1alpha1.ControlPlane {
	t.Helper()
	cp := controlPlane(ns, name, "")
	if err := k8s.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	passOver(t, r, cp)
	if c := meta.FindStatusCondition(getControlPlane(t, ns, name).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); c == nil || c.Reason != lodiav1alpha1.ReasonWaitingForTarget {
		t.Fatalf("DatastoreReady is %+v, want WaitingForTarget", c)
	}
	return getControlPlane(t, ns, name)
}

func liveRecord(t *testing.T, cp *lodiav1alpha1.ControlPlane) lodiav1alpha1.MemberRestoreStatus {
	t.Helper()
	return getControlPlane(t, cp.Namespace, cp.Name).Status.MemberRestore
}

func wantRecord(t *testing.T, cp *lodiav1alpha1.ControlPlane, want lodiav1alpha1.MemberRestoreStatus) {
	t.Helper()
	got := liveRecord(t, cp)
	if got == (lodiav1alpha1.MemberRestoreStatus{}) || got.Member != want.Member || !got.DetectedAt.Equal(&want.DetectedAt) || got.From != want.From {
		t.Fatalf("status.memberRestore is %+v, want %+v", got, want)
	}
}

func unchanged(t *testing.T, cp *lodiav1alpha1.ControlPlane, during func()) {
	t.Helper()
	before := getControlPlane(t, cp.Namespace, cp.Name).ResourceVersion
	during()
	if after := getControlPlane(t, cp.Namespace, cp.Name).ResourceVersion; after != before {
		t.Fatalf("the ControlPlane moved from resourceVersion %s to %s, want nothing written", before, after)
	}
}

func tell(l *datastore.Latches, cp *lodiav1alpha1.ControlPlane, told lodiav1alpha1.MemberRestoreStatus) {
	datastore.Tell(l, client.ObjectKeyFromObject(cp), cp.UID, told)
}

func afterRead(during func()) client.WithWatch {
	var once sync.Once
	return interceptor.NewClient(k8s, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		err := c.Get(ctx, key, obj, opts...)
		if _, ok := obj.(*lodiav1alpha1.ControlPlane); ok && err == nil {
			once.Do(during)
		}
		return err
	}})
}

func rewrite(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	current := getControlPlane(t, cp.Namespace, cp.Name)
	condition, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, Message: "another manager's", ObservedGeneration: current.Generation, LastTransitionTime: metav1.Now()})
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(), "kind": "ControlPlane",
		"metadata": map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":   map[string]any{"conditions": []any{condition}},
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner("test/other"), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func wantConflict(t *testing.T, result ctrl.Result, err error) {
	t.Helper()
	if err != nil || result.RequeueAfter != time.Second {
		t.Fatalf("the pass returned %+v, %v, want a requeue after a second and no error", result, err)
	}
}

func TestALatchIsRecordedOnceAndDropped(t *testing.T) {
	t.Parallel()
	ns := leftAlone(t)
	latches := datastore.NewLatches()
	r := recorder(k8s, k8s, latches)
	cp := waiting(t, ns, "once", r)
	told := toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
	tell(latches, cp, told)
	passOver(t, r, cp)
	wantRecord(t, cp, told)
	if got := datastore.Latched(latches, client.ObjectKeyFromObject(cp)); got != nil {
		t.Errorf("the latch %+v stays after its record landed, want it dropped", got)
	}
	unchanged(t, cp, func() { passOver(t, r, cp) })
}

func TestALatchOfAnotherUIDIsNotRecorded(t *testing.T) {
	t.Parallel()
	ns := leftAlone(t)
	latches := datastore.NewLatches()
	r := recorder(k8s, k8s, latches)
	cp := waiting(t, ns, "successor", r)
	told := toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
	datastore.Tell(latches, client.ObjectKeyFromObject(cp), "uid-of-a-predecessor", told)
	unchanged(t, cp, func() { passOver(t, r, cp) })
	if got := datastore.Latched(latches, client.ObjectKeyFromObject(cp)); got == nil || *got != told {
		t.Errorf("the store holds %+v, want the predecessor's latch %+v left", got, told)
	}
}

func TestAPassWhoseCacheLagsAnotherManagersWriteLandsItsRecord(t *testing.T) {
	t.Parallel()
	ns := leftAlone(t)
	latches := datastore.NewLatches()
	cp := waiting(t, ns, "lagged", recorder(k8s, k8s, latches))
	stale := getControlPlane(t, ns, cp.Name)
	rewrite(t, cp)
	told := toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
	tell(latches, cp, told)
	passOver(t, recorder(caching(stale), k8s, latches), cp)
	wantRecord(t, cp, told)
}

func TestASamplerPassSendsNoLatchAReconcilerPassTook(t *testing.T) {
	t.Parallel()
	ns := leftAlone(t)
	latches := datastore.NewLatches()
	cp := waiting(t, ns, "taken", recorder(k8s, k8s, latches))
	tell(latches, cp, toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots))
	failing := interceptor.NewClient(k8s, interceptor.Funcs{SubResourceApply: func(context.Context, client.Client, string, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
		return errors.New("the API server does not answer")
	}})
	if _, err := pass(t, recorder(failing, k8s, latches), cp); err == nil {
		t.Fatal("a pass whose apply failed returned no error")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	read := make(chan struct{})
	go func() {
		defer close(read)
		select {
		case e := <-datastore.Told(latches):
			t.Errorf("a sampler pass sent %s, whose latch a reconciler pass took", client.ObjectKeyFromObject(e.Object))
		case <-ctx.Done():
		}
	}()
	sampler := &datastore.SampleReconciler{Cache: k8s, Reader: k8s, Druid: newDruid(), Clock: clock.RealClock{}, Latches: latches}
	if _, err := sampler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	<-read
}

func TestASamplerPassSendsNoLatchOfAnotherUID(t *testing.T) {
	t.Parallel()
	cp, _ := sampledControlPlane("recreated", "uid-new")
	key := client.ObjectKeyFromObject(cp)
	r, _ := sampler(cp)
	datastore.Tell(r.Latches, key, "uid-old", toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	read := make(chan struct{})
	go func() {
		defer close(read)
		select {
		case e := <-datastore.Told(r.Latches):
			t.Errorf("a sampler pass on UID uid-new sent %s for a latch of UID uid-old", client.ObjectKeyFromObject(e.Object))
		case <-ctx.Done():
		}
	}()
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	<-read
}

func TestALatchIsWrittenPastTheRecordItReplaces(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name  string
		live  lodiav1alpha1.MemberRestoreStatus
		latch lodiav1alpha1.MemberRestoreStatus
		want  lodiav1alpha1.MemberRestoreStatus
	}{
		{"a record stamped later by another replica's clock",
			toldAt("dev-etcd-0", restoredAt.Add(time.Hour), lodiav1alpha1.MemberRestoreFromSnapshots),
			toldAt("dev-etcd-1", restoredAt, lodiav1alpha1.MemberRestoreFromEmpty),
			toldAt("dev-etcd-1", restoredAt.Add(time.Hour+time.Second), lodiav1alpha1.MemberRestoreFromEmpty)},
		{"a latch equal to the record",
			toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots),
			toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots),
			toldAt("dev-etcd-0", restoredAt.Add(time.Second), lodiav1alpha1.MemberRestoreFromSnapshots)},
		{"a latch that differs only in from",
			toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots),
			toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromEmpty),
			toldAt("dev-etcd-0", restoredAt.Add(time.Second), lodiav1alpha1.MemberRestoreFromEmpty)},
		{"a latch after the record",
			toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots),
			toldAt("dev-etcd-0", restoredAt.Add(time.Minute), lodiav1alpha1.MemberRestoreFromSnapshots),
			toldAt("dev-etcd-0", restoredAt.Add(time.Minute), lodiav1alpha1.MemberRestoreFromSnapshots)},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ns := leftAlone(t)
			other, latches := datastore.NewLatches(), datastore.NewLatches()
			cp := waiting(t, ns, "past", recorder(k8s, k8s, other))
			tell(other, cp, c.live)
			passOver(t, recorder(k8s, k8s, other), cp)
			tell(latches, cp, c.latch)
			passOver(t, recorder(k8s, k8s, latches), cp)
			wantRecord(t, cp, c.want)
		})
	}
}

func TestAnApplyThatLandsButFailsIsNotWrittenAgainAndItsLatchGoesAtTheNextPass(t *testing.T) {
	t.Parallel()
	ns := leftAlone(t)
	latches := datastore.NewLatches()
	cp := waiting(t, ns, "landed", recorder(k8s, k8s, latches))
	told := toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
	tell(latches, cp, told)
	failed := errors.New("the answer was lost")
	landing := interceptor.NewClient(k8s, interceptor.Funcs{SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
		if err := c.SubResource(subResource).Apply(ctx, obj, opts...); err != nil {
			return err
		}
		return failed
	}})
	if _, err := pass(t, recorder(landing, k8s, latches), cp); !errors.Is(err, failed) {
		t.Fatalf("the pass returned %v, want the apply's error", err)
	}
	wantRecord(t, cp, told)
	unchanged(t, cp, func() { passOver(t, recorder(k8s, k8s, latches), cp) })
	if got := datastore.Latched(latches, client.ObjectKeyFromObject(cp)); got != nil {
		t.Errorf("the latch %+v stays once its record is read landed, want it dropped", got)
	}
}

func TestARecordThatDidNotLandIsWrittenAtTheNextPass(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name  string
		apply func(t *testing.T, cp *lodiav1alpha1.ControlPlane, latches *datastore.Latches)
	}{
		{"a first record whose apply failed", func(t *testing.T, cp *lodiav1alpha1.ControlPlane, latches *datastore.Latches) {
			failing := interceptor.NewClient(k8s, interceptor.Funcs{SubResourceApply: func(context.Context, client.Client, string, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
				return errors.New("the API server does not answer")
			}})
			if _, err := pass(t, recorder(failing, k8s, latches), cp); err == nil {
				t.Fatal("a pass whose apply failed returned no error")
			}
		}},
		{"a first record whose apply met another manager's write", func(t *testing.T, cp *lodiav1alpha1.ControlPlane, latches *datastore.Latches) {
			r := recorder(k8s, afterRead(func() { rewrite(t, cp) }), latches)
			recorded := events.NewFakeRecorder(10)
			r.Events = recorded
			result, err := pass(t, r, cp)
			wantConflict(t, result, err)
			if len(recorded.Events) > 0 {
				t.Errorf("the pass that conflicted recorded %q, want no event", <-recorded.Events)
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ns := leftAlone(t)
			latches := datastore.NewLatches()
			cp := waiting(t, ns, "unlanded", recorder(k8s, k8s, latches))
			told := toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
			tell(latches, cp, told)
			c.apply(t, cp, latches)
			if got := liveRecord(t, cp); got != (lodiav1alpha1.MemberRestoreStatus{}) {
				t.Fatalf("status.memberRestore is %+v after the apply that did not land, want none", got)
			}
			passOver(t, recorder(k8s, k8s, latches), cp)
			wantRecord(t, cp, told)
		})
	}
}

func TestAConflictAloneOnTheLastApplyOfAFormedTenantRequeuesWithNoErrorAndNoEvent(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	cp := formedControlPlane(t, ns, "conflicted")
	managerReads.leave(ns)
	latches := datastore.NewLatches()
	told := toldAt("conflicted-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
	tell(latches, cp, told)
	r := recorder(k8s, afterRead(func() { rewrite(t, cp) }), latches)
	recorded := events.NewFakeRecorder(10)
	r.Events = recorded
	result, err := pass(t, r, cp)
	wantConflict(t, result, err)
	for len(recorded.Events) > 0 {
		t.Errorf("the pass whose last apply conflicted recorded %q, want no event", <-recorded.Events)
	}
	got := getControlPlane(t, ns, cp.Name)
	if c := meta.FindStatusCondition(got.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); c == nil || c.Message != "another manager's" || got.Status.MemberRestore != (lodiav1alpha1.MemberRestoreStatus{}) {
		t.Fatalf("DatastoreReady is %+v and status.memberRestore %+v after the apply that conflicted, want the other manager's write left as it is and no record", c, got.Status.MemberRestore)
	}
	if _, err := pass(t, recorder(k8s, k8s, latches), cp); err != nil {
		t.Fatal(err)
	}
	wantRecord(t, cp, told)
	if c := meta.FindStatusCondition(getControlPlane(t, ns, cp.Name).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); c == nil || c.Reason != lodiav1alpha1.ReasonMemberRestored {
		t.Errorf("DatastoreReady is %+v after the next pass, want MemberRestored", c)
	}
}

func TestALatchReplacedBetweenTheTakeAndTheBaseIsWrittenPastTheRecordJustApplied(t *testing.T) {
	t.Parallel()
	ns := leftAlone(t)
	latches := datastore.NewLatches()
	cp := waiting(t, ns, "replaced", recorder(k8s, k8s, latches))
	first := toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
	second := toldAt("dev-etcd-0", restoredAt.Add(-time.Minute), lodiav1alpha1.MemberRestoreFromEmpty)
	tell(latches, cp, first)
	passOver(t, recorder(k8s, afterRead(func() { tell(latches, cp, second) }), latches), cp)
	wantRecord(t, cp, first)
	passOver(t, recorder(k8s, k8s, latches), cp)
	wantRecord(t, cp, toldAt("dev-etcd-0", restoredAt.Add(time.Second), lodiav1alpha1.MemberRestoreFromEmpty))
}

func TestALatchReplacedOnceTheRecordLandedStaysAndIsWrittenAtTheNextPass(t *testing.T) {
	t.Parallel()
	ns := leftAlone(t)
	latches := datastore.NewLatches()
	cp := waiting(t, ns, "after", recorder(k8s, k8s, latches))
	first := toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
	second := toldAt("dev-etcd-0", restoredAt.Add(time.Minute), lodiav1alpha1.MemberRestoreFromEmpty)
	tell(latches, cp, first)
	telling := interceptor.NewClient(k8s, interceptor.Funcs{SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
		err := c.SubResource(subResource).Apply(ctx, obj, opts...)
		tell(latches, cp, second)
		return err
	}})
	passOver(t, recorder(telling, k8s, latches), cp)
	wantRecord(t, cp, first)
	passOver(t, recorder(k8s, k8s, latches), cp)
	wantRecord(t, cp, second)
}

func TestAPreviousLeadersApplyDecidedBeforeTheNextLeadersRecordConflicts(t *testing.T) {
	t.Parallel()
	earlier := toldAt("dev-etcd-0", restoredAt.Add(-time.Hour), lodiav1alpha1.MemberRestoreFromSnapshots)
	for _, c := range []struct {
		name   string
		before *lodiav1alpha1.MemberRestoreStatus
		own    *lodiav1alpha1.MemberRestoreStatus
	}{
		{"carrying no record", nil, nil},
		{"carrying the record before", &earlier, nil},
		{"carrying a record of its own, its clock ahead", &earlier, new(toldAt("dev-etcd-0", restoredAt.Add(time.Hour), lodiav1alpha1.MemberRestoreFromEmpty))},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ns := leftAlone(t)
			previous, next := datastore.NewLatches(), datastore.NewLatches()
			cp := waiting(t, ns, "stalled", recorder(k8s, k8s, next))
			if c.before != nil {
				tell(next, cp, *c.before)
				passOver(t, recorder(k8s, k8s, next), cp)
			}
			if c.own != nil {
				tell(previous, cp, *c.own)
			}
			rewrite(t, cp)
			recorded := toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
			result, err := pass(t, recorder(k8s, afterRead(func() {
				tell(next, cp, recorded)
				passOver(t, recorder(k8s, k8s, next), cp)
				rewrite(t, cp)
			}), previous), cp)
			wantConflict(t, result, err)
			wantRecord(t, cp, recorded)
		})
	}
}

func TestALatchTakenBeforeAStallWritesNothingOverTheNextLeadersRecordUntilANewerRestore(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name string
		at   time.Time
	}{{"its clock behind", restoredAt.Add(-time.Hour)}, {"its clock ahead", restoredAt.Add(time.Hour)}} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ns := leftAlone(t)
			previous, next := datastore.NewLatches(), datastore.NewLatches()
			cp := waiting(t, ns, "taken", recorder(k8s, k8s, next))
			tell(previous, cp, toldAt("dev-etcd-0", c.at, lodiav1alpha1.MemberRestoreFromEmpty))
			failing := interceptor.NewClient(k8s, interceptor.Funcs{SubResourceApply: func(context.Context, client.Client, string, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
				return errors.New("the operator stalls")
			}})
			if _, err := pass(t, recorder(failing, k8s, previous), cp); err == nil {
				t.Fatal("the stalled apply returned no error")
			}
			recorded := toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
			tell(next, cp, recorded)
			passOver(t, recorder(k8s, k8s, next), cp)
			unchanged(t, cp, func() { passOver(t, recorder(k8s, k8s, previous), cp) })
			wantRecord(t, cp, recorded)
			if got := datastore.Latched(previous, client.ObjectKeyFromObject(cp)); got == nil {
				t.Fatal("the previous leader's latch went, want it kept")
			}
			newer := toldAt("dev-etcd-0", restoredAt.Add(2*time.Hour), lodiav1alpha1.MemberRestoreFromSnapshots)
			tell(previous, cp, newer)
			passOver(t, recorder(k8s, k8s, previous), cp)
			wantRecord(t, cp, newer)
		})
	}
}

func TestALatchFirstTakenAfterAStallIsWrittenOncePastTheNextLeadersRecord(t *testing.T) {
	t.Parallel()
	ns := leftAlone(t)
	previous, next := datastore.NewLatches(), datastore.NewLatches()
	cp := waiting(t, ns, "woken", recorder(k8s, k8s, next))
	tell(previous, cp, toldAt("dev-etcd-0", restoredAt.Add(-time.Minute), lodiav1alpha1.MemberRestoreFromEmpty))
	recorded := toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
	tell(next, cp, recorded)
	passOver(t, recorder(k8s, k8s, next), cp)
	passOver(t, recorder(k8s, k8s, previous), cp)
	wantRecord(t, cp, toldAt("dev-etcd-0", restoredAt.Add(time.Second), lodiav1alpha1.MemberRestoreFromEmpty))
	unchanged(t, cp, func() {
		for range 3 {
			passOver(t, recorder(k8s, k8s, next), cp)
			passOver(t, recorder(k8s, k8s, previous), cp)
		}
	})
}

func TestAPreviousLeadersFirstTakeBetweenTheNextLeadersReadAndApplyStandsForTheLaterLife(t *testing.T) {
	t.Parallel()
	ns := leftAlone(t)
	previous, next := datastore.NewLatches(), datastore.NewLatches()
	cp := waiting(t, ns, "overtaken", recorder(k8s, k8s, next))
	tell(previous, cp, toldAt("dev-etcd-0", restoredAt.Add(-time.Minute), lodiav1alpha1.MemberRestoreFromSnapshots))
	restored := toldAt("dev-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
	tell(next, cp, restored)
	passOver(t, recorder(k8s, k8s, next), cp)
	empty := toldAt("dev-etcd-0", restoredAt.Add(time.Minute), lodiav1alpha1.MemberRestoreFromEmpty)
	tell(next, cp, empty)
	result, err := pass(t, recorder(k8s, afterRead(func() { passOver(t, recorder(k8s, k8s, previous), cp) }), next), cp)
	wantConflict(t, result, err)
	stands := toldAt("dev-etcd-0", restoredAt.Add(time.Second), lodiav1alpha1.MemberRestoreFromSnapshots)
	wantRecord(t, cp, stands)
	unchanged(t, cp, func() { passOver(t, recorder(k8s, k8s, next), cp) })
	if got := datastore.Latched(next, client.ObjectKeyFromObject(cp)); got == nil || *got != empty {
		t.Errorf("the next leader holds %+v, want its latch of the empty start kept", got)
	}
}

func TestAFinalizerTheCacheLacksStays(t *testing.T) {
	t.Parallel()
	ns := leftAlone(t)
	cp := controlPlane(ns, "kept", "")
	if err := k8s.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	stale := getControlPlane(t, ns, cp.Name)
	held := stale.DeepCopy()
	held.Finalizers = []string{"example.com/keep"}
	if err := k8s.Update(t.Context(), held); err != nil {
		t.Fatal(err)
	}
	result, err := pass(t, recorder(caching(stale), k8s, datastore.NewLatches()), cp)
	if err != nil || result.RequeueAfter != time.Second {
		t.Fatalf("a pass whose cache lacks a finalizer returned %+v, %v, want a requeue after a second", result, err)
	}
	if got := getControlPlane(t, ns, cp.Name).Finalizers; len(got) != 1 || got[0] != "example.com/keep" {
		t.Fatalf("the finalizers are %v, want example.com/keep alone", got)
	}
	passOver(t, recorder(k8s, k8s, datastore.NewLatches()), cp)
	if got := getControlPlane(t, ns, cp.Name).Finalizers; len(got) != 2 || got[0] != "example.com/keep" || got[1] != lodiav1alpha1.DatastoreFinalizer {
		t.Errorf("the finalizers are %v, want example.com/keep and %s", got, lodiav1alpha1.DatastoreFinalizer)
	}
}
