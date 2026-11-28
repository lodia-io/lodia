// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/events"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/etcddruid/druidtest"
	"go.lodia.io/lodia/internal/testenv"
)

func recordForManager(t *testing.T, cp *lodiav1alpha1.ControlPlane, told lodiav1alpha1.MemberRestoreStatus) {
	t.Helper()
	tell(managerLatches, cp, told)
	datastore.Send(t.Context(), managerLatches, cp)
	testenv.Eventually(t, func() error {
		if got := liveRecord(t, cp); got == (lodiav1alpha1.MemberRestoreStatus{}) || !got.DetectedAt.Equal(&told.DetectedAt) {
			return fmt.Errorf("status.memberRestore is %+v, want %+v", got, told)
		}
		return nil
	})
}

func reportRoll(t *testing.T, cp *lodiav1alpha1.ControlPlane, detectedAt metav1.Time, components ...string) {
	t.Helper()
	status := map[string]any{}
	for _, component := range components {
		status[component] = map[string]any{"total": int64(1), "ready": int64(1), "memberRestoreDetectedAt": detectedAt.UTC().Format(time.RFC3339)}
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(), "kind": "ControlPlane",
		"metadata": map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":   map[string]any{"components": status},
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner("test/workloads"), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func warned(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	testenv.Eventually(t, func() error {
		var list eventsv1.EventList
		if err := k8s.List(t.Context(), &list, client.InNamespace(cp.Namespace)); err != nil {
			return err
		}
		for _, e := range list.Items {
			if e.Reason == lodiav1alpha1.ReasonMemberRestored && e.Type == "Warning" && e.ReportingController == "lodia.io/"+datastore.Name && e.Regarding.Name == cp.Name {
				return nil
			}
		}
		return fmt.Errorf("no Warning event %s regarding %s among %d", lodiav1alpha1.ReasonMemberRestored, cp.Name, len(list.Items))
	})
}

func reportedAt(t *testing.T, cp *lodiav1alpha1.ControlPlane) *metav1.Time {
	t.Helper()
	record := liveRecord(t, cp)
	if record == (lodiav1alpha1.MemberRestoreStatus{}) {
		t.Fatal("no status.memberRestore")
	}
	return record.ReportedAt
}

var rolledComponents = []string{"apiServer", "controllerManager", "scheduler"}

func TestARestoredMemberIsReportedUntilItsRollIsDone(t *testing.T) {
	t.Parallel()
	for _, last := range rolledComponents {
		t.Run(last+" last", func(t *testing.T) {
			t.Parallel()
			ns := namespace(t)
			cp := formedControlPlane(t, ns, "rolling")
			told := toldAt("rolling-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
			recordForManager(t, cp, told)
			waitCondition(t, ns, cp.Name, metav1.ConditionTrue, lodiav1alpha1.ReasonMemberRestored,
				"member rolling-etcd-0 was found restored from the store's snapshots at 2026-11-26T10:00:00Z: the writes etcd acknowledged after the last delta snapshot are lost; Lodia rolls the API server, then the controller manager and the scheduler")
			warned(t, cp)
			if reportedAt(t, cp) == nil {
				t.Error("MemberRestored is reported and status.memberRestore.reportedAt absent")
			}
			if got := datastore.Latched(managerLatches, client.ObjectKeyFromObject(cp)); got != nil {
				t.Errorf("the latch %+v stays once its record has landed", got)
			}
			reportRoll(t, cp, told.DetectedAt, slices.DeleteFunc(slices.Clone(rolledComponents), func(c string) bool { return c == last })...)
			settle(t, ns, cp.Name)
			waitCondition(t, ns, cp.Name, metav1.ConditionTrue, lodiav1alpha1.ReasonMemberRestored, "member rolling-etcd-0")
			reportRoll(t, cp, told.DetectedAt, rolledComponents...)
			waitCondition(t, ns, cp.Name, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 1 of 1 members ready")
			if _, err := pass(t, recorder(k8s, k8s, datastore.NewLatches()), cp); err != nil || liveRecord(t, cp) == (lodiav1alpha1.MemberRestoreStatus{}) {
				t.Errorf("a restarted reconciler's pass returned %v and left status.memberRestore %+v, want it kept", err, liveRecord(t, cp))
			}
		})
	}
}

func TestAnEmptyStartIsReportedWithEveryWriteLost(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	cp := formedControlPlane(t, ns, "emptied")
	recordForManager(t, cp, toldAt("emptied-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromEmpty))
	waitCondition(t, ns, cp.Name, metav1.ConditionTrue, lodiav1alpha1.ReasonMemberRestored,
		"member emptied-etcd-0 was found started empty at 2026-11-26T10:00:00Z: every write is lost and the tenant's cluster starts again from nothing; Lodia rolls the API server, then the controller manager and the scheduler")
}

func TestARestoreToldWhileQuorumIsLostIsReportedByTheFirstTrue(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	cp := formedControlPlane(t, ns, "lost")
	setEtcdStatus(t, ns, cp.Name, druidtest.Members(1, 0))
	waitCondition(t, ns, cp.Name, metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost, "0 of 1 members ready")
	recordForManager(t, cp, toldAt("lost-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots))
	waitCondition(t, ns, cp.Name, metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost, "0 of 1 members ready")
	if got := reportedAt(t, cp); got != nil {
		t.Errorf("a record told while quorum is lost is reported at %s", got)
	}
	setEtcdStatus(t, ns, cp.Name)
	waitCondition(t, ns, cp.Name, metav1.ConditionTrue, lodiav1alpha1.ReasonMemberRestored, "member lost-etcd-0")
	warned(t, cp)
}

func TestAHeldClassKeepsItsReasonAndTheRestoreIsReportedOnceTheHoldEnds(t *testing.T) {
	t.Parallel()
	ns, credentialsNamespace := namespace(t), namespace(t)
	const name = "heldrestore"
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass("heldrestore", credentialsNamespace))
	create(t, controlPlane(ns, name, "heldrestore"))
	writeGeneration(t, ns, name, 1)
	waitEtcd(t, ns, name, namesGeneration(1))
	readyOn(t, ns, name, 1)
	if err := k8s.Delete(t.Context(), credentials(credentialsNamespace)); err != nil {
		t.Fatal(err)
	}
	settle(t, ns, name)
	waitCondition(t, ns, name, metav1.ConditionTrue, lodiav1alpha1.ReasonSecretNotFound, "is held: its credentials Secret")
	cp := getControlPlane(t, ns, name)
	told := toldAt("heldrestore-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
	recordForManager(t, cp, told)
	reportRoll(t, cp, told.DetectedAt, rolledComponents...)
	settle(t, ns, name)
	waitCondition(t, ns, name, metav1.ConditionTrue, lodiav1alpha1.ReasonSecretNotFound, "is held: its credentials Secret")
	if got := reportedAt(t, cp); got != nil {
		t.Errorf("a record told while the class is held is reported at %s", got)
	}
	create(t, credentials(credentialsNamespace))
	settle(t, ns, name)
	waitCondition(t, ns, name, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum, 1 of 1 members ready")
	if reportedAt(t, cp) == nil {
		t.Error("a restore whose roll ended during the hold has no status.memberRestore.reportedAt once the hold ends")
	}
	warned(t, cp)
}

func TestARecordIsCarriedThroughPassesWithoutAnEtcd(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	create(t, controlPlane(ns, "classless", "missing"))
	waitCondition(t, ns, "classless", metav1.ConditionFalse, lodiav1alpha1.ReasonDatastoreClassNotFound, "DatastoreClass missing does not exist")
	cp := getControlPlane(t, ns, "classless")
	told := toldAt("classless-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
	recordForManager(t, cp, told)
	settle(t, ns, "classless")
	waitCondition(t, ns, "classless", metav1.ConditionFalse, lodiav1alpha1.ReasonDatastoreClassNotFound, "DatastoreClass missing does not exist")
	wantRecord(t, cp, told)
}

func TestAPassWhoseCacheLacksTheRecordCarriesItAndReportsIt(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	cp := formedControlPlane(t, ns, "lagging")
	managerReads.leave(ns)
	before := getControlPlane(t, ns, cp.Name)
	previous := datastore.NewLatches()
	tell(previous, cp, toldAt("lagging-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots))
	if _, err := pass(t, recorder(k8s, k8s, previous), cp); err != nil {
		t.Fatal(err)
	}
	recorded := getControlPlane(t, ns, cp.Name)
	if c := meta.FindStatusCondition(recorded.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); c == nil || c.Reason != lodiav1alpha1.ReasonMemberRestored || recorded.Status.MemberRestore.ReportedAt == nil {
		t.Fatalf("DatastoreReady is %+v and status.memberRestore %+v after the previous leader's record, want MemberRestored and reportedAt", c, recorded.Status.MemberRestore)
	}
	rewrite(t, cp)
	if _, err := pass(t, recorder(caching(before), k8s, datastore.NewLatches()), cp); err != nil {
		t.Fatal(err)
	}
	got := getControlPlane(t, ns, cp.Name)
	if c := meta.FindStatusCondition(got.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); c == nil || c.Reason != lodiav1alpha1.ReasonMemberRestored {
		t.Errorf("DatastoreReady is %+v after a pass whose cache lacks the record, want MemberRestored", c)
	}
	if !equality.Semantic.DeepEqual(got.Status.MemberRestore, recorded.Status.MemberRestore) {
		t.Errorf("status.memberRestore is %+v after a pass whose cache lacks it, want %+v", got.Status.MemberRestore, recorded.Status.MemberRestore)
	}
}

func TestANextLeaderWhoseCacheLagsTheReportsOfAFinishedRollKeepsReady(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	cp := formedControlPlane(t, ns, "relapse")
	managerReads.leave(ns)
	latches := datastore.NewLatches()
	told := toldAt("relapse-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots)
	tell(latches, cp, told)
	if _, err := pass(t, recorder(k8s, k8s, latches), cp); err != nil {
		t.Fatal(err)
	}
	stale := getControlPlane(t, ns, cp.Name)
	if c := meta.FindStatusCondition(stale.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); c == nil || c.Reason != lodiav1alpha1.ReasonMemberRestored || stale.Status.MemberRestore.ReportedAt == nil {
		t.Fatalf("after the record DatastoreReady is %+v and status.memberRestore %+v, want MemberRestored and reportedAt", c, stale.Status.MemberRestore)
	}
	reportRoll(t, cp, told.DetectedAt, rolledComponents...)
	if _, err := pass(t, recorder(k8s, k8s, latches), cp); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(getControlPlane(t, ns, cp.Name).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); c == nil || c.Reason != lodiav1alpha1.ReasonReady {
		t.Fatalf("after the three reports DatastoreReady is %+v, want Ready", c)
	}
	next := recorder(caching(stale), k8s, datastore.NewLatches())
	recorded := events.NewFakeRecorder(10)
	next.Events = recorded
	if _, err := pass(t, next, cp); err != nil {
		t.Fatal(err)
	}
	got := getControlPlane(t, ns, cp.Name)
	if c := meta.FindStatusCondition(got.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); c == nil || c.Reason != lodiav1alpha1.ReasonReady {
		t.Errorf("a next leader whose cache lags the three reports wrote DatastoreReady %+v, want Ready, the API server holding the reports %+v at detectedAt %s", c, got.Status.Components, got.Status.MemberRestore.DetectedAt)
	}
	for len(recorded.Events) > 0 {
		t.Errorf("a next leader whose cache lags the three reports recorded %q", <-recorded.Events)
	}
}

func TestALaterPassBeforeTheRollKeepsReportedAtAndWritesNothing(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	cp := formedControlPlane(t, ns, "reported")
	managerReads.leave(ns)
	latches := datastore.NewLatches()
	tell(latches, cp, toldAt("reported-etcd-0", restoredAt, lodiav1alpha1.MemberRestoreFromSnapshots))
	clk := clocktesting.NewFakeClock(time.Now())
	r := recorder(k8s, k8s, latches)
	r.Clock = clk
	if _, err := pass(t, r, cp); err != nil {
		t.Fatal(err)
	}
	first := getControlPlane(t, ns, cp.Name)
	if c := meta.FindStatusCondition(first.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); c == nil || c.Reason != lodiav1alpha1.ReasonMemberRestored || first.Status.MemberRestore.ReportedAt == nil {
		t.Fatalf("DatastoreReady is %+v and status.memberRestore %+v, want MemberRestored and reportedAt", c, first.Status.MemberRestore)
	}
	clk.Step(time.Minute)
	if _, err := pass(t, r, cp); err != nil {
		t.Fatal(err)
	}
	got := getControlPlane(t, ns, cp.Name)
	if !got.Status.MemberRestore.ReportedAt.Equal(first.Status.MemberRestore.ReportedAt) || got.ResourceVersion != first.ResourceVersion {
		t.Errorf("a later pass before the roll moved reportedAt from %s to %s and the resourceVersion from %s to %s, want both kept", first.Status.MemberRestore.ReportedAt, got.Status.MemberRestore.ReportedAt, first.ResourceVersion, got.ResourceVersion)
	}
}
