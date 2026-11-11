// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func patches(f *facts) {
	if f.cp.Spec.Maintenance == nil {
		f.cp.Spec.Maintenance = &lodiav1alpha1.Maintenance{}
	}
	f.cp.Spec.Maintenance.AutoUpdate = &lodiav1alpha1.AutoUpdate{KubernetesPatch: true}
}

func specAt(version string) func(*facts) { return func(f *facts) { f.cp.Spec.Version = version } }

func targetAt(version string) func(*facts) {
	return func(f *facts) { f.cp.Status.Target.Version = version }
}

func runsAt(version string) func(*facts) {
	return func(f *facts) {
		for _, c := range []*lodiav1alpha1.ComponentStatus{f.cp.Status.Components.APIServer, f.cp.Status.Components.ControllerManager, f.cp.Status.Components.Scheduler} {
			c.Version = version
		}
		f.cp.Status.Addons.Version = version
	}
}

func upgrading(step lodiav1alpha1.UpgradeStep) func(*facts) {
	return func(f *facts) {
		f.cp.Status.Kubernetes = &lodiav1alpha1.KubernetesStatus{Upgrade: &lodiav1alpha1.UpgradeStatus{Step: step, StartedAt: started}}
	}
}

func edited(f *facts) {
	f.cp.Generation = 4
	for i := range f.cp.Status.Conditions {
		f.cp.Status.Conditions[i].ObservedGeneration = 3
	}
}

type versioned struct {
	version string
	step    lodiav1alpha1.UpgradeStep
	since   time.Time
}

func expectVersion(t *testing.T, d decision, want versioned) {
	t.Helper()
	if got := d.status.Target; got == nil || got.Version != want.version {
		t.Errorf("target is %+v, want version %s", got, want.version)
	}
	var upgrade *lodiav1alpha1.UpgradeStatus
	if k := d.status.Kubernetes; k != nil {
		upgrade = k.Upgrade
	}
	switch {
	case want.step == "" && upgrade != nil:
		t.Errorf("upgrade is %+v, want none", upgrade)
	case want.step != "" && (upgrade == nil || upgrade.Step != want.step || !upgrade.StartedAt.Time.Equal(want.since)):
		t.Errorf("upgrade is %+v, want step %s since %s", upgrade, want.step, want.since)
	}
}

func TestAPatchUpdateIsGrantedByItsOwnRules(t *testing.T) {
	nextStart := time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)
	nowSecond := now.Rfc3339Copy().Time
	stays := versioned{version: "v1.37.1"}
	moves := versioned{version: "v1.37.2", step: lodiav1alpha1.UpgradeStepAPIServer, since: nowSecond}
	waitsForTheWindow := granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, message: "Kubernetes v1.37.2 waits for the maintenance window 13:00-14:00 UTC, which opens at 2026-11-09T13:00:00Z", scheduledFor: &nextStart, requeue: 30 * time.Minute}
	unreachable := condition(metav1.Condition{Type: lodiav1alpha1.ConditionReachable, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, Message: "api: refused"})
	refused := condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, Message: "the tenant refuses"})
	for _, c := range []struct {
		name    string
		f       facts
		want    granted
		version versioned
	}{
		{"pending alone, it waits for a closed window", held(running(revisionA), closedWindow, patches), waitsForTheWindow, stays},
		{"at once with no target yet, set at latest with no upgrade", held(running(revisionA), closedWindow, patches, func(f *facts) { f.cp.Status.Target = nil }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonRollingOut, message: "kube-apiserver: reports v1.37.1, not v1.37.2"}, versioned{version: "v1.37.2"}},
		{"at once on a token, the owner's: no slot", held(running(revisionA), closedWindow, patches, requested("t1")),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonUpgrading, message: "upgrading to v1.37.2, step APIServer", lastHandled: "t1"}, moves},
		{"at once in a slot, which it keeps", held(running(revisionA), closedWindow, patches, slotted(lodiav1alpha1.ReasonMaintaining, 3)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, message: "the change Lodia started rolls: kube-apiserver: reports v1.37.1, not v1.37.2", slot: true}, moves},
		{"in a slot an upgrade under way keeps its startedAt", held(running(revisionA), closedWindow, patches, slotted(lodiav1alpha1.ReasonMaintaining, 3), upgrading(lodiav1alpha1.UpgradeStepAddons), func(f *facts) { f.cp.Status.Addons.Version = "v1.37.0" }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, slot: true}, versioned{version: "v1.37.2", step: lodiav1alpha1.UpgradeStepAPIServer, since: started.Time}},
		{"not at once on an owner's edit not taken up", held(running(revisionA), closedWindow, patches, progressingAs(lodiav1alpha1.ReasonIdle, 2)), waitsForTheWindow, stays},
		{"not at once during an owner's upgrade", held(running(revisionA), closedWindow, patches, upgrading(lodiav1alpha1.UpgradeStepAddons), func(f *facts) { f.cp.Status.Addons.Version = "v1.37.0" }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonUpgrading, scheduledFor: &nextStart, requeue: 30 * time.Minute}, versioned{version: "v1.37.1", step: lodiav1alpha1.UpgradeStepAddons, since: started.Time}},
		{"not at once on addons never applied", held(running(revisionA), closedWindow, patches, func(f *facts) { f.cp.Status.Addons = nil },
			condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForAccess})),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonRollingOut, scheduledFor: &nextStart, requeue: 30 * time.Minute}, stays},
		{"not at once on APIServerReady WaitingForTarget", held(running(revisionA), closedWindow, patches,
			condition(metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForTarget})), waitsForTheWindow, stays},
		{"not at once on a restored tenant whose controller manager is not released", held(running(revisionA), closedWindow, patches, func(f *facts) {
			f.cp.Spec.RestoreFrom = &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: "a-1"}}
			f.cp.Status.Components.ControllerManager.Total = 0
		}), waitsForTheWindow, stays},
		{"not while Failed stands", held(running(revisionA), patches, refused, upgrading(lodiav1alpha1.UpgradeStepAddons), func(f *facts) { f.cp.Status.Addons.Version = "v1.37.0" }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonFailed, message: "the tenant refuses"}, versioned{version: "v1.37.1", step: lodiav1alpha1.UpgradeStepAddons, since: started.Time}},
		{"not while TimeZoneUnsupported stands, with no scheduledFor", held(running(revisionA), inWindow("12:00", "13:00", "Mars/Olympus_Mons"), patches),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonTimeZoneUnsupported}, stays},
		{"not while the tenant is not Ready", held(running(revisionA), patches, unreachable),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonExternalProbeFailed, message: "Reachable: api: refused"}, stays},
		{"not with no slot free", held(running(revisionA), patches, fleet(holding("a", "a")), func(f *facts) { f.limit = 1 }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, message: "Kubernetes v1.37.2 waits for one of the fleet's 1 slots", scheduledFor: &nowSecond, requeue: fleetRequeue}, stays},
		{"in a free slot with the revision, in one apply", held(inWindow("12:00", "13:00", ""), patches),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, message: "the change Lodia started rolls: kube-apiserver: reports v1.37.1, not v1.37.2", slot: true, takesSlot: true}, moves},
		{"both wait for a closed window, named in one message", held(closedWindow, patches),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, message: "operator revision " + revisionB + " and Kubernetes v1.37.2 wait for the maintenance window", scheduledFor: &nextStart, requeue: 30 * time.Minute}, stays},
		{"none that would move the target below what it is", held(running(revisionA), patches, targetAt("v1.37.3"), runsAt("v1.37.3")),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}, versioned{version: "v1.37.3"}},
		{"none at latest", held(running(revisionA), patches, targetAt("v1.37.2"), runsAt("v1.37.2")),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}, versioned{version: "v1.37.2"}},
		{"none with patch updates off", held(running(revisionA), closedWindow),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}, stays},
		{"none during a minor step", held(running(revisionA), closedWindow, patches, specAt("v1.37.1"), targetAt("v1.36.5"), runsAt("v1.36.5"), upgrading(lodiav1alpha1.UpgradeStepBackup)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonUpgrading}, versioned{version: "v1.36.5", step: lodiav1alpha1.UpgradeStepBackup, since: started.Time}},
		{"an owner's edit up moves the target at once", held(running(revisionA), closedWindow, patches, specAt("v1.37.3"), edited),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonLatestGenerationNotObserved}, versioned{version: "v1.37.3", step: lodiav1alpha1.UpgradeStepAPIServer, since: nowSecond}},
		{"an owner's edit to latest moves the target at once", held(running(revisionA), closedWindow, patches, specAt("v1.37.2"), edited),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonLatestGenerationNotObserved}, moves},
		{"an owner's edit down does not move the target below a patch update", held(running(revisionA), closedWindow, patches, specAt("v1.37.0"), targetAt("v1.37.2"), runsAt("v1.37.2")),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}, versioned{version: "v1.37.2"}},
		{"an owner's edit down below the target leaves the patch pending", held(running(revisionA), closedWindow, patches, specAt("v1.37.0")), waitsForTheWindow, stays},
		{"turning patch updates off returns to spec.version at once", held(running(revisionA), closedWindow, targetAt("v1.37.2"), runsAt("v1.37.2")),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonUpgrading}, versioned{version: "v1.37.1", step: lodiav1alpha1.UpgradeStepAPIServer, since: nowSecond}},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectVersion(t, expectGrant(t, c.f, c.want), c.version)
		})
	}
}

func TestANewControlPlaneWithPatchUpdatesStartsAtLatest(t *testing.T) {
	w := world{spec: "v1.37.0", reports: at("")}
	f := w.facts()
	patches(&f)
	d := decide(f)
	expectVersion(t, d, versioned{version: "v1.37.2"})
	f.cp.Spec.Maintenance = nil
	expectVersion(t, decide(f), versioned{version: "v1.37.0"})
}

func TestANewPatchInTheTableRollsWithTheReleaseThatAddsItInOneSlot(t *testing.T) {
	f := held(inWindow("12:00", "13:00", ""), patches)
	d := expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, takesSlot: true})
	expectVersion(t, d, versioned{version: "v1.37.2", step: lodiav1alpha1.UpgradeStepAPIServer, since: now.Rfc3339Copy().Time})
	adopt(f.cp, d)
	f.now = present.Add(time.Minute)
	expectVersion(t, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, message: "kube-apiserver: reports v1.37.1, not v1.37.2"}),
		versioned{version: "v1.37.2", step: lodiav1alpha1.UpgradeStepAPIServer, since: now.Rfc3339Copy().Time})
	reportsAll(revisionB)(&f)
	runsAt("v1.37.2")(&f)
	d = expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, settlingSince: new(f.now), requeue: settling})
	expectVersion(t, d, versioned{version: "v1.37.2"})
	adopt(f.cp, d)
	f.now = f.now.Add(settling)
	expectVersion(t, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonIdle}), versioned{version: "v1.37.2"})
}

const (
	sharedUID   = "11111111-0000-0000-0000-000000000001"
	recreateUID = "11111111-0000-0000-0000-000000000002"
)

func shared(g int64) lodiav1alpha1.DatastoreClassGeneration {
	if g == 0 {
		return noClass
	}
	return lodiav1alpha1.DatastoreClassGeneration{UID: sharedUID, Generation: g}
}

func again(g int64) lodiav1alpha1.DatastoreClassGeneration {
	return lodiav1alpha1.DatastoreClassGeneration{UID: recreateUID, Generation: g}
}

func onClass(running, target, class int64) func(*facts) {
	return onClasses(shared(running), shared(target), shared(class))
}

func onClasses(running, target, class lodiav1alpha1.DatastoreClassGeneration) func(*facts) {
	return func(f *facts) {
		f.cp.Status.Datastore.Class = &lodiav1alpha1.DatastoreClassStatus{Name: "shared", DatastoreClassGeneration: running}
		if f.cp.Status.Target != nil {
			f.cp.Status.Target.DatastoreClass = nil
			if target != noClass {
				f.cp.Status.Target.DatastoreClass = &target
			}
		}
		f.class = class
	}
}

func expectClass(t *testing.T, d decision, want lodiav1alpha1.DatastoreClassGeneration) {
	t.Helper()
	if got := classTargetOf(&lodiav1alpha1.ControlPlane{Status: d.status}); got != want {
		t.Errorf("target is %+v, want datastoreClass %+v", d.status.Target, want)
	}
}

func TestAClassEditIsGrantedByItsOwnRules(t *testing.T) {
	nextStart := time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)
	nowSecond := now.Rfc3339Copy().Time
	waitsForTheWindow := granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, message: "DatastoreClass shared generation 5 waits for the maintenance window 13:00-14:00 UTC, which opens at 2026-11-09T13:00:00Z", scheduledFor: &nextStart, requeue: 30 * time.Minute}
	refusedEtcd := condition(metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, Message: "the API server refuses Etcd dev-etcd"})
	for _, c := range []struct {
		name  string
		f     facts
		want  granted
		class int64
	}{
		{"pending alone, it waits for a closed window", held(running(revisionA), closedWindow, onClass(4, 4, 5)), waitsForTheWindow, 4},
		{"at once on a token, the owner's: no slot", held(running(revisionA), closedWindow, onClass(4, 4, 5), requested("t1")),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonRollingOut, message: "etcd: runs DatastoreClass shared generation 4, not 5", lastHandled: "t1"}, 5},
		{"at once in a slot, which it keeps", held(running(revisionA), closedWindow, onClass(4, 4, 5), slotted(lodiav1alpha1.ReasonMaintaining, 3)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, message: "the change Lodia started rolls: etcd: runs DatastoreClass shared generation 4, not 5", slot: true}, 5},
		{"not at once with no target yet, which takes what the Etcd runs", held(running(revisionA), closedWindow, onClass(4, 0, 5)), waitsForTheWindow, 4},
		{"not at once on an owner's edit not taken up", held(running(revisionA), closedWindow, onClass(4, 4, 5), progressingAs(lodiav1alpha1.ReasonIdle, 2)), waitsForTheWindow, 4},
		{"not at once during an owner's upgrade", held(running(revisionA), closedWindow, onClass(4, 4, 5), upgrading(lodiav1alpha1.UpgradeStepAddons), func(f *facts) { f.cp.Status.Addons.Version = "v1.37.0" }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonUpgrading, scheduledFor: &nextStart, requeue: 30 * time.Minute}, 4},
		{"not at once on addons never applied", held(running(revisionA), closedWindow, onClass(4, 4, 5), func(f *facts) { f.cp.Status.Addons = nil },
			condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForAccess})),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonRollingOut, scheduledFor: &nextStart, requeue: 30 * time.Minute}, 4},
		{"not at once on APIServerReady WaitingForTarget", held(running(revisionA), closedWindow, onClass(4, 4, 5),
			condition(metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForTarget})), waitsForTheWindow, 4},
		{"not at once on a restored tenant whose controller manager is not released", held(running(revisionA), closedWindow, onClass(4, 4, 5), func(f *facts) {
			f.cp.Spec.RestoreFrom = &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: "a-1"}}
			f.cp.Status.Components.ControllerManager.Total = 0
		}), waitsForTheWindow, 4},
		{"not with no slot free", held(running(revisionA), onClass(4, 4, 5), fleet(holding("a", "a")), func(f *facts) { f.limit = 1 }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, message: "DatastoreClass shared generation 5 waits for one of the fleet's 1 slots", scheduledFor: &nowSecond, requeue: fleetRequeue}, 4},
		{"in a free slot", held(running(revisionA), onClass(4, 4, 5)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, message: "the change Lodia started rolls: etcd: runs DatastoreClass shared generation 4, not 5", slot: true, takesSlot: true}, 5},
		{"an absent target takes what the Etcd runs at once", held(running(revisionA), closedWindow, onClass(4, 0, 4)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}, 4},
		{"so does a target the class is not at, once it is at what the Etcd runs", held(running(revisionA), closedWindow, onClass(7, 5, 7)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}, 7},
		{"a class that is gone is not pending", held(running(revisionA), closedWindow, onClass(4, 4, 0)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}, 4},
		{"a class gone before the Etcd took its granted edit leaves the target at what the Etcd runs", held(running(revisionA), closedWindow, onClass(4, 5, 0), slotted(lodiav1alpha1.ReasonMaintaining, 3)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, message: "keeps its slot of the fleet until", slot: true, settlingSince: &nowSecond, requeue: settling}, 4},
		{"an Etcd without a class reported keeps the target", held(running(revisionA), closedWindow, func(f *facts) { f.cp.Status.Target.DatastoreClass = new(shared(4)) }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}, 4},
		{"the owner's edit refused is Failed", held(running(revisionA), onClass(4, 5, 5), refusedEtcd),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonFailed, message: "DatastoreReady: Refused: the API server refuses Etcd dev-etcd"}, 5},
		{"a standing refusal once the Etcd runs the target is Ready's to report", held(running(revisionA), onClass(5, 5, 5), refusedEtcd),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}, 5},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectClass(t, expectGrant(t, c.f, c.want), shared(c.class))
		})
	}
}

func TestEveryKindPendingIsGrantedInOneSlot(t *testing.T) {
	nextStart := time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)
	waits := held(closedWindow, patches, onClass(4, 4, 5))
	d := expectGrant(t, waits, granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow,
		message: "operator revision " + revisionB + ", Kubernetes v1.37.2 and DatastoreClass shared generation 5 wait for the maintenance window", scheduledFor: &nextStart, requeue: 30 * time.Minute})
	expectVersion(t, d, versioned{version: "v1.37.1"})
	expectClass(t, d, shared(4))

	d = expectGrant(t, held(inWindow("12:00", "13:00", ""), patches, onClass(4, 4, 5)), granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, takesSlot: true,
		message: "the change Lodia started rolls: kube-apiserver: reports v1.37.1, not v1.37.2"})
	expectVersion(t, d, versioned{version: "v1.37.2", step: lodiav1alpha1.UpgradeStepAPIServer, since: now.Rfc3339Copy().Time})
	expectClass(t, d, shared(5))
}

func TestAGrantOfAnotherKindKeepsTheClassTarget(t *testing.T) {
	for _, c := range []struct {
		name string
		f    facts
		want granted
	}{
		{"in a free slot", held(inWindow("12:00", "13:00", ""), onClass(4, 4, 4)),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, takesSlot: true}},
		{"on a token", held(closedWindow, onClass(4, 4, 4), requested("t1")),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut, lastHandled: "t1"}},
		{"in a slot it holds", held(closedWindow, onClass(4, 4, 4), slotted(lodiav1alpha1.ReasonMaintaining, 3)),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectClass(t, expectGrant(t, c.f, c.want), shared(4))
		})
	}
}

func TestAClassEditInASlotRollsSettlesAndFails(t *testing.T) {
	nowSecond := now.Rfc3339Copy().Time
	tenSecondsAgo := present.Add(-10 * time.Second)
	inSlot := slotted(lodiav1alpha1.ReasonMaintaining, 3)
	notReady := []func(*facts){
		condition(metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonQuorumLost, Message: "Etcd dev-etcd has 0 of 1 members ready"}),
		condition(metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonQuorumLost, ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(present.Add(-time.Hour))}),
	}
	for _, c := range []struct {
		name  string
		f     facts
		want  granted
		class int64
	}{
		{"rolling while the Etcd runs another generation", held(running(revisionA), inSlot, onClass(4, 5, 5)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, slot: true, message: "the change Lodia started rolls: etcd: runs DatastoreClass shared generation 4, not 5"}, 5},
		{"rolled out once it runs the target: settlingSince now, a pass at S", held(running(revisionA), inSlot, onClass(5, 5, 5)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, slot: true, settlingSince: &nowSecond, requeue: settling}, 5},
		{"Idle after S with Ready True", held(running(revisionA), inSlot, onClass(5, 5, 5), settlingAt(present.Add(-settling))),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}, 5},
		{"MaintenanceFailed after S with Ready not True", held(append([]func(*facts){running(revisionA), inSlot, onClass(5, 5, 5), settlingAt(present.Add(-settling))}, notReady...)...),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintenanceFailed, slot: true, message: "Ready is not True 45s after the change Lodia started rolled out: QuorumLost: DatastoreReady: Etcd dev-etcd has 0 of 1 members ready", settlingSince: new(present.Add(-settling))}, 5},
		{"Maintaining within S with Ready not True", held(append([]func(*facts){running(revisionA), inSlot, onClass(5, 5, 5), settlingAt(tenSecondsAgo)}, notReady...)...),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, slot: true, settlingSince: &tenSecondsAgo, requeue: settling - 10*time.Second}, 5},
		{"rolling again removes settlingSince", held(running(revisionA), inSlot, onClass(4, 5, 5), settlingAt(tenSecondsAgo)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, slot: true, message: "etcd: runs DatastoreClass shared generation 4, not 5"}, 5},
		{"MaintenanceFailed at once on an Etcd refused", held(running(revisionA), inSlot, onClass(4, 5, 5),
			condition(metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, Message: "the API server refuses Etcd dev-etcd"})),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintenanceFailed, slot: true, message: "the change Lodia started failed: DatastoreReady: Refused: the API server refuses Etcd dev-etcd"}, 5},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectClass(t, expectGrant(t, c.f, c.want), shared(c.class))
		})
	}
}

func TestAClassHeldForItsCredentialsTakesNoSlotAndFailsInOne(t *testing.T) {
	const note = "Etcd dev-etcd has quorum, 1 of 1 members ready; DatastoreClass shared generation 5 waits for status.target.datastoreClass; DatastoreClass shared is held: its credentials Secret lodia/s3-new is missing"
	heldFor := func(reason string) func(*facts) {
		return condition(metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionTrue, Reason: reason, Message: note})
	}
	for _, reason := range []string{lodiav1alpha1.ReasonSecretNotFound, lodiav1alpha1.ReasonObjectsControlledElsewhere, lodiav1alpha1.ReasonRefused} {
		t.Run(reason, func(t *testing.T) {
			expectClass(t, expectGrant(t, held(running(revisionA), onClass(4, 4, 5), heldFor(reason)),
				granted{revision: revisionA, reason: reason, message: "DatastoreReady: " + note}), shared(4))

			f := held(running(revisionA), onClass(4, 5, 5), slotted(lodiav1alpha1.ReasonMaintaining, 3), heldFor(reason))
			failed := granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintenanceFailed, message: "the change Lodia started failed: DatastoreReady: " + reason + ": " + note, slot: true}
			d := expectGrant(t, f, failed)
			expectClass(t, d, shared(5))
			adopt(f.cp, d)
			f.now = present.Add(7 * 24 * time.Hour)
			expectGrant(t, f, failed)

			rolled := held(running(revisionA), onClass(5, 5, 5), slotted(lodiav1alpha1.ReasonMaintaining, 3), settlingAt(present.Add(-10*time.Second)), heldFor(reason))
			expectGrant(t, rolled, granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintenanceFailed, message: failed.message, slot: true, settlingSince: new(present.Add(-10 * time.Second))})
		})
	}

	f := held(running(revisionA), closedWindow, onClass(4, 4, 5), heldFor(lodiav1alpha1.ReasonSecretNotFound))
	nextStart := time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, scheduledFor: &nextStart, requeue: 30 * time.Minute}))
	f.now = nextStart
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonSecretNotFound, message: "DatastoreReady: " + note}))
	condition(metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, Message: "Etcd dev-etcd has quorum, 1 of 1 members ready"})(&f)
	expectClass(t, expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, message: "etcd: runs DatastoreClass shared generation 4, not 5", slot: true, takesSlot: true}), shared(5))
}

func TestAClassEditedTwiceBeforeTheGrantTakesTheLatest(t *testing.T) {
	f := held(running(revisionA), closedWindow, onClass(4, 4, 5))
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, message: "generation 5 waits", scheduledFor: new(time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)), requeue: 30 * time.Minute}))
	f.class = shared(6)
	d := expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, message: "generation 6 waits", scheduledFor: new(time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)), requeue: 30 * time.Minute})
	expectClass(t, d, shared(4))
	adopt(f.cp, d)
	f.now = time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)
	expectClass(t, expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, message: "runs DatastoreClass shared generation 4, not 6", slot: true, takesSlot: true}), shared(6))
}

func TestAClassEditedBetweenTwoPassesOfItsSlotIsTakenAtOnce(t *testing.T) {
	f := held(running(revisionA), onClass(4, 4, 5))
	d := expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, slot: true, takesSlot: true})
	expectClass(t, d, shared(5))
	adopt(f.cp, d)
	f.class = shared(6)
	closedWindow(&f)
	expectClass(t, expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, message: "runs DatastoreClass shared generation 4, not 6", slot: true}), shared(6))
}

func TestAClassDeletedWhileItsEditWaitsIsNoLongerPending(t *testing.T) {
	f := held(running(revisionA), closedWindow, onClass(4, 4, 5))
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, scheduledFor: new(time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)), requeue: 30 * time.Minute}))
	f.class = noClass
	expectClass(t, expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}), shared(4))
	f.class = again(1)
	expectClass(t, expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, message: "DatastoreClass shared generation 1 of UID " + recreateUID + " waits", scheduledFor: new(time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)), requeue: 30 * time.Minute}), shared(4))
}

func TestAClassCreatedAgainUnderItsNameWaitsForItsGrant(t *testing.T) {
	nextStart := time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)
	nowSecond := now.Rfc3339Copy().Time
	waits := func(generation string) granted {
		return granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, message: "DatastoreClass shared generation " + generation + " of UID " + recreateUID + " waits for the maintenance window", scheduledFor: &nextStart, requeue: 30 * time.Minute}
	}
	for _, c := range []struct {
		name  string
		f     facts
		want  granted
		class lodiav1alpha1.DatastoreClassGeneration
	}{
		{"at the generation granted to the class it replaces", held(running(revisionA), closedWindow, onClasses(shared(1), shared(1), again(1))), waits("1"), shared(1)},
		{"at a generation above the one granted", held(running(revisionA), closedWindow, onClasses(shared(1), shared(1), again(2))), waits("2"), shared(1)},
		{"reaching the generation granted to the class it replaces", held(running(revisionA), closedWindow, onClasses(shared(2), shared(2), again(2))), waits("2"), shared(2)},
		{"with no target yet, which takes what the Etcd runs", held(running(revisionA), closedWindow, onClasses(shared(1), noClass, again(1))), waits("1"), shared(1)},
		{"granted in a free slot", held(running(revisionA), onClasses(shared(1), shared(1), again(1))),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, message: "the change Lodia started rolls: etcd: runs DatastoreClass shared generation 1, not 1 of UID " + recreateUID, slot: true, takesSlot: true}, again(1)},
		{"granted at once in a slot", held(running(revisionA), closedWindow, onClasses(shared(1), shared(1), again(1)), slotted(lodiav1alpha1.ReasonMaintaining, 3)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, message: "etcd: runs DatastoreClass shared generation 1, not 1 of UID " + recreateUID, slot: true}, again(1)},
		{"rolled out once the Etcd runs it", held(running(revisionA), onClasses(again(1), again(1), again(1)), slotted(lodiav1alpha1.ReasonMaintaining, 3)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonMaintaining, message: "keeps its slot of the fleet until", slot: true, settlingSince: &nowSecond, requeue: settling}, again(1)},
		{"an Etcd created again by hand takes it as it is, and so does the target", held(running(revisionA), closedWindow, onClasses(again(1), shared(1), again(1))),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}, again(1)},
		{"failed while the Etcd runs the class it replaces", held(running(revisionA), onClasses(shared(1), again(1), again(1)),
			condition(metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, Message: "the API server refuses Etcd dev-etcd"})),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonFailed, message: "DatastoreReady: Refused"}, again(1)},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectClass(t, expectGrant(t, c.f, c.want), c.class)
		})
	}
}

func TestAClassEditOnATenantWithNoTargetWaitsForTheWindow(t *testing.T) {
	f := held(running(revisionA), closedWindow, onClass(4, 0, 5), func(f *facts) { f.cp.Status.Target = nil })
	d := expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, message: "DatastoreClass shared generation 5 waits for the maintenance window",
		scheduledFor: new(time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)), requeue: 30 * time.Minute})
	expectClass(t, d, shared(4))
}

func TestAStandingRefusalIsIdleWhereTheClassTargetTakesWhatTheEtcdRuns(t *testing.T) {
	refused := condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, Message: "the tenant refuses"})
	for _, c := range []struct {
		name  string
		f     facts
		class lodiav1alpha1.DatastoreClassGeneration
	}{
		{"an absent target", held(running(revisionA), onClass(4, 0, 4), refused), shared(4)},
		{"an Etcd created again by hand", held(running(revisionA), onClass(7, 5, 7), refused), shared(7)},
		{"a class gone after its grant", held(running(revisionA), onClass(4, 5, 0), refused), shared(4)},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectClass(t, expectGrant(t, c.f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}), c.class)
		})
	}
}

func TestAPausedOrDeletingPassKeepsTheClassTarget(t *testing.T) {
	for _, change := range []func(*facts){
		func(f *facts) { f.cp.Spec.Paused = true },
		func(f *facts) { f.cp.DeletionTimestamp = &started },
	} {
		f := held(running(revisionA), onClass(4, 4, 5), change)
		expectClass(t, decide(f), shared(4))
		f = held(running(revisionA), onClass(4, 0, 4), change)
		if target := decide(f).status.Target; target == nil || target.DatastoreClass != nil {
			t.Errorf("a paused or deleting pass wrote target %+v, want no class", target)
		}
	}
}
