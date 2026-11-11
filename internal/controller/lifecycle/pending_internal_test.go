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
