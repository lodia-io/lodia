// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const revisionC = "v1.2.0+cccc"

const settling = 45 * time.Second

var slotTaken = present.Add(-2 * time.Hour)

func held(change ...func(*facts)) facts {
	f := world{spec: "v1.37.1", target: "v1.37.1", revision: revisionA, reports: at("v1.37.1")}.facts()
	f.revision, f.limit, f.settling = revisionB, 5, settling
	f.cp.Status.Datastore = &lodiav1alpha1.DatastoreStatus{Members: lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: 1, Ready: 1}, Updated: new(int32(1))}}
	reported(&f, revisionA)
	set(&f, metav1.Condition{Type: lodiav1alpha1.ConditionProgressing, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonIdle, ObservedGeneration: 3})
	set(&f, metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(present.Add(-time.Hour))})
	for _, c := range change {
		c(&f)
	}
	return f
}

func reported(f *facts, revision string) {
	c := f.cp.Status.Components
	for _, component := range []*lodiav1alpha1.ComponentStatus{c.APIServer, c.ControllerManager, c.Scheduler} {
		component.OperatorRevision = revision
	}
	if f.cp.Status.Addons != nil {
		f.cp.Status.Addons.OperatorRevision = revision
	}
	f.cp.Status.Datastore.OperatorRevision = revision
}

func set(f *facts, c metav1.Condition) {
	meta.RemoveStatusCondition(&f.cp.Status.Conditions, c.Type)
	f.cp.Status.Conditions = append(f.cp.Status.Conditions, c)
}

func reportsAll(revision string) func(*facts) { return func(f *facts) { reported(f, revision) } }

func condition(c metav1.Condition) func(*facts) {
	if c.ObservedGeneration == 0 {
		c.ObservedGeneration = 3
	}
	return func(f *facts) { set(f, c) }
}

func progressingAs(reason string, generation int64) func(*facts) {
	status := progressingStatus(reason)
	return condition(metav1.Condition{Type: lodiav1alpha1.ConditionProgressing, Status: status, Reason: reason, ObservedGeneration: generation})
}

func slotted(reason string, generation int64) func(*facts) {
	return func(f *facts) {
		progressingAs(reason, generation)(f)
		if f.cp.Status.Maintenance == nil {
			f.cp.Status.Maintenance = &lodiav1alpha1.MaintenanceStatus{}
		}
		f.cp.Status.Maintenance.SlotSince = &metav1.Time{Time: slotTaken}
	}
}

func inWindow(begin, end, zone string) func(*facts) {
	return func(f *facts) {
		f.cp.Spec.Maintenance = &lodiav1alpha1.Maintenance{Window: &lodiav1alpha1.MaintenanceWindow{Begin: begin, End: end, TimeZone: zone}}
	}
}

var closedWindow = inWindow("13:00", "14:00", "")

func requested(token string) func(*facts) {
	return func(f *facts) {
		f.cp.Annotations = map[string]string{lodiav1alpha1.MaintainRequestedAtAnnotation: token}
	}
}

func handled(token string) func(*facts) {
	return func(f *facts) {
		if f.cp.Status.Maintenance == nil {
			f.cp.Status.Maintenance = &lodiav1alpha1.MaintenanceStatus{}
		}
		f.cp.Status.Maintenance.LastHandled = token
	}
}

func settlingAt(at time.Time) func(*facts) {
	return func(f *facts) {
		if f.cp.Status.Maintenance == nil {
			f.cp.Status.Maintenance = &lodiav1alpha1.MaintenanceStatus{}
		}
		f.cp.Status.Maintenance.SettlingSince = &metav1.Time{Time: at}
	}
}

func scheduled(at time.Time) func(*facts) {
	return func(f *facts) { f.cp.Status.Target.ScheduledFor = &metav1.Time{Time: at} }
}

func running(revision string) func(*facts) { return func(f *facts) { f.revision = revision } }

func fleet(members ...member) func(*facts) { return func(f *facts) { f.fleet = members } }

func holding(namespace, name string) member {
	return member{slot: true, key: queueKey{namespace: namespace, name: name}}
}

func waiting(namespace, name string, since time.Time) member {
	return member{key: queueKey{scheduledFor: since, namespace: namespace, name: name}}
}

type granted struct {
	revision      string
	reason        string
	message       string
	scheduledFor  *time.Time
	lastHandled   string
	settlingSince *time.Time
	requeue       time.Duration
	takesSlot     bool
	slot          bool
}

func expectGrant(t *testing.T, f facts, want granted) decision {
	t.Helper()
	before := f.cp.DeepCopy()
	d := decide(f)
	if !equality.Semantic.DeepEqual(before, f.cp) {
		t.Errorf("decide changed its facts")
	}
	target := lodiav1alpha1.Target{}
	if d.status.Target != nil {
		target = *d.status.Target
	}
	if target.OperatorRevision != want.revision {
		t.Errorf("target.operatorRevision is %q, want %q", target.OperatorRevision, want.revision)
	}
	switch {
	case want.scheduledFor == nil && target.ScheduledFor != nil:
		t.Errorf("target.scheduledFor is %s, want it absent", target.ScheduledFor)
	case want.scheduledFor != nil && (target.ScheduledFor == nil || !target.ScheduledFor.Time.Equal(*want.scheduledFor)):
		t.Errorf("target.scheduledFor is %v, want %s", target.ScheduledFor, want.scheduledFor)
	}
	m := lodiav1alpha1.MaintenanceStatus{}
	if d.status.Maintenance != nil {
		m = *d.status.Maintenance
	}
	if m.LastHandled != want.lastHandled {
		t.Errorf("maintenance.lastHandled is %q, want %q", m.LastHandled, want.lastHandled)
	}
	switch {
	case want.settlingSince == nil && m.SettlingSince != nil:
		t.Errorf("maintenance.settlingSince is %s, want it absent", m.SettlingSince)
	case want.settlingSince != nil && (m.SettlingSince == nil || !m.SettlingSince.Time.Equal(*want.settlingSince)):
		t.Errorf("maintenance.settlingSince is %v, want %s", m.SettlingSince, want.settlingSince)
	}
	if d.requeue != want.requeue {
		t.Errorf("requeue after %s, want %s", d.requeue, want.requeue)
	}
	if d.takesSlot != want.takesSlot {
		t.Errorf("takesSlot is %t, want %t", d.takesSlot, want.takesSlot)
	}
	slotSince := slotOf(before)
	if want.takesSlot {
		slotSince = &metav1.Time{Time: metav1.NewTime(f.now).Rfc3339Copy().Time}
	}
	switch {
	case !want.slot && m.SlotSince != nil:
		t.Errorf("maintenance.slotSince is %s, want it absent", m.SlotSince)
	case want.slot && (m.SlotSince == nil || slotSince == nil || !m.SlotSince.Equal(slotSince)):
		t.Errorf("maintenance.slotSince is %v, want %v", m.SlotSince, slotSince)
	}
	c := meta.FindStatusCondition(d.status.Conditions, lodiav1alpha1.ConditionProgressing)
	if c == nil || c.Reason != want.reason || c.Status != progressingStatus(want.reason) || c.ObservedGeneration != f.cp.Generation {
		t.Fatalf("Progressing is %+v, want %s %s at generation %d", c, progressingStatus(want.reason), want.reason, f.cp.Generation)
	}
	if !strings.Contains(c.Message, want.message) {
		t.Errorf("Progressing's message is %q, want it to say %q", c.Message, want.message)
	}
	return d
}

func TestTheGrantFollowsTheRulesInTheirOrder(t *testing.T) {
	long := strings.Repeat("x", 257)
	wide := strings.Repeat("é", 256)
	refused := metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, Message: "the tenant refuses"}
	elsewhere := metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonObjectsControlledElsewhere, Message: "ConfigMap kube-system/x exists and is not Lodia's"}
	applying := metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonApplying}
	waitingForAccess := metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForAccess, Message: "AccessReady is not True"}
	unreachable := metav1.Condition{Type: lodiav1alpha1.ConditionReachable, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, Message: "api: refused"}
	pastDeadline := metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonProgressDeadlineExceeded, Message: "past the deadline"}
	heldAPI := metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForTarget}
	nextStart := time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)
	nowSecond := now.Rfc3339Copy().Time
	for _, c := range []struct {
		name string
		f    facts
		want granted
	}{
		{"1: a new token is recorded with nothing pending", held(running(revisionA), requested("t1")),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle, lastHandled: "t1"}},
		{"1: a token over 256 characters is not handled, and the message says so", held(closedWindow, requested(long), handled("t0")),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, message: "lodia.io/maintain-requested-at holds more than 256 characters and is not handled", scheduledFor: &nextStart, lastHandled: "t0", requeue: 30 * time.Minute}},
		{"1: a token of 256 characters of two bytes each is handled", held(closedWindow, requested(wide)),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut, lastHandled: wide}},
		{"1: TimeZoneUnsupported whenever a window is set, nothing pending", held(running(revisionA), inWindow("13:00", "14:00", "Mars/Olympus_Mons")),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonTimeZoneUnsupported, message: "spec.maintenance.window.timeZone Mars/Olympus_Mons is not in the operator's zone database"}},
		{"2: nothing pending: nothing, whatever the window", held(running(revisionA), closedWindow),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}},
		{"2: a revision that cannot render a report is granted on no path, a token's neither", held(requested("t1"), func(f *facts) { f.cp.Status.Addons.Version = "v1.34.9" }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonUnsupported, message: "does not run status.addons.version v1.34.9", lastHandled: "t1"}},
		{"2: nor an owner's edit", held(progressingAs(lodiav1alpha1.ReasonIdle, 2), func(f *facts) { f.cp.Status.Components.Scheduler.Version = "v1.33.1" }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonUnsupported, message: "status.components.scheduler.version v1.33.1"}},
		{"2: nor a tenant in a slot, which keeps it", held(slotted(lodiav1alpha1.ReasonMaintaining, 3), func(f *facts) { f.cp.Status.Components.APIServer.Version = "v1.34.9" }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonUnsupported, slot: true}},
		{"3: no target yet", held(closedWindow, func(f *facts) { f.cp.Status.Target = nil }),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut}},
		{"3: a target granted no revision yet", held(closedWindow, func(f *facts) { f.cp.Status.Target.OperatorRevision = "" }),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut}},
		{"3: an owner's edit under a hold, ahead of Progressing's generation", held(closedWindow, progressingAs(lodiav1alpha1.ReasonWaitingForWindow, 2)),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut}},
		{"3: an owner's upgrade under way", held(closedWindow, func(f *facts) {
			f.cp.Status.Kubernetes = &lodiav1alpha1.KubernetesStatus{Upgrade: &lodiav1alpha1.UpgradeStatus{Step: lodiav1alpha1.UpgradeStepAddons, StartedAt: started}}
			f.cp.Status.Addons.Version = "v1.37.0"
		}), granted{revision: revisionB, reason: lodiav1alpha1.ReasonUpgrading}},
		{"3: a token, whose grant is the owner's: no slot", held(closedWindow, requested("t1"), handled("t0")),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut, lastHandled: "t1"}},
		{"3: a tenant in its slot takes a new release at once and keeps the slot, the settling cleared", held(closedWindow, running(revisionC), reportsAll(revisionB), func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB }, slotted(lodiav1alpha1.ReasonMaintaining, 3), settlingAt(present.Add(-10*time.Second))),
			granted{revision: revisionC, reason: lodiav1alpha1.ReasonMaintaining, slot: true, message: "kube-apiserver: reports operator revision " + revisionB + ", not " + revisionC}},
		{"3: a tenant whose change failed in its slot takes the running revision at once", held(closedWindow, slotted(lodiav1alpha1.ReasonMaintenanceFailed, 3)),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true}},
		{"3: addons never applied in full", held(closedWindow, condition(waitingForAccess), func(f *facts) { f.cp.Status.Addons = nil }),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut}},
		{"3: addons refused", held(closedWindow, condition(refused)),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonFailed, message: "the tenant refuses"}},
		{"3: addons stopped on another owner's object", held(closedWindow, condition(elsewhere)),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonFailed, message: "AddonsApplied: ObjectsControlledElsewhere: ConfigMap kube-system/x exists and is not Lodia's"}},
		{"3: addons whose apply did not finish", held(closedWindow, condition(applying)),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut}},
		{"3: APIServerReady False WaitingForTarget at the object's generation", held(closedWindow, condition(heldAPI)),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut}},
		{"3: a restored tenant whose controller manager is not released", held(closedWindow, func(f *facts) {
			f.cp.Spec.RestoreFrom = &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: "a-1"}}
			f.cp.Status.Components.ControllerManager.Total = 0
		}), granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut}},
		{"3: not addons that wait with their version reported", held(closedWindow, condition(waitingForAccess)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, scheduledFor: &nextStart, requeue: 30 * time.Minute}},
		{"3: not WaitingForTarget at an earlier generation", held(closedWindow, condition(metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForTarget, ObservedGeneration: 2})),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonLatestGenerationNotObserved, scheduledFor: &nextStart, requeue: 30 * time.Minute}},
		{"3: not a restored tenant whose controller manager runs", held(closedWindow, func(f *facts) {
			f.cp.Spec.RestoreFrom = &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: "a-1"}}
		}), granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, scheduledFor: &nextStart, requeue: 30 * time.Minute}},
		{"4: Unsupported is the owner's to fix", held(func(f *facts) { f.cp.Spec.Version = "v1.38.0" }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonUnsupported, message: "spec.version v1.38.0"}},
		{"4: so is Failed", held(condition(pastDeadline), func(f *facts) { f.cp.Status.Components.Scheduler.OperatorRevision = revisionC }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonFailed, message: "past the deadline"}},
		{"5: TimeZoneUnsupported: no grant and no scheduledFor", held(inWindow("13:00", "14:00", "Mars/Olympus_Mons")),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonTimeZoneUnsupported}},
		{"5: a closed window: WaitingForWindow, scheduledFor its next start", held(closedWindow),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, message: "waits for the maintenance window 13:00-14:00 UTC, which opens at 2026-11-09T13:00:00Z", scheduledFor: &nextStart, requeue: 30 * time.Minute}},
		{"5: a window that opens in more than an hour is looked at again in one", held(inWindow("20:00", "21:00", "Europe/Kyiv")),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, scheduledFor: new(time.Date(2026, 11, 9, 18, 0, 0, 0, time.UTC)), requeue: time.Hour}},
		{"5: an Etcd created under the hold reports the running revision, which is not rolling", held(closedWindow, func(f *facts) { f.cp.Status.Datastore.OperatorRevision = revisionB }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForWindow, scheduledFor: &nextStart, requeue: 30 * time.Minute}},
		{"5: what rolls comes before the wait, which keeps its scheduledFor", held(closedWindow, func(f *facts) { *f.cp.Status.Components.APIServer.Updated = 0 }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonRollingOut, scheduledFor: &nextStart, requeue: 30 * time.Minute}},
		{"6: not Ready in an open window: Ready's reason", held(inWindow("12:00", "13:00", ""), condition(unreachable)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonExternalProbeFailed, message: "Reachable: api: refused"}},
		{"6: Ready's reason before a cause standing on an input of Ready", held(inWindow("12:00", "13:00", ""), condition(pastDeadline), condition(unreachable)),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonExternalProbeFailed, message: "Reachable: api: refused"}},
		{"6: Ready True at an earlier generation counts", held(func(f *facts) {
			f.cp.Generation = 4
			for i := range f.cp.Status.Conditions {
				f.cp.Status.Conditions[i].ObservedGeneration = 3
			}
			meta.FindStatusCondition(f.cp.Status.Conditions, lodiav1alpha1.ConditionProgressing).ObservedGeneration = 4
		}), granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, takesSlot: true}},
		{"6: AddonsApplied waiting at an earlier generation with AccessReady True is left out", held(condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForAccess, ObservedGeneration: 2})),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, takesSlot: true}},
		{"6: so is AddonsApplied WaitingForTarget", held(condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForTarget})),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, takesSlot: true, slot: true}},
		{"6: but not without AccessReady", held(condition(waitingForAccess), condition(metav1.Condition{Type: lodiav1alpha1.ConditionAccessReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonBinding, Message: "not confirmed"})),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonBinding, message: "AccessReady: not confirmed"}},
		{"7: a free slot: Maintaining in the same apply as the grant", held(inWindow("12:00", "13:00", "")),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, message: "operator revision " + revisionB + " rolls: kube-apiserver: reports operator revision " + revisionA + ", not " + revisionB, takesSlot: true}},
		{"7: no slot: FleetLimit, scheduledFor when it became eligible", held(fleet(holding("a", "a"), holding("b", "b")), func(f *facts) { f.limit = 2 }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, message: "one of the fleet's 2 slots", scheduledFor: &nowSecond, requeue: 30 * time.Second}},
		{"7: FleetLimit keeps its scheduledFor while it waits", held(fleet(holding("a", "a")), scheduled(present.Add(-time.Hour)), progressingAs(lodiav1alpha1.ReasonFleetLimit, 3), func(f *facts) { f.limit = 1 }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: new(present.Add(-time.Hour)), requeue: 30 * time.Second}},
		{"7: the window's start, kept from the wait, is when it became eligible", held(fleet(holding("a", "a")), inWindow("12:00", "13:00", ""), scheduled(time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)), func(f *facts) { f.limit = 1 }),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: new(time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)), requeue: 30 * time.Second}},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectGrant(t, c.f, c.want)
		})
	}
}

func TestRuleSixWaitsWhileReadyIsUnknown(t *testing.T) {
	for _, c := range []struct {
		name    string
		change  func(*facts)
		message string
	}{
		{"an input of Ready missing", func(f *facts) {
			meta.RemoveStatusCondition(&f.cp.Status.Conditions, lodiav1alpha1.ConditionControllersLeading)
		}, "Ready is Unknown: LatestGenerationNotObserved: ControllersLeading is not reported for generation 3"},
		{"an input on its writer's first pass", condition(metav1.Condition{Type: lodiav1alpha1.ConditionReachable, Status: metav1.ConditionUnknown, Reason: lodiav1alpha1.ReasonReconciling, Message: "the first pass has not finished"}),
			"Ready is Unknown: Reconciling: Reachable: the first pass has not finished"},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectGrant(t, held(scheduled(present.Add(-time.Minute)), c.change), granted{revision: revisionA, reason: lodiav1alpha1.ReasonWaitingForReady, message: c.message})
		})
	}
}

func TestTheQueueForASlot(t *testing.T) {
	before, after := present.Add(-time.Minute), present.Add(time.Minute)
	for _, c := range []struct {
		name  string
		limit int
		fleet []member
		free  bool
	}{
		{"an empty fleet", 1, nil, true},
		{"used slots count", 2, []member{holding("a", "a"), holding("b", "b")}, false},
		{"tenants queued earlier count", 2, []member{holding("a", "a"), waiting("b", "b", before)}, false},
		{"tenants queued later do not", 2, []member{holding("a", "a"), waiting("b", "b", after)}, true},
		{"at the same time, an earlier namespace counts", 1, []member{waiting("da", "zz", present)}, false},
		{"at the same time, a later namespace does not", 1, []member{waiting("tz", "aa", present)}, true},
		{"at the same time and namespace, an earlier name counts", 1, []member{waiting("team", "cp", present)}, false},
		{"at the same time and namespace, a later name does not", 1, []member{waiting("team", "eve", present)}, true},
		{"used and queued add up below the limit", 3, []member{holding("a", "a"), waiting("b", "b", before), waiting("c", "c", after)}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := facts{cp: world{spec: "v1.37.1"}.controlPlane(), fleet: c.fleet, limit: c.limit, now: present}
			if got := f.free(queueKey{scheduledFor: present, namespace: "team", name: "dev"}); got != c.free {
				t.Errorf("free is %t, want %t", got, c.free)
			}
		})
	}
	t.Run("the key is the pass's now until a scheduledFor is written", func(t *testing.T) {
		f := held()
		if got := f.key(); !got.scheduledFor.Equal(now.Rfc3339Copy().Time) || got.namespace != "team" || got.name != "dev" {
			t.Errorf("the key is %+v", got)
		}
		scheduled(before)(&f)
		if got := f.key(); !got.scheduledFor.Equal(before) {
			t.Errorf("the key is %+v, want the scheduledFor %s", got, before)
		}
		scheduled(after)(&f)
		if got := f.key(); !got.scheduledFor.Equal(now.Rfc3339Copy().Time) {
			t.Errorf("the key is %+v, want now, since the scheduledFor %s is a window's next start", got, after)
		}
	})
}

func TestTheFleetCountsSlotsPausedOrNotAndQueuesOthersNotPaused(t *testing.T) {
	cp := world{spec: "v1.37.1"}.controlPlane()
	type other struct {
		name, reason           string
		scheduledFor           time.Time
		slot, paused, deleting bool
	}
	tenant := func(o other) lodiav1alpha1.ControlPlane {
		c := lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: "a", Name: o.name, Generation: 2}, Spec: lodiav1alpha1.ControlPlaneSpec{Paused: o.paused}}
		if o.name == "dev" {
			c.Namespace = "team"
		}
		if o.reason != "" {
			c.Status.Conditions = []metav1.Condition{{Type: lodiav1alpha1.ConditionProgressing, Reason: o.reason, ObservedGeneration: 2}}
		}
		if o.slot {
			c.Status.Maintenance = &lodiav1alpha1.MaintenanceStatus{SlotSince: &metav1.Time{Time: slotTaken}}
		}
		if !o.scheduledFor.IsZero() {
			c.Status.Target = &lodiav1alpha1.Target{ScheduledFor: &metav1.Time{Time: o.scheduledFor}}
		}
		if o.deleting {
			c.DeletionTimestamp = &started
		}
		return c
	}
	var cps []lodiav1alpha1.ControlPlane
	for _, o := range []other{
		{name: "dev", reason: lodiav1alpha1.ReasonMaintaining, slot: true},
		{name: "maintaining", reason: lodiav1alpha1.ReasonMaintaining, slot: true},
		{name: "failed", reason: lodiav1alpha1.ReasonMaintenanceFailed, slot: true},
		{name: "zone", reason: lodiav1alpha1.ReasonTimeZoneUnsupported, slot: true},
		{name: "edited", reason: lodiav1alpha1.ReasonLatestGenerationNotObserved, slot: true},
		{name: "paused-in-a-slot", reason: lodiav1alpha1.ReasonPaused, slot: true, scheduledFor: present.Add(-time.Minute), paused: true},
		{name: "queued", reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: present.Add(-time.Minute)},
		{name: "rolling", reason: lodiav1alpha1.ReasonRollingOut, scheduledFor: present.Add(-time.Minute)},
		{name: "opening", reason: lodiav1alpha1.ReasonWaitingForWindow, scheduledFor: present},
		{name: "later", reason: lodiav1alpha1.ReasonWaitingForWindow, scheduledFor: present.Add(time.Second)},
		{name: "paused", reason: lodiav1alpha1.ReasonPaused, scheduledFor: present.Add(-time.Minute), paused: true},
		{name: "deleted", reason: lodiav1alpha1.ReasonMaintaining, slot: true, deleting: true},
		{name: "deleted-queued", reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: present.Add(-time.Minute), deleting: true},
		{name: "idle", reason: lodiav1alpha1.ReasonIdle},
		{name: "new"},
	} {
		cps = append(cps, tenant(o))
	}
	var names []string
	for _, m := range fleetOf(cps, cp, present) {
		names = append(names, fmt.Sprintf("%s %t %s", m.key.name, m.slot, m.key.scheduledFor.Format(time.TimeOnly)))
	}
	if got, want := strings.Join(names, ", "), "maintaining true 00:00:00, failed true 00:00:00, zone true 00:00:00, edited true 00:00:00, paused-in-a-slot true 00:00:00, queued false 12:29:00, rolling false 12:29:00, opening false 12:30:00"; got != want {
		t.Errorf("the fleet counts %s, want %s", got, want)
	}
}

func TestAChangeInASlotSettles(t *testing.T) {
	inSlot := slotted(lodiav1alpha1.ReasonMaintaining, 3)
	grantedB := func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB }
	tenSecondsAgo := present.Add(-10 * time.Second)
	notReady := []func(*facts){
		condition(metav1.Condition{Type: lodiav1alpha1.ConditionReachable, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, Message: "api: refused"}),
		condition(metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(present.Add(-time.Hour))}),
	}
	nowSecond := now.Rfc3339Copy().Time
	for _, c := range []struct {
		name string
		f    facts
		want granted
	}{
		{"Maintaining while it rolls, with no settlingSince", held(inSlot, grantedB),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, message: "the addons: reports operator revision " + revisionA}},
		{"rolled out: settlingSince written now, and a pass at S", held(inSlot, grantedB, reportsAll(revisionB)),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, message: "keeps its slot of the fleet until 2026-11-09T12:30:45Z", settlingSince: &nowSecond, requeue: settling}},
		{"settlingSince kept, and the pass at what is left of S", held(inSlot, grantedB, reportsAll(revisionB), settlingAt(tenSecondsAgo)),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, settlingSince: &tenSecondsAgo, requeue: settling - 10*time.Second}},
		{"Idle after S with Ready True, settlingSince removed and the slot free", held(inSlot, grantedB, reportsAll(revisionB), settlingAt(present.Add(-settling))),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonIdle}},
		{"MaintenanceFailed after S with Ready not True, keeping the slot", held(append([]func(*facts){inSlot, grantedB, reportsAll(revisionB), settlingAt(present.Add(-settling))}, notReady...)...),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintenanceFailed, slot: true, message: "Ready is not True 45s after operator revision " + revisionB + " rolled out: ExternalProbeFailed: Reachable: api: refused", settlingSince: new(present.Add(-settling))}},
		{"Maintaining within S with Ready not True", held(append([]func(*facts){inSlot, grantedB, reportsAll(revisionB), settlingAt(tenSecondsAgo)}, notReady...)...),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, settlingSince: &tenSecondsAgo, requeue: settling - 10*time.Second}},
		{"Ready coming back restarts S from now, the write's transition", held(slotted(lodiav1alpha1.ReasonMaintenanceFailed, 3), grantedB, reportsAll(revisionB), settlingAt(present.Add(-time.Hour)), notReady[1]),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, settlingSince: new(present.Add(-time.Hour)), requeue: settling}},
		{"S runs from Ready's later transition", held(inSlot, grantedB, reportsAll(revisionB), settlingAt(present.Add(-time.Hour)), condition(metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(tenSecondsAgo)})),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, settlingSince: new(present.Add(-time.Hour)), requeue: settling - 10*time.Second}},
		{"etcd not at the granted revision keeps it rolling", held(inSlot, grantedB, reportsAll(revisionB), func(f *facts) { f.cp.Status.Datastore.OperatorRevision = revisionA }),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, message: "etcd: reports operator revision " + revisionA + ", not " + revisionB}},
		{"a new grant clears settlingSince, even with nothing left to roll", held(slotted(lodiav1alpha1.ReasonMaintenanceFailed, 3), running(revisionC), reportsAll(revisionC), settlingAt(present.Add(-time.Hour))),
			granted{revision: revisionC, reason: lodiav1alpha1.ReasonMaintaining, slot: true, message: "operator revision " + revisionC + " rolls"}},
		{"rolling again removes settlingSince", held(inSlot, grantedB, reportsAll(revisionB), settlingAt(tenSecondsAgo), func(f *facts) { f.cp.Status.Datastore.Members.Updated = nil }),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, message: "etcd: its updated members are not reported yet"}},
		{"an owner's edit keeps the slot and settlingSince", held(slotted(lodiav1alpha1.ReasonMaintaining, 2), grantedB, reportsAll(revisionB), settlingAt(tenSecondsAgo)),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, settlingSince: &tenSecondsAgo, requeue: settling - 10*time.Second}},
	} {
		t.Run(c.name, func(t *testing.T) {
			expectGrant(t, c.f, c.want)
		})
	}
	for _, cause := range []metav1.Condition{
		{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, Message: "refused"},
		{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonObjectsControlledElsewhere, Message: "kept elsewhere"},
		{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonUnsupported, Message: "not built"},
		{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonProgressDeadlineExceeded, Message: "available, past the deadline"},
		{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonProgressDeadlineExceeded, Message: "unavailable, past the deadline"},
	} {
		t.Run("MaintenanceFailed on "+cause.Reason+" "+string(cause.Status)+", rolling or rolled out, keeping the slot", func(t *testing.T) {
			expectGrant(t, held(inSlot, grantedB, condition(cause)), granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintenanceFailed, slot: true, message: cause.Message})
			expectGrant(t, held(inSlot, grantedB, reportsAll(revisionB), settlingAt(tenSecondsAgo), condition(cause)),
				granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintenanceFailed, slot: true, message: cause.Message, settlingSince: &tenSecondsAgo})
		})
	}
}

func TestTheOriginIsKeptAcrossPassesAtOneGeneration(t *testing.T) {
	f := held(inWindow("12:00", "13:00", ""))
	d := expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, takesSlot: true})
	adopt(f.cp, d)
	f.cp.Spec.Maintenance.Window = &lodiav1alpha1.MaintenanceWindow{Begin: "13:00", End: "14:00"}
	expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true})
	reported(&f, revisionB)
	d = expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true, settlingSince: new(now.Rfc3339Copy().Time), requeue: settling})
	adopt(f.cp, d)
	f.now = present.Add(settling)
	d = expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonIdle})
	adopt(f.cp, d)
	if again := decide(f); again.differs(f.cp) || again.requeue != 0 {
		t.Errorf("a pass after the slot was freed still differs or requeues: %+v", again)
	}
}

func TestProgressingOrderWithMaintenance(t *testing.T) {
	inSlot := slotted(lodiav1alpha1.ReasonMaintaining, 3)
	grantedB := func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB }
	refused := condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused})
	for _, c := range []struct {
		name string
		f    facts
		want string
	}{
		{"Paused before Maintaining", held(inSlot, grantedB, func(f *facts) { f.cp.Spec.Paused = true }), lodiav1alpha1.ReasonPaused},
		{"Unsupported before MaintenanceFailed", held(inSlot, grantedB, refused, func(f *facts) { f.cp.Spec.Version = "v1.38.0" }), lodiav1alpha1.ReasonUnsupported},
		{"MaintenanceFailed before TimeZoneUnsupported", held(inSlot, grantedB, refused, inWindow("12:00", "13:00", "Mars/Olympus_Mons")), lodiav1alpha1.ReasonMaintenanceFailed},
		{"TimeZoneUnsupported before Maintaining", held(inSlot, grantedB, inWindow("12:00", "13:00", "Mars/Olympus_Mons")), lodiav1alpha1.ReasonTimeZoneUnsupported},
		{"Maintaining before a preflight", held(inSlot, grantedB, func(f *facts) {
			f.cp.Status.Target.Version = "v1.36.6"
			for _, c := range []*lodiav1alpha1.ComponentStatus{f.cp.Status.Components.APIServer, f.cp.Status.Components.ControllerManager, f.cp.Status.Components.Scheduler} {
				c.Version = "v1.36.6"
			}
			f.cp.Status.Addons.Version = "v1.36.6"
			f.cp.Status.Nodes = nil
		}), lodiav1alpha1.ReasonMaintaining},
		{"the same preflight out of a slot", held(running(revisionA), func(f *facts) {
			f.cp.Status.Target.Version = "v1.36.6"
			for _, c := range []*lodiav1alpha1.ComponentStatus{f.cp.Status.Components.APIServer, f.cp.Status.Components.ControllerManager, f.cp.Status.Components.Scheduler} {
				c.Version = "v1.36.6"
			}
			f.cp.Status.Addons.Version = "v1.36.6"
			f.cp.Status.Nodes = nil
		}), lodiav1alpha1.ReasonWaitingForNodes},
		{"Maintaining before LatestGenerationNotObserved", held(inSlot, grantedB, condition(metav1.Condition{Type: lodiav1alpha1.ConditionAccessReady, Status: metav1.ConditionTrue, ObservedGeneration: 2})), lodiav1alpha1.ReasonMaintaining},
		{"RollingOut before WaitingForWindow", held(closedWindow, func(f *facts) { *f.cp.Status.Components.Scheduler.Updated = 0 }), lodiav1alpha1.ReasonRollingOut},
		{"LatestGenerationNotObserved before Ready's reason", held(inWindow("12:00", "13:00", ""), condition(metav1.Condition{Type: lodiav1alpha1.ConditionAccessReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonBinding, ObservedGeneration: 2})), lodiav1alpha1.ReasonLatestGenerationNotObserved},
		{"WaitingForWindow before Idle", held(closedWindow), lodiav1alpha1.ReasonWaitingForWindow},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := meta.FindStatusCondition(decide(c.f).status.Conditions, lodiav1alpha1.ConditionProgressing)
			if got == nil || got.Reason != c.want {
				t.Errorf("Progressing is %+v, want %s", got, c.want)
			}
		})
	}
}

func TestAnOwnersRevisionThatIsRefusedFails(t *testing.T) {
	refused := condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, Message: "the tenant refuses"})
	expectGrant(t, held(running(revisionA), refused, func(f *facts) { f.cp.Status.Components.APIServer.OperatorRevision = revisionC }),
		granted{revision: revisionA, reason: lodiav1alpha1.ReasonFailed, message: "the tenant refuses"})
	expectGrant(t, held(running(revisionA), refused),
		granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle})
}

func TestAnotherOwnersObjectInTheTenantFailsTheChangeInItsSlot(t *testing.T) {
	const message = "the change Lodia started failed: AddonsApplied: ObjectsControlledElsewhere: ConfigMap kube-system/x exists and is not Lodia's"
	f := held(slotted(lodiav1alpha1.ReasonMaintaining, 3), func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB }, reportsAll(revisionB),
		func(f *facts) { f.cp.Status.Addons.OperatorRevision = revisionA },
		condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonObjectsControlledElsewhere, Message: "ConfigMap kube-system/x exists and is not Lodia's; delete it or disable the addon"}),
		condition(metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonObjectsControlledElsewhere, ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(present.Add(-time.Hour))}))
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintenanceFailed, slot: true, message: message}))
	f.now = present.Add(24 * time.Hour)
	expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintenanceFailed, slot: true, message: message})
	f.revision = revisionC
	expectGrant(t, f, granted{revision: revisionC, reason: lodiav1alpha1.ReasonMaintenanceFailed, slot: true, message: message})
}

func TestAnUnrenderableReleaseBetweenKeepsTheSlotOfAFailedTenant(t *testing.T) {
	const revisionD = "v1.3.0+dddd"
	f := held(slotted(lodiav1alpha1.ReasonMaintenanceFailed, 3), func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB }, reportsAll(revisionB), settlingAt(present.Add(-time.Hour)), closedWindow, running(revisionC),
		condition(metav1.Condition{Type: lodiav1alpha1.ConditionReachable, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, Message: "api: refused"}),
		condition(metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(present.Add(-time.Hour))}),
		func(f *facts) { f.cp.Status.Components.Scheduler.Version = "v1.34.9" })
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonUnsupported, message: "does not run status.components.scheduler.version v1.34.9", slot: true}))
	f.cp.Status.Components.Scheduler.Version = "v1.37.1"
	f.revision = revisionD
	expectGrant(t, f, granted{revision: revisionD, reason: lodiav1alpha1.ReasonMaintaining, message: "operator revision " + revisionD + " rolls", slot: true})
}

func TestASlotSurvivesTimeZoneUnsupported(t *testing.T) {
	f := held(slotted(lodiav1alpha1.ReasonMaintaining, 3), func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB }, inWindow("12:00", "13:00", "Mars/Olympus_Mons"))
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonTimeZoneUnsupported, slot: true}))
	reportsAll(revisionB)(&f)
	condition(metav1.Condition{Type: lodiav1alpha1.ConditionReachable, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, Message: "api: refused"})(&f)
	rolledOut := present.Add(time.Hour)
	f.now = rolledOut
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonTimeZoneUnsupported, settlingSince: &rolledOut, requeue: settling, slot: true}))
	condition(metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(rolledOut)})(&f)
	f.now = rolledOut.Add(settling)
	expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintenanceFailed, message: "Ready is not True 45s after operator revision " + revisionB + " rolled out", settlingSince: &rolledOut, slot: true})
}

func TestAPausedPassKeepsTheSlotAndADeletingPassFreesIt(t *testing.T) {
	tenSecondsAgo := metav1.NewTime(present.Add(-10 * time.Second))
	for _, c := range []struct {
		name   string
		change func(*facts)
		want   *lodiav1alpha1.MaintenanceStatus
	}{
		{"paused, an edit", func(f *facts) { f.cp.Spec.Paused, f.cp.Generation = true, 4 },
			&lodiav1alpha1.MaintenanceStatus{LastHandled: "t1", SettlingSince: &tenSecondsAgo, SlotSince: &metav1.Time{Time: slotTaken}}},
		{"deleting", func(f *facts) { f.cp.DeletionTimestamp = &started },
			&lodiav1alpha1.MaintenanceStatus{LastHandled: "t1"}},
		{"paused and deleting", func(f *facts) { f.cp.Spec.Paused, f.cp.DeletionTimestamp = true, &started },
			&lodiav1alpha1.MaintenanceStatus{LastHandled: "t1"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := decide(held(slotted(lodiav1alpha1.ReasonMaintaining, 3), settlingAt(tenSecondsAgo.Time), handled("t1"), scheduled(present.Add(-time.Minute)), c.change))
			if !equality.Semantic.DeepEqual(d.status.Maintenance, c.want) {
				t.Errorf("maintenance is %+v, want %+v", d.status.Maintenance, c.want)
			}
			if target := d.status.Target; target == nil || target.OperatorRevision != revisionA || target.ScheduledFor != nil {
				t.Errorf("target is %+v, want the stored revision %s and no scheduledFor", target, revisionA)
			}
		})
	}
}

func tenantIn(namespace string, change ...func(*facts)) facts {
	f := held(append([]func(*facts){func(f *facts) { f.limit = 1 }}, change...)...)
	f.cp.Namespace = namespace
	return f
}

func among(f *facts, others ...facts) {
	var cps []lodiav1alpha1.ControlPlane
	for _, o := range others {
		cps = append(cps, *o.cp)
	}
	f.fleet = fleetOf(cps, f.cp, f.now)
}

func when(t time.Time) func(*facts) { return func(f *facts) { f.now = t } }

func TestAWaitForASlotUnderAnEarlierReasonIsQueued(t *testing.T) {
	nowSecond := now.Rfc3339Copy().Time
	tenMinutesOn := present.Add(10 * time.Minute)
	for _, c := range []struct {
		name, reason string
		change       func(*facts)
	}{
		{"what rolls", lodiav1alpha1.ReasonRollingOut, func(f *facts) { f.cp.Status.Datastore.Members.Updated = nil }},
		{"a condition behind the generation", lodiav1alpha1.ReasonLatestGenerationNotObserved, func(f *facts) {
			f.cp.Generation = 4
			for i := range f.cp.Status.Conditions {
				f.cp.Status.Conditions[i].ObservedGeneration = 3
			}
			meta.FindStatusCondition(f.cp.Status.Conditions, lodiav1alpha1.ConditionProgressing).ObservedGeneration = 4
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			x := tenantIn("x", slotted(lodiav1alpha1.ReasonMaintaining, 3), func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB })
			waits := tenantIn("a", c.change)
			among(&waits, x)
			adopt(waits.cp, expectGrant(t, waits, granted{revision: revisionA, reason: c.reason, scheduledFor: &nowSecond, requeue: fleetRequeue}))

			x.cp.Status.Maintenance = nil
			later := tenantIn("b", when(tenMinutesOn))
			among(&later, x, waits)
			adopt(later.cp, expectGrant(t, later, granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: &tenMinutesOn, requeue: fleetRequeue}))

			waits = tenantIn("a", when(tenMinutesOn), func(f *facts) { f.cp.Status = waits.cp.Status })
			among(&waits, x, later)
			expectGrant(t, waits, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, takesSlot: true, slot: true})
		})
	}
}

func TestASharedWindowStartGoesByTheQueue(t *testing.T) {
	opens := func(f *facts) {
		f.cp.Spec.Maintenance = &lodiav1alpha1.Maintenance{Window: &lodiav1alpha1.MaintenanceWindow{Begin: "12:30", End: "13:30"}}
		f.cp.Status.Target.ScheduledFor = &metav1.Time{Time: present}
		set(f, metav1.Condition{Type: lodiav1alpha1.ConditionProgressing, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForWindow, ObservedGeneration: 3})
	}
	for _, first := range []string{"a", "z"} {
		t.Run(first+" passes first", func(t *testing.T) {
			tenants := map[string]facts{"a": tenantIn("a", opens), "z": tenantIn("z", opens)}
			other := map[string]string{"a": "z", "z": "a"}
			for _, ns := range []string{first, other[first]} {
				f := tenants[ns]
				among(&f, tenants[other[ns]])
				want := granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, takesSlot: true, slot: true}
				if ns == "z" {
					want = granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: &present, requeue: fleetRequeue}
				}
				adopt(f.cp, expectGrant(t, f, want))
			}
		})
	}
}

func TestATenantNotReadyLeavesTheQueue(t *testing.T) {
	unreachable := condition(metav1.Condition{Type: lodiav1alpha1.ConditionReachable, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, Message: "api: refused"})
	reachable := condition(metav1.Condition{Type: lodiav1alpha1.ConditionReachable, Status: metav1.ConditionTrue, Reason: "Fine"})
	t.Run("counted at its window's start until its own pass there finds it not Ready", func(t *testing.T) {
		a := tenantIn("a", unreachable, func(f *facts) {
			f.cp.Spec.Maintenance = &lodiav1alpha1.Maintenance{Window: &lodiav1alpha1.MaintenanceWindow{Begin: "12:30", End: "13:30"}}
			f.cp.Status.Target.ScheduledFor = &metav1.Time{Time: present}
		})
		z := tenantIn("z")
		among(&z, a)
		adopt(z.cp, expectGrant(t, z, granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: &present, requeue: fleetRequeue}))
		among(&a, z)
		adopt(a.cp, expectGrant(t, a, granted{revision: revisionA, reason: lodiav1alpha1.ReasonExternalProbeFailed, message: "Reachable: api: refused"}))
		when(present.Add(fleetRequeue))(&z)
		among(&z, a)
		expectGrant(t, z, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, takesSlot: true, slot: true})
	})
	t.Run("and comes back behind those queued meanwhile", func(t *testing.T) {
		x := tenantIn("x", slotted(lodiav1alpha1.ReasonMaintaining, 3), func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB })
		first, second, third := present.Add(-30*time.Minute), present.Add(-20*time.Minute), present.Add(-10*time.Minute)
		a := tenantIn("a", scheduled(first), progressingAs(lodiav1alpha1.ReasonFleetLimit, 3), unreachable, when(second))
		among(&a, x)
		adopt(a.cp, expectGrant(t, a, granted{revision: revisionA, reason: lodiav1alpha1.ReasonExternalProbeFailed, message: "Reachable: api: refused"}))
		b := tenantIn("b", when(second))
		among(&b, x, a)
		adopt(b.cp, expectGrant(t, b, granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: &second, requeue: fleetRequeue}))
		reachable(&a)
		when(third)(&a)
		among(&a, x, b)
		adopt(a.cp, expectGrant(t, a, granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: &third, requeue: fleetRequeue}))

		x.cp.Status.Maintenance = nil
		when(present)(&a)
		among(&a, x, b)
		adopt(a.cp, expectGrant(t, a, granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: &third, requeue: fleetRequeue}))
		when(present)(&b)
		among(&b, x, a)
		expectGrant(t, b, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, takesSlot: true, slot: true})
	})
}

func TestADeadlineThatStandsBeforeTheGrantKeepsItWaiting(t *testing.T) {
	pastDeadline := condition(metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonProgressDeadlineExceeded, Message: "kube-apiserver: ReplicaSet has timed out progressing"})
	f := held(inWindow("12:00", "13:00", ""), pastDeadline, scheduled(present.Add(-time.Minute)))
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionA, reason: lodiav1alpha1.ReasonProgressDeadlineExceeded, message: "APIServerReady: kube-apiserver: ReplicaSet has timed out progressing"}))
	condition(metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionTrue, Reason: "Fine"})(&f)
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, takesSlot: true, slot: true}))
	pastDeadline(&f)
	expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintenanceFailed, message: "the change Lodia started failed: APIServerReady: ProgressDeadlineExceeded: kube-apiserver: ReplicaSet has timed out progressing", slot: true})
}

func TestATenantWaitingAfterTheGrantKeepsTheSlotMaintaining(t *testing.T) {
	f := held(inWindow("12:00", "13:00", ""),
		condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForAccess, Message: "Secret dev-ca-bundle is not controlled by this ControlPlane"}),
		condition(metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForAccess, ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(present.Add(-time.Hour))}))
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, takesSlot: true, slot: true}))
	reportsAll(revisionB)(&f)
	f.cp.Status.Addons.OperatorRevision = revisionA
	for _, after := range []time.Duration{time.Hour, 24 * time.Hour} {
		f.now = present.Add(after)
		adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, message: "the addons: reports operator revision " + revisionA + ", not " + revisionB, slot: true}))
	}

	ends := f
	ends.cp = f.cp.DeepCopy()
	condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonApplied})(&ends)
	reportsAll(revisionB)(&ends)
	expectGrant(t, ends, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, settlingSince: new(f.now), requeue: settling, slot: true})

	edited := f
	edited.cp = f.cp.DeepCopy()
	edited.cp.Generation = 4
	expectGrant(t, edited, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, message: "the addons: reports operator revision " + revisionA + ", not " + revisionB, slot: true})
}

func TestATokenRemovedAndSetAgainIsNotHandledTwice(t *testing.T) {
	nextStart := time.Date(2026, 11, 9, 13, 0, 0, 0, time.UTC)
	f := held(closedWindow, requested("t1"), handled("t0"))
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut, lastHandled: "t1"}))
	reportsAll(revisionB)(&f)
	f.cp.Annotations = nil
	f.revision = revisionC
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonWaitingForWindow, lastHandled: "t1", scheduledFor: &nextStart, requeue: 30 * time.Minute}))
	requested("t1")(&f)
	expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonWaitingForWindow, lastHandled: "t1", scheduledFor: &nextStart, requeue: 30 * time.Minute})
}

func TestRuleTwoKeepsTheSlotSettling(t *testing.T) {
	tenSecondsAgo := present.Add(-10 * time.Second)
	outside := func(f *facts) {
		f.cp.Spec.Version, f.cp.Status.Target.Version, f.cp.Status.Target.OperatorRevision = "v1.34.9", "v1.34.9", revisionB
		for _, c := range []*lodiav1alpha1.ComponentStatus{f.cp.Status.Components.APIServer, f.cp.Status.Components.ControllerManager, f.cp.Status.Components.Scheduler} {
			c.Version = "v1.34.9"
		}
		f.cp.Status.Addons.Version = "v1.34.9"
	}
	f := held(slotted(lodiav1alpha1.ReasonMaintaining, 3), outside, reportsAll(revisionB), running(revisionC), settlingAt(tenSecondsAgo))
	expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonUnsupported, settlingSince: &tenSecondsAgo, requeue: settling - 10*time.Second, slot: true})
	f.now = present.Add(settling)
	expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonUnsupported})
}

func TestAnOwnersEditOrAPauseOfAFailedTenantKeepsTheNextReleaseReachingIt(t *testing.T) {
	hourAgo := present.Add(-time.Hour)
	failedByB := func(t *testing.T) facts {
		f := held(slotted(lodiav1alpha1.ReasonMaintenanceFailed, 3), func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB }, reportsAll(revisionB), settlingAt(hourAgo),
			condition(metav1.Condition{Type: lodiav1alpha1.ConditionReachable, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, Message: "api: refused"}),
			condition(metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, ObservedGeneration: 3, LastTransitionTime: metav1.NewTime(hourAgo)}))
		adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintenanceFailed, settlingSince: &hourAgo, slot: true}))
		return f
	}
	stillFailed := granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintenanceFailed, message: "Ready is not True 45s after operator revision " + revisionB + " rolled out", settlingSince: &hourAgo, slot: true}
	paused := granted{revision: revisionB, reason: lodiav1alpha1.ReasonPaused, settlingSince: &hourAgo, slot: true}
	fixed := granted{revision: revisionC, reason: lodiav1alpha1.ReasonMaintaining, message: "operator revision " + revisionC + " rolls: kube-apiserver: reports operator revision " + revisionB + ", not " + revisionC, slot: true}
	t.Run("an owner's edit while release B runs", func(t *testing.T) {
		f := failedByB(t)
		f.cp.Generation = 4
		adopt(f.cp, expectGrant(t, f, stillFailed))
		for i := range f.cp.Status.Conditions {
			f.cp.Status.Conditions[i].ObservedGeneration = 4
		}
		adopt(f.cp, expectGrant(t, f, stillFailed))
		f.revision = revisionC
		adopt(f.cp, expectGrant(t, f, fixed))
		f.now = present.Add(24 * time.Hour)
		expectGrant(t, f, fixed)
	})
	t.Run("a pause and an unpause while release B runs", func(t *testing.T) {
		f := failedByB(t)
		f.cp.Spec.Paused, f.cp.Generation = true, 4
		adopt(f.cp, expectGrant(t, f, paused))
		f.cp.Spec.Paused, f.cp.Generation = false, 5
		adopt(f.cp, expectGrant(t, f, stillFailed))
		f.revision = revisionC
		expectGrant(t, f, fixed)
	})
	t.Run("release C while paused, granted at the unpause", func(t *testing.T) {
		f := failedByB(t)
		f.cp.Spec.Paused, f.cp.Generation = true, 4
		f.revision = revisionC
		adopt(f.cp, expectGrant(t, f, paused))
		f.cp.Spec.Paused, f.cp.Generation = false, 5
		expectGrant(t, f, fixed)
	})
}

func TestAnOwnersEditOfATenantInASlotRollsInItAndSettlesIt(t *testing.T) {
	f := held(slotted(lodiav1alpha1.ReasonMaintaining, 3), func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB })
	f.cp.Generation = 4
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, message: "the addons: reports operator revision " + revisionA, slot: true}))
	for i := range f.cp.Status.Conditions {
		f.cp.Status.Conditions[i].ObservedGeneration = 4
	}
	reportsAll(revisionB)(&f)
	*f.cp.Status.Components.APIServer.Updated = 0
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, message: "kube-apiserver: 0 of 1 replicas updated", slot: true}))

	refused := f
	refused.cp = f.cp.DeepCopy()
	condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, Message: "the tenant refuses the edit", ObservedGeneration: 4})(&refused)
	expectGrant(t, refused, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintenanceFailed, message: "the change Lodia started failed: AddonsApplied: Refused: the tenant refuses the edit", slot: true})

	*f.cp.Status.Components.APIServer.Updated = 1
	adopt(f.cp, expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, settlingSince: new(now.Rfc3339Copy().Time), requeue: settling, slot: true}))
	f.now = present.Add(settling)
	expectGrant(t, f, granted{revision: revisionB, reason: lodiav1alpha1.ReasonIdle})
}

func TestAPausedTenantInASlotCountsSoUnpausingStaysWithinTheLimit(t *testing.T) {
	nowSecond := now.Rfc3339Copy().Time
	x := tenantIn("x", slotted(lodiav1alpha1.ReasonMaintaining, 3), func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB })
	x.cp.Spec.Paused, x.cp.Generation = true, 4
	adopt(x.cp, expectGrant(t, x, granted{revision: revisionB, reason: lodiav1alpha1.ReasonPaused, slot: true}))
	y := tenantIn("y")
	among(&y, x)
	adopt(y.cp, expectGrant(t, y, granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: &nowSecond, requeue: fleetRequeue}))

	x.cp.Spec.Paused, x.cp.Generation = false, 5
	among(&x, y)
	adopt(x.cp, expectGrant(t, x, granted{revision: revisionB, reason: lodiav1alpha1.ReasonMaintaining, slot: true}))
	among(&y, x)
	expectGrant(t, y, granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: &nowSecond, requeue: fleetRequeue})
}

func TestEveryPathThatDoesNotWaitRemovesScheduledFor(t *testing.T) {
	pastDeadline := condition(metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonProgressDeadlineExceeded, Message: "past the deadline"})
	for _, c := range []struct {
		name   string
		change func(*facts)
		want   granted
	}{
		{"nothing pending: the running revision is the granted one", running(revisionA),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonIdle}},
		{"rule 2: the running revision cannot render a report", func(f *facts) { f.cp.Status.Addons.Version = "v1.34.9" },
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonUnsupported}},
		{"rule 3: a token", requested("t1"),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut, lastHandled: "t1"}},
		{"rule 3: APIServerReady False WaitingForTarget", condition(metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForTarget}),
			granted{revision: revisionB, reason: lodiav1alpha1.ReasonRollingOut}},
		{"rule 4: Failed", func(f *facts) {
			pastDeadline(f)
			f.cp.Status.Components.Scheduler.OperatorRevision = revisionC
		}, granted{revision: revisionA, reason: lodiav1alpha1.ReasonFailed}},
		{"rule 5: TimeZoneUnsupported", inWindow("12:00", "13:00", "Mars/Olympus_Mons"),
			granted{revision: revisionA, reason: lodiav1alpha1.ReasonTimeZoneUnsupported}},
	} {
		t.Run(c.name, func(t *testing.T) {
			x := tenantIn("x", scheduled(present.Add(-time.Hour)), progressingAs(lodiav1alpha1.ReasonFleetLimit, 3), c.change)
			adopt(x.cp, expectGrant(t, x, c.want))
			y := tenantIn("y")
			if x.revision == revisionA {
				y = tenantIn("y", running(revisionA), func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB })
			}
			among(&y, x)
			expectGrant(t, y, granted{revision: y.revision, reason: lodiav1alpha1.ReasonMaintaining, takesSlot: true, slot: true})
		})
	}
}

func TestATenantOfTheSameNamespaceCounts(t *testing.T) {
	nowSecond := now.Rfc3339Copy().Time
	named := func(name string) func(*facts) { return func(f *facts) { f.cp.Name = name } }
	for _, c := range []struct {
		name  string
		other facts
	}{
		{"in the only slot", tenantIn("team", named("dev"), slotted(lodiav1alpha1.ReasonMaintaining, 3), func(f *facts) { f.cp.Status.Target.OperatorRevision = revisionB })},
		{"queued before it", tenantIn("team", named("qa"), scheduled(present.Add(-time.Hour)), progressingAs(lodiav1alpha1.ReasonFleetLimit, 3))},
	} {
		t.Run(c.name, func(t *testing.T) {
			prod := tenantIn("team", named("prod"))
			among(&prod, c.other)
			expectGrant(t, prod, granted{revision: revisionA, reason: lodiav1alpha1.ReasonFleetLimit, scheduledFor: &nowSecond, requeue: fleetRequeue})
		})
	}
}
