// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

var (
	started = metav1.NewTime(time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC))
	present = time.Date(2026, 11, 9, 12, 30, 0, 0, time.UTC)
	now     = metav1.NewTime(present)
)

const uid = "11111111-2222-3333-4444-555555555555"

type world struct {
	spec, target        string
	revision            string
	step                lodiav1alpha1.UpgradeStep
	since               *metav1.Time
	reports             [4]string
	notReady            string
	oldestKubelet       string
	nodesAbsent         bool
	held                bool
	generation, condGen int64
	cond                *metav1.Condition
	rolling             bool
	paused, deleting    bool
	backupReason        string
	backupStatus        metav1.ConditionStatus
	backupAbsentReady   bool
	backupDeleting      bool
}

func at(version string) [4]string { return [4]string{version, version, version, version} }

func (w world) controlPlane() *lodiav1alpha1.ControlPlane {
	if w.generation == 0 {
		w.generation = 3
	}
	if w.condGen == 0 {
		w.condGen = w.generation
	}
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team", UID: uid, Generation: w.generation},
		Spec:       lodiav1alpha1.ControlPlaneSpec{Version: w.spec, Paused: w.paused},
	}
	if w.deleting {
		cp.DeletionTimestamp = &started
	}
	if w.target != "" || w.revision != "" {
		cp.Status.Target = &lodiav1alpha1.Target{Version: w.target, OperatorRevision: w.revision}
	}
	if w.step != "" {
		since := started
		if w.since != nil {
			since = *w.since
		}
		cp.Status.Kubernetes = &lodiav1alpha1.KubernetesStatus{Upgrade: &lodiav1alpha1.UpgradeStatus{Step: w.step, StartedAt: since}}
	}
	component := func(version string) *lodiav1alpha1.ComponentStatus {
		updated := int32(1)
		if w.rolling {
			updated = 0
		}
		c := &lodiav1alpha1.ComponentStatus{RolloutCount: lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: 1, Ready: 1}, Updated: &updated}}
		c.Version = version
		return c
	}
	cp.Status.Components = &lodiav1alpha1.ComponentsStatus{APIServer: component(w.reports[0]), ControllerManager: component(w.reports[1]), Scheduler: component(w.reports[2])}
	if w.reports[3] != "" {
		cp.Status.Addons = &lodiav1alpha1.Rendered{Version: w.reports[3]}
	}
	for _, conditionType := range []string{
		lodiav1alpha1.ConditionCertificatesReady, lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ConditionAPIServerReady,
		lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ConditionReachable, lodiav1alpha1.ConditionControllersLeading,
	} {
		c := metav1.Condition{Type: conditionType, Status: metav1.ConditionTrue, Reason: "Fine", ObservedGeneration: w.condGen}
		if conditionType == lodiav1alpha1.ConditionDatastoreReady {
			c.ObservedGeneration = w.generation
		}
		if conditionType == w.notReady {
			c.Status, c.Reason = metav1.ConditionFalse, "NotYet"
		}
		cp.Status.Conditions = append(cp.Status.Conditions, c)
	}
	if w.cond != nil {
		meta.RemoveStatusCondition(&cp.Status.Conditions, w.cond.Type)
		cp.Status.Conditions = append(cp.Status.Conditions, *w.cond)
	}
	if w.held {
		cp.Status.Conditions = append(cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionControllerManagerHeld, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonNodesSilent})
	}
	if !w.nodesAbsent {
		cp.Status.Nodes = &lodiav1alpha1.NodesStatus{OldestKubelet: w.oldestKubelet}
	}
	return cp
}

func (w world) facts() facts {
	f := facts{cp: w.controlPlane(), now: present}
	if w.backupReason != "" || w.backupAbsentReady || w.backupDeleting {
		backup := &lodiav1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: lodiav1alpha1.PreUpgradeBackupName("dev", backupMinor(f.cp), uid), Namespace: "team", CreationTimestamp: started}}
		if w.backupDeleting {
			backup.DeletionTimestamp = &started
		}
		if w.backupReason != "" {
			status := w.backupStatus
			if status == "" {
				status = metav1.ConditionUnknown
			}
			backup.Status.Conditions = []metav1.Condition{{Type: lodiav1alpha1.ConditionReady, Status: status, Reason: w.backupReason, Message: w.backupReason + " says so"}}
		}
		f.backup = backup
	}
	return f
}

type want struct {
	target, reason string
	message, lacks string
	step           lodiav1alpha1.UpgradeStep
	since          *metav1.Time
	noUpgrade      bool
	backup         string
	version        string
}

const name136 = "dev-pre-upgrade-v1.36-" + uid
const name137 = "dev-pre-upgrade-v1.37-" + uid

func check(t *testing.T, f facts, w want) {
	t.Helper()
	before := f.cp.DeepCopy()
	d := decide(f)
	if !equality.Semantic.DeepEqual(before, f.cp) {
		t.Errorf("decide changed its facts")
	}
	got := d.status
	target := ""
	if got.Target != nil {
		target = got.Target.Version
	}
	if target != w.target {
		t.Errorf("target is %q, want %q", target, w.target)
	}
	var upgrade *lodiav1alpha1.UpgradeStatus
	version := ""
	if got.Kubernetes != nil {
		upgrade, version = got.Kubernetes.Upgrade, got.Kubernetes.Version
	}
	if version != w.version {
		t.Errorf("kubernetes.version is %q, want %q", version, w.version)
	}
	switch {
	case w.noUpgrade && upgrade != nil:
		t.Errorf("upgrade is %+v, want it absent", upgrade)
	case !w.noUpgrade && upgrade == nil:
		t.Errorf("upgrade is absent, want step %s", w.step)
	case !w.noUpgrade && upgrade.Step != w.step:
		t.Errorf("step is %s, want %s", upgrade.Step, w.step)
	case !w.noUpgrade && w.since != nil && !upgrade.StartedAt.Equal(w.since):
		t.Errorf("startedAt is %s, want %s", upgrade.StartedAt, w.since)
	}
	if d.createBackup != w.backup {
		t.Errorf("creates Backup %q, want %q", d.createBackup, w.backup)
	}
	c := meta.FindStatusCondition(got.Conditions, lodiav1alpha1.ConditionProgressing)
	if c == nil || c.Reason != w.reason {
		t.Fatalf("Progressing is %+v, want %s", c, w.reason)
	}
	if status := progressingStatus(w.reason); c.Status != status {
		t.Errorf("Progressing is %s with reason %s, want %s", c.Status, c.Reason, status)
	}
	if w.message != "" && !strings.Contains(c.Message, w.message) {
		t.Errorf("Progressing's message is %q, want it to say %q", c.Message, w.message)
	}
	if w.lacks != "" && strings.Contains(c.Message, w.lacks) {
		t.Errorf("Progressing's message is %q, which must not say %q", c.Message, w.lacks)
	}
}

func progressingStatus(reason string) metav1.ConditionStatus {
	switch reason {
	case lodiav1alpha1.ReasonUpgrading, lodiav1alpha1.ReasonRollingOut, lodiav1alpha1.ReasonLatestGenerationNotObserved, lodiav1alpha1.ReasonMaintaining:
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

var startedNow = &now

func at4(a, b, c, d string) [4]string { return [4]string{a, b, c, d} }

func TestDecide(t *testing.T) {
	t0 := &started
	const v1361, v1362 = "v1.36.1", "v1.36.2"
	refused := &metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, ObservedGeneration: 3, Message: "the tenant refuses"}
	pastDeadline := &metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonProgressDeadlineExceeded, ObservedGeneration: 3, Message: "past the deadline"}
	staleDatastore := &metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionTrue, Reason: "Fine", ObservedGeneration: 2}
	cases := []struct {
		name string
		w    world
		want want
	}{
		{"a new ControlPlane takes spec.version with no upgrade", world{spec: "v1.36.6", reports: at("")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonRollingOut, noUpgrade: true}},
		{"a patch edit moves the target with upgrade APIServer now", world{spec: "v1.36.6", target: "v1.36.4", reports: at("v1.36.4"), notReady: lodiav1alpha1.ConditionDatastoreReady, oldestKubelet: "v1.30.0"},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: startedNow, version: "v1.36.4"}},
		{"a patch edit keeps startedAt of a change under way", world{spec: "v1.36.6", target: "v1.36.5", step: lodiav1alpha1.UpgradeStepAddons, reports: at("v1.36.5")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: t0, version: "v1.36.5"}},
		{"a patch edit at step APIServer moves the target and keeps startedAt", world{spec: "v1.36.6", target: "v1.36.5", step: lodiav1alpha1.UpgradeStepAPIServer, reports: at("v1.36.5")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: t0, version: "v1.36.5"}},
		{"a patch edit at step ControllerManager moves the target and keeps startedAt", world{spec: "v1.36.6", target: "v1.36.5", step: lodiav1alpha1.UpgradeStepControllerManager, reports: at("v1.36.5")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: t0, version: "v1.36.5"}},
		{"a patch edit at step Scheduler moves the target and keeps startedAt", world{spec: "v1.36.6", target: "v1.36.5", step: lodiav1alpha1.UpgradeStepScheduler, reports: at("v1.36.5")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: t0, version: "v1.36.5"}},
		{"a patch downgrade moves the target down", world{spec: "v1.36.1", target: "v1.36.2", reports: at("v1.36.2")},
			want{target: "v1.36.1", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: startedNow, version: "v1.36.2"}},
		{"a downgrade's API server, reported early, reads step ControllerManager", world{spec: v1361, target: v1361, step: lodiav1alpha1.UpgradeStepAPIServer, reports: at4(v1361, v1362, v1362, v1362)},
			want{target: v1361, reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepControllerManager, since: t0, version: v1361}},
		{"step is the first part not at the target", world{spec: "v1.37.1", target: "v1.37.1", step: lodiav1alpha1.UpgradeStepAPIServer, reports: at4("v1.37.1", "v1.36.6", "v1.36.6", "v1.36.6")},
			want{target: "v1.37.1", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepControllerManager, since: t0, version: "v1.36.6"}},
		{"the scheduler after the controller manager", world{spec: "v1.37.1", target: "v1.37.1", step: lodiav1alpha1.UpgradeStepControllerManager, reports: at4("v1.37.1", "v1.37.1", "v1.36.6", "v1.36.6")},
			want{target: "v1.37.1", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepScheduler, since: t0, version: "v1.36.6"}},
		{"then the addons", world{spec: "v1.37.1", target: "v1.37.1", step: lodiav1alpha1.UpgradeStepScheduler, reports: at4("v1.37.1", "v1.37.1", "v1.37.1", "v1.36.6")},
			want{target: "v1.37.1", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAddons, since: t0, version: "v1.36.6"}},
		{"upgrade goes when all four are at the target", world{spec: "v1.37.1", target: "v1.37.1", step: lodiav1alpha1.UpgradeStepAddons, reports: at("v1.37.1")},
			want{target: "v1.37.1", reason: lodiav1alpha1.ReasonIdle, noUpgrade: true, version: "v1.37.1"}},
		{"an edit to a later minor at step APIServer waits for the step", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepAPIServer, reports: at4("v1.36.5", "v1.36.5", "v1.36.5", "v1.36.5")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: t0, version: "v1.36.5"}},
		{"an edit to a later minor at step ControllerManager waits for the step", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepControllerManager, reports: at4("v1.36.6", "v1.36.5", "v1.36.5", "v1.36.5")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepControllerManager, since: t0, version: "v1.36.5"}},
		{"an edit to a later minor at step Scheduler waits for the step", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepScheduler, reports: at4("v1.36.6", "v1.36.6", "v1.36.5", "v1.36.5")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepScheduler, since: t0, version: "v1.36.5"}},
		{"an edit to a later minor at step Addons waits for the step", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepAddons, reports: at4("v1.36.6", "v1.36.6", "v1.36.6", "v1.36.5")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAddons, since: t0, version: "v1.36.5"}},
		{"a further minor starts at the preflight with a new startedAt when the last part arrives", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepAddons, reports: at("v1.36.6")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: startedNow, backup: name137, version: "v1.36.6", message: name137}},
		{"a further minor failing the preflight drops upgrade", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepAddons, reports: at("v1.36.6"), held: true},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonControllerManagerHeld, noUpgrade: true, version: "v1.36.6"}},
		{"a patch edit at step Backup keeps Backup and its Backup", world{spec: "v1.37.2", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonCopying},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: lodiav1alpha1.ReasonCopying, version: "v1.36.6"}},
		{"a patch edit at step Backup moves the target to the current next", world{spec: "v1.37.2", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonCompleted, backupStatus: metav1.ConditionTrue},
			want{target: "v1.37.2", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: t0, version: "v1.36.6"}},
		{"an edit to a later minor at step Backup goes through the next minor's latest", world{spec: "v1.37.1", target: "v1.35.10", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.35.10"), backupReason: lodiav1alpha1.ReasonCompleted, backupStatus: metav1.ConditionTrue},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: t0, version: "v1.35.10"}},
		{"an edit to an unsupported minor at step Backup stops there", world{spec: "v1.38.0", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUnsupported, step: lodiav1alpha1.UpgradeStepBackup, since: t0, version: "v1.36.6"}},
		{"an edit to an unsupported minor leaves the rollout under way to finish", world{spec: "v1.38.0", target: "v1.37.1", step: lodiav1alpha1.UpgradeStepScheduler, reports: at4("v1.37.1", "v1.37.1", "v1.36.6", "v1.36.6")},
			want{target: "v1.37.1", reason: lodiav1alpha1.ReasonUnsupported, step: lodiav1alpha1.UpgradeStepScheduler, since: t0, version: "v1.36.6"}},
		{"an edit to an unsupported minor still derives the step", world{spec: "v1.38.0", target: "v1.37.1", step: lodiav1alpha1.UpgradeStepAPIServer, reports: at4("v1.37.1", "v1.36.6", "v1.36.6", "v1.36.6")},
			want{target: "v1.37.1", reason: lodiav1alpha1.ReasonUnsupported, step: lodiav1alpha1.UpgradeStepControllerManager, since: t0, version: "v1.36.6"}},
		{"an edit to an unsupported minor drops upgrade once all four are at the target", world{spec: "v1.38.0", target: "v1.37.1", step: lodiav1alpha1.UpgradeStepAPIServer, reports: at("v1.37.1")},
			want{target: "v1.37.1", reason: lodiav1alpha1.ReasonUnsupported, noUpgrade: true, version: "v1.37.1"}},
		{"a later minor waits until all four parts are at the target", world{spec: "v1.37.1", target: "v1.36.2", reports: at4("v1.36.2", "v1.36.4", "v1.36.4", "v1.36.2")},
			want{target: "v1.36.2", reason: lodiav1alpha1.ReasonRollingOut, noUpgrade: true, message: "minor step towards v1.37.1 waits until every part runs v1.36.2", version: "v1.36.2"}},
		{"a tenant waiting only for reports reads RollingOut", world{spec: "v1.37.1", target: "v1.36.6", reports: at4("v1.36.6", "v1.36.6", "v1.36.6", ""), oldestKubelet: "v1.36.6"},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonRollingOut, noUpgrade: true, message: "minor step", version: ""}},
		{"the preflight passes: upgrade Backup now, the Backup created", world{spec: "v1.37.1", target: "v1.36.6", reports: at("v1.36.6"), oldestKubelet: "v1.36.6"},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: startedNow, backup: name137, version: "v1.36.6"}},
		{"the next minor outside the table is the preflight's first", world{spec: "v1.35.1", target: "v1.33.9", reports: at("v1.33.9"), notReady: lodiav1alpha1.ConditionDatastoreReady, held: true},
			want{target: "v1.33.9", reason: lodiav1alpha1.ReasonUnsupported, noUpgrade: true, message: "the next minor, 1.34", version: "v1.33.9"}},
		{"DatastoreReady not True is the second", world{spec: "v1.37.1", target: "v1.36.6", reports: at("v1.36.6"), notReady: lodiav1alpha1.ConditionDatastoreReady, oldestKubelet: "v1.30.0", held: true},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonWaitingForDatastore, noUpgrade: true, version: "v1.36.6"}},
		{"DatastoreReady True at an older generation is not enough", world{spec: "v1.37.1", target: "v1.36.6", reports: at("v1.36.6"), cond: staleDatastore},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonWaitingForDatastore, noUpgrade: true, version: "v1.36.6"}},
		{"a kubelet four minors older is the third", world{spec: "v1.37.1", target: "v1.36.6", reports: at("v1.36.6"), oldestKubelet: "v1.33.2", held: true},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonWaitingForNodes, noUpgrade: true, version: "v1.36.6"}},
		{"a kubelet three minors older passes", world{spec: "v1.37.1", target: "v1.36.6", reports: at("v1.36.6"), oldestKubelet: "v1.34.2"},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: startedNow, backup: name137, version: "v1.36.6"}},
		{"no report of nodes waits for the first list", world{spec: "v1.37.1", target: "v1.36.6", reports: at("v1.36.6"), nodesAbsent: true},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonWaitingForNodes, noUpgrade: true, message: "the nodes are not listed yet", version: "v1.36.6"}},
		{"a tenant listed with no nodes passes", world{spec: "v1.37.1", target: "v1.36.6", reports: at("v1.36.6")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: startedNow, backup: name137, version: "v1.36.6"}},
		{"a hold is the fourth", world{spec: "v1.37.1", target: "v1.36.6", reports: at("v1.36.6"), held: true},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonControllerManagerHeld, noUpgrade: true, version: "v1.36.6"}},
		{"a Backup absent is created", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6")},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: t0, backup: name137, version: "v1.36.6"}},
		{"a hold, a Datastore and a kubelet arriving after the preflight stall nothing", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), held: true, notReady: lodiav1alpha1.ConditionDatastoreReady, oldestKubelet: "v1.30.0"},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: t0, backup: name137, version: "v1.36.6"}},
		{"a minor step's Backup named for the next minor", world{spec: "v1.37.1", target: "v1.35.10", reports: at("v1.35.10"), oldestKubelet: "v1.35.10"},
			want{target: "v1.35.10", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: startedNow, backup: name136, version: "v1.35.10"}},
		{"Backup absent reported nothing yet", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupAbsentReady: true},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: "reported nothing yet", version: "v1.36.6"}},
		{"Backup being deleted waits for it to go", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupDeleting: true, backupReason: lodiav1alpha1.ReasonCompleted, backupStatus: metav1.ConditionTrue},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: "being deleted", version: "v1.36.6"}},
		{"Backup Completed moves the target", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonCompleted, backupStatus: metav1.ConditionTrue},
			want{target: "v1.37.1", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: t0, version: "v1.36.6"}},
		{"Backup Completed under a Refused condition stays, Failed", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonCompleted, backupStatus: metav1.ConditionTrue, cond: refused},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonFailed, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: "the tenant refuses", version: "v1.36.6"}},
		{"Backup Completed under a rollout past its deadline stays, Failed", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonCompleted, backupStatus: metav1.ConditionTrue, cond: pastDeadline},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonFailed, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: "past the deadline", version: "v1.36.6"}},
		{"Backup NoBackupStore under a Refused condition stays, Failed", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonNoBackupStore, backupStatus: metav1.ConditionFalse, cond: refused},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonFailed, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: "the tenant refuses", version: "v1.36.6"}},
		{"Backup NoBackupStore moves the target and says so", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonNoBackupStore, backupStatus: metav1.ConditionFalse},
			want{target: "v1.37.1", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: t0, version: "v1.36.6", message: "no pre-upgrade Backup was taken"}},
		{"a later patch of that minor does not repeat that no Backup was taken", world{spec: "v1.37.2", target: "v1.37.2", step: lodiav1alpha1.UpgradeStepAPIServer, since: startedNow, reports: at("v1.37.1"), backupReason: lodiav1alpha1.ReasonNoBackupStore, backupStatus: metav1.ConditionFalse},
			want{target: "v1.37.2", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: startedNow, version: "v1.37.1", lacks: "no pre-upgrade Backup was taken"}},
		{"Backup Failed is Failed, and deleting it takes another", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonFailed, backupStatus: metav1.ConditionFalse},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonFailed, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: "delete it", version: "v1.36.6"}},
		{"Backup SealLost is Failed", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonSealLost, backupStatus: metav1.ConditionFalse},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonFailed, step: lodiav1alpha1.UpgradeStepBackup, since: t0, version: "v1.36.6"}},
		{"Backup Unsupported is Failed", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonUnsupported, backupStatus: metav1.ConditionFalse},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonFailed, step: lodiav1alpha1.UpgradeStepBackup, since: t0, version: "v1.36.6"}},
		{"Backup WaitingForDatastore is quoted", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonWaitingForDatastore, backupStatus: metav1.ConditionFalse},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: lodiav1alpha1.ReasonWaitingForDatastore + ": ", version: "v1.36.6"}},
		{"Backup Snapshotting is quoted", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonSnapshotting},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: lodiav1alpha1.ReasonSnapshotting + ": ", version: "v1.36.6"}},
		{"Backup Copying is quoted", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonCopying},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: lodiav1alpha1.ReasonCopying + ": ", version: "v1.36.6"}},
		{"Backup ObjectsControlledElsewhere is quoted", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonObjectsControlledElsewhere, backupStatus: metav1.ConditionFalse},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: lodiav1alpha1.ReasonObjectsControlledElsewhere + ": ", version: "v1.36.6"}},
		{"Backup ControlPlaneNotFound is quoted", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: lodiav1alpha1.ReasonControlPlaneNotFound, backupStatus: metav1.ConditionFalse},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: lodiav1alpha1.ReasonControlPlaneNotFound + ": ", version: "v1.36.6"}},
		{"a next minor that left the table while the step stands is Unsupported, and the target stays", world{spec: "v1.35.1", target: "v1.33.9", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.33.9"), backupReason: lodiav1alpha1.ReasonCompleted, backupStatus: metav1.ConditionTrue},
			want{target: "v1.33.9", reason: lodiav1alpha1.ReasonUnsupported, step: lodiav1alpha1.UpgradeStepBackup, since: t0, message: "the next minor, 1.34", version: "v1.33.9"}},
		{"an older minor than the target stays", world{spec: "v1.36.1", target: "v1.37.1", reports: at("v1.37.1")},
			want{target: "v1.37.1", reason: lodiav1alpha1.ReasonUnsupported, noUpgrade: true, version: "v1.37.1"}},
		{"Paused carries the target and the step, and creates no Backup", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), paused: true},
			want{target: "v1.36.6", reason: lodiav1alpha1.ReasonPaused, step: lodiav1alpha1.UpgradeStepBackup, since: t0, version: "v1.36.6"}},
		{"Deleting carries the target and the step, and creates no Backup", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), deleting: true},
			want{target: "v1.36.6", reason: "", step: lodiav1alpha1.UpgradeStepBackup, since: t0, version: "v1.36.6"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := c.w.facts()
			if c.want.reason == "" {
				f.cp.Status.Conditions = append(f.cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionProgressing, Reason: "Kept", Status: metav1.ConditionFalse})
				c.want.reason = "Kept"
			}
			check(t, f, c.want)
		})
	}
}

func TestFailedAndItsClearing(t *testing.T) {
	const version = "v1.37.1"
	base := world{spec: version, target: version, step: lodiav1alpha1.UpgradeStepAPIServer, reports: at4("v1.37.0", "v1.37.0", "v1.37.0", "v1.37.0")}
	for _, c := range []struct {
		name string
		cond metav1.Condition
	}{
		{"Refused", metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, ObservedGeneration: 3, Message: "the tenant refuses"}},
		{"ObjectsControlledElsewhere", metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonObjectsControlledElsewhere, ObservedGeneration: 3, Message: "held elsewhere"}},
		{"Unsupported", metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonUnsupported, ObservedGeneration: 3, Message: "not built"}},
		{"ProgressDeadlineExceeded available", metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonProgressDeadlineExceeded, ObservedGeneration: 3, Message: "past the deadline"}},
		{"ProgressDeadlineExceeded unavailable", metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonProgressDeadlineExceeded, ObservedGeneration: 3, Message: "past the deadline"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := base
			w.cond = &c.cond
			check(t, w.facts(), want{target: version, reason: lodiav1alpha1.ReasonFailed, step: lodiav1alpha1.UpgradeStepAPIServer, since: &started, message: c.cond.Message, version: "v1.37.0"})
			w.cond = nil
			check(t, w.facts(), want{target: version, reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepAPIServer, since: &started, version: "v1.37.0"})
		})
	}
	for _, reason := range []string{lodiav1alpha1.ReasonFailed, lodiav1alpha1.ReasonSealLost, lodiav1alpha1.ReasonUnsupported} {
		t.Run("the Backup's "+reason+" clears once it is deleted", func(t *testing.T) {
			failed := world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6"), backupReason: reason, backupStatus: metav1.ConditionFalse}
			check(t, failed.facts(), want{target: "v1.36.6", reason: lodiav1alpha1.ReasonFailed, step: lodiav1alpha1.UpgradeStepBackup, since: &started, version: "v1.36.6"})
			failed.backupReason = ""
			check(t, failed.facts(), want{target: "v1.36.6", reason: lodiav1alpha1.ReasonUpgrading, step: lodiav1alpha1.UpgradeStepBackup, since: &started, backup: name137, version: "v1.36.6"})
		})
	}
	t.Run("a standing Refused on an idle tenant is not Failed", func(t *testing.T) {
		w := world{spec: version, target: version, reports: at(version), cond: &metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, ObservedGeneration: 3, Message: "the tenant refuses"}}
		check(t, w.facts(), want{target: version, reason: lodiav1alpha1.ReasonIdle, noUpgrade: true, version: version})
	})
	t.Run("the same Refused on a rollout that is no upgrade is not Failed", func(t *testing.T) {
		w := world{spec: version, target: version, reports: at(version), rolling: true, cond: &metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, ObservedGeneration: 3}}
		check(t, w.facts(), want{target: version, reason: lodiav1alpha1.ReasonRollingOut, noUpgrade: true, version: version})
	})
}

func TestProgressingOrder(t *testing.T) {
	const version = "v1.37.1"
	refused := metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, ObservedGeneration: 3}
	for _, c := range []struct {
		name string
		w    world
		want string
	}{
		{"Paused before everything", world{spec: "v1.38.0", target: version, reports: at(version), step: lodiav1alpha1.UpgradeStepAddons, cond: &refused, paused: true}, lodiav1alpha1.ReasonPaused},
		{"Unsupported before Failed", world{spec: "v1.38.0", target: version, reports: at("v1.37.0"), step: lodiav1alpha1.UpgradeStepAPIServer, cond: &refused}, lodiav1alpha1.ReasonUnsupported},
		{"Failed before LatestGenerationNotObserved", world{spec: version, target: version, reports: at("v1.37.0"), step: lodiav1alpha1.UpgradeStepAPIServer, cond: &refused, generation: 4, condGen: 3}, lodiav1alpha1.ReasonFailed},
		{"Failed before Upgrading", world{spec: version, target: version, reports: at("v1.37.0"), step: lodiav1alpha1.UpgradeStepAPIServer, cond: &refused}, lodiav1alpha1.ReasonFailed},
		{"a preflight before LatestGenerationNotObserved", world{spec: "v1.38.0", target: "v1.36.6", reports: at("v1.36.6"), notReady: lodiav1alpha1.ConditionDatastoreReady, generation: 4, condGen: 3}, lodiav1alpha1.ReasonUnsupported},
		{"WaitingForDatastore before LatestGenerationNotObserved", world{spec: version, target: "v1.36.6", reports: at("v1.36.6"), notReady: lodiav1alpha1.ConditionDatastoreReady, generation: 4, condGen: 3}, lodiav1alpha1.ReasonWaitingForDatastore},
		{"WaitingForNodes before LatestGenerationNotObserved", world{spec: version, target: "v1.36.6", reports: at("v1.36.6"), oldestKubelet: "v1.30.0", generation: 4, condGen: 3}, lodiav1alpha1.ReasonWaitingForNodes},
		{"ControllerManagerHeld before LatestGenerationNotObserved", world{spec: version, target: "v1.36.6", reports: at("v1.36.6"), held: true, generation: 4, condGen: 3}, lodiav1alpha1.ReasonControllerManagerHeld},
		{"LatestGenerationNotObserved before Upgrading", world{spec: version, target: version, reports: at("v1.37.0"), step: lodiav1alpha1.UpgradeStepAPIServer, generation: 4, condGen: 3}, lodiav1alpha1.ReasonLatestGenerationNotObserved},
		{"LatestGenerationNotObserved before RollingOut", world{spec: version, target: version, reports: at(version), rolling: true, generation: 4, condGen: 3}, lodiav1alpha1.ReasonLatestGenerationNotObserved},
		{"Upgrading before RollingOut", world{spec: version, target: version, reports: at("v1.37.0"), step: lodiav1alpha1.UpgradeStepAPIServer, rolling: true}, lodiav1alpha1.ReasonUpgrading},
		{"RollingOut before Idle", world{spec: version, target: version, reports: at(version), rolling: true}, lodiav1alpha1.ReasonRollingOut},
		{"Idle last", world{spec: version, target: version, reports: at(version)}, lodiav1alpha1.ReasonIdle},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := decide(c.w.facts())
			got := meta.FindStatusCondition(d.status.Conditions, lodiav1alpha1.ConditionProgressing)
			if got == nil || got.Reason != c.want {
				t.Errorf("Progressing is %+v, want %s", got, c.want)
			}
		})
	}
}

func TestAChainTakesOneMinorAtATime(t *testing.T) {
	const final = "v1.37.1"
	cp := world{spec: final, target: "v1.35.8", reports: at("v1.35.8"), oldestKubelet: "v1.35.8"}.controlPlane()
	var backups = map[string]*lodiav1alpha1.Backup{}
	pass := func(when time.Time) decision {
		t.Helper()
		f := facts{cp: cp.DeepCopy(), now: when}
		f.backup = backups[lodiav1alpha1.PreUpgradeBackupName("dev", backupMinor(cp), uid)]
		d := decide(f)
		cp.Status.Target = d.status.Target
		cp.Status.Kubernetes = d.status.Kubernetes
		for _, c := range d.status.Conditions {
			meta.SetStatusCondition(&cp.Status.Conditions, c)
		}
		return d
	}
	expect := func(step lodiav1alpha1.UpgradeStep, target string) {
		t.Helper()
		if cp.Status.Target.Version != target {
			t.Errorf("target is %s, want %s", cp.Status.Target.Version, target)
		}
		var got lodiav1alpha1.UpgradeStep
		if cp.Status.Kubernetes != nil && cp.Status.Kubernetes.Upgrade != nil {
			got = cp.Status.Kubernetes.Upgrade.Step
		}
		if got != step {
			t.Errorf("step is %q, want %q", got, step)
		}
	}
	setReports := func(api, cm, sched, addons string) {
		cp.Status.Components.APIServer.Version, cp.Status.Components.ControllerManager.Version, cp.Status.Components.Scheduler.Version = api, cm, sched
		cp.Status.Addons = &lodiav1alpha1.Rendered{Version: addons}
	}
	complete := func(name string) {
		backups[name] = &lodiav1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: lodiav1alpha1.BackupStatus{Conditions: []metav1.Condition{{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonCompleted}}}}
	}

	first := present
	if d := pass(first); d.createBackup != name136 {
		t.Fatalf("the first minor step creates %q, want %s", d.createBackup, name136)
	}
	expect(lodiav1alpha1.UpgradeStepBackup, "v1.35.8")
	complete(name136)
	pass(first)
	expect(lodiav1alpha1.UpgradeStepAPIServer, "v1.36.6")
	setReports("v1.36.6", "v1.35.8", "v1.35.8", "v1.35.8")
	pass(first)
	expect(lodiav1alpha1.UpgradeStepControllerManager, "v1.36.6")
	setReports("v1.36.6", "v1.36.6", "v1.36.6", "v1.35.8")
	pass(first)
	expect(lodiav1alpha1.UpgradeStepAddons, "v1.36.6")
	setReports("v1.36.6", "v1.36.6", "v1.36.6", "v1.36.6")
	second := first.Add(time.Hour)
	if d := pass(second); d.createBackup != name137 {
		t.Fatalf("the second minor step creates %q, want %s", d.createBackup, name137)
	}
	expect(lodiav1alpha1.UpgradeStepBackup, "v1.36.6")
	if got := cp.Status.Kubernetes.Upgrade.StartedAt; !got.Time.Equal(second) {
		t.Errorf("the further minor's startedAt is %s, want the new %s", got, second)
	}
	complete(name137)
	pass(second)
	expect(lodiav1alpha1.UpgradeStepAPIServer, final)
	setReports(final, final, final, final)
	if d := pass(second); d.createBackup != "" {
		t.Errorf("the end creates %q", d.createBackup)
	}
	expect("", final)
}

func TestBackupMinor(t *testing.T) {
	for _, c := range []struct {
		name string
		w    world
		want string
	}{
		{"no upgrade and the spec at the target", world{spec: "v1.37.1", target: "v1.37.1", reports: at("v1.37.1")}, ""},
		{"no target", world{spec: "v1.37.1", reports: at("")}, ""},
		{"a minor step starting", world{spec: "v1.37.1", target: "v1.36.6", reports: at("v1.36.6")}, "1.37"},
		{"at step Backup", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepBackup, reports: at("v1.36.6")}, "1.37"},
		{"under way, the target's minor", world{spec: "v1.37.1", target: "v1.37.1", step: lodiav1alpha1.UpgradeStepAPIServer, reports: at("v1.36.6")}, "1.37"},
		{"the last part arrived and a further minor starts", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepAddons, reports: at("v1.36.6")}, "1.37"},
		{"the last part of a patch under a later spec", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepAddons, reports: at4("v1.36.6", "v1.36.6", "v1.36.6", "v1.36.5")}, "1.36"},
	} {
		if got := backupMinor(c.w.controlPlane()); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

const revisionA, revisionB = "v1.0.0+aaaa", "v1.1.0+bbbb"

func adopt(cp *lodiav1alpha1.ControlPlane, d decision) {
	cp.Status.ObservedGeneration = d.status.ObservedGeneration
	cp.Status.Target = d.status.Target
	cp.Status.Kubernetes = d.status.Kubernetes
	cp.Status.Maintenance = d.status.Maintenance
	for _, c := range d.status.Conditions {
		meta.SetStatusCondition(&cp.Status.Conditions, c)
	}
}

func TestDecideWritesTheRunningRevision(t *testing.T) {
	steady := world{spec: "v1.37.1", target: "v1.37.1", reports: at("v1.37.1")}
	for _, c := range []struct {
		name         string
		w            world
		was, running string
		want         *lodiav1alpha1.Target
		writes       bool
	}{
		{"a first pass grants it with the target", world{spec: "v1.37.1", reports: at("")}, "", revisionA,
			&lodiav1alpha1.Target{Version: "v1.37.1", OperatorRevision: revisionA}, true},
		{"a pass that finds it granted writes nothing", steady, revisionA, revisionA,
			&lodiav1alpha1.Target{Version: "v1.37.1", OperatorRevision: revisionA}, false},
		{"a new release replaces its predecessor's in a slot of the fleet", steady, revisionA, revisionB,
			&lodiav1alpha1.Target{Version: "v1.37.1", OperatorRevision: revisionB}, true},
		{"a rollback is one more revision", steady, revisionB, revisionA,
			&lodiav1alpha1.Target{Version: "v1.37.1", OperatorRevision: revisionA}, true},
		{"a version change under way carries it", world{spec: "v1.37.1", target: "v1.36.6", step: lodiav1alpha1.UpgradeStepAddons, reports: at("v1.36.6")}, revisionA, revisionB,
			&lodiav1alpha1.Target{Version: "v1.36.6", OperatorRevision: revisionB}, true},
		{"a spec of a minor Lodia does not run has the revision and no version", world{spec: "v1.34.2", reports: at("")}, "", revisionA,
			&lodiav1alpha1.Target{OperatorRevision: revisionA}, true},
		{"a paused ControlPlane keeps the stored one", world{spec: "v1.37.1", target: "v1.37.1", revision: revisionA, paused: true, reports: at("v1.37.1")}, revisionA, revisionB,
			&lodiav1alpha1.Target{Version: "v1.37.1", OperatorRevision: revisionA}, false},
		{"a paused ControlPlane that holds none is granted none", world{spec: "v1.37.1", paused: true, reports: at("")}, revisionA, revisionB,
			nil, false},
		{"a deleting ControlPlane keeps the stored one", world{spec: "v1.37.1", target: "v1.37.1", revision: revisionA, deleting: true, reports: at("v1.37.1")}, revisionA, revisionB,
			&lodiav1alpha1.Target{Version: "v1.37.1", OperatorRevision: revisionA}, false},
		{"a deleting ControlPlane that holds none is granted none", world{spec: "v1.37.1", deleting: true, reports: at("")}, revisionA, revisionB,
			nil, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := c.w.facts()
			f.limit = 5
			if c.was != "" {
				f.revision = c.was
				adopt(f.cp, decide(f))
			}
			f.revision = c.running
			d := decide(f)
			if !equality.Semantic.DeepEqual(d.status.Target, c.want) {
				t.Errorf("target is %+v, want %+v", d.status.Target, c.want)
			}
			if got := d.differs(f.cp); got != c.writes {
				t.Errorf("differs is %t, want %t", got, c.writes)
			}
			adopt(f.cp, d)
			if again := decide(f); again.differs(f.cp) {
				t.Errorf("a second pass over what the first wrote still differs: %+v", again.status)
			}
		})
	}
}
