// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package statusapply_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/statusapply"
)

const manager = "lodia/test"

var (
	k8s     client.Client
	earlier = metav1.Date(2026, 11, 2, 18, 0, 0, 0, time.UTC)
	later   = metav1.Date(2026, 11, 3, 18, 0, 0, 0, time.UTC)
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Exit(m.Run())
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "api", "crds")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if k8s, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}

func condition(status metav1.ConditionStatus, message string, at metav1.Time) metav1.Condition {
	return metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: status, Reason: lodiav1alpha1.ReasonReconciling, Message: message, ObservedGeneration: 1, LastTransitionTime: at}
}

func namespace(t *testing.T) string {
	t.Helper()
	if k8s == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set; run make test")
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "statusapply-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	return ns.Name
}

func controlPlane(t *testing.T, ns string) *lodiav1alpha1.ControlPlane {
	t.Helper()
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   "v1.35.0",
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint:  lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	if err := k8s.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func stored(t *testing.T, cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.ControlPlane {
	t.Helper()
	got := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), got); err != nil {
		t.Fatal(err)
	}
	return got
}

func apply(ctx context.Context, cp *lodiav1alpha1.ControlPlane, desired lodiav1alpha1.ControlPlaneStatus, changed bool) error {
	return statusapply.Apply(ctx, k8s, k8s, manager, cp, desired, changed)
}

func TestMerge(t *testing.T) {
	previous := condition(metav1.ConditionTrue, "as it was", earlier)
	for _, c := range []struct {
		name        string
		current     []metav1.Condition
		set         metav1.Condition
		wantChanged bool
		wantTime    metav1.Time
	}{
		{"a new condition takes its time", nil, condition(metav1.ConditionFalse, "new", later), true, later},
		{"the same status keeps the earlier time", []metav1.Condition{previous}, condition(metav1.ConditionTrue, "another message", later), true, earlier},
		{"another status takes its time", []metav1.Condition{previous}, condition(metav1.ConditionFalse, "as it was", later), true, later},
		{"nothing new changes nothing", []metav1.Condition{previous}, previous, false, earlier},
	} {
		t.Run(c.name, func(t *testing.T) {
			all, applied, changed, err := statusapply.Merge(c.current, c.set)
			if err != nil {
				t.Fatal(err)
			}
			if changed != c.wantChanged {
				t.Errorf("changed %t, want %t", changed, c.wantChanged)
			}
			got := meta.FindStatusCondition(all, c.set.Type)
			if got == nil || !got.LastTransitionTime.Equal(&c.wantTime) {
				t.Fatalf("condition %+v, want lastTransitionTime %s", got, c.wantTime)
			}
			if len(applied) != 1 || applied[0].(map[string]any)["lastTransitionTime"] != c.wantTime.UTC().Format(time.RFC3339) {
				t.Errorf("applied %v, want one entry at %s", applied, c.wantTime)
			}
		})
	}
}

func TestMergeLeavesCurrentAlone(t *testing.T) {
	current := []metav1.Condition{condition(metav1.ConditionTrue, "as it was", earlier)}
	if _, _, _, err := statusapply.Merge(current, condition(metav1.ConditionFalse, "new", later)); err != nil {
		t.Fatal(err)
	}
	if current[0].Status != metav1.ConditionTrue || current[0].Message != "as it was" {
		t.Errorf("current became %+v", current[0])
	}
}

func TestMergeNamesATimeWhenTheConditionHasNone(t *testing.T) {
	all, _, _, err := statusapply.Merge(nil, condition(metav1.ConditionTrue, "new", metav1.Time{}))
	if err != nil {
		t.Fatal(err)
	}
	if all[0].LastTransitionTime.IsZero() {
		t.Error("lastTransitionTime is zero")
	}
}

func TestDiffersOnEachField(t *testing.T) {
	previous := condition(metav1.ConditionTrue, "m", earlier)
	if statusapply.Differs(&previous, previous) {
		t.Error("a condition differs from itself")
	}
	moved := previous
	moved.LastTransitionTime = later
	if statusapply.Differs(&previous, moved) {
		t.Error("lastTransitionTime alone counts as a difference")
	}
	if !statusapply.Differs(nil, previous) {
		t.Error("no previous condition does not differ")
	}
	for name, change := range map[string]func(*metav1.Condition){
		"status":             func(c *metav1.Condition) { c.Status = metav1.ConditionFalse },
		"reason":             func(c *metav1.Condition) { c.Reason = "Other" },
		"message":            func(c *metav1.Condition) { c.Message = "other" },
		"observedGeneration": func(c *metav1.Condition) { c.ObservedGeneration = 2 },
	} {
		c := previous
		change(&c)
		if !statusapply.Differs(&previous, c) {
			t.Errorf("a different %s does not differ", name)
		}
	}
}

func TestFirstPassIsUnknownAtTheGeneration(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Generation: 3}}
	c := statusapply.FirstPass(cp, lodiav1alpha1.ConditionReady)
	if c.Type != lodiav1alpha1.ConditionReady || c.Status != metav1.ConditionUnknown || c.Reason != lodiav1alpha1.ReasonReconciling || c.Message != "the first pass has not finished" || c.ObservedGeneration != 3 {
		t.Errorf("%+v", c)
	}
}

func TestTrueAt(t *testing.T) {
	for _, c := range []struct {
		name       string
		conditions []metav1.Condition
		want       bool
	}{
		{"none", nil, false},
		{"True at the generation", []metav1.Condition{{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 2}}, true},
		{"True at an earlier generation", []metav1.Condition{{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionTrue, ObservedGeneration: 1}}, false},
		{"False at the generation", []metav1.Condition{{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, ObservedGeneration: 2}}, false},
		{"another condition", []metav1.Condition{{Type: lodiav1alpha1.ConditionProgressing, Status: metav1.ConditionTrue, ObservedGeneration: 2}}, false},
	} {
		cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Generation: 2}, Status: lodiav1alpha1.ControlPlaneStatus{Conditions: c.conditions}}
		if got := statusapply.TrueAt(cp, lodiav1alpha1.ConditionReady); got != c.want {
			t.Errorf("%s: %t, want %t", c.name, got, c.want)
		}
	}
}

func TestWaitKeepsOnlyAnEarlierGeneration(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Generation: 2}}
	waiting := metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: "WaitingForX"}
	earlierGeneration := metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: "Kept", ObservedGeneration: 1}
	current := metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: "Current", ObservedGeneration: 2}
	if got := statusapply.Wait(cp, &earlierGeneration, waiting); got.Reason != "Kept" {
		t.Errorf("an earlier generation gave %+v", got)
	}
	if got := statusapply.Wait(cp, &current, waiting); got.Reason != "WaitingForX" {
		t.Errorf("the current generation gave %+v", got)
	}
	if got := statusapply.Wait(cp, nil, waiting); got.Reason != "WaitingForX" {
		t.Errorf("no condition gave %+v", got)
	}
}

func TestNothingChangedReadsAndWritesNothing(t *testing.T) {
	held := condition(metav1.ConditionTrue, "as it is", earlier)
	cp := &lodiav1alpha1.ControlPlane{Status: lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{held}}}
	if err := statusapply.Apply(t.Context(), nil, nil, manager, cp, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{held}}, false); err != nil {
		t.Fatal(err)
	}
}

func TestACacheHoldingAPredecessorWritesNothingToItsSuccessor(t *testing.T) {
	ns := namespace(t)
	predecessor := controlPlane(t, ns)
	cached := predecessor.DeepCopy()
	if err := k8s.Delete(t.Context(), predecessor); err != nil {
		t.Fatal(err)
	}
	successor := controlPlane(t, ns)
	if successor.UID == cached.UID {
		t.Fatal("the successor has the predecessor's UID")
	}
	err := apply(t.Context(), cached, lodiav1alpha1.ControlPlaneStatus{
		Conditions:         []metav1.Condition{condition(metav1.ConditionTrue, "the predecessor's", earlier)},
		ObservedGeneration: 1,
	}, true)
	if err == nil {
		t.Fatal("the apply from a predecessor's cache succeeded")
	}
	got := stored(t, successor)
	if len(got.Status.Conditions) != 0 || got.Status.ObservedGeneration != 0 {
		t.Errorf("the successor's status became %+v", got.Status)
	}
}

func TestADeletedObjectIsNotWritten(t *testing.T) {
	ns := namespace(t)
	cp := controlPlane(t, ns)
	cached := cp.DeepCopy()
	if err := k8s.Delete(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	err := apply(t.Context(), cached, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{condition(metav1.ConditionTrue, "m", earlier)}}, false)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("error %v, want NotFound", err)
	}
}

func TestTheTransitionComesFromTheAPIServerNotTheCache(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	cached := cp.DeepCopy()
	if err := apply(t.Context(), cp, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{condition(metav1.ConditionTrue, "before", earlier)}}, false); err != nil {
		t.Fatal(err)
	}
	cached.Status.Conditions = []metav1.Condition{condition(metav1.ConditionFalse, "the cache lags", earlier)}
	if err := statusapply.ApplyInto(t.Context(), k8s, k8s, manager, cached, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{condition(metav1.ConditionTrue, "after", later)}}, false); err != nil {
		t.Fatal(err)
	}
	got := meta.FindStatusCondition(stored(t, cp).Status.Conditions, lodiav1alpha1.ConditionReady)
	if got.Message != "after" || !got.LastTransitionTime.Equal(&earlier) {
		t.Errorf("%+v, want the message after and lastTransitionTime %s", got, earlier)
	}
	held := meta.FindStatusCondition(cached.Status.Conditions, lodiav1alpha1.ConditionReady)
	if held.Message != "after" || !held.LastTransitionTime.Equal(&earlier) {
		t.Errorf("the object is left with %+v, not what was written", held)
	}
}

func TestApplyLeavesTheCallersConditionsAlone(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	if err := apply(t.Context(), cp, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{condition(metav1.ConditionTrue, "server", earlier)}}, false); err != nil {
		t.Fatal(err)
	}
	if len(cp.Status.Conditions) != 0 {
		t.Errorf("the first apply left %+v", cp.Status.Conditions)
	}
	cached := condition(metav1.ConditionFalse, "cached", earlier)
	cp.Status.Conditions = []metav1.Condition{cached}
	if err := apply(t.Context(), cp, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{condition(metav1.ConditionTrue, "new", later)}}, false); err != nil {
		t.Fatal(err)
	}
	if got := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady); got.Message != "cached" {
		t.Errorf("Ready became %+v, want the caller's", got)
	}
	if got := meta.FindStatusCondition(stored(t, cp).Status.Conditions, lodiav1alpha1.ConditionReady); got.Message != "new" {
		t.Errorf("the server holds %+v", got)
	}
}

func TestAWriteTheServerAlreadyHoldsIsNotRepeated(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	cached := cp.DeepCopy()
	held := condition(metav1.ConditionTrue, "held", earlier)
	if err := apply(t.Context(), cp, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{held}}, false); err != nil {
		t.Fatal(err)
	}
	before := stored(t, cp).ResourceVersion
	if err := statusapply.Apply(t.Context(), noWrite{t}, k8s, manager, cached, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{held}}, false); err != nil {
		t.Fatal(err)
	}
	if after := stored(t, cp).ResourceVersion; after != before {
		t.Errorf("resourceVersion moved from %s to %s", before, after)
	}
}

func TestAnotherFieldThatChangedIsWrittenWithTheConditions(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	held := condition(metav1.ConditionTrue, "held", earlier)
	if err := apply(t.Context(), cp, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{held}}, false); err != nil {
		t.Fatal(err)
	}
	cp.Status.Conditions = []metav1.Condition{held}
	if err := apply(t.Context(), cp, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{held}, ObservedGeneration: 1}, true); err != nil {
		t.Fatal(err)
	}
	got := stored(t, cp)
	if got.Status.ObservedGeneration != 1 {
		t.Errorf("observedGeneration %d, want 1", got.Status.ObservedGeneration)
	}
	if c := meta.FindStatusCondition(got.Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Message != "held" {
		t.Errorf("Ready is %+v", c)
	}
}

func TestAFieldThisWriterLeavesOutIsAnotherWritersToKeep(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	if err := statusapply.Apply(t.Context(), k8s, k8s, "lodia/other", cp, lodiav1alpha1.ControlPlaneStatus{
		Conditions: []metav1.Condition{{Type: lodiav1alpha1.ConditionProgressing, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReconciling, ObservedGeneration: 1}},
		Target:     &lodiav1alpha1.Target{Version: "v1.35.0"},
	}, true); err != nil {
		t.Fatal(err)
	}
	if err := apply(t.Context(), stored(t, cp), lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{condition(metav1.ConditionTrue, "m", earlier)}}, false); err != nil {
		t.Fatal(err)
	}
	got := stored(t, cp)
	if got.Status.Target == nil || got.Status.Target.Version != "v1.35.0" || meta.FindStatusCondition(got.Status.Conditions, lodiav1alpha1.ConditionProgressing) == nil {
		t.Errorf("another writer's fields went: %+v", got.Status)
	}
	if owners := managers(got); !owners[manager] || !owners["lodia/other"] {
		t.Errorf("managers %v, want both", owners)
	}
}

func TestAPassThatUsedItsWholeDeadlineStillWrites(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	var deadline time.Time
	writer := recordsDeadline{SubResourceWriter: k8s.Status(), at: &deadline}
	if err := statusapply.Apply(ctx, writer, k8s, manager, cp, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{condition(metav1.ConditionFalse, "why the pass ended", earlier)}}, false); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(stored(t, cp).Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Message != "why the pass ended" {
		t.Errorf("Ready is %+v", c)
	}
	if left := time.Until(deadline); left <= 0 || left > 10*time.Second {
		t.Errorf("the write had %s left, want its own 10 s", left)
	}
}

func TestALockedApplyPastItsDeadlineStillWrites(t *testing.T) {
	backup := newBackup(t, namespace(t))
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	if err := statusapply.ApplyLocked(ctx, k8s, manager, "Backup", backup, backupConditions(t, "why the pass ended")); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(storedBackup(t, backup).Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Message != "why the pass ended" {
		t.Errorf("Ready is %+v", c)
	}
}

func TestACancelledPassWritesNothingAndReturnsItsError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	desired := lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{condition(metav1.ConditionFalse, "only the cancellation", earlier)}}
	fields := backupConditions(t, "only the cancellation")
	for name, write := range map[string]func(cp *lodiav1alpha1.ControlPlane, backup *lodiav1alpha1.Backup) error{
		"Apply": func(cp *lodiav1alpha1.ControlPlane, _ *lodiav1alpha1.Backup) error {
			return apply(ctx, cp, desired, false)
		},
		"Apply of another field": func(cp *lodiav1alpha1.ControlPlane, _ *lodiav1alpha1.Backup) error {
			return apply(ctx, cp, lodiav1alpha1.ControlPlaneStatus{ObservedGeneration: 1}, true)
		},
		"Apply with no change": func(cp *lodiav1alpha1.ControlPlane, _ *lodiav1alpha1.Backup) error {
			held := cp.DeepCopy()
			held.Status.Conditions = desired.Conditions
			return apply(ctx, held, desired, false)
		},
		"ApplyInto": func(cp *lodiav1alpha1.ControlPlane, _ *lodiav1alpha1.Backup) error {
			return statusapply.ApplyInto(ctx, k8s, k8s, manager, cp, desired, false)
		},
		"ApplyLocked": func(_ *lodiav1alpha1.ControlPlane, backup *lodiav1alpha1.Backup) error {
			return statusapply.ApplyLocked(ctx, k8s, manager, "Backup", backup, fields)
		},
	} {
		t.Run(name, func(t *testing.T) {
			ns := namespace(t)
			cp, backup := controlPlane(t, ns), newBackup(t, ns)
			if err := write(cp, backup); !errors.Is(err, context.Canceled) {
				t.Errorf("the write returned %v, want the cancellation", err)
			}
			if got := stored(t, cp); got.ResourceVersion != cp.ResourceVersion || len(got.Status.Conditions) != 0 {
				t.Errorf("the ControlPlane was written: resourceVersion %s, then %s, conditions %+v", cp.ResourceVersion, got.ResourceVersion, got.Status.Conditions)
			}
			if got := storedBackup(t, backup); got.ResourceVersion != backup.ResourceVersion {
				t.Errorf("the Backup was written: resourceVersion %s, then %s", backup.ResourceVersion, got.ResourceVersion)
			}
			if len(cp.Status.Conditions) != 0 {
				t.Errorf("the caller's object was left with %+v", cp.Status.Conditions)
			}
		})
	}
}

func TestALockedApplyConflictsOnAStaleResourceVersion(t *testing.T) {
	backup := newBackup(t, namespace(t))
	if err := statusapply.ApplyLocked(t.Context(), k8s, manager, "Backup", backup, backupConditions(t, "first")); err != nil {
		t.Fatal(err)
	}
	err := statusapply.ApplyLocked(t.Context(), k8s, manager, "Backup", backup, backupConditions(t, "computed from a stale copy"))
	if !apierrors.IsConflict(err) {
		t.Fatalf("error %v, want a conflict", err)
	}
	got := storedBackup(t, backup)
	if c := meta.FindStatusCondition(got.Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Message != "first" {
		t.Errorf("Ready is %+v", c)
	}
	if err := statusapply.ApplyLocked(t.Context(), k8s, manager, "Backup", got, backupConditions(t, "from the latest copy")); err != nil {
		t.Fatal(err)
	}
}

func newBackup(t *testing.T, ns string) *lodiav1alpha1.Backup {
	t.Helper()
	backup := &lodiav1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: ns},
		Spec:       lodiav1alpha1.BackupSpec{ControlPlaneRef: lodiav1alpha1.LocalReference{Name: "dev"}},
	}
	if err := k8s.Create(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	return backup
}

func storedBackup(t *testing.T, backup *lodiav1alpha1.Backup) *lodiav1alpha1.Backup {
	t.Helper()
	got := &lodiav1alpha1.Backup{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(backup), got); err != nil {
		t.Fatal(err)
	}
	return got
}

func backupConditions(t *testing.T, message string) map[string]any {
	t.Helper()
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonReconciling, Message: message, LastTransitionTime: earlier})
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{"conditions": []any{u}}
}

type recordsDeadline struct {
	client.SubResourceWriter
	at *time.Time
}

func (w recordsDeadline) Status() client.SubResourceWriter {
	return w
}

func (w recordsDeadline) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	*w.at, _ = ctx.Deadline()
	return w.SubResourceWriter.Apply(ctx, obj, opts...)
}

type noWrite struct {
	t *testing.T
}

func (w noWrite) Status() client.SubResourceWriter {
	w.t.Error("the status was written")
	return nil
}

func managers(cp *lodiav1alpha1.ControlPlane) map[string]bool {
	found := map[string]bool{}
	for _, m := range cp.ManagedFields {
		found[m.Manager] = true
	}
	return found
}
