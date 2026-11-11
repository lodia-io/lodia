// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/lifecycle"
	"go.lodia.io/lodia/internal/testenv"
)

func datastoreClass(t *testing.T, c client.Client, name string) *lodiav1alpha1.DatastoreClass {
	t.Helper()
	class := &lodiav1alpha1.DatastoreClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       lodiav1alpha1.DatastoreClassSpec{Storage: lodiav1alpha1.EtcdStorage{Size: resource.MustParse("8Gi"), ReclaimPolicy: "Retain"}},
	}
	if err := c.Create(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.IgnoreNotFound(c.Delete(context.Background(), class)); err != nil {
			t.Error(err)
		}
	})
	return class
}

func editClass(t *testing.T, c client.Client, name string) int64 {
	t.Helper()
	class := &lodiav1alpha1.DatastoreClass{}
	if err := c.Get(t.Context(), client.ObjectKey{Name: name}, class); err != nil {
		t.Fatal(err)
	}
	class.Spec.Placement = &lodiav1alpha1.Placement{PriorityClassName: fmt.Sprintf("edit-%d", class.Generation)}
	if err := c.Update(t.Context(), class); err != nil {
		t.Fatal(err)
	}
	return class.Generation
}

func runsClass(class *lodiav1alpha1.DatastoreClass, generation int64) map[string]any {
	return map[string]any{"datastore": map[string]any{
		"endpoints":        []any{map[string]any{"host": "dev-etcd-client.team.svc", "port": int64(2379)}},
		"members":          map[string]any{"total": int64(1), "ready": int64(1), "updated": int64(1)},
		"operatorRevision": runningRevision,
		"class":            map[string]any{"name": class.Name, "uid": string(class.UID), "generation": generation},
	}}
}

func classAt(class *lodiav1alpha1.DatastoreClass, generation int64) lodiav1alpha1.DatastoreClassGeneration {
	return lodiav1alpha1.DatastoreClassGeneration{UID: class.UID, Generation: generation}
}

func classTarget(cp *lodiav1alpha1.ControlPlane) lodiav1alpha1.DatastoreClassGeneration {
	if cp.Status.Target == nil || cp.Status.Target.DatastoreClass == nil {
		return lodiav1alpha1.DatastoreClassGeneration{}
	}
	return *cp.Status.Target.DatastoreClass
}

func TestAClassEditReachesTheControlPlanesWhoseEtcdRunsIt(t *testing.T) {
	const edited, kept = "lifecycle-edited", "lifecycle-kept"
	classes := map[string]*lodiav1alpha1.DatastoreClass{edited: datastoreClass(t, k8s, edited), kept: datastoreClass(t, k8s, kept)}
	tenants := map[string]*lodiav1alpha1.ControlPlane{}
	for _, class := range []string{edited, kept} {
		cp := controlPlane(t, "v1.37.1")
		update(t, cp, func(cp *lodiav1alpha1.ControlPlane) {
			cp.Spec.Maintenance = &lodiav1alpha1.Maintenance{Window: &lodiav1alpha1.MaintenanceWindow{Begin: "20:00", End: "21:00"}}
		})
		for _, conditionType := range readiness {
			setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
		}
		reportAll(t, cp, "v1.37.1")
		applyStatus(t, cp, "test/datastore", runsClass(classes[class], 1))
		tenants[class] = cp
	}
	for class, cp := range tenants {
		testenv.Eventually(t, func() error {
			if got := classTarget(get(t, cp)); got != classAt(classes[class], 1) {
				return fmt.Errorf("target.datastoreClass is %+v, want generation 1 of UID %s, what the Etcd runs", got, classes[class].UID)
			}
			return expect(t, cp, "v1.37.1", lodiav1alpha1.ReasonIdle)
		})
	}

	editClass(t, k8s, edited)
	testenv.Eventually(t, func() error {
		fresh := get(t, tenants[edited])
		c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionProgressing)
		if want := "DatastoreClass " + edited + " generation 2 waits for the maintenance window 20:00-21:00 UTC"; c == nil || c.Reason != lodiav1alpha1.ReasonWaitingForWindow || !strings.Contains(c.Message, want) || classTarget(fresh) != classAt(classes[edited], 1) {
			return fmt.Errorf("Progressing is %+v with target %+v, want %s %q and generation 1", c, fresh.Status.Target, lodiav1alpha1.ReasonWaitingForWindow, want)
		}
		return nil
	})
	if fresh := get(t, tenants[kept]); classTarget(fresh) != classAt(classes[kept], 1) || fresh.Status.Target.ScheduledFor != nil {
		t.Errorf("the tenant on the class not edited has target %+v", fresh.Status.Target)
	}
}

func seedSlot(t *testing.T, cp *lodiav1alpha1.ControlPlane, target map[string]any, upgrade bool, settlingSince *time.Time) {
	t.Helper()
	fresh := getQuiet(t, cp)
	condition := func(conditionType, status, reason string) map[string]any {
		return map[string]any{"type": conditionType, "status": status, "reason": reason, "message": "seeded", "observedGeneration": fresh.Generation, "lastTransitionTime": startedAt.Format(time.RFC3339)}
	}
	maintenance := map[string]any{"slotSince": startedAt.Format(time.RFC3339)}
	if settlingSince != nil {
		maintenance["settlingSince"] = settlingSince.Format(time.RFC3339)
	}
	status := map[string]any{
		"observedGeneration": fresh.Generation,
		"target":             target,
		"conditions":         []any{condition(lodiav1alpha1.ConditionProgressing, "True", lodiav1alpha1.ReasonMaintaining), condition(lodiav1alpha1.ConditionReady, "True", lodiav1alpha1.ReasonReady)},
		"maintenance":        maintenance,
	}
	if upgrade {
		status["kubernetes"] = map[string]any{"upgrade": map[string]any{"step": string(lodiav1alpha1.UpgradeStepAPIServer), "startedAt": startedAt.Format(time.RFC3339)}}
	}
	applyQuiet(t, cp, lifecycle.FieldManager, status)
}

func classGrant(class *lodiav1alpha1.DatastoreClass, generation int64) map[string]any {
	return map[string]any{"version": "v1.37.1", "operatorRevision": runningRevision, "datastoreClass": map[string]any{"uid": string(class.UID), "generation": generation}}
}

func TestARestartedOperatorKeepsAClassEditInItsSlot(t *testing.T) {
	const class = "lifecycle-restart"
	restart := datastoreClass(t, quiet, class)
	cp := fleetTenant(t, "class-restart")
	reportsOf(t, cp, runningRevision)
	applyQuiet(t, cp, "test/class", runsClass(restart, 1))
	seedSlot(t, cp, classGrant(restart, editClass(t, quiet, class)), false, nil)

	mustPass(t, fleetReconciler(quiet, quiet, later, 1), cp)
	fresh := expectMaintenance(t, cp, lodiav1alpha1.ReasonMaintaining, runningRevision)
	if c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionProgressing); classTarget(fresh) != classAt(restart, 2) || !slotOn(fresh).Time.Equal(startedAt) || !strings.Contains(c.Message, "etcd: runs DatastoreClass "+class+" generation 1, not 2") {
		t.Errorf("after a restart: target %+v, maintenance %+v, Progressing %q; want generation 2 rolling in the slot taken at %s", fresh.Status.Target, fresh.Status.Maintenance, c.Message, startedAt)
	}

	applyQuiet(t, cp, "test/class", runsClass(restart, 2))
	if result := mustPass(t, fleetReconciler(quiet, quiet, later, 1), cp); result.RequeueAfter != settlingTime {
		t.Errorf("a class edit rolled out is looked at again after %s, want the settling time", result.RequeueAfter)
	}
	if fresh := expectMaintenance(t, cp, lodiav1alpha1.ReasonMaintaining, runningRevision); fresh.Status.Maintenance.SettlingSince == nil || !fresh.Status.Maintenance.SettlingSince.Time.Equal(later) {
		t.Errorf("maintenance is %+v, want settlingSince %s", fresh.Status.Maintenance, later)
	}
	mustPass(t, fleetReconciler(quiet, quiet, later.Add(settlingTime), 1), cp)
	if fresh := expectMaintenance(t, cp, lodiav1alpha1.ReasonIdle, runningRevision); classTarget(fresh) != classAt(restart, 2) || fresh.Status.Maintenance != nil {
		t.Errorf("after the settling time: target %+v, maintenance %+v, want generation 2 and the slot free", fresh.Status.Target, fresh.Status.Maintenance)
	}
}

func TestAClassDeletedWhileItsEditWaitsLeavesNothingPendingUntilItIsCreatedAgain(t *testing.T) {
	const class = "lifecycle-deleted"
	deleted := datastoreClass(t, quiet, class)
	cp := fleetTenant(t, "class-deleted")
	fresh := getQuiet(t, cp)
	fresh.Spec.Maintenance = &lodiav1alpha1.Maintenance{Window: &lodiav1alpha1.MaintenanceWindow{Begin: "20:00", End: "21:00"}}
	if err := quiet.Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	reportsOf(t, cp, runningRevision)
	applyQuiet(t, cp, "test/class", runsClass(deleted, 1))
	seedLifecycle(t, cp, runningRevision, lodiav1alpha1.ReasonIdle, nil, nil)
	editClass(t, quiet, class)

	r := fleetReconciler(quiet, quiet, later, 5)
	mustPass(t, r, cp)
	if fresh := expectMaintenance(t, cp, lodiav1alpha1.ReasonWaitingForWindow, runningRevision); classTarget(fresh) != classAt(deleted, 1) || fresh.Status.Target.ScheduledFor == nil {
		t.Errorf("a class edit waiting for the window left target %+v, want generation 1 and a scheduledFor", fresh.Status.Target)
	}
	if err := quiet.Delete(t.Context(), deleted); err != nil {
		t.Fatal(err)
	}
	mustPass(t, r, cp)
	if fresh := expectMaintenance(t, cp, lodiav1alpha1.ReasonIdle, runningRevision); classTarget(fresh) != classAt(deleted, 1) || fresh.Status.Target.ScheduledFor != nil {
		t.Errorf("once the class is gone the target is %+v, want generation 1 and no scheduledFor", fresh.Status.Target)
	}

	created := datastoreClass(t, quiet, class)
	mustPass(t, r, cp)
	fresh = expectMaintenance(t, cp, lodiav1alpha1.ReasonWaitingForWindow, runningRevision)
	c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionProgressing)
	if want := "DatastoreClass " + class + " generation 1 of UID " + string(created.UID) + " waits for the maintenance window"; classTarget(fresh) != classAt(deleted, 1) || !strings.Contains(c.Message, want) {
		t.Errorf("the class created again leaves target %+v and Progressing %q, want generation 1 of UID %s kept and %q", fresh.Status.Target, c.Message, deleted.UID, want)
	}
}

func TestAClassEditReachesAnEtcdThatNeverRolledOut(t *testing.T) {
	for _, row := range []struct {
		name, begin string
		token       bool
		reason      string
	}{
		{"on a token", "20:00", true, lodiav1alpha1.ReasonRollingOut},
		{"in its window", "11:00", false, lodiav1alpha1.ReasonMaintaining},
	} {
		t.Run(row.name, func(t *testing.T) {
			class := datastoreClass(t, quiet, "lifecycle-unrolled-"+row.begin[:2])
			cp := quietControlPlane(t)
			fresh := getQuiet(t, cp)
			fresh.Spec.Maintenance = &lodiav1alpha1.Maintenance{Window: &lodiav1alpha1.MaintenanceWindow{Begin: row.begin, End: row.begin[:2] + ":59"}}
			if err := quiet.Update(t.Context(), fresh); err != nil {
				t.Fatal(err)
			}
			seedQuiet(t, getQuiet(t, cp), "v1.37.1", "", all("v1.37.1"), "")
			seedLifecycle(t, cp, runningRevision, lodiav1alpha1.ReasonIdle, nil, nil)
			applyQuiet(t, cp, "test/class", map[string]any{"datastore": map[string]any{
				"endpoints": []any{map[string]any{"host": "dev-etcd-client.team.svc", "port": int64(2379)}},
				"members":   map[string]any{"total": int64(3), "ready": int64(2), "updated": int64(2)},
				"class":     map[string]any{"name": class.Name, "uid": string(class.UID), "generation": int64(1)},
			}})
			r := fleetReconciler(quiet, quiet, later, 5)
			mustPass(t, r, cp)
			if got := classTarget(getQuiet(t, cp)); got != classAt(class, 1) {
				t.Fatalf("target.datastoreClass is %+v, want generation 1 of UID %s, what the Etcd was applied with", got, class.UID)
			}

			generation := editClass(t, quiet, class.Name)
			if row.token {
				mustPass(t, r, cp)
				fresh := expectMaintenance(t, cp, lodiav1alpha1.ReasonRollingOut, runningRevision)
				if classTarget(fresh) != classAt(class, 1) || fresh.Status.Target.ScheduledFor == nil {
					t.Errorf("a class edit outside the window left target %+v, want generation 1 and a scheduledFor", fresh.Status.Target)
				}
				fresh.Annotations = map[string]string{lodiav1alpha1.MaintainRequestedAtAnnotation: "2026-11-09T11:00:00Z"}
				if err := quiet.Update(t.Context(), fresh); err != nil {
					t.Fatal(err)
				}
			}
			mustPass(t, r, cp)
			fresh = expectMaintenance(t, cp, row.reason, runningRevision)
			if c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionProgressing); classTarget(fresh) != classAt(class, generation) || !strings.Contains(c.Message, "etcd: runs DatastoreClass "+class.Name+" generation 1, not 2") {
				t.Errorf("the grant left target %+v and Progressing %q, want generation 2 rolling", fresh.Status.Target, c.Message)
			}
		})
	}
}

func TestAClassEditAndAPatchUpdateWaitForAFullFleet(t *testing.T) {
	y := fleetTenant(t, "full-y")
	seedLifecycle(t, y, runningRevision, lodiav1alpha1.ReasonMaintaining, nil, nil)

	class := datastoreClass(t, quiet, "lifecycle-full")
	x := fleetTenant(t, "full-class")
	reportsOf(t, x, runningRevision)
	applyQuiet(t, x, "test/class", runsClass(class, 1))
	seedLifecycle(t, x, runningRevision, lodiav1alpha1.ReasonIdle, nil, nil)
	mustPass(t, fleetReconciler(quiet, quiet, later, 1), x)
	editClass(t, quiet, class.Name)
	mustPass(t, fleetReconciler(quiet, quiet, later, 1), x)
	if fresh := expectMaintenance(t, x, lodiav1alpha1.ReasonFleetLimit, runningRevision); classTarget(fresh) != classAt(class, 1) {
		t.Errorf("a class edit took target %+v past the fleet limit", fresh.Status.Target)
	}

	z := fleetTenant(t, "full-patch")
	autoUpdate(t, z)
	reportsOf(t, z, runningRevision)
	seedLifecycle(t, z, runningRevision, lodiav1alpha1.ReasonIdle, nil, nil)
	mustPass(t, fleetReconciler(quiet, quiet, later, 1), z)
	if fresh := expectMaintenance(t, z, lodiav1alpha1.ReasonFleetLimit, runningRevision); fresh.Status.Target.Version != "v1.37.1" {
		t.Errorf("a patch update took target %+v past the fleet limit", fresh.Status.Target)
	}
}

func TestAClassReadThatFailsWritesNothing(t *testing.T) {
	class := datastoreClass(t, quiet, "lifecycle-unread")
	cp := fleetTenant(t, "class-unread")
	reportsOf(t, cp, runningRevision)
	applyQuiet(t, cp, "test/class", runsClass(class, 1))
	seedSlot(t, cp, classGrant(class, editClass(t, quiet, class.Name)), false, nil)
	before := getQuiet(t, cp)
	injected := errors.New("injected: the class cannot be read")
	failing := interceptor.NewClient(quiet, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*lodiav1alpha1.DatastoreClass); ok {
				return injected
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	if _, err := passOf(t.Context(), fleetReconciler(failing, quiet, later, 1), cp); !errors.Is(err, injected) {
		t.Errorf("a pass whose class read fails returned %v, want the read's error", err)
	}
	if after := getQuiet(t, cp); after.ResourceVersion != before.ResourceVersion {
		t.Errorf("a pass whose class read failed wrote target %+v and maintenance %+v", after.Status.Target, after.Status.Maintenance)
	}
}
