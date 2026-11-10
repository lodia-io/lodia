// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/lifecycle"
	"go.lodia.io/lodia/internal/testenv"
)

func fleetTenant(t *testing.T, namespace string) *lodiav1alpha1.ControlPlane {
	t.Helper()
	if err := quiet.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: namespace},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   "v1.37.1",
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint:  lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	if err := quiet.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.IgnoreNotFound(quiet.Delete(context.Background(), cp)); err != nil {
			t.Error(err)
		}
	})
	reportsOf(t, cp, otherRevision)
	seedLifecycle(t, cp, otherRevision, lodiav1alpha1.ReasonIdle, nil, nil)
	return cp
}

func reportsOf(t *testing.T, cp *lodiav1alpha1.ControlPlane, revision string) {
	t.Helper()
	components := map[string]any{}
	for _, key := range []string{"apiServer", "controllerManager", "scheduler"} {
		components[key] = map[string]any{"total": int64(1), "ready": int64(1), "updated": int64(1), "version": "v1.37.1", "operatorRevision": revision}
	}
	applyQuiet(t, cp, "test/seed", map[string]any{
		"components": components,
		"addons":     map[string]any{"version": "v1.37.1", "operatorRevision": revision},
		"datastore": map[string]any{
			"endpoints":        []any{map[string]any{"host": "dev-etcd-client.team.svc", "port": int64(2379)}},
			"members":          map[string]any{"total": int64(1), "ready": int64(1), "updated": int64(1)},
			"operatorRevision": revision,
		},
		"conditions": quietConditions(getQuiet(t, cp)),
		"nodes":      map[string]any{"total": int64(0), "ready": int64(0)},
	})
}

func seedLifecycle(t *testing.T, cp *lodiav1alpha1.ControlPlane, granted, reason string, scheduledFor, settlingSince *time.Time) {
	t.Helper()
	seedStatus(t, cp, granted, reason, scheduledFor, settlingSince, reason == lodiav1alpha1.ReasonMaintaining || reason == lodiav1alpha1.ReasonMaintenanceFailed)
}

func seedStatus(t *testing.T, cp *lodiav1alpha1.ControlPlane, granted, reason string, scheduledFor, settlingSince *time.Time, slot bool) {
	t.Helper()
	generation := getQuiet(t, cp).Generation
	status := "False"
	if reason == lodiav1alpha1.ReasonMaintaining {
		status = "True"
	}
	condition := func(conditionType, status, reason string) map[string]any {
		return map[string]any{"type": conditionType, "status": status, "reason": reason, "message": "seeded", "observedGeneration": generation, "lastTransitionTime": startedAt.Format(time.RFC3339)}
	}
	target := map[string]any{"version": "v1.37.1", "operatorRevision": granted}
	if scheduledFor != nil {
		target["scheduledFor"] = scheduledFor.Format(time.RFC3339)
	}
	fields := map[string]any{
		"observedGeneration": generation,
		"target":             target,
		"conditions":         []any{condition(lodiav1alpha1.ConditionProgressing, status, reason), condition(lodiav1alpha1.ConditionReady, "True", lodiav1alpha1.ReasonReady)},
	}
	maintenance := map[string]any{}
	if settlingSince != nil {
		maintenance["settlingSince"] = settlingSince.Format(time.RFC3339)
	}
	if slot {
		maintenance["slotGeneration"] = generation
	}
	if len(maintenance) > 0 {
		fields["maintenance"] = maintenance
	}
	applyQuiet(t, cp, lifecycle.FieldManager, fields)
}

func fleetReconciler(c client.Client, reader client.Reader, now time.Time, limit int) *lifecycle.Reconciler {
	return &lifecycle.Reconciler{Client: c, Reader: reader, Clock: clocktesting.NewFakePassiveClock(now), Revision: runningRevision, MaxConcurrentRollouts: limit, Settling: settlingTime}
}

func passOf(ctx context.Context, r *lifecycle.Reconciler, cp *lodiav1alpha1.ControlPlane) (ctrl.Result, error) {
	return r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
}

func mustPass(t *testing.T, r *lifecycle.Reconciler, cp *lodiav1alpha1.ControlPlane) ctrl.Result {
	t.Helper()
	result, err := passOf(t.Context(), r, cp)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func expectMaintenance(t *testing.T, cp *lodiav1alpha1.ControlPlane, reason, revision string) *lodiav1alpha1.ControlPlane {
	t.Helper()
	fresh := getQuiet(t, cp)
	c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionProgressing)
	if c == nil || c.Reason != reason || revisionOn(fresh) != revision {
		t.Fatalf("%s: Progressing is %+v with revision %q, want %s with %q", cp.Namespace, c, revisionOn(fresh), reason, revision)
	}
	if slot := slotOn(fresh) == fresh.Generation; slot != (reason == lodiav1alpha1.ReasonMaintaining || reason == lodiav1alpha1.ReasonMaintenanceFailed) {
		t.Fatalf("%s: %s with maintenance.slotGeneration %d at generation %d", cp.Namespace, reason, slotOn(fresh), fresh.Generation)
	}
	return fresh
}

func slotOn(cp *lodiav1alpha1.ControlPlane) int64 {
	if cp.Status.Maintenance == nil {
		return 0
	}
	return cp.Status.Maintenance.SlotGeneration
}

func cacheShowing(change func(*lodiav1alpha1.ControlPlane)) client.WithWatch {
	return interceptor.NewClient(quiet, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := c.List(ctx, list, opts...); err != nil {
				return err
			}
			if cps, ok := list.(*lodiav1alpha1.ControlPlaneList); ok {
				for i := range cps.Items {
					change(&cps.Items[i])
				}
			}
			return nil
		},
	})
}

func listCounting(lists *int) client.WithWatch {
	return interceptor.NewClient(quiet, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			*lists++
			return c.List(ctx, list, opts...)
		},
	})
}

func idle(namespace string) func(*lodiav1alpha1.ControlPlane) {
	return func(cp *lodiav1alpha1.ControlPlane) {
		if cp.Namespace == namespace {
			meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionProgressing, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonIdle})
			cp.Status.Target.ScheduledFor, cp.Status.Maintenance = nil, nil
		}
	}
}

func TestASlotIsCountedFromTheCacheThenFromTheAPIServer(t *testing.T) {
	x, y, z := fleetTenant(t, "slot-x"), fleetTenant(t, "slot-y"), fleetTenant(t, "slot-z")
	mustPass(t, fleetReconciler(quiet, quiet, later, 1), x)
	expectMaintenance(t, x, lodiav1alpha1.ReasonMaintaining, runningRevision)

	lists := 0
	if result := mustPass(t, fleetReconciler(quiet, listCounting(&lists), later, 1), y); result.RequeueAfter != 30*time.Second {
		t.Errorf("a tenant waiting for a slot is looked at again after %s, want 30s", result.RequeueAfter)
	}
	expectMaintenance(t, y, lodiav1alpha1.ReasonFleetLimit, otherRevision)
	if lists != 0 {
		t.Errorf("a pass the cache counts out of a slot listed the fleet %d times without the cache", lists)
	}

	applies := 0
	writes := interceptor.NewClient(cacheShowing(func(cp *lodiav1alpha1.ControlPlane) { idle(x.Namespace)(cp); idle(y.Namespace)(cp) }), interceptor.Funcs{
		SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
			applies++
			return c.SubResource(subResource).Apply(ctx, obj, opts...)
		},
	})
	mustPass(t, fleetReconciler(writes, listCounting(&lists), later, 1), z)
	if fresh := expectMaintenance(t, z, lodiav1alpha1.ReasonFleetLimit, otherRevision); fresh.Status.Target.ScheduledFor == nil {
		t.Error("the refused grant left no scheduledFor")
	}
	if lists != 1 || applies != 1 {
		t.Errorf("a grant from a cache that has not seen the slot taken listed the fleet without the cache %d times and wrote %d times, want once each, the refusal", lists, applies)
	}

	seedLifecycle(t, x, runningRevision, lodiav1alpha1.ReasonIdle, nil, nil)
	if result := mustPass(t, fleetReconciler(cacheShowing(func(cp *lodiav1alpha1.ControlPlane) {
		if cp.Namespace == x.Namespace {
			meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionProgressing, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonMaintaining})
			cp.Status.Maintenance = &lodiav1alpha1.MaintenanceStatus{SlotGeneration: cp.Generation}
		}
	}), quiet, later, 1), y); result.RequeueAfter != 30*time.Second {
		t.Errorf("an over-count is looked at again after %s, want 30s", result.RequeueAfter)
	}
	expectMaintenance(t, y, lodiav1alpha1.ReasonFleetLimit, otherRevision)
	mustPass(t, fleetReconciler(quiet, quiet, later, 1), y)
	expectMaintenance(t, y, lodiav1alpha1.ReasonMaintaining, runningRevision)
}

func TestTwoTenantsRacingForTheLastSlotGetOne(t *testing.T) {
	a, b := fleetTenant(t, "race-a"), fleetTenant(t, "race-b")
	before := &lodiav1alpha1.ControlPlaneList{}
	if err := quiet.List(t.Context(), before); err != nil {
		t.Fatal(err)
	}
	snapshot := interceptor.NewClient(quiet, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if cps, ok := list.(*lodiav1alpha1.ControlPlaneList); ok {
				before.DeepCopyInto(cps)
				return nil
			}
			return c.List(ctx, list, opts...)
		},
	})
	var (
		mu       sync.Mutex
		counting bool
		overlaps int
		first    = true
	)
	writer := interceptor.NewClient(snapshot, interceptor.Funcs{
		SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
			err := c.SubResource(subResource).Apply(ctx, obj, opts...)
			mu.Lock()
			counting = false
			mu.Unlock()
			return err
		},
	})
	reader := interceptor.NewClient(quiet, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			mu.Lock()
			if counting {
				overlaps++
			}
			counting = true
			wait := first
			first = false
			mu.Unlock()
			err := c.List(ctx, list, opts...)
			if wait {
				time.Sleep(time.Second)
			}
			return err
		},
	})
	r := fleetReconciler(writer, reader, later, 1)
	errs := make(chan error, 2)
	for _, cp := range []*lodiav1alpha1.ControlPlane{a, b} {
		go func() {
			_, err := passOf(t.Context(), r, cp)
			errs <- err
		}()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if overlaps != 0 {
		t.Errorf("a second pass counted the fleet without the cache %d times while the first had not written its grant", overlaps)
	}
	reasons := map[string]int{}
	for _, cp := range []*lodiav1alpha1.ControlPlane{a, b} {
		reasons[meta.FindStatusCondition(getQuiet(t, cp).Status.Conditions, lodiav1alpha1.ConditionProgressing).Reason]++
	}
	if reasons[lodiav1alpha1.ReasonMaintaining] != 1 || reasons[lodiav1alpha1.ReasonFleetLimit] != 1 {
		t.Errorf("two tenants that both saw the last slot free ended %v, want one Maintaining and one FleetLimit", reasons)
	}
}

func TestARestartedOperatorCountsTheSlotsFromStatus(t *testing.T) {
	x, y := fleetTenant(t, "restart-x"), fleetTenant(t, "restart-y")
	reportsOf(t, x, runningRevision)
	settling := later.Add(-10 * time.Second)
	seedLifecycle(t, x, runningRevision, lodiav1alpha1.ReasonMaintaining, nil, &settling)

	if result := mustPass(t, fleetReconciler(quiet, quiet, later, 1), x); result.RequeueAfter != settlingTime-10*time.Second {
		t.Errorf("a restarted operator looks at a settling change again after %s, want %s", result.RequeueAfter, settlingTime-10*time.Second)
	}
	if fresh := expectMaintenance(t, x, lodiav1alpha1.ReasonMaintaining, runningRevision); fresh.Status.Maintenance == nil || !fresh.Status.Maintenance.SettlingSince.Time.Equal(settling) {
		t.Errorf("maintenance is %+v, want settlingSince kept at %s", fresh.Status.Maintenance, settling)
	}
	mustPass(t, fleetReconciler(quiet, quiet, later, 1), y)
	expectMaintenance(t, y, lodiav1alpha1.ReasonFleetLimit, otherRevision)

	end := later.Add(settlingTime)
	mustPass(t, fleetReconciler(quiet, quiet, end, 1), x)
	if fresh := expectMaintenance(t, x, lodiav1alpha1.ReasonIdle, runningRevision); fresh.Status.Maintenance != nil {
		t.Errorf("maintenance is %+v once the slot is free, want it absent", fresh.Status.Maintenance)
	}
	mustPass(t, fleetReconciler(quiet, quiet, end, 1), y)
	expectMaintenance(t, y, lodiav1alpha1.ReasonMaintaining, runningRevision)
}

func TestARestartedOperatorReadsTheSlotWhateverProgressingSays(t *testing.T) {
	x, y := fleetTenant(t, "restart-zone-x"), fleetTenant(t, "restart-zone-y")
	fresh := getQuiet(t, x)
	fresh.Spec.Maintenance = &lodiav1alpha1.Maintenance{Window: &lodiav1alpha1.MaintenanceWindow{Begin: "20:00", End: "21:00", TimeZone: "Mars/Olympus_Mons"}}
	if err := quiet.Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	reportsOf(t, x, runningRevision)
	settling := later.Add(-10 * time.Second)
	seedStatus(t, x, runningRevision, lodiav1alpha1.ReasonTimeZoneUnsupported, nil, &settling, true)

	mustPass(t, fleetReconciler(quiet, quiet, later, 1), y)
	expectMaintenance(t, y, lodiav1alpha1.ReasonFleetLimit, otherRevision)
	mustPass(t, fleetReconciler(quiet, quiet, later, 1), x)
	if fresh := getQuiet(t, x); slotOn(fresh) != fresh.Generation || fresh.Status.Maintenance.SettlingSince == nil || !fresh.Status.Maintenance.SettlingSince.Time.Equal(settling) {
		t.Errorf("a restarted operator left maintenance %+v at generation %d, want the slot and settlingSince kept", fresh.Status.Maintenance, fresh.Generation)
	}

	end := later.Add(settlingTime)
	mustPass(t, fleetReconciler(quiet, quiet, end, 1), x)
	if fresh := getQuiet(t, x); fresh.Status.Maintenance != nil {
		t.Errorf("maintenance is %+v once the change settled, want it absent", fresh.Status.Maintenance)
	}
	mustPass(t, fleetReconciler(quiet, quiet, end, 1), y)
	expectMaintenance(t, y, lodiav1alpha1.ReasonMaintaining, runningRevision)
}

func TestAnOwnersEditFreesTheSlotForTheOthers(t *testing.T) {
	x, y := fleetTenant(t, "edited-x"), fleetTenant(t, "edited-y")
	seedLifecycle(t, x, runningRevision, lodiav1alpha1.ReasonMaintaining, nil, nil)
	mustPass(t, fleetReconciler(quiet, quiet, later, 1), y)
	expectMaintenance(t, y, lodiav1alpha1.ReasonFleetLimit, otherRevision)

	fresh := getQuiet(t, x)
	fresh.Spec.Maintenance = &lodiav1alpha1.Maintenance{Window: &lodiav1alpha1.MaintenanceWindow{Begin: "20:00", End: "21:00"}}
	if err := quiet.Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	mustPass(t, fleetReconciler(quiet, quiet, later, 1), y)
	expectMaintenance(t, y, lodiav1alpha1.ReasonMaintaining, runningRevision)
}

func TestADeletedTenantFreesItsSlot(t *testing.T) {
	x, y := fleetTenant(t, "deleted-x"), fleetTenant(t, "deleted-y")
	seedLifecycle(t, x, runningRevision, lodiav1alpha1.ReasonMaintaining, nil, nil)
	mustPass(t, fleetReconciler(quiet, quiet, later, 1), y)
	expectMaintenance(t, y, lodiav1alpha1.ReasonFleetLimit, otherRevision)

	fresh := getQuiet(t, x)
	fresh.Finalizers = []string{"test.lodia.io/hold"}
	if err := quiet.Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		fresh := &lodiav1alpha1.ControlPlane{}
		if err := quiet.Get(context.Background(), client.ObjectKeyFromObject(x), fresh); err == nil {
			fresh.Finalizers = nil
			if err := quiet.Update(context.Background(), fresh); err != nil {
				t.Error(err)
			}
		}
	})
	if err := quiet.Delete(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	mustPass(t, fleetReconciler(quiet, quiet, later, 1), y)
	expectMaintenance(t, y, lodiav1alpha1.ReasonMaintaining, runningRevision)
}

func TestTheQueueGoesByScheduledForNamespaceAndName(t *testing.T) {
	x := fleetTenant(t, "queue-x")
	seedLifecycle(t, x, runningRevision, lodiav1alpha1.ReasonMaintaining, nil, nil)
	first, second := startedAt, startedAt.Add(time.Minute)
	q1, q2, q3 := fleetTenant(t, "queue-c"), fleetTenant(t, "queue-a"), fleetTenant(t, "queue-b")
	seedLifecycle(t, q1, otherRevision, lodiav1alpha1.ReasonFleetLimit, &first, nil)
	seedLifecycle(t, q2, otherRevision, lodiav1alpha1.ReasonFleetLimit, &second, nil)
	seedLifecycle(t, q3, otherRevision, lodiav1alpha1.ReasonFleetLimit, &second, nil)
	r := fleetReconciler(quiet, quiet, later, 1)

	seedLifecycle(t, x, runningRevision, lodiav1alpha1.ReasonIdle, nil, nil)
	for _, cp := range []*lodiav1alpha1.ControlPlane{q3, q2} {
		mustPass(t, r, cp)
		if fresh := expectMaintenance(t, cp, lodiav1alpha1.ReasonFleetLimit, otherRevision); !fresh.Status.Target.ScheduledFor.Time.Equal(second) {
			t.Errorf("%s waits since %s, want its scheduledFor %s kept", cp.Namespace, fresh.Status.Target.ScheduledFor, second)
		}
	}
	mustPass(t, r, q1)
	expectMaintenance(t, q1, lodiav1alpha1.ReasonMaintaining, runningRevision)

	seedLifecycle(t, q1, runningRevision, lodiav1alpha1.ReasonIdle, nil, nil)
	mustPass(t, r, q3)
	expectMaintenance(t, q3, lodiav1alpha1.ReasonFleetLimit, otherRevision)
	mustPass(t, r, q2)
	expectMaintenance(t, q2, lodiav1alpha1.ReasonMaintaining, runningRevision)
}

func TestAStaleCacheNeverRevokesAGrantOrASlot(t *testing.T) {
	settling := later.Add(-10 * time.Second)
	for i, c := range []struct {
		name   string
		seed   func(t *testing.T, cp *lodiav1alpha1.ControlPlane)
		behind func(cp *lodiav1alpha1.ControlPlane)
	}{
		{"a cache from before the grant", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			seedLifecycle(t, cp, runningRevision, lodiav1alpha1.ReasonMaintaining, nil, nil)
		}, func(cp *lodiav1alpha1.ControlPlane) {
			cp.Status.Target.OperatorRevision = otherRevision
			cp.Status.Maintenance = nil
			meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionProgressing, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonIdle, ObservedGeneration: cp.Generation})
		}},
		{"a cache that still has the tenant waiting for a slot", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			seedLifecycle(t, cp, runningRevision, lodiav1alpha1.ReasonMaintaining, nil, nil)
		}, func(cp *lodiav1alpha1.ControlPlane) {
			cp.Status.Target.OperatorRevision = otherRevision
			cp.Status.Target.ScheduledFor = &metav1.Time{Time: startedAt}
			cp.Status.Maintenance = nil
			meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionProgressing, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonFleetLimit, ObservedGeneration: cp.Generation})
		}},
		{"a cache that has not seen the change roll out", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			reportsOf(t, cp, runningRevision)
			seedLifecycle(t, cp, runningRevision, lodiav1alpha1.ReasonMaintaining, nil, &settling)
		}, func(cp *lodiav1alpha1.ControlPlane) {
			cp.Status.Maintenance.SettlingSince = nil
			cp.Status.Addons.OperatorRevision = otherRevision
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := fleetTenant(t, fmt.Sprintf("stale-%d", i))
			c.seed(t, cp)
			r := fleetReconciler(quiet, quiet, later, 1)
			mustPass(t, r, cp)
			settled := expectMaintenance(t, cp, lodiav1alpha1.ReasonMaintaining, runningRevision)
			stale := settled.DeepCopy()
			c.behind(stale)
			applies := 0
			if _, err := passOf(t.Context(), fleetReconciler(staleClient(stale, &applies), quiet, later, 1), cp); err != nil {
				t.Fatal(err)
			}
			if after := getQuiet(t, cp); applies != 0 || after.ResourceVersion != settled.ResourceVersion {
				t.Errorf("a pass on a stale cache applied %d times: resourceVersion %s, then %s", applies, settled.ResourceVersion, after.ResourceVersion)
			}
		})
	}
}

func TestASlotPassThatCannotFinishWritesNothingAndFreesTheSemaphore(t *testing.T) {
	injected := errors.New("injected: the API server did not answer")
	cp := fleetTenant(t, "edges")
	r := fleetReconciler(synced(quiet), quiet, later, 1)

	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	before := getQuiet(t, cp)
	if _, err := passOf(expired, r, cp); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a slot pass on a spent deadline returned %v, want the deadline", err)
	}
	if after := getQuiet(t, cp); after.ResourceVersion != before.ResourceVersion {
		t.Error("a slot pass on a spent deadline wrote the ControlPlane")
	}

	for _, broken := range []struct {
		name          string
		writer        client.Client
		reader        client.Reader
		applyAttempts *int
	}{
		{"a recount that fails", synced(quiet), interceptor.NewClient(quiet, interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return injected
			},
		}), nil},
		{"a write that fails", interceptor.NewClient(synced(quiet), interceptor.Funcs{
			SubResourceApply: func(context.Context, client.Client, string, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
				return injected
			},
		}), quiet, nil},
	} {
		r := fleetReconciler(broken.writer, broken.reader, later, 1)
		if _, err := passOf(t.Context(), r, cp); !errors.Is(err, injected) {
			t.Errorf("%s: the pass returned %v", broken.name, err)
		}
		expectMaintenance(t, cp, lodiav1alpha1.ReasonIdle, otherRevision)
		r.Client, r.Reader = quiet, quiet
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		if _, err := passOf(ctx, r, cp); err != nil {
			t.Errorf("%s: the next pass of the same reconciler returned %v: the semaphore was not released", broken.name, err)
		}
		cancel()
		expectMaintenance(t, cp, lodiav1alpha1.ReasonMaintaining, runningRevision)
		seedLifecycle(t, cp, otherRevision, lodiav1alpha1.ReasonIdle, nil, nil)
	}
}

func TestTheWindowsRequeueAndATimeZoneChange(t *testing.T) {
	cp := fleetTenant(t, "window")
	setWindow := func(window *lodiav1alpha1.MaintenanceWindow) {
		fresh := getQuiet(t, cp)
		fresh.Spec.Maintenance = &lodiav1alpha1.Maintenance{Window: window}
		if err := quiet.Update(t.Context(), fresh); err != nil {
			t.Fatal(err)
		}
		reportsOf(t, cp, otherRevision)
		seedLifecycle(t, cp, otherRevision, lodiav1alpha1.ReasonIdle, nil, nil)
	}
	r := fleetReconciler(quiet, quiet, later, 5)
	setWindow(&lodiav1alpha1.MaintenanceWindow{Begin: "11:20", End: "12:00"})
	for range 2 {
		if result := mustPass(t, r, cp); result.RequeueAfter != 20*time.Minute {
			t.Errorf("a window that opens in 20 minutes is looked at again after %s", result.RequeueAfter)
		}
	}
	if fresh := expectMaintenance(t, cp, lodiav1alpha1.ReasonWaitingForWindow, otherRevision); !fresh.Status.Target.ScheduledFor.Time.Equal(later.Add(20 * time.Minute)) {
		t.Errorf("scheduledFor is %s, want the window's start", fresh.Status.Target.ScheduledFor)
	}

	setWindow(&lodiav1alpha1.MaintenanceWindow{Begin: "11:20", End: "12:00", TimeZone: "Asia/Tokyo"})
	if result := mustPass(t, r, cp); result.RequeueAfter != time.Hour {
		t.Errorf("a window that opens in hours is looked at again after %s, want an hour", result.RequeueAfter)
	}
	if fresh := expectMaintenance(t, cp, lodiav1alpha1.ReasonWaitingForWindow, otherRevision); !fresh.Status.Target.ScheduledFor.Time.Equal(time.Date(2026, 11, 10, 2, 20, 0, 0, time.UTC)) {
		t.Errorf("scheduledFor is %s after the zone moved, want the next start in Tokyo", fresh.Status.Target.ScheduledFor)
	}

	fresh := getQuiet(t, cp)
	fresh.Spec.Maintenance.Window.TimeZone = "Europe/London"
	if err := quiet.Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	mustPass(t, r, cp)
	expectMaintenance(t, cp, lodiav1alpha1.ReasonLatestGenerationNotObserved, runningRevision)
}

func TestAWindowEdgeCrossedBetweenTwoPasses(t *testing.T) {
	cp := fleetTenant(t, "edge")
	fresh := getQuiet(t, cp)
	fresh.Spec.Maintenance = &lodiav1alpha1.Maintenance{Window: &lodiav1alpha1.MaintenanceWindow{Begin: "11:00", End: "12:00"}}
	if err := quiet.Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	reportsOf(t, cp, otherRevision)
	seedLifecycle(t, cp, otherRevision, lodiav1alpha1.ReasonIdle, nil, nil)
	if result := mustPass(t, fleetReconciler(quiet, quiet, later.Add(-time.Second), 5), cp); result.RequeueAfter != time.Second {
		t.Errorf("a second before the window, the pass comes again after %s", result.RequeueAfter)
	}
	expectMaintenance(t, cp, lodiav1alpha1.ReasonWaitingForWindow, otherRevision)
	mustPass(t, fleetReconciler(quiet, quiet, later, 5), cp)
	expectMaintenance(t, cp, lodiav1alpha1.ReasonMaintaining, runningRevision)

	other := fleetTenant(t, "edge-end")
	fresh = getQuiet(t, other)
	fresh.Spec.Maintenance = &lodiav1alpha1.Maintenance{Window: &lodiav1alpha1.MaintenanceWindow{Begin: "10:00", End: "11:00"}}
	if err := quiet.Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	reportsOf(t, other, otherRevision)
	seedLifecycle(t, other, otherRevision, lodiav1alpha1.ReasonIdle, nil, nil)
	mustPass(t, fleetReconciler(quiet, quiet, later, 5), other)
	if fresh := expectMaintenance(t, other, lodiav1alpha1.ReasonWaitingForWindow, otherRevision); !fresh.Status.Target.ScheduledFor.Time.Equal(startedAt.Add(24 * time.Hour)) {
		t.Errorf("at the window's end scheduledFor is %s, want the next day's start", fresh.Status.Target.ScheduledFor)
	}
}

func TestATokenIsHandledOncePerValue(t *testing.T) {
	cp := fleetTenant(t, "token")
	annotate := func(token string) {
		fresh := getQuiet(t, cp)
		fresh.Annotations = map[string]string{lodiav1alpha1.MaintainRequestedAtAnnotation: token}
		if err := quiet.Update(t.Context(), fresh); err != nil {
			t.Fatal(err)
		}
	}
	fresh := getQuiet(t, cp)
	fresh.Spec.Maintenance = &lodiav1alpha1.Maintenance{Window: &lodiav1alpha1.MaintenanceWindow{Begin: "20:00", End: "21:00"}}
	if err := quiet.Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	reportsOf(t, cp, otherRevision)
	seedLifecycle(t, cp, otherRevision, lodiav1alpha1.ReasonIdle, nil, nil)

	annotate("2026-11-09T11:00:00Z")
	mustPass(t, fleetReconciler(quiet, quiet, later, 5), cp)
	if fresh := expectMaintenance(t, cp, lodiav1alpha1.ReasonRollingOut, runningRevision); fresh.Status.Maintenance == nil || fresh.Status.Maintenance.LastHandled != "2026-11-09T11:00:00Z" {
		t.Errorf("maintenance is %+v, want the token handled", fresh.Status.Maintenance)
	}
	reportsOf(t, cp, runningRevision)

	const next = "v1.2.0+0000000000000000"
	r := &lifecycle.Reconciler{Client: quiet, Reader: quiet, Clock: clocktesting.NewFakePassiveClock(later), Revision: next, MaxConcurrentRollouts: 5, Settling: settlingTime}
	mustPass(t, r, cp)
	expectMaintenance(t, cp, lodiav1alpha1.ReasonWaitingForWindow, runningRevision)

	annotate(strings.Repeat("0", 257))
	mustPass(t, r, cp)
	if fresh := expectMaintenance(t, cp, lodiav1alpha1.ReasonWaitingForWindow, runningRevision); fresh.Status.Maintenance.LastHandled != "2026-11-09T11:00:00Z" {
		t.Errorf("a token over 256 characters was handled: %+v", fresh.Status.Maintenance)
	}

	annotate("2026-11-09T11:05:00Z")
	mustPass(t, r, cp)
	if fresh := expectMaintenance(t, cp, lodiav1alpha1.ReasonRollingOut, next); fresh.Status.Maintenance.LastHandled != "2026-11-09T11:05:00Z" {
		t.Errorf("maintenance is %+v, want the new token handled", fresh.Status.Maintenance)
	}

	reportsOf(t, cp, next)
	mustPass(t, r, cp)
	settled := expectMaintenance(t, cp, lodiav1alpha1.ReasonIdle, next)
	annotate("2026-11-09T11:10:00Z")
	mustPass(t, r, cp)
	fresh = expectMaintenance(t, cp, lodiav1alpha1.ReasonIdle, next)
	if fresh.Status.Maintenance.LastHandled != "2026-11-09T11:10:00Z" || !equality.Semantic.DeepEqual(fresh.Status.Target, settled.Status.Target) || !equality.Semantic.DeepEqual(fresh.Status.Conditions, settled.Status.Conditions) {
		t.Errorf("with nothing pending a token left maintenance %+v, target %+v and conditions %+v, want the token recorded and nothing else changed", fresh.Status.Maintenance, fresh.Status.Target, fresh.Status.Conditions)
	}
}

func TestTheManagerTakesASlotAsTheLifecycleUser(t *testing.T) {
	cp := controlPlane(t, "v1.37.1")
	testenv.Eventually(t, func() error {
		if got := revisionOn(get(t, cp)); got != runningRevision {
			return fmt.Errorf("target.operatorRevision is %q", got)
		}
		return nil
	})
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	reportAll(t, cp, "v1.37.1")
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.37.1", lodiav1alpha1.ReasonIdle) })

	generation := get(t, cp).Generation
	applyStatus(t, cp, lifecycle.FieldManager, map[string]any{
		"observedGeneration": generation,
		"target":             map[string]any{"version": "v1.37.1", "operatorRevision": otherRevision},
		"conditions": []any{
			map[string]any{"type": lodiav1alpha1.ConditionProgressing, "status": "False", "reason": lodiav1alpha1.ReasonIdle, "message": "an earlier release", "observedGeneration": generation, "lastTransitionTime": time.Now().UTC().Format(time.RFC3339)},
			map[string]any{"type": lodiav1alpha1.ConditionReady, "status": "True", "reason": lodiav1alpha1.ReasonReady, "message": "an earlier release", "observedGeneration": generation, "lastTransitionTime": time.Now().UTC().Format(time.RFC3339)},
		},
	})
	testenv.Eventually(t, func() error {
		fresh := get(t, cp)
		c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionProgressing)
		if revisionOn(fresh) != runningRevision || c == nil || c.Reason != lodiav1alpha1.ReasonMaintaining || slotOn(fresh) != fresh.Generation {
			return fmt.Errorf("revision %q, Progressing %+v, maintenance %+v, want %s in a slot", revisionOn(fresh), c, fresh.Status.Maintenance, runningRevision)
		}
		return nil
	})
}
