// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/pki"
	"go.lodia.io/lodia/internal/controller/tenant"
	"go.lodia.io/lodia/internal/testenv"
)

type unpaused struct {
	client.Client
}

func (c unpaused) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
		cp.Spec.Paused = false
	}
	return nil
}

func runningOperator() *tenant.Reconciler {
	return &tenant.Reconciler{Client: unpaused{k8s}, Reader: k8s, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Revision: revision, Dial: *dial.Load()}
}

func passWith(t *testing.T, r *tenant.Reconciler, cp *lodiav1alpha1.ControlPlane) error {
	t.Helper()
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	return err
}

func expectAt(t *testing.T, cp *lodiav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason string, generation int64) {
	t.Helper()
	if c := condition(t, cp, conditionType); c.Status != status || c.Reason != reason || c.ObservedGeneration != generation {
		t.Errorf("%s is %s %s at generation %d (%s), want %s %s at %d", conditionType, c.Status, c.Reason, c.ObservedGeneration, c.Message, status, reason, generation)
	}
}

func TestWithoutAGrantedRevisionTenantWaitsForTheTarget(t *testing.T) {
	cp := controlPlane(t, "tenant-no-revision")
	publish(t, cp)
	reconcilePKI(t, &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour, Clock: clock.RealClock{}}, cp)
	testenv.EventuallyWithin(t, 10*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	})
	update(t, cp, paused)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	applyStatus(t, cp, "test/target", map[string]any{"target": map[string]any{"version": version}})
	r := runningOperator()
	r.Dial = unreachable
	if err := passWith(t, r, cp); err != nil {
		t.Fatalf("the pass returned %v, which a wait does not", err)
	}
	for _, conditionType := range []string{lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ConditionAddonsAvailable} {
		expectAt(t, cp, conditionType, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, cp.Generation)
		if got := condition(t, cp, conditionType).Message; got != "status.target has no operatorRevision" {
			t.Errorf("%s says %q", conditionType, got)
		}
	}
}
