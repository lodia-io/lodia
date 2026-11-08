// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const heldByHealth = "kube-controller-manager has 0 of 1 replicas ready while Health holds its leader Lease"

func setManagerHeld(t *testing.T, cp *lodiav1alpha1.ControlPlane, status metav1.ConditionStatus, reason string) {
	t.Helper()
	applyStatus(t, cp, "test/health-nodes", map[string]any{"conditions": []any{map[string]any{
		"type":               lodiav1alpha1.ConditionControllerManagerHeld,
		"status":             string(status),
		"reason":             reason,
		"message":            "set by the test",
		"observedGeneration": get(t, cp).Generation,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}}})
}

func stopReplicas(t *testing.T, cp *lodiav1alpha1.ControlPlane, component string) {
	t.Helper()
	d := eventuallyDeployment(t, cp, component)
	eventually(t, func() error {
		fresh := &appsv1.Deployment{}
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(d), fresh); err != nil {
			return err
		}
		fresh.Status = appsv1.DeploymentStatus{ObservedGeneration: fresh.Generation, Replicas: *fresh.Spec.Replicas, UpdatedReplicas: *fresh.Spec.Replicas}
		return k8s.Status().Update(t.Context(), fresh)
	})
}

func expectAPIServerReady(t *testing.T, cp *lodiav1alpha1.ControlPlane, generation int64, status metav1.ConditionStatus, reason, message string) {
	t.Helper()
	eventually(t, func() error { return expectAt(cp, generation, status, reason, message) })
}

func TestAHeldControllerManagerWithNoReadyReplicaLeavesAPIServerReadyThroughARollout(t *testing.T) {
	cp := running(t)
	generation := get(t, cp).Generation
	setManagerHeld(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent)
	stopReplicas(t, cp, "kube-controller-manager")
	expectAPIServerReady(t, cp, generation, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, heldByHealth)

	setManagerHeld(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld)
	expectAPIServerReady(t, cp, generation, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-controller-manager has 0 of 1 replicas ready")
}

func TestAHeldControllerManagerWithNoReadyReplicaLeavesAPIServerReadyThroughAWaitAtANewGeneration(t *testing.T) {
	cp := running(t)
	setManagerHeld(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent)
	stopReplicas(t, cp, "kube-controller-manager")
	generation := get(t, cp).Generation
	expectAPIServerReady(t, cp, generation, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, heldByHealth)

	changeSpec(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.APIServer = &lodiav1alpha1.APIServer{ExtraArgs: map[string]string{"audit-log-maxage": "7"}}
	})
	expectAPIServerReady(t, cp, generation, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, heldByHealth)

	setManagerHeld(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld)
	expectAPIServerReady(t, cp, generation, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-controller-manager has 0 of 1 replicas ready")
}

func TestABrokenAPIServerOrSchedulerIsStillUnavailableUnderAHold(t *testing.T) {
	for _, c := range []struct {
		component string
		message   string
	}{
		{"kube-apiserver", "kube-apiserver has 0 of 1 replicas ready; " + heldByHealth},
		{"kube-scheduler", heldByHealth + "; kube-scheduler has 0 of 1 replicas ready"},
	} {
		t.Run(c.component, func(t *testing.T) {
			cp := running(t)
			generation := get(t, cp).Generation
			setManagerHeld(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent)
			stopReplicas(t, cp, "kube-controller-manager")
			stopReplicas(t, cp, c.component)
			expectAPIServerReady(t, cp, generation, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, c.message)
		})
	}
}
