// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestOnlyWhetherAccessReadyIsTrueBringsAPass(t *testing.T) {
	access := func(status metav1.ConditionStatus, generation int64) *lodiav1alpha1.ControlPlane {
		return &lodiav1alpha1.ControlPlane{Status: lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{
			{Type: lodiav1alpha1.ConditionAccessReady, Status: status, ObservedGeneration: generation},
		}}}
	}
	for _, c := range []struct {
		name          string
		before, after *lodiav1alpha1.ControlPlane
		want          bool
	}{
		{"True at a newer generation", access(metav1.ConditionTrue, 1), access(metav1.ConditionTrue, 2), false},
		{"False to Unknown", access(metav1.ConditionFalse, 1), access(metav1.ConditionUnknown, 2), false},
		{"True to False", access(metav1.ConditionTrue, 1), access(metav1.ConditionFalse, 1), true},
		{"missing to True", &lodiav1alpha1.ControlPlane{}, access(metav1.ConditionTrue, 1), true},
	} {
		if got := inputsChanged().Update(event.UpdateEvent{ObjectOld: c.before, ObjectNew: c.after}); got != c.want {
			t.Errorf("%s brings a pass: %t, want %t", c.name, got, c.want)
		}
	}
}
