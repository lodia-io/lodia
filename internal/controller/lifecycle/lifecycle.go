// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package lifecycle is the Lifecycle controller.
package lifecycle

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/versions"
)

const (
	Name         = "lifecycle"
	FieldManager = "brygga/" + Name
)

const (
	ReasonIdle                    = "Idle"
	ReasonUnsupportedVersion      = "UnsupportedVersion"
	ReasonMinorUpgradeUnsupported = "MinorUpgradeUnsupported"
	ReasonPaused                  = "Paused"
	ReasonReconciling             = "Reconciling"
	ReasonReady                   = "Ready"
)

// readiness is what Ready is made of, in the order its reason is taken from.
var readiness = []string{
	bryggav1alpha1.ConditionCertificatesReady,
	bryggav1alpha1.ConditionDatastoreReady,
	bryggav1alpha1.ConditionAPIServerReady,
	bryggav1alpha1.ConditionAccessReady,
	bryggav1alpha1.ConditionAddonsApplied,
	bryggav1alpha1.ConditionReachable,
}

type Reconciler struct {
	client.Client
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&bryggav1alpha1.ControlPlane{}).
		Complete(r)
}

type owned struct {
	progressing, ready *metav1.Condition
	target             bryggav1alpha1.Target
	observedGeneration int64
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &bryggav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cp.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	next := owned{
		ready:              meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionReady),
		observedGeneration: cp.Status.ObservedGeneration,
	}
	if cp.Status.Target != nil {
		next.target = bryggav1alpha1.Target{Version: cp.Status.Target.Version, Reason: cp.Status.Target.Reason}
	}
	if cp.Spec.Paused {
		next.progressing = newCondition(cp, bryggav1alpha1.ConditionProgressing, metav1.ConditionFalse, ReasonPaused, "spec.paused is true: Brygga reconciles nothing")
	} else {
		next.target, next.progressing = target(cp, next.target.Version)
		next.ready = ready(cp)
		if observed(cp) {
			next.observedGeneration = cp.Generation
		}
	}
	return ctrl.Result{}, r.apply(ctx, cp, next)
}

// target moves within a minor only: a minor upgrade needs the documented
// order, the API server before the controller manager and the scheduler,
// which phase 2 builds (docs/decisions/upgrades-and-maintenance-windows.md).
func target(cp *bryggav1alpha1.ControlPlane, current string) (bryggav1alpha1.Target, *metav1.Condition) {
	want := cp.Spec.Version
	if _, ok := versions.For(want); !ok {
		message := fmt.Sprintf("Kubernetes %s is not of a minor Brygga runs: %s", want, strings.Join(versions.Minors(), ", "))
		return bryggav1alpha1.Target{Version: current, Reason: ReasonUnsupportedVersion},
			newCondition(cp, bryggav1alpha1.ConditionProgressing, metav1.ConditionFalse, ReasonUnsupportedVersion, message)
	}
	wantMinor, _ := versions.Minor(want)
	if currentMinor, ok := versions.Minor(current); ok && currentMinor != wantMinor {
		message := fmt.Sprintf("an upgrade from %s to %s changes the minor, which is not supported yet; the target stays %s", current, want, current)
		return bryggav1alpha1.Target{Version: current, Reason: ReasonMinorUpgradeUnsupported},
			newCondition(cp, bryggav1alpha1.ConditionProgressing, metav1.ConditionFalse, ReasonMinorUpgradeUnsupported, message)
	}
	return bryggav1alpha1.Target{Version: want},
		newCondition(cp, bryggav1alpha1.ConditionProgressing, metav1.ConditionFalse, ReasonIdle, "no upgrade, rotation or hibernation is running")
}

func ready(cp *bryggav1alpha1.ControlPlane) *metav1.Condition {
	for _, conditionType := range readiness {
		c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
		switch {
		case c == nil || c.ObservedGeneration != cp.Generation:
			return newCondition(cp, bryggav1alpha1.ConditionReady, metav1.ConditionUnknown, ReasonReconciling,
				fmt.Sprintf("%s is not reported for generation %d", conditionType, cp.Generation))
		case c.Status != metav1.ConditionTrue:
			message := conditionType
			if c.Message != "" {
				message += ": " + c.Message
			}
			return newCondition(cp, bryggav1alpha1.ConditionReady, c.Status, c.Reason, message)
		}
	}
	return newCondition(cp, bryggav1alpha1.ConditionReady, metav1.ConditionTrue, ReasonReady,
		strings.Join(readiness[:len(readiness)-1], ", ")+" and "+readiness[len(readiness)-1]+" are True")
}

func observed(cp *bryggav1alpha1.ControlPlane) bool {
	return !slices.ContainsFunc(readiness, func(conditionType string) bool {
		c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
		return c == nil || c.ObservedGeneration != cp.Generation
	})
}

func newCondition(cp *bryggav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason, message string) *metav1.Condition {
	return &metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: message, ObservedGeneration: cp.Generation}
}

// apply writes only on a change, and always every field Lifecycle owns: an
// apply that leaves one out removes it.
func (r *Reconciler) apply(ctx context.Context, cp *bryggav1alpha1.ControlPlane, next owned) error {
	previousTarget := bryggav1alpha1.Target{}
	if cp.Status.Target != nil {
		previousTarget = bryggav1alpha1.Target{Version: cp.Status.Target.Version, Reason: cp.Status.Target.Reason}
	}
	changed := next.target != previousTarget || next.observedGeneration != cp.Status.ObservedGeneration
	conditions := slices.Clone(cp.Status.Conditions)
	var applied []any
	for _, c := range []*metav1.Condition{next.progressing, next.ready} {
		if c == nil {
			continue
		}
		if previous := meta.FindStatusCondition(cp.Status.Conditions, c.Type); previous == nil || previous.Status != c.Status ||
			previous.Reason != c.Reason || previous.Message != c.Message || previous.ObservedGeneration != c.ObservedGeneration {
			changed = true
		}
		meta.SetStatusCondition(&conditions, *c)
		u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(meta.FindStatusCondition(conditions, c.Type))
		if err != nil {
			return err
		}
		applied = append(applied, u)
	}
	if !changed {
		return nil
	}
	fields := map[string]any{"conditions": applied}
	target := map[string]any{}
	if next.target.Version != "" {
		target["version"] = next.target.Version
	}
	if next.target.Reason != "" {
		target["reason"] = next.target.Reason
	}
	if len(target) > 0 {
		fields["target"] = target
	}
	if next.observedGeneration > 0 {
		fields["observedGeneration"] = next.observedGeneration
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": bryggav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     fields,
	}}
	return r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(FieldManager), client.ForceOwnership)
}
