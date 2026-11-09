// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package statusapply writes a controller's part of an object's status with
// server-side apply, and holds the rules of
// docs/decisions/independent-controllers.md for the conditions it carries.
// What generation a condition describes stays with its writer.
package statusapply

import (
	"context"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/live"
)

const writeTimeout = 10 * time.Second

// FirstPass is what a writer reports of a condition before its first pass has
// finished.
func FirstPass(cp *lodiav1alpha1.ControlPlane, conditionType string) metav1.Condition {
	return metav1.Condition{Type: conditionType, Status: metav1.ConditionUnknown, Reason: lodiav1alpha1.ReasonReconciling, Message: "the first pass has not finished", ObservedGeneration: cp.Generation}
}

// TrueAt says whether the condition is True for cp's generation.
func TrueAt(cp *lodiav1alpha1.ControlPlane, conditionType string) bool {
	c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
	return c != nil && c.Status == metav1.ConditionTrue && c.ObservedGeneration == cp.Generation
}

// Wait is a waiting writer's condition: kept while it describes an earlier
// generation, and otherwise waiting.
func Wait(cp *lodiav1alpha1.ControlPlane, kept *metav1.Condition, waiting metav1.Condition) metav1.Condition {
	if kept != nil && kept.ObservedGeneration < cp.Generation {
		return *kept
	}
	return waiting
}

// Differs says whether c is not what previous says already.
func Differs(previous *metav1.Condition, c metav1.Condition) bool {
	return previous == nil || previous.Status != c.Status || previous.Reason != c.Reason || previous.Message != c.Message || previous.ObservedGeneration != c.ObservedGeneration
}

// Merge sets conditions in a copy of current, a condition's LastTransitionTime
// counting when its status changes and now when it names none. It returns the
// copy, the entries an apply of them carries, and whether any differs from
// current.
func Merge(current []metav1.Condition, conditions ...metav1.Condition) ([]metav1.Condition, []any, bool, error) {
	all := slices.Clone(current)
	changed := false
	applied := make([]any, 0, len(conditions))
	for _, c := range conditions {
		changed = changed || Differs(meta.FindStatusCondition(current, c.Type), c)
		meta.SetStatusCondition(&all, c)
		u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(meta.FindStatusCondition(all, c.Type))
		if err != nil {
			return nil, nil, false, err
		}
		applied = append(applied, u)
	}
	return all, applied, changed, nil
}

// Apply writes desired, every status field and condition manager owns, since
// an apply that leaves one out removes it, and only when a condition differs
// from cp's or the caller says another field does; it leaves cp with the
// conditions it wrote.
//
// A change is first read through reader, which must not be a cache: the
// transitions come from the API server's copy, and the write stops on another
// UID, so a cache still holding a deleted predecessor writes nothing to its
// successor. metadata.uid in the apply would not refuse it, since an update
// of a custom resource's status takes the stored metadata
// (k8s.io/apiextensions-apiserver@v0.37.0 pkg/registry/customresource/status_strategy.go#L81).
func Apply(ctx context.Context, writer client.StatusClient, reader client.Reader, manager string, cp *lodiav1alpha1.ControlPlane, desired lodiav1alpha1.ControlPlaneStatus, changed bool) error {
	if !changed && !slices.ContainsFunc(desired.Conditions, func(c metav1.Condition) bool {
		return Differs(meta.FindStatusCondition(cp.Status.Conditions, c.Type), c)
	}) {
		return nil
	}
	ctx, cancel := ownDeadline(ctx)
	defer cancel()
	conditionTypes := make([]string, 0, len(desired.Conditions))
	for _, c := range desired.Conditions {
		conditionTypes = append(conditionTypes, c.Type)
	}
	if err := live.Conditions(ctx, reader, cp, conditionTypes...); err != nil {
		return err
	}
	all, applied, conditionsChanged, err := Merge(cp.Status.Conditions, desired.Conditions...)
	if err != nil {
		return err
	}
	if !changed && !conditionsChanged {
		return nil
	}
	fields, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&desired)
	if err != nil {
		return err
	}
	fields["conditions"] = applied
	if err := apply(ctx, writer, manager, "ControlPlane", map[string]any{"name": cp.Name, "namespace": cp.Namespace}, fields); err != nil {
		return err
	}
	cp.Status.Conditions = all
	return nil
}

// ApplyLocked writes fields under the resourceVersion obj was read at, for a
// kind with one writer, whose watch requeues every update: a write computed
// from a stale copy conflicts.
func ApplyLocked(ctx context.Context, writer client.StatusClient, manager, kind string, obj client.Object, fields map[string]any) error {
	ctx, cancel := ownDeadline(ctx)
	defer cancel()
	return apply(ctx, writer, manager, kind, map[string]any{"name": obj.GetName(), "namespace": obj.GetNamespace(), "resourceVersion": obj.GetResourceVersion()}, fields)
}

// ownDeadline detaches a status write from its pass: one that has used its
// whole timeout still has to say why.
func ownDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
}

func apply(ctx context.Context, writer client.StatusClient, manager, kind string, metadata, fields map[string]any) error {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       kind,
		"metadata":   metadata,
		"status":     fields,
	}}
	return writer.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(manager), client.ForceOwnership)
}
