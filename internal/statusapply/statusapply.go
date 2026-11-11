// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package statusapply writes a controller's part of an object's status with
// server-side apply, and holds the rules of
// docs/decisions/independent-controllers.md for the conditions it carries.
// What generation a condition describes stays with its writer.
package statusapply

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
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

// Writer writes one controller's part of an object's status and records an
// event for each transition of a condition it writes, regarding the object
// without its resourceVersion, so that client-go folds a repeat into a series.
// An event's type follows api/v1alpha1's WarningReasons and the condition it
// replaces on the API server.
type Writer struct {
	Client  client.StatusClient
	Reader  client.Reader
	Manager string
	// Events records nothing when nil.
	Events events.EventRecorder
}

// Apply writes desired, every status field and condition the Writer's
// manager owns, since an apply that leaves one out removes it, and only when a
// condition differs from cp's or the caller says another field does.
//
// A change is first read through the Writer's reader, which must not be a
// cache: the transitions come from the API server's copy, and the write stops
// on another UID, so a cache still holding a deleted predecessor writes
// nothing to its successor. metadata.uid in the apply would not refuse it,
// since an update of a custom resource's status takes the stored metadata
// (k8s.io/apiextensions-apiserver@v0.37.0 pkg/registry/customresource/status_strategy.go#L81).
// In place of FirstPass's condition the apply keeps a condition of the type
// that copy holds with another reason, since a writer tells its first pass
// from a cache that can lag behind its own last write.
func (w Writer) Apply(ctx context.Context, cp *lodiav1alpha1.ControlPlane, desired lodiav1alpha1.ControlPlaneStatus, changed bool) error {
	_, err := w.write(ctx, cp, desired, changed)
	return err
}

// ApplyInto is Apply for a writer that goes on from its own conditions in the
// same pass: it leaves cp with the conditions it wrote.
func (w Writer) ApplyInto(ctx context.Context, cp *lodiav1alpha1.ControlPlane, desired lodiav1alpha1.ControlPlaneStatus, changed bool) error {
	written, err := w.write(ctx, cp, desired, changed)
	if written != nil {
		cp.Status.Conditions = written
	}
	return err
}

// Live reads cp as the API server holds it for a writer that decides again from
// that copy, under the deadline of the write it decides, so that a pass that
// has used up its time can still say why.
func Live(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane) (*lodiav1alpha1.ControlPlane, error) {
	ctx, cancel, err := ownDeadline(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	return live.Object(ctx, reader, cp)
}

// write returns the conditions it wrote among cp's, nil when it wrote none.
func (w Writer) write(ctx context.Context, cp *lodiav1alpha1.ControlPlane, desired lodiav1alpha1.ControlPlaneStatus, changed bool) ([]metav1.Condition, error) {
	ctx, cancel, err := ownDeadline(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if !changed && !slices.ContainsFunc(desired.Conditions, func(c metav1.Condition) bool {
		return Differs(meta.FindStatusCondition(cp.Status.Conditions, c.Type), c)
	}) {
		return nil, nil
	}
	conditionTypes := make([]string, 0, len(desired.Conditions))
	for _, c := range desired.Conditions {
		conditionTypes = append(conditionTypes, c.Type)
	}
	read := cp.DeepCopy()
	if err := live.Conditions(ctx, w.Reader, read, conditionTypes...); err != nil {
		return nil, err
	}
	conditions := slices.Clone(desired.Conditions)
	for i, c := range conditions {
		first := FirstPass(cp, c.Type)
		if held := meta.FindStatusCondition(read.Status.Conditions, c.Type); held != nil && !Differs(&first, c) && held.Reason != c.Reason {
			conditions[i] = *held
		}
	}
	all, applied, conditionsChanged, err := Merge(read.Status.Conditions, conditions...)
	if err != nil {
		return nil, err
	}
	if !changed && !conditionsChanged {
		return nil, nil
	}
	fields, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&desired)
	if err != nil {
		return nil, err
	}
	fields["conditions"] = applied
	if err := w.apply(ctx, "ControlPlane", map[string]any{"name": cp.Name, "namespace": cp.Namespace}, fields); err != nil {
		return nil, err
	}
	w.record("ControlPlane", cp, read.Status.Conditions, conditions)
	return all, nil
}

// ApplyLocked writes fields and conditions under the resourceVersion obj was
// read at, for a kind with one writer, whose watch requeues every update: a
// write computed from a stale copy conflicts. The transitions count from read,
// obj's conditions at that resourceVersion.
func (w Writer) ApplyLocked(ctx context.Context, kind string, obj client.Object, read []metav1.Condition, fields map[string]any, conditions ...metav1.Condition) error {
	ctx, cancel, err := ownDeadline(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	applied := make([]any, 0, len(conditions))
	for i := range conditions {
		u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&conditions[i])
		if err != nil {
			return err
		}
		applied = append(applied, u)
	}
	status := maps.Clone(fields)
	status["conditions"] = applied
	if err := w.apply(ctx, kind, map[string]any{"name": obj.GetName(), "namespace": obj.GetNamespace(), "resourceVersion": obj.GetResourceVersion()}, status); err != nil {
		return err
	}
	w.record(kind, obj, read, conditions)
	return nil
}

// ownDeadline detaches a status write from a pass that has used its whole
// timeout, which still has to say why. A pass that was cancelled, by the
// manager's shutdown, gets its error instead: it observed nothing to report.
func ownDeadline(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if err := ctx.Err(); errors.Is(err, context.Canceled) {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
	return ctx, cancel, nil
}

func (w Writer) apply(ctx context.Context, kind string, metadata, fields map[string]any) error {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       kind,
		"metadata":   metadata,
		"status":     fields,
	}}
	return w.Client.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(w.Manager), client.ForceOwnership)
}

// record records each condition written whose status or reason differs from
// the one it replaced, but a first pass where there was none.
func (w Writer) record(kind string, obj metav1.Object, replaced []metav1.Condition, written []metav1.Condition) {
	if w.Events == nil {
		return
	}
	regarding := &corev1.ObjectReference{APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: kind, Namespace: obj.GetNamespace(), Name: obj.GetName(), UID: obj.GetUID()}
	for _, c := range written {
		previous := meta.FindStatusCondition(replaced, c.Type)
		if previous == nil && c.Reason == lodiav1alpha1.ReasonReconciling || previous != nil && previous.Status == c.Status && previous.Reason == c.Reason {
			continue
		}
		w.Events.Eventf(regarding, nil, eventType(previous, c.Reason), c.Reason, c.Type, "%s", note(c.Message))
	}
}

// eventType applies the rule of api/v1alpha1's WarningReasons to the
// condition reason replaces.
func eventType(replaced *metav1.Condition, reason string) string {
	group, warning := lodiav1alpha1.WarningReasons[reason]
	switch {
	case !warning:
		return corev1.EventTypeNormal
	case group == lodiav1alpha1.WarningFailure:
		return corev1.EventTypeWarning
	case replaced == nil:
		return corev1.EventTypeNormal
	case replaced.Status == metav1.ConditionTrue:
		return corev1.EventTypeWarning
	}
	if _, listed := lodiav1alpha1.WarningReasons[replaced.Reason]; listed || passing(replaced.Reason) {
		return corev1.EventTypeNormal
	}
	return corev1.EventTypeWarning
}

// passing says whether reason is a first pass or a wait.
func passing(reason string) bool {
	switch reason {
	case lodiav1alpha1.ReasonReconciling, lodiav1alpha1.ReasonLatestGenerationNotObserved,
		lodiav1alpha1.ReasonProvisioning, lodiav1alpha1.ReasonRestoring, lodiav1alpha1.ReasonRolloutPending, lodiav1alpha1.ReasonDeleting,
		lodiav1alpha1.ReasonAwaitingRelease, lodiav1alpha1.ReasonSourceExists:
		return true
	}
	return strings.HasPrefix(reason, "WaitingFor")
}

// noteLimit is the most bytes the API server takes in an event's note
// (https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/apis/core/validation/events.go#L33-L38).
const noteLimit = 1024

// note is message cut to noteLimit before the rune that crosses it.
func note(message string) string {
	if len(message) <= noteLimit {
		return message
	}
	cut := noteLimit
	for cut > 0 && !utf8.RuneStart(message[cut]) {
		cut--
	}
	return message[:cut]
}
