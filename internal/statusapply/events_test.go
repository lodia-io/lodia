// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package statusapply_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/statusapply"
)

type recorded struct {
	regarding                       runtime.Object
	eventType, reason, action, note string
}

type recorder struct {
	events []recorded
}

func (r *recorder) Eventf(regarding, _ runtime.Object, eventType, reason, action, note string, args ...any) {
	r.events = append(r.events, recorded{regarding, eventType, reason, action, fmt.Sprintf(note, args...)})
}

func recording() (statusapply.Writer, *recorder) {
	r := &recorder{}
	w := writer()
	w.Events = r
	return w, r
}

func broadcasting(t *testing.T) statusapply.Writer {
	t.Helper()
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	broadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: clientset.EventsV1()})
	if err := broadcaster.StartRecordingToSinkWithContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(broadcaster.Shutdown)
	w := writer()
	w.Events = broadcaster.NewRecorder(k8s.Scheme(), "lodia.io/test")
	return w
}

func reported(conditionType string, status metav1.ConditionStatus, reason string) metav1.Condition {
	return metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: reason + " as the writer saw it", ObservedGeneration: 1, LastTransitionTime: earlier}
}

func conditions(c ...metav1.Condition) lodiav1alpha1.ControlPlaneStatus {
	return lodiav1alpha1.ControlPlaneStatus{Conditions: c}
}

func regarding(kind string, obj metav1.Object) *corev1.ObjectReference {
	return &corev1.ObjectReference{APIVersion: "lodia.io/v1alpha1", Kind: kind, Namespace: obj.GetNamespace(), Name: obj.GetName(), UID: obj.GetUID()}
}

func TestOneTransitionIsOneEvent(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	w, r := recording()
	unavailable := reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)
	for range 2 {
		if err := w.Apply(t.Context(), stored(t, cp), conditions(unavailable), false); err != nil {
			t.Fatal(err)
		}
	}
	want := []recorded{{regarding("ControlPlane", cp), corev1.EventTypeNormal, lodiav1alpha1.ReasonUnavailable, lodiav1alpha1.ConditionReady, unavailable.Message}}
	if !reflect.DeepEqual(r.events, want) {
		t.Errorf("recorded %+v, want %+v", r.events, want)
	}
}

func TestAReasonChangeAtOneStatusIsOneEvent(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	w, r := recording()
	for _, reason := range []string{lodiav1alpha1.ReasonUnavailable, lodiav1alpha1.ReasonControllerManagerNotLeading} {
		if err := w.Apply(t.Context(), stored(t, cp), conditions(reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, reason)), false); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.events) != 2 || r.events[1].reason != lodiav1alpha1.ReasonControllerManagerNotLeading {
		t.Errorf("recorded %+v, want Unavailable, then ControllerManagerNotLeading", r.events)
	}
}

func TestAMessageChangeIsNoEvent(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	w, r := recording()
	unavailable := reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)
	if err := w.Apply(t.Context(), cp, conditions(unavailable), false); err != nil {
		t.Fatal(err)
	}
	unavailable.Message = "another replica is not ready"
	if err := w.Apply(t.Context(), stored(t, cp), conditions(unavailable), false); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(stored(t, cp).Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Message != unavailable.Message {
		t.Fatalf("Ready is %+v, want the new message written", c)
	}
	if len(r.events) != 1 {
		t.Errorf("recorded %+v, want the first transition alone", r.events)
	}
}

func TestALockedMessageChangeIsNoEvent(t *testing.T) {
	backup := newBackup(t, namespace(t))
	w, r := recording()
	snapshotting := reported(lodiav1alpha1.ConditionReady, metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotting)
	if err := w.ApplyLocked(t.Context(), "Backup", backup, backup.Status.Conditions, map[string]any{}, snapshotting); err != nil {
		t.Fatal(err)
	}
	got := storedBackup(t, backup)
	snapshotting.Message = "the seal is at revision 7 in dev's etcd"
	if err := w.ApplyLocked(t.Context(), "Backup", got, got.Status.Conditions, map[string]any{}, snapshotting); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(storedBackup(t, backup).Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Message != snapshotting.Message {
		t.Fatalf("Ready is %+v, want the new message written", c)
	}
	if len(r.events) != 1 {
		t.Errorf("recorded %+v, want the first transition alone", r.events)
	}
}

func TestAWriteTheServerRefusesIsNoEvent(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	w, r := recording()
	err := w.Apply(t.Context(), cp, conditions(reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, "Not A Reason")), false)
	if !apierrors.IsInvalid(err) {
		t.Fatalf("error %v, want the API server's refusal", err)
	}
	if len(r.events) != 0 {
		t.Errorf("recorded %+v", r.events)
	}
}

func TestAPassOnACacheHoldingThePreviousConditionIsNoEvent(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	w, r := recording()
	if err := w.Apply(t.Context(), cp, conditions(reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)), false); err != nil {
		t.Fatal(err)
	}
	cached := stored(t, cp)
	ready := reported(lodiav1alpha1.ConditionReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
	if err := w.Apply(t.Context(), cached, conditions(ready), false); err != nil {
		t.Fatal(err)
	}
	if err := w.Apply(t.Context(), cached, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{ready}, ObservedGeneration: 1}, true); err != nil {
		t.Fatal(err)
	}
	if got := stored(t, cp); got.Status.ObservedGeneration != 1 {
		t.Fatalf("observedGeneration %d, want the pass on the cache written", got.Status.ObservedGeneration)
	}
	if len(r.events) != 2 || r.events[1].reason != lodiav1alpha1.ReasonReady {
		t.Errorf("recorded %+v, want Unavailable, then Ready", r.events)
	}
}

func TestTheFirstPassIsNoEvent(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	w, r := recording()
	if err := w.Apply(t.Context(), cp, conditions(statusapply.FirstPass(cp, lodiav1alpha1.ConditionReady)), false); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(stored(t, cp).Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Reason != lodiav1alpha1.ReasonReconciling {
		t.Fatalf("Ready is %+v, want the first pass written", c)
	}
	if len(r.events) != 0 {
		t.Errorf("recorded %+v", r.events)
	}
}

func TestAKeptFirstPassIsNoEvent(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	if err := apply(t.Context(), cp, conditions(reported(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)), false); err != nil {
		t.Fatal(err)
	}
	w, r := recording()
	if err := w.Apply(t.Context(), cp, lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{statusapply.FirstPass(cp, lodiav1alpha1.ConditionAPIServerReady)}, ObservedGeneration: 1}, true); err != nil {
		t.Fatal(err)
	}
	got := stored(t, cp)
	if got.Status.ObservedGeneration != 1 {
		t.Fatalf("observedGeneration %d, want the apply made", got.Status.ObservedGeneration)
	}
	if c := meta.FindStatusCondition(got.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady); c == nil || c.Reason != lodiav1alpha1.ReasonUnavailable {
		t.Fatalf("APIServerReady is %+v, want Unavailable kept", c)
	}
	if len(r.events) != 0 {
		t.Errorf("recorded %+v for a kept first pass", r.events)
	}
}

func TestTheNoteIsTheMessageCutBeforeTheRuneAcrossItsLimit(t *testing.T) {
	ns := namespace(t)
	cp := controlPlane(t, ns)
	prefix := "100% of %s replicas: "
	message := prefix + strings.Repeat("a", 1023-len(prefix)) + "€" + strings.Repeat("b", 32768-1026)
	if len(message) != 32768 || strings.Index(message, "€") != 1023 {
		t.Fatalf("the message is %d bytes with € at %d", len(message), strings.Index(message, "€"))
	}
	unavailable := reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)
	unavailable.Message = message
	if err := broadcasting(t).Apply(t.Context(), cp, conditions(unavailable), false); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(stored(t, cp).Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Message != message {
		t.Fatal("Ready does not hold the whole message")
	}
	got := eventually(t, ns, func(list []eventsv1.Event) error {
		if len(list) != 1 {
			return fmt.Errorf("%d events", len(list))
		}
		return nil
	})
	if want := message[:1023]; got[0].Note != want {
		t.Errorf("the note is %d bytes ending %q, want %d ending %q", len(got[0].Note), got[0].Note[max(0, len(got[0].Note)-8):], len(want), want[len(want)-8:])
	}
}

func TestAnASCIINoteIsCutAtItsLimit(t *testing.T) {
	ns := namespace(t)
	cp := controlPlane(t, ns)
	message := strings.Repeat("a", 1025)
	unavailable := reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)
	unavailable.Message = message
	if err := broadcasting(t).Apply(t.Context(), cp, conditions(unavailable), false); err != nil {
		t.Fatal(err)
	}
	got := eventually(t, ns, func(list []eventsv1.Event) error {
		if len(list) != 1 {
			return fmt.Errorf("%d events", len(list))
		}
		return nil
	})
	if got[0].Note != message[:1024] {
		t.Errorf("the note is %d bytes, want 1024", len(got[0].Note))
	}
}

func TestAConditionThatFlapsTwiceIsOneEventWithASeries(t *testing.T) {
	ns := namespace(t)
	cp := controlPlane(t, ns)
	w := broadcasting(t)
	ready := reported(lodiav1alpha1.ConditionReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
	unavailable := reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)
	for _, c := range []metav1.Condition{ready, unavailable, ready, unavailable} {
		if err := w.Apply(t.Context(), stored(t, cp), conditions(c), false); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, ns, func(list []eventsv1.Event) error {
		var flaps []eventsv1.Event
		for _, e := range list {
			if e.Reason == lodiav1alpha1.ReasonUnavailable {
				flaps = append(flaps, e)
			}
		}
		if len(flaps) != 1 || flaps[0].Series == nil || flaps[0].Series.Count != 2 || flaps[0].Type != corev1.EventTypeWarning {
			return fmt.Errorf("the Unavailable events are %+v, want one Warning with a series of 2", flaps)
		}
		if ref := flaps[0].Regarding; ref.ResourceVersion != "" || ref.UID != cp.UID || ref.Kind != "ControlPlane" {
			return fmt.Errorf("the event regards %+v", ref)
		}
		return nil
	})
}

func TestALockedWriteThatConflictsIsNoEvent(t *testing.T) {
	backup := newBackup(t, namespace(t))
	w, r := recording()
	sealLost := reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonSealLost)
	if err := w.ApplyLocked(t.Context(), "Backup", backup, backup.Status.Conditions, map[string]any{}, sealLost); err != nil {
		t.Fatal(err)
	}
	err := w.ApplyLocked(t.Context(), "Backup", backup, backup.Status.Conditions, map[string]any{}, reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonControlPlaneNotFound))
	if !apierrors.IsConflict(err) {
		t.Fatalf("error %v, want a conflict", err)
	}
	want := []recorded{{regarding("Backup", backup), corev1.EventTypeWarning, lodiav1alpha1.ReasonSealLost, lodiav1alpha1.ConditionReady, sealLost.Message}}
	if !reflect.DeepEqual(r.events, want) {
		t.Errorf("recorded %+v, want %+v", r.events, want)
	}
}

func TestAKubeconfigRequestsFailureIsAWarning(t *testing.T) {
	kr := &lodiav1alpha1.KubeconfigRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: namespace(t)},
		Spec: lodiav1alpha1.KubeconfigRequestSpec{
			ControlPlaneRef: lodiav1alpha1.LocalReference{Name: "dev"},
			User:            "alice",
			Request:         []byte("-----BEGIN CERTIFICATE REQUEST-----\nA\n-----END CERTIFICATE REQUEST-----\n"),
		},
	}
	if err := k8s.Create(t.Context(), kr); err != nil {
		t.Fatal(err)
	}
	w, r := recording()
	invalid := reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonRequestInvalid)
	if err := w.ApplyLocked(t.Context(), "KubeconfigRequest", kr, kr.Status.Conditions, map[string]any{}, invalid); err != nil {
		t.Fatal(err)
	}
	want := []recorded{{regarding("KubeconfigRequest", kr), corev1.EventTypeWarning, lodiav1alpha1.ReasonRequestInvalid, lodiav1alpha1.ConditionReady, invalid.Message}}
	if !reflect.DeepEqual(r.events, want) {
		t.Errorf("recorded %+v, want %+v", r.events, want)
	}
}

func TestARetriedWriteIsOneEvent(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	r := &recorder{}
	failed := false
	w := statusapply.Writer{Client: failsOnce{SubResourceWriter: k8s.Status(), failed: &failed}, Reader: k8s, Manager: manager, Events: r}
	unavailable := conditions(reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable))
	if err := w.Apply(t.Context(), cp, unavailable, false); !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("error %v, want the transient one", err)
	}
	if len(r.events) != 0 {
		t.Fatalf("recorded %+v for a write that failed", r.events)
	}
	if err := w.Apply(t.Context(), cp, unavailable, false); err != nil {
		t.Fatal(err)
	}
	if len(r.events) != 1 {
		t.Errorf("recorded %+v, want the retried transition once", r.events)
	}
}

func TestACacheHoldingAPredecessorRecordsNothing(t *testing.T) {
	ns := namespace(t)
	predecessor := controlPlane(t, ns)
	if err := k8s.Delete(t.Context(), predecessor); err != nil {
		t.Fatal(err)
	}
	controlPlane(t, ns)
	w, r := recording()
	if err := w.Apply(t.Context(), predecessor, conditions(reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)), false); err == nil {
		t.Fatal("the apply from a predecessor's cache succeeded")
	}
	if len(r.events) != 0 {
		t.Errorf("recorded %+v", r.events)
	}
}

func TestAPassPastItsDeadlineRecordsAndACancelledOneDoesNot(t *testing.T) {
	cp := controlPlane(t, namespace(t))
	w, r := recording()
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	if err := w.Apply(expired, cp, conditions(reported(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)), false); err != nil {
		t.Fatal(err)
	}
	cancelled, stop := context.WithCancel(t.Context())
	stop()
	if err := w.Apply(cancelled, stored(t, cp), conditions(reported(lodiav1alpha1.ConditionReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)), false); !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want the cancellation", err)
	}
	if len(r.events) != 1 || r.events[0].reason != lodiav1alpha1.ReasonUnavailable {
		t.Errorf("recorded %+v, want the expired pass's Unavailable alone", r.events)
	}
}

func TestTheTypeFollowsTheReasonAndWhatItReplaces(t *testing.T) {
	c := reported
	for _, tc := range []struct {
		name     string
		replaced *metav1.Condition
		written  metav1.Condition
		want     string
	}{
		{"APIServerReady Unavailable over none", nil, c(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable), corev1.EventTypeNormal},
		{"APIServerReady Unavailable over Reconciling", new(c(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionUnknown, lodiav1alpha1.ReasonReconciling)), c(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable), corev1.EventTypeNormal},
		{"APIServerReady Unavailable over WaitingForDatastore", new(c(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore)), c(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable), corev1.EventTypeNormal},
		{"Ready Unavailable over Restoring on a restore", new(c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring)), c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable), corev1.EventTypeNormal},
		{"Ready ControllerManagerNotLeading over Unavailable", new(c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)), c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerNotLeading), corev1.EventTypeNormal},
		{"Ready Unavailable over LatestGenerationNotObserved", new(c(lodiav1alpha1.ConditionReady, metav1.ConditionUnknown, lodiav1alpha1.ReasonLatestGenerationNotObserved)), c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable), corev1.EventTypeNormal},
		{"RoutesReady HostnameConflict on a held restore's first pass", new(c(lodiav1alpha1.ConditionRoutesReady, metav1.ConditionUnknown, lodiav1alpha1.ReasonReconciling)), c(lodiav1alpha1.ConditionRoutesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonHostnameConflict), corev1.EventTypeNormal},
		{"ControllersLeading ControllerManagerNotLeading over AwaitingRelease at a release", new(c(lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease)), c(lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerNotLeading), corev1.EventTypeNormal},
		{"ControllersLeading ControllerManagerNotLeading over SourceExists at a release", new(c(lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonSourceExists)), c(lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerNotLeading), corev1.EventTypeNormal},
		{"Ready Unavailable over RolloutPending", new(c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending)), c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable), corev1.EventTypeNormal},
		{"Ready Unavailable over Deleting", new(c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonDeleting)), c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable), corev1.EventTypeNormal},
		{"Ready ExternalProbeFailed over a first apply's Applying", new(c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying)), c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed), corev1.EventTypeNormal},
		{"Ready ExternalProbeFailed over a first bind's Binding", new(c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding)), c(lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed), corev1.EventTypeNormal},
		{"DatastoreReady QuorumLost over Provisioning", new(c(lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning)), c(lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost), corev1.EventTypeNormal},
		{"Progressing Idle over RollingOut", new(c(lodiav1alpha1.ConditionProgressing, metav1.ConditionTrue, lodiav1alpha1.ReasonRollingOut)), c(lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonIdle), corev1.EventTypeNormal},
		{"BackupReady NoBackupStore over BackupSucceeded", new(c(lodiav1alpha1.ConditionBackupReady, metav1.ConditionTrue, lodiav1alpha1.ReasonBackupSucceeded)), c(lodiav1alpha1.ConditionBackupReady, metav1.ConditionFalse, lodiav1alpha1.ReasonNoBackupStore), corev1.EventTypeNormal},
		{"APIServerReady Unavailable over Ready, as at a restore's release", new(c(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)), c(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable), corev1.EventTypeWarning},
		{"APIServerReady Unavailable over True ProgressDeadlineExceeded", new(c(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionTrue, lodiav1alpha1.ReasonProgressDeadlineExceeded)), c(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable), corev1.EventTypeWarning},
		{"APIServerReady True ProgressDeadlineExceeded over Ready", new(c(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)), c(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionTrue, lodiav1alpha1.ReasonProgressDeadlineExceeded), corev1.EventTypeWarning},
		{"ControllerManagerHeld HoldBlocked over NotHeld", new(c(lodiav1alpha1.ConditionControllerManagerHeld, metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld)), c(lodiav1alpha1.ConditionControllerManagerHeld, metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked), corev1.EventTypeWarning},
		{"ControllerManagerHeld HoldBlocked over NodesSilent", new(c(lodiav1alpha1.ConditionControllerManagerHeld, metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent)), c(lodiav1alpha1.ConditionControllerManagerHeld, metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked), corev1.EventTypeWarning},
		{"ControllerManagerHeld NodesSilent over NotHeld", new(c(lodiav1alpha1.ConditionControllerManagerHeld, metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld)), c(lodiav1alpha1.ConditionControllerManagerHeld, metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent), corev1.EventTypeWarning},
		{"DatastoreReady QuorumLost over True with a refusal reason", new(c(lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionTrue, lodiav1alpha1.ReasonSecretNotFound)), c(lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost), corev1.EventTypeWarning},
		{"DatastoreReady RestoreFailed over Restoring", new(c(lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring)), c(lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoreFailed), corev1.EventTypeWarning},
		{"Progressing MaintenanceFailed over none", nil, c(lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonMaintenanceFailed), corev1.EventTypeWarning},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cp := controlPlane(t, namespace(t))
			if tc.replaced != nil {
				if err := apply(t.Context(), cp, conditions(*tc.replaced), false); err != nil {
					t.Fatal(err)
				}
			}
			w, r := recording()
			if err := w.Apply(t.Context(), stored(t, cp), conditions(tc.written), false); err != nil {
				t.Fatal(err)
			}
			if len(r.events) != 1 || r.events[0].eventType != tc.want || r.events[0].reason != tc.written.Reason || r.events[0].action != tc.written.Type {
				t.Errorf("recorded %+v, want one %s %s of %s", r.events, tc.want, tc.written.Reason, tc.written.Type)
			}
		})
	}
}

func eventually(t *testing.T, ns string, check func([]eventsv1.Event) error) []eventsv1.Event {
	t.Helper()
	var list eventsv1.EventList
	var last error
	if err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := k8s.List(ctx, &list, client.InNamespace(ns)); err != nil {
			return false, err
		}
		last = check(list.Items)
		return last == nil, nil
	}); err != nil {
		t.Fatalf("%v: %v", err, last)
	}
	return list.Items
}

type failsOnce struct {
	client.SubResourceWriter
	failed *bool
}

func (w failsOnce) Status() client.SubResourceWriter {
	return w
}

func (w failsOnce) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	if !*w.failed {
		*w.failed = true
		return apierrors.NewServiceUnavailable("the API server is busy")
	}
	return w.SubResourceWriter.Apply(ctx, obj, opts...)
}
