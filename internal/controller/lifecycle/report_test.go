// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/lifecycle"
	"go.lodia.io/lodia/internal/testenv"
)

func component(version string) map[string]any {
	c := map[string]any{"total": int64(1), "ready": int64(1), "updated": int64(1), "operatorRevision": runningRevision}
	if version != "" {
		c["version"] = version
	}
	return c
}

const missing = "-"

func report(t *testing.T, cp *lodiav1alpha1.ControlPlane, apiServer, controllerManager, scheduler, addons string) {
	t.Helper()
	components := map[string]any{}
	for key, version := range map[string]string{"apiServer": apiServer, "controllerManager": controllerManager, "scheduler": scheduler} {
		if version != missing {
			components[key] = component(version)
		}
	}
	status := map[string]any{"components": components}
	if addons != "" {
		status["addons"] = map[string]any{"version": addons, "operatorRevision": runningRevision}
	}
	applyStatus(t, cp, "test/reports", status)
}

func expectKubernetes(t *testing.T, cp *lodiav1alpha1.ControlPlane, want string) {
	t.Helper()
	testenv.Eventually(t, func() error {
		k := get(t, cp).Status.Kubernetes
		switch {
		case want == "" && k != nil:
			return fmt.Errorf("status.kubernetes is %+v, want it absent", k)
		case want != "" && (k == nil || k.Version != want):
			return fmt.Errorf("status.kubernetes is %+v, want version %s", k, want)
		}
		return nil
	})
}

func reportAll(t *testing.T, cp *lodiav1alpha1.ControlPlane, version string) {
	t.Helper()
	report(t, cp, version, version, version, version)
}

func TestKubernetesVersionIsTheOldestOfTheFourReports(t *testing.T) {
	cp := controlPlane(t, "v1.37.1")
	testenv.Eventually(t, func() error {
		if target := get(t, cp).Status.Target; target == nil || target.Version != "v1.37.1" {
			return fmt.Errorf("status.target is %+v", target)
		}
		return nil
	})
	expectKubernetes(t, cp, "")

	for _, c := range []struct {
		name string
		with [4]string
	}{
		{"the API server's version", [4]string{"", "v1.37.1", "v1.37.1", "v1.37.1"}},
		{"the controller manager's version", [4]string{"v1.37.1", "", "v1.37.1", "v1.37.1"}},
		{"the scheduler's version", [4]string{"v1.37.1", "v1.37.1", "", "v1.37.1"}},
		{"the scheduler", [4]string{"v1.37.1", "v1.37.1", missing, "v1.37.1"}},
		{"the API server", [4]string{missing, "v1.37.1", "v1.37.1", "v1.37.1"}},
		{"the addons", [4]string{"v1.37.1", "v1.37.1", "v1.37.1", ""}},
	} {
		t.Run(c.name+" unreported", func(t *testing.T) {
			report(t, cp, c.with[0], c.with[1], c.with[2], c.with[3])
			expectKubernetes(t, cp, "")
		})
	}

	report(t, cp, "v1.37.1", "v1.37.1", "v1.37.1", "v1.37.1")
	expectKubernetes(t, cp, "v1.37.1")
	owned := false
	for _, e := range get(t, cp).ManagedFields {
		if e.Subresource == "status" && e.FieldsV1 != nil && strings.Contains(e.FieldsV1.GetRawString(), `"f:kubernetes"`) {
			owned = true
			if e.Manager != lifecycle.FieldManager {
				t.Errorf("status.kubernetes is also owned by %s", e.Manager)
			}
		}
	}
	if !owned {
		t.Error("no manager owns status.kubernetes")
	}

	applyStatus(t, cp, "test/reports", map[string]any{"addons": map[string]any{"version": "v1.37.1"}})
	expectKubernetes(t, cp, "")

	report(t, cp, "v1.37.3", "v1.37.2", "v1.37.2", "v1.37.0")
	expectKubernetes(t, cp, "v1.37.0")
	report(t, cp, "v1.37.3", "v1.37.2", "v1.36.9", "v1.37.0")
	expectKubernetes(t, cp, "v1.36.9")
	report(t, cp, "v1.37.3", "v1.36.10", "v1.36.9", "v1.37.0")
	expectKubernetes(t, cp, "v1.36.9")
	report(t, cp, "v1.37.3", "v1.36.10", "v1.37.1", "v1.37.0")
	expectKubernetes(t, cp, "v1.36.10")
	report(t, cp, "v1.37.3", "v1.9.0", "v1.37.1", "v1.37.0")
	expectKubernetes(t, cp, "v1.9.0")

	report(t, cp, "v1.37.3", "latest", "v1.37.1", "v1.37.0")
	expectKubernetes(t, cp, "")
}

func TestKubernetesVersionFollowsTheReportsWhilePausedAndDeleting(t *testing.T) {
	cp := controlPlane(t, "v1.37.1")
	report(t, cp, "v1.37.1", "v1.37.1", "v1.37.1", "v1.37.1")
	expectKubernetes(t, cp, "v1.37.1")

	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.37.1", lodiav1alpha1.ReasonPaused) })
	report(t, cp, "v1.37.1", "v1.37.1", "v1.37.1", "v1.37.0")
	expectKubernetes(t, cp, "v1.37.0")
	report(t, cp, "v1.37.1", "v1.37.1", "v1.37.1", "")
	expectKubernetes(t, cp, "")
	report(t, cp, "v1.37.1", "v1.37.1", "v1.37.1", "v1.37.1")
	expectKubernetes(t, cp, "v1.37.1")

	setFinalizers(t, cp, "test.lodia.io/hold")
	if err := k8s.Delete(t.Context(), get(t, cp)); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		c := condition(t, cp, lodiav1alpha1.ConditionReady)
		if c == nil || c.Reason != lodiav1alpha1.ReasonDeleting {
			return fmt.Errorf("Ready is %+v, want %s", c, lodiav1alpha1.ReasonDeleting)
		}
		return nil
	})
	report(t, cp, "v1.37.1", "v1.37.1", "v1.37.1", "v1.37.0")
	expectKubernetes(t, cp, "v1.37.0")
	setFinalizers(t, cp)
}

func TestKubernetesVersionIsWrittenWhenNothingElseChanged(t *testing.T) {
	cp := controlPlane(t, "v1.37.1")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	report(t, cp, "v1.37.1", "v1.37.1", "v1.37.1", "v1.37.1")
	expectKubernetes(t, cp, "v1.37.1")
	settled := get(t, cp)
	report(t, cp, "v1.37.1", "v1.37.1", "v1.37.1", "v1.37.0")
	expectKubernetes(t, cp, "v1.37.0")
	fresh := get(t, cp)
	before, after := meta.FindStatusCondition(settled.Status.Conditions, lodiav1alpha1.ConditionReady), meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionReady)
	if before == nil || after == nil || !equality.Semantic.DeepEqual(before, after) {
		t.Errorf("Ready moved from %+v to %+v: a report does not change it", before, after)
	}
}

func TestAPassThatChangesNothingWritesNothingWithAKubernetesVersion(t *testing.T) {
	cp := controlPlane(t, "v1.36.0")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	report(t, cp, "v1.36.0", "v1.36.0", "v1.36.0", "v1.36.0")
	expectKubernetes(t, cp, "v1.36.0")
	expectNoWrite(t, cp, lodiav1alpha1.ConditionReady, lodiav1alpha1.ReasonReady)
}

func TestAPassWritesTheKubernetesVersionThroughAFailureAndADeadlineThatIsSpent(t *testing.T) {
	cp := controlPlane(t, "v1.37.1")
	report(t, cp, "v1.37.1", "v1.37.1", "v1.37.1", "v1.37.1")
	expectKubernetes(t, cp, "v1.37.1")
	behind := get(t, cp)
	behind.Status.Kubernetes = nil
	stale := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			behind.DeepCopyInto(obj.(*lodiav1alpha1.ControlPlane))
			return nil
		},
	})

	var applied []*lodiav1alpha1.KubernetesStatus
	pass := func(ctx context.Context, reader client.Reader, apply error) error {
		applied = nil
		c := interceptor.NewClient(stale, interceptor.Funcs{
			SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
				raw, err := json.Marshal(obj)
				if err != nil {
					t.Fatal(err)
				}
				written := &lodiav1alpha1.ControlPlane{}
				if err := json.Unmarshal(raw, written); err != nil {
					t.Fatal(err)
				}
				applied = append(applied, written.Status.Kubernetes)
				if apply != nil {
					return apply
				}
				return c.SubResource(subResource).Apply(ctx, obj, opts...)
			},
		})
		_, err := (&lifecycle.Reconciler{Client: c, Reader: reader, Clock: clock.RealClock{}, Revision: runningRevision, MaxConcurrentRollouts: 5, Settling: settlingTime}).Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		return err
	}
	wrote := func() bool { return len(applied) == 1 && applied[0] != nil && applied[0].Version == "v1.37.1" }

	injected := errors.New("injected: the API server did not answer")
	if err := pass(t.Context(), stale, injected); !errors.Is(err, injected) || !wrote() {
		t.Errorf("a pass whose write fails returned %v after applying %+v, want the write's error", err, applied)
	}

	unavailable := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return injected
		},
	})
	if err := pass(t.Context(), unavailable, nil); !errors.Is(err, injected) || len(applied) != 0 {
		t.Errorf("a pass that cannot read the ControlPlane live returned %v after applying %+v, want the read's error and no write", err, applied)
	}

	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	if err := pass(expired, stale, nil); err != nil || !wrote() {
		t.Errorf("a pass whose deadline is spent returned %v after applying %+v, want it to write the version", err, applied)
	}
}
