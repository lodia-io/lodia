// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/lifecycle"
	"go.lodia.io/lodia/internal/testenv"
)

var readiness = []string{
	lodiav1alpha1.ConditionCertificatesReady,
	lodiav1alpha1.ConditionDatastoreReady,
	lodiav1alpha1.ConditionAPIServerReady,
	lodiav1alpha1.ConditionAccessReady,
	lodiav1alpha1.ConditionAddonsApplied,
	lodiav1alpha1.ConditionReachable,
	lodiav1alpha1.ConditionControllersLeading,
}

var k8s client.WithWatch

func TestMain(m *testing.M) {
	testenv.SkipUnlessAvailable("the lifecycle controller tests")
	env := testenv.Start(testenv.Options{})
	k8s = env.Client
	mgr := env.Manager(env.AsController("lifecycle"), false)
	if err := (&lifecycle.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader()}).SetupWithManager(mgr, 1); err != nil {
		env.Fatal("controller", err)
	}
	env.StartManager(mgr)
	os.Exit(env.Finish(m.Run()))
}

func TestTheTargetFollowsTheVersionWithinAMinor(t *testing.T) {
	cp := controlPlane(t, "v1.35.0")
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.35.0", lodiav1alpha1.ReasonIdle) })

	setVersion(t, cp, "v1.35.1")
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.35.1", lodiav1alpha1.ReasonIdle) })

	setVersion(t, cp, "v1.36.0")
	testenv.Eventually(t, func() error {
		return expect(t, cp, "v1.35.1", lodiav1alpha1.ReasonUnsupported)
	})
	if c := condition(t, cp, lodiav1alpha1.ConditionProgressing); c.Message != "spec.version v1.36.0 is another minor than the target v1.35.1, which stays until minor upgrades are supported" {
		t.Errorf("Progressing says %q", c.Message)
	}

	setVersion(t, cp, "v1.38.0")
	testenv.Eventually(t, func() error {
		return expect(t, cp, "v1.35.1", lodiav1alpha1.ReasonUnsupported)
	})
	if c := condition(t, cp, lodiav1alpha1.ConditionProgressing); c.Message != "spec.version v1.38.0 is not of a minor Lodia runs: 1.35, 1.36, 1.37" {
		t.Errorf("Progressing says %q", c.Message)
	}
}

func TestAnUnsupportedMinorGetsNoTarget(t *testing.T) {
	cp := controlPlane(t, "v1.34.2")
	testenv.Eventually(t, func() error {
		return expect(t, cp, "", lodiav1alpha1.ReasonUnsupported)
	})

	setVersion(t, cp, "v1.35.0")
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.35.0", lodiav1alpha1.ReasonIdle) })
}

func TestReadyFollowsTheReadinessConditions(t *testing.T) {
	cp := controlPlane(t, "v1.37.1")
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionUnknown, lodiav1alpha1.ReasonLatestGenerationNotObserved, "CertificatesReady is not reported for generation 1", 1, 0)
	})

	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady,
			"CertificatesReady, DatastoreReady, APIServerReady, AccessReady, AddonsApplied, Reachable and ControllersLeading are True", 1, 1)
	})

	setCondition(t, cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionFalse, lodiav1alpha1.ReasonSchedulerNotLeading, "Lease kube-system/kube-scheduler has no holder")
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonSchedulerNotLeading, "ControllersLeading: Lease kube-system/kube-scheduler has no holder", 1, 1)
	})

	setCondition(t, cp, lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready")
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "APIServerReady: kube-apiserver has 0 of 1 replicas ready", 1, 1)
	})

	setCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonIssuing, "dev-ca-bundle is not written yet")
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonIssuing, "CertificatesReady: dev-ca-bundle is not written yet", 1, 1)
	})

	setVersion(t, cp, "v1.37.2")
	testenv.Eventually(t, func() error {
		if err := expectReady(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonIssuing, "CertificatesReady: dev-ca-bundle is not written yet", 1, 1); err != nil {
			return err
		}
		return expectProgressing(t, cp, lodiav1alpha1.ReasonLatestGenerationNotObserved,
			"generation 2 is not taken up yet: CertificatesReady at 1, DatastoreReady at 1, APIServerReady at 1, AccessReady at 1, AddonsApplied at 1, Reachable at 1, ControllersLeading at 1")
	})
}

func TestReadyStaysTrueWhileTheWritersTakeAChangeUp(t *testing.T) {
	const allTrue = "CertificatesReady, DatastoreReady, APIServerReady, AccessReady, AddonsApplied, Reachable and ControllersLeading are True"
	cp := controlPlane(t, "v1.37.1")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 1, 1)
	})

	setVersion(t, cp, "v1.37.2")
	testenv.Eventually(t, func() error {
		if err := expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 1, 1); err != nil {
			return err
		}
		return expectProgressing(t, cp, lodiav1alpha1.ReasonLatestGenerationNotObserved,
			"generation 2 is not taken up yet: CertificatesReady at 1, DatastoreReady at 1, APIServerReady at 1, AccessReady at 1, AddonsApplied at 1, Reachable at 1, ControllersLeading at 1")
	})

	setCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, "Fine", "")
	setCondition(t, cp, lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionTrue, "Fine", "")
	testenv.Eventually(t, func() error {
		if err := expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 1, 1); err != nil {
			return err
		}
		return expectProgressing(t, cp, lodiav1alpha1.ReasonLatestGenerationNotObserved,
			"generation 2 is not taken up yet: APIServerReady at 1, AccessReady at 1, AddonsApplied at 1, Reachable at 1, ControllersLeading at 1")
	})

	setVersion(t, cp, "v1.37.3")
	testenv.Eventually(t, func() error {
		if err := expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 1, 1); err != nil {
			return err
		}
		return expectProgressing(t, cp, lodiav1alpha1.ReasonLatestGenerationNotObserved,
			"generation 3 is not taken up yet: CertificatesReady at 2, DatastoreReady at 2, APIServerReady at 1, AccessReady at 1, AddonsApplied at 1, Reachable at 1, ControllersLeading at 1")
	})

	for _, conditionType := range readiness[:len(readiness)-1] {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		if err := expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 1, 1); err != nil {
			return err
		}
		return expectProgressing(t, cp, lodiav1alpha1.ReasonLatestGenerationNotObserved,
			"generation 3 is not taken up yet: ControllersLeading at 1")
	})

	setCondition(t, cp, lodiav1alpha1.ConditionControllersLeading, metav1.ConditionTrue, "Fine", "")
	testenv.Eventually(t, func() error {
		if err := expect(t, cp, "v1.37.3", lodiav1alpha1.ReasonIdle); err != nil {
			return err
		}
		return expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 3, 3)
	})
}

func TestAMissingInputLeavesReadyAtTheObjectsGeneration(t *testing.T) {
	cp := controlPlane(t, "v1.37.1")
	for _, conditionType := range readiness[:len(readiness)-1] {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionUnknown, lodiav1alpha1.ReasonLatestGenerationNotObserved, "ControllersLeading is not reported for generation 1", 1, 0)
	})

	setVersion(t, cp, "v1.37.2")
	testenv.Eventually(t, func() error {
		if err := expectReady(t, cp, metav1.ConditionUnknown, lodiav1alpha1.ReasonLatestGenerationNotObserved, "ControllersLeading is not reported for generation 2", 2, 0); err != nil {
			return err
		}
		return expectProgressing(t, cp, lodiav1alpha1.ReasonLatestGenerationNotObserved,
			"generation 2 is not taken up yet: CertificatesReady at 1, DatastoreReady at 1, APIServerReady at 1, AccessReady at 1, AddonsApplied at 1, Reachable at 1")
	})

	setCondition(t, cp, lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready")
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "APIServerReady: kube-apiserver has 0 of 1 replicas ready", 2, 0)
	})
}

func TestProgressingNamesTheLagAfterUnsupportedAndBeforeTheRollout(t *testing.T) {
	cp := controlPlane(t, "v1.35.0")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	applyStatus(t, cp, "test/components", map[string]any{"components": map[string]any{
		"apiServer": map[string]any{"total": int64(2), "ready": int64(2), "updated": int64(1)},
	}})
	testenv.Eventually(t, func() error {
		return expectProgressing(t, cp, lodiav1alpha1.ReasonRollingOut, "kube-apiserver: 1 of 2 replicas updated")
	})

	setVersion(t, cp, "v1.35.1")
	testenv.Eventually(t, func() error {
		return expectProgressing(t, cp, lodiav1alpha1.ReasonLatestGenerationNotObserved,
			"generation 2 is not taken up yet: CertificatesReady at 1, DatastoreReady at 1, APIServerReady at 1, AccessReady at 1, AddonsApplied at 1, Reachable at 1, ControllersLeading at 1")
	})

	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		return expectProgressing(t, cp, lodiav1alpha1.ReasonRollingOut, "kube-apiserver: 1 of 2 replicas updated")
	})

	setVersion(t, cp, "v1.36.0")
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.35.1", lodiav1alpha1.ReasonUnsupported) })

	setVersion(t, cp, "v1.38.0")
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.35.1", lodiav1alpha1.ReasonUnsupported) })
}

func TestTheTargetFollowsTheSpecWhileAnInputLags(t *testing.T) {
	const allTrue = "CertificatesReady, DatastoreReady, APIServerReady, AccessReady, AddonsApplied, Reachable and ControllersLeading are True"
	cp := controlPlane(t, "v1.37.1")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 1, 1)
	})
	setVersion(t, cp, "v1.37.2")
	for _, conditionType := range readiness {
		if conditionType != lodiav1alpha1.ConditionAccessReady {
			setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
		}
	}
	testenv.Eventually(t, func() error {
		if err := expectProgressing(t, cp, lodiav1alpha1.ReasonLatestGenerationNotObserved, "generation 2 is not taken up yet: AccessReady at 1"); err != nil {
			return err
		}
		if target := get(t, cp).Status.Target; target == nil || target.Version != "v1.37.2" {
			return fmt.Errorf("target is %+v while AccessReady lags, want v1.37.2", target)
		}
		return nil
	})
}

func TestAConditionOutsideReadyHoldsNoChange(t *testing.T) {
	const allTrue = "CertificatesReady, DatastoreReady, APIServerReady, AccessReady, AddonsApplied, Reachable and ControllersLeading are True"
	cp := controlPlane(t, "v1.37.1")
	setCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable, "")
	setVersion(t, cp, "v1.37.2")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		if err := expect(t, cp, "v1.37.2", lodiav1alpha1.ReasonIdle); err != nil {
			return err
		}
		return expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 2, 2)
	})
}

func TestAStuckInputKeepsTheChangeInProgress(t *testing.T) {
	const allTrue = "CertificatesReady, DatastoreReady, APIServerReady, AccessReady, AddonsApplied, Reachable and ControllersLeading are True"
	cp := controlPlane(t, "v1.37.1")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 1, 1)
	})

	setVersion(t, cp, "v1.37.2")
	for _, conditionType := range readiness {
		if conditionType != lodiav1alpha1.ConditionAccessReady {
			setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
		}
	}
	testenv.Eventually(t, func() error {
		if err := expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 1, 1); err != nil {
			return err
		}
		return expectProgressing(t, cp, lodiav1alpha1.ReasonLatestGenerationNotObserved,
			"generation 2 is not taken up yet: AccessReady at 1")
	})

	setCondition(t, cp, lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready")
	testenv.Eventually(t, func() error {
		if err := expectReady(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "APIServerReady: kube-apiserver has 0 of 1 replicas ready", 1, 1); err != nil {
			return err
		}
		return expectProgressing(t, cp, lodiav1alpha1.ReasonLatestGenerationNotObserved,
			"generation 2 is not taken up yet: AccessReady at 1")
	})
}

func TestProgressingSumsTheRollout(t *testing.T) {
	cp := controlPlane(t, "v1.37.1")
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.37.1", lodiav1alpha1.ReasonIdle) })

	applyStatus(t, cp, "test/components", map[string]any{"components": map[string]any{
		"apiServer":         map[string]any{"total": int64(2), "ready": int64(3), "updated": int64(1)},
		"controllerManager": map[string]any{"total": int64(2), "ready": int64(2), "updated": int64(2)},
		"scheduler":         map[string]any{"total": int64(2), "ready": int64(2)},
	}})
	testenv.Eventually(t, func() error {
		return expectProgressing(t, cp, lodiav1alpha1.ReasonRollingOut,
			"kube-apiserver: 1 of 2 replicas updated; kube-scheduler: its updated replicas are not reported yet")
	})
	applyStatus(t, cp, "test/components", map[string]any{"components": map[string]any{
		"apiServer":         map[string]any{"total": int64(2), "ready": int64(2), "updated": int64(2)},
		"controllerManager": map[string]any{"total": int64(2), "ready": int64(2), "updated": int64(2)},
		"scheduler":         map[string]any{"total": int64(2), "ready": int64(1), "updated": int64(2)},
	}})
	applyStatus(t, cp, "test/datastore", map[string]any{"datastore": map[string]any{
		"endpoints": []any{map[string]any{"host": "dev-etcd-client.team.svc", "port": int64(2379)}},
		"members":   map[string]any{"total": int64(3), "ready": int64(3), "updated": int64(2)},
	}})
	testenv.Eventually(t, func() error {
		return expectProgressing(t, cp, lodiav1alpha1.ReasonRollingOut, "etcd: 2 of 3 members updated")
	})
	applyStatus(t, cp, "test/datastore", map[string]any{"datastore": map[string]any{
		"endpoints": []any{map[string]any{"host": "dev-etcd-client.team.svc", "port": int64(2379)}},
		"members":   map[string]any{"total": int64(3), "ready": int64(2), "updated": int64(3)},
	}})
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.37.1", lodiav1alpha1.ReasonIdle) })

	applyStatus(t, cp, "test/datastore", map[string]any{"datastore": map[string]any{
		"endpoints": []any{map[string]any{"host": "dev-etcd-client.team.svc", "port": int64(2379)}},
		"members":   map[string]any{"total": int64(3), "ready": int64(3)},
	}})
	testenv.Eventually(t, func() error {
		return expectProgressing(t, cp, lodiav1alpha1.ReasonRollingOut, "etcd: its updated members are not reported yet")
	})
	setVersion(t, cp, "v1.38.0")
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.37.1", lodiav1alpha1.ReasonUnsupported) })
}

func TestAPausedControlPlaneKeepsItsTarget(t *testing.T) {
	cp := controlPlane(t, "v1.35.0")
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.35.0", lodiav1alpha1.ReasonIdle) })

	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.Paused = true
		cp.Spec.Version = "v1.35.1"
	})
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.35.0", lodiav1alpha1.ReasonPaused) })

	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = false })
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.35.1", lodiav1alpha1.ReasonIdle) })
}

func TestReadyDuringDeletion(t *testing.T) {
	cp := controlPlane(t, "v1.35.0")
	setFinalizers(t, cp, "test.lodia.io/hold")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady,
			"CertificatesReady, DatastoreReady, APIServerReady, AccessReady, AddonsApplied, Reachable and ControllersLeading are True", 1, 1)
	})
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.Placement = &lodiav1alpha1.Placement{PriorityClassName: "system-cluster-critical"}
	})
	for _, conditionType := range readiness[:len(readiness)-1] {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		c := condition(t, cp, lodiav1alpha1.ConditionProgressing)
		if want := "generation 2 is not taken up yet: ControllersLeading at 1"; c == nil || c.Reason != lodiav1alpha1.ReasonLatestGenerationNotObserved || c.Message != want {
			return fmt.Errorf("Progressing is %+v, want %s %q", c, lodiav1alpha1.ReasonLatestGenerationNotObserved, want)
		}
		return nil
	})

	if err := k8s.Delete(t.Context(), get(t, cp)); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		fresh := get(t, cp)
		c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionReady)
		if c == nil || c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonDeleting || c.ObservedGeneration != fresh.Generation {
			return fmt.Errorf("Ready is %+v, want False %s at generation %d", c, lodiav1alpha1.ReasonDeleting, fresh.Generation)
		}
		return nil
	})
	setFinalizers(t, cp)
}

func TestReadyInGatewayModeTakesRoutesReadyFirst(t *testing.T) {
	const allTrue = "RoutesReady, CertificatesReady, DatastoreReady, APIServerReady, AccessReady, AddonsApplied, Reachable and ControllersLeading are True"
	cp := controlPlaneBehind(t, "v1.37.1", gateway())
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionUnknown, lodiav1alpha1.ReasonLatestGenerationNotObserved, "RoutesReady is not reported for generation 1", 1, 0)
	})

	setCondition(t, cp, lodiav1alpha1.ConditionRoutesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted, "")
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 1, 1)
	})

	setCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for status.endpoint")
	for _, reason := range []string{
		lodiav1alpha1.ReasonGatewayUnsupported,
		lodiav1alpha1.ReasonGatewayNotFound,
		lodiav1alpha1.ReasonHostnameConflict,
		lodiav1alpha1.ReasonRouteNotAccepted,
		lodiav1alpha1.ReasonRefused,
		lodiav1alpha1.ReasonObjectsControlledElsewhere,
	} {
		setCondition(t, cp, lodiav1alpha1.ConditionRoutesReady, metav1.ConditionFalse, reason, "routes of dev: "+reason)
		testenv.Eventually(t, func() error {
			return expectReady(t, cp, metav1.ConditionFalse, reason, "RoutesReady: routes of dev: "+reason, 1, 1)
		})
	}

	setCondition(t, cp, lodiav1alpha1.ConditionRoutesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted, "")
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "CertificatesReady: waiting for status.endpoint", 1, 1)
	})
}

func TestGatewayModeCountsRoutesReadyAmongTheInputs(t *testing.T) {
	const allTrue = "RoutesReady, CertificatesReady, DatastoreReady, APIServerReady, AccessReady, AddonsApplied, Reachable and ControllersLeading are True"
	cp := controlPlaneBehind(t, "v1.37.1", gateway())
	for _, conditionType := range append([]string{lodiav1alpha1.ConditionRoutesReady}, readiness...) {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 1, 1)
	})

	setVersion(t, cp, "v1.37.2")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	testenv.Eventually(t, func() error {
		if err := expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 1, 1); err != nil {
			return err
		}
		return expectProgressing(t, cp, lodiav1alpha1.ReasonLatestGenerationNotObserved, "generation 2 is not taken up yet: RoutesReady at 1")
	})

	setCondition(t, cp, lodiav1alpha1.ConditionRoutesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted, "")
	testenv.Eventually(t, func() error {
		if err := expect(t, cp, "v1.37.2", lodiav1alpha1.ReasonIdle); err != nil {
			return err
		}
		return expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 2, 2)
	})
}

func TestRoutesReadyIsNoInputOutsideGatewayMode(t *testing.T) {
	const allTrue = "CertificatesReady, DatastoreReady, APIServerReady, AccessReady, AddonsApplied, Reachable and ControllersLeading are True"
	for _, c := range []struct {
		name     string
		endpoint lodiav1alpha1.Endpoint
	}{
		{"nodePort", lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}},
		{"loadBalancer", lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := controlPlaneBehind(t, "v1.37.1", c.endpoint)
			setCondition(t, cp, lodiav1alpha1.ConditionRoutesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonHostnameConflict, "api.dev.example.com is held")
			for _, conditionType := range readiness {
				setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
			}
			testenv.Eventually(t, func() error {
				return expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 1, 1)
			})

			setVersion(t, cp, "v1.37.2")
			for _, conditionType := range readiness {
				setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
			}
			testenv.Eventually(t, func() error {
				if err := expect(t, cp, "v1.37.2", lodiav1alpha1.ReasonIdle); err != nil {
					return err
				}
				return expectReady(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, allTrue, 2, 2)
			})
		})
	}
}

func gateway() lodiav1alpha1.Endpoint {
	return lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
		ParentRef: lodiav1alpha1.ParentReference{Name: "edge", Namespace: "gateways", SectionName: "tls"},
		Hostnames: lodiav1alpha1.GatewayHostnames{APIServer: "api.dev.example.com", Konnectivity: "konnectivity.dev.example.com", Tunnel: "tunnel.dev.example.com"},
	}}
}

func setFinalizers(t *testing.T, cp *lodiav1alpha1.ControlPlane, finalizers ...string) {
	t.Helper()
	fresh := get(t, cp)
	patch := client.MergeFrom(fresh.DeepCopy())
	fresh.Finalizers = finalizers
	if err := k8s.Patch(t.Context(), fresh, patch); err != nil {
		t.Fatal(err)
	}
}

func TestAPassThatChangesNothingWritesNothing(t *testing.T) {
	cp := controlPlane(t, "v1.36.0")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	expectNoWrite(t, cp, lodiav1alpha1.ConditionReady, lodiav1alpha1.ReasonReady)

	setVersion(t, cp, "v1.36.1")
	expectNoWrite(t, cp, lodiav1alpha1.ConditionProgressing, lodiav1alpha1.ReasonLatestGenerationNotObserved)
}

func expectNoWrite(t *testing.T, cp *lodiav1alpha1.ControlPlane, conditionType, reason string) {
	t.Helper()
	var settled string
	testenv.Eventually(t, func() error {
		fresh := get(t, cp)
		c := meta.FindStatusCondition(fresh.Status.Conditions, conditionType)
		if c == nil || c.Reason != reason || settled != fresh.ResourceVersion {
			settled = fresh.ResourceVersion
			return fmt.Errorf("resourceVersion %s, %s %v", fresh.ResourceVersion, conditionType, c)
		}
		return nil
	})
	applies := 0
	counting := interceptor.NewClient(k8s, interceptor.Funcs{
		SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
			applies++
			return c.SubResource(subResource).Apply(ctx, obj, opts...)
		},
	})
	if _, err := (&lifecycle.Reconciler{Client: counting, Reader: k8s}).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if after := get(t, cp).ResourceVersion; applies > 0 || after != settled {
		t.Errorf("a pass over an unchanged ControlPlane sent %d applies; resourceVersion %s, then %s", applies, settled, after)
	}
}

func controlPlane(t *testing.T, version string) *lodiav1alpha1.ControlPlane {
	t.Helper()
	return controlPlaneBehind(t, version, lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}})
}

func controlPlaneBehind(t *testing.T, version string, endpoint lodiav1alpha1.Endpoint) *lodiav1alpha1.ControlPlane {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "lifecycle-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns.Name},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   version,
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint:  endpoint,
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	if err := k8s.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func get(t *testing.T, cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.ControlPlane {
	t.Helper()
	fresh := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		t.Fatal(err)
	}
	return fresh
}

func update(t *testing.T, cp *lodiav1alpha1.ControlPlane, change func(*lodiav1alpha1.ControlPlane)) {
	t.Helper()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &lodiav1alpha1.ControlPlane{}
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
			return err
		}
		change(fresh)
		return k8s.Update(t.Context(), fresh)
	}); err != nil {
		t.Fatal(err)
	}
}

func setVersion(t *testing.T, cp *lodiav1alpha1.ControlPlane, version string) {
	t.Helper()
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Version = version })
}

func setCondition(t *testing.T, cp *lodiav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason, message string) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status": map[string]any{"conditions": []any{map[string]any{
			"type":               conditionType,
			"status":             string(status),
			"reason":             reason,
			"message":            message,
			"observedGeneration": get(t, cp).Generation,
			"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		}}},
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner("test/"+conditionType), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func condition(t *testing.T, cp *lodiav1alpha1.ControlPlane, conditionType string) *metav1.Condition {
	t.Helper()
	return meta.FindStatusCondition(get(t, cp).Status.Conditions, conditionType)
}

func expect(t *testing.T, cp *lodiav1alpha1.ControlPlane, version, progressing string) error {
	t.Helper()
	fresh := get(t, cp)
	target := lodiav1alpha1.Target{}
	if fresh.Status.Target != nil {
		target = *fresh.Status.Target
	}
	c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionProgressing)
	switch {
	case target.Version != version:
		return fmt.Errorf("target is %q, want %q", target.Version, version)
	case c == nil || c.Status != metav1.ConditionFalse || c.Reason != progressing || c.ObservedGeneration != fresh.Generation:
		return fmt.Errorf("Progressing is %+v, want False %s at generation %d", c, progressing, fresh.Generation)
	}
	return nil
}

func expectReady(t *testing.T, cp *lodiav1alpha1.ControlPlane, status metav1.ConditionStatus, reason, message string, generation, observedGeneration int64) error {
	t.Helper()
	fresh := get(t, cp)
	c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionReady)
	switch {
	case c == nil || c.Status != status || c.Reason != reason || c.Message != message || c.ObservedGeneration != generation:
		return fmt.Errorf("Ready is %+v, want %s %s %q at generation %d", c, status, reason, message, generation)
	case fresh.Status.ObservedGeneration != observedGeneration:
		return fmt.Errorf("status.observedGeneration is %d, want %d", fresh.Status.ObservedGeneration, observedGeneration)
	}
	return nil
}

func applyStatus(t *testing.T, cp *lodiav1alpha1.ControlPlane, manager string, status map[string]any) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     status,
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(manager), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func expectProgressing(t *testing.T, cp *lodiav1alpha1.ControlPlane, reason, message string) error {
	t.Helper()
	fresh := get(t, cp)
	c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionProgressing)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != reason || c.Message != message || c.ObservedGeneration != fresh.Generation {
		return fmt.Errorf("Progressing is %+v, want True %s %q at generation %d", c, reason, message, fresh.Generation)
	}
	return nil
}
