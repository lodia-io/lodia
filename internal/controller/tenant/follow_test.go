// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/pki"
	"go.lodia.io/lodia/internal/testenv"
)

type reported struct {
	version               string
	total, ready, updated int64
}

func setComponents(t *testing.T, cp *lodiav1alpha1.ControlPlane, apiServer, controllerManager, scheduler *reported) {
	t.Helper()
	components := map[string]any{}
	for name, r := range map[string]*reported{"apiServer": apiServer, "controllerManager": controllerManager, "scheduler": scheduler} {
		if r != nil {
			components[name] = map[string]any{"total": r.total, "ready": r.ready, "updated": r.updated, "version": r.version}
		}
	}
	if len(components) == 0 {
		applyStatus(t, cp, "test/components", map[string]any{})
		return
	}
	applyStatus(t, cp, "test/components", map[string]any{"components": components})
}

func running(version string) *reported {
	return &reported{version: version, total: 2, ready: 2, updated: 2}
}

func setTargetVersion(t *testing.T, cp *lodiav1alpha1.ControlPlane, to string) {
	t.Helper()
	setTarget(t, cp, to, revision)
}

func eventuallyAddonsAt(t *testing.T, cp *lodiav1alpha1.ControlPlane, admin *tenantClient, version, coreDNS, konnectivity string) {
	t.Helper()
	testenv.Eventually(t, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
			return err
		}
		return addonsAt(t, cp, admin, version, coreDNS, konnectivity)
	})
}

func addonsAt(t *testing.T, cp *lodiav1alpha1.ControlPlane, admin *tenantClient, version, coreDNS, konnectivity string) error {
	t.Helper()
	var kubeadm struct {
		KubernetesVersion string `json:"kubernetesVersion"`
	}
	if err := yaml.Unmarshal([]byte(get(t, admin, &corev1.ConfigMap{}, "kube-system", "kubeadm-config").Data["ClusterConfiguration"]), &kubeadm); err != nil {
		return err
	}
	for what, got := range map[string][2]string{
		"kube-proxy":         {get(t, admin, &appsv1.DaemonSet{}, "kube-system", "kube-proxy").Spec.Template.Spec.Containers[0].Image, "registry.k8s.io/kube-proxy:" + version},
		"CoreDNS":            {get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns").Spec.Template.Spec.Containers[0].Image, "registry.k8s.io/coredns/coredns:" + coreDNS},
		"konnectivity-agent": {get(t, admin, &appsv1.DaemonSet{}, "kube-system", "konnectivity-agent").Spec.Template.Spec.Containers[0].Image, "registry.k8s.io/kas-network-proxy/proxy-agent:" + konnectivity},
		"kubeadm-config":     {kubeadm.KubernetesVersion, version},
		"status.addons":      {storedAddons(t, cp), version},
	} {
		if got[0] != got[1] {
			return fmt.Errorf("%s is %s, want %s", what, got[0], got[1])
		}
	}
	return nil
}

func settled(t *testing.T, cp *lodiav1alpha1.ControlPlane, admin *tenantClient, replicas int32) {
	t.Helper()
	setReplicas(t, cp, replicas)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	testenv.Eventually(t, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
			return err
		}
		if got := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns").Spec.Replicas; got == nil || *got != replicas {
			return fmt.Errorf("CoreDNS has %v replicas, want %d, the pass of this generation", got, replicas)
		}
		return nil
	})
}

func TestTheAddonsFollowTheOldestComponentThroughAMinorUpgrade(t *testing.T) {
	cp, admin := servedTenant(t, "tenant-oldest")
	eventuallyAddonsAt(t, cp, admin, "v1.35.8", "v1.13.1", "v0.35.0")

	setTargetVersion(t, cp, "v1.36.2")
	eventuallyAddonsAt(t, cp, admin, "v1.36.2", "v1.14.2", "v0.36.0")

	setComponents(t, cp, running("v1.36.2"), running("v1.35.8"), running("v1.35.8"))
	eventuallyAddonsAt(t, cp, admin, "v1.35.8", "v1.13.1", "v0.35.0")

	setComponents(t, cp, running("v1.36.2"), running("v1.36.2"), running("v1.35.8"))
	settled(t, cp, admin, 3)
	if err := addonsAt(t, cp, admin, "v1.35.8", "v1.13.1", "v0.35.0"); err != nil {
		t.Errorf("with the scheduler the only one a minor behind: %v", err)
	}

	setComponents(t, cp, running("v1.36.2"), running("v1.36.2"), running("v1.36.2"))
	eventuallyAddonsAt(t, cp, admin, "v1.36.2", "v1.14.2", "v0.36.0")

	setTargetVersion(t, cp, "v1.36.3")
	setComponents(t, cp, running("v1.36.3"), running("v1.36.2"), running("v1.36.3"))
	settled(t, cp, admin, 4)
	if err := addonsAt(t, cp, admin, "v1.36.2", "v1.14.2", "v0.36.0"); err != nil {
		t.Errorf("with the controller manager a patch behind: %v", err)
	}

	setComponents(t, cp, running("v1.36.3"), running("v1.36.3"), running("v1.36.3"))
	eventuallyAddonsAt(t, cp, admin, "v1.36.3", "v1.14.2", "v0.36.0")

	setTargetVersion(t, cp, "v1.36.4")
	setComponents(t, cp, running("v1.36.3"), nil, running("v1.36.4"))
	settled(t, cp, admin, 5)
	if err := addonsAt(t, cp, admin, "v1.36.3", "v1.14.2", "v0.36.0"); err != nil {
		t.Errorf("with the controller manager's report absent, the target v1.36.4 and the API server at v1.36.3: %v", err)
	}

	setComponents(t, cp, nil, nil, nil)
	eventuallyAddonsAt(t, cp, admin, "v1.36.4", "v1.14.2", "v0.36.0")
}

func TestAVersionOutsideTheTableWaitsOnBothConditions(t *testing.T) {
	cp, admin := servedTenant(t, "tenant-outside")
	setComponents(t, cp, running("v1.35.8"), running("v1.35.8"), running("v1.35.8"))
	eventuallyAddonsAt(t, cp, admin, "v1.35.8", "v1.13.1", "v0.35.0")

	setComponents(t, cp, running("v1.35.8"), running("v1.34.2"), running("v1.35.8"))
	testenv.Eventually(t, func() error {
		for _, conditionType := range []string{lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ConditionAddonsAvailable} {
			if err := expectCondition(t, cp, conditionType, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget); err != nil {
				return err
			}
			if c := condition(t, cp, conditionType); !strings.Contains(c.Message, `"v1.34.2"`) {
				return fmt.Errorf("%s says %q, want it to name v1.34.2", conditionType, c.Message)
			}
		}
		return nil
	})
	if got := storedAddons(t, cp); got != "v1.35.8" {
		t.Errorf("status.addons.version is %s while the render waits, want the last render's v1.35.8", got)
	}
	if err := addonsAt(t, cp, admin, "v1.35.8", "v1.13.1", "v0.35.0"); err != nil {
		t.Errorf("while the render waits: %v", err)
	}

	setComponents(t, cp, running("v1.35.8"), running("v1.36.0"), running("v1.35.8"))
	eventuallyAddonsAt(t, cp, admin, "v1.35.8", "v1.13.1", "v0.35.0")
}

func TestARestoredControlPlaneWithItsControllerManagerAtZeroStillGetsItsAddons(t *testing.T) {
	cp := controlPlane(t, "tenant-restored-zero", func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.RestoreFrom = &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: "prod-backup"}}
	})
	publish(t, cp)
	issuer := &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour, Clock: clock.RealClock{}}
	reconcilePKI(t, issuer, cp)
	admin := startTenant(t, cp)
	setDial(t, admin.host)
	issuer.Dial = *dial.Load()
	setCondition(t, cp, lodiav1alpha1.ConditionAPIServerReady, "Ready")
	setTargetVersion(t, cp, "v1.35.9")
	setComponents(t, cp, running("v1.35.9"), &reported{version: "v1.35.8"}, running("v1.35.9"))
	reconcilePKI(t, issuer, cp)
	expectNow(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionTrue, lodiav1alpha1.ReasonBound)

	eventuallyAddonsAt(t, cp, admin, "v1.35.8", "v1.13.1", "v0.35.0")

	setComponents(t, cp, running("v1.35.9"), nil, running("v1.35.9"))
	eventuallyAddonsAt(t, cp, admin, "v1.35.9", "v1.13.1", "v0.35.0")
}
