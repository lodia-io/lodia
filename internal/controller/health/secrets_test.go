// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/health"
)

func TestThePassReadsTheAccessSecretsThroughTheirCopiesAndSendsNothingElse(t *testing.T) {
	f := newGateway(t)
	f.publishWorking(t)
	setAccess(t, f.cp)
	writeKubeconfig(t, f.cp, tenantKubeconfig)
	copies, copied := readsOf(t)
	live, liveReads := readsOf(t)
	r := &health.Reconciler{Client: k8s, Reader: live, Secrets: copies, FailuresBeforeFalse: failuresBeforeFalse, DialContext: dialService, Period: time.Hour, Timeout: 2 * time.Second, Clock: clock.RealClock{}}
	reconcile(t, r, f.cp)
	want := []string{lodiav1alpha1.CABundleSecret.Name(f.cp.Name), lodiav1alpha1.LodiaKubeconfigSecret.Name(f.cp.Name), lodiav1alpha1.TunnelClientSecret.Name(f.cp.Name)}
	if got := slices.Sorted(slices.Values(*copied)); !slices.Equal(got, want) {
		t.Errorf("the pass read the Secrets %v through the copies, want %v", got, want)
	}
	if len(*liveReads) != 0 {
		t.Errorf("a pass that waits for none read the Secrets %v from the API server", *liveReads)
	}
}

func TestHealthNodesReadsLodiasKubeconfigThroughItsCopyAndSendsNothingElse(t *testing.T) {
	cp := tenantControlPlane(t)
	joined(t, "a", corev1.ConditionTrue, time.Now())
	copies, copied := readsOf(t)
	live, liveReads := readsOf(t)
	r := nodesReconciler(clocktesting.NewFakePassiveClock(time.Now().Truncate(time.Second)))
	r.Reader, r.Secrets = live, copies
	sample(t, r, cp)
	expectNodes(t, cp, nodesOf(1, 1, 0, "v1.35.0"))
	if got, want := *copied, []string{lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name)}; !slices.Equal(got, want) {
		t.Errorf("the pass read the Secrets %v through the copies, want %v", got, want)
	}
	if len(*liveReads) != 0 {
		t.Errorf("a pass that waits for none read the Secrets %v from the API server", *liveReads)
	}
}
