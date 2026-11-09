// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestAWriteKeepsTheStrategyAndThePreferredHolderOfTheLease(t *testing.T) {
	read := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: lodiav1alpha1.LeaderElectionNamespace, Name: lodiav1alpha1.ControllerManagerLease, ResourceVersion: "7"},
		Spec: coordinationv1.LeaseSpec{
			Strategy:        ptr.To(coordinationv1.OldestEmulationVersion),
			PreferredHolder: new("dev-kube-controller-manager-7d9_1"),
		},
	}
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	for name, write := range map[string]leaseWrite{"take": takeLease, "renewal": renewLease, "release": releaseLease} {
		t.Run(name, func(t *testing.T) {
			r := &NodesReconciler{Timeout: time.Second}
			leases := fake.NewClientset(read.DeepCopy()).CoordinationV1().Leases(read.Namespace)
			written, err := r.writeLease(t.Context(), &tenant{leases: leases}, read, write, now)
			if err != nil {
				t.Fatal(err)
			}
			if ptr.Deref(written.Spec.Strategy, "") != coordinationv1.OldestEmulationVersion || ptr.Deref(written.Spec.PreferredHolder, "") != "dev-kube-controller-manager-7d9_1" {
				t.Errorf("the %s left strategy %v and preferredHolder %v", name, written.Spec.Strategy, written.Spec.PreferredHolder)
			}
			if written.ResourceVersion != "7" {
				t.Errorf("the write used resourceVersion %q, want the one it read", written.ResourceVersion)
			}
		})
	}
}

func TestAFirstWriteCreatesTheLease(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	r := &NodesReconciler{Timeout: time.Second}
	leases := fake.NewClientset().CoordinationV1().Leases(lodiav1alpha1.LeaderElectionNamespace)
	written, err := r.writeLease(t.Context(), &tenant{leases: leases}, nil, takeLease, now)
	if err != nil {
		t.Fatal(err)
	}
	if written.Namespace != lodiav1alpha1.LeaderElectionNamespace || written.Name != lodiav1alpha1.ControllerManagerLease {
		t.Fatalf("created %s/%s", written.Namespace, written.Name)
	}
	if ptr.Deref(written.Spec.HolderIdentity, "") != lodiav1alpha1.HoldHolderIdentity || ptr.Deref(written.Spec.LeaseDurationSeconds, 0) != heldLeaseSeconds ||
		!written.Spec.AcquireTime.Time.Equal(now) || !written.Spec.RenewTime.Time.Equal(now) || ptr.Deref(written.Spec.LeaseTransitions, 0) != 1 {
		t.Errorf("the created Lease is %+v", written.Spec)
	}
}
