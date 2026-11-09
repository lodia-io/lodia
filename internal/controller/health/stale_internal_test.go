// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestALongHolderOfAStaleLeaseIsCutInTheMessage(t *testing.T) {
	r := &Reconciler{}
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "kube-scheduler", ResourceVersion: "1"},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: new(strings.Repeat("h", 40000)), LeaseDurationSeconds: new(int32(15))},
	}
	start := time.Now()
	r.stale(key, lease, start)
	got := r.stale(key, lease, start.Add(26*time.Second))
	if got == "" {
		t.Fatal("the Lease is not stale after 26 seconds")
	}
	if !strings.Contains(got, strings.Repeat("h", maxQuoted)+" was last renewed") {
		t.Errorf("the holder is not cut at %d characters: %d characters in all", maxQuoted, len(got))
	}
}
