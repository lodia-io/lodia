// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

func TestALongHolderOfAStaleLeaseIsCutInTheMessage(t *testing.T) {
	r := &Reconciler{}
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	lease := &leaderLease{}
	lease.Metadata.ResourceVersion = "1"
	lease.Spec.HolderIdentity, lease.Spec.LeaseDurationSeconds = new(strings.Repeat("h", 40000)), new(int32(15))
	start := time.Now()
	r.stale(key, "kube-scheduler", lease, start)
	got := r.stale(key, "kube-scheduler", lease, start.Add(26*time.Second))
	if got == "" {
		t.Fatal("the Lease is not stale after 26 seconds")
	}
	if !strings.Contains(got, " of "+strings.Repeat("h", maxQuoted)+" was last renewed") {
		t.Errorf("the holder is not cut at %d characters: %d characters in all", maxQuoted, len(got))
	}
}
