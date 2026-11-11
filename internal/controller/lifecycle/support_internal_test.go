// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

var endOf137 = time.Date(2027, 10, 28, 0, 0, 0, 0, time.UTC)

func TestTheEndOfSupportIsNotedFromSixtyDaysBefore(t *testing.T) {
	const reaches, reached = "Kubernetes 1.37 reaches its end of support on 2027-10-28", "Kubernetes 1.37 reached its end of support on 2027-10-28"
	for _, c := range []struct {
		name    string
		version string
		now     time.Time
		want    string
	}{
		{"61 days before", "v1.37.1", endOf137.AddDate(0, 0, -61), ""},
		{"a second before 60 days", "v1.37.1", endOf137.AddDate(0, 0, -60).Add(-time.Second), ""},
		{"60 days before", "v1.37.1", endOf137.AddDate(0, 0, -60), reaches},
		{"a second before the date", "v1.37.1", endOf137.Add(-time.Second), reaches},
		{"0 days before", "v1.37.1", endOf137, reached},
		{"after it", "v1.37.1", endOf137.AddDate(1, 0, 0), reached},
		{"another minor's date", "v1.35.8", time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC), "Kubernetes 1.35 reaches its end of support on 2027-02-28"},
		{"a minor without a date", "v1.34.9", endOf137, ""},
		{"no version reported", "", endOf137, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := supportNotice(c.version, c.now); got != c.want {
				t.Errorf("the notice of %s at %s is %q, want %q", c.version, c.now, got, c.want)
			}
		})
	}
}

func TestTheEndOfSupportEndsProgressingsMessageOnEveryReason(t *testing.T) {
	const notice = "; Kubernetes 1.37 reaches its end of support on 2027-10-28"
	sixtyBefore := when(endOf137.AddDate(0, 0, -60))
	for _, c := range []struct {
		name, reason string
		f            facts
	}{
		{"Idle", lodiav1alpha1.ReasonIdle, held(running(revisionA), sixtyBefore)},
		{"Paused", lodiav1alpha1.ReasonPaused, held(sixtyBefore, func(f *facts) { f.cp.Spec.Paused = true })},
		{"WaitingForWindow, after the token's note", lodiav1alpha1.ReasonWaitingForWindow, held(sixtyBefore, inWindow("20:00", "21:00", ""), requested(strings.Repeat("x", 257)))},
		{"Maintaining", lodiav1alpha1.ReasonMaintaining, held(sixtyBefore)},
		{"Upgrading", lodiav1alpha1.ReasonUpgrading, held(running(revisionA), sixtyBefore, specAt("v1.37.2"))},
		{"Unsupported", lodiav1alpha1.ReasonUnsupported, held(sixtyBefore, specAt("v1.38.0"))},
		{"Failed", lodiav1alpha1.ReasonFailed, held(running(revisionA), sixtyBefore, upgrading(lodiav1alpha1.UpgradeStepAPIServer), specAt("v1.37.2"), targetAt("v1.37.2"),
			condition(metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused}))},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := meta.FindStatusCondition(decide(c.f).status.Conditions, lodiav1alpha1.ConditionProgressing)
			if got == nil || got.Reason != c.reason || !strings.HasSuffix(got.Message, notice) || strings.Count(got.Message, notice) != 1 {
				t.Errorf("Progressing is %+v, want %s ending with %q", got, c.reason, notice)
			}
		})
	}
	t.Run("a deleting pass keeps Progressing as it is", func(t *testing.T) {
		f := held(sixtyBefore, func(f *facts) { f.cp.DeletionTimestamp = &started })
		stored := meta.FindStatusCondition(f.cp.Status.Conditions, lodiav1alpha1.ConditionProgressing).DeepCopy()
		if got := meta.FindStatusCondition(decide(f).status.Conditions, lodiav1alpha1.ConditionProgressing); got == nil || *got != *stored {
			t.Errorf("a deleting pass wrote Progressing %+v, want the stored %+v", got, stored)
		}
	})
	t.Run("the minor is the oldest reported, not the target's", func(t *testing.T) {
		f := held(running(revisionA), sixtyBefore, specAt("v1.37.1"), targetAt("v1.37.1"), runsAt("v1.36.6"), upgrading(lodiav1alpha1.UpgradeStepAPIServer))
		if got := meta.FindStatusCondition(decide(f).status.Conditions, lodiav1alpha1.ConditionProgressing); got == nil || !strings.HasSuffix(got.Message, "; Kubernetes 1.36 reached its end of support on 2027-06-28") {
			t.Errorf("Progressing is %+v, want the notice of 1.36, which status.kubernetes.version names", got)
		}
	})
}
