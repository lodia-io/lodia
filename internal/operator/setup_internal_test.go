// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package operator

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHealthRunsOnTheFilesValues(t *testing.T) {
	cfg := DefaultConfig().Health
	cfg.Period = metav1.Duration{Duration: 7 * time.Second}
	cfg.FailuresBeforeFalse = 4
	got := newHealth(nil, nil, cfg)
	if got.Period != 7*time.Second {
		t.Errorf("Period = %s, want 7s", got.Period)
	}
	if got.FailuresBeforeFalse != 4 {
		t.Errorf("FailuresBeforeFalse = %d, want 4", got.FailuresBeforeFalse)
	}
}
