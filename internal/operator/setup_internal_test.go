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

func TestHealthNodesRunOnTheFilesValues(t *testing.T) {
	cfg := DefaultConfig().Health
	cfg.Period = metav1.Duration{Duration: 7 * time.Second}
	cfg.SilentAfter = metav1.Duration{Duration: 25 * time.Second}
	cfg.HoldFraction = 750
	got := newNodes(nil, nil, cfg)
	if got.Period != 7*time.Second {
		t.Errorf("Period = %s, want 7s", got.Period)
	}
	if got.SilentAfter != 25*time.Second {
		t.Errorf("SilentAfter = %s, want 25s", got.SilentAfter)
	}
	if got.HoldFraction != 750 {
		t.Errorf("HoldFraction = %d, want 750", got.HoldFraction)
	}
	if want := newHealth(nil, nil, cfg).Timeout; got.Timeout != want {
		t.Errorf("Timeout = %s, want Health's %s", got.Timeout, want)
	}
}
