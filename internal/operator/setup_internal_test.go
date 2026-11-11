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

func TestLifecycleRunsOnTheFilesValues(t *testing.T) {
	if got := newLifecycle(nil, nil, DefaultConfig(), "v1.0.0+0"); got.Settling != 45*time.Second || got.MaxConcurrentRollouts != 5 {
		t.Errorf("at the defaults Settling = %s and MaxConcurrentRollouts = %d, want 45s and 5", got.Settling, got.MaxConcurrentRollouts)
	}
	cfg := DefaultConfig()
	cfg.Health.Period = metav1.Duration{Duration: 7 * time.Second}
	cfg.Health.FailuresBeforeFalse = 4
	cfg.Fleet.MaxConcurrentRollouts = 2
	got := newLifecycle(nil, nil, cfg, "v1.0.0+0")
	if want := 4*(7*time.Second+newHealth(nil, nil, cfg.Health).Timeout) + 15*time.Second; got.Settling != want {
		t.Errorf("Settling = %s, want %s", got.Settling, want)
	}
	if got.MaxConcurrentRollouts != 2 || got.Revision != "v1.0.0+0" {
		t.Errorf("MaxConcurrentRollouts = %d and Revision = %q, want 2 and v1.0.0+0", got.MaxConcurrentRollouts, got.Revision)
	}
}

func TestTheMetricsRunOnTheFilesLimitAndTheRevisionLifecycleGrants(t *testing.T) {
	in := Inputs{Config: DefaultConfig(), Version: "v1.0.0", SignerImage: "signer", TunnelImage: "tunnel", EtcdDruidVersion: "v0.39.0", EtcdDruidNamespace: "druid", OperatorNamespace: "lodia"}
	in.Config.Fleet.MaxConcurrentRollouts = 2
	revision, err := in.Revision()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Metrics(nil, nil, in)
	if err != nil {
		t.Fatal(err)
	}
	if want := newLifecycle(nil, nil, in.Config, revision); got.Revision != want.Revision || got.MaxConcurrentRollouts != want.MaxConcurrentRollouts {
		t.Errorf("the metrics run on revision %q and limit %d, want Lifecycle's %q and %d", got.Revision, got.MaxConcurrentRollouts, want.Revision, want.MaxConcurrentRollouts)
	}
	if got.MaxConcurrentRollouts != 2 {
		t.Errorf("MaxConcurrentRollouts = %d, want the file's 2", got.MaxConcurrentRollouts)
	}
}
