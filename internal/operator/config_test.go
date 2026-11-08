// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package operator_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/operator"
)

func load(t *testing.T, content string) (operator.Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return operator.LoadConfig(path)
}

func seconds(s float64) metav1.Duration {
	return metav1.Duration{Duration: time.Duration(s * float64(time.Second))}
}

func TestWithoutAFileTheDefaultsApply(t *testing.T) {
	want := operator.Config{Health: operator.HealthConfig{
		Period:              seconds(5),
		FailuresBeforeFalse: 3,
		SilentAfter:         seconds(20),
		HoldFraction:        600,
		ReleaseAfter:        seconds(120),
	}}
	got, err := operator.LoadConfig("")
	if err != nil || got != want {
		t.Errorf("LoadConfig(\"\") is %+v, %v, want %+v", got, err, want)
	}
	if got := operator.DefaultConfig(); got != want {
		t.Errorf("DefaultConfig() is %+v, want %+v", got, want)
	}
}

func TestTheDefaultsLeaveTheMarginAgainstTheGracePeriod(t *testing.T) {
	h := operator.DefaultConfig().Health
	got := h.SilentAfter.Duration + h.Period.Duration + 10400*time.Millisecond +
		lodiav1alpha1.ControllerManagerRenewDeadline + lodiav1alpha1.ControllerManagerRetryPeriod + 10*time.Second
	if got != 57400*time.Millisecond || got > lodiav1alpha1.NodeMonitorGracePeriod || lodiav1alpha1.NodeMonitorGracePeriod != 60*time.Second {
		t.Errorf("the defaults add up to %s against a grace period of %s, want 57.4s against 60s", got, lodiav1alpha1.NodeMonitorGracePeriod)
	}
}

func TestTheChartsValuesAreTheDefaults(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "lodia", "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		Health operator.HealthConfig `json:"health"`
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	if want := operator.DefaultConfig().Health; values.Health != want {
		t.Errorf("the chart's health values are %+v, want the operator's defaults %+v", values.Health, want)
	}
}

func TestAMissingFileRefusesToStart(t *testing.T) {
	_, err := operator.LoadConfig(filepath.Join(t.TempDir(), "config.yaml"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("LoadConfig of a missing file returned %v, want an error that says it does not exist", err)
	}
}

func TestAFileKeepsTheDefaultsOfTheKeysItLeavesOut(t *testing.T) {
	for name, content := range map[string]string{"empty": "", "no keys": "health: {}\n"} {
		t.Run(name, func(t *testing.T) {
			got, err := load(t, content)
			if err != nil || got != operator.DefaultConfig() {
				t.Errorf("a file with %s gives %+v, %v, want the defaults", name, got, err)
			}
		})
	}
	got, err := load(t, "health:\n  period: 4s\n")
	want := operator.DefaultConfig()
	want.Health.Period = seconds(4)
	if err != nil || got != want {
		t.Errorf("a file that sets only the period gives %+v, %v, want %+v", got, err, want)
	}
}

func TestAnUnknownKeyRefusesToStart(t *testing.T) {
	for _, c := range []struct{ name, content, key string }{
		{"a section no feature reads", "registry: mirror.example\n", `"registry"`},
		{"a key of the health section", "health:\n  perod: 5s\n", `"perod"`},
		{"a key beside the health section", "health:\n  period: 5s\nfleet:\n  maxConcurrentRollouts: 5\n", `"fleet"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			if _, err := load(t, c.content); err == nil || !strings.Contains(err.Error(), c.key) {
				t.Errorf("LoadConfig returned %v, want an error that names %s", err, c.key)
			}
		})
	}
}

func TestADurationWrittenAsANumberRefusesToStart(t *testing.T) {
	if _, err := load(t, "health:\n  period: 5\n"); err == nil {
		t.Error("LoadConfig accepted a period of 5 with no unit")
	}
}

func TestTheHealthSettingsAreCheckedBeforeTheManagerStarts(t *testing.T) {
	const (
		grace       = lodiav1alpha1.NodeMonitorGracePeriod
		controllers = lodiav1alpha1.ControllerManagerRenewDeadline + lodiav1alpha1.ControllerManagerRetryPeriod
		renewals    = 10400 * time.Millisecond
		slack       = 10 * time.Second
	)
	period := 5 * time.Second
	edge := grace - period - renewals - controllers - slack
	if edge != 22600*time.Millisecond {
		t.Fatalf("the largest silentAfter at a period of 5s is %s, want 22.6s", edge)
	}
	for _, c := range []struct {
		name    string
		content string
		refused string
		holds   operator.Thousandths
	}{
		{name: "the defaults written out", content: "health:\n  period: 5s\n  failuresBeforeFalse: 3\n  silentAfter: 20s\n  holdFraction: 0.6\n  releaseAfter: 2m\n", holds: 600},

		{name: "a zero period", content: "health:\n  period: 0s\n", refused: "health.period"},
		{name: "a negative period", content: "health:\n  period: -5s\n", refused: "health.period"},
		{name: "the shortest period that fits", content: "health:\n  period: 1ms\n"},

		{name: "a zero releaseAfter", content: "health:\n  releaseAfter: 0s\n", refused: "health.releaseAfter"},
		{name: "a negative releaseAfter", content: "health:\n  releaseAfter: -1m\n", refused: "health.releaseAfter"},
		{name: "the shortest releaseAfter that fits", content: "health:\n  releaseAfter: 1ns\n"},

		{name: "zero failuresBeforeFalse", content: "health:\n  failuresBeforeFalse: 0\n", refused: "health.failuresBeforeFalse"},
		{name: "negative failuresBeforeFalse", content: "health:\n  failuresBeforeFalse: -1\n", refused: "health.failuresBeforeFalse"},
		{name: "one failure before False", content: "health:\n  failuresBeforeFalse: 1\n"},

		{name: "a holdFraction of one half", content: "health:\n  holdFraction: 0.5\n", refused: "health.holdFraction"},
		{name: "a holdFraction that rounds down to one half", content: "health:\n  holdFraction: 0.5004\n", refused: "health.holdFraction"},
		{name: "a holdFraction below one half", content: "health:\n  holdFraction: 0.4996\n", refused: "health.holdFraction"},
		{name: "a zero holdFraction", content: "health:\n  holdFraction: 0\n", refused: "health.holdFraction"},
		{name: "a negative holdFraction", content: "health:\n  holdFraction: -0.6\n", refused: "health.holdFraction"},
		{name: "the least holdFraction above one half", content: "health:\n  holdFraction: 0.501\n", holds: 501},
		{name: "a holdFraction that rounds up above one half", content: "health:\n  holdFraction: 0.5006\n", holds: 501},
		{name: "a holdFraction of 1", content: "health:\n  holdFraction: 1\n", holds: 1000},
		{name: "a holdFraction that rounds down to 1", content: "health:\n  holdFraction: 1.0004\n", holds: 1000},
		{name: "a holdFraction above 1", content: "health:\n  holdFraction: 1.001\n", refused: "health.holdFraction"},

		{name: "a silentAfter of one period and the renewals", content: "health:\n  silentAfter: 15.4s\n", refused: "health.silentAfter"},
		{name: "a silentAfter just above them", content: "health:\n  silentAfter: 15.401s\n"},
		{name: "the same at a period of 6s", content: "health:\n  period: 6s\n  silentAfter: 16.4s\n", refused: "health.silentAfter"},
		{name: "just above it at a period of 6s", content: "health:\n  period: 6s\n  silentAfter: 16.401s\n"},
		{name: "a zero silentAfter", content: "health:\n  silentAfter: 0s\n", refused: "health.silentAfter"},

		{name: "a margin of exactly the grace period", content: "health:\n  silentAfter: " + edge.String() + "\n"},
		{name: "a millisecond over the grace period", content: "health:\n  silentAfter: " + (edge + time.Millisecond).String() + "\n", refused: "node monitor grace period"},
		{name: "a period that tips it over", content: "health:\n  period: 5.001s\n  silentAfter: " + edge.String() + "\n", refused: "node monitor grace period"},
		{name: "a period that leaves it under", content: "health:\n  period: 4.999s\n  silentAfter: " + edge.String() + "\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := load(t, c.content)
			if c.refused != "" {
				if err == nil || !strings.Contains(err.Error(), c.refused) {
					t.Fatalf("LoadConfig returned %+v, %v, want an error that mentions %q", got, err, c.refused)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfig refused: %v", err)
			}
			if c.holds != 0 && got.Health.HoldFraction != c.holds {
				t.Errorf("holdFraction is %d thousandths, want %d", got.Health.HoldFraction, c.holds)
			}
		})
	}
}
