// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package operator_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"go.lodia.io/lodia/internal/contract"
	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/controller/health"
	"go.lodia.io/lodia/internal/controller/lifecycle"
	"go.lodia.io/lodia/internal/controller/tenant"
	"go.lodia.io/lodia/internal/controller/workloads"
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

func workers(n int) operator.ControllerConfig { return operator.ControllerConfig{Workers: n} }

func seconds(s float64) metav1.Duration {
	return metav1.Duration{Duration: time.Duration(s * float64(time.Second))}
}

func TestWithoutAFileTheDefaultsApply(t *testing.T) {
	want := operator.Config{
		Health: operator.HealthConfig{
			Period:              seconds(5),
			FailuresBeforeFalse: 3,
			SilentAfter:         seconds(20),
			HoldFraction:        600,
			ReleaseAfter:        seconds(120),
			MaxNodes:            5000,
		},
		Fleet: operator.FleetConfig{MaxConcurrentRollouts: 5},
		Controllers: operator.ControllersConfig{
			PKI: workers(10), KubeconfigRequest: workers(10), Datastore: workers(10), Backup: workers(10), DatastoreSample: workers(10),
			Lifecycle: workers(10), Tenant: workers(10), Health: workers(10), HealthNodes: workers(100), Workloads: workers(10),
		},
	}
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
		contract.ControllerManagerRenewDeadline + contract.ControllerManagerRetryPeriod + 10*time.Second
	if got != 57400*time.Millisecond || got > contract.NodeMonitorGracePeriod || contract.NodeMonitorGracePeriod != 60*time.Second {
		t.Errorf("the defaults add up to %s against a grace period of %s, want 57.4s against 60s", got, contract.NodeMonitorGracePeriod)
	}
}

func TestTheChartsValuesAreTheDefaults(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "lodia", "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		Health      operator.HealthConfig      `json:"health"`
		Fleet       operator.FleetConfig       `json:"fleet"`
		Controllers operator.ControllersConfig `json:"controllers"`
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	if want := operator.DefaultConfig().Health; values.Health != want {
		t.Errorf("the chart's health values are %+v, want the operator's defaults %+v", values.Health, want)
	}
	if want := operator.DefaultConfig().Fleet; values.Fleet != want {
		t.Errorf("the chart's fleet values are %+v, want the operator's defaults %+v", values.Fleet, want)
	}
	if want := operator.DefaultConfig().Controllers; values.Controllers != want {
		t.Errorf("the chart's controllers values are %+v, want the operator's defaults %+v", values.Controllers, want)
	}
}

func TestEachControllersWorkersComeFromTheKeyOfItsName(t *testing.T) {
	names := []string{"pki", "kubeconfigrequest", datastore.Name, datastore.BackupName, datastore.SampleName, lifecycle.Name, tenant.Name, health.Name, health.NodesName, workloads.Name}
	var content strings.Builder
	content.WriteString("controllers:\n")
	for i, name := range names {
		fmt.Fprintf(&content, "  %s:\n    workers: %d\n", name, 11+i)
	}
	got, err := load(t, content.String())
	if err != nil {
		t.Fatal(err)
	}
	c := got.Controllers
	for i, field := range []operator.ControllerConfig{c.PKI, c.KubeconfigRequest, c.Datastore, c.Backup, c.DatastoreSample, c.Lifecycle, c.Tenant, c.Health, c.HealthNodes, c.Workloads} {
		if field.Workers != 11+i {
			t.Errorf("controllers.%s.workers is %d, want %d", names[i], field.Workers, 11+i)
		}
	}
	got, err = load(t, "controllers:\n  health:\n    workers: 30\n  workloads: {}\n")
	want := operator.DefaultConfig()
	want.Controllers.Health.Workers = 30
	if err != nil || got != want {
		t.Errorf("a file that sets health's workers alone gives %+v, %v, want %+v", got, err, want)
	}
}

func TestAControllerWithoutWorkersOrOfNoNameRefusesToStart(t *testing.T) {
	for _, c := range []struct{ name, content, refused string }{
		{"no workers", "controllers:\n  health-nodes:\n    workers: 0\n", "controllers.health-nodes.workers"},
		{"fewer than none", "controllers:\n  tenant:\n    workers: -1\n", "controllers.tenant.workers"},
		{"a name no controller has", "controllers:\n  nodes:\n    workers: 10\n", `"nodes"`},
		{"a key beside workers", "controllers:\n  health:\n    worker: 10\n", `"worker"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got, err := load(t, c.content); err == nil || !strings.Contains(err.Error(), c.refused) {
				t.Errorf("LoadConfig returned %+v, %v, want an error that names %s", got, err, c.refused)
			}
		})
	}
}

func TestAMissingFileRefusesToStart(t *testing.T) {
	_, err := operator.LoadConfig(filepath.Join(t.TempDir(), "config.yaml"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("LoadConfig of a missing file returned %v, want an error that says it does not exist", err)
	}
}

func TestAFileKeepsTheDefaultsOfTheKeysItLeavesOut(t *testing.T) {
	for name, content := range map[string]string{"empty": "", "no keys": "health: {}\nfleet: {}\n"} {
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

func TestTheMaxNodesComeFromTheFile(t *testing.T) {
	got, err := load(t, "health:\n  maxNodes: 120\n")
	want := operator.DefaultConfig()
	want.Health.MaxNodes = 120
	if err != nil || got != want {
		t.Errorf("a file that sets maxNodes to 120 gives %+v, %v, want %+v", got, err, want)
	}
	if got, err := load(t, ""); err != nil || got.Health.MaxNodes != 5000 {
		t.Errorf("a file without maxNodes gives %d, %v, want 5000", got.Health.MaxNodes, err)
	}
}

func TestAnUnknownKeyRefusesToStart(t *testing.T) {
	for _, c := range []struct{ name, content, key string }{
		{"a section no feature reads", "registry: mirror.example\n", `"registry"`},
		{"a key of the health section", "health:\n  perod: 5s\n", `"perod"`},
		{"a key beside the health section", "health:\n  period: 5s\ntenant:\n  applyInterval: 10m\n", `"tenant"`},
		{"a key of the fleet section", "fleet:\n  maxConcurrentRollout: 5\n", `"maxConcurrentRollout"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			if _, err := load(t, c.content); err == nil || !strings.Contains(err.Error(), c.key) {
				t.Errorf("LoadConfig returned %v, want an error that names %s", err, c.key)
			}
		})
	}
}

func TestTheFleetLimitComesFromTheFileAndIsAtLeastOne(t *testing.T) {
	got, err := load(t, "fleet:\n  maxConcurrentRollouts: 2\n")
	want := operator.DefaultConfig()
	want.Fleet.MaxConcurrentRollouts = 2
	if err != nil || got != want {
		t.Errorf("a file that sets the fleet limit to 2 gives %+v, %v, want %+v", got, err, want)
	}
	for _, n := range []string{"0", "-1"} {
		if got, err := load(t, "fleet:\n  maxConcurrentRollouts: "+n+"\n"); err == nil || !strings.Contains(err.Error(), "fleet.maxConcurrentRollouts") {
			t.Errorf("a fleet limit of %s gives %+v, %v, want an error that names fleet.maxConcurrentRollouts", n, got, err)
		}
	}
}

func TestADurationWrittenAsANumberRefusesToStart(t *testing.T) {
	if _, err := load(t, "health:\n  period: 5\n"); err == nil {
		t.Error("LoadConfig accepted a period of 5 with no unit")
	}
}

func TestTheHealthSettingsAreCheckedBeforeTheManagerStarts(t *testing.T) {
	const (
		grace       = contract.NodeMonitorGracePeriod
		controllers = contract.ControllerManagerRenewDeadline + contract.ControllerManagerRetryPeriod
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
		{name: "the defaults written out", content: "health:\n  period: 5s\n  failuresBeforeFalse: 3\n  silentAfter: 20s\n  holdFraction: 0.6\n  releaseAfter: 2m\n  maxNodes: 5000\n", holds: 600},

		{name: "a zero maxNodes", content: "health:\n  maxNodes: 0\n", refused: "health.maxNodes"},
		{name: "a negative maxNodes", content: "health:\n  maxNodes: -1\n", refused: "health.maxNodes"},
		{name: "one node", content: "health:\n  maxNodes: 1\n"},

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
		{name: "values whose sum wraps past the longest duration", content: "health:\n  silentAfter: 2562047h47m\n  period: 2562047h47m10s\n", refused: "grace period"},
		{name: "a period past the grace period", content: "health:\n  period: 61s\n", refused: "grace period"},
		{name: "a silentAfter past the grace period", content: "health:\n  silentAfter: 61s\n", refused: "grace period"},

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
