// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package operator

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	// renewalSpread is how long after the first silent node's last renewal
	// the last one's can come: a kubelet renews its Lease every quarter of
	// its 40 s duration, with up to 4% jitter, so 10 s + 0.4 s
	// (https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubelet/kubelet.go#L1155-L1165,
	// https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubelet/apis/config/v1beta1/defaults.go#L150,
	// https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/component-helpers/apimachinery/lease/controller.go#L153).
	renewalSpread = 10*time.Second + 400*time.Millisecond

	// holdSlack is what the margin of docs/decisions/controller-manager-hold.md
	// keeps for a delay in Health's queue, in the API or in a failover of the
	// operator.
	holdSlack = 10 * time.Second
)

// Config is the operator's configuration file, read with --config. It holds
// only the sections some feature reads; a key outside them refuses to start.
type Config struct {
	Health HealthConfig `json:"health"`
}

// HealthConfig holds Health's settings.
type HealthConfig struct {
	Period              metav1.Duration `json:"period"`
	FailuresBeforeFalse int             `json:"failuresBeforeFalse"`
	SilentAfter         metav1.Duration `json:"silentAfter"`
	HoldFraction        Thousandths     `json:"holdFraction"`
	ReleaseAfter        metav1.Duration `json:"releaseAfter"`
}

// Thousandths is a fraction held as a whole number of thousandths, rounded
// from the value in the file, so that the checks and the share they guard
// compare the same integer.
type Thousandths int

func (t *Thousandths) UnmarshalJSON(data []byte) error {
	var fraction float64
	if err := json.Unmarshal(data, &fraction); err != nil {
		return err
	}
	*t = Thousandths(math.Round(fraction * 1000))
	return nil
}

// DefaultConfig is what the operator runs with when it is given no file, and
// for every key a file leaves out.
func DefaultConfig() Config {
	return Config{Health: HealthConfig{
		Period:              metav1.Duration{Duration: 5 * time.Second},
		FailuresBeforeFalse: 3,
		SilentAfter:         metav1.Duration{Duration: 20 * time.Second},
		HoldFraction:        600,
		ReleaseAfter:        metav1.Duration{Duration: 2 * time.Minute},
	}}
}

// LoadConfig reads the file at path, strictly, over the defaults, and checks
// the result. An empty path gives the defaults.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read the configuration: %w", err)
		}
		if err := yaml.UnmarshalStrict(raw, &cfg); err != nil {
			return Config{}, fmt.Errorf("decode %s: %w", path, err)
		}
	}
	if err := cfg.Health.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (h HealthConfig) validate() error {
	period, silentAfter := h.Period.Duration, h.SilentAfter.Duration
	switch {
	case period <= 0:
		return fmt.Errorf("health.period is %s, want a positive duration", period)
	case h.ReleaseAfter.Duration <= 0:
		return fmt.Errorf("health.releaseAfter is %s, want a positive duration", h.ReleaseAfter.Duration)
	case h.FailuresBeforeFalse <= 0:
		return fmt.Errorf("health.failuresBeforeFalse is %d, want a positive number", h.FailuresBeforeFalse)
	case h.HoldFraction <= 500 || h.HoldFraction > 1000:
		return fmt.Errorf("health.holdFraction is %d thousandths, want more than 500 and at most 1000: one dead machine of two must not start a hold", h.HoldFraction)
	case max(period, silentAfter) > lodiav1alpha1.NodeMonitorGracePeriod:
		return fmt.Errorf("health.period %s and health.silentAfter %s must each fit the node monitor grace period %s", period, silentAfter, lodiav1alpha1.NodeMonitorGracePeriod)
	case silentAfter <= renewalSpread+period:
		return fmt.Errorf("health.silentAfter is %s, want more than %s, the kubelets' renewals %s apart and one period, or a healthy node reads as silent", silentAfter, renewalSpread+period, renewalSpread)
	}
	hold := silentAfter + period + renewalSpread + lodiav1alpha1.ControllerManagerRenewDeadline + lodiav1alpha1.ControllerManagerRetryPeriod + holdSlack
	if hold > lodiav1alpha1.NodeMonitorGracePeriod {
		return fmt.Errorf("health.silentAfter %s, health.period %s, the kubelets' renewals %s apart, the controller manager's renew deadline %s and retry period %s and %s of slack add up to %s, past the node monitor grace period %s: the first node could be marked before the controller manager exits",
			silentAfter, period, renewalSpread, lodiav1alpha1.ControllerManagerRenewDeadline, lodiav1alpha1.ControllerManagerRetryPeriod, holdSlack, hold, lodiav1alpha1.NodeMonitorGracePeriod)
	}
	return nil
}
