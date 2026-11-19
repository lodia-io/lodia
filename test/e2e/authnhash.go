// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"fmt"
	"strings"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

const lastConfigInfo = "apiserver_authentication_config_controller_last_config_info"

// loadedConfigHash is the label hash of the one sample of
// apiserver_authentication_config_controller_last_config_info in metrics, the
// SHA-256 of the authentication configuration the API server loaded.
func loadedConfigHash(metrics string) (string, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(metrics))
	if err != nil {
		return "", err
	}
	samples := families[lastConfigInfo].GetMetric()
	if len(samples) != 1 {
		return "", fmt.Errorf("%d samples of %s", len(samples), lastConfigInfo)
	}
	for _, label := range samples[0].GetLabel() {
		if label.GetName() == "hash" {
			return label.GetValue(), nil
		}
	}
	return "", fmt.Errorf("%s has no label hash", lastConfigInfo)
}
