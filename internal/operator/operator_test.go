// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package operator_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

var gatewayAPICRDs = filepath.Join("..", "..", "test", "testdata", "gateway-api", "v1.6.3", "standard-install.yaml")

func TestTheGatewayAPICRDsAreTheReleases(t *testing.T) {
	const (
		release = "https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.6.3"
		digest  = "356c2b8e286a964dc20995435659ad1095386329974793af10bbfe3eff977fe5"
	)
	raw, err := os.ReadFile(gatewayAPICRDs)
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(raw); hex.EncodeToString(sum[:]) != digest {
		t.Errorf("%s has sha256 %x, want %s, the digest of standard-install.yaml at %s", gatewayAPICRDs, sum, digest, release)
	}
}
