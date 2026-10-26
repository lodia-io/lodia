// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package signer

import (
	"testing"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestTheSignerReadsTheKeyPKIWrites(t *testing.T) {
	if signingKeyFile != lodiav1alpha1.SigningKeyKey {
		t.Errorf("the signer reads %s, PKI writes the signing key under %s", signingKeyFile, lodiav1alpha1.SigningKeyKey)
	}
}
