// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package signer

import (
	"testing"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
)

func TestTheSignerReadsTheKeyPKIWrites(t *testing.T) {
	if signingKeyFile != bryggav1alpha1.SigningKeyKey {
		t.Errorf("the signer reads %s, PKI writes the signing key under %s", signingKeyFile, bryggav1alpha1.SigningKeyKey)
	}
}
