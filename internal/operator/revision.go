// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package operator

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"unicode/utf8"
)

// maxRevisionLength is the MaxLength of the fields that hold a revision
// (api/v1alpha1, Target, Rendered and DatastoreStatus).
const maxRevisionLength = 128

// Revision is the operator revision, "<version>+<digest>"
// (docs/decisions/upgrades-and-maintenance-windows.md).
func (in Inputs) Revision() (string, error) {
	if in.Version == "" {
		return "", errors.New("the operator was built without a version: set VERSION when building it with ko")
	}
	if !utf8.ValidString(in.Version) {
		return "", fmt.Errorf("the operator's version %q is not UTF-8, which status fields would not hold as written", in.Version)
	}
	sum := sha256.Sum256(fmt.Appendf(nil, "%q", []string{in.SignerImage, in.TunnelImage, in.EtcdDruidVersion, in.EtcdDruidNamespace, in.OperatorNamespace}))
	revision := in.Version + "+" + hex.EncodeToString(sum[:])
	if len(revision) > maxRevisionLength {
		return "", fmt.Errorf("the operator revision %q is %d characters, more than the %d its fields hold: shorten the version", revision, len(revision), maxRevisionLength)
	}
	return revision, nil
}
