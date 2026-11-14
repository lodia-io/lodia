// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build !race

package health_test

import (
	"testing"
	"time"
)

func TestATenantOfMaxNodesNodesIsCountedWholeAtTheOperatorsTimeout(t *testing.T) {
	countATenantOfMaxNodes(t, 5*time.Second)
}
