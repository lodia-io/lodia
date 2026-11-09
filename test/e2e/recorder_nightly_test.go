// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && (ha || hold)

package e2e

import (
	"testing"
	"time"
)

func (r *recorder) alwaysTrue(t *testing.T, from, to time.Time, conditionTypes ...string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, conditionType := range conditionTypes {
		if longest := r.longestNotTrue(conditionType, from, to); longest > 0 {
			t.Errorf("%s was not True for %s", conditionType, longest)
		}
	}
}
