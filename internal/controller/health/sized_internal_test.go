// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"testing"

	"go.lodia.io/lodia/internal/jsonscan"
)

func sizedForTheBounds(t testing.TB) {
	t.Helper()
	for _, c := range []struct {
		name        string
		bound, most int
	}{
		{"maxNameRaw", maxNameRaw, 2048},
		{"maxVersionRaw", maxVersionRaw, 64},
		{"maxTimeRaw", maxTimeRaw, 64},
		{"maxWordRaw", maxWordRaw, 72},
		{"maxReasonRaw", maxReasonRaw, 126},
		{"maxHolderRaw", maxHolderRaw, 512},
		{"maxKubeletRaw", maxKubeletRaw, 384},
		{"maxNumberRaw", maxNumberRaw, 11},
		{"maxMessageRaw", maxMessageRaw, 1024},
		{"maxConditions", maxConditions, 64},
		{"maxWebhooks", maxWebhooks, 256},
		{"maxCauses", maxCauses, 64},
		{"leasePage", leasePage, 500},
		{"nodePage", nodePage, 100},
		{"listLimit", listLimit, 500},
		{"maxKubeletVersion", maxKubeletVersion, 64},
		{"maxName", maxName, 253},
		{"maxContinue", maxContinue, 4096},
		{"maxResponseBytes", maxResponseBytes, 8 << 20},
		{"maxResponseHeaderBytes", maxResponseHeaderBytes, 64 << 10},
		{"jsonscan.MaxKey", jsonscan.MaxKey, 256},
	} {
		if c.bound > c.most {
			t.Fatalf("%s is %d, past the %d that the inputs of this test are sized for", c.name, c.bound, c.most)
		}
	}
}

func TestEveryBoundATestBuildsAnInputFromIsNoMoreThanItsInputIsSizedFor(t *testing.T) {
	sizedForTheBounds(t)
}
