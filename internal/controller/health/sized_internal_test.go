// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"testing"

	"go.lodia.io/lodia/internal/jsonscan"
	"go.lodia.io/lodia/internal/tenantclient"
)

func sizedForTheBounds(t testing.TB) {
	t.Helper()
	for _, c := range []struct {
		name        string
		bound, most int
	}{
		{"tenantclient.MaxNameRaw", tenantclient.MaxNameRaw, 2048},
		{"tenantclient.MaxVersionRaw", tenantclient.MaxVersionRaw, 64},
		{"maxTimeRaw", maxTimeRaw, 64},
		{"tenantclient.MaxWordRaw", tenantclient.MaxWordRaw, 72},
		{"tenantclient.MaxReasonRaw", tenantclient.MaxReasonRaw, 126},
		{"maxHolderRaw", maxHolderRaw, 512},
		{"maxKubeletRaw", maxKubeletRaw, 384},
		{"tenantclient.MaxNumberRaw", tenantclient.MaxNumberRaw, 11},
		{"tenantclient.MaxMessage", tenantclient.MaxMessage, 1024},
		{"maxConditions", maxConditions, 64},
		{"maxWebhooks", maxWebhooks, 256},
		{"tenantclient.MaxCauses", tenantclient.MaxCauses, 64},
		{"leasePage", leasePage, 500},
		{"nodePage", nodePage, 100},
		{"listLimit", listLimit, 500},
		{"maxKubeletVersion", maxKubeletVersion, 64},
		{"tenantclient.MaxName", tenantclient.MaxName, 253},
		{"maxContinue", maxContinue, 4096},
		{"tenantclient.MaxResponseBytes", tenantclient.MaxResponseBytes, 8 << 20},
		{"tenantclient.MaxResponseHeaderBytes", tenantclient.MaxResponseHeaderBytes, 64 << 10},
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
