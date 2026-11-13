// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"runtime"
	"strings"
	"testing"
)

func TestACutStringHoldsNoneOfTheStringItWasCutFrom(t *testing.T) {
	const items = 2000
	body := []byte(`{"metadata":{"name":"` + strings.Repeat("\xff", maxNameRaw) + `"},"status":{"nodeInfo":{"kubeletVersion":"` + strings.Repeat("\xff", maxKubeletRaw) + `"}}}`)
	kept := make([]nodeObject, 0, items)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for range items {
		n, err := nodeOf(body)
		if err != nil {
			t.Fatal(err)
		}
		kept = append(kept, n)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	held := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	runtime.KeepAlive(kept)
	if len(kept[0].Metadata.Name) != maxName+1 || len(kept[0].Status.NodeInfo.KubeletVersion) != maxKubeletVersion+1 {
		t.Fatalf("a Node holds a name of %d bytes and a version of %d", len(kept[0].Metadata.Name), len(kept[0].Status.NodeInfo.KubeletVersion))
	}
	if limit := int64(items * 1024); held > limit {
		t.Errorf("%d Nodes hold %d bytes of heap, %d each, want at most %d each: a name of %d bytes and a version of %d, 3 bytes each decoded from %d and %d", items, held, held/items, limit/items, maxName+1, maxKubeletVersion+1, maxNameRaw, maxKubeletRaw)
	}
}
