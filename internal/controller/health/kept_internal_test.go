// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

func retained(build func() any) (any, uint64) {
	var deltas []int64
	for range 3 {
		before := liveHeap()
		v := build()
		deltas = append(deltas, int64(liveHeap())-int64(before))
		runtime.KeepAlive(v)
	}
	slices.Sort(deltas)
	return build(), uint64(max(deltas[1], 0))
}

func repeated(n int, item string) string {
	return strings.TrimSuffix(strings.Repeat(item+",", n), ",")
}

func TestWhatTheTypesKeepAtTheRawBoundsIsUnderKeptAtMost(t *testing.T) {
	for _, kind := range []struct {
		name string
		of   func(int) string
	}{
		{"letters", func(n int) string { return strings.Repeat("a", n) }},
		{"invalid UTF-8", func(n int) string { return strings.Repeat("\xff", n) }},
	} {
		s := kind.of
		ready := `{"type":"Ready","status":"` + s(maxWordRaw) + `","lastTransitionTime":"2026-11-13T10:00:00Z"}`
		node := `{"metadata":{"name":"` + s(maxNameRaw) + `"},"status":{"nodeInfo":{"kubeletVersion":"` + s(maxKubeletRaw) + `"},"conditions":[` + repeated(maxConditions, ready) + `]}}`
		lease := `{"metadata":{"name":"` + s(maxNameRaw) + `","resourceVersion":"` + s(maxVersionRaw) + `"}}`
		svc := `{"metadata":{"namespace":"` + s(maxNameRaw) + `","name":"` + s(maxNameRaw) + `"},"spec":{"type":"` + s(maxWordRaw) + `"}}`
		pvc := `{"metadata":{"namespace":"` + s(maxNameRaw) + `","name":"` + s(maxNameRaw) + `"},"status":{"phase":"` + s(maxWordRaw) + `"}}`
		pv := `{"metadata":{"name":"` + s(maxNameRaw) + `"},"spec":{"persistentVolumeReclaimPolicy":"` + s(maxWordRaw) + `"}}`
		cron := `{"metadata":{"namespace":"` + s(maxNameRaw) + `","name":"` + s(maxNameRaw) + `"},"spec":{"suspend":false}}`
		hook := `{"name":"` + s(maxNameRaw) + `","failurePolicy":"` + s(maxWordRaw) + `","clientConfig":{"url":"x"}}`
		config := `{"metadata":{"name":"` + s(maxNameRaw) + `"},"webhooks":[` + repeated(maxWebhooks, hook) + `]}`
		list := func(n int, item string) []byte {
			return []byte(`{"metadata":{"continue":""},"items":[` + repeated(n, item) + `]}`)
		}
		for _, c := range []struct {
			name string
			body []byte
			read func([]byte) (any, error)
		}{
			{"100 Nodes of 64 Ready conditions", list(nodePage, node), func(b []byte) (any, error) { return listFrom(b, nodePage, nodeOf) }},
			{"500 Leases", list(leasePage, lease), func(b []byte) (any, error) { return listFrom(b, leasePage, leaseItemOf) }},
			{"500 Services", list(listLimit, svc), func(b []byte) (any, error) { return listFrom(b, listLimit, serviceOf) }},
			{"500 PersistentVolumeClaims", list(listLimit, pvc), func(b []byte) (any, error) { return listFrom(b, listLimit, persistentVolumeClaimOf) }},
			{"500 PersistentVolumes", list(listLimit, pv), func(b []byte) (any, error) { return listFrom(b, listLimit, persistentVolumeOf) }},
			{"500 CronJobs", list(listLimit, cron), func(b []byte) (any, error) { return listFrom(b, listLimit, cronJobOf) }},
			{"one webhook configuration of 256 webhooks", []byte(config), func(b []byte) (any, error) { return webhookConfigurationOf(b) }},
		} {
			t.Run(c.name+", "+kind.name, func(t *testing.T) {
				if len(c.body) > maxResponseBytes {
					t.Fatalf("a body of %d bytes", len(c.body))
				}
				var err error
				v, kept := retained(func() any {
					var out any
					out, err = c.read(c.body)
					return out
				})
				if err != nil {
					t.Fatal(err)
				}
				runtime.KeepAlive(v)
				t.Logf("a body of %d bytes: the decoded page holds %.2f MiB", len(c.body), mib(kept))
				if kept > keptAtMost {
					t.Errorf("the decoded page holds %.2f MiB, want at most keptAtMost, %.2f MiB", mib(kept), mib(keptAtMost))
				}
			})
		}
	}
}

func TestWhatAPassBuildsBesideTheSamplingIsUnderSummariesAtMost(t *testing.T) {
	const nodes = 5000
	name := func(i int) string { return fmt.Sprintf("%05d", i) + strings.Repeat("n", maxName-5) }
	v, kept := retained(func() any {
		var leases []leaseVersion
		var summaries []node
		for p := range nodes / leasePage {
			page := make([]leaseItem, leasePage)
			for i := range page {
				page[i].Metadata.Name = strings.Clone(name(p*leasePage + i))
				page[i].Metadata.ResourceVersion = "12345678901234567890"
			}
			leases = append(leases, leaseVersions(page)...)
		}
		for p := range nodes / nodePage {
			page := make([]nodeObject, nodePage)
			for i := range page {
				page[i].Metadata.Name = strings.Clone(name(p*nodePage + i))
				page[i].Status.NodeInfo.KubeletVersion = strings.Repeat("v", maxKubeletVersion)
			}
			got, _ := summarize(page)
			summaries = append(summaries, got...)
		}
		return []any{leases, seen(nil, leases, time.Time{}), summaries}
	})
	runtime.KeepAlive(v)
	t.Logf("%d Leases and %d Nodes named with %d bytes: the summaries a pass builds hold %.2f MiB", nodes, nodes, maxName, mib(kept))
	if kept > summariesAtMost {
		t.Errorf("the summaries hold %.2f MiB, want at most summariesAtMost, %.2f MiB", mib(kept), mib(summariesAtMost))
	}
}
