// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"go.lodia.io/lodia/internal/tenantclient"
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
	inTheStepWithoutEnvtest(t)
	sizedForTheBounds(t)
	for _, kind := range []struct {
		name string
		of   func(int) string
	}{
		{"letters", func(n int) string { return strings.Repeat("a", n) }},
		{"invalid UTF-8", func(n int) string { return strings.Repeat("\xff", n) }},
	} {
		s := kind.of
		ready := `{"type":"Ready","status":"` + s(tenantclient.MaxWordRaw) + `","lastTransitionTime":"2026-11-13T10:00:00Z"}`
		node := `{"metadata":{"name":"` + s(tenantclient.MaxNameRaw) + `"},"status":{"nodeInfo":{"kubeletVersion":"` + s(maxKubeletRaw) + `"},"conditions":[` + repeated(maxConditions, ready) + `]}}`
		lease := `{"metadata":{"name":"` + s(tenantclient.MaxNameRaw) + `","resourceVersion":"` + s(tenantclient.MaxVersionRaw) + `"}}`
		svc := `{"metadata":{"namespace":"` + s(tenantclient.MaxNameRaw) + `","name":"` + s(tenantclient.MaxNameRaw) + `"},"spec":{"type":"` + s(tenantclient.MaxWordRaw) + `"}}`
		pvc := `{"metadata":{"namespace":"` + s(tenantclient.MaxNameRaw) + `","name":"` + s(tenantclient.MaxNameRaw) + `"},"status":{"phase":"` + s(tenantclient.MaxWordRaw) + `"}}`
		pv := `{"metadata":{"name":"` + s(tenantclient.MaxNameRaw) + `"},"spec":{"persistentVolumeReclaimPolicy":"` + s(tenantclient.MaxWordRaw) + `"}}`
		cron := `{"metadata":{"namespace":"` + s(tenantclient.MaxNameRaw) + `","name":"` + s(tenantclient.MaxNameRaw) + `"},"spec":{"suspend":false}}`
		hook := `{"name":"` + s(tenantclient.MaxNameRaw) + `","failurePolicy":"` + s(tenantclient.MaxWordRaw) + `","clientConfig":{"url":"x"}}`
		config := `{"metadata":{"name":"` + s(tenantclient.MaxNameRaw) + `"},"webhooks":[` + repeated(maxWebhooks, hook) + `]}`
		status := `{"kind":"Status","status":"Failure","reason":"` + s(tenantclient.MaxReasonRaw) + `","message":"` + s(tenantclient.MaxMessageRaw) + `","code":500,"details":{"causes":[` + repeated(tenantclient.MaxCauses, `{"reason":"`+s(tenantclient.MaxReasonRaw)+`"}`) + `]}}`
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
			{"a Status of 64 causes", []byte(status), func(b []byte) (any, error) {
				failure, ok := tenantclient.FailureOf(b)
				if !ok {
					return nil, errors.New(tenantclient.NoStatusMessage)
				}
				return failure, nil
			}},
		} {
			t.Run(c.name+", "+kind.name, func(t *testing.T) {
				if len(c.body) > tenantclient.MaxResponseBytes {
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
	inTheStepWithoutEnvtest(t)
	sizedForTheBounds(t)
	const nodes = 5000
	name := func(i int) string { return fmt.Sprintf("%05d", i) + strings.Repeat("n", tenantclient.MaxName-5) }
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
	t.Logf("%d Leases and %d Nodes named with %d bytes: the summaries a pass builds hold %.2f MiB", nodes, nodes, tenantclient.MaxName, mib(kept))
	if kept > summariesAtMost {
		t.Errorf("the summaries hold %.2f MiB, want at most summariesAtMost, %.2f MiB", mib(kept), mib(summariesAtMost))
	}
}
