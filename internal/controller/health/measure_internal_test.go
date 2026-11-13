// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"go.lodia.io/lodia/internal/jsonscan"
)

const (
	measureHeap  = "LODIA_MEASURE_HEAP"
	measureGOGC  = 5
	measureCount = 100
)

func paddedPages(t *testing.T) (leases, nodes []byte) {
	t.Helper()
	page := func(items int, body func(pad string) any) []byte {
		pad := 1
		for range 8 {
			out, err := json.Marshal(body(strings.Repeat("x", pad)))
			if err != nil {
				t.Fatal(err)
			}
			room := maxResponseBytes - len(out)
			if room >= 0 && room < items {
				return out
			}
			if room < 0 {
				pad -= (-room + items - 1) / items
			} else {
				pad += room / items
			}
		}
		t.Fatalf("no padding makes a page of %d items just under %d bytes", items, maxResponseBytes)
		return nil
	}
	meta := func(i int, pad string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: fmt.Sprintf("n%05d", i), ResourceVersion: "1", Annotations: map[string]string{"pad": pad}}
	}
	list := metav1.ListMeta{ResourceVersion: "1", Continue: "more"}
	leases = page(leasePage, func(pad string) any {
		out := &coordinationv1.LeaseList{TypeMeta: metav1.TypeMeta{Kind: "LeaseList", APIVersion: "coordination.k8s.io/v1"}, ListMeta: list}
		for i := range leasePage {
			out.Items = append(out.Items, coordinationv1.Lease{ObjectMeta: meta(i, pad)})
		}
		return out
	})
	nodes = page(nodePage, func(pad string) any {
		out := &corev1.NodeList{TypeMeta: metav1.TypeMeta{Kind: "NodeList", APIVersion: "v1"}, ListMeta: list}
		for i := range nodePage {
			n := corev1.Node{ObjectMeta: meta(i, pad)}
			n.Status.NodeInfo.KubeletVersion = "v1.35.0"
			n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
			out.Items = append(out.Items, n)
		}
		return out
	})
	return leases, nodes
}

type heapPeaks struct{ live, inUse uint64 }

func measureLiveHeap(run func()) heapPeaks {
	defer debug.SetGCPercent(debug.SetGCPercent(measureGOGC))
	samples := []metrics.Sample{{Name: "/gc/heap/live:bytes"}, {Name: "/memory/classes/heap/objects:bytes"}}
	read := func() (live, inUse uint64) {
		metrics.Read(samples)
		return samples[0].Value.Uint64(), samples[1].Value.Uint64()
	}
	runtime.GC()
	baseLive, baseInUse := read()
	var peaks heapPeaks
	done := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			live, inUse := read()
			peaks.live = max(peaks.live, live-min(live, baseLive))
			peaks.inUse = max(peaks.inUse, inUse-min(inUse, baseInUse))
		}
	})
	run()
	close(done)
	sampler.Wait()
	return peaks
}

func TestMeasureTheLiveHeapOfPassesOverTenantsPaddedToTheirBounds(t *testing.T) {
	if os.Getenv(measureHeap) == "" {
		t.Skipf("set %s to measure the heap of passes over tenants padded to their bounds", measureHeap)
	}
	leases, nodes := paddedPages(t)
	var served atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body := nodes
		if req.URL.Path == nodeLeasesPath {
			body = leases
		}
		served.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	r := &NodesReconciler{Timeout: 10 * time.Minute, MaxNodes: 5000}
	pass := func(name string) {
		tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: name}, kubeconfigFor(t, server.URL))
		if err != nil {
			t.Error(err)
			return
		}
		s := &sampling{}
		if _, err := r.list(t.Context(), tenant, s, time.Now()); err != nil || s.leasesStop == "" || s.nodesStop == "" {
			t.Errorf("pass over %s returned %v, stopped %q and %q, want both lists stopped at maxNodes", name, err, s.leasesStop, s.nodesStop)
		}
	}

	begun := time.Now()
	one := measureLiveHeap(func() { pass("dev") })
	t.Logf("one pass over %d bytes of padded pages, %d responses, took %v: live heap %.1f MiB, %.2f responses of %d bytes; heap in use %.1f MiB",
		maxResponseBytes, served.Load(), time.Since(begun).Round(time.Millisecond), mib(one.live), float64(one.live)/maxResponseBytes, maxResponseBytes, mib(one.inUse))

	served.Store(0)
	begun = time.Now()
	many := measureLiveHeap(func() {
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range measureCount {
			wg.Go(func() {
				<-start
				pass(fmt.Sprintf("dev-%03d", i))
			})
		}
		close(start)
		wg.Wait()
	})
	t.Logf("%d passes at once on %d tenants, %d responses, took %v: live heap %.1f MiB, %.1f MiB a pass; heap in use %.1f MiB; %d times one pass %.1f MiB",
		measureCount, measureCount, served.Load(), time.Since(begun).Round(time.Millisecond), mib(many.live), mib(many.live)/measureCount, mib(many.inUse), measureCount, measureCount*mib(one.live))
}

func fill(size int, head, tail string) []byte {
	const element = `{}`
	n := (size - len(head) - len(tail) + 1) / (len(element) + 1)
	var out bytes.Buffer
	out.Grow(size)
	out.WriteString(head)
	for i := range n {
		if i > 0 {
			out.WriteByte(',')
		}
		out.WriteString(element)
	}
	out.WriteString(tail)
	return out.Bytes()
}

func escaped(size int, head, tail string) []byte {
	return []byte(head + strings.Repeat("a", size-len(head)-len(tail)-len(`\n`)) + `\n` + tail)
}

func zeros(size int, head, tail string) []byte {
	return []byte(head + strings.Repeat("0", size-len(head)-len(tail)) + tail)
}

func invalid(size int, head, tail string) []byte {
	return append(append([]byte(head), bytes.Repeat([]byte{0xff}, size-len(head)-len(tail))...), tail...)
}

func escapedKey(raw int) []byte {
	return []byte(`{"metadata":{},"items":[],"` + strings.Repeat(`\n`, raw/2) + `":0}`)
}

func leaseStop(field string, most int) string {
	return fmt.Sprintf("kept 0, stopped %q, error <nil>", fmt.Sprintf("%s in the Leases of kube-node-lease is longer than %d bytes", field, most))
}

func nodeStop(field string, most int) string {
	return fmt.Sprintf("kept 0, stopped %q, error <nil>", fmt.Sprintf("%s in the Nodes is longer than %d bytes", field, most))
}

func padding(size int, head, tail string) []byte {
	return []byte(head + strings.Repeat("a", size-len(head)-len(tail)) + tail)
}

type amplifier struct {
	name   string
	status int
	body   []byte
	read   func(context.Context, *tenant, *NodesReconciler) string
	want   string
}

func readLeases(ctx context.Context, t *tenant, r *NodesReconciler) string {
	kept, stop, err := r.listLeases(ctx, t)
	return fmt.Sprintf("kept %d, stopped %q, error %v", len(kept), stop, err)
}

func readNodes(ctx context.Context, t *tenant, r *NodesReconciler) string {
	kept, stop, err := r.listNodes(ctx, t)
	return fmt.Sprintf("kept %d, stopped %q, error %v", len(kept), stop, err)
}

func readFirstPage[T any](path string, element func([]byte) (T, error)) func(context.Context, *tenant, *NodesReconciler) string {
	return func(ctx context.Context, t *tenant, _ *NodesReconciler) string {
		_, err := firstPage(ctx, t, path, element)
		return fmt.Sprintf("error %v", err)
	}
}

func readWebhooks(path string) func(context.Context, *tenant, *NodesReconciler) string {
	return func(ctx context.Context, t *tenant, _ *NodesReconciler) string {
		found, err := outsideWebhooks(ctx, t, path)
		return fmt.Sprintf("named %d, more %t, error %v", found.n, found.more, err)
	}
}

func readLeaderHolder(ctx context.Context, t *tenant, _ *NodesReconciler) string {
	lease, err := t.getLease(ctx, "kube-controller-manager")
	if err != nil {
		return fmt.Sprintf("error %v", err)
	}
	return fmt.Sprintf("holder of %d bytes", len(ptr.Deref(lease.Spec.HolderIdentity, "")))
}

func readLeaderError(ctx context.Context, t *tenant, _ *NodesReconciler) string {
	_, err := t.getLease(ctx, "kube-controller-manager")
	return fmt.Sprintf("error of %d bytes", len(err.Error()))
}

func readLeader(ctx context.Context, t *tenant, _ *NodesReconciler) string {
	lease, err := t.getLease(ctx, "kube-controller-manager")
	if err != nil {
		return fmt.Sprintf("error %v", err)
	}
	return fmt.Sprintf("holder %q", ptr.Deref(lease.Spec.HolderIdentity, ""))
}

func readPatched(ctx context.Context, t *tenant, _ *NodesReconciler) string {
	lease, err := t.patchLease(ctx, []byte(`{}`))
	if err != nil {
		return fmt.Sprintf("error %v", err)
	}
	return fmt.Sprintf("holder %q", ptr.Deref(lease.Spec.HolderIdentity, ""))
}

func amplifiers(size int) []amplifier {
	items := fill(size, `{"items":[`, `]}`)
	managed := func(rest string) []byte {
		return fill(size, `{"items":[{"metadata":{"name":"n","resourceVersion":"1","managedFields":[`, `]}`+rest+`}]}`)
	}
	object := func(rest string) []byte {
		return fill(size, `{"metadata":{"resourceVersion":"1","managedFields":[`, `]}`+rest+`}`)
	}
	const (
		overItems   = "error " + "the response has more elements in an array than Health reads"
		leaseHolder = `holder "h"`
	)
	return []amplifier{
		{"a page of Leases of empty items", 200, items, readLeases, `kept 500, stopped "a page of the Leases of kube-node-lease holds more than 500 items", error <nil>`},
		{"a page of Nodes of empty items", 200, items, readNodes, `kept 100, stopped "a page of the Nodes holds more than 100 items", error <nil>`},
		{"a Lease with empty managedFields", 200, managed(``), readLeases, `kept 1, stopped "", error <nil>`},
		{"a Node with empty managedFields", 200, managed(`,"status":{"conditions":[{"type":"Ready","status":"True"}]}`), readNodes, `kept 1, stopped "", error <nil>`},
		{"a Node with empty conditions", 200, fill(size, `{"items":[{"metadata":{"name":"n"},"status":{"conditions":[`, `]}}]}`), readNodes, `kept 0, stopped "a Node has more than 64 conditions", error <nil>`},
		{"a leader Lease with empty managedFields", 200, object(`,"spec":{"holderIdentity":"h"}`), readLeader, leaseHolder},
		{"an answer to a patch with empty managedFields", 200, object(`,"spec":{"holderIdentity":"h"}`), readPatched, leaseHolder},
		{"a failed request with empty causes", 500, fill(size, `{"kind":"Status","status":"Failure","reason":"InternalError","code":500,"message":"m","details":{"causes":[`, `]}}`), readLeader, "error m"},
		{"a list of Services of empty items", 200, items, readFirstPage("/api/v1/services", serviceOf), overItems},
		{"a list of PersistentVolumes of empty items", 200, items, readFirstPage("/api/v1/persistentvolumes", persistentVolumeOf), overItems},
		{"a list of PersistentVolumeClaims of empty items", 200, items, readFirstPage("/api/v1/persistentvolumeclaims", persistentVolumeClaimOf), overItems},
		{"a list of webhook configurations of empty items", 200, items, readWebhooks("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations"), "named 0, more false, " + overItems},
		{"a webhook configuration with empty webhooks", 200, fill(size, `{"items":[{"metadata":{"name":"c"},"webhooks":[`, `]}]}`), readWebhooks("/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations"), "named 0, more false, error read the response: " + strings.TrimPrefix(overItems, "error ")},
		{"a list of APIServices of empty items", 200, items, readFirstPage("/apis/apiregistration.k8s.io/v1/apiservices", apiServiceOf), overItems},
		{"a list of CronJobs of empty items", 200, items, readFirstPage("/apis/batch/v1/cronjobs", cronJobOf), overItems},
		{"a page of Leases of one Lease padded", 200, padding(size, `{"metadata":{},"items":[{"metadata":{"name":"n","resourceVersion":"1","annotations":{"pad":"`, `"}}}]}`), readLeases, `kept 1, stopped "", error <nil>`},
		{"a leader Lease padded", 200, padding(size, `{"metadata":{"resourceVersion":"1","annotations":{"pad":"`, `"}},"spec":{"holderIdentity":"h"}}`), readLeader, leaseHolder},
		{"a Lease with a resourceVersion of escapes", 200, escaped(size, `{"metadata":{},"items":[{"metadata":{"name":"n","resourceVersion":"`, `"}}]}`), readLeases, leaseStop("metadata.resourceVersion", 64)},
		{"a Lease named with escapes", 200, escaped(size, `{"metadata":{},"items":[{"metadata":{"name":"`, `"}}]}`), readLeases, leaseStop("metadata.name", 2048)},
		{"a Lease named with invalid UTF-8", 200, invalid(size, `{"metadata":{},"items":[{"metadata":{"name":"`, `"}}]}`), readLeases, leaseStop("metadata.name", 2048)},
		{"a page of Leases with a continue token of escapes", 200, escaped(size, `{"metadata":{"continue":"`, `"},"items":[]}`), readLeases, leaseStop("metadata.continue", 4096)},
		{"a page of Leases with a continue token of invalid UTF-8", 200, invalid(size, `{"metadata":{"continue":"`, `"},"items":[]}`), readLeases, leaseStop("metadata.continue", 4096)},
		{"a page of Leases with a key of invalid UTF-8", 200, invalid(size, `{"metadata":{},"items":[],"`, `":0}`), readLeases, `kept 0, stopped "", error <nil>`},
		{"a page of Leases with a key of letters", 200, padding(size, `{"metadata":{},"items":[],"`, `":0}`), readLeases, `kept 0, stopped "", error <nil>`},
		{"a page of Leases with a key of escapes", 200, escaped(size, `{"metadata":{},"items":[],"`, `":0}`), readLeases, `kept 0, stopped "", error <nil>`},
		{"a page of Leases with a key of escapes at the bound", 200, escapedKey(jsonscan.MaxKey), readLeases, `kept 0, stopped "", error <nil>`},
		{"a page of Leases of one Lease padded with invalid UTF-8", 200, invalid(size, `{"metadata":{},"items":[{"metadata":{"name":"n","resourceVersion":"1","annotations":{"pad":"`, `"}}}]}`), readLeases, `kept 1, stopped "", error <nil>`},
		{"a Node with a kubelet version of escapes", 200, escaped(size, `{"metadata":{},"items":[{"metadata":{"name":"n"},"status":{"nodeInfo":{"kubeletVersion":"`, `"}}}]}`), readNodes, nodeStop("status.nodeInfo.kubeletVersion", 384)},
		{"a Node named with escapes", 200, escaped(size, `{"metadata":{},"items":[{"metadata":{"name":"`, `"}}]}`), readNodes, nodeStop("metadata.name", 2048)},
		{"a Node named with invalid UTF-8", 200, invalid(size, `{"metadata":{},"items":[{"metadata":{"name":"`, `"}}]}`), readNodes, nodeStop("metadata.name", 2048)},
		{"a Node whose condition has a time of letters", 200, padding(size, `{"metadata":{},"items":[{"metadata":{"name":"n"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"`, `"}]}}]}`), readNodes, nodeStop("status.conditions.lastTransitionTime", 64)},
		{"a Node whose condition has a time of invalid UTF-8", 200, invalid(size, `{"metadata":{},"items":[{"metadata":{"name":"n"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"`, `"}]}}]}`), readNodes, nodeStop("status.conditions.lastTransitionTime", 64)},
		{"a Node whose condition has a time that is no time", 200, []byte(`{"metadata":{},"items":[{"metadata":{"name":"n"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"` + strings.Repeat("a", 64) + `"}]}}]}`), readNodes, `kept 0, stopped "", error read the response: status.conditions.lastTransitionTime is not a time`},
		{"a Node whose other condition has a time of letters", 200, padding(size, `{"metadata":{},"items":[{"metadata":{"name":"n"},"status":{"conditions":[{"type":"MemoryPressure","status":"False","lastTransitionTime":"`, `"}]}}]}`), readNodes, `kept 1, stopped "", error <nil>`},
		{"a leader Lease with a holder of escapes", 200, escaped(size, `{"metadata":{"resourceVersion":"1"},"spec":{"holderIdentity":"`, `"}}`), readLeaderHolder, "error read the response: spec.holderIdentity is longer than 512 bytes"},
		{"a leader Lease with a holder of invalid UTF-8", 200, invalid(size, `{"metadata":{"resourceVersion":"1"},"spec":{"holderIdentity":"`, `"}}`), readLeaderHolder, "error read the response: spec.holderIdentity is longer than 512 bytes"},
		{"a leader Lease with a resourceVersion of escapes", 200, escaped(size, `{"metadata":{"resourceVersion":"`, `"},"spec":{"holderIdentity":"h"}}`), readLeader, "error read the response: metadata.resourceVersion is longer than 64 bytes"},
		{"a leader Lease with a renewTime of invalid UTF-8", 200, invalid(size, `{"metadata":{"resourceVersion":"1"},"spec":{"renewTime":"`, `"}}`), readLeader, "error read the response: spec.renewTime is longer than 64 bytes"},
		{"a leader Lease with an acquireTime of letters", 200, []byte(`{"metadata":{"resourceVersion":"1"},"spec":{"acquireTime":"` + strings.Repeat("a", 64) + `"}}`), readLeader, "error read the response: spec.acquireTime is not a time"},
		{"a leader Lease with a number of digits", 200, zeros(size, `{"metadata":{"resourceVersion":"1"},"spec":{"leaseTransitions":1`, `}}`), readLeader, "error read the response: spec.leaseTransitions is longer than 11 bytes"},
		{"a failed request with a message of escapes", 500, escaped(size, `{"kind":"Status","status":"Failure","reason":"InternalError","code":500,"message":"`, `"}`), readLeaderError, "error of " + strconv.Itoa(len(longMessage)) + " bytes"},
		{"a failed request with a message of invalid UTF-8", 500, invalid(size, `{"kind":"Status","status":"Failure","reason":"InternalError","code":500,"message":"`, `"}`), readLeaderError, "error of " + strconv.Itoa(len(longMessage)) + " bytes"},
		{"a failed request with a reason of invalid UTF-8", 500, invalid(size, `{"kind":"Status","status":"Failure","code":500,"message":"m","reason":"`, `"}`), readLeaderError, "error of " + strconv.Itoa(len(noStatusMessage)) + " bytes"},
		{"a Service named with escapes", 200, escaped(size, `{"items":[{"metadata":{"namespace":"n","name":"`, `"},"spec":{"type":"LoadBalancer"}}]}`), readFirstPage("/api/v1/services", serviceOf), "error read the response: metadata.name is longer than 2048 bytes"},
		{"a Service of a type of invalid UTF-8", 200, invalid(size, `{"items":[{"metadata":{"namespace":"n","name":"s"},"spec":{"type":"`, `"}}]}`), readFirstPage("/api/v1/services", serviceOf), "error <nil>"},
		{"a webhook configuration named with escapes", 200, escaped(size, `{"items":[{"webhooks":[{}],"metadata":{"name":"`, `"}}]}`), readWebhooks("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations"), "named 0, more false, error read the response: metadata.name is longer than 2048 bytes"},
		{"a webhook with a failure policy of invalid UTF-8", 200, invalid(size, `{"items":[{"metadata":{"name":"c"},"webhooks":[{"failurePolicy":"`, `"}]}]}`), readWebhooks("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations"), "named 0, more false, error <nil>"},
	}
}

func answering(c amplifier) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(c.body)))
		w.WriteHeader(c.status)
		_, _ = w.Write(c.body)
	})
}

func servingEveryPath(t *testing.T, c amplifier) (*NodesReconciler, *tenant) {
	t.Helper()
	return http1.tenant(t, answering(c))
}

func TestAnAmplifyingResponseIsReadAsFarAsItsBoundsAllow(t *testing.T) {
	for _, c := range amplifiers(1 << 20) {
		t.Run(c.name, func(t *testing.T) {
			r, tenant := servingEveryPath(t, c)
			if got := c.read(t.Context(), tenant, r); got != c.want {
				t.Errorf("the read says %q, want %q", got, c.want)
			}
		})
	}
}

func TestMeasureTheLiveHeapOfEveryReadOverAnAmplifyingResponse(t *testing.T) {
	if os.Getenv(measureHeap) == "" {
		t.Skipf("set %s to measure the heap of every read over an amplifying response", measureHeap)
	}
	for _, p := range protocols {
		for _, c := range amplifiers(maxResponseBytes) {
			t.Run(p.name+", "+c.name, func(t *testing.T) {
				r, tenant := p.tenant(t, answering(c))
				var outcome string
				begun := time.Now()
				peaks := measureLiveHeap(func() { outcome = c.read(t.Context(), tenant, r) })
				t.Logf("a response of %d bytes took %v: live heap %.1f MiB, %.2f responses; heap in use %.1f MiB; %s",
					len(c.body), time.Since(begun).Round(time.Millisecond), mib(peaks.live), float64(peaks.live)/maxResponseBytes, mib(peaks.inUse), outcome)
				if outcome != c.want {
					t.Errorf("the read says %q, want %q", outcome, c.want)
				}
				if bound := readsBound(p, 1, len(c.body)); peaks.inUse > bound {
					t.Errorf("the read held %.2f MiB of heap in use, more than the structural bound of %.2f MiB", mib(peaks.inUse), mib(bound))
				}
			})
		}
	}
}

func mib(bytes uint64) float64 { return float64(bytes) / (1 << 20) }

const (
	mebibyte = 1 << 20

	connectionHTTP1 = 64 << 10
	connectionHTTP2 = mebibyte + 64<<10
	streamBuffer    = 4<<20 + 32<<10
	headersHTTP1    = 17 * mebibyte / 10
	headersHTTP2    = mebibyte / 2
	keptAtMost      = 2 * mebibyte
	summariesAtMost = 4 * mebibyte
)

func readsBound(p protocol, reads, body int) uint64 {
	each := uint64(2.6*float64(body)) + headersHTTP1
	connections := uint64(reads) * connectionHTTP1
	if p.http2 {
		each, connections = uint64(2.6*float64(body))+headersHTTP2+streamBuffer, connectionHTTP2
	}
	return max(uint64(reads)*each+connections, uint64(reads)*(uint64(body)+keptAtMost))
}

func servingTogether(t *testing.T, p protocol, answers map[string][]byte, requests int) *tenant {
	t.Helper()
	var (
		mu      sync.Mutex
		arrived int
	)
	release := make(chan struct{})
	return servingOver(t, p, answers, func() {
		mu.Lock()
		if arrived++; arrived == requests {
			close(release)
		}
		mu.Unlock()
		<-release
	})
}

func servingAPass(t *testing.T, p protocol, lease, leases, nodes []byte) *tenant {
	t.Helper()
	var (
		mu      sync.Mutex
		arrived int
	)
	together := make(chan struct{})
	_, tenant := p.tenant(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body := lease
		switch req.URL.Path {
		case nodeLeasesPath, nodesPath:
			body = nodes
			if req.URL.Path == nodeLeasesPath {
				body = leases
			}
			mu.Lock()
			if arrived++; arrived == 2 {
				close(together)
			}
			mu.Unlock()
			<-together
		}
		if req.Method == http.MethodPatch {
			_, _ = io.Copy(io.Discard, req.Body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	return tenant
}

func TestMeasureTheLiveHeapOfAPassWhoseEveryReadIsAtItsWorst(t *testing.T) {
	if os.Getenv(measureHeap) == "" {
		t.Skipf("set %s to measure the heap of a pass whose every read is at its worst", measureHeap)
	}
	measure := func(t *testing.T, bound uint64, run func()) {
		begun := time.Now()
		peaks := measureLiveHeap(run)
		t.Logf("took %v: live heap %.1f MiB, %.2f responses of %d bytes; heap in use %.1f MiB",
			time.Since(begun).Round(time.Millisecond), mib(peaks.live), float64(peaks.live)/maxResponseBytes, maxResponseBytes, mib(peaks.inUse))
		if peaks.inUse > bound {
			t.Errorf("the pass held %.1f MiB of heap in use, more than the structural bound of %.1f MiB", mib(peaks.inUse), mib(bound))
		}
	}
	body := padding(maxResponseBytes, `{"items":[{"webhooks":[{}],"spec":{"type":"LoadBalancer"},"metadata":{"namespace":"n","name":"x"},"pad":"`, `"}]}`)
	answers := map[string][]byte{
		servicesPath: body, volumesPath: body, claimsPath: body, validatingPath: body, mutatingPath: body, apiServicePath: body, cronJobsPath: body,
	}
	_, reference := http1.tenant(t, answering(amplifier{status: http.StatusOK, body: []byte(`{"items":[{"webhooks":[{}],"spec":{"type":"LoadBalancer"},"metadata":{"namespace":"n","name":"x"}}]}`)}))
	wantRestore := (&Reconciler{Timeout: 10 * time.Minute}).actedOn(t.Context(), reference)
	for _, p := range protocols {
		t.Run(p.name+", a pass of health-nodes: the Lease, both lists together and the renewal", func(t *testing.T) {
			tenant := servingAPass(t, p,
				padding(maxResponseBytes, `{"metadata":{"resourceVersion":"1","annotations":{"pad":"`, `"}},"spec":{"holderIdentity":"lodia"}}`),
				padding(maxResponseBytes, `{"metadata":{},"items":[{"metadata":{"name":"n","resourceVersion":"1","annotations":{"pad":"`, `"}}}]}`),
				padding(maxResponseBytes, `{"metadata":{},"items":[{"metadata":{"name":"n","annotations":{"pad":"`, `"}},"status":{"nodeInfo":{"kubeletVersion":"v1.35.0"},"conditions":[{"type":"Ready","status":"True"}]}}]}`))
			r := &NodesReconciler{Timeout: 10 * time.Minute, MaxNodes: 5000}
			measure(t, readsBound(p, 2, maxResponseBytes)+summariesAtMost, func() {
				read, err := r.readLease(t.Context(), tenant)
				if err != nil {
					t.Error(err)
					return
				}
				s := &sampling{}
				leases, err := r.list(t.Context(), tenant, s, time.Now())
				if err != nil || len(leases) != 1 || len(s.nodes) != 1 || s.leasesStop != "" || s.nodesStop != "" {
					t.Errorf("the lists returned %d Leases, %d Nodes, stops %q and %q, error %v, want one of each", len(leases), len(s.nodes), s.leasesStop, s.nodesStop, err)
				}
				if _, err := r.writeLease(t.Context(), tenant, read, renewLease, time.Now()); err != nil {
					t.Error(err)
				}
			})
		})
		t.Run(p.name+", a pass of health over a held restore", func(t *testing.T) {
			tenant := servingOver(t, p, answers, func() {})
			var got string
			measure(t, readsBound(p, len(answers), maxResponseBytes), func() { got = (&Reconciler{Timeout: 10 * time.Minute}).actedOn(t.Context(), tenant) })
			if got != wantRestore {
				t.Errorf("the pass says %q, want %q", got, wantRestore)
			}
		})
		t.Run(p.name+", a pass of health over a held restore whose seven lists are answered together", func(t *testing.T) {
			tenant := servingTogether(t, p, answers, len(answers))
			var got string
			measure(t, readsBound(p, len(answers), maxResponseBytes), func() { got = (&Reconciler{Timeout: 10 * time.Minute}).actedOn(t.Context(), tenant) })
			if got != wantRestore {
				t.Errorf("the pass says %q, want %q", got, wantRestore)
			}
		})
	}
}

func distinctNames(budget, overhead int) []string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	var names []string
	for length, total := 1, len(alphabet); ; length, total = length+1, total*len(alphabet) {
		for i := range total {
			if budget < length+overhead {
				return names
			}
			name := make([]byte, length)
			for at, rest := length-1, i; at >= 0; at, rest = at-1, rest/len(alphabet) {
				name[at] = alphabet[rest%len(alphabet)]
			}
			names = append(names, string(name))
			budget -= length + overhead
		}
	}
}

func cutAnswers(t *testing.T, p protocol, body []byte, headers bool) *tenant {
	t.Helper()
	if !p.http2 {
		head := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n"
		if headers {
			for _, name := range distinctNames(maxResponseHeaderBytes-len(head)-len("\r\n"), len(":\r\n")) {
				head += name + ":\r\n"
			}
		}
		head += "\r\n"
		if headers && len(head) != maxResponseHeaderBytes && len(head) < maxResponseHeaderBytes-8 {
			t.Fatalf("the head has %d bytes, want it to fill the bound of %d", len(head), maxResponseHeaderBytes)
		}
		url, _ := answerOnTCP(t, head, string(body))
		r := &NodesReconciler{Timeout: 10 * time.Minute, MaxNodes: 5000}
		tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, url))
		if err != nil {
			t.Fatal(err)
		}
		return tenant
	}
	var names []string
	if headers {
		names = distinctNames(maxResponseHeaderBytes+320-200, 32)
	}
	_, tenant := p.tenant(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for _, name := range names {
			w.Header()[textproto.CanonicalMIMEHeaderKey(name)] = []string{""}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	return tenant
}

func TestMeasureTheLiveHeapOfAReadWhoseAnswerIsCutAtTheBoundWithHeadersAtTheirs(t *testing.T) {
	if os.Getenv(measureHeap) == "" {
		t.Skipf("set %s to measure the heap of a read whose answer is cut, with headers at their bound", measureHeap)
	}
	body := bytes.Repeat([]byte("a"), 2*maxResponseBytes)
	for _, p := range protocols {
		for _, headers := range []bool{false, true} {
			name := p.name + ", an answer of 16 MiB"
			if headers {
				name += ", headers at their bound"
			}
			t.Run(name, func(t *testing.T) {
				tenant := cutAnswers(t, p, body, headers)
				bound := readsBound(p, 1, maxResponseBytes)
				var most heapPeaks
				var over, wrong int
				var outcome error
				for range measureCount {
					peaks := measureLiveHeap(func() { _, outcome = tenant.getLease(t.Context(), "kube-controller-manager") })
					most = heapPeaks{max(most.live, peaks.live), max(most.inUse, peaks.inUse)}
					if peaks.inUse > bound {
						over++
					}
					if !errors.Is(outcome, errResponseTooLong) {
						wrong++
					}
				}
				t.Logf("%d runs: the most live heap %.2f MiB, the most heap in use %.2f MiB; the structural bound %.2f MiB; %d runs over it", measureCount, mib(most.live), mib(most.inUse), mib(bound), over)
				if wrong > 0 {
					t.Errorf("%d runs did not end in %q, the last in %v", wrong, errResponseTooLong, outcome)
				}
				if over > 0 {
					t.Errorf("%d runs held more heap in use than the structural bound of %.2f MiB, the most %.2f MiB", over, mib(bound), mib(most.inUse))
				}
			})
		}
	}
}
