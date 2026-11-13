// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
)

const (
	measureHeap  = "LODIA_MEASURE_HEAP"
	measureGOGC  = 5
	measureCount = 100
)

func paddedPages(t *testing.T, encode func(k8sruntime.Object) ([]byte, error)) (leases, nodes []byte) {
	t.Helper()
	page := func(items int, body func(pad string) k8sruntime.Object) []byte {
		pad := 1
		for range 8 {
			out, err := encode(body(strings.Repeat("x", pad)))
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
	leases = page(leasePage, func(pad string) k8sruntime.Object {
		out := &coordinationv1.LeaseList{TypeMeta: metav1.TypeMeta{Kind: "LeaseList", APIVersion: "coordination.k8s.io/v1"}, ListMeta: list}
		for i := range leasePage {
			out.Items = append(out.Items, coordinationv1.Lease{ObjectMeta: meta(i, pad)})
		}
		return out
	})
	nodes = page(nodePage, func(pad string) k8sruntime.Object {
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
	for _, c := range []struct {
		name, contentType string
		encode            func(k8sruntime.Object) ([]byte, error)
	}{
		{"JSON", "application/json", func(o k8sruntime.Object) ([]byte, error) { return json.Marshal(o) }},
		{"protobuf", "application/vnd.kubernetes.protobuf", func(o k8sruntime.Object) ([]byte, error) {
			return k8sruntime.Encode(protobuf.NewSerializer(scheme.Scheme, scheme.Scheme), o)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			leases, nodes := paddedPages(t, c.encode)
			var served atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				body := nodes
				if req.URL.Path == leasesPath {
					body = leases
				}
				served.Add(1)
				w.Header().Set("Content-Type", c.contentType)
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
		})
	}
}

func mib(bytes uint64) float64 { return float64(bytes) / (1 << 20) }
