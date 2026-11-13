// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bytes"
	"context"
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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
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

func readFirstPage[T any](path string) func(context.Context, *tenant, *NodesReconciler) string {
	return func(ctx context.Context, t *tenant, _ *NodesReconciler) string {
		_, err := firstPage[T](ctx, t, path)
		return fmt.Sprintf("error %v", err)
	}
}

func readWebhooks(path string) func(context.Context, *tenant, *NodesReconciler) string {
	return func(ctx context.Context, t *tenant, _ *NodesReconciler) string {
		names, more, err := outsideWebhooks(ctx, t, path)
		return fmt.Sprintf("named %d, more %t, error %v", len(names), more, err)
	}
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
		{"a list of Services of empty items", 200, items, readFirstPage[service]("/api/v1/services"), overItems},
		{"a list of PersistentVolumes of empty items", 200, items, readFirstPage[persistentVolume]("/api/v1/persistentvolumes"), overItems},
		{"a list of PersistentVolumeClaims of empty items", 200, items, readFirstPage[persistentVolumeClaim]("/api/v1/persistentvolumeclaims"), overItems},
		{"a list of webhook configurations of empty items", 200, items, readWebhooks("/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations"), "named 0, more false, " + overItems},
		{"a webhook configuration with empty webhooks", 200, fill(size, `{"items":[{"metadata":{"name":"c"},"webhooks":[`, `]}]}`), readWebhooks("/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations"), "named 0, more false, " + overItems},
		{"a list of APIServices of empty items", 200, items, readFirstPage[apiService]("/apis/apiregistration.k8s.io/v1/apiservices"), overItems},
		{"a list of CronJobs of empty items", 200, items, readFirstPage[cronJob]("/apis/batch/v1/cronjobs"), overItems},
	}
}

func servingEveryPath(t *testing.T, c amplifier) (*NodesReconciler, *tenant) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(c.body)))
		w.WriteHeader(c.status)
		_, _ = w.Write(c.body)
	}))
	t.Cleanup(server.Close)
	r := &NodesReconciler{Timeout: 10 * time.Minute, MaxNodes: 5000}
	tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
	if err != nil {
		t.Fatal(err)
	}
	return r, tenant
}

func TestAResponseOfNothingButEmptyObjectsStopsOrFailsAtTheBoundOfItsArray(t *testing.T) {
	for _, c := range amplifiers(1 << 20) {
		t.Run(c.name, func(t *testing.T) {
			r, tenant := servingEveryPath(t, c)
			if got := c.read(t.Context(), tenant, r); got != c.want {
				t.Errorf("the read says %q, want %q", got, c.want)
			}
		})
	}
}

func TestMeasureTheLiveHeapOfEveryReadOverAResponseOfNothingButEmptyObjects(t *testing.T) {
	if os.Getenv(measureHeap) == "" {
		t.Skipf("set %s to measure the heap of every read over a response of empty objects", measureHeap)
	}
	for _, c := range amplifiers(maxResponseBytes) {
		t.Run(c.name, func(t *testing.T) {
			r, tenant := servingEveryPath(t, c)
			var outcome string
			begun := time.Now()
			peaks := measureLiveHeap(func() { outcome = c.read(t.Context(), tenant, r) })
			t.Logf("a response of %d bytes took %v: live heap %.1f MiB, %.2f responses; heap in use %.1f MiB; %s",
				len(c.body), time.Since(begun).Round(time.Millisecond), mib(peaks.live), float64(peaks.live)/maxResponseBytes, mib(peaks.inUse), outcome)
		})
	}
}

func mib(bytes uint64) float64 { return float64(bytes) / (1 << 20) }
