// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
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
	jsonType     = "application/json"
	protobufType = "application/vnd.kubernetes.protobuf"

	peakPerPassInResponses = 18
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

func heapPeak(run func()) (peak uint64) {
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	heap := func() uint64 {
		metrics.Read(sample)
		return sample[0].Value.Uint64()
	}
	defer debug.SetGCPercent(debug.SetGCPercent(100))
	runtime.GC()
	base := heap()
	done := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			now := heap()
			peak = max(peak, now-min(now, base))
		}
	})
	run()
	close(done)
	sampler.Wait()
	return peak
}

func TestOnePassOverPagesAtTheBoundKeepsTheHeapUnderItsPeak(t *testing.T) {
	for _, c := range []struct {
		name, contentType string
		encode            func(k8sruntime.Object) ([]byte, error)
	}{
		{"JSON", jsonType, func(o k8sruntime.Object) ([]byte, error) { return json.Marshal(o) }},
		{"protobuf", protobufType, func(o k8sruntime.Object) ([]byte, error) {
			return k8sruntime.Encode(protobuf.NewSerializer(scheme.Scheme, scheme.Scheme), o)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			leases, nodes := paddedPages(t, c.encode)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				body := nodes
				if req.URL.Path == leasesPath {
					body = leases
				}
				w.Header().Set("Content-Type", c.contentType)
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				_, _ = w.Write(body)
			}))
			t.Cleanup(server.Close)
			r := &NodesReconciler{Timeout: 5 * time.Minute, MaxNodes: 5000}
			tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
			if err != nil {
				t.Fatal(err)
			}
			s := &sampling{}
			var listErr error
			peak := heapPeak(func() { _, listErr = r.list(t.Context(), tenant, s, time.Now()) })
			if listErr != nil || s.leasesStop == "" || s.nodesStop == "" {
				t.Fatalf("list returned %v, stopped %q and %q, want both lists stopped at maxNodes", listErr, s.leasesStop, s.nodesStop)
			}
			t.Logf("the heap peaked at %d bytes, %.2f responses of %d bytes", peak, float64(peak)/maxResponseBytes, maxResponseBytes)
			if peak > peakPerPassInResponses*maxResponseBytes {
				t.Errorf("the heap peaked at %d bytes, more than %d responses of %d bytes", peak, peakPerPassInResponses, maxResponseBytes)
			}
		})
	}
}
