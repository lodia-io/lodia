// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"weak"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

type decodedPages struct {
	mu    sync.Mutex
	pages map[string][][]func() bool
}

func weakly[T any](ptr *T) func() bool {
	w := weak.Make(ptr)
	return func() bool { return w.Value() != nil }
}

func (d *decodedPages) add(path string, page ...func() bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.pages == nil {
		d.pages = map[string][][]func() bool{}
	}
	d.pages[path] = append(d.pages[path], page)
}

func (d *decodedPages) of(path string) [][]func() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([][]func() bool(nil), d.pages[path]...)
}

type watchedLeases struct {
	coordinationv1client.LeaseInterface
	decoded *decodedPages
}

func (w watchedLeases) List(ctx context.Context, options metav1.ListOptions) (*coordinationv1.LeaseList, error) {
	list, err := w.LeaseInterface.List(ctx, options)
	if err == nil {
		w.decoded.add(leasesPath, weakly(list), weakly(&list.Items[0]))
	}
	return list, err
}

type watchedCore struct {
	corev1client.CoreV1Interface
	decoded *decodedPages
}

func (c watchedCore) Nodes() corev1client.NodeInterface {
	return watchedNodes{c.CoreV1Interface.Nodes(), c.decoded}
}

type watchedNodes struct {
	corev1client.NodeInterface
	decoded *decodedPages
}

func (n watchedNodes) List(ctx context.Context, options metav1.ListOptions) (*corev1.NodeList, error) {
	list, err := n.NodeInterface.List(ctx, options)
	if err == nil {
		n.decoded.add(nodesPath, weakly(list), weakly(&list.Items[0]))
	}
	return list, err
}

func TestNoDecodedPageIsReachableWhenTheNextOneIsRequested(t *testing.T) {
	for _, c := range []struct {
		name     string
		protobuf bool
	}{{"JSON", false}, {"protobuf", true}} {
		t.Run(c.name, func(t *testing.T) {
			var decoded decodedPages
			f := &servedTenant{leases: 2000, nodes: 500, protobuf: c.protobuf}
			f.wrap = func(tn *tenant) {
				tn.nodeLeases = watchedLeases{tn.nodeLeases, &decoded}
				tn.core = watchedCore{tn.core, &decoded}
			}
			f.before = func(req *http.Request) {
				runtime.GC()
				runtime.GC()
				for page, refs := range decoded.of(req.URL.Path) {
					for part, alive := range refs {
						if alive() {
							t.Errorf("%s: part %d of decoded page %d is reachable when page %d is requested", req.URL.Path, part, page+1, len(decoded.of(req.URL.Path))+1)
						}
					}
				}
			}
			got := listTenant(t, f, 5000)
			if got.err != nil || len(got.leases) != 2000 || len(got.s.nodes) != 500 || got.s.stopped() != "" {
				t.Fatalf("list read %d Leases and %d Nodes, stopped %q, error %v, want 2000, 500, none", len(got.leases), len(got.s.nodes), got.s.stopped(), got.err)
			}
			for path, want := range map[string]int{leasesPath: 4, nodesPath: 5} {
				if pages := len(decoded.of(path)); pages != want {
					t.Errorf("%s: %d decoded pages were watched, want %d", path, pages, want)
				}
			}
		})
	}
}
