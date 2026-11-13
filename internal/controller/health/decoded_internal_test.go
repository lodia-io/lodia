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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestNoDecodedPageIsReachableWhenTheNextOneIsRequested(t *testing.T) {
	var decoded decodedPages
	f := &servedTenant{leases: 2000, nodes: 500}
	f.wrap = func(tn *tenant) {
		leases, nodes := tn.leasePage, tn.nodePage
		tn.leasePage = func(ctx context.Context, options metav1.ListOptions) (*leaseList, error) {
			page, err := leases(ctx, options)
			if err == nil {
				decoded.add(nodeLeasesPath, weakly(page), weakly(&page.Items.items[0]))
			}
			return page, err
		}
		tn.nodePage = func(ctx context.Context, options metav1.ListOptions) (*nodeList, error) {
			page, err := nodes(ctx, options)
			if err == nil {
				decoded.add(nodesPath, weakly(page), weakly(&page.Items.items[0]))
			}
			return page, err
		}
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
	for path, want := range map[string]int{nodeLeasesPath: 4, nodesPath: 5} {
		if pages := len(decoded.of(path)); pages != want {
			t.Errorf("%s: %d decoded pages were watched, want %d", path, pages, want)
		}
	}
}
