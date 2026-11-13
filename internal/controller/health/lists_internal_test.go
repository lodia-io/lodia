// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	leasesPath = "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases"
	nodesPath  = "/api/v1/nodes"
)

type listRequest struct {
	path  string
	query url.Values
}

type servedTenant struct {
	leases, nodes int
	oversize      map[string]int
	failing       map[string]int

	mu       sync.Mutex
	requests []listRequest
}

func (f *servedTenant) asked(path string) []listRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []listRequest
	for _, r := range f.requests {
		if r.path == path {
			out = append(out, r)
		}
	}
	return out
}

func (f *servedTenant) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	total, size := f.leases, leasePage
	if req.URL.Path == nodesPath {
		total, size = f.nodes, nodePage
	}
	f.mu.Lock()
	f.requests = append(f.requests, listRequest{req.URL.Path, req.URL.Query()})
	f.mu.Unlock()
	query := req.URL.Query()
	limit, _ := strconv.Atoi(query.Get("limit"))
	if limit != size {
		http.Error(w, "limit "+query.Get("limit"), http.StatusBadRequest)
		return
	}
	from, _ := strconv.Atoi(query.Get("continue"))
	to := min(from+limit, total)
	next := ""
	if to < total {
		next = strconv.Itoa(to)
	}
	page := from/limit + 1
	if f.failing[req.URL.Path] == page {
		http.Error(w, "injected", http.StatusInternalServerError)
		return
	}
	annotations := map[string]string(nil)
	if f.oversize[req.URL.Path] == page {
		annotations = map[string]string{"pad": strings.Repeat("x", maxResponseBytes)}
	}
	meta := func(i int) metav1.ObjectMeta {
		m := metav1.ObjectMeta{Name: fmt.Sprintf("n%05d", i), ResourceVersion: "1"}
		if i == from {
			m.Annotations = annotations
		}
		return m
	}
	list := metav1.ListMeta{ResourceVersion: "1", Continue: next}
	w.Header().Set("Content-Type", "application/json")
	if req.URL.Path == nodesPath {
		out := corev1.NodeList{TypeMeta: metav1.TypeMeta{Kind: "NodeList", APIVersion: "v1"}, ListMeta: list}
		for i := from; i < to; i++ {
			n := corev1.Node{ObjectMeta: meta(i)}
			n.Status.NodeInfo.KubeletVersion = "v1.35.0"
			n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
			out.Items = append(out.Items, n)
		}
		_ = json.NewEncoder(w).Encode(out)
		return
	}
	out := coordinationv1.LeaseList{TypeMeta: metav1.TypeMeta{Kind: "LeaseList", APIVersion: "coordination.k8s.io/v1"}, ListMeta: list}
	for i := from; i < to; i++ {
		out.Items = append(out.Items, coordinationv1.Lease{ObjectMeta: meta(i)})
	}
	_ = json.NewEncoder(w).Encode(out)
}

type listed struct {
	leases []leaseVersion
	err    error
	s      *sampling
	read   *counting
}

func listTenant(t *testing.T, f *servedTenant, maxNodes int) listed {
	t.Helper()
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	r := &NodesReconciler{Timeout: 30 * time.Second, MaxNodes: maxNodes}
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	tenant, err := r.tenant(key, kubeconfigFor(t, server.URL))
	if err != nil {
		t.Fatal(err)
	}
	read := &counting{next: r.clients[key].transport.Transport}
	r.clients[key].transport.Transport = read
	s := &sampling{}
	leases, err := r.list(t.Context(), tenant, s, time.Now())
	return listed{leases, err, s, read}
}

func TestBothListsPageToTheirEnd(t *testing.T) {
	f := &servedTenant{leases: 1201, nodes: 250}
	got := listTenant(t, f, 5000)
	if got.err != nil || len(got.leases) != 1201 || len(got.s.nodes) != 250 || got.s.stopped() != "" {
		t.Fatalf("list read %d Leases and %d Nodes, stopped %q, error %v, want 1201, 250, none", len(got.leases), len(got.s.nodes), got.s.stopped(), got.err)
	}
	for path, want := range map[string][]string{leasesPath: {"", "500", "1000"}, nodesPath: {"", "100", "200"}} {
		asked := f.asked(path)
		if len(asked) != len(want) {
			t.Fatalf("%s asked %d times, want %d", path, len(asked), len(want))
		}
		for i, r := range asked {
			limit := map[string]string{leasesPath: "500", nodesPath: "100"}[path]
			if r.query.Get("limit") != limit || r.query.Get("continue") != want[i] || r.query.Has("resourceVersion") || r.query.Has("resourceVersionMatch") {
				t.Errorf("%s page %d asked %v, want limit %s, continue %q and no resourceVersion", path, i+1, r.query, limit, want[i])
			}
		}
	}
}

func TestAListStopsOnceItHasReadMoreThanMaxNodes(t *testing.T) {
	for _, c := range []struct {
		name                    string
		leases, nodes, maxNodes int
		leaseAsks, nodeAsks     int
		kept                    [2]int
		stopped                 string
	}{
		{"Leases past the first page", 1201, 3, 300, 1, 1, [2]int{300, 3}, "more than 300 Leases of kube-node-lease (health.maxNodes)"},
		{"Leases past the second page", 1201, 3, 700, 2, 1, [2]int{700, 3}, "more than 700 Leases of kube-node-lease (health.maxNodes)"},
		{"one Lease more than a page that is the bound", 501, 3, 500, 2, 1, [2]int{500, 3}, "more than 500 Leases of kube-node-lease (health.maxNodes)"},
		{"exactly the bound in one page", 500, 3, 500, 1, 1, [2]int{500, 3}, ""},
		{"exactly the bound in two pages", 1000, 3, 1000, 2, 1, [2]int{1000, 3}, ""},
		{"Nodes past the second page", 3, 250, 120, 1, 2, [2]int{3, 120}, "more than 120 Nodes (health.maxNodes)"},
		{"one Node more than a page that is the bound", 3, 101, 100, 1, 2, [2]int{3, 100}, "more than 100 Nodes (health.maxNodes)"},
		{"exactly the bound in one page of Nodes", 3, 100, 100, 1, 1, [2]int{3, 100}, ""},
		{"both lists", 1201, 250, 120, 1, 2, [2]int{120, 120}, "more than 120 Leases of kube-node-lease (health.maxNodes)"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &servedTenant{leases: c.leases, nodes: c.nodes}
			got := listTenant(t, f, c.maxNodes)
			if got.err != nil {
				t.Fatal(got.err)
			}
			if len(got.leases) != c.kept[0] || len(got.s.nodes) != c.kept[1] {
				t.Errorf("list kept %d Leases and %d Nodes, want %d and %d", len(got.leases), len(got.s.nodes), c.kept[0], c.kept[1])
			}
			if len(got.leases) > c.maxNodes || len(got.s.nodes) > c.maxNodes {
				t.Errorf("list kept %d Leases and %d Nodes, more than maxNodes %d", len(got.leases), len(got.s.nodes), c.maxNodes)
			}
			if asked := len(f.asked(leasesPath)); asked != c.leaseAsks {
				t.Errorf("the Leases were asked for %d times, want %d", asked, c.leaseAsks)
			}
			if asked := len(f.asked(nodesPath)); asked != c.nodeAsks {
				t.Errorf("the Nodes were asked for %d times, want %d", asked, c.nodeAsks)
			}
			if got.s.stopped() != c.stopped {
				t.Errorf("stopped %q, want %q", got.s.stopped(), c.stopped)
			}
			if pages := int64(c.leaseAsks + c.nodeAsks); got.read.bytes.Load() > pages*maxResponseBytes {
				t.Errorf("the list read %d bytes in %d pages, more than %d each", got.read.bytes.Load(), pages, maxResponseBytes)
			}
		})
	}
}

func TestAResponseOverTheBoundStopsTheListAfterTheBytesOfThePagesItRead(t *testing.T) {
	const (
		leasesStop = "a response listing the Leases of kube-node-lease is longer than 8388608 bytes"
		nodesStop  = "a response listing the Nodes is longer than 8388608 bytes"
	)
	for _, c := range []struct {
		name                string
		oversize            map[string]int
		leaseAsks, nodeAsks int
		nodes               int
		stopped             string
	}{
		{"the first page of Leases", map[string]int{leasesPath: 1}, 1, 2, 150, leasesStop},
		{"the second page of Leases", map[string]int{leasesPath: 2}, 2, 2, 150, leasesStop},
		{"the first page of Nodes", map[string]int{nodesPath: 1}, 3, 1, 0, nodesStop},
		{"the second page of Nodes", map[string]int{nodesPath: 2}, 3, 2, 100, nodesStop},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &servedTenant{leases: 1201, nodes: 150, oversize: c.oversize}
			got := listTenant(t, f, 5000)
			if got.err != nil {
				t.Fatal(got.err)
			}
			if asked := len(f.asked(leasesPath)); asked != c.leaseAsks {
				t.Errorf("the Leases were asked for %d times, want %d", asked, c.leaseAsks)
			}
			if asked := len(f.asked(nodesPath)); asked != c.nodeAsks {
				t.Errorf("the Nodes were asked for %d times, want %d", asked, c.nodeAsks)
			}
			if len(got.s.nodes) != c.nodes {
				t.Errorf("the list kept %d Nodes, want %d", len(got.s.nodes), c.nodes)
			}
			if got.s.stopped() != c.stopped {
				t.Errorf("stopped %q, want %q", got.s.stopped(), c.stopped)
			}
			pages := int64(c.leaseAsks + c.nodeAsks)
			if read := got.read.bytes.Load(); read < maxResponseBytes || read > pages*maxResponseBytes {
				t.Errorf("the list read %d bytes in %d pages, want from one %d to %d", read, pages, maxResponseBytes, pages*maxResponseBytes)
			}
		})
	}
}

func TestAFailureOnALaterPageIsAFailedListAndNotAStop(t *testing.T) {
	for _, path := range []string{leasesPath, nodesPath} {
		t.Run(path, func(t *testing.T) {
			f := &servedTenant{leases: 1201, nodes: 250, failing: map[string]int{path: 2}}
			got := listTenant(t, f, 5000)
			if path == leasesPath && got.err == nil {
				t.Error("a list of the Leases whose second page failed returned no error")
			}
			if path == nodesPath && (got.err != nil || len(got.s.nodes) != 0 || !got.s.listedAt.IsZero()) {
				t.Errorf("a list of the Nodes whose second page failed returned %v and kept %d Nodes listed at %v, want the last good list left as it was", got.err, len(got.s.nodes), got.s.listedAt)
			}
			if got.s.stopped() != "" {
				t.Errorf("a failed page is a stop: %q", got.s.stopped())
			}
		})
	}
}
