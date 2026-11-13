// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
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

type listRequest struct {
	path  string
	query url.Values
}

type servedTenant struct {
	leases, nodes int
	oversize      map[string]int
	failing       map[string]int
	empty         map[string]int
	overfull      map[string]int
	tokenLen      map[string]int
	conditions    map[int]int
	nameLen       int
	versionLen    int
	kubeletLen    int
	before        func(*http.Request)
	wrap          func(*tenant)

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
	if f.before != nil {
		f.before(req)
	}
	query := req.URL.Query()
	limit, _ := strconv.Atoi(query.Get("limit"))
	if limit != size {
		http.Error(w, "limit "+query.Get("limit"), http.StatusBadRequest)
		return
	}
	from, _ := strconv.Atoi(strings.TrimRight(query.Get("continue"), "x"))
	page := from/limit + 1
	to := min(from+limit, total)
	if f.overfull[req.URL.Path] == page {
		to = min(to+1, total)
	}
	next := ""
	if to < total {
		next = padded(strconv.Itoa(to), f.tokenLen[req.URL.Path])
	}
	if f.failing[req.URL.Path] == page {
		http.Error(w, "injected", http.StatusInternalServerError)
		return
	}
	if n, ok := f.empty[req.URL.Path]; ok {
		served, _ := strconv.Atoi(query.Get("continue"))
		continued := ""
		if served < n {
			continued = strconv.Itoa(served + 1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"resourceVersion":"1","continue":"` + continued + `"},"items":[]}`))
		return
	}
	annotations := map[string]string(nil)
	if f.oversize[req.URL.Path] == page {
		annotations = map[string]string{"pad": strings.Repeat("x", maxResponseBytes)}
	}
	meta := func(i int) metav1.ObjectMeta {
		m := metav1.ObjectMeta{Name: padded(fmt.Sprintf("n%05d", i), f.nameLen), ResourceVersion: padded("1", f.versionLen)}
		if i == from {
			m.Annotations = annotations
		}
		return m
	}
	list := metav1.ListMeta{ResourceVersion: "1", Continue: next}
	if req.URL.Path == nodesPath {
		out := corev1.NodeList{TypeMeta: metav1.TypeMeta{Kind: "NodeList", APIVersion: "v1"}, ListMeta: list}
		for i := from; i < to; i++ {
			n := corev1.Node{ObjectMeta: meta(i)}
			n.Status.NodeInfo.KubeletVersion = padded("v1.35.0", f.kubeletLen)
			n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
			for c := 1; c < f.conditions[i]; c++ {
				n.Status.Conditions = append(n.Status.Conditions, corev1.NodeCondition{Type: corev1.NodeConditionType("C" + strconv.Itoa(c)), Status: corev1.ConditionFalse})
			}
			out.Items = append(out.Items, n)
		}
		f.write(w, &out)
		return
	}
	out := coordinationv1.LeaseList{TypeMeta: metav1.TypeMeta{Kind: "LeaseList", APIVersion: "coordination.k8s.io/v1"}, ListMeta: list}
	for i := from; i < to; i++ {
		out.Items = append(out.Items, coordinationv1.Lease{ObjectMeta: meta(i)})
	}
	f.write(w, &out)
}

func (f *servedTenant) write(w http.ResponseWriter, out any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func padded(s string, length int) string {
	if len(s) >= length {
		return s
	}
	return s + strings.Repeat("x", length-len(s))
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
	if f.wrap != nil {
		f.wrap(tenant)
	}
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
	for path, want := range map[string][]string{nodeLeasesPath: {"", "500", "1000"}, nodesPath: {"", "100", "200"}} {
		asked := f.asked(path)
		if len(asked) != len(want) {
			t.Fatalf("%s asked %d times, want %d", path, len(asked), len(want))
		}
		for i, r := range asked {
			limit := map[string]string{nodeLeasesPath: "500", nodesPath: "100"}[path]
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
			if asked := len(f.asked(nodeLeasesPath)); asked != c.leaseAsks {
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
	sizedForTheBounds(t)
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
		{"the first page of Leases", map[string]int{nodeLeasesPath: 1}, 1, 2, 150, leasesStop},
		{"the second page of Leases", map[string]int{nodeLeasesPath: 2}, 2, 2, 150, leasesStop},
		{"the first page of Nodes", map[string]int{nodesPath: 1}, 3, 1, 0, nodesStop},
		{"the second page of Nodes", map[string]int{nodesPath: 2}, 3, 2, 100, nodesStop},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &servedTenant{leases: 1201, nodes: 150, oversize: c.oversize}
			got := listTenant(t, f, 5000)
			if got.err != nil {
				t.Fatal(got.err)
			}
			if asked := len(f.asked(nodeLeasesPath)); asked != c.leaseAsks {
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

func TestAPageOfMoreItemsThanItsLimitStopsItsListAtTheItemsItHolds(t *testing.T) {
	for _, c := range []struct {
		name      string
		f         *servedTenant
		leases    int
		nodes     int
		stopped   string
		leaseAsks int
		nodeAsks  int
	}{
		{name: "one Lease more", f: &servedTenant{leases: 1200, nodes: 3, overfull: map[string]int{nodeLeasesPath: 1}}, leases: 500, nodes: 3, stopped: "a page of the Leases of kube-node-lease holds more than 500 items", leaseAsks: 1, nodeAsks: 1},
		{name: "one Lease more on the second page", f: &servedTenant{leases: 1200, nodes: 3, overfull: map[string]int{nodeLeasesPath: 2}}, leases: 1000, nodes: 3, stopped: "a page of the Leases of kube-node-lease holds more than 500 items", leaseAsks: 2, nodeAsks: 1},
		{name: "one Node more", f: &servedTenant{leases: 3, nodes: 250, overfull: map[string]int{nodesPath: 1}}, leases: 3, nodes: 100, stopped: "a page of the Nodes holds more than 100 items", leaseAsks: 1, nodeAsks: 1},
		{name: "a page that holds its limit", f: &servedTenant{leases: 1000, nodes: 200}, leases: 1000, nodes: 200, leaseAsks: 2, nodeAsks: 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := listTenant(t, c.f, 5000)
			if got.err != nil {
				t.Fatal(got.err)
			}
			if len(got.leases) != c.leases || len(got.s.nodes) != c.nodes {
				t.Errorf("list kept %d Leases and %d Nodes, want %d and %d", len(got.leases), len(got.s.nodes), c.leases, c.nodes)
			}
			if got.s.stopped() != c.stopped {
				t.Errorf("stopped %q, want %q", got.s.stopped(), c.stopped)
			}
			if asked := len(c.f.asked(nodeLeasesPath)); asked != c.leaseAsks {
				t.Errorf("the Leases were asked for %d times, want %d", asked, c.leaseAsks)
			}
			if asked := len(c.f.asked(nodesPath)); asked != c.nodeAsks {
				t.Errorf("the Nodes were asked for %d times, want %d", asked, c.nodeAsks)
			}
		})
	}
}

func TestANodeWithMoreConditionsThanTheBoundStopsTheListBeforeIt(t *testing.T) {
	sizedForTheBounds(t)
	for _, c := range []struct {
		name       string
		conditions int
		kept       int
		stopped    string
	}{
		{"64 conditions", maxConditions, 3, ""},
		{"65 conditions", maxConditions + 1, 1, "a Node has more than 64 conditions"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &servedTenant{leases: 3, nodes: 3, conditions: map[int]int{1: c.conditions}}
			got := listTenant(t, f, 5000)
			if got.err != nil {
				t.Fatal(got.err)
			}
			if len(got.s.nodes) != c.kept || got.s.stopped() != c.stopped {
				t.Errorf("list kept %d Nodes and stopped %q, want %d and %q", len(got.s.nodes), got.s.stopped(), c.kept, c.stopped)
			}
			if len(got.s.nodes) > 0 && got.s.nodes[0].ready != corev1.ConditionTrue {
				t.Errorf("the Node before the one with too many conditions is %+v", got.s.nodes[0])
			}
		})
	}
}

func TestAFailureOnALaterPageIsAFailedListAndNotAStop(t *testing.T) {
	for _, path := range []string{nodeLeasesPath, nodesPath} {
		t.Run(path, func(t *testing.T) {
			f := &servedTenant{leases: 1201, nodes: 250, failing: map[string]int{path: 2}}
			got := listTenant(t, f, 5000)
			if path == nodeLeasesPath && got.err == nil {
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

func TestAContinueTokenOverItsBoundEndsTheListWithoutBeingSentBack(t *testing.T) {
	sizedForTheBounds(t)
	const (
		leasesStop = "metadata.continue in the Leases of kube-node-lease is longer than 4096 bytes"
		nodesStop  = "metadata.continue in the Nodes is longer than 4096 bytes"
	)
	for _, c := range []struct {
		name                string
		path                string
		tokenLen            int
		leaseAsks, nodeAsks int
		leases, nodes       int
		stopped             string
		sent                int
	}{
		{"Leases with a token of 4,096 bytes", nodeLeasesPath, 4096, 3, 1, 1201, 3, "", 4096},
		{"Leases with a token of 4,097 bytes", nodeLeasesPath, 4097, 1, 1, 0, 3, leasesStop, 0},
		{"Leases with a token of 4 MiB", nodeLeasesPath, maxResponseBytes / 2, 1, 1, 0, 3, leasesStop, 0},
		{"Nodes with a token of 4,096 bytes", nodesPath, 4096, 1, 3, 3, 250, "", 4096},
		{"Nodes with a token of 4,097 bytes", nodesPath, 4097, 1, 1, 3, 0, nodesStop, 0},
		{"Nodes with a token of 4 MiB", nodesPath, maxResponseBytes / 2, 1, 1, 3, 0, nodesStop, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &servedTenant{leases: 1201, nodes: 250, tokenLen: map[string]int{c.path: c.tokenLen}}
			if c.path == nodeLeasesPath {
				f.nodes = 3
			} else {
				f.leases = 3
			}
			got := listTenant(t, f, 5000)
			if got.err != nil {
				t.Fatal(got.err)
			}
			if asked := len(f.asked(nodeLeasesPath)); asked != c.leaseAsks {
				t.Errorf("the Leases were asked for %d times, want %d", asked, c.leaseAsks)
			}
			if asked := len(f.asked(nodesPath)); asked != c.nodeAsks {
				t.Errorf("the Nodes were asked for %d times, want %d", asked, c.nodeAsks)
			}
			if len(got.leases) != c.leases || len(got.s.nodes) != c.nodes {
				t.Errorf("list kept %d Leases and %d Nodes, want %d and %d", len(got.leases), len(got.s.nodes), c.leases, c.nodes)
			}
			if got.s.stopped() != c.stopped {
				t.Errorf("stopped %q, want %q", got.s.stopped(), c.stopped)
			}
			for _, r := range f.asked(c.path)[1:] {
				if sent := len(r.query.Get("continue")); sent != c.sent {
					t.Errorf("a later page of %s was asked for with a token of %d bytes, want %d", c.path, sent, c.sent)
				}
			}
		})
	}
}

func TestAListOfPagesWithNoItemsStopsAtItsBoundOfPagesAndNotBefore(t *testing.T) {
	for _, c := range []struct {
		name, path string
		maxNodes   int
		tokens     int
		pages      int
		stopped    string
	}{
		{"Leases that never end", nodeLeasesPath, 5000, 1 << 20, 11, "more than 11 pages of Leases of kube-node-lease (health.maxNodes)"},
		{"Nodes that never end", nodesPath, 5000, 1 << 20, 51, "more than 51 pages of Nodes (health.maxNodes)"},
		{"Leases that end on the page of the bound", nodeLeasesPath, 5000, 10, 11, ""},
		{"Leases with a token on the page of the bound", nodeLeasesPath, 5000, 11, 11, "more than 11 pages of Leases of kube-node-lease (health.maxNodes)"},
		{"Nodes that end on the page of the bound", nodesPath, 5000, 50, 51, ""},
		{"Nodes with a token on the page of the bound", nodesPath, 5000, 51, 51, "more than 51 pages of Nodes (health.maxNodes)"},
		{"Leases at a bound under a page", nodeLeasesPath, 3, 1 << 20, 2, "more than 2 pages of Leases of kube-node-lease (health.maxNodes)"},
		{"Leases at a bound that is a page", nodeLeasesPath, 500, 1 << 20, 2, "more than 2 pages of Leases of kube-node-lease (health.maxNodes)"},
		{"Leases at a bound a page over", nodeLeasesPath, 501, 1 << 20, 3, "more than 3 pages of Leases of kube-node-lease (health.maxNodes)"},
		{"Nodes at a bound that is a page", nodesPath, 100, 1 << 20, 2, "more than 2 pages of Nodes (health.maxNodes)"},
		{"Nodes at a bound a page over", nodesPath, 101, 1 << 20, 3, "more than 3 pages of Nodes (health.maxNodes)"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &servedTenant{leases: 3, nodes: 3, empty: map[string]int{c.path: c.tokens}}
			got := listTenant(t, f, c.maxNodes)
			if got.err != nil {
				t.Fatal(got.err)
			}
			if got.s.stopped() != c.stopped {
				t.Errorf("stopped %q, want %q", got.s.stopped(), c.stopped)
			}
			if asked := len(f.asked(c.path)); asked != c.pages {
				t.Errorf("the pages of %s were asked for %d times, want %d", c.path, asked, c.pages)
			}
		})
	}
}

func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func TestWhatASamplingKeepsRestsOnItsOwnBoundsAndNotOnWhatTheServerAnswers(t *testing.T) {
	sizedForTheBounds(t)
	const maxNodes = 5000
	for _, c := range []struct {
		name                            string
		nameLen, versionLen, kubeletLen int
		kept                            int
		retainedUnder                   int64
	}{
		{"names at the raw bound", maxNameRaw, maxVersionRaw, maxKubeletRaw, 0, 1 << 20},
		{"names of 254 bytes", maxName + 1, 1, 1, 0, 1 << 20},
		{"names of 253 bytes, versions at the raw bound and kubelet versions of 65 to 384 bytes", maxName, maxVersionRaw, maxKubeletRaw, maxNodes, 5 << 20},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &servedTenant{leases: maxNodes, nodes: maxNodes, nameLen: c.nameLen, versionLen: c.versionLen, kubeletLen: c.kubeletLen}
			before := liveHeap()
			got := listTenant(t, f, maxNodes)
			if got.err != nil || got.s.stopped() != "" {
				t.Fatalf("list returned %v, stopped %q, want a list of 5,000 each that ended", got.err, got.s.stopped())
			}
			got.s.observe(got.leases, time.Now())
			got.leases = nil
			retained := int64(liveHeap()) - int64(before)
			t.Logf("the sampling keeps %d Lease versions and %d Node summaries, %d bytes of heap", len(got.s.leases), len(got.s.nodes), retained)
			if len(got.s.leases) != c.kept || len(got.s.nodes) != c.kept {
				t.Errorf("the sampling keeps %d Lease versions and %d Node summaries, want %d each", len(got.s.leases), len(got.s.nodes), c.kept)
			}
			for name := range got.s.leases {
				if len(name) > maxName {
					t.Fatalf("the sampling keeps a Lease named with %d bytes", len(name))
				}
			}
			for _, n := range got.s.nodes {
				if len(n.name) > maxName || len(n.kubelet) > maxKubeletVersion {
					t.Fatalf("the sampling keeps a Node named with %d bytes with a kubelet version of %d", len(n.name), len(n.kubelet))
				}
			}
			if retained > c.retainedUnder {
				t.Errorf("the sampling holds %d bytes of heap, more than %d", retained, c.retainedUnder)
			}
			runtime.KeepAlive(got)
		})
	}
}
