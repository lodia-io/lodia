// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

const (
	leasesPage  = `{"metadata":{},"items":[{"metadata":{"name":"n","resourceVersion":"1"}}]}`
	nodesPage   = `{"metadata":{},"items":[{"metadata":{"name":"n"},"status":{"nodeInfo":{"kubeletVersion":"v1.35.0"},"conditions":[{"type":"Ready","status":"True"}]}}]}`
	leasesFirst = `{"metadata":{"continue":"next"},"items":[{"metadata":{"name":"m","resourceVersion":"1"}}]}`
	leaseBody   = `{"metadata":{"resourceVersion":"1"},"spec":{"holderIdentity":"lodia"}}`
)

type connections struct {
	mu       sync.Mutex
	inFlight map[string]int
	most     int
}

func (c *connections) enter(conn string) (leave func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inFlight[conn]++
	busy := 0
	for _, n := range c.inFlight {
		if n > 0 {
			busy++
		}
	}
	c.most = max(c.most, busy)
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.inFlight[conn]--
	}
}

func listsOverHTTP2(t *testing.T, streams int, handler func(w http.ResponseWriter, req *http.Request)) *connections {
	t.Helper()
	c := &connections{inFlight: map[string]int{}}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer c.enter(req.RemoteAddr)()
		handler(w, req)
	}))
	server.EnableHTTP2 = true
	if streams > 0 {
		server.Config.HTTP2 = &http.HTTP2Config{MaxConcurrentStreams: streams}
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
	tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigForTLS(t, server))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.readLease(t.Context(), tenant); err != nil {
		t.Fatal(err)
	}
	s := &sampling{}
	leases, err := r.list(t.Context(), tenant, s, time.Now())
	if err != nil || len(leases) == 0 || len(s.nodes) != 1 || s.leasesStop != "" || s.nodesStop != "" {
		t.Errorf("the lists returned %d Leases, %d Nodes, stops %q and %q, error %v", len(leases), len(s.nodes), s.leasesStop, s.nodesStop, err)
	}
	return c
}

func respondWith(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = io.WriteString(w, body)
}

func TestTheTwoListsOfAPassAreOnConnectionsOfTheirOwnOverHTTP2WhenTheServerAllowsOneStream(t *testing.T) {
	for _, c := range []struct {
		name    string
		streams int
		want    int
	}{
		{"a server with Go's default of streams", 0, 1},
		{"a server that allows one stream", 1, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			var (
				mu       sync.Mutex
				arrived  int
				together = make(chan struct{})
			)
			got := listsOverHTTP2(t, c.streams, func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path != nodesPath && req.URL.Path != nodeLeasesPath {
					respondWith(w, leaseBody)
					return
				}
				mu.Lock()
				if arrived++; arrived == 2 {
					close(together)
				}
				mu.Unlock()
				select {
				case <-together:
				case <-time.After(10 * time.Second):
					t.Errorf("the two lists did not arrive together")
				}
				if req.URL.Path == nodesPath {
					respondWith(w, nodesPage)
					return
				}
				respondWith(w, leasesPage)
			})
			if got.most != c.want {
				t.Errorf("the two lists of a pass were in flight on %d connections at once, want %d", got.most, c.want)
			}
		})
	}
}

func TestAGoawayAfterAPageOfLeasesPutsTheNextPageOnASecondConnectionWhileTheNodeListIsInFlightOnTheFirst(t *testing.T) {
	var (
		once         sync.Once
		secondLeases = make(chan struct{})
	)
	got := listsOverHTTP2(t, 0, func(w http.ResponseWriter, req *http.Request) {
		switch {
		case req.URL.Path == nodesPath:
			select {
			case <-secondLeases:
			case <-time.After(10 * time.Second):
				t.Errorf("the second page of Leases never came")
			}
			respondWith(w, nodesPage)
		case req.URL.Path != nodeLeasesPath:
			respondWith(w, leaseBody)
		case req.URL.Query().Get("continue") == "":
			w.Header().Set("Connection", "close")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			time.Sleep(100 * time.Millisecond)
			_, _ = io.WriteString(w, leasesFirst)
		default:
			once.Do(func() { close(secondLeases) })
			respondWith(w, leasesPage)
		}
	})
	if got.most != 2 {
		t.Errorf("a pass was in flight on %d connections at once, want 2: the Node list on the first and the second page of Leases on a second", got.most)
	}
}
