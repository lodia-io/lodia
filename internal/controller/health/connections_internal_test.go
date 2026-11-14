// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"crypto/tls"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2/hpack"
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

const http2GoAway = 7

type rawRequest struct {
	conn   int
	stream uint32
	path   string
}

type rawConn struct {
	c    *tls.Conn
	mu   sync.Mutex
	last uint32
}

func (c *rawConn) write(frames ...[]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, frame := range frames {
		if _, err := c.c.Write(frame); err != nil {
			return
		}
	}
}

func (c *rawConn) goAway() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	payload := make([]byte, 8)
	binary.BigEndian.PutUint32(payload, c.last)
	return http2Frame(http2GoAway, 0, 0, payload)
}

func answerHead(stream uint32) []byte {
	head := append([]byte{hpackStatus200}, hpackLiteral("content-type", "application/json")...)
	return http2Frame(http2Headers, http2EndHeaders, stream, head)
}

func answerBody(stream uint32, body string) []byte {
	return http2Frame(http2Data, http2EndStream, stream, []byte(body))
}

func serveRaw(c *tls.Conn, id int, handle func(conn *rawConn, req rawRequest)) {
	defer func() { _ = c.Close() }()
	if _, err := io.CopyN(io.Discard, c, int64(len("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"))); err != nil {
		return
	}
	conn := &rawConn{c: c}
	conn.write(http2Frame(http2Settings, 0, 0, nil))
	decoder := hpack.NewDecoder(4096, nil)
	head := make([]byte, 9)
	for {
		if _, err := io.ReadFull(c, head); err != nil {
			return
		}
		payload := make([]byte, int(head[0])<<16|int(head[1])<<8|int(head[2]))
		if _, err := io.ReadFull(c, payload); err != nil {
			return
		}
		switch head[3] {
		case http2Settings:
			if head[4]&http2Ack == 0 {
				conn.write(http2Frame(http2Settings, http2Ack, 0, nil))
			}
		case http2Headers:
			fields, err := decoder.DecodeFull(payload)
			if err != nil {
				return
			}
			req := rawRequest{conn: id, stream: binary.BigEndian.Uint32(head[5:]) & (1<<31 - 1)}
			for _, field := range fields {
				if field.Name == ":path" {
					req.path = field.Value
				}
			}
			conn.mu.Lock()
			conn.last = max(conn.last, req.stream)
			conn.mu.Unlock()
			go handle(conn, req)
		}
	}
}

func TestAGoawayAfterAPageOfLeasesPutsTheNextPageOnASecondConnectionWhileTheNodeListIsInFlightOnTheFirst(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []rawRequest
		dialed   atomic.Int32
		nodeIn   = make(chan struct{})
		secondIn = make(chan struct{})
		answered atomic.Bool
		nodeOnce sync.Once
		pageOnce sync.Once
	)
	waitFor := func(arrived chan struct{}, what string) {
		select {
		case <-arrived:
		case <-time.After(10 * time.Second):
			t.Errorf("%s never came", what)
		}
	}
	handle := func(conn *rawConn, req rawRequest) {
		mu.Lock()
		requests = append(requests, req)
		mu.Unlock()
		switch {
		case strings.HasPrefix(req.path, nodesPath):
			nodeOnce.Do(func() { close(nodeIn) })
			waitFor(secondIn, "the second page of Leases")
			answered.Store(true)
			conn.write(answerHead(req.stream), answerBody(req.stream, nodesPage))
		case strings.HasPrefix(req.path, nodeLeasesPath) && !strings.Contains(req.path, "continue="):
			waitFor(nodeIn, "the Node list")
			conn.write(answerHead(req.stream), conn.goAway(), answerBody(req.stream, leasesFirst))
		case strings.HasPrefix(req.path, nodeLeasesPath):
			if answered.Load() {
				t.Errorf("the Node list was answered before the second page of Leases came")
			}
			pageOnce.Do(func() { close(secondIn) })
			conn.write(answerHead(req.stream), answerBody(req.stream, leasesPage))
		default:
			conn.write(answerHead(req.stream), answerBody(req.stream, leaseBody))
		}
	}
	server := httptest.NewUnstartedServer(http.NotFoundHandler())
	server.EnableHTTP2 = true
	server.Config.TLSNextProto = map[string]func(*http.Server, *tls.Conn, http.Handler){
		"h2": func(_ *http.Server, c *tls.Conn, _ http.Handler) { serveRaw(c, int(dialed.Add(1)), handle) },
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	tenant, err := r.tenant(key, kubeconfigForTLS(t, server))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.forget(key) })
	if _, err := r.readLease(t.Context(), tenant); err != nil {
		t.Fatal(err)
	}
	s := &sampling{}
	leases, err := r.list(t.Context(), tenant, s, time.Now())
	if err != nil || len(leases) != 2 || len(s.nodes) != 1 || s.leasesStop != "" || s.nodesStop != "" {
		t.Errorf("the lists returned %d Leases, %d Nodes, stops %q and %q, error %v, want 2 and 1", len(leases), len(s.nodes), s.leasesStop, s.nodesStop, err)
	}
	mu.Lock()
	defer mu.Unlock()
	var first, nodes, second *rawRequest
	for i, req := range requests {
		t.Logf("connection %d, stream %d: %s", req.conn, req.stream, req.path)
		switch {
		case strings.HasPrefix(req.path, nodesPath):
			nodes = &requests[i]
		case strings.HasPrefix(req.path, nodeLeasesPath) && !strings.Contains(req.path, "continue="):
			first = &requests[i]
		case strings.HasPrefix(req.path, nodeLeasesPath):
			second = &requests[i]
		}
	}
	if first == nil || nodes == nil || second == nil {
		t.Fatalf("the server saw %d requests, want the first page of Leases, the Node list and the second page of Leases among them", len(requests))
	}
	if first.conn != nodes.conn || second.conn == nodes.conn {
		t.Errorf("the first page of Leases was on connection %d, the Node list on %d and the second page on %d, want the first two on one and the second page on another", first.conn, nodes.conn, second.conn)
	}
}
