// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

type countedListener struct {
	net.Listener
	open, dialed atomic.Int64
}

type countedConn struct {
	net.Conn
	listener *countedListener
	once     sync.Once
}

func (l *countedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.open.Add(1)
	l.dialed.Add(1)
	return &countedConn{Conn: conn, listener: l}, nil
}

func (c *countedConn) Close() error {
	c.once.Do(func() { c.listener.open.Add(-1) })
	return c.Conn.Close()
}

type tenantServer struct {
	kubeconfig []byte
	listener   *countedListener
}

func serveCounted(t *testing.T, p protocol, streams int, handler http.Handler) tenantServer {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	listener := &countedListener{Listener: server.Listener}
	server.Listener = listener
	if !p.http2 {
		server.Start()
		t.Cleanup(server.Close)
		return tenantServer{kubeconfigFor(t, server.URL), listener}
	}
	server.EnableHTTP2 = true
	if streams > 0 {
		server.Config.HTTP2 = &http.HTTP2Config{MaxConcurrentStreams: streams}
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	return tenantServer{kubeconfigForTLS(t, server), listener}
}

func silentHTTP2(c *tls.Conn) {
	defer func() { _ = c.Close() }()
	if _, err := io.CopyN(io.Discard, c, int64(len("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"))); err != nil {
		return
	}
	settings := make([]byte, 6)
	binary.BigEndian.PutUint16(settings, 0x3)
	binary.BigEndian.PutUint32(settings[2:], 1)
	if _, err := c.Write(http2Frame(http2Settings, 0, 0, settings)); err != nil {
		return
	}
	head := make([]byte, 9)
	for {
		if _, err := io.ReadFull(c, head); err != nil {
			return
		}
		if _, err := io.CopyN(io.Discard, c, int64(head[0])<<16|int64(head[1])<<8|int64(head[2])); err != nil {
			return
		}
		if head[3] == http2Settings && head[4]&http2Ack == 0 {
			if _, err := c.Write(http2Frame(http2Settings, http2Ack, 0, nil)); err != nil {
				return
			}
		}
	}
}

func serveSilently(t *testing.T, p protocol) tenantServer {
	t.Helper()
	if !p.http2 {
		return serveCounted(t, p, 0, http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
			_, _ = io.Copy(io.Discard, req.Body)
			<-req.Context().Done()
		}))
	}
	server := httptest.NewUnstartedServer(http.NotFoundHandler())
	listener := &countedListener{Listener: server.Listener}
	server.Listener = listener
	server.EnableHTTP2 = true
	server.Config.TLSNextProto = map[string]func(*http.Server, *tls.Conn, http.Handler){
		"h2": func(_ *http.Server, c *tls.Conn, _ http.Handler) { silentHTTP2(c) },
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	return tenantServer{kubeconfigForTLS(t, server), listener}
}

func answerHonestly(w http.ResponseWriter, req *http.Request) {
	switch {
	case req.URL.Path == nodesPath:
		respondWith(w, nodesPage)
	case req.URL.Path == nodeLeasesPath:
		respondWith(w, leasesPage)
	case strings.Contains(req.URL.Path, "/leases/"):
		respondWith(w, leaseBody)
	default:
		respondWith(w, `{"metadata":{},"items":[]}`)
	}
}

func connectionsKept(p protocol, reads int) int {
	if p.http2 {
		return 2*reads + 1
	}
	return 2 * reads
}

func connectionsOfHonestPasses(p protocol, reads int) int {
	if p.http2 {
		return reads + 1
	}
	return 2 * reads
}

func settles(open *atomic.Int64, most int) (int64, bool) {
	for range 1000 {
		if got := open.Load(); got <= int64(most) {
			return got, true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return open.Load(), false
}

var renewedAt = time.Unix(1700000000, 0)

type readsOf struct {
	name  string
	reads int
	pass  func(t *testing.T, kubeconfig []byte, timeout time.Duration) func()
}

func nodesTenant(t *testing.T, kubeconfig []byte, timeout time.Duration) (*NodesReconciler, *tenant) {
	t.Helper()
	r := &NodesReconciler{Timeout: timeout, MaxNodes: 5000}
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	tenant, err := r.tenant(key, kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.forget(key) })
	return r, tenant
}

var (
	readOfLease = readsOf{"the read of the controller manager's Lease", 1, func(t *testing.T, kubeconfig []byte, timeout time.Duration) func() {
		r, tenant := nodesTenant(t, kubeconfig, timeout)
		return func() { _, _ = r.readLease(context.Background(), tenant) }
	}}
	listsOfNodes = readsOf{"the two lists of health-nodes", 2, func(t *testing.T, kubeconfig []byte, timeout time.Duration) func() {
		r, tenant := nodesTenant(t, kubeconfig, timeout)
		return func() { _, _ = r.list(context.Background(), tenant, &sampling{}, renewedAt) }
	}}
	writeOfLease = readsOf{"the write of the hold", 1, func(t *testing.T, kubeconfig []byte, timeout time.Duration) func() {
		r, tenant := nodesTenant(t, kubeconfig, timeout)
		read := &leaderLease{}
		read.Metadata.ResourceVersion = "1"
		return func() { _, _ = r.writeLease(context.Background(), tenant, read, renewLease, renewedAt) }
	}}
	listsOfRestore = readsOf{"the seven lists of a held restore", 7, func(t *testing.T, kubeconfig []byte, timeout time.Duration) func() {
		r := &Reconciler{Timeout: timeout}
		key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
		tenant, err := r.tenant(key, kubeconfig)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.forget(key) })
		return func() { _ = r.actedOn(context.Background(), tenant) }
	}}
	cutReads = []readsOf{readOfLease, listsOfNodes, writeOfLease, listsOfRestore}
)

func TestPassesWhoseReadsADeadlineCutKeepAtMostConnectionsKeptWhateverTheNumberOfPasses(t *testing.T) {
	for _, p := range protocols {
		for _, c := range cutReads {
			t.Run(p.name+", "+c.name, func(t *testing.T) {
				server := serveSilently(t, p)
				pass := c.pass(t, server.kubeconfig, 100*time.Millisecond)
				most := int64(0)
				for i := range 20 {
					pass()
					got, ok := settles(&server.listener.open, connectionsKept(p, c.reads))
					most = max(most, got)
					if !ok {
						t.Fatalf("after %d passes whose %d reads the deadline cut, %d connections were open, want at most %d", i+1, c.reads, got, connectionsKept(p, c.reads))
					}
				}
				t.Logf("%d connections dialed in 20 passes, at most %d open after one", server.listener.dialed.Load(), most)
			})
		}
	}
}

func TestPassesAgainstAServerThatAllowsOneStreamKeepAtMostTheConnectionsOfHonestPasses(t *testing.T) {
	for _, p := range protocols {
		for _, c := range []readsOf{
			{"health-nodes, the Lease, its two lists and the renewal", 2, func(t *testing.T, kubeconfig []byte, timeout time.Duration) func() {
				r, tenant := nodesTenant(t, kubeconfig, timeout)
				return func() {
					read, err := r.readLease(context.Background(), tenant)
					if err != nil {
						t.Error(err)
					}
					if _, err := r.list(context.Background(), tenant, &sampling{}, renewedAt); err != nil {
						t.Error(err)
					}
					if _, err := r.writeLease(context.Background(), tenant, read, renewLease, renewedAt); err != nil {
						t.Error(err)
					}
				}
			}},
			listsOfRestore,
		} {
			t.Run(p.name+", "+c.name, func(t *testing.T) {
				server := serveCounted(t, p, 1, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					time.Sleep(time.Millisecond)
					answerHonestly(w, req)
				}))
				pass := c.pass(t, server.kubeconfig, time.Minute)
				most := int64(0)
				for i := range 40 {
					pass()
					got, ok := settles(&server.listener.open, connectionsOfHonestPasses(p, c.reads))
					most = max(most, got)
					if !ok {
						t.Fatalf("after %d passes of %d reads at once, %d connections were open, want at most %d", i+1, c.reads, got, connectionsOfHonestPasses(p, c.reads))
					}
				}
				t.Logf("%d connections dialed in 40 passes, at most %d open after one", server.listener.dialed.Load(), most)
			})
		}
	}
}

func TestPassesOfAnHonestTenantWithNoFailedReadDialNoConnectionAfterTheFirst(t *testing.T) {
	for _, p := range protocols {
		for _, c := range []readsOf{listsOfNodes, listsOfRestore} {
			t.Run(p.name+", "+c.name, func(t *testing.T) {
				server := serveCounted(t, p, 0, http.HandlerFunc(answerHonestly))
				pass := c.pass(t, server.kubeconfig, time.Minute)
				pass()
				dialed := server.listener.dialed.Load()
				for range 30 {
					pass()
				}
				if got := server.listener.dialed.Load(); got != dialed {
					t.Errorf("30 passes after the first dialed %d connections more", got-dialed)
				}
			})
		}
	}
}

func TestAReadAnsweredWithAStatusLeavesTheConnectionsOpen(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			var refuse atomic.Bool
			server := serveCounted(t, p, 0, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if refuse.Load() {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"no","reason":"InternalError","code":500}`)
					return
				}
				answerHonestly(w, req)
			}))
			r, tenant := nodesTenant(t, server.kubeconfig, time.Minute)
			if _, err := r.readLease(context.Background(), tenant); err != nil {
				t.Fatal(err)
			}
			dialed := server.listener.dialed.Load()
			refuse.Store(true)
			if _, err := r.readLease(context.Background(), tenant); err == nil {
				t.Fatal("a Status of Failure was read as the Lease")
			}
			refuse.Store(false)
			time.Sleep(50 * time.Millisecond)
			if _, err := r.readLease(context.Background(), tenant); err != nil {
				t.Fatal(err)
			}
			if got := server.listener.dialed.Load(); got != dialed {
				t.Errorf("a read answered with a Status and the one after it dialed %d connections more, want none", got-dialed)
			}
		})
	}
}

func TestAnAnswerThatFailsWhileItsBodyComesClosesTheIdleConnectionsOfTheClient(t *testing.T) {
	for _, c := range []struct {
		name  string
		body  func(w http.ResponseWriter)
		fails bool
	}{
		{"a stream reset in the body", func(w http.ResponseWriter) {
			_, _ = io.WriteString(w, `{"metadata":{},"items":[`)
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}, true},
		{"a body past maxResponseBytes", func(w http.ResponseWriter) {
			chunk := []byte(strings.Repeat(" ", 1<<20))
			for range maxResponseBytes/len(chunk) + 1 {
				if _, err := w.Write(chunk); err != nil {
					return
				}
			}
		}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			var fail atomic.Bool
			server := serveCounted(t, http2, 0, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == nodeLeasesPath && fail.Load() {
					w.Header().Set("Content-Type", "application/json")
					c.body(w)
					return
				}
				answerHonestly(w, req)
			}))
			r, tenant := nodesTenant(t, server.kubeconfig, time.Minute)
			if _, err := r.readLease(context.Background(), tenant); err != nil {
				t.Fatal(err)
			}
			if got := server.listener.open.Load(); got != 1 {
				t.Fatalf("%d connections were open after one read, want 1", got)
			}
			fail.Store(true)
			if _, _, err := r.listLeases(context.Background(), tenant); (err != nil) != c.fails {
				t.Fatalf("the list of Leases returned the error %v, want one: %t", err, c.fails)
			}
			if got, ok := settles(&server.listener.open, 0); !ok {
				t.Errorf("%d connections were open after a read that failed while its body came, want none: the client's idle connections are closed", got)
			}
		})
	}
}
