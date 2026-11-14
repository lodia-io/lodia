// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

const (
	leasesPage = `{"metadata":{},"items":[{"metadata":{"name":"n","resourceVersion":"1"}}]}`
	nodesPage  = `{"metadata":{},"items":[{"metadata":{"name":"n"},"status":{"nodeInfo":{"kubeletVersion":"v1.35.0"},"conditions":[{"type":"Ready","status":"True"}]}}]}`
	leaseBody  = `{"metadata":{"resourceVersion":"1"},"spec":{"holderIdentity":"lodia"}}`
	emptyPage  = `{"metadata":{},"items":[]}`
)

func respondWith(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = io.WriteString(w, body)
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
		respondWith(w, emptyPage)
	}
}

type countedListener struct {
	net.Listener
	open, peak atomic.Int64
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
	open := l.open.Add(1)
	for {
		peak := l.peak.Load()
		if open <= peak || l.peak.CompareAndSwap(peak, open) {
			break
		}
	}
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

func serveCounted(t *testing.T, handler http.Handler) tenantServer {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	listener := &countedListener{Listener: server.Listener}
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return tenantServer{kubeconfigFor(t, server.URL), listener}
}

func serveSilently(t *testing.T) tenantServer {
	t.Helper()
	return serveCounted(t, http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		<-req.Context().Done()
	}))
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

type passes struct {
	name  string
	reads int
	start func(t *testing.T, kubeconfig []byte, timeout time.Duration) (*tenant, func(context.Context) error)
}

func nodesPasses(t *testing.T, kubeconfig []byte, timeout time.Duration) (*tenant, func(context.Context) error) {
	t.Helper()
	r := &NodesReconciler{Timeout: timeout, MaxNodes: 5000}
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	tenant, err := r.tenant(key, kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.forget(key) })
	return tenant, func(ctx context.Context) error {
		read, err := r.readLease(ctx, tenant)
		if err != nil {
			return err
		}
		if _, err := r.list(ctx, tenant, &sampling{}, renewedAt); err != nil {
			return err
		}
		_, err = r.writeLease(ctx, tenant, read, renewLease, renewedAt)
		return err
	}
}

func restorePasses(t *testing.T, kubeconfig []byte, timeout time.Duration) (*tenant, func(context.Context) error) {
	t.Helper()
	r := &Reconciler{Timeout: timeout}
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	tenant, err := r.tenant(key, kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.forget(key) })
	return tenant, func(ctx context.Context) error {
		if message := r.actedOn(ctx, tenant); strings.Contains(message, "could not be listed") {
			return errors.New(message)
		}
		return nil
	}
}

var (
	healthNodes = passes{"health-nodes", nodesReads, nodesPasses}
	health      = passes{"health", restoreReads, restorePasses}
	clients     = []passes{healthNodes, health}
)

func TestTheTenantClientsSpeakHTTP1EvenWhenTheServerOffersHTTP2First(t *testing.T) {
	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			var seen atomic.Int64
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				seen.Add(1)
				negotiated := ""
				if req.TLS != nil {
					negotiated = req.TLS.NegotiatedProtocol
				}
				if req.ProtoMajor != 1 || negotiated == "h2" {
					t.Errorf("the request came over HTTP/%d, ALPN %q, want HTTP/1.1", req.ProtoMajor, negotiated)
				}
				answerHonestly(w, req)
			}))
			server.EnableHTTP2 = true
			server.StartTLS()
			t.Cleanup(server.Close)
			if !strings.Contains(strings.Join(server.TLS.NextProtos, ","), "h2") {
				t.Fatalf("the server offers %v, want h2 among them", server.TLS.NextProtos)
			}
			_, pass := c.start(t, kubeconfigForTLS(t, server), time.Minute)
			if err := pass(t.Context()); err != nil {
				t.Fatal(err)
			}
			if seen.Load() < int64(c.reads) {
				t.Errorf("the server saw %d requests, want at least %d", seen.Load(), c.reads)
			}
		})
	}
}

func kubeconfigWithToken(t *testing.T, address, token string) []byte {
	t.Helper()
	out, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"tenant": {Server: address}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": {Token: token}},
		Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
		CurrentContext: "tenant",
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestTheReadPastTheCapWaitsForAConnectionToBeGivenBackAndFailsAtItsDeadlineWhenNoneIs(t *testing.T) {
	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			var arrived, extra atomic.Int64
			release := make(chan struct{}, c.reads)
			server := serveCounted(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if strings.HasSuffix(req.URL.Path, "/extra") {
					extra.Add(1)
				} else {
					arrived.Add(1)
					select {
					case <-release:
					case <-req.Context().Done():
					}
				}
				respondWith(w, leaseBody)
			}))
			tenant, _ := c.start(t, server.kubeconfig, time.Minute)
			var held sync.WaitGroup
			for range c.reads {
				held.Go(func() {
					if _, err := tenant.getLease(t.Context(), "held"); err != nil {
						t.Error(err)
					}
				})
			}
			until := func(what string, done func() bool) {
				t.Helper()
				for range 1000 {
					if done() {
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
				t.Fatalf("%s did not happen", what)
			}
			until("every read the cap allows reaching the server", func() bool { return arrived.Load() == int64(c.reads) })

			deadline, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			begun := time.Now()
			_, err := tenant.getLease(deadline, "extra")
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(begun) > 3*time.Second {
				t.Errorf("the read past the cap returned %v after %v, want its deadline exceeded after 300 ms", err, time.Since(begun))
			}
			if extra.Load() != 0 || server.listener.open.Load() != int64(c.reads) {
				t.Errorf("the read past the cap reached the server %d times and the server has %d connections open, want 0 and %d", extra.Load(), server.listener.open.Load(), c.reads)
			}

			var reused atomic.Bool
			trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused.Store(info.Reused) }}
			waiting := make(chan error, 1)
			go func() {
				_, err := tenant.getLease(httptrace.WithClientTrace(t.Context(), trace), "extra")
				waiting <- err
			}()
			time.Sleep(300 * time.Millisecond)
			if extra.Load() != 0 {
				t.Errorf("a read past the cap reached the server while the %d connections were all in use", c.reads)
			}
			release <- struct{}{}
			if err := <-waiting; err != nil {
				t.Errorf("the read past the cap returned %v once a connection was given back", err)
			}
			if !reused.Load() || extra.Load() != 1 {
				t.Errorf("the read past the cap reached the server %d times, on a reused connection: %t, want once, on the connection given back", extra.Load(), reused.Load())
			}
			for range c.reads - 1 {
				release <- struct{}{}
			}
			held.Wait()
			if peak := server.listener.peak.Load(); peak > int64(c.reads) {
				t.Errorf("%d connections were open at once, want at most %d", peak, c.reads)
			}
		})
	}
}

func TestAServerThatClosesAfterEveryAnswerLeavesAtMostTheCapOpenAfterEachPass(t *testing.T) {
	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			server := serveCounted(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Connection", "close")
				answerHonestly(w, req)
			}))
			_, pass := c.start(t, server.kubeconfig, time.Minute)
			for i := range 40 {
				if err := pass(t.Context()); err != nil {
					t.Fatalf("pass %d: %v", i+1, err)
				}
				if got, ok := settles(&server.listener.open, c.reads); !ok {
					t.Fatalf("after %d passes, %d connections were open, want at most %d", i+1, got, c.reads)
				}
			}
		})
	}
}

func TestAServerThatKeepsItsConnectionsOpenAndNeverAnswersLeavesAtMostTheCapOpenAfterEachPass(t *testing.T) {
	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			server := serveSilently(t)
			_, pass := c.start(t, server.kubeconfig, 100*time.Millisecond)
			for i := range 40 {
				_ = pass(t.Context())
				if got, ok := settles(&server.listener.open, c.reads); !ok {
					t.Fatalf("after %d passes whose reads the deadline cut, %d connections were open, want at most %d", i+1, got, c.reads)
				}
			}
		})
	}
}

type reusedOrNot struct {
	mu  sync.Mutex
	got []bool
}

func (r *reusedOrNot) trace(ctx context.Context) context.Context {
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.got = append(r.got, info.Reused)
	}})
}

func (r *reusedOrNot) counts() (reused, dialed int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, got := range r.got {
		if got {
			reused++
		} else {
			dialed++
		}
	}
	return reused, dialed
}

func TestThePagesOfAListTakeTheConnectionsOfThePagesBeforeThem(t *testing.T) {
	pageOf := func(w http.ResponseWriter, req *http.Request, item string) {
		next := map[string]string{"": "t1", "t1": "t2"}[req.URL.Query().Get("continue")]
		respondWith(w, `{"metadata":{"continue":"`+next+`"},"items":[`+item+`]}`)
	}
	var listed atomic.Int64
	bothIn := make(chan struct{})
	server := serveCounted(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if (req.URL.Path == nodesPath || req.URL.Path == nodeLeasesPath) && req.URL.Query().Get("continue") == "" {
			if listed.Add(1) == nodesReads {
				close(bothIn)
			}
			<-bothIn
		}
		switch req.URL.Path {
		case nodesPath:
			pageOf(w, req, `{"metadata":{"name":"n"},"status":{"nodeInfo":{"kubeletVersion":"v1.35.0"},"conditions":[{"type":"Ready","status":"True"}]}}`)
		case nodeLeasesPath:
			pageOf(w, req, `{"metadata":{"name":"n","resourceVersion":"1"}}`)
		default:
			answerHonestly(w, req)
		}
	}))
	r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
	tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, server.kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	var seen reusedOrNot
	s := &sampling{}
	leases, err := r.list(seen.trace(t.Context()), tenant, s, renewedAt)
	if err != nil || len(leases) != 3 || len(s.nodes) != 3 {
		t.Fatalf("the lists returned %d Leases and %d Nodes, error %v, want 3 of each", len(leases), len(s.nodes), err)
	}
	if reused, dialed := seen.counts(); reused != 4 || dialed != 2 {
		t.Errorf("the six pages of the two lists reused a connection %d times and dialed %d, want 4 and 2: each list dials for its first page, and each next page takes the connection the page before gave back", reused, dialed)
	}

	var arrived atomic.Int64
	together := make(chan struct{})
	restore := restoreTenantOf(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if arrived.Add(1) == restoreReads {
			close(together)
		}
		if arrived.Load() <= restoreReads {
			<-together
		}
		answerHonestly(w, req)
	}))
	var first, second reusedOrNot
	reader := &Reconciler{Timeout: time.Minute}
	_ = reader.actedOn(first.trace(t.Context()), restore)
	_ = reader.actedOn(second.trace(t.Context()), restore)
	if reused, dialed := first.counts(); reused != 0 || dialed != restoreReads {
		t.Errorf("the first pass over a held restore reused %d connections and dialed %d, want 0 and %d", reused, dialed, restoreReads)
	}
	if reused, dialed := second.counts(); reused != restoreReads || dialed != 0 {
		t.Errorf("the second pass over a held restore reused %d connections and dialed %d, want %d and 0", reused, dialed, restoreReads)
	}
}

func transportOf(t *testing.T, tenant *tenant) *http.Transport {
	t.Helper()
	bottom := tenant.api.Client.Transport
	for {
		wrapped, ok := bottom.(utilnet.RoundTripperWrapper)
		if !ok {
			break
		}
		bottom = wrapped.WrappedRoundTripper()
	}
	transport, ok := bottom.(*http.Transport)
	if !ok {
		t.Fatalf("the transport below the tenant's client is a %T, want an *http.Transport", bottom)
	}
	return transport
}

type socketCount struct {
	open, peak atomic.Int64
}

type countedSocket struct {
	net.Conn
	count *socketCount
	once  sync.Once
}

func (c *countedSocket) Close() error {
	c.once.Do(func() { c.count.open.Add(-1) })
	return c.Conn.Close()
}

func (c *socketCount) watch(transport *http.Transport) {
	dial := transport.DialContext
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		open := c.open.Add(1)
		for {
			peak := c.peak.Load()
			if open <= peak || c.peak.CompareAndSwap(peak, open) {
				break
			}
		}
		conn, err := dial(ctx, network, address)
		if err != nil {
			c.open.Add(-1)
			return nil, err
		}
		return &countedSocket{Conn: conn, count: c}, nil
	}
}

func TestAPassNeedsNoMoreConnectionsThanItsCapWhenTheCapIsLifted(t *testing.T) {
	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			var inFlight, mostInFlight atomic.Int64
			server := serveCounted(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				now := inFlight.Add(1)
				for {
					most := mostInFlight.Load()
					if now <= most || mostInFlight.CompareAndSwap(most, now) {
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
				inFlight.Add(-1)
				answerHonestly(w, req)
			}))
			tenant, pass := c.start(t, server.kubeconfig, time.Minute)
			transport := transportOf(t, tenant)
			if transport.MaxConnsPerHost != c.reads {
				t.Errorf("the transport caps its connections at %d, want the %d reads of a pass of %s", transport.MaxConnsPerHost, c.reads, c.name)
			}
			transport.MaxConnsPerHost = 0
			var sockets socketCount
			sockets.watch(transport)
			for i := range 40 {
				if err := pass(t.Context()); err != nil {
					t.Fatalf("pass %d: %v", i+1, err)
				}
			}
			t.Logf("%s: %d reads in flight at most, %d sockets open at most, with no cap", c.name, mostInFlight.Load(), sockets.peak.Load())
			if mostInFlight.Load() != int64(c.reads) || sockets.peak.Load() != int64(c.reads) || server.listener.peak.Load() != int64(c.reads) {
				t.Errorf("a pass had %d reads in flight and %d sockets open at most, %d on the server, want the %d reads of the cap in each", mostInFlight.Load(), sockets.peak.Load(), server.listener.peak.Load(), c.reads)
			}
		})
	}
}

func TestAnHonestPassAgainstAKubeAPIServerNeedsNoMoreConnectionsThanItsCapAndFailsNoRead(t *testing.T) {
	_, kubeconfig := startKubeAPIServer(t)
	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			tenant, pass := c.start(t, kubeconfig, time.Minute)
			transport := transportOf(t, tenant)
			transport.MaxConnsPerHost = 0
			var sockets socketCount
			sockets.watch(transport)
			for i := range 40 {
				if err := pass(t.Context()); err != nil {
					t.Fatalf("pass %d: %v", i+1, err)
				}
			}
			t.Logf("%s: %d sockets open at most over 40 passes, with no cap", c.name, sockets.peak.Load())
			if sockets.peak.Load() > int64(c.reads) {
				t.Errorf("a pass had %d sockets open at once, want at most the %d reads of the cap, so that no read of an honest tenant waits on it", sockets.peak.Load(), c.reads)
			}
		})
	}
}

func TestAKubeconfigThatChangesClosesTheOldClientsIdleConnectionsAndTheNewClientHasItsOwnCap(t *testing.T) {
	release := make(chan struct{}, nodesReads)
	var arrived atomic.Int64
	server := serveCounted(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		arrived.Add(1)
		<-release
		respondWith(w, leaseBody)
	}))
	address := "http://" + server.listener.Addr().String()
	r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	t.Cleanup(func() { r.forget(key) })
	readTogether := func(tenant *tenant) {
		t.Helper()
		before := arrived.Load()
		var wg sync.WaitGroup
		for range nodesReads {
			wg.Go(func() {
				if _, err := tenant.getLease(t.Context(), "held"); err != nil {
					t.Error(err)
				}
			})
		}
		for arrived.Load() < before+nodesReads {
			time.Sleep(5 * time.Millisecond)
		}
		for range nodesReads {
			release <- struct{}{}
		}
		wg.Wait()
	}
	old, err := r.tenant(key, kubeconfigWithToken(t, address, "old"))
	if err != nil {
		t.Fatal(err)
	}
	readTogether(old)
	if got := server.listener.open.Load(); got != nodesReads {
		t.Fatalf("the old client left %d connections open, want its %d", got, nodesReads)
	}
	changed, err := r.tenant(key, kubeconfigWithToken(t, address, "new"))
	if err != nil {
		t.Fatal(err)
	}
	if changed == old {
		t.Fatal("a changed kubeconfig returned the old client")
	}
	if got, ok := settles(&server.listener.open, 0); !ok {
		t.Fatalf("%d connections of the old client were open after the kubeconfig changed, want none", got)
	}
	readTogether(changed)
	if got, peak := server.listener.open.Load(), server.listener.peak.Load(); got != nodesReads || peak > nodesReads {
		t.Errorf("the new client has %d connections open, %d at the most, want %d", got, peak, nodesReads)
	}
}
