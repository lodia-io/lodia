// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"k8s.io/apimachinery/pkg/types"

	"go.lodia.io/lodia/internal/tenantclient"
)

func TestTheTenantClientsAskForNoCompression(t *testing.T) {
	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			var requests, asked atomic.Int64
			server := serveCounted(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests.Add(1)
				if req.Header.Get("Accept-Encoding") != "" {
					asked.Add(1)
				}
				answerHonestly(w, req)
			}))
			_, pass := c.start(t, server.kubeconfig, time.Minute)
			if err := pass(t.Context()); err != nil {
				t.Fatal(err)
			}
			if requests.Load() == 0 || asked.Load() != 0 {
				t.Errorf("%d of %d requests carried an Accept-Encoding, want none of them", asked.Load(), requests.Load())
			}
		})
	}
}

type remembered struct {
	next http.RoundTripper
	last *weak.Pointer[http.Response]
}

func (r remembered) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.next.RoundTrip(req)
	if resp != nil {
		*r.last = weak.Make(resp)
	}
	return resp, err
}

func gzipped(t *testing.T, body []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	writer, err := gzip.NewWriterLevel(&out, gzip.NoCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestAGzipAnswerSentUnaskedIsAFailedReadThatKeepsNothingOfItsResponse(t *testing.T) {
	random := make([]byte, 9<<20)
	state := uint32(1)
	for i := range random {
		state = state*1664525 + 1013904223
		random[i] = byte(state >> 24)
	}
	for _, answer := range []struct {
		name   string
		stream []byte
	}{
		{"a Lease", gzipped(t, []byte(leaseBody))},
		{"9 MiB", gzipped(t, random)},
	} {
		for _, c := range clients {
			t.Run(answer.name+", "+c.name, func(t *testing.T) {
				server := serveCounted(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Content-Encoding", "gzip")
					_, _ = w.Write(answer.stream)
				}))
				tenant, _ := c.start(t, server.kubeconfig, time.Minute)
				var last weak.Pointer[http.Response]
				tenant.client.HTTP().Transport = remembered{tenant.client.HTTP().Transport, &last}
				_, err := tenant.getLease(t.Context(), "x")
				if err == nil {
					t.Fatal("a gzip answer nobody asked for was read")
				}
				t.Log(err)
				runtime.GC()
				if last.Value() != nil {
					t.Errorf("the response of the failed read is reachable after a collection")
				}
			})
		}
	}
}

type readCounted struct {
	net.Conn
	read *atomic.Int64
}

func (c readCounted) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.read.Add(int64(n))
	return n, err
}

// A probe answered with a chunked body and no Connection: close closes its connection without reading what the server sends after.
func TestAProbeAnsweredWithABodyAndNoConnectionCloseReadsNothingMoreOfItAndClosesItsConnection(t *testing.T) {
	config, certificate := localTLS(t)
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := tls.NewListener(raw, config)
	t.Cleanup(func() { _ = listener.Close() })
	returned, ended := make(chan struct{}), make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			ended <- err
			return
		}
		defer func() { _ = conn.Close() }()
		if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
			ended <- err
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nsent\r\n")
		<-returned
		_, _ = io.WriteString(conn, "5\r\nafter\r\n0\r\n\r\n")
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		_, err = conn.Read(make([]byte, 1))
		ended <- err
	}()
	var read atomic.Int64
	r := &Reconciler{Timeout: 5 * time.Second, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return readCounted{conn, &read}, nil
	}}
	p := r.prober(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}))
	code, err := r.probe(t.Context(), p.direct, "https://"+raw.Addr().String()+"/readyz")
	before := read.Load()
	close(returned)
	if code != http.StatusOK {
		t.Fatalf("the probe answered %d, %v, want 200", code, err)
	}
	if err := <-ended; errors.Is(err, os.ErrDeadlineExceeded) || read.Load() != before {
		t.Errorf("the server's read after the probe returned %v, and the probe read %d bytes of what the server sent after, want the connection closed and none of it read", err, read.Load()-before)
	}
}

func TestTheTenantClientsFollowNoRedirectAndNameNoByteOfItsLocation(t *testing.T) {
	const marker = "location-of-a-tenants-choosing"
	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			var others []tenantServer
			var reached atomic.Int64
			for range 5 {
				others = append(others, serveCounted(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					reached.Add(1)
					answerHonestly(w, req)
				})))
			}
			var requests atomic.Int64
			var holding atomic.Bool
			origin := serveCounted(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if holding.Load() {
					time.Sleep(50 * time.Millisecond)
				}
				other := others[int(requests.Add(1))%len(others)]
				http.Redirect(w, req, "http://"+other.listener.Addr().String()+"/"+marker+req.URL.RequestURI(), http.StatusFound)
			}))
			tenant, pass := c.start(t, origin.kubeconfig, time.Minute)
			var sockets socketCount
			sockets.watch(transportOf(t, tenant))
			_, err := tenant.getLease(t.Context(), "kube-controller-manager")
			if err == nil || strings.Contains(err.Error(), marker) {
				t.Errorf("a read redirected returned %v, want an error that names none of the Location", err)
			}
			if requests.Load() != 1 || reached.Load() != 0 {
				t.Errorf("a read redirected to 5 hosts sent %d requests to its host and %d to the others, want 1 and 0", requests.Load(), reached.Load())
			}
			for range 10 {
				_ = pass(t.Context())
			}
			holding.Store(true)
			readsAtOnce(t, tenant, 2*c.reads)
			if reached.Load() != 0 || sockets.peak.Load() > int64(c.reads) {
				t.Errorf("10 passes and %d reads at once redirected to 5 hosts reached the others %d times and had %d sockets open at most, want 0 and at most the cap, %d", 2*c.reads, reached.Load(), sockets.peak.Load(), c.reads)
			}
		})
	}
}

func behindAHeldPlace(t *testing.T, c passes, timeout time.Duration, tenant *tenant, starts func() []time.Duration, begun time.Time) {
	t.Helper()
	ctx, stop := context.WithTimeout(t.Context(), 5*timeout)
	t.Cleanup(stop)
	var held sync.WaitGroup
	for range c.reads {
		held.Go(func() { _, _ = tenant.getLease(ctx, "held") })
	}
	for range 1000 {
		if len(starts()) == c.reads {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := starts(); len(got) != c.reads {
		t.Fatalf("%d of the %d reads of the cap began to dial", len(got), c.reads)
	}

	deadline, cancel := context.WithTimeout(ctx, timeout/4)
	_, err := tenant.getLease(deadline, "behind")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(begun) >= timeout || len(starts()) != c.reads {
		t.Errorf("a read behind the places held returned %v after %v with %d dials begun, want its deadline of %v, before Timeout and %d dials", err, time.Since(begun), len(starts()), timeout/4, c.reads)
	}

	_, err = tenant.getLease(ctx, "behind")
	got := starts()
	if len(got) != c.reads+1 || got[c.reads] < timeout*9/10 || got[c.reads] > timeout*9/5 {
		t.Errorf("the dials began at %v, want the cap's and one more, the last after Timeout, %v, and within 1.8 of them", got, timeout)
	}
	if err == nil {
		t.Error("a read behind a dial that never ends succeeded")
	}
	held.Wait()
}

func TestADialHeldPastTimeoutGivesItsPlaceBackAtTimeoutAndAReadBehindItDialsAfterwards(t *testing.T) {
	const timeout = 500 * time.Millisecond
	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			var mu sync.Mutex
			var started []time.Duration
			begun := time.Now()
			released := make(chan struct{})
			t.Cleanup(func() { close(released) })
			clients := tenantclient.NewClients(tenantclient.Options{Connections: c.reads, Timeout: timeout, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				mu.Lock()
				started = append(started, time.Since(begun))
				mu.Unlock()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-released:
					return nil, errors.New("released")
				}
			}})
			client, err := clients.Of(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, "http://127.0.0.1:1"))
			if err != nil {
				t.Fatal(err)
			}
			behindAHeldPlace(t, c, timeout, newTenant(client), func() []time.Duration {
				mu.Lock()
				defer mu.Unlock()
				return slices.Clone(started)
			}, begun)
		})
	}
}

func TestAHandshakeHeldPastTimeoutGivesItsPlaceBackAtTimeoutAndAReadBehindItDialsAfterwards(t *testing.T) {
	const timeout = 500 * time.Millisecond
	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var accepted []time.Duration
			var conns []net.Conn
			begun := time.Now()
			go func() {
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					mu.Lock()
					accepted = append(accepted, time.Since(begun))
					conns = append(conns, conn)
					mu.Unlock()
				}
			}()
			t.Cleanup(func() {
				_ = listener.Close()
				mu.Lock()
				defer mu.Unlock()
				for _, conn := range conns {
					_ = conn.Close()
				}
			})
			clients := tenantclient.NewClients(tenantclient.Options{Connections: c.reads, Timeout: timeout})
			client, err := clients.Of(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, "https://"+listener.Addr().String()))
			if err != nil {
				t.Fatal(err)
			}
			behindAHeldPlace(t, c, timeout, newTenant(client), func() []time.Duration {
				mu.Lock()
				defer mu.Unlock()
				return slices.Clone(accepted)
			}, begun)
		})
	}
}

func TestTheTenantClientsBoundTheirHandshakeByTheTimeoutOfTheirReconciler(t *testing.T) {
	for _, c := range clients {
		t.Run(c.name, func(t *testing.T) {
			tenant, _ := c.start(t, kubeconfigFor(t, "http://127.0.0.1:1"), 7*time.Second)
			if got := transportOf(t, tenant).TLSHandshakeTimeout; got != 7*time.Second {
				t.Errorf("a handshake of %s ends within %v, want the Timeout of its reconciler, 7s", c.name, got)
			}
		})
	}
}
