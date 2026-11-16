// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"net"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"
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
				tenant.api.Client.Transport = remembered{tenant.api.Client.Transport, &last}
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
			origin := serveCounted(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				other := others[int(requests.Add(1))%len(others)]
				http.Redirect(w, req, "http://"+other.listener.Addr().String()+"/"+marker+req.URL.RequestURI(), http.StatusFound)
			}))
			tenant, pass := c.start(t, origin.kubeconfig, time.Minute)
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
			time.Sleep(100 * time.Millisecond)
			var open int64
			for _, server := range append(others, origin) {
				open += server.listener.peak.Load()
			}
			if reached.Load() != 0 || open > int64(c.reads) {
				t.Errorf("10 passes redirected to 5 hosts reached the others %d times and had %d connections open at most across the 6 servers, want 0 and %d", reached.Load(), open, c.reads)
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
			clients := &tenantClients{connections: c.reads, timeout: timeout, dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				mu.Lock()
				started = append(started, time.Since(begun))
				mu.Unlock()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-released:
					return nil, errors.New("released")
				}
			}}
			tenant, err := clients.of(kubeconfigFor(t, "http://127.0.0.1:1"))
			if err != nil {
				t.Fatal(err)
			}
			behindAHeldPlace(t, c, timeout, tenant, func() []time.Duration {
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
			clients := &tenantClients{connections: c.reads, timeout: timeout}
			tenant, err := clients.of(kubeconfigFor(t, "https://"+listener.Addr().String()))
			if err != nil {
				t.Fatal(err)
			}
			behindAHeldPlace(t, c, timeout, tenant, func() []time.Duration {
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

func TestTheDialFunctionOfAClientGetsAContextThatEndsWithinTimeoutWhicheverDialFunctionItIs(t *testing.T) {
	const timeout = 7 * time.Second
	for _, c := range []struct {
		name   string
		parent func() (context.Context, context.CancelFunc)
		most   time.Duration
	}{
		{"net/http's context, with no deadline", func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) }, timeout},
		{"a context that ends sooner", func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Second)
		}, time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			var left time.Duration
			var bounded bool
			clients := &tenantClients{timeout: timeout, dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				deadline, ok := ctx.Deadline()
				bounded, left = ok, time.Until(deadline)
				return nil, errors.New("no connection")
			}}
			ctx, cancel := c.parent()
			defer cancel()
			if _, err := clients.dialContext(ctx, "tcp", "tenant.example:6443"); err == nil {
				t.Fatal("the dial function that refuses returned a connection")
			}
			if !bounded || left <= 0 || left > c.most {
				t.Errorf("the dial function got a context that ends in %v, bounded %t, want one that ends in at most %v", left, bounded, c.most)
			}
		})
	}
}

func TestTheProductionDialerOfAClientIsBoundByTimeoutWhenNoDialFunctionIsSet(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	for _, c := range []struct {
		timeout  time.Duration
		connects bool
	}{
		{time.Nanosecond, false},
		{time.Minute, true},
	} {
		conn, err := (&tenantClients{timeout: c.timeout}).dialContext(context.Background(), "tcp", listener.Addr().String())
		if conn != nil {
			_ = conn.Close()
		}
		if (err == nil) != c.connects {
			t.Errorf("a dial of a Timeout of %v to a listener that answers ended in %v, want a connection: %t", c.timeout, err, c.connects)
		}
	}
}
