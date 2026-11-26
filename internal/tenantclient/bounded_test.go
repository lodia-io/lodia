// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/rest"
)

type idleCloser struct {
	http.RoundTripper
	closed atomic.Bool
}

func (i *idleCloser) CloseIdleConnections() { i.closed.Store(true) }

func TestTheCapStillLetsAForgottenControlPlaneCloseItsIdleConnections(t *testing.T) {
	below := &idleCloser{}
	utilnet.CloseIdleConnectionsFor(capResponses(below, 1, time.Minute))
	if !below.closed.Load() {
		t.Error("CloseIdleConnectionsFor did not reach the transport below the cap")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestTheCapFailsABodyPastTheBoundAndSaysSoToTheRequestThatAsked(t *testing.T) {
	for _, c := range []struct {
		name    string
		size    int
		flagged bool
		wantCut bool
	}{
		{"a byte over, asked", MaxResponseBytes + 1, true, true},
		{"a byte over, not asked", MaxResponseBytes + 1, false, false},
		{"exactly the bound, asked", MaxResponseBytes, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := strings.NewReader(strings.Repeat("x", c.size))
			transport := capResponses(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(body)}, nil
			}), 1, time.Minute)
			var cut bool
			ctx := t.Context()
			if c.flagged {
				ctx = context.WithValue(ctx, cutKey{}, &cut)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://tenant", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			read, err := io.ReadAll(resp.Body)
			if wantErr := c.size > MaxResponseBytes; wantErr != errors.Is(err, ErrResponseTooLong) || len(read) != min(c.size, MaxResponseBytes) {
				t.Errorf("read %d bytes and returned %v, want %d bytes and the error of the bound: %t", len(read), err, min(c.size, MaxResponseBytes), wantErr)
			}
			if cut != c.wantCut {
				t.Errorf("the request was told of the cut: %t, want %t", cut, c.wantCut)
			}
		})
	}
}

func chunked(size int) func(http.ResponseWriter) {
	body := bytes.Repeat([]byte("x"), size)
	return func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		w.(http.Flusher).Flush()
		_, _ = w.Write(body)
	}
}

// A read cut at the bound closes its connection, also when the rest of the answer is short enough for net/http to drain it, and a read to its end or an answer with no body closed unread leaves its connection to the next read.
func TestAReadCutAtTheBoundClosesItsConnectionAndOneReadToItsEndOrWithNoBodyLeavesItToTheNext(t *testing.T) {
	for _, c := range []struct {
		name        string
		answer      func(http.ResponseWriter)
		cut, unread bool
	}{
		{"cut, a chunked answer 1 MiB past the bound", chunked(MaxResponseBytes + 1<<20), true, false},
		{"cut, a chunked answer 100 KiB past the bound", chunked(MaxResponseBytes + 100<<10), true, false},
		{"a chunked answer of 1 MiB read to its end", chunked(1 << 20), false, false},
		{"no content, closed unread", func(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }, false, true},
		{"a length of 0, closed unread", func(w http.ResponseWriter) { w.Header().Set("Content-Length", "0") }, false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			var dialed atomic.Int64
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/next" {
					_, _ = w.Write([]byte(`{}`))
					return
				}
				c.answer(w)
			}))
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					dialed.Add(1)
				}
			}
			server.Start()
			t.Cleanup(server.Close)
			client, err := New(&rest.Config{Host: server.URL}, Options{Connections: 1, Timeout: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(client.Close)

			if c.unread {
				req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/answer", nil)
				if err != nil {
					t.Fatal(err)
				}
				resp, err := client.HTTP().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_ = resp.Body.Close()
			} else if _, err := client.Do(t.Context(), client.Get().AbsPath("/answer")); c.cut != errors.Is(err, ErrResponseTooLong) || (!c.cut && err != nil) {
				t.Fatalf("the read returned %v, want the error of the bound: %t", err, c.cut)
			}

			var reused atomic.Bool
			trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused.Store(info.Reused) }}
			if _, err := client.Do(httptrace.WithClientTrace(t.Context(), trace), client.Get().AbsPath("/next")); err != nil {
				t.Fatal(err)
			}
			if want := map[bool]int64{true: 2, false: 1}[c.cut]; reused.Load() == c.cut || dialed.Load() != want {
				t.Errorf("the next read reused a connection: %t, after %d dials, want %t and %d", reused.Load(), dialed.Load(), !c.cut, want)
			}
		})
	}
}
