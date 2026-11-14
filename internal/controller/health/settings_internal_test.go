// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"runtime"
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
