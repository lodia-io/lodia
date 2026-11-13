// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

type chunkedReader struct {
	left, step int
	atEnd      func()
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.left == 0 {
		r.atEnd()
		return 0, io.EOF
	}
	n := min(len(p), r.step, r.left)
	r.left -= n
	return n, nil
}

func TestIoReadAllHoldsAtMostOnePointSixBodiesInChunksBesideTheSliceItReturns(t *testing.T) {
	for _, size := range []int{100_000, 1 << 20, 3_300_000, 5_500_000, 7_000_000, maxResponseBytes} {
		for _, step := range []int{1 << 10, 4 << 10, 32 << 10} {
			least := ^uint64(0)
			for range 3 {
				base := liveHeap()
				var chunks uint64
				reader := &chunkedReader{left: size, step: step, atEnd: func() { chunks = uint64(max(int64(liveHeap())-int64(base), 0)) }}
				data, err := io.ReadAll(reader)
				if err != nil || len(data) != size {
					t.Fatalf("ReadAll returned %d bytes and %v, want %d", len(data), err, size)
				}
				least = min(least, chunks)
			}
			t.Logf("a body of %d bytes read %d at a time: %.3f bodies in chunks", size, step, float64(least)/float64(size))
			if limit := uint64(1.6*float64(size)) + 128<<10; least > limit {
				t.Errorf("a body of %d bytes read %d at a time is held in %d bytes of chunks, want at most %d, 1.6 bodies", size, step, least, limit)
			}
		}
	}
}

func TestAnHTTP2StreamBuffersItsWindowOfABodyNobodyReadsAndNoMore(t *testing.T) {
	body := bytes.Repeat([]byte("a"), 12<<20)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
	if _, err := r.tenant(key, kubeconfigForTLS(t, server)); err != nil {
		t.Fatal(err)
	}
	client := r.clients[key].transport
	var held []int64
	for range 3 {
		base := liveHeap()
		resp, err := client.Get(server.URL + nodeLeasesPath)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(500 * time.Millisecond)
		held = append(held, int64(liveHeap())-int64(base))
		_ = resp.Body.Close()
	}
	slices.Sort(held)
	if held[1] < 4<<20-1<<18 || held[1] > streamBuffer+1<<18 {
		t.Errorf("the transport holds %d bytes of a body nobody has read, want the stream's window of 4 MiB and at most %d", held[1], streamBuffer+1<<18)
	}
}
