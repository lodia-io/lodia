// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

type pacedReader struct {
	left, free, size, served int
	slow                     bool
	caps, opened             []int
	atEnd                    func()
}

func (r *pacedReader) Read(p []byte) (int, error) {
	if r.left == 0 {
		r.atEnd()
		return 0, io.EOF
	}
	if len(p) != r.free {
		r.caps = append(r.caps, len(p))
		r.opened = append(r.opened, r.served)
		r.size = len(p)
	}
	n := min(len(p), r.left)
	if r.slow {
		n = min(n, max(r.size-r.size/16+1-(r.size-len(p)), 1))
	}
	r.left -= n
	r.served += n
	r.free = len(p) - n
	return n, nil
}

func (r *pacedReader) held(body int) int {
	return body + sum(r.caps)
}

func sum(values []int) (total int) {
	for _, v := range values {
		total += v
	}
	return total
}

func TestIoReadAllHoldsAtMostItsChunksAndTheSliceItReturnsAtTheCut(t *testing.T) {
	sizedForTheBounds(t)
	for _, c := range []struct {
		name string
		slow bool
	}{
		{"every read filled", false},
		{"every chunk closed once it is 15/16 full", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			var heldAtMost int
			var seen, chunks []int
			for size := 1 << 20; size <= maxResponseBytes; size += 64 << 10 {
				reader := &pacedReader{left: size, slow: c.slow, atEnd: func() {}}
				if data, err := io.ReadAll(reader); err != nil || len(data) != size {
					t.Fatalf("ReadAll returned %d bytes and %v, want %d", len(data), err, size)
				}
				if held := reader.held(size); held > heldAtMost {
					heldAtMost, seen = held, reader.caps
				}
			}
			reader := &pacedReader{left: maxResponseBytes, slow: c.slow}
			var measured []int64
			for range 3 {
				base := liveHeap()
				reader.left, reader.free, reader.served, reader.caps, reader.opened = maxResponseBytes, 0, 0, nil, nil
				reader.atEnd = func() { measured = append(measured, int64(liveHeap())-int64(base)) }
				if data, err := io.ReadAll(reader); err != nil || len(data) != maxResponseBytes {
					t.Fatalf("ReadAll returned %d bytes and %v, want %d", len(data), err, maxResponseBytes)
				}
				chunks = reader.caps
			}
			slices.Sort(measured)
			open, closed := chunks[len(chunks)-1], sum(chunks[:len(chunks)-1])
			t.Logf("at the cut: %d closed chunks of %.2f MiB, the open one of %.2f MiB opened at %.2f MiB of body, the slice %.2f MiB, %.2f MiB; %.2f MiB of chunks measured; the most up to the cut %.2f MiB, with %d chunks",
				len(chunks)-1, mib(uint64(closed)), mib(uint64(open)), mib(uint64(reader.opened[len(reader.opened)-1])), mib(maxResponseBytes), mib(uint64(reader.held(maxResponseBytes))), mib(uint64(measured[1])), mib(uint64(heldAtMost)), len(seen))
			if got := int64(closed + open); measured[1] < got-256<<10 || measured[1] > got+256<<10 {
				t.Errorf("the chunks hold %d bytes, the sizes ReadAll offered add up to %d", measured[1], got)
			}
			if heldAtMost > readAllAtMost {
				t.Errorf("ReadAll holds %.2f MiB of chunks and slice up to the cut, want at most readAllAtMost, %.2f MiB", mib(uint64(heldAtMost)), mib(readAllAtMost))
			}
			if c.slow && heldAtMost < readAllAtMost-readAllAtMost/100 {
				t.Errorf("the most ReadAll holds up to the cut is %.2f MiB, and readAllAtMost, %.2f MiB, is not reached", mib(uint64(heldAtMost)), mib(readAllAtMost))
			}
		})
	}
}

func tenantClient(t *testing.T, handler http.Handler) (*http.Client, string) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
	if _, err := r.tenant(key, kubeconfigFor(t, server.URL)); err != nil {
		t.Fatal(err)
	}
	return r.clients[key].transport, server.URL
}

func medianOf(measure func() int64) int64 {
	var all []int64
	for range 5 {
		all = append(all, measure())
	}
	slices.Sort(all)
	return all[len(all)/2]
}

func TestTheTenantClientAsksForGzipAndReadsIt(t *testing.T) {
	const body = `{"metadata":{"resourceVersion":"7"},"spec":{"holderIdentity":"x"}}`
	var asked atomic.Value
	_, tenant := tenantOf(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		asked.Store(req.Header.Get("Accept-Encoding"))
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		writer := gzip.NewWriter(w)
		_, _ = io.WriteString(writer, body)
		_ = writer.Close()
	}))
	lease, err := tenant.getLease(t.Context(), "kube-controller-manager")
	if err != nil || lease.Metadata.ResourceVersion != "7" {
		t.Errorf("a gzipped answer read as %+v, error %v, want resourceVersion 7", lease, err)
	}
	if got, _ := asked.Load().(string); got != "gzip" {
		t.Errorf("the request asked for %q, want gzip", got)
	}
}
