// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"fmt"
	"io"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

func canonicalPrefixed(names []string, prefix string) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = http.CanonicalHeaderKey(prefix + name)
	}
	return out
}

func distinctCanonical(alphabet []byte, length int) []string {
	seen := map[string]bool{}
	var out []string
	var extend func(prefix []byte)
	extend = func(prefix []byte) {
		if len(prefix) == length {
			if name := http.CanonicalHeaderKey(string(prefix)); len(name) == length && !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
			return
		}
		for _, c := range alphabet {
			extend(append(prefix, c))
		}
	}
	extend(nil)
	return out
}

func announcedNamesOfAHead(room int) []string {
	var alphabet []byte
	for c := 0x21; c <= 0xff; c++ {
		if c != ',' && c != 0x7f {
			alphabet = append(alphabet, byte(c))
		}
	}
	var names []string
	for _, name := range slices.Concat(distinctCanonical(alphabet, 1), distinctCanonical(alphabet, 2)) {
		if room < len(name)+1 {
			break
		}
		names = append(names, name)
		room -= len(name) + 1
	}
	return names
}

func heldOnceRead(t *testing.T, client *http.Client, url string, keys *int) int64 {
	t.Helper()
	return medianOf(func() int64 {
		base := liveHeap()
		resp, err := client.Get(url + nodeLeasesPath)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			t.Fatal(err)
		}
		now := liveHeap()
		*keys = len(resp.Trailer)
		runtime.KeepAlive(resp)
		_ = resp.Body.Close()
		return int64(now) - int64(base)
	})
}

func rawTenantClient(t *testing.T, url string) *http.Client {
	t.Helper()
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	r := &NodesReconciler{Timeout: time.Minute, MaxNodes: 5000}
	if _, err := r.tenant(key, kubeconfigFor(t, url)); err != nil {
		t.Fatal(err)
	}
	return r.clients[key].transport
}

func TestAnHTTP2AnswersAnnouncedNamesAndTrailersAreHeldBesideItsHeadersAndNoMoreThanTrailersHTTP2(t *testing.T) {
	headers := distinctNames(budgetHTTP2-200, 32)
	announced := canonicalPrefixed(distinctNames(budgetHTTP2-200, 33), "y")
	trailers := canonicalPrefixed(distinctNames(budgetHTTP2-200, 33), "z")
	var keys int
	held := func(announces, sendsTrailers bool) int64 {
		client, url := tenantClient(t, http2, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			for _, name := range headers {
				w.Header()[http.CanonicalHeaderKey(name)] = []string{""}
			}
			if announces {
				w.Header().Set("Trailer", strings.Join(announced, ","))
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"metadata":{"resourceVersion":"1"}}`))
			if sendsTrailers {
				for _, name := range trailers {
					w.Header()[http.TrailerPrefix+name] = []string{""}
				}
			}
		}))
		return heldOnceRead(t, client, url, &keys)
	}
	without := held(false, false)
	with := held(true, true)
	parts := len(announced)*announcedName + len(trailers)*trailerField + 2*mapTable
	t.Logf("%d header names held %.3f MiB, with %d announced names and %d trailers %.3f MiB, %.3f MiB more; the parts add to %.3f MiB, trailersHTTP2 is %.3f MiB",
		len(headers), mib(uint64(without)), len(announced), len(trailers), mib(uint64(with)), mib(uint64(with-without)), mib(uint64(parts)), mib(trailersHTTP2))
	if len(announced) > fieldsOfABudget || len(trailers) > fieldsOfABudget {
		t.Fatalf("%d names announced and %d trailers, want at most fieldsOfABudget, %d, of each", len(announced), len(trailers), fieldsOfABudget)
	}
	if keys < (len(announced)+len(trailers))*9/10 {
		t.Errorf("the response has %d keys of Trailer, want most of the %d names announced and trailers sent", keys, len(announced)+len(trailers))
	}
	if more := int(with - without); more < parts/2 || more > trailersHTTP2 {
		t.Errorf("the names and the trailers are held in %d bytes, want %d to trailersHTTP2, %d", more, parts/2, trailersHTTP2)
	}
}

func TestAnHTTP1AnswersAnnouncedNamesAreHeldBesideItsHeadAndNoMoreThanHeadersHTTP1(t *testing.T) {
	prefix := "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nTrailer: "
	suffix := "\r\n\r\n"
	names := announcedNamesOfAHead(maxResponseHeaderBytes - len(prefix) - len(suffix))
	head := prefix + strings.Join(names, ",") + suffix
	if len(head) > maxResponseHeaderBytes || len(head) < maxResponseHeaderBytes-8 {
		t.Fatalf("the head has %d bytes, want it at the bound of %d", len(head), maxResponseHeaderBytes)
	}
	const body = `{"metadata":{"resourceVersion":"1"}}`
	url, _ := answerOnTCP(t, head, fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(body), body))
	var keys int
	held := heldOnceRead(t, rawTenantClient(t, url), url, &keys)
	parts := len(names)*announcedName + mapTable
	t.Logf("a head of %d bytes announcing %d names: %d keys of Trailer, held %.3f MiB; the parts add to %.3f MiB, headersHTTP1 is %.3f MiB",
		len(head), len(names), keys, mib(uint64(held)), mib(uint64(parts)), mib(headersHTTP1))
	if keys != len(names) {
		t.Errorf("the response has %d keys of Trailer, want the %d names announced", keys, len(names))
	}
	if len(names) > announcedNamesHTTP1 {
		t.Errorf("a head of the bound announced %d names, more than announcedNamesHTTP1, %d", len(names), announcedNamesHTTP1)
	}
	if held < int64(parts)/2 || held > headersHTTP1 {
		t.Errorf("the names are held in %d bytes, want %d to headersHTTP1, %d", held, parts/2, headersHTTP1)
	}
}

func TestAnHTTP1ChunkedAnswersTrailersAreHeldAtItsEndAndNoMoreThanTrailersHTTP1(t *testing.T) {
	const body = `{"metadata":{"resourceVersion":"1"}}`
	trailers := distinctNames(4096-2, 3)
	if len(trailers) > trailerFieldsHTTP1 {
		t.Fatalf("a trailer block of 4,096 bytes has %d fields, want at most trailerFieldsHTTP1, %d", len(trailers), trailerFieldsHTTP1)
	}
	var keys int
	held := func(sendsTrailers bool) int64 {
		tail := "0\r\n"
		if sendsTrailers {
			for _, name := range trailers {
				tail += name + ":\r\n"
			}
		}
		url, _ := answerOnTCP(t, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nTrailer: Zz\r\n\r\n", fmt.Sprintf("%x\r\n%s\r\n", len(body), body)+tail+"\r\n")
		return heldOnceRead(t, rawTenantClient(t, url), url, &keys)
	}
	without := held(false)
	with := held(true)
	parts := len(trailers)*trailerFieldHTTP1 + 2*mapTable
	t.Logf("%d trailers: %d keys of Trailer, held %.3f MiB more; the parts add to %.3f MiB, trailersHTTP1 is %.3f MiB",
		len(trailers), keys, mib(uint64(with-without)), mib(uint64(parts)), mib(trailersHTTP1))
	if keys != len(trailers)+1 {
		t.Errorf("the response has %d keys of Trailer, want the %d trailers and the name announced", keys, len(trailers))
	}
	if more := int(with - without); more < len(trailers)*trailerFieldHTTP1/4 || more > trailersHTTP1 {
		t.Errorf("the trailers are held in %d bytes, want %d to trailersHTTP1, %d", more, len(trailers)*trailerFieldHTTP1/4, trailersHTTP1)
	}
}
