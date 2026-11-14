// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"fmt"
	"io"
	"net/http"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

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

type keysRead struct{ header, trailer int }

func heldOnceRead(t *testing.T, client *http.Client, url string, keys *keysRead) int64 {
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
		*keys = keysRead{len(resp.Header), len(resp.Trailer)}
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

const tokenCharacters = "!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func canonicalFieldNames(room int) []string {
	seen := map[string]bool{}
	var names []string
	for length := 1; ; length++ {
		var extend func(prefix []byte) bool
		extend = func(prefix []byte) bool {
			if len(prefix) < length {
				for i := range len(tokenCharacters) {
					if !extend(append(prefix, tokenCharacters[i])) {
						return false
					}
				}
				return true
			}
			name := http.CanonicalHeaderKey(string(prefix))
			if len(name) != length || seen[name] {
				return true
			}
			if room < length+len(":\n") {
				return false
			}
			seen[name] = true
			names = append(names, name)
			room -= length + len(":\n")
			return true
		}
		if !extend(nil) {
			return names
		}
	}
}

func TestAnHTTP1HeadOfFieldLinesEndingInALineFeedIsHeldAtMostHeadersHTTP1(t *testing.T) {
	sizedForTheBounds(t)
	const body = `{"metadata":{"resourceVersion":"1"}}`
	start := "HTTP/1.1 200 OK\nContent-Type: application/json\nContent-Length: " + strconv.Itoa(len(body)) + "\n"
	names := canonicalFieldNames(maxResponseHeaderBytes - len(start) - len("\n"))
	head := start + strings.Join(names, ":\n") + ":\n\n"
	if len(head) > maxResponseHeaderBytes || len(head) < maxResponseHeaderBytes-4 {
		t.Fatalf("the head has %d bytes, want it at the bound of %d", len(head), maxResponseHeaderBytes)
	}
	url, _ := answerOnTCP(t, head, body)
	var keys keysRead
	held := heldOnceRead(t, rawTenantClient(t, url), url, &keys)
	parts := len(names)*fieldLine + mapTable
	t.Logf("a head of %d bytes of %d field lines ended by a line feed: %d keys read, held %.3f MiB; the parts add to %.3f MiB, headersHTTP1 is %.3f MiB",
		len(head), len(names), keys.header, mib(uint64(held)), mib(uint64(parts)), mib(headersHTTP1))
	if len(names) > fieldLinesHTTP1 || len(names) < fieldLinesHTTP1*9/10 || keys.header != len(names)+2 {
		t.Errorf("a head of the bound carried %d field lines and was read as %d keys, want nine tenths of fieldLinesHTTP1, %d, to all of it, and two keys more", len(names), keys.header, fieldLinesHTTP1)
	}
	if held < int64(parts)/2 || held > headersHTTP1 {
		t.Errorf("the head is held in %d bytes, want %d to headersHTTP1, %d", held, parts/2, headersHTTP1)
	}
}

func TestAnHTTP1HeadOfNamesAndFieldLinesTogetherWithTheTrailerValueIsHeldAtMostHeadersHTTP1(t *testing.T) {
	sizedForTheBounds(t)
	prefix := "HTTP/1.1 200 OK\nContent-Type: application/json\nTransfer-Encoding: chunked\n"
	var lines []string
	for _, name := range canonicalFieldNames(oneByteFieldNames * 3) {
		if len(name) == 1 {
			lines = append(lines, name)
		}
	}
	fields := strings.Join(lines, ":\n") + ":\n"
	const trailer, suffix = "Trailer: ", "\n\n"
	names := announcedNamesOfAHead(maxResponseHeaderBytes - len(prefix) - len(fields) - len(trailer) - len(suffix))
	value := strings.Join(names, ",")
	head := prefix + fields + trailer + value + suffix
	if len(head) > maxResponseHeaderBytes || len(head) < maxResponseHeaderBytes-4 {
		t.Fatalf("the head has %d bytes, want it at the bound of %d", len(head), maxResponseHeaderBytes)
	}
	const body = `{"metadata":{"resourceVersion":"1"}}`
	url, _ := answerOnTCP(t, head, fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(body), body))
	var keys keysRead
	held := heldOnceRead(t, rawTenantClient(t, url), url, &keys)
	oneByte := 0
	for _, name := range names {
		if len(name) == 1 {
			oneByte++
		}
	}
	parts := len(names)*announcedName + len(lines)*fieldLine + 2*mapTable + len(value)
	t.Logf("a head of %d bytes with %d field lines of one byte and a Trailer of %d names, %d of one byte: %d keys of Trailer, held %.3f MiB; the parts add to %.3f MiB, headersHTTP1 is %.3f MiB",
		len(head), len(lines), len(names), oneByte, keys.trailer, mib(uint64(held)), mib(uint64(parts)), mib(headersHTTP1))
	if keys.trailer != len(names) || keys.header != len(lines)+1 {
		t.Errorf("the response has %d keys of Trailer and %d of the head, want the %d names announced and the %d field lines and Content-Type", keys.trailer, keys.header, len(names), len(lines))
	}
	if len(names) > announcedNamesHTTP1 || oneByte != 195 || len(lines) != 51 || oneByte > oneByteNames || len(lines) > oneByteFieldNames {
		t.Errorf("a head of the bound announced %d names, %d of one byte, and carried %d field lines of one byte, want at most %d, and 195 and 51, which are at most %d and %d", len(names), oneByte, len(lines), announcedNamesHTTP1, oneByteNames, oneByteFieldNames)
	}
	if held < int64(parts)/2 || held > headersHTTP1 {
		t.Errorf("the head is held in %d bytes, want %d to headersHTTP1, %d", held, parts/2, headersHTTP1)
	}
}

func TestAnHTTP1ChunkedAnswersTrailersEndingInALineFeedAreHeldAtItsEndAtMostTrailersHTTP1(t *testing.T) {
	const body = `{"metadata":{"resourceVersion":"1"}}`
	names := canonicalFieldNames(4096 - 3)
	var keys keysRead
	held := func(sendsTrailers bool) int64 {
		tail := "0\r\n"
		if sendsTrailers {
			tail += strings.Join(names[:len(names)-1], ":\n") + ":\n" + names[len(names)-1] + ":\r\n\r\n"
		} else {
			tail += "\r\n"
		}
		url, _ := answerOnTCP(t, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nTrailer: Zz\r\n\r\n", fmt.Sprintf("%x\r\n%s\r\n", len(body), body)+tail)
		return heldOnceRead(t, rawTenantClient(t, url), url, &keys)
	}
	without := held(false)
	with := held(true)
	parts := len(names)*trailerFieldHTTP1 + 2*mapTable
	t.Logf("%d trailers ended by a line feed: %d keys of Trailer, held %.3f MiB more; the parts add to %.3f MiB, trailersHTTP1 is %.3f MiB",
		len(names), keys.trailer, mib(uint64(with-without)), mib(uint64(parts)), mib(trailersHTTP1))
	if len(names) > trailerFieldsHTTP1 || len(names) != 1036 || keys.trailer != len(names)+1 {
		t.Errorf("a block of 4,096 bytes carried %d fields and the response has %d keys of Trailer, want 1,036, at most trailerFieldsHTTP1, %d, and the name announced besides", len(names), keys.trailer, trailerFieldsHTTP1)
	}
	if more := int(with - without); more < len(names)*trailerFieldHTTP1/4 || more > trailersHTTP1 {
		t.Errorf("the trailers are held in %d bytes, want %d to trailersHTTP1, %d", more, len(names)*trailerFieldHTTP1/4, trailersHTTP1)
	}
}
