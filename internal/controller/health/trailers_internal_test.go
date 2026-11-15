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
	"sync"
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

func heldByARead(t *testing.T, client *http.Client, url string, keys *keysRead) int64 {
	t.Helper()
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
}

func heldOnceRead(t *testing.T, client *http.Client, url string, keys *keysRead) int64 {
	t.Helper()
	return medianOf(func() int64 { return heldByARead(t, client, url, keys) })
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

func TestAnHTTP1HeadOfFieldLinesEndingInALineFeedIsHeldAtMostHeadAndTrailersHTTP1(t *testing.T) {
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
	parts := headPartsAtMost(headMix{keys: len(names) + 2}).sum()
	t.Logf("a head of %d bytes of %d field lines ended by a line feed: %d keys read, held %.3f MiB; the parts add to %.3f MiB, headAndTrailersHTTP1 is %.3f MiB",
		len(head), len(names), keys.header, mib(uint64(held)), mib(uint64(parts)), mib(headAndTrailersHTTP1))
	if len(names) > fieldLinesHTTP1 || len(names) < fieldLinesHTTP1*9/10 || keys.header != len(names)+2 {
		t.Errorf("a head of the bound carried %d field lines and was read as %d keys, want nine tenths of fieldLinesHTTP1, %d, to all of it, and two keys more", len(names), keys.header, fieldLinesHTTP1)
	}
	if held < int64(parts)/2 || held > int64(parts) || parts > headAndTrailersHTTP1 {
		t.Errorf("the head is held in %d bytes and its parts add to %d, want the parts' half to the parts, and the parts at most headAndTrailersHTTP1, %d", held, parts, headAndTrailersHTTP1)
	}
}

func TestAnHTTP1HeadOfNamesAndFieldLinesTogetherWithTheTrailerValueIsHeldAtMostHeadAndTrailersHTTP1(t *testing.T) {
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
	parts := headPartsAtMost(headMix{keys: len(lines) + 3, announced: len(names), announcedBytes: len(value) + 1}).sum()
	t.Logf("a head of %d bytes with %d field lines of one byte and a Trailer of %d names, %d of one byte: %d keys of Trailer, held %.3f MiB; the parts add to %.3f MiB, headAndTrailersHTTP1 is %.3f MiB",
		len(head), len(lines), len(names), oneByte, keys.trailer, mib(uint64(held)), mib(uint64(parts)), mib(headAndTrailersHTTP1))
	if keys.trailer != len(names) || keys.header != len(lines)+1 {
		t.Errorf("the response has %d keys of Trailer and %d of the head, want the %d names announced and the %d field lines and Content-Type", keys.trailer, keys.header, len(names), len(lines))
	}
	if len(names) > announcedNamesHTTP1 || oneByte != 195 || len(lines) != 51 || oneByte > oneByteNames || len(lines) > oneByteFieldNames {
		t.Errorf("a head of the bound announced %d names, %d of one byte, and carried %d field lines of one byte, want at most %d, and 195 and 51, which are at most %d and %d", len(names), oneByte, len(lines), announcedNamesHTTP1, oneByteNames, oneByteFieldNames)
	}
	if held < int64(parts)/2 || held > int64(parts) || parts > headAndTrailersHTTP1 {
		t.Errorf("the head is held in %d bytes and its parts add to %d, want the parts' half to the parts, and the parts at most headAndTrailersHTTP1, %d", held, parts, headAndTrailersHTTP1)
	}
}

const (
	headOfAMixPrefix = "HTTP/1.1 200 OK\nContent-Type: application/json\nTransfer-Encoding: chunked\n"
	headOfAMixKeys   = 3

	mostOfTheScanNames = 14600
	mostOfTheScanLines = 4890
)

type realizedHead struct {
	text                                          string
	fieldLines, announced, announcedBytes, trails int
}

func theTrailersOfAFullBlock() (names []string, block string) {
	names = canonicalFieldNames(trailerBlockHTTP1)
	return names, strings.Join(names[:len(names)-1], ":\n") + ":\n" + names[len(names)-1] + ":\r\n\r\n"
}

var announcedCandidates = sync.OnceValue(func() []string {
	var alphabet []byte
	for c := 0x21; c <= 0xff; c++ {
		if c != ',' && c != 0x7f {
			alphabet = append(alphabet, byte(c))
		}
	}
	return slices.Concat(distinctCanonical(alphabet, 1), distinctCanonical(alphabet, 2))
})

func announcedNamesApartFrom(trailers []string, count, room int) []string {
	apart := map[string]bool{}
	for _, name := range trailers {
		apart[name] = true
	}
	var names []string
	for _, name := range announcedCandidates() {
		if apart[name] {
			continue
		}
		if len(names) == count || room < len(name)+1 {
			break
		}
		names = append(names, name)
		room -= len(name) + 1
	}
	return names
}

func headOfAMix(t *testing.T, prefix, lineBreak string, announced, lines int, trailers []string) realizedHead {
	t.Helper()
	const trailer = "Trailer: "
	names := announcedNamesApartFrom(trailers, announced, maxResponseHeaderBytes)
	if len(names) != announced {
		t.Fatalf("a head of %d announced names is asked for, and %d fit", announced, len(names))
	}
	value := strings.Join(names, ",")
	room := maxResponseHeaderBytes - len(prefix) - len(trailer) - len(value) - len(lineBreak)
	fields := canonicalFieldNames(room)
	if lines >= 0 {
		if len(fields) < lines {
			t.Fatalf("a head of %d announced names and %d field lines is asked for, and %d lines fit", announced, lines, len(fields))
		}
		fields = fields[:lines]
	}
	text := prefix
	if len(fields) > 0 {
		text += strings.Join(fields, ":\n") + ":\n"
	}
	text += trailer + value + lineBreak
	if len(text) > maxResponseHeaderBytes {
		t.Fatalf("the head has %d bytes, want at most the bound of %d", len(text), maxResponseHeaderBytes)
	}
	return realizedHead{text, len(fields), len(names), len(value) + 1, len(trailers)}
}

func (h realizedHead) parts() headParts {
	return headPartsAtMost(headMix{keys: h.fieldLines + headOfAMixKeys, announced: h.announced, announcedBytes: h.announcedBytes, trailers: h.trails})
}

func answerWithTrailers(t *testing.T, head realizedHead, block string) (client *http.Client, url string) {
	t.Helper()
	const body = `{"metadata":{"resourceVersion":"1"}}`
	url, _ = answerOnTCP(t, head.text, fmt.Sprintf("%x\r\n%s\r\n0\r\n", len(body), body)+block)
	return rawTenantClient(t, url), url
}

func TestAnHTTP1HeadThatAnnouncesTheNamesOfTheDearestCountWithAFullBlockOfTrailersNoneOfThemAnnouncedIsHeldAtMostHeadAndTrailersHTTP1(t *testing.T) {
	sizedForTheBounds(t)
	dearest := theDearestHead()
	trailers, block := theTrailersOfAFullBlock()
	head := headOfAMix(t, headOfAMixPrefix, "\n\n", dearest.announced, -1, trailers)
	client, url := answerWithTrailers(t, head, block)
	var keys keysRead
	held := heldOnceRead(t, client, url, &keys)
	parts := head.parts()
	t.Logf("a head of %d bytes of %d field lines and %d announced names, then %d trailers: held %.3f MiB; its parts add to %.3f MiB, the dearest of the count, %d lines and %d names, %.3f MiB, headAndTrailersHTTP1 is %.3f MiB",
		len(head.text), head.fieldLines, head.announced, len(trailers), mib(uint64(held)), mib(uint64(parts.sum())), dearest.fieldLines, dearest.announced, mib(uint64(dearest.bytes())), mib(headAndTrailersHTTP1))
	if keys.trailer != head.announced+len(trailers) || keys.header != head.fieldLines+1 || len(trailers) != 1036 {
		t.Errorf("the response has %d keys of Trailer and %d of the head, want the %d names announced and the %d trailers, and the %d field lines and Content-Type", keys.trailer, keys.header, head.announced, len(trailers), head.fieldLines)
	}
	if held > int64(parts.sum()) || parts.sum() > dearest.bytes() || dearest.bytes() > headAndTrailersHTTP1 {
		t.Errorf("the head with its trailers is held in %d bytes, its parts add to %d, the dearest head's to %d, want them in that order and the last at most headAndTrailersHTTP1, %d", held, parts.sum(), dearest.bytes(), headAndTrailersHTTP1)
	}
}

func TestTheMostAHeadOfDistinctNamesAndFieldLinesWithItsTrailersHoldsIsFoundByScanningItsAnnouncedNames(t *testing.T) {
	sizedForTheBounds(t)
	trailers, block := theTrailersOfAFullBlock()
	var most, mostNames, mostLines int64
	var scanned int
	for announced := 12000; ; announced += 200 {
		if len(announcedNamesApartFrom(trailers, announced, maxResponseHeaderBytes-len(headOfAMixPrefix)-len("Trailer: ")-len("\n\n"))) < announced {
			break
		}
		head := headOfAMix(t, headOfAMixPrefix, "\n\n", announced, -1, trailers)
		client, url := answerWithTrailers(t, head, block)
		var keys keysRead
		held := heldByARead(t, client, url, &keys)
		scanned++
		if parts := head.parts().sum(); held > int64(parts) || parts > headAndTrailersHTTP1 {
			t.Errorf("a head of %d announced names and %d field lines is held in %d bytes and its parts add to %d, want the parts at most headAndTrailersHTTP1, %d", head.announced, head.fieldLines, held, parts, headAndTrailersHTTP1)
		}
		if held > most {
			most, mostNames, mostLines = held, int64(head.announced), int64(head.fieldLines)
		}
	}
	if scanned < 49 {
		t.Errorf("the scan read %d heads, want one in every 200 announced names from 12,000 to the most a head carries, at least 49", scanned)
	}
	pinned := headOfAMix(t, headOfAMixPrefix, "\n\n", mostOfTheScanNames, mostOfTheScanLines, trailers)
	client, url := answerWithTrailers(t, pinned, block)
	var keys keysRead
	held := heldOnceRead(t, client, url, &keys)
	t.Logf("the most a head of distinct names and field lines holds with its trailers, of the heads of 12,000 announced names and over in steps of 200: %.3f MiB, at %d announced names and %d field lines; the head of %d names and %d lines the realized test and the gated measures read holds %.3f MiB; the term is %.3f MiB",
		mib(uint64(most)), mostNames, mostLines, mostOfTheScanNames, mostOfTheScanLines, mib(uint64(held)), mib(headAndTrailersHTTP1))
	if held < most-mebibyte/10 || most < held-mebibyte/10 {
		t.Errorf("the head of %d names and %d lines holds %d bytes, more than 0.1 MiB from the %d the scan found at %d names and %d lines", mostOfTheScanNames, mostOfTheScanLines, held, most, mostNames, mostLines)
	}
}

func TestTheHeadTheScanFindsMostWithItsTrailersIsHeldAtMostItsPartsAndTheTerm(t *testing.T) {
	sizedForTheBounds(t)
	dearest := theDearestHead()
	trailers, block := theTrailersOfAFullBlock()
	head := headOfAMix(t, headOfAMixPrefix, "\n\n", mostOfTheScanNames, mostOfTheScanLines, trailers)
	client, url := answerWithTrailers(t, head, block)
	var keys keysRead
	held := heldOnceRead(t, client, url, &keys)
	parts := head.parts()
	t.Logf("a head of %d bytes of %d field lines and %d announced names, then %d trailers: held %.3f MiB; its parts add to %.3f MiB, the dearest of the count, %.3f MiB, headAndTrailersHTTP1 is %.3f MiB",
		len(head.text), head.fieldLines, head.announced, len(trailers), mib(uint64(held)), mib(uint64(parts.sum())), mib(uint64(dearest.bytes())), mib(headAndTrailersHTTP1))
	if head.announced != mostOfTheScanNames || head.fieldLines != mostOfTheScanLines || keys.trailer != mostOfTheScanNames+len(trailers) || keys.header != mostOfTheScanLines+1 {
		t.Errorf("the head announces %d names and carries %d field lines, and the response has %d keys of Trailer and %d of the head, want the %d names and %d lines the scan found, and the %d trailers besides, and Content-Type",
			head.announced, head.fieldLines, keys.trailer, keys.header, mostOfTheScanNames, mostOfTheScanLines, len(trailers))
	}
	if held > int64(parts.sum()) || parts.sum() > dearest.bytes() || dearest.bytes() > headAndTrailersHTTP1 {
		t.Errorf("the head with its trailers is held in %d bytes, its parts add to %d, the dearest of the count's to %d, want them in that order and the last at most headAndTrailersHTTP1, %d", held, parts.sum(), dearest.bytes(), headAndTrailersHTTP1)
	}
}
