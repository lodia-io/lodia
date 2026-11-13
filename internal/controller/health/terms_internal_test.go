// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"math"
	"math/bits"
	"net/http"
	"reflect"
	"runtime"
	"strconv"
	"testing"

	utilnet "k8s.io/apimachinery/pkg/util/net"
)

const (
	fieldOfABudget  = 33
	budgetHTTP2     = maxResponseHeaderBytes + 320
	fieldsOfABudget = budgetHTTP2 / fieldOfABudget

	frameSize   = 16 << 10
	hpackSaved  = 2 * (budgetHTTP2 + 8)
	hpackBuffer = 256 << 10

	mapSlot      = 94
	mapTable     = 1024 * 41
	nameCopy     = 16
	valueCopy    = 16
	valueSlice   = 16
	decodedField = 40

	announcedName     = mapSlot + nameCopy
	fieldLine         = mapSlot + nameCopy + valueCopy + valueSlice
	trailerField      = 2*mapSlot + nameCopy + valueSlice
	trailerNamedField = 3 * valueSlice
	trailersFrame     = 2 * fieldsOfABudget * decodedField

	oneByteNames        = 256
	oneByteFieldNames   = 77
	twoByteFieldNames   = oneByteFieldNames * oneByteFieldNames
	headRest            = maxResponseHeaderBytes - 2*oneByteNames - 3*oneByteFieldNames
	announcedNamesHTTP1 = oneByteNames + (maxResponseHeaderBytes-2*oneByteNames)/3
	headersHTTP1Parts   = oneByteNames*announcedName + oneByteFieldNames*fieldLine + (headRest*announcedName+2)/3 + 2*mapTable + maxResponseHeaderBytes
	fieldLinesHTTP1     = oneByteFieldNames + twoByteFieldNames + (maxResponseHeaderBytes-3*oneByteFieldNames-4*twoByteFieldNames)/5
	trailerBlockHTTP1   = 4096 - 3
	trailerFieldsHTTP1  = oneByteFieldNames + (trailerBlockHTTP1-3*oneByteFieldNames)/4
	trailerFieldHTTP1   = trailerField

	gzipReaderStruct = 704
	gzipBufioStruct  = 96
	gzipBufioBuffer  = 4096
	gzipDecompressor = 4864
	gzipBitsTables   = 2688 + 160
	gzipWindow       = 32 << 10
	gzipLinkTables   = 137 + 8
	gzipLinkTable    = 64 * 4
	gzipLinkSlices   = 3456 + 192
	gzipExtra        = 64 << 10
	gzipName         = 1152
	gzipComment      = 1152
	gzipRunes        = 2048
	gzipWrapper      = 48

	gzipFixedParts  = gzipReaderStruct + gzipBufioStruct + gzipBufioBuffer + gzipDecompressor + gzipBitsTables + gzipWindow
	gzipLinks       = gzipLinkTables*gzipLinkTable + gzipLinkSlices
	gzipHeader      = gzipExtra + gzipName + gzipComment
	gzipLaterHeader = gzipHeader + gzipRunes
	gzipParts       = gzipFixedParts + gzipLinks + gzipHeader + gzipLaterHeader + gzipWrapper
)

func TestEveryTermOfAReadIsAtLeastItsPartsAndLessThanTheStepOfItsRounding(t *testing.T) {
	for _, c := range []struct {
		name        string
		term, parts int
		step        int
	}{
		{"the head of an HTTP/1.1 answer, its names, field lines and Trailer value", headersHTTP1, headersHTTP1Parts, mebibyte / 10},
		{"the trailers of a chunked HTTP/1.1 answer", trailersHTTP1, trailerFieldsHTTP1*trailerFieldHTTP1 + 2*mapTable, mebibyte / 10},
		{"the names a head announces and the trailers of an HTTP/2 answer", trailersHTTP2,
			fieldsOfABudget*(announcedName+trailerField+trailerNamedField) + trailersFrame + 2*mapTable, mebibyte / 10},
		{"a gzip reader", gzipReader, gzipParts, 1 << 10},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.term < c.parts || c.term-c.parts >= c.step {
				t.Errorf("the term is %d bytes and its parts add to %d, want the term at least the parts and under %d bytes more", c.term, c.parts, c.step)
			}
		})
	}
}

func allocatedBy(build func()) int {
	rounds := max(100, min(10000, (8<<20)/max(int(allocated(build)), 1)))
	least := math.MaxInt
	for range 20 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		for range rounds {
			build()
		}
		runtime.ReadMemStats(&after)
		least = min(least, int((after.TotalAlloc-before.TotalAlloc)/uint64(rounds)))
	}
	return least
}

func theWrapperOfTheGzipReaderTakes(t *testing.T) int {
	t.Helper()
	most := 0
	for _, p := range protocols {
		client, url := tenantClient(t, p, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Encoding", "gzip")
			_, _ = w.Write(gzipOfAByte(t))
		}))
		bottom := client.Transport
		for {
			wrapper, ok := bottom.(utilnet.RoundTripperWrapper)
			if !ok {
				break
			}
			bottom = wrapper.WrappedRoundTripper()
		}
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url+nodeLeasesPath, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := bottom.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		wrapperType := reflect.TypeOf(resp.Body).Elem()
		_ = resp.Body.Close()
		var made reflect.Value
		most = max(most, allocatedBy(func() { made = reflect.New(wrapperType) }))
		runtime.KeepAlive(made)
	}
	return most
}

func TestTheSizesTheTermsAreBuiltFromAreTheSourcesOnThisPlatform(t *testing.T) {
	if got := reflect.TypeFor[gzip.Reader]().Size(); got != 664 {
		t.Errorf("a gzip.Reader is %d bytes, want 664", got)
	}
	if got := reflect.TypeFor[bufio.Reader]().Size(); got != 88 {
		t.Errorf("a bufio.Reader is %d bytes, want 88", got)
	}
	if mapSlot < (41*16+6)/7 {
		t.Errorf("a map slot of 40 bytes and a control byte costs up to 16/7 of 41 bytes, %d, and mapSlot is %d", (41*16+6)/7, mapSlot)
	}
	flateReader := flate.NewReader(bytes.NewReader(nil))
	decompressor := reflect.TypeOf(flateReader).Elem()
	if got := decompressor.Size(); got != 4392 {
		t.Errorf("flate's decompressor is %d bytes, want 4392", got)
	}
	pointee := func(field string) reflect.Type {
		f, ok := decompressor.FieldByName(field)
		if !ok {
			t.Fatalf("flate's decompressor has no field %s", field)
		}
		return f.Type.Elem()
	}
	window := reflect.ValueOf(flateReader).Elem().FieldByName("dict").FieldByName("hist").Len()
	runes := bytes.Repeat([]byte{0xe9}, 511)
	converted := make([]rune, len(runes))
	for i, b := range runes {
		converted[i] = rune(b)
	}
	var (
		text      string
		made      reflect.Value
		block     []byte
		links     []uint32
		linkSlice [][]uint32
		buffered  *bufio.Reader
		rune511   []rune
	)
	worstCopy := 0
	for n := 1; n <= 16; n++ {
		piece := runes[:n]
		worstCopy = max(worstCopy, allocatedBy(func() { text = string(piece) }))
	}
	literalTables, distanceTables := mostLinkTables(286, 15), mostLinkTables(30, 15)
	for _, c := range []struct {
		name      string
		got, want int
	}{
		{"a copy of a name of up to 16 bytes", worstCopy, nameCopy},
		{"a copy of a value of up to 16 bytes", worstCopy, valueCopy},
		{"an element of the slice of a header's values", int(reflect.TypeFor[string]().Size()), valueSlice},
		{"a gzip.Reader", allocatedBy(func() { made = reflect.New(reflect.TypeFor[gzip.Reader]()) }), gzipReaderStruct},
		{"a bufio.Reader", allocatedBy(func() { made = reflect.New(reflect.TypeFor[bufio.Reader]()) }), gzipBufioStruct},
		{"a bufio.Reader of the default buffer, with its buffer", allocatedBy(func() { buffered = bufio.NewReader(plainReader{}) }), gzipBufioStruct + gzipBufioBuffer},
		{"flate's decompressor", allocatedBy(func() { made = reflect.New(decompressor) }), gzipDecompressor},
		{"the lengths of its two codes and of the code of lengths", allocatedBy(func() { made = reflect.New(pointee("bits")) }) + allocatedBy(func() { made = reflect.New(pointee("codebits")) }), gzipBitsTables},
		{"flate's window", allocatedBy(func() { block = make([]byte, window) }), gzipWindow},
		{"a table of the links of a prefix, of 64 entries", allocatedBy(func() { links = make([]uint32, 1<<6) }), gzipLinkTable},
		{"the prefixes of the literal and length code and of the distance code", literalTables + distanceTables, gzipLinkTables},
		{"the slices of their tables", allocatedBy(func() { linkSlice = make([][]uint32, literalTables) }) + allocatedBy(func() { linkSlice = make([][]uint32, distanceTables) }), gzipLinkSlices},
		{"the extra field of a gzip header", allocatedBy(func() { block = make([]byte, 65535) }), gzipExtra},
		{"a name of 511 Latin-1 bytes, as a string of 1,022 bytes", allocatedBy(func() { text = string(converted) }), gzipName},
		{"a comment of 511 Latin-1 bytes", allocatedBy(func() { text = string(converted) }), gzipComment},
		{"a slice of 511 runes", allocatedBy(func() { rune511 = make([]rune, 0, len(runes)) }), gzipRunes},
		{"the transport's wrapper of the gzip reader", theWrapperOfTheGzipReaderTakes(t), gzipWrapper},
	} {
		if c.got != c.want {
			t.Errorf("%s takes %d bytes, want %d", c.name, c.got, c.want)
		}
	}
	if window != gzipWindow || len(text) != 1022 || cap(rune511) != 511 {
		t.Errorf("flate's window is %d bytes, a name's string %d and a slice of runes %d, want %d, 1,022 and 511", window, len(text), cap(rune511), gzipWindow)
	}
	runtime.KeepAlive([]any{made, block, links, linkSlice, buffered})
}

func TestTheHpackBufferIsWhatABytesBufferGrowsToByFramesWhileAFieldIsUnfinished(t *testing.T) {
	sizedForTheBounds(t)
	var saved bytes.Buffer
	for saved.Len() <= hpackSaved {
		saved.Write(make([]byte, frameSize))
	}
	if saved.Cap() != hpackBuffer {
		t.Errorf("a buffer that takes frames of %d bytes until it holds more than the %d an unfinished field may take is %d bytes, want hpackBuffer, %d", frameSize, hpackSaved, saved.Cap(), hpackBuffer)
	}
}

func mostLinkTables(symbols, longest int) int {
	most := 0
	for prefixes := 1; prefixes < 512; prefixes++ {
		leaves := 2*prefixes + longest - 10
		if short := symbols - leaves; short >= bits.OnesCount(uint(512-prefixes)) {
			most = prefixes
		}
	}
	return most
}

func TestACompleteCodeHasAtMostAsManyLongPrefixesAsTheGzipTermCounts(t *testing.T) {
	for _, c := range []struct {
		name    string
		symbols int
		tables  int
	}{
		{"the literal and length code", 286, 137},
		{"the distance code", 30, 8},
	} {
		t.Run(c.name, func(t *testing.T) {
			largest := 0
			for longest := 10; longest <= 15; longest++ {
				largest = max(largest, mostLinkTables(c.symbols, longest)<<(longest-9))
			}
			if got := mostLinkTables(c.symbols, 15); got != c.tables || largest != c.tables<<6 {
				t.Errorf("a code over %d symbols has at most %d prefixes of 64 entries, and %d entries at the most of any longest length, want %d and %d", c.symbols, got, largest, c.tables, c.tables<<6)
			}
		})
	}
}

func roundedUp(x float64, decimals int) float64 {
	scale := math.Pow(10, float64(decimals))
	return math.Ceil(x*scale-1e-9) / scale
}

func TestEveryFigureOfTheDerivationIsTheSumOfItsTermsRoundedUp(t *testing.T) {
	const gibibyte = 1 << 30
	pass := func(p protocol) uint64 { return readsBound(p, 2) + summariesAtMost }
	for _, c := range []struct {
		name     string
		got      float64
		decimals int
		want     float64
	}{
		{"a read over HTTP/2, MiB", mib(readsBound(http2, 1)), 1, 28.5},
		{"a read over HTTP/1.1, MiB", mib(readsBound(http1, 1)), 1, 25.5},
		{"a read over HTTP/2 apart from its connection, MiB", mib(readsBound(http2, 1) - connectionHTTP2), 1, 26.2},
		{"a read over HTTP/1.1 apart from its connection, MiB", mib(readsBound(http1, 1) - connectionHTTP1), 1, 23.5},
		{"a pass of health-nodes over HTTP/2, MiB", mib(pass(http2)), 1, 60.9},
		{"a pass of health-nodes over HTTP/1.1, MiB", mib(pass(http1)), 1, 54.9},
		{"a pass of health over a held restore over HTTP/2, MiB", mib(readsBound(http2, 7)), 1, 198.9},
		{"a pass of health over a held restore over HTTP/1.1, MiB", mib(readsBound(http1, 7)), 1, 177.9},
		{"100 workers of health-nodes over HTTP/2, GiB", 100 * float64(pass(http2)) / gibibyte, 2, 5.94},
		{"10 workers of health over HTTP/2, GiB", 10 * float64(readsBound(http2, 7)) / gibibyte, 2, 1.95},
		{"100 passes of health-nodes over HTTP/1.1, GiB", 100 * float64(pass(http1)) / gibibyte, 1, 5.4},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := roundedUp(c.got, c.decimals); math.Abs(got-c.want) > 1e-9 {
				t.Errorf("the terms add to %.4f, which rounded up is %.*f, want %.*f", c.got, c.decimals, got, c.decimals, c.want)
			}
		})
	}
}

func TestAMapOfHeadersHoldsAtMostASlotOfMapSlotBytesForEachKeyAndOneTableMore(t *testing.T) {
	sizedForTheBounds(t)
	for _, keys := range []int{fieldsOfABudget, announcedNamesHTTP1} {
		names := make([]string, keys)
		for i := range names {
			names[i] = strconv.Itoa(i)
		}
		var most int64
		for range 100 {
			base := liveHeap()
			header := make(http.Header)
			for _, name := range names {
				header[name] = nil
			}
			most = max(most, int64(liveHeap())-int64(base))
			runtime.KeepAlive(header)
		}
		t.Logf("a map of %d keys held at the most of 100, %d bytes, %.1f a key", keys, most, float64(most)/float64(keys))
		if bound := int64(keys*mapSlot + mapTable); most > bound {
			t.Errorf("a map of %d keys held %d bytes, want at most %d: %d a key and a table of %d slots", keys, most, bound, mapSlot, 1024)
		}
		if least := int64(keys * 41); most < least {
			t.Errorf("a map of %d keys held %d bytes at the most of 100, want at least a slot of 41 bytes a key, %d", keys, most, least)
		}
	}
}
