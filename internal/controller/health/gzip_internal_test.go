// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"hash/crc32"
	"io"
	"net/http"
	"reflect"
	"runtime"
	"testing"
	"unsafe"
	"weak"
)

type bitWriter struct {
	out   []byte
	acc   uint64
	nbits uint
}

func (w *bitWriter) bits(v uint64, n uint) {
	w.acc |= v << w.nbits
	w.nbits += n
	for w.nbits >= 8 {
		w.out = append(w.out, byte(w.acc))
		w.acc >>= 8
		w.nbits -= 8
	}
}

func (w *bitWriter) code(code uint64, n uint) {
	var reversed uint64
	for i := range n {
		reversed = reversed<<1 | (code>>i)&1
	}
	w.bits(reversed, n)
}

func (w *bitWriter) flush() []byte {
	if w.nbits > 0 {
		w.out = append(w.out, byte(w.acc))
	}
	return w.out
}

func canonicalCodes(lengths []int) []uint64 {
	var count [16]int
	for _, l := range lengths {
		if l > 0 {
			count[l]++
		}
	}
	var next [16]uint64
	code := uint64(0)
	for l := 1; l < 16; l++ {
		code = (code + uint64(count[l-1])) << 1
		if l == 1 {
			code = 0
		}
		next[l] = code
	}
	codes := make([]uint64, len(lengths))
	for s, l := range lengths {
		if l > 0 {
			codes[s] = next[l]
			next[l]++
		}
	}
	return codes
}

func completeLengths(symbols int, short, shortLengths []int, prefixes int) []int {
	lengths := make([]int, symbols)
	used := map[int]bool{}
	for i, s := range short {
		lengths[s] = shortLengths[i]
		used[s] = true
	}
	var rest []int
	for s := range symbols {
		if !used[s] {
			rest = append(rest, s)
		}
	}
	tens := 1 + 2*(prefixes-1)
	for i := range tens {
		lengths[rest[i]] = 10
	}
	for i, l := range []int{11, 12, 13, 14, 15, 15} {
		lengths[rest[tens+i]] = l
	}
	return lengths
}

func craftedBlock() []byte {
	literals := completeLengths(286, []int{97, 256, 0, 1, 2, 3, 4}, []int{1, 3, 4, 5, 7, 8, 9}, 137)
	distances := completeLengths(30, []int{0, 1, 2, 3, 4, 5}, []int{1, 2, 3, 4, 5, 6}, 8)
	var w bitWriter
	w.bits(1, 1)
	w.bits(2, 2)
	w.bits(286-257, 5)
	w.bits(30-1, 5)
	w.bits(19-4, 4)
	order := []int{16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15}
	lengthCode := make([]int, 19)
	for s := range 16 {
		lengthCode[s] = 4
	}
	for _, s := range order {
		w.bits(uint64(lengthCode[s]), 3)
	}
	lengthCodes := canonicalCodes(lengthCode)
	for _, l := range append(append([]int{}, literals...), distances...) {
		w.code(lengthCodes[l], 4)
	}
	literalCodes := canonicalCodes(literals)
	w.code(literalCodes[97], uint(literals[97]))
	w.code(literalCodes[256], uint(literals[256]))
	return w.flush()
}

func gzipHeaderOf(fields bool) []byte {
	flags := byte(0)
	if fields {
		flags = 4 | 8 | 16
	}
	out := []byte{0x1f, 0x8b, 8, flags, 0, 0, 0, 0, 0, 255}
	if fields {
		out = append(out, 0xff, 0xff)
		out = append(out, bytes.Repeat([]byte{'x'}, 65535)...)
		out = append(out, bytes.Repeat([]byte{0xe9}, 511)...)
		out = append(out, 0)
		out = append(out, bytes.Repeat([]byte{0xe9}, 511)...)
		out = append(out, 0)
	}
	return out
}

func gzipTrailerOfA() []byte {
	var out [8]byte
	binary.LittleEndian.PutUint32(out[:4], crc32.ChecksumIEEE([]byte("a")))
	binary.LittleEndian.PutUint32(out[4:], 1)
	return out[:]
}

func craftedGzip(fields bool) []byte {
	return bytes.Join([][]byte{gzipHeaderOf(fields), craftedBlock(), gzipTrailerOfA()}, nil)
}

func gzipOfAByte(t *testing.T) []byte {
	t.Helper()
	var out bytes.Buffer
	writer := gzip.NewWriter(&out)
	if _, err := writer.Write([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

type plainReader struct{ io.Reader }

func allocatedByAGzipReader(t *testing.T, stream []byte, alongside func()) int {
	t.Helper()
	source := bytes.NewReader(stream)
	var reader io.Reader = plainReader{source}
	one := make([]byte, 1)
	var made *gzip.Reader
	got := allocatedBy(func() {
		source.Reset(stream)
		var err error
		if made, err = gzip.NewReader(reader); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(made, one); err != nil {
			t.Fatal(err)
		}
		if alongside != nil {
			alongside()
		}
	})
	runtime.KeepAlive(made)
	return got
}

func mallocsOf(stream []byte) uint64 {
	least := ^uint64(0)
	for range 7 {
		var before, after runtime.MemStats
		one := make([]byte, 1)
		source := plainReader{bytes.NewReader(stream)}
		runtime.GC()
		runtime.ReadMemStats(&before)
		reader, err := gzip.NewReader(source)
		if err != nil {
			panic(err)
		}
		if _, err := io.ReadFull(reader, one); err != nil {
			panic(err)
		}
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(reader)
		least = min(least, after.Mallocs-before.Mallocs)
	}
	return least
}

func TestTheCraftedGzipStreamsMakeTheTablesAndTheHeaderTheirPartsAssume(t *testing.T) {
	if got, want := mallocsOf(craftedGzip(false))-mallocsOf(gzipOfAByte(t)), uint64(gzipLinkTables+2); got != want {
		t.Errorf("a reader of the crafted block makes %d allocations more than one of a stream of gzip.Writer, want %d: a table for each of the %d prefixes and a slice of them for each of two codes", got, want, gzipLinkTables)
	}
	reader, err := gzip.NewReader(bytes.NewReader(craftedGzip(true)))
	if err != nil {
		t.Fatal(err)
	}
	if len(reader.Extra) != 1<<16-1 || len(reader.Name) != 1022 || len(reader.Comment) != 1022 {
		t.Errorf("the header of the crafted stream has an extra field of %d bytes, a name of %d and a comment of %d, want 65,535, 1,022 and 1,022", len(reader.Extra), len(reader.Name), len(reader.Comment))
	}
}

func TestAGzipReaderAllocatesExactlyTheSumOfItsPartsAndTheRunesItConvertsOnEachWorstStream(t *testing.T) {
	for _, c := range []struct {
		name   string
		stream []byte
		want   int
	}{
		{"one byte from gzip.Writer", gzipOfAByte(t), gzipFixedParts},
		{"one byte, a code of 137 and 8 link tables", craftedGzip(false), gzipFixedParts + gzipLinks},
		{"one byte, the same code, an extra field, a name and a comment", craftedGzip(true), gzipFixedParts + gzipLinks + gzipHeader + 2*gzipRunes},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := allocatedByAGzipReader(t, c.stream, nil)
			t.Logf("a gzip.Reader allocates %d bytes up to its first byte, its parts and the runes of its conversions add to %d, gzipReader is %d", got, c.want, gzipReader)
			if got != c.want {
				t.Errorf("a gzip.Reader allocates %d bytes up to its first byte, want the %d of its parts and of the slices of runes a name and a comment are converted through", got, c.want)
			}
		})
	}
}

func TestAGzipReaderThatReadsALaterMembersHeaderAllocatesExactlyGzipPartsAndTheRunesOfItsThreeEarlierConversions(t *testing.T) {
	wrapper := theWrapperOfTheGzipReader(t)
	var made reflect.Value
	got := allocatedByAGzipReader(t, append(craftedGzip(true), gzipHeaderOf(true)...), func() { made = reflect.New(wrapper) })
	runtime.KeepAlive(made)
	want := gzipParts + 3*gzipRunes
	t.Logf("a gzip.Reader and the wrapper of the transport that read a second member's header allocate %d bytes, gzipParts is %d and three slices of runes are %d, gzipReader is %d", got, gzipParts, 3*gzipRunes, gzipReader)
	if got != want {
		t.Errorf("a gzip.Reader and its wrapper allocate %d bytes, want the %d of gzipParts and of the slices of runes the first header's name and comment and the second header's name are converted through", got, want)
	}
}

func weakTo(parts []weak.Pointer[byte], p unsafe.Pointer) []weak.Pointer[byte] {
	if p == nil {
		return parts
	}
	return append(parts, weak.Make((*byte)(p)))
}

func weakPartsOf(reader *gzip.Reader) []weak.Pointer[byte] {
	var parts []weak.Pointer[byte]
	parts = weakTo(parts, unsafe.Pointer(unsafe.SliceData(reader.Extra)))
	parts = weakTo(parts, unsafe.Pointer(unsafe.StringData(reader.Name)))
	parts = weakTo(parts, unsafe.Pointer(unsafe.StringData(reader.Comment)))
	value := reflect.ValueOf(reader).Elem()
	buffered := value.FieldByName("r").Elem()
	parts = weakTo(parts, buffered.UnsafePointer())
	parts = weakTo(parts, buffered.Elem().FieldByName("buf").UnsafePointer())
	decompressor := value.FieldByName("decompressor").Elem().Elem()
	for _, name := range []string{"h1", "h2"} {
		links := decompressor.FieldByName(name).FieldByName("links")
		parts = weakTo(parts, links.UnsafePointer())
		for i := range links.Len() {
			parts = weakTo(parts, links.Index(i).UnsafePointer())
		}
	}
	return parts
}

func TestAGzipReaderThatIsResetHoldsNothingOfTheStreamItReadBefore(t *testing.T) {
	reader, err := gzip.NewReader(plainReader{bytes.NewReader(craftedGzip(true))})
	if err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 1)
	if _, err := io.ReadFull(reader, one); err != nil {
		t.Fatal(err)
	}
	before := weakPartsOf(reader)
	if want := 3 + 2 + 2 + gzipLinkTables; len(before) != want {
		t.Fatalf("the reader of the crafted stream has %d parts of that stream to watch, want its extra field, name and comment, its buffered reader and the buffer of it, the slices of the links of its two codes and their %d tables, %d", len(before), gzipLinkTables, want)
	}
	runtime.GC()
	for i, part := range before {
		if part.Value() == nil {
			t.Fatalf("part %d of the stream was collected while the reader holds it", i)
		}
	}
	if err := reader.Reset(plainReader{bytes.NewReader(gzipOfAByte(t))}); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(reader, one); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	runtime.GC()
	kept := 0
	for _, part := range before {
		if part.Value() != nil {
			kept++
		}
	}
	if kept != 0 {
		t.Errorf("a gzip.Reader reset to a stream of one byte holds %d of the %d parts of the stream it read before, want none", kept, len(before))
	}
	runtime.KeepAlive(reader)
}

func TestTheTenantClientHoldsAGzipReaderOfTheWorstStreamUnderGzipReader(t *testing.T) {
	stream := craftedGzip(true)
	client, url := tenantClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(stream)
	}))
	held := medianOf(func() int64 {
		base := liveHeap()
		resp, err := client.Get(url + nodeLeasesPath)
		if err != nil {
			t.Fatal(err)
		}
		one := make([]byte, 1)
		if _, err := io.ReadFull(resp.Body, one); err != nil || one[0] != 'a' {
			t.Fatalf("read %q, %v", one, err)
		}
		now := liveHeap()
		runtime.KeepAlive(resp)
		_ = resp.Body.Close()
		return int64(now) - int64(base)
	})
	floor := int64(gzipFixedParts + gzipLinks + gzipHeader)
	t.Logf("a response read by one byte holds %d bytes, a gzip.Reader of the worst stream %d, gzipReader is %d", held, floor, gzipReader)
	if held < floor || held > gzipReader {
		t.Errorf("a response of the worst gzip stream holds %d bytes once read by one byte, want %d to %d", held, floor, gzipReader)
	}
}
