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
	"runtime"
	"slices"
	"testing"
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

type measuringReader struct {
	data  []byte
	atEnd func()
}

func (r *measuringReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := min(len(p), len(r.data))
	if len(r.data) <= 8<<10 {
		n = 1
	}
	if len(r.data) == 1 && r.atEnd != nil {
		r.atEnd()
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

const noise = 32 << 10

func typicalOf(measure func() int64) int64 {
	var all []int64
	for range 15 {
		all = append(all, measure())
	}
	slices.Sort(all)
	return all[len(all)/2]
}

func heldByAGzipReader(t *testing.T, stream []byte) int64 {
	t.Helper()
	return typicalOf(func() int64 {
		var source io.Reader = plainReader{bytes.NewReader(stream)}
		one := make([]byte, 1)
		base := liveHeap()
		reader, err := gzip.NewReader(source)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(reader, one); err != nil {
			t.Fatal(err)
		}
		now := liveHeap()
		runtime.KeepAlive([]any{reader, source, one})
		return int64(now) - int64(base)
	})
}

func TestAGzipReaderThatHasReadHoldsTheSumOfItsPartsOnTheWorstStream(t *testing.T) {
	for _, c := range []struct {
		name   string
		stream []byte
		parts  int
	}{
		{"one byte from gzip.Writer", gzipOfAByte(t), gzipFixedParts},
		{"one byte, a code of 137 and 8 link tables", craftedGzip(false), gzipFixedParts + gzipLinks},
		{"one byte, the same code, an extra field, a name and a comment", craftedGzip(true), gzipFixedParts + gzipLinks + gzipHeader},
	} {
		t.Run(c.name, func(t *testing.T) {
			held := heldByAGzipReader(t, c.stream)
			t.Logf("a gzip.Reader that has read holds %d bytes, its parts add to %d, gzipReader is %d", held, c.parts, gzipReader)
			if held < int64(c.parts)-noise || held > int64(c.parts)+noise {
				t.Errorf("a gzip.Reader holds %d bytes, want the %d of its parts within %d", held, c.parts, noise)
			}
		})
	}
}

func TestAGzipReaderThatIsResetHoldsNothingOfTheStreamItReadBefore(t *testing.T) {
	crafted, plain := craftedGzip(true), gzipOfAByte(t)
	held := typicalOf(func() int64 {
		var first, second io.Reader = plainReader{bytes.NewReader(crafted)}, plainReader{bytes.NewReader(plain)}
		one := make([]byte, 1)
		base := liveHeap()
		reader, err := gzip.NewReader(first)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(reader, one); err != nil {
			t.Fatal(err)
		}
		if err := reader.Reset(second); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(reader, one); err != nil {
			t.Fatal(err)
		}
		now := liveHeap()
		runtime.KeepAlive([]any{reader, first, second, one})
		return int64(now) - int64(base)
	})
	if want := int64(gzipFixedParts); held < want-noise || held > want+noise {
		t.Errorf("a gzip.Reader reset to a stream of one byte holds %d bytes, want the %d of its fixed parts within %d", held, want, noise)
	}
}

func TestAGzipReaderThatReadsALaterMembersHeaderHoldsItBesideTheFirstAndItsTables(t *testing.T) {
	stream := append(craftedGzip(true), gzipHeaderOf(true)...)
	held := typicalOf(func() int64 {
		var base, seen int64
		source := &measuringReader{data: slices.Clone(stream), atEnd: func() { seen = int64(liveHeap()) - base }}
		one := make([]byte, 1)
		base = int64(liveHeap())
		reader, err := gzip.NewReader(source)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(reader, one); err != nil {
			t.Fatal(err)
		}
		runtime.KeepAlive([]any{reader, source, one})
		return seen
	})
	want := int64(gzipFixedParts + gzipLinks + gzipHeader + gzipExtra + gzipName)
	t.Logf("a gzip.Reader that reads the header of a second member holds %d bytes of its own up to the last byte of it, its parts up to there add to %d, gzipParts less the wrapper is %d, gzipReader is %d", held, want, gzipParts-gzipWrapper, gzipReader)
	if held < want-noise || held > want+noise {
		t.Errorf("a gzip.Reader holds %d bytes, want the %d of its parts within %d", held, want, noise)
	}
}

func TestTheTenantClientsHoldAGzipReaderOfTheWorstStreamUnderGzipReader(t *testing.T) {
	stream := craftedGzip(true)
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			client, url := tenantClient(t, p, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
		})
	}
}
