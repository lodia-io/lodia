// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"math"
	"net/http"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	mapGroup       = 8 + 8*(16+24)
	mapTableStruct = 32
	mapStruct      = 48
	mapSplit       = 897
	mapTail        = 1e-12

	liveHeapNoise = 256

	headFixed               = len("HTTP/1.1 200 OK\nContent-Type: application/json\nTransfer-Encoding: chunked\n") + len("Trailer: ") + len("\n\n")
	headKeysOutsideTheLines = 2
)

var sizeClasses = [...]int{
	8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640,
	704, 768, 896, 1024, 1152, 1280, 1408, 1536, 1792, 2048, 2304, 2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912,
	8192, 9472, 9728, 10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264, 28672, 32768,
}

func sizeClass(n int) int {
	if n > sizeClasses[len(sizeClasses)-1] {
		return (n + 8191) / 8192 * 8192
	}
	return sizeClasses[sort.SearchInts(sizeClasses[:], n)]
}

func tableBytes(slots int) int { return sizeClass(slots / 8 * mapGroup) }

func alignUpPow2(n int) int {
	p := 1
	for p < n {
		p *= 2
	}
	return p
}

func binomialAtLeast(n int, p float64, t int) float64 {
	if t <= 0 {
		return 1
	}
	if t > n || p <= 0 {
		return 0
	}
	mean, sd := float64(n)*p, math.Sqrt(float64(n)*p*(1-p))
	if n > 64 && float64(t) < mean-12*sd {
		return 1
	}
	if n > 64 && float64(t) > mean+45*sd {
		return 0
	}
	logP, logQ := math.Log(p), math.Log1p(-p)
	logTerm := func(k int) float64 {
		a, _ := math.Lgamma(float64(n + 1))
		b, _ := math.Lgamma(float64(k + 1))
		c, _ := math.Lgamma(float64(n - k + 1))
		return a - b - c + float64(k)*logP + float64(n-k)*logQ
	}
	first := logTerm(t)
	sum := 1.0
	for k := t + 1; k <= n; k++ {
		term := math.Exp(logTerm(k) - first)
		sum += term
		if float64(k) > mean && term < 1e-18 {
			break
		}
	}
	return math.Min(1, math.Exp(first)*sum)
}

func exceedersAtMost(n int, q, tail float64) int {
	for m := range n {
		if binomialAtLeast(n, q, m+1) <= tail {
			return m
		}
	}
	return n
}

func mapBytesAtMost(keys, hint int) int {
	if keys == 0 && hint <= 8 {
		return mapStruct
	}
	initial := 1
	var slots int
	if hint > 8 {
		target := hint * 8 / 7
		initial = alignUpPow2((target + 1023) / 1024)
		slots = alignUpPow2(max(8, target/initial))
	} else {
		if keys <= 8 {
			return mapStruct + sizeClass(mapGroup)
		}
		single := 16
		for single < 1024 && keys > single*7/8 {
			single *= 2
		}
		if keys <= 896 {
			return mapStruct + tableBytes(single) + mapTableStruct + sizeClass(8)
		}
		slots = 1024
	}
	depth0 := alignLog2(initial)
	split := map[int]int{}
	for j := depth0; ; j++ {
		q := binomialAtLeast(keys, math.Pow(2, -float64(j)), mapSplit)
		m := min(exceedersAtMost(1<<j, q, mapTail), keys/mapSplit)
		if j == 0 && keys >= mapSplit {
			m = 1
		}
		if m == 0 {
			break
		}
		split[j] = m
	}
	splits, deepest := 0, depth0
	for j, m := range split {
		splits += m
		deepest = max(deepest, j+1)
	}
	tables := initial + splits
	unsplit := initial - split[depth0]
	count := keys
	if initial > 1 {
		count = keys/initial + 6*int(math.Sqrt(float64(keys/initial)))
	}
	for slots < 1024 && count > slots*7/8 {
		slots *= 2
	}
	return mapStruct + sizeClass(8<<deepest) + tables*mapTableStruct + unsplit*tableBytes(slots) + (tables-unsplit)*tableBytes(1024)
}

func alignLog2(n int) int {
	d := 0
	for 1<<d < n {
		d++
	}
	return d
}

func headBytesAtMost(fieldLines, announced, announcedBytes int) int {
	keys := fieldLines + headKeysOutsideTheLines
	hint := min(keys, 1000)
	header := mapBytesAtMost(keys, hint) + sizeClass(valueSlice*hint) + valueSlice*max(0, keys-1000) + nameCopy*keys
	trailer := mapBytesAtMost(announced, 0) + nameCopy*announced + announcedBytes
	return header + trailer
}

func trailersBytesAtMost(fields int) int {
	hint := min(fields, 1000)
	return mapBytesAtMost(fields, hint) + sizeClass(valueSlice*hint) + valueSlice*max(0, fields-1000) + mapBytesAtMost(fields, 0) + nameCopy*fields
}

func announcedBytes(names int) int {
	bytes := 0
	for _, c := range []struct{ most, cost int }{{oneByteNames, 2}, {65536, 3}, {math.MaxInt, 4}} {
		take := min(names, c.most)
		bytes += take * c.cost
		names -= take
	}
	return bytes
}

func fieldLinesIn(room int) int {
	lines := 0
	for _, c := range []struct{ most, cost int }{{oneByteFieldNames, 3}, {twoByteFieldNames, 4}, {math.MaxInt, 5}} {
		take := min(room/c.cost, c.most)
		lines += take
		room -= take * c.cost
		if take < c.most {
			break
		}
	}
	return lines
}

type dearestHead struct{ fieldLines, announced, bytes int }

func theDearestHead() dearestHead {
	var most dearestHead
	for announced := 0; ; announced++ {
		cost := announcedBytes(announced)
		room := maxResponseHeaderBytes - headFixed - cost
		if room < 0 {
			return most
		}
		lines := fieldLinesIn(room)
		if bytes := headBytesAtMost(lines, announced, cost); bytes > most.bytes {
			most = dearestHead{lines, announced, bytes}
		}
	}
}

type realizedHead struct {
	text                                string
	fieldLines, announced, announcedLen int
}

func theDearestHeadOf(prefix, lineBreak string) realizedHead {
	most := theDearestHead()
	names := announcedNamesOfAHead(maxResponseHeaderBytes)
	names = names[:min(len(names), most.announced)]
	value := strings.Join(names, ",")
	room := maxResponseHeaderBytes - len(prefix) - len("Trailer: ") - len(value) - len(lineBreak)
	lines := canonicalFieldNames(room)
	lines = lines[:min(len(lines), most.fieldLines)]
	fields := ""
	if len(lines) > 0 {
		fields = strings.Join(lines, ":\n") + ":\n"
	}
	return realizedHead{prefix + fields + "Trailer: " + value + lineBreak, len(lines), len(names), len(value)}
}

func TestTheSizeClassesTheMapsAreBuiltFromAreTheAllocators(t *testing.T) {
	for _, n := range append([]int{1, 9, 100, 1000, 32769, 41984, 49153, 98304}, sizeClasses[:]...) {
		for _, size := range []int{n - 1, n, n + 1} {
			if size < 16 {
				continue
			}
			var made []byte
			if got, want := allocatedBy(func() { made = make([]byte, size) }), sizeClass(size); got != want {
				t.Errorf("an object of %d bytes takes %d, want its size class, %d", size, got, want)
			}
			runtime.KeepAlive(made)
		}
	}
	if got, want := tableBytes(1024), 49152; got != want {
		t.Errorf("a table of 1,024 slots takes %d bytes, want %d", got, want)
	}
}

func mapSplitPoints(hint func(keys int) int) []int {
	points := map[int]bool{}
	last := -1
	for keys := 1; keys <= announcedNamesHTTP1; keys++ {
		if bytes := mapBytesAtMost(keys, hint(keys)); bytes != last {
			points[keys-1], points[keys] = true, true
			last = bytes
		}
	}
	var out []int
	for keys := range points {
		if keys > 0 {
			out = append(out, keys)
		}
	}
	sort.Ints(out)
	return out
}

func TestAMapOfHeadersHoldsAtMostWhatItsTablesTakeAtEverySplitPointAndOneKeyOnEachSideOfIt(t *testing.T) {
	sizedForTheBounds(t)
	names := make([]string, announcedNamesHTTP1)
	for i := range names {
		names[i] = strconv.Itoa(i)
	}
	for _, c := range []struct {
		name string
		hint func(keys int) int
	}{
		{"made with no hint", func(int) int { return 0 }},
		{"made with the hint of the keys it gets, at most 1,000", func(keys int) int { return min(keys, 1000) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, keys := range mapSplitPoints(c.hint) {
				var held []int64
				for range 5 {
					base := liveHeap()
					header := make(http.Header, c.hint(keys))
					for _, name := range names[:keys] {
						header[name] = nil
					}
					held = append(held, int64(liveHeap())-int64(base))
					runtime.KeepAlive(header)
				}
				slices.Sort(held)
				bound := int64(mapBytesAtMost(keys, c.hint(keys)))
				if held[2] > bound+liveHeapNoise {
					t.Errorf("a map of %d keys held %d bytes, the median of %v, want at most %d", keys, held[2], held, bound)
				}
				if keys > 8 && keys < mapSplit && held[2] < bound-mapStruct-liveHeapNoise {
					t.Errorf("a map of %d keys held %d bytes, the median of %v, want the %d its tables take, to the map's own %d bytes", keys, held[2], held, bound, mapStruct)
				}
			}
		})
	}
}
