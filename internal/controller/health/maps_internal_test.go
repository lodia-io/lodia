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
	"sync"
	"testing"
)

const (
	mapGroup       = 8 + 8*(16+24)
	mapTableStruct = 32
	mapStruct      = 48
	mapSplit       = 897
	mapTail        = 1e-14

	liveHeapNoise = 2048
	mapTrials     = 9

	headFixed               = len("HTTP/1.1 200 \nTransfer-Encoding:chunked\nTrailer:") + len("\n\n")
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

func alignLog2(n int) int {
	d := 0
	for 1<<d < n {
		d++
	}
	return d
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

func unionAtLeast(n int, q float64, k int) float64 {
	if k <= 0 {
		return 1
	}
	if k > n || q <= 0 {
		return 0
	}
	a, _ := math.Lgamma(float64(n + 1))
	b, _ := math.Lgamma(float64(k + 1))
	c, _ := math.Lgamma(float64(n - k + 1))
	return math.Min(1, math.Exp(a-b-c+float64(k)*math.Log(q)))
}

func exceedersAtMost(n int, q, tail float64) int {
	for m := range n {
		if unionAtLeast(n, q, m+1) <= tail {
			return m
		}
	}
	return n
}

type hintShape struct{ initial, slots int }

func shapeOfHint(hint int) hintShape {
	if hint <= 8 {
		return hintShape{}
	}
	target := hint * 8 / 7
	initial := alignUpPow2((target + 1023) / 1024)
	return hintShape{initial, alignUpPow2(max(8, target/initial))}
}

type splitDepth struct {
	depth, counted int
	q              float64
}

func splitDepths(keys, depth0 int, tail float64) []splitDepth {
	var out []splitDepth
	for j := depth0; j < 24; j++ {
		q := binomialAtLeast(keys, math.Pow(2, -float64(j)), mapSplit)
		if q == 0 {
			break
		}
		m := min(exceedersAtMost(1<<j, q, tail), keys/mapSplit)
		if j == 0 && keys >= mapSplit {
			m = 1
		}
		out = append(out, splitDepth{j, m, q})
	}
	return out
}

type mapKey struct {
	keys, initial, slots int
	tail                 float64
}

var mapMemo = struct {
	sync.Mutex
	bytes map[mapKey]int
}{bytes: map[mapKey]int{}}

func mapBytesAtMost(keys, hint int) int { return mapBytesAtTail(keys, hint, mapTail) }

func mapBytesAtTail(keys, hint int, tail float64) int {
	shape := shapeOfHint(hint)
	if shape.initial == 0 && keys > 896 || shape.initial == 1 && keys >= mapSplit {
		shape = hintShape{1, 1024}
	}
	key := mapKey{keys, shape.initial, shape.slots, tail}
	mapMemo.Lock()
	bytes, ok := mapMemo.bytes[key]
	mapMemo.Unlock()
	if !ok {
		bytes = mapBytesOfShape(keys, shape, tail)
		mapMemo.Lock()
		mapMemo.bytes[key] = bytes
		mapMemo.Unlock()
	}
	return bytes
}

func mapBytesOfShape(keys int, shape hintShape, tail float64) int {
	initial, slots := shape.initial, shape.slots
	if initial == 0 {
		if keys == 0 {
			return mapStruct
		}
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
		initial, slots = 1, 1024
	}
	depth0 := alignLog2(initial)
	split := map[int]int{}
	for _, d := range splitDepths(keys, depth0, tail) {
		if d.counted == 0 {
			break
		}
		split[d.depth] = d.counted
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

func elementsOf(keys, hint int) int {
	if hint == 0 {
		return valueSlice * keys
	}
	return sizeClass(valueSlice*hint) + valueSlice*max(0, keys-hint)
}

type hintSegment struct {
	shape     hintShape
	lo, hi    int
	slackiest int
}

var hintSegments = sync.OnceValue(func() []hintSegment {
	var out []hintSegment
	for hint := 0; hint <= 1000; hint++ {
		shape := shapeOfHint(hint)
		if len(out) == 0 || out[len(out)-1].shape != shape {
			out = append(out, hintSegment{shape: shape, lo: hint, slackiest: hint})
		}
		last := &out[len(out)-1]
		last.hi = hint
		if elementsSlack(hint) > elementsSlack(last.slackiest) {
			last.slackiest = hint
		}
	}
	return out
})

func elementsSlack(hint int) int {
	if hint == 0 {
		return 0
	}
	return sizeClass(valueSlice*hint) - valueSlice*hint
}

func worstHint(keys int, tail float64) (hint, mapBytes, elements int) {
	top := min(keys, 1000)
	best := -1
	try := func(h, bytes int) {
		if total := bytes + elementsOf(keys, h); total > best {
			best, hint, mapBytes, elements = total, h, bytes, elementsOf(keys, h)
		}
	}
	for _, segment := range hintSegments() {
		if segment.lo > top {
			break
		}
		bytes := mapBytesAtTail(keys, segment.lo, tail)
		if segment.hi <= top {
			try(segment.slackiest, bytes)
			continue
		}
		for h := segment.lo; h <= top; h++ {
			try(h, bytes)
		}
	}
	return hint, mapBytes, elements
}

func trailersReadAtMost(fields int) int {
	if fields == 0 {
		return 0
	}
	hint := min(fields, 1000)
	return mapBytesAtMost(fields, hint) + elementsOf(fields, hint) + nameCopy*fields
}

type headMix struct{ keys, announced, announcedBytes, trailers int }

type headParts struct{ hint, headerMap, elements, announcedMap, copies, value, trailers int }

func (p headParts) sum() int {
	return p.headerMap + p.elements + p.announcedMap + p.copies + p.value + p.trailers
}

func headPartsAtMost(m headMix) headParts { return headPartsAtTail(m, mapTail) }

func headPartsAtTail(m headMix, tail float64) headParts {
	hint, headerMap, elements := worstHint(m.keys, tail)
	value := 0
	if m.announcedBytes > 0 {
		value = sizeClass(m.announcedBytes - 1)
	}
	return headParts{
		hint:         hint,
		headerMap:    headerMap,
		elements:     elements,
		announcedMap: mapBytesAtTail(m.announced+m.trailers, 0, tail),
		copies:       nameCopy * (m.keys + m.announced),
		value:        value,
		trailers:     trailersReadAtMost(m.trailers),
	}
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

type countedHead struct {
	fieldLines, announced int
	parts                 headParts
}

func (h countedHead) bytes() int { return h.parts.sum() }

var headsMemo = struct {
	sync.Mutex
	counted map[float64][]countedHead
}{counted: map[float64][]countedHead{}}

func headsCountedAt(tail float64) []countedHead {
	headsMemo.Lock()
	defer headsMemo.Unlock()
	if heads, ok := headsMemo.counted[tail]; ok {
		return heads
	}
	var heads []countedHead
	for announced := 0; ; announced++ {
		cost := announcedBytes(announced)
		room := maxResponseHeaderBytes - headFixed - cost
		if room < 0 {
			headsMemo.counted[tail] = heads
			return heads
		}
		lines := fieldLinesIn(room)
		mix := headMix{keys: lines + headKeysOutsideTheLines, announced: announced, announcedBytes: cost, trailers: trailerFieldsHTTP1}
		heads = append(heads, countedHead{lines, announced, headPartsAtTail(mix, tail)})
	}
}

func theDearestHead() countedHead { return theDearestHeadAt(mapTail) }

func theDearestHeadAt(tail float64) countedHead {
	var most countedHead
	for _, head := range headsCountedAt(tail) {
		if head.bytes() > most.bytes() {
			most = head
		}
	}
	return most
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
	points := map[int]bool{0: true, 1: true, 8: true}
	last := -1
	for keys := 1; keys <= announcedNamesHTTP1; keys++ {
		if bytes := mapBytesAtMost(keys, hint(keys)); bytes != last {
			points[keys-1], points[keys] = true, true
			last = bytes
		}
	}
	var out []int
	for keys := range points {
		out = append(out, keys)
	}
	sort.Ints(out)
	return out
}

var mapSink http.Header

func allocationOfAMap(names []string, keys, hint int) int {
	return allocatedBy(func() {
		header := make(http.Header, hint)
		for _, name := range names[:keys] {
			header[name] = nil
		}
		mapSink = header
	})
}

func tablesAMapGrowsThrough(keys int) int {
	if keys <= 8 {
		return 0
	}
	garbage := sizeClass(mapGroup)
	for slots := 16; keys > slots*7/8 && slots < 1024; slots *= 2 {
		garbage += tableBytes(slots) + mapTableStruct
	}
	return garbage
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
				hint := c.hint(keys)
				bound := int64(mapBytesAtMost(keys, hint))
				if keys < mapSplit {
					want := int(bound)
					if hint == 0 {
						want += tablesAMapGrowsThrough(keys)
					}
					if got := allocationOfAMap(names, keys, hint); got != want {
						t.Errorf("a map of %d keys made with a hint of %d allocates %d bytes, want %d: the %d its tables take and the %d of the tables it grew through", keys, hint, got, want, bound, want-int(bound))
					}
				}
				var held []int64
				for range mapTrials {
					base := liveHeap()
					header := make(http.Header, hint)
					for _, name := range names[:keys] {
						header[name] = nil
					}
					held = append(held, int64(liveHeap())-int64(base))
					runtime.KeepAlive(header)
				}
				slices.Sort(held)
				median := held[mapTrials/2]
				if median > bound+liveHeapNoise {
					t.Errorf("a map of %d keys held %d bytes, the median of %v, want at most %d", keys, median, held, bound)
				}
				if keys > 8 && keys < mapSplit && median < bound-mapStruct-liveHeapNoise {
					t.Errorf("a map of %d keys held %d bytes, the median of %v, want the %d its tables take, to the map's own %d bytes", keys, median, held, bound, mapStruct)
				}
			}
		})
	}
}

func TestTheTailOfABinomialIsTheExactSumTheArgumentQuotes(t *testing.T) {
	for _, c := range []struct {
		trials int
		depth  int
		want   float64
	}{
		{5684, 3, 2.7218622682327275e-13},
		{3000, 2, 6.858138278973632e-10},
		{21831, 5, 8.173406719980120e-16},
		{14352, 4, 0.5048723029221396},
	} {
		got := binomialAtLeast(c.trials, math.Pow(2, -float64(c.depth)), mapSplit)
		if math.Abs(got-c.want) > c.want*1e-6 {
			t.Errorf("a prefix of %d bits of %d keys holds %d of them with chance %g, want %g, the exact sum", c.depth, c.trials, mapSplit, got, c.want)
		}
	}
}

func TestTheCountOfPrefixesThatHoldAMapIsTheUnionBoundNotTheBinomialQuantile(t *testing.T) {
	for _, c := range []struct {
		prefixes int
		q        float64
		want     int
		quantile int
	}{
		{16, 0.1, 14, 13},
		{32, 0.1, 20, 19},
		{32, 0.063096, 17, 16},
	} {
		if got := exceedersAtMost(c.prefixes, c.q, 1e-12); got != c.want {
			t.Errorf("%d prefixes of chance %g each: the union bound at 1e-12 counts %d, want %d", c.prefixes, c.q, got, c.want)
		}
		if got := unionAtLeast(c.prefixes, c.q, c.want+1); got > 1e-12 {
			t.Errorf("%d prefixes of chance %g each: %d of them have chance at most %g by the union bound, want at most 1e-12", c.prefixes, c.q, c.want+1, got)
		}
		if got := unionAtLeast(c.prefixes, c.q, c.want); got <= 1e-12 {
			t.Errorf("%d prefixes of chance %g each: %d of them have chance at most %g by the union bound, want over 1e-12", c.prefixes, c.q, c.want, got)
		}
		if got := binomialAtLeast(c.prefixes, c.q, c.quantile+1); got > 1e-12 {
			t.Errorf("%d prefixes of chance %g each: %d of them have chance %g, want the quantile %d at most 1e-12", c.prefixes, c.q, c.quantile+1, got, c.quantile)
		}
	}
}

func TestTheDearestHeadIsTheMixTheArgumentNamesAndItsPartsAreTheOnesItQuotes(t *testing.T) {
	dearest := theDearestHead()
	if dearest.fieldLines != 6738 || dearest.announced != 12711 || dearest.bytes() != 2857152 {
		t.Errorf("the dearest head is %d field lines and %d announced names of %d bytes, want 6,738, 12,711 and 2,857,152", dearest.fieldLines, dearest.announced, dearest.bytes())
	}
	want := headParts{hint: 897, headerMap: 737936, elements: 109872, announcedMap: 1525008, copies: 311216, value: 40960, trailers: 132160}
	if dearest.parts != want {
		t.Errorf("its parts are %+v, want %+v", dearest.parts, want)
	}
	for keys, tables := range map[int]int{896: 1, 897: 2, 449: 1, 14352: 32, 21831: 33} {
		if got, want := (mapBytesAtMost(keys, 0)-mapStruct-sizeClass(8*alignUpPow2(max(1, tables))))/(tableBytes(1024)+mapTableStruct), tables; got != want {
			t.Errorf("a map of %d keys has %d tables of 1,024 slots counted, want %d", keys, got, want)
		}
	}
}

func TestTheTermHoldsTheCountAtAChanceOfOneInAMillionAndAtOneInASeptillion(t *testing.T) {
	for _, tail := range []float64{1e-6, mapTail, 1e-24} {
		dearest := theDearestHeadAt(tail)
		t.Logf("at a chance of %g a depth the dearest head is %d field lines and %d announced names of %d bytes, %.4f MiB; the term is %.4f MiB", tail, dearest.fieldLines, dearest.announced, dearest.bytes(), mib(uint64(dearest.bytes())), mib(headAndTrailersHTTP1))
		if dearest.bytes() > headAndTrailersHTTP1 {
			t.Errorf("at a chance of %g a depth the dearest head takes %d bytes, more than the term, %d", tail, dearest.bytes(), headAndTrailersHTTP1)
		}
	}
}
