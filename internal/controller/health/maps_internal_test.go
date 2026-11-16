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

	headFixed               = len("HTTP/1.1 200\nTransfer-Encoding:chunked\nTrailer:") + len("\n\n") - len(",")
	headKeysOutsideTheLines = 2
	trailerLineBytes        = len("Trailer:")
	mostTrailerLines        = 8

	allocHeader       = 8
	mostWithoutHeader = 512
	mostSmallObject   = 32768
	mostMapHint       = 1000

	mostHeadRepeatedBytes = maxResponseHeaderBytes
	mostRepeatedLines     = mostHeadRepeatedBytes / 3
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

func headerOf(n int) int {
	if n > mostWithoutHeader && n <= mostSmallObject-allocHeader {
		return allocHeader
	}
	return 0
}

func sizeClassP(n int) int { return sizeClass(n + headerOf(n)) }

func tableBytes(slots int) int { return sizeClassP(slots / 8 * mapGroup) }

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

type splitDepth struct{ depth, counted int }

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
		out = append(out, splitDepth{j, m})
	}
	return out
}

type mapKey struct {
	keys, initial, slots int
	tail                 float64
}

type mapTables struct{ bytes, split int }

var mapMemo = struct {
	sync.Mutex
	tables map[mapKey]mapTables
}{tables: map[mapKey]mapTables{}}

func mapBytesAtMost(keys, hint int) int { return mapBytesAtTail(keys, hint, mapTail) }

func mapBytesAtTail(keys, hint int, tail float64) int { return tablesOfAMap(keys, hint, tail).bytes }

func mapSplitBytesAtTail(keys, hint int, tail float64) int {
	return tablesOfAMap(keys, hint, tail).split
}

func tablesOfAMap(keys, hint int, tail float64) mapTables {
	shape := shapeOfHint(hint)
	if shape.initial == 0 && keys > 896 || shape.initial == 1 && keys >= mapSplit {
		shape = hintShape{1, 1024}
	}
	key := mapKey{keys, shape.initial, shape.slots, tail}
	mapMemo.Lock()
	tables, ok := mapMemo.tables[key]
	mapMemo.Unlock()
	if !ok {
		tables = mapTablesOfShape(keys, shape, tail)
		mapMemo.Lock()
		mapMemo.tables[key] = tables
		mapMemo.Unlock()
	}
	return tables
}

func mapTablesOfShape(keys int, shape hintShape, tail float64) mapTables {
	initial, slots := shape.initial, shape.slots
	if initial == 0 {
		if keys == 0 {
			return mapTables{bytes: mapStruct}
		}
		if keys <= 8 {
			return mapTables{bytes: mapStruct + sizeClass(mapGroup)}
		}
		single := 16
		for single < 1024 && keys > single*7/8 {
			single *= 2
		}
		if keys <= 896 {
			return mapTables{bytes: mapStruct + tableBytes(single) + mapTableStruct + sizeClass(8)}
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
	held := mapStruct + sizeClass(8<<deepest) + tables*mapTableStruct + unsplit*tableBytes(slots) + (tables-unsplit)*tableBytes(1024)
	if splits == 0 {
		return mapTables{bytes: held}
	}
	return mapTables{held, tableBytes(1024) + mapTableStruct + sizeClass(8<<(deepest-1))}
}

func elementsOf(keys, hint int) int {
	if hint == 0 {
		return valueSlice * keys
	}
	return sizeClassP(valueSlice*hint) + valueSlice*max(0, keys-hint)
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
	return sizeClassP(valueSlice*hint) - valueSlice*hint
}

func worstHint(keys int, tail float64) (hint, mapBytes, elements int) {
	const top = 1000
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

func trailersReadAtMost(fields int) int { return trailersReadAtTail(fields, fields, mapTail) }

func trailersReadAtTail(fields, lines int, tail float64) int {
	if fields == 0 {
		return 0
	}
	hint := min(lines, mostMapHint)
	return mapBytesAtTail(fields, hint, tail) + elementsOf(fields, hint) + nameCopy*fields
}

var pieceBounds = sync.OnceValue(func() []int {
	bounds := []int{1}
	for _, class := range sizeClasses {
		bounds = append(bounds, class+1)
	}
	for page := 32768; page <= maxResponseHeaderBytes; page += 8192 {
		bounds = append(bounds, page+1)
	}
	return bounds
})

var valueMemo = struct {
	sync.Mutex
	bytes [][]int32
}{}

func valueBytesAtMost(length, lines int) int {
	if lines <= 1 {
		return sizeClass(length)
	}
	valueMemo.Lock()
	for len(valueMemo.bytes) <= lines {
		valueMemo.bytes = append(valueMemo.bytes, make([]int32, maxResponseHeaderBytes+1))
	}
	best := int(valueMemo.bytes[lines][length])
	valueMemo.Unlock()
	if best != 0 {
		return best
	}
	best = sizeClass(length)
	for _, piece := range pieceBounds() {
		if piece >= length-1 {
			break
		}
		best = max(best, sizeClass(piece)+valueBytesAtMost(length-1-piece, lines-1))
	}
	valueMemo.Lock()
	valueMemo.bytes[lines][length] = int32(best)
	valueMemo.Unlock()
	return best
}

func valueSliceBytes(lines int) int {
	if lines <= 1 {
		return 0
	}
	return sizeClassP(valueSlice * alignUpPow2(lines))
}

type headMix struct {
	keys, announced, announcedBytes, valueLines, trailers, trailerLines, repeated int
}

type headParts struct{ hint, headerMap, elements, announcedMap, copies, value, trailers, repeated, split int }

func (p headParts) sum() int {
	return p.headerMap + p.elements + p.announcedMap + p.copies + p.value + p.trailers + p.repeated + p.split
}

func headPartsAtMost(m headMix) headParts { return headPartsAtTail(m, mapTail) }

func headPartsAtTail(m headMix, tail float64) headParts {
	hint, headerMap, elements := worstHint(m.keys, tail)
	value := 0
	if m.announcedBytes > 0 {
		value = valueBytesAtMost(m.announcedBytes-1, m.valueLines)
		elements += valueSliceBytes(m.valueLines)
	}
	return headParts{
		hint:         hint,
		headerMap:    headerMap,
		elements:     elements,
		announcedMap: mapBytesAtTail(m.announced+m.trailers, 0, tail),
		copies:       nameCopy * (m.keys + m.announced),
		value:        value,
		trailers:     trailersReadAtTail(m.trailers, max(m.trailers, m.trailerLines), tail),
		repeated:     m.repeated,
		split:        mapSplitBytesAtTail(m.announced+m.trailers, 0, tail),
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
	fieldLines, announced, valueLines, headRepeated, blockKeys, found int
	parts                                                             headParts
}

func (h countedHead) bytes() int { return h.parts.sum() }

type searchedTail struct {
	tail        float64
	names       []int
	header      []int
	byLineBytes []int
	block       []int
	blockKeys   []int

	standard     sync.Once
	standardHead countedHead
}

var searchedTails = struct {
	sync.Mutex
	tails map[float64]*searchedTail
}{tails: map[float64]*searchedTail{}}

func mostAnnouncedNames() int {
	names := 0
	for maxResponseHeaderBytes-headFixed-announcedBytes(names+1) >= 0 {
		names++
	}
	return names
}

func searchOfTail(tail float64) *searchedTail {
	searchedTails.Lock()
	defer searchedTails.Unlock()
	if s, ok := searchedTails.tails[tail]; ok {
		return s
	}
	most := mostAnnouncedNames()
	s := &searchedTail{tail: tail}
	s.names = make([]int, most+trailerFieldsHTTP1+1)
	for keys := range s.names {
		s.names[keys] = mapBytesAtTail(keys, 0, tail) + mapSplitBytesAtTail(keys, 0, tail)
	}
	mostLines := fieldLinesIn(maxResponseHeaderBytes - headFixed)
	s.header = make([]int, mostLines+headKeysOutsideTheLines+1)
	for keys := range s.header {
		_, mapBytes, elements := worstHint(keys, tail)
		s.header[keys] = mapBytes + elements + nameCopy*keys
	}
	s.byLineBytes = make([]int, maxResponseHeaderBytes-headFixed+1)
	for room := range s.byLineBytes {
		s.byLineBytes[room] = s.header[fieldLinesIn(room)+headKeysOutsideTheLines]
	}
	reads := make([]int, trailerFieldsHTTP1+1)
	for keys := 1; keys <= trailerFieldsHTTP1; keys++ {
		reads[keys] = trailersReadAtTail(keys, mostMapHint, tail) + blockRepeatedBytes(keys)
	}
	s.block = make([]int, most+1)
	s.blockKeys = make([]int, most+1)
	for announced := range s.block {
		best, keys := -1, 0
		for trailers, read := range reads {
			if total := s.names[announced+trailers] + read; total > best {
				best, keys = total, trailers
			}
		}
		s.block[announced], s.blockKeys[announced] = best, keys
	}
	searchedTails.tails[tail] = s
	return s
}

const repeatBucket = 256

func (s *searchedTail) mostWithRepeats(room int) int {
	repeats := headRepeats()
	best := 0
	for spent := 0; spent <= min(room, mostHeadRepeatedBytes); spent += repeatBucket {
		best = max(best, s.byLineBytes[room-spent]+int(repeats[min(spent+repeatBucket-1, mostHeadRepeatedBytes)]))
	}
	return best
}

func announcedPart(announced, valueLines int) int {
	if announced == 0 {
		return 0
	}
	return valueBytesAtMost(announcedBytes(announced)-1, valueLines) + valueSliceBytes(valueLines) + nameCopy*announced
}

func (s *searchedTail) dearest(valueLines func(announced int) (least, most int)) countedHead {
	type candidate struct{ announced, valueLines, room, base, bound int }
	var found countedHead
	var maybe []candidate
	for announced := range s.block {
		least, most := valueLines(announced)
		for lines := least; lines <= most; lines++ {
			room := maxResponseHeaderBytes - headFixed - announcedBytes(announced) - trailerLineBytes*(lines-1)
			if room < 0 {
				break
			}
			base := announcedPart(announced, lines) + s.block[announced]
			if own := base + s.byLineBytes[room]; own > found.found {
				found = countedHead{announced: announced, valueLines: lines, found: own}
				found.fieldLines = fieldLinesIn(room)
			}
			if bound := base + s.mostWithRepeats(room); bound > base+s.byLineBytes[room] {
				maybe = append(maybe, candidate{announced, lines, room, base, bound})
			}
		}
	}
	repeats := headRepeats()
	for _, c := range maybe {
		if c.bound <= found.found {
			continue
		}
		for spent := 1; spent <= min(c.room, mostHeadRepeatedBytes); spent++ {
			if total := c.base + s.byLineBytes[c.room-spent] + int(repeats[spent]); total > found.found {
				found = countedHead{fieldLines: fieldLinesIn(c.room - spent), announced: c.announced, valueLines: c.valueLines, headRepeated: spent, found: total}
			}
		}
	}
	found.blockKeys = s.blockKeys[found.announced]
	mix := headMix{
		keys:           found.fieldLines + headKeysOutsideTheLines,
		announced:      found.announced,
		announcedBytes: announcedBytes(found.announced),
		valueLines:     found.valueLines,
		trailers:       found.blockKeys,
		repeated:       int(repeats[found.headRepeated]) + blockRepeatedBytes(found.blockKeys),
	}
	if found.blockKeys > 0 {
		mix.trailerLines = mostMapHint
	}
	found.parts = headPartsAtTail(mix, s.tail)
	return found
}

func valueLinesOf(announced int) (least, most int) {
	return 1, max(1, min(announced, mostTrailerLines))
}

var mostKeysOfTheCount = sync.OnceValue(func() int {
	return max(mostAnnouncedNames()+trailerFieldsHTTP1, fieldLinesIn(maxResponseHeaderBytes-headFixed)+headKeysOutsideTheLines)
})

func theDearestHead() countedHead { return theDearestHeadAt(mapTail) }

func theDearestHeadWithValueLines(tail float64, lines int) countedHead {
	return searchOfTail(tail).dearest(func(announced int) (int, int) {
		if announced < lines {
			return 1, 0
		}
		return lines, lines
	})
}

func theDearestHeadAt(tail float64) countedHead {
	s := searchOfTail(tail)
	s.standard.Do(func() { s.standardHead = s.dearest(valueLinesOf) })
	return s.standardHead
}

func TestTheSizeClassesTheMapsAreBuiltFromAreTheAllocators(t *testing.T) {
	inTheStepWithoutEnvtest(t)
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
	points := map[int]bool{0: true, 1: true, 8: true, mostKeysOfTheCount(): true}
	last := -1
	for keys := 1; keys <= mostKeysOfTheCount(); keys++ {
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
	inTheStepWithoutEnvtest(t)
	sizedForTheBounds(t)
	names := make([]string, mostKeysOfTheCount())
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
			points := mapSplitPoints(c.hint)
			if last := points[len(points)-1]; last != mostKeysOfTheCount() {
				t.Fatalf("the last key count the test reads is %d, want the most the count's maps reach, %d", last, mostKeysOfTheCount())
			}
			for _, keys := range points {
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

func TestTheTailOfABinomialOfFewTrialsIsTheExactSum(t *testing.T) {
	for _, c := range []struct {
		trials, atLeast int
		p, want         float64
	}{
		{8, 1, 2.7218622682327275e-13, 2.1774898145841075e-12},
		{16, 14, 0.1, 9.865e-13},
		{32, 20, 0.1, 6.805576836257488e-13},
	} {
		got := binomialAtLeast(c.trials, c.p, c.atLeast)
		if math.Abs(got-c.want) > c.want*1e-6 {
			t.Errorf("%d trials of chance %g hold %d at least with chance %g, want %g, the exact sum", c.trials, c.p, c.atLeast, got, c.want)
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
	if dearest.fieldLines != 6640 || dearest.announced != 12865 || dearest.valueLines != 5 || dearest.blockKeys != 888 || dearest.headRepeated != 0 || dearest.bytes() != 2916688 || dearest.found != dearest.bytes() {
		t.Errorf("the dearest head is %d field lines and %d announced names, the value on %d lines, then a block of %d keys, with %d bytes of its head spent on repeated lines, of %d bytes, found as %d, want 6,640, 12,865, 5, 888, 0, 2,916,688 and the same", dearest.fieldLines, dearest.announced, dearest.valueLines, dearest.blockKeys, dearest.headRepeated, dearest.bytes(), dearest.found)
	}
	if dearest.announced != countedNamesAtAHundredTrillionth || dearest.valueLines != countedValueLinesAtAHundredTrillionth || dearest.bytes() != countedBytesAtAHundredTrillionth || theDearestHeadAt(1e-24).announced != countedNamesAtASeptillionth {
		t.Errorf("the count's dearest heads announce %d and %d names, the first with its value on %d lines, of %d bytes, want the %d, %d, %d and %d the realized heads are built from", dearest.announced, theDearestHeadAt(1e-24).announced, dearest.valueLines, dearest.bytes(), countedNamesAtAHundredTrillionth, countedNamesAtASeptillionth, countedValueLinesAtAHundredTrillionth, countedBytesAtAHundredTrillionth)
	}
	want := headParts{hint: 896, headerMap: 737936, elements: 108448, announcedMap: 1525008, copies: 312112, value: 47488, trailers: 129024, repeated: 7360, split: 49312}
	if dearest.parts != want {
		t.Errorf("its parts are %+v, want %+v", dearest.parts, want)
	}
	if mostAnnouncedNames() != 21914 || mostKeysOfTheCount() != 22956 {
		t.Errorf("the count reads to %d announced names and %d keys of a map, want 21,914 and 22,956", mostAnnouncedNames(), mostKeysOfTheCount())
	}
	for keys, tables := range map[int]int{896: 1, 897: 2, 449: 1, 14352: 32, 21831: 33, 22956: 33} {
		if got, want := (mapBytesAtMost(keys, 0)-mapStruct-sizeClass(8*alignUpPow2(max(1, tables))))/(tableBytes(1024)+mapTableStruct), tables; got != want {
			t.Errorf("a map of %d keys has %d tables of 1,024 slots counted, want %d", keys, got, want)
		}
	}
}

func TestTheTermHoldsTheCountAtAChanceOfOneInAMillionAndAtOneInASeptillion(t *testing.T) {
	for _, c := range []struct {
		tail                                                float64
		fieldLines, announced, valueLines, blockKeys, bytes int
		parts                                               headParts
	}{
		{1e-6, 6081, 13799, 4, 553, 2681840, headParts{hint: 896, headerMap: 442832, elements: 99440, announcedMap: 1574192, copies: 318112, value: 50864, trailers: 123664, repeated: 23424, split: 49312}},
		{mapTail, 6640, 12865, 5, 888, 2916688, headParts{hint: 896, headerMap: 737936, elements: 108448, announcedMap: 1525008, copies: 312112, value: 47488, trailers: 129024, repeated: 7360, split: 49312}},
		{1e-24, 6428, 13221, 4, 234, 2937696, headParts{hint: 896, headerMap: 737936, elements: 104992, announcedMap: 1525008, copies: 314416, value: 48752, trailers: 118560, repeated: 38720, split: 49312}},
	} {
		if c.parts.sum() != c.bytes {
			t.Fatalf("at a chance of %g a depth the parts pinned add to %d bytes, want the %d pinned", c.tail, c.parts.sum(), c.bytes)
		}
		dearest := theDearestHeadAt(c.tail)
		t.Logf("at a chance of %g a depth the dearest head is %d field lines and %d announced names, the value on %d lines, then a block of %d keys, of %d bytes, %.4f MiB; the term is %.4f MiB, %d bytes over it", c.tail, dearest.fieldLines, dearest.announced, dearest.valueLines, dearest.blockKeys, dearest.bytes(), mib(uint64(dearest.bytes())), mib(headAndTrailersHTTP1), headAndTrailersHTTP1-dearest.bytes())
		if dearest.fieldLines != c.fieldLines || dearest.announced != c.announced || dearest.valueLines != c.valueLines || dearest.blockKeys != c.blockKeys || dearest.headRepeated != 0 || dearest.bytes() != c.bytes || dearest.found != c.bytes || dearest.parts != c.parts {
			t.Errorf("at a chance of %g a depth the dearest head is %d field lines and %d announced names, the value on %d lines, then a block of %d keys, of %d bytes, found as %d, with the parts %+v, want %d, %d, %d, %d, %d and the parts %+v", c.tail, dearest.fieldLines, dearest.announced, dearest.valueLines, dearest.blockKeys, dearest.bytes(), dearest.found, dearest.parts, c.fieldLines, c.announced, c.valueLines, c.blockKeys, c.bytes, c.parts)
		}
		if dearest.bytes()+headSlack > headAndTrailersHTTP1 {
			t.Errorf("at a chance of %g a depth the dearest head takes %d bytes, want it and the slack, %d, at most the term, %d", c.tail, dearest.bytes(), headSlack, headAndTrailersHTTP1)
		}
	}
}

func TestMoreTrailerLinesThanTheCountReadsHoldNoMoreThanTheMostItFinds(t *testing.T) {
	for _, c := range []struct {
		tail         float64
		lines, bytes int
	}{
		{mapTail, mostTrailerLines + 1, 2916232},
		{mapTail, 2 * mostTrailerLines, 2915432},
		{1e-24, mostTrailerLines + 1, 2937376},
		{1e-24, 2 * mostTrailerLines, 2936384},
	} {
		most := theDearestHeadAt(c.tail).bytes()
		dearest := theDearestHeadWithValueLines(c.tail, c.lines)
		if dearest.bytes() != c.bytes || dearest.bytes() > most {
			t.Errorf("at a chance of %g a depth a head with its value on %d lines takes %d bytes, want %d, and no more than the most the count finds on %d lines at most, %d", c.tail, c.lines, dearest.bytes(), c.bytes, mostTrailerLines, most)
		}
	}
}

func TestTheTableASplitHoldsIsTheOneOfTheNamesMapTheTrailersAreMergedInto(t *testing.T) {
	for _, c := range []struct{ announced, trailers, want int }{
		{13, 0, 0},
		{896, 0, 0},
		{897, 0, 49192},
		{13, 1042, 49192},
		{12815, 1042, 49312},
		{20696, 1042, 49312},
		{20697, 1042, 49440},
		{21914, 1042, 49440},
	} {
		if got := headPartsAtMost(headMix{keys: 3, announced: c.announced, trailers: c.trailers}).split; got != c.want {
			t.Errorf("a head of %d announced names and %d trailers holds %d bytes of a split besides its tables, want %d: a table and the directory it replaces, once the map has split", c.announced, c.trailers, got, c.want)
		}
	}
}

func TestTheTrailersOwnReadIsTheSameAtEveryChanceSinceItsTwoTablesSplitWithNone(t *testing.T) {
	for _, tail := range []float64{1e-6, mapTail, 1e-24, 1e-100} {
		if got := trailersReadAtTail(trailerFieldsHTTP1, trailerFieldsHTTP1, tail); got != 132160 {
			t.Errorf("the trailers' own read takes %d bytes at a chance of %g a depth, want 132,160 at every chance", got, tail)
		}
	}
}

func TestTheWorstHintIsTheDearestOfEveryHintATenantCanSetByTheBytesTheAllocatorTakesForTheSlice(t *testing.T) {
	inTheStepWithoutEnvtest(t)
	sizedForTheBounds(t)
	taken := make([]int, mostMapHint+1)
	for lo := 1; lo <= mostMapHint; {
		hi := lo
		for hi < mostMapHint && sizeClassP(valueSlice*(hi+1)) == sizeClassP(valueSlice*lo) {
			hi++
		}
		var made []string
		first := allocatedBy(func() { made = make([]string, lo) })
		last := allocatedBy(func() { made = make([]string, hi) })
		runtime.KeepAlive(made)
		if first != last {
			t.Errorf("make([]string, %d) allocates %d bytes and make([]string, %d) %d, want the same size class", lo, first, hi, last)
		}
		for hint := lo; hint <= hi; hint++ {
			taken[hint] = first
		}
		lo = hi + 1
	}
	for _, keys := range []int{0, 1, 9, 100, 663, 896, 897, 898, 1500, 6642, 6740} {
		best := -1
		for hint := 0; hint <= mostMapHint; hint++ {
			elements := valueSlice * keys
			if hint > 0 {
				elements = taken[hint] + valueSlice*max(0, keys-hint)
			}
			best = max(best, mapBytesAtMost(keys, hint)+elements)
		}
		hint, mapBytes, elements := worstHint(keys, mapTail)
		if mapBytes+elements != best || mapBytes != mapBytesAtMost(keys, hint) || elements != elementsOf(keys, hint) {
			t.Errorf("the worst hint of %d keys is %d, taking %d and %d bytes, want %d in all by the bytes the allocator takes", keys, hint, mapBytes, elements, best)
		}
	}
	if hint, _, _ := worstHint(6642, mapTail); hint != 896 {
		t.Errorf("the worst hint of the dearest head's 6,642 keys is %d, want 896, whose slice takes %d bytes", hint, taken[896])
	}
}

func TestTheTrailerValueTheCountSplitsIsTheMostOfEveryLengthOfItsPiecesOnEveryNumberOfLines(t *testing.T) {
	const longest = 2500
	exactly := make([][]int, mostTrailerLines+1)
	for pieces := 1; pieces <= mostTrailerLines; pieces++ {
		exactly[pieces] = make([]int, longest+1)
		for total := pieces; total <= longest; total++ {
			if pieces == 1 {
				exactly[1][total] = sizeClass(total)
				continue
			}
			for piece := 1; piece <= total-(pieces-1); piece++ {
				exactly[pieces][total] = max(exactly[pieces][total], sizeClass(piece)+exactly[pieces-1][total-piece])
			}
		}
	}
	wrong := 0
	for length := 1; length <= longest; length++ {
		for lines := 1; lines <= mostTrailerLines; lines++ {
			want := 0
			for pieces := 1; pieces <= lines && length-(pieces-1) >= pieces; pieces++ {
				want = max(want, exactly[pieces][length-(pieces-1)])
			}
			if got := valueBytesAtMost(length, lines); got != want && wrong < 5 {
				wrong++
				t.Errorf("a value of %d bytes on up to %d lines takes %d bytes in its strings, want %d, the most of every length of every piece", length, lines, got, want)
			}
		}
	}
	for length := 3; length <= maxResponseHeaderBytes; length += 211 {
		want := sizeClass(length)
		for piece := 1; piece <= length-2; piece++ {
			want = max(want, sizeClass(piece)+sizeClass(length-1-piece))
		}
		if got := valueBytesAtMost(length, 2); got != want && wrong < 5 {
			wrong++
			t.Errorf("a value of %d bytes on up to 2 lines takes %d bytes in its strings, want %d, the most of every length of its two pieces", length, got, want)
		}
	}
}
