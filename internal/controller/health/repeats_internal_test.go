// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bufio"
	"net/textproto"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func nextSliceCap(newLen, oldCap int) int {
	if newLen > 2*oldCap {
		return newLen
	}
	if oldCap < 256 {
		return 2 * oldCap
	}
	newCap := oldCap
	for {
		newCap += (newCap + 3*256) >> 2
		if newCap >= newLen {
			return newCap
		}
	}
}

func grownCapacity(oldCap, newLen int) int {
	size := valueSlice * nextSliceCap(newLen, oldCap)
	return (sizeClassP(size) - headerOf(size)) / valueSlice
}

var heldByRepeats = sync.OnceValue(func() []int {
	held := make([]int, mostRepeatedLines+1)
	capacity := 1
	for lines := 1; lines <= mostRepeatedLines; lines++ {
		if lines+1 > capacity {
			capacity = grownCapacity(capacity, lines+1)
		}
		held[lines] = sizeClassP(valueSlice * capacity)
	}
	return held
})

type repeatStep struct{ lines, bytes int }

var repeatSteps = sync.OnceValue(func() []repeatStep {
	held := heldByRepeats()
	var steps []repeatStep
	for lines := 1; lines < len(held); lines++ {
		if held[lines] != held[lines-1] {
			steps = append(steps, repeatStep{lines, held[lines]})
		}
	}
	return steps
})

func addKeyRepeats(into, from []int32, lineBytes int) {
	steps := repeatSteps()
	for budget := range into {
		best := from[budget]
		for _, step := range steps {
			spent := lineBytes * step.lines
			if spent > budget {
				break
			}
			best = max(best, from[budget-spent]+int32(step.bytes))
		}
		into[budget] = best
	}
}

func blockLineBytes(key int) int {
	if key <= oneByteFieldNames {
		return 3
	}
	return 4
}

func blockOwnBytes(keys int) int {
	return 3*min(keys, oneByteFieldNames) + 4*max(0, keys-oneByteFieldNames)
}

var blockRepeats = sync.OnceValue(func() [][]int32 {
	layers := make([][]int32, trailerFieldsHTTP1+1)
	layers[0] = make([]int32, trailerBlockHTTP1+1)
	for keys := 1; keys <= trailerFieldsHTTP1; keys++ {
		layers[keys] = make([]int32, trailerBlockHTTP1-blockOwnBytes(keys)+1)
		addKeyRepeats(layers[keys], layers[keys-1], blockLineBytes(keys))
	}
	return layers
})

func blockRepeatedBytes(keys int) int {
	layer := blockRepeats()[keys]
	return int(layer[len(layer)-1])
}

var headRepeats = sync.OnceValue(func() []int32 {
	best := make([]int32, mostHeadRepeatedBytes+1)
	next := make([]int32, len(best))
	for range oneByteFieldNames {
		addKeyRepeats(next, best, 3)
		best, next = next, best
	}
	for budget := range best {
		for _, step := range repeatSteps() {
			spent := 4 * step.lines
			if spent > budget {
				break
			}
			best[budget] = max(best[budget], best[budget-spent]+int32(step.bytes))
		}
	}
	return best
})

func repeatsOfABlock(keys int) []int {
	layers, steps := blockRepeats(), repeatSteps()
	lines := make([]int, keys+1)
	budget := len(layers[keys]) - 1
	for key := keys; key > 0; key-- {
		got := layers[key][budget]
		if got == layers[key-1][budget] {
			continue
		}
		for _, step := range steps {
			spent := blockLineBytes(key) * step.lines
			if spent <= budget && layers[key-1][budget-spent]+int32(step.bytes) == got {
				lines[key] = step.lines
				budget -= spent
				break
			}
		}
	}
	return lines[1:]
}

func TestASliceOfStringsTakesTheSizeClassOfItsBytesAndTheAllocatorsHeaderPastFiveHundredAndTwelve(t *testing.T) {
	if inAProcessOfItsOwn(t) {
		return
	}
	defer theBodyRanToItsEnd(t)
	sizedForTheBounds(t)
	tried := map[int]bool{}
	for _, class := range append(sizeClasses[:], 3*mostSmallObject/2) {
		for _, elements := range []int{class/valueSlice - 1, class / valueSlice, class/valueSlice + 1} {
			if elements < 1 || tried[elements] {
				continue
			}
			tried[elements] = true
			var made []string
			if got, want := allocatedBy(func() { made = make([]string, elements) }), sizeClassP(valueSlice*elements); got != want {
				t.Errorf("make([]string, %d) allocates %d bytes, want %d", elements, got, want)
			}
			runtime.KeepAlive(made)
		}
	}
	for elements, want := range map[int]int{32: 512, 33: 576, 896: 16384, 897: 16384, 2047: 32768, 2048: 32768, 2049: 40960} {
		if got := sizeClassP(valueSlice * elements); got != want {
			t.Errorf("a slice of %d strings is priced at %d bytes, want %d", elements, got, want)
		}
	}
}

func TestASliceOfPointersTakesTheSizeClassOfItsBytesAndTheAllocatorsHeaderOfEightBytes(t *testing.T) {
	if inAProcessOfItsOwn(t) {
		return
	}
	defer theBodyRanToItsEnd(t)
	tried := map[int]bool{}
	for _, class := range sizeClasses {
		for _, elements := range []int{class/8 - 1, class / 8, class/8 + 1} {
			if elements < 1 || tried[elements] || elements*8 <= mostWithoutHeader {
				continue
			}
			tried[elements] = true
			var made []*byte
			if got, want := allocatedBy(func() { made = make([]*byte, elements) }), sizeClassP(8*elements); got != want {
				t.Errorf("make([]*byte, %d) allocates %d bytes, want %d", elements, got, want)
			}
			runtime.KeepAlive(made)
		}
	}
}

func TestATableOfGroupsTakesTheSameSizeClassWithTheHeaderAndWithoutIt(t *testing.T) {
	for slots := 8; slots <= 1024; slots *= 2 {
		if got, want := tableBytes(slots), sizeClass(slots/8*mapGroup); got != want {
			t.Errorf("a table of %d slots takes %d bytes with the allocator's header, %d without it, want the same", slots, got, want)
		}
	}
}

func TestAKeyRepeatedByLinesKeepsTheSliceAppendGrewToInTheBytesTheAllocatorTakesForIt(t *testing.T) {
	if inAProcessOfItsOwn(t) {
		return
	}
	defer theBodyRanToItsEnd(t)
	sizedForTheBounds(t)
	held := heldByRepeats()
	read := func(lines int) []string {
		text := strings.Repeat("A:\n", lines+1) + "\n"
		header, err := textproto.NewReader(bufio.NewReaderSize(strings.NewReader(text), 1<<15)).ReadMIMEHeader()
		if err != nil {
			t.Fatal(err)
		}
		return header["A"]
	}
	for lines := 1; lines <= 1400; lines++ {
		values := read(lines)
		if len(values) != lines+1 || sizeClassP(valueSlice*cap(values)) != held[lines] {
			t.Fatalf("a key repeated by %d lines keeps %d values in a slice of %d bytes, want %d values in %d", lines, len(values), sizeClassP(valueSlice*cap(values)), lines+1, held[lines])
		}
	}
	for _, lines := range []int{2000, 3000, 4000, mostRepeatedLines} {
		if values := read(lines); sizeClassP(valueSlice*cap(values)) != held[lines] {
			t.Errorf("a key repeated by %d lines keeps a slice of %d bytes, want %d", lines, sizeClassP(valueSlice*cap(values)), held[lines])
		}
	}
	grown := make([]string, 1)
	for lines := 1; lines <= mostRepeatedLines; lines++ {
		if len(grown) < cap(grown) {
			grown = append(grown, "")
			continue
		}
		full := slices.Clone(grown)
		var next []string
		if got := allocatedBy(func() { next = full; next = append(next, "") }); got != held[lines] {
			t.Errorf("the line that makes %d values allocates %d bytes for the slice, want %d", lines+1, got, held[lines])
		}
		grown = append(grown, "")
		if len(next) != len(grown) || cap(next) != cap(grown) {
			t.Errorf("append to %d values gave a capacity of %d, the model's %d", len(full), cap(next), cap(grown))
		}
	}
}

func bruteRepeats(budget int, lineBytes []int) int {
	held := heldByRepeats()
	best := make([]int, budget+1)
	for _, bytes := range lineBytes {
		next := slices.Clone(best)
		for spent := range next {
			for lines := 1; bytes*lines <= spent; lines++ {
				next[spent] = max(next[spent], best[spent-bytes*lines]+held[lines])
			}
		}
		best = next
	}
	return best[budget]
}

func TestTheMostRepeatedLinesBuyForTheKeysOfABlockIsTheBestChoiceOfHowOftenToRepeatEachOfThem(t *testing.T) {
	for keys := 1; keys <= 3; keys++ {
		costs := slices.Repeat([]int{3}, keys)
		if got, want := blockRepeatedBytes(keys), bruteRepeats(trailerBlockHTTP1-blockOwnBytes(keys), costs); got != want {
			t.Errorf("a block of %d keys that repeats them buys %d bytes of slices, want %d, the best of every number of repeats of each", keys, got, want)
		}
	}
	for _, keys := range []int{1020, 1030, 1040, trailerFieldsHTTP1} {
		costs := slices.Concat(slices.Repeat([]int{3}, oneByteFieldNames), slices.Repeat([]int{4}, keys-oneByteFieldNames))
		if got, want := blockRepeatedBytes(keys), bruteRepeats(trailerBlockHTTP1-blockOwnBytes(keys), costs); got != want {
			t.Errorf("a block of %d keys that repeats them buys %d bytes of slices, want %d, the best of every number of repeats of each", keys, got, want)
		}
	}
	for _, c := range []struct {
		budget int
		costs  []int
	}{{60, []int{3, 3, 3}}, {60, []int{3, 4, 4}}, {90, []int{4, 4, 3, 3}}, {41, []int{4}}} {
		from := make([]int32, c.budget+1)
		for _, cost := range c.costs {
			into := make([]int32, c.budget+1)
			addKeyRepeats(into, from, cost)
			from = into
		}
		for budget := 0; budget <= c.budget; budget++ {
			if got, want := int(from[budget]), bruteRepeats(budget, c.costs); got != want {
				t.Errorf("keys of lines of %v bytes repeated within %d bytes buy %d, want %d", c.costs, budget, got, want)
			}
		}
	}
}

func TestAHeadThatRepeatsKeysBuysAtMostWhatItsOneByteAndTwoByteLinesBuy(t *testing.T) {
	repeats := headRepeats()
	budgets := []int{231, 232, 300, 400, 500, 700}
	for budget := range 200 {
		budgets = append(budgets, budget)
	}
	for _, budget := range budgets {
		costs := slices.Concat(slices.Repeat([]int{3}, min(oneByteFieldNames, budget/3)), slices.Repeat([]int{4}, budget/4))
		if got, want := int(repeats[budget]), bruteRepeats(budget, costs); got != want {
			t.Errorf("a head that repeats keys within %d bytes buys %d bytes of slices, want %d", budget, got, want)
		}
	}
	if repeats[0] != 0 || !slices.IsSorted(repeats) {
		t.Errorf("the slices %d bytes of repeats buy do not grow with the bytes", repeats[0])
	}
}

func TestABlockThatRepeatsTheKeysTheCountPicksKeepsTheSlicesTheCountAddsAndFillsItsBytesWithMoreThanAThousandLines(t *testing.T) {
	for _, tail := range []float64{1e-6, mapTail, 1e-24} {
		keys := theDearestHeadAt(tail).blockKeys
		repeats := repeatsOfABlock(keys)
		var block strings.Builder
		bytes, lines := blockOwnBytes(keys), keys
		for i, again := range repeats {
			block.WriteString(strings.Repeat("K"+strconv.Itoa(i)+":\n", again+1))
			bytes += again * blockLineBytes(i+1)
			lines += again
		}
		fillers := (trailerBlockHTTP1 - bytes) / 3
		block.WriteString(strings.Repeat("K0:\n", fillers))
		lines += fillers
		if bytes > trailerBlockHTTP1 || lines < mostMapHint {
			t.Errorf("a block of %d keys that repeats them takes %d of %d bytes in %d lines, want it to fit and to have the %d lines that make the reader's hint %d", keys, bytes, trailerBlockHTTP1, lines, mostMapHint, mostMapHint)
		}
		header, err := textproto.NewReader(bufio.NewReaderSize(strings.NewReader(block.String()+"\n"), 1<<16)).ReadMIMEHeader()
		if err != nil {
			t.Fatal(err)
		}
		held := 0
		for _, values := range header {
			if len(values) > 1 {
				held += sizeClassP(valueSlice * cap(values))
			}
		}
		if len(header) != keys || held != blockRepeatedBytes(keys) {
			t.Errorf("at a chance of %g a block of %d keys read through textproto has %d keys and keeps %d bytes of slices of repeated keys, want %d and %d", tail, keys, len(header), held, keys, blockRepeatedBytes(keys))
		}
	}
	if least := (trailerBlockHTTP1 - 2) / 4; least < mostMapHint {
		t.Errorf("a block of lines of 4 bytes at the most that leaves 2 bytes holds %d lines, want at least %d", least, mostMapHint)
	}
}

func bruteDearest(s *searchedTail, valueLines func(announced int) (least, most int)) (found, foundWithNoRepeats int, at countedHead) {
	repeats := headRepeats()
	for n := range s.block {
		least, most := valueLines(n)
		for lines := least; lines <= most; lines++ {
			room := maxResponseHeaderBytes - headFixed - announcedBytes(n) - trailerLineBytes*(lines-1)
			if room < 0 {
				break
			}
			for repeated := 0; repeated <= min(room, mostHeadRepeatedBytes); repeated++ {
				total := announcedPart(n, lines) + s.block[n] + s.byLineBytes[room-repeated] + int(repeats[repeated])
				if total > found {
					found, at = total, countedHead{announced: n, valueLines: lines, headRepeated: repeated}
				}
				if repeated == 0 {
					foundWithNoRepeats = max(foundWithNoRepeats, total)
				}
			}
		}
	}
	return found, foundWithNoRepeats, at
}

func TestTheDearestHeadByTheBoundsOfItsRepeatedBytesIsTheDearestByEveryChoiceOfThem(t *testing.T) {
	s := searchOfTail(mapTail)
	repeatedWins := 0
	for _, w := range []struct{ from, to int }{{0, 0}, {0, 2}, {60, 62}, {1000, 1002}, {1267, 1267}, {5000, 5002}, {9000, 9002}, {12850, 12852}, {12988, 12988}, {13128, 13128}, {16000, 16002}, {21912, 21914}} {
		within := func(announced int) (int, int) {
			if announced < w.from || announced > w.to {
				return 1, 0
			}
			return valueLinesOf(announced)
		}
		found, withNoRepeats, at := bruteDearest(s, within)
		got := s.dearest(within)
		if got.found != found || got.bytes() != found {
			t.Errorf("among %d to %d announced names the dearest head is %d bytes, its parts add to %d, want %d, the dearest by every number of repeated bytes", w.from, w.to, got.found, got.bytes(), found)
		}
		if found > withNoRepeats {
			repeatedWins++
			if got.announced != at.announced || got.valueLines != at.valueLines || got.headRepeated != at.headRepeated {
				t.Errorf("among %d to %d announced names the dearest head is at %d names, its value on %d lines and %d repeated bytes, want %d, %d and %d", w.from, w.to, got.announced, got.valueLines, got.headRepeated, at.announced, at.valueLines, at.headRepeated)
			}
		}
	}
	if repeatedWins == 0 {
		t.Errorf("no window of announced names has a dearest head that repeats keys, so the test holds nothing of the bounds on them")
	}
}

func TestTheBlockTheCountAddsToANumberOfAnnouncedNamesIsTheDearestOfEveryNumberOfKeysItHas(t *testing.T) {
	for _, tail := range []float64{mapTail, 1e-24} {
		s := searchOfTail(tail)
		overAThousand := 0
		for announced := 0; announced <= mostAnnouncedNames(); announced += 500 {
			best, keys := -1, 0
			for trailers := 0; trailers <= trailerFieldsHTTP1; trailers++ {
				total := mapBytesAtTail(announced+trailers, 0, tail) + mapSplitBytesAtTail(announced+trailers, 0, tail)
				if trailers > 0 {
					total += trailersReadAtTail(trailers, mostMapHint, tail) + blockRepeatedBytes(trailers)
				}
				if total > best {
					best, keys = total, trailers
				}
			}
			if s.block[announced] != best || s.blockKeys[announced] != keys {
				t.Errorf("at a chance of %g a depth the dearest block after %d announced names is %d keys and %d bytes with the names' map, want %d keys and %d", tail, announced, s.blockKeys[announced], s.block[announced], keys, best)
			}
			if keys > 1000 {
				overAThousand++
			}
		}
		if overAThousand == 0 {
			t.Errorf("at a chance of %g a depth no number of announced names read has a dearest block of more than 1,000 keys", tail)
		}
	}
}

func theMostRepeatsOfKeysBySteps(budget int) []int {
	held := heldByRepeats()
	var steps []int
	for lines := 1; lines < len(held); lines++ {
		if held[lines] > held[lines-1] {
			steps = append(steps, lines)
		}
	}
	withKeys := make([][]int, oneByteFieldNames+1)
	withKeys[0] = make([]int, budget+1)
	for keys := 1; keys <= oneByteFieldNames; keys++ {
		withKeys[keys] = slices.Clone(withKeys[keys-1])
		for spent := range withKeys[keys] {
			for _, lines := range steps {
				if 3*lines <= spent {
					withKeys[keys][spent] = max(withKeys[keys][spent], withKeys[keys-1][spent-3*lines]+held[lines])
				}
			}
		}
	}
	best := slices.Clone(withKeys[oneByteFieldNames])
	for spent := range best {
		for _, lines := range steps {
			if 4*lines <= spent {
				best[spent] = max(best[spent], best[spent-4*lines]+held[lines])
			}
		}
	}
	return best
}

func TestAHeadThatRepeatsKeysBuysTheSameAsAnotherSearchByStepsWhenItSpendsEnoughBytesToUseEveryKeyOfOneByte(t *testing.T) {
	const most = 12000
	repeats, want := headRepeats(), theMostRepeatsOfKeysBySteps(most)
	for budget := range want {
		if int(repeats[budget]) != want[budget] {
			t.Fatalf("a head that repeats keys within %d bytes buys %d bytes of slices, want %d", budget, repeats[budget], want[budget])
		}
	}
}

func TestNoBlockRepeatsAsManyKeysAsThereAreKeysOfOneByteSoTheCostOfTheLastOfThemIsNone(t *testing.T) {
	for keys := 1; keys <= trailerFieldsHTTP1; keys++ {
		repeated := 0
		for _, lines := range repeatsOfABlock(keys) {
			if lines > 0 {
				repeated++
			}
		}
		if repeated >= oneByteFieldNames {
			t.Errorf("a block of %d keys repeats %d of them, want fewer than the %d of one byte", keys, repeated, oneByteFieldNames)
		}
	}
}
