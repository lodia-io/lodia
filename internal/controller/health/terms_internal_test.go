// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"net/textproto"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

const (
	headSlack = 6 * mebibyte / 10

	allocationRepeats = 5

	nameCopy   = 16
	valueCopy  = 16
	valueSlice = 16

	oneByteNames        = 256
	oneByteFieldNames   = 77
	twoByteFieldNames   = oneByteFieldNames * oneByteFieldNames
	announcedNamesHTTP1 = oneByteNames + (maxResponseHeaderBytes-2*oneByteNames)/3
	fieldLinesHTTP1     = oneByteFieldNames + twoByteFieldNames + (maxResponseHeaderBytes-3*oneByteFieldNames-4*twoByteFieldNames)/5
	trailerBlockHTTP1   = 4096 - 3
	trailerFieldsHTTP1  = oneByteFieldNames + (trailerBlockHTTP1-3*oneByteFieldNames)/4
)

func TestTheTermOfTheHeadAndItsTrailersIsTheCountAtOneInASeptillionADepthAndRoomToSpare(t *testing.T) {
	count := theDearestHeadAt(1e-24).bytes()
	if got := fmt.Sprintf("%.2f", mib(uint64(count))); got != "2.80" {
		t.Errorf("the count of the head and its trailers at 1e-24 a depth is %s MiB, want 2.80", got)
	}
	if count+headSlack > headAndTrailersHTTP1 {
		t.Errorf("the term is %d bytes, want at least the count, %d, and the slack, %d", headAndTrailersHTTP1, count, headSlack)
	}
}

func allocatedBy(build func()) int {
	rounds := max(100, min(10000, (8<<20)/max(int(allocated(build)), 1)))
	least := math.MaxInt
	for range allocationRepeats {
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

func TestTheSizesTheTermsAreBuiltFromAreTheSourcesOnThisPlatform(t *testing.T) {
	inTheStepWithoutEnvtest(t)
	runes := bytes.Repeat([]byte{0xe9}, 16)
	var text string
	worstCopy := 0
	for n := 1; n <= 16; n++ {
		piece := runes[:n]
		worstCopy = max(worstCopy, allocatedBy(func() { text = string(piece) }))
	}
	for _, c := range []struct {
		name      string
		got, want int
	}{
		{"a copy of a name of up to 16 bytes", worstCopy, nameCopy},
		{"a copy of a value of up to 16 bytes", worstCopy, valueCopy},
		{"an element of the slice of a header's values", int(reflect.TypeFor[string]().Size()), valueSlice},
	} {
		if c.got != c.want {
			t.Errorf("%s takes %d bytes, want %d", c.name, c.got, c.want)
		}
	}
	runtime.KeepAlive(text)
}

func roundedUp(x float64, decimals int) float64 {
	scale := math.Pow(10, float64(decimals))
	return math.Ceil(x*scale-1e-9) / scale
}

func TestEveryFigureOfTheDerivationIsTheSumOfItsTermsRoundedUp(t *testing.T) {
	const gibibyte = 1 << 30
	pass := readsBound(nodesReads) + summariesAtMost
	atRest := uint64(summariesAtMost + (nodesReads+restoreReads)*connectionMargin)
	for _, c := range []struct {
		name     string
		got      float64
		decimals int
		want     float64
	}{
		{"a read, MiB", mib(readsBound(1)), 1, 25.9},
		{"a read apart from its connection, MiB", mib(readsBound(1) - connectionMargin), 1, 23.9},
		{"the two reads of a pass of health-nodes, MiB", mib(readsBound(nodesReads)), 1, 51.8},
		{"a pass of health-nodes, MiB", mib(pass), 1, 55.8},
		{"a pass of health over a held restore, MiB", mib(readsBound(restoreReads)), 1, 181.3},
		{"the connections of a tenant at rest, MiB", mib(uint64((nodesReads + restoreReads) * connectionMargin)), 1, 18},
		{"a tenant at rest, MiB", mib(atRest), 1, 22},
		{"100 passes of health-nodes, GiB", 100 * float64(pass) / gibibyte, 2, 5.45},
		{"10 passes of health over a held restore, GiB", 10 * float64(readsBound(restoreReads)) / gibibyte, 2, 1.78},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := roundedUp(c.got, c.decimals); math.Abs(got-c.want) > 1e-9 {
				t.Errorf("the terms add to %.4f, which rounded up is %.*f, want %.*f", c.got, c.decimals, got, c.decimals, c.want)
			}
		})
	}
}

func TestTheLeastAnHTTP1FieldLineAddsToAHeadIsItsCopiesAndAnElementOfTheSliceSinceTheMapGrowsInSteps(t *testing.T) {
	inTheStepWithoutEnvtest(t)
	head := func(lines int) string {
		var text strings.Builder
		for i := range lines {
			fmt.Fprintf(&text, "X%015d: %016d\r\n", i, i)
		}
		return text.String() + "\r\n"
	}
	allocatedByReading := func(lines int) int {
		text := head(lines)
		source := strings.NewReader(text)
		buffered := bufio.NewReader(source)
		return allocatedBy(func() {
			source.Reset(text)
			buffered.Reset(source)
			if _, err := textproto.NewReader(buffered).ReadMIMEHeader(); err != nil {
				t.Fatal(err)
			}
		})
	}
	least := math.MaxInt
	for lines := 150; lines < 158; lines++ {
		least = min(least, allocatedByReading(lines+1)-allocatedByReading(lines))
	}
	if want := nameCopy + valueCopy + valueSlice; least != want {
		t.Errorf("a field line of a name and a value of 16 bytes adds %d bytes at the least to a head, want %d: a copy of each and an element of the slice of values", least, want)
	}
}
