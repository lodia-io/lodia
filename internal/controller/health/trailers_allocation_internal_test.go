// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bufio"
	"fmt"
	"maps"
	"net/textproto"
	"strings"
	"testing"
)

func mapGrowthAllocates(keys int) int {
	total := sizeClass(8)
	for slots := 16; ; slots *= 2 {
		total += tableBytes(slots) + mapTableStruct
		if keys <= slots*7/8 {
			return total
		}
	}
}

func theNamesOfABlockNoAllocatorShares(count int) (names []string, block string) {
	for i := range count {
		names = append(names, fmt.Sprintf("X%011d", i))
	}
	return names, strings.Join(names[:count-1], ":\n") + ":\n" + names[count-1] + ":\r\n\r\n"
}

func TestABlockOfTrailersOfNamesNoAllocatorSharesIsReadInAllocationsEqualToItsParts(t *testing.T) {
	inTheStepWithoutEnvtest(t)
	for _, count := range []int{292, 1100} {
		names, block := theNamesOfABlockNoAllocatorShares(count)
		source := strings.NewReader(block)
		buffered := bufio.NewReaderSize(source, 32<<10)
		read := allocatedBy(func() {
			source.Reset(block)
			buffered.Reset(source)
			if _, err := textproto.NewReader(buffered).ReadMIMEHeader(); err != nil {
				t.Fatal(err)
			}
		})
		if want := trailersReadAtMost(len(names)); read != want {
			t.Errorf("reading %d trailers of 12 bytes allocates %d bytes, want its parts, %d: the map made for them, the slice of their values and a copy of each name", len(names), read, want)
		}
	}
}

func TestABlockOfTrailersIsMergedIntoAMapOfOneNameInAllocationsEqualToItsParts(t *testing.T) {
	inTheStepWithoutEnvtest(t)
	names, block := theNamesOfABlockNoAllocatorShares(292)
	header, err := textproto.NewReader(bufio.NewReader(strings.NewReader(block))).ReadMIMEHeader()
	if err != nil {
		t.Fatal(err)
	}
	merge := allocatedBy(func() { maps.Copy(map[string][]string{"Zz": nil}, header) })
	if want := mapGrowthAllocates(len(names)); merge != want {
		t.Errorf("merging %d trailers into a map of one name allocates %d bytes, want its parts, %d: the tables the map grows through", len(names), merge, want)
	}
}
