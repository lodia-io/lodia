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

func TestAnHTTP1TrailerBlockOfNamesNoAllocatorSharesIsReadAndMergedInAllocationsEqualToItsParts(t *testing.T) {
	var names []string
	for i := 0; (len(names)+1)*(len("X00000000000")+len(":\n")) <= trailerBlockHTTP1; i++ {
		names = append(names, fmt.Sprintf("X%011d", i))
	}
	block := strings.Join(names[:len(names)-1], ":\n") + ":\n" + names[len(names)-1] + ":\r\n\r\n"
	source := strings.NewReader(block)
	buffered := bufio.NewReaderSize(source, 4096)
	readBlock := func() textproto.MIMEHeader {
		source.Reset(block)
		buffered.Reset(source)
		header, err := textproto.NewReader(buffered).ReadMIMEHeader()
		if err != nil {
			t.Fatal(err)
		}
		return header
	}
	header := readBlock()
	read := allocatedBy(func() { readBlock() })
	merge := allocatedBy(func() { maps.Copy(map[string][]string{"Zz": nil}, header) })
	hint := min(len(names), 1000)
	readParts := mapBytesAtMost(len(names), hint) + sizeClass(valueSlice*hint) + nameCopy*len(names)
	mergeParts := mapGrowthAllocates(len(names))
	t.Logf("%d names of 12 bytes: read allocates %d bytes, its parts add to %d; merged into a map of one name, %d, its parts %d", len(names), read, readParts, merge, mergeParts)
	if read != readParts {
		t.Errorf("reading %d trailers of 12 bytes allocates %d bytes, want its parts, %d: the map made for them, the slice of their values and a copy of each name", len(names), read, readParts)
	}
	if merge != mergeParts {
		t.Errorf("merging them into a map of one name allocates %d bytes, want its parts, %d: the tables the map grows through", merge, mergeParts)
	}
}
