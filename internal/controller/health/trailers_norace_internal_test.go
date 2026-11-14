// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build !race

package health

import (
	"bufio"
	"maps"
	"net/textproto"
	"strings"
	"testing"
)

func TestAnHTTP1TrailerBlockReadAndMergedAllocatesAtMostItsPartsAndLessThanItsSmallestPartUnderThem(t *testing.T) {
	names := canonicalFieldNames(trailerBlockHTTP1)
	block := strings.Join(names[:len(names)-1], ":\n") + ":\n" + names[len(names)-1] + ":\r\n\r\n"
	source := strings.NewReader(block)
	buffered := bufio.NewReaderSize(source, 4096)
	got := allocatedBy(func() {
		source.Reset(block)
		buffered.Reset(source)
		header, err := textproto.NewReader(buffered).ReadMIMEHeader()
		if err != nil {
			t.Fatal(err)
		}
		maps.Copy(map[string][]string{"Zz": nil}, header)
	})
	parts := len(names)*trailerField + 2*mapTable
	smallest := len(names) * min(nameCopy, valueSlice)
	t.Logf("%d trailers read and merged allocate %d bytes, the parts add to %d", len(names), got, parts)
	if got > parts || parts-got >= smallest {
		t.Errorf("%d trailers read and merged allocate %d bytes, and their parts add to %d, want the parts at least that and less than %d more", len(names), got, parts, smallest)
	}
}
