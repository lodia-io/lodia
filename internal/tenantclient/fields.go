// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantclient

import (
	"strings"

	"go.lodia.io/lodia/internal/jsonscan"
)

// The administrator of a tenant can have any server answer, so nothing a tenant
// sends is decoded unless the reader keeps the field it fills and the raw token
// is within the bound below, checked before the decode
// (docs/review-checklist.md, item 10). A bound counts the bytes of a string
// between its quotes, in which a character may be escaped as \uXXXX, six bytes.
// The bounds every reader of a tenant shares are:
//   - MaxNameRaw: a name of MaxName bytes with every character escaped, 1,518
//     bytes, rounded up;
//   - MaxVersionRaw: a resourceVersion is a revision of at most 19 digits;
//   - MaxWordRaw: LoadBalancer, the longest word that a field read with
//     jsonscan.Word is compared with, with every character escaped, 72 bytes;
//     such a field (a condition's type and status, a Status's status, a failure
//     policy, a Service's type, a reclaim policy, a phase) is only compared with
//     a word, and a token past this bound is none of the words; a reason is
//     compared with longer words and has MaxReasonRaw;
//   - MaxReasonRaw: RequestEntityTooLarge, the longest of the reasons that
//     apierrors compares a Status's reason with, with every character
//     escaped, 126 bytes; a cause's reason is compared with
//     NamespaceTerminating, 120;
//   - MaxNumberRaw: a number kept is an int32, at most 11 bytes;
//   - MaxMessage: a Status's message is kept to its first MaxMessage bytes,
//     decoded, so that a long answer is read as a short one is.
//
// A token past its bound, but for a word, fails the read, or stops a list of
// nodes, and the error names the field and the bound and nothing of the value.
const (
	MaxName       = 253
	MaxNameRaw    = 2048
	MaxVersionRaw = 64
	MaxWordRaw    = 72
	MaxReasonRaw  = 126
	MaxNumberRaw  = 11
	MaxMessage    = 1024
)

// Cut keeps a copy of the first most+1 bytes of s: a string over most is never
// kept, and its length is all the readers ask of it. A slice of s would hold all
// of s, which is up to three times its raw bytes.
func Cut(s string, most int) string {
	return strings.Clone(s[:min(len(s), most+1)])
}

func ObjectName(field string, value []byte) (string, error) {
	s, err := jsonscan.Text(field, value, MaxNameRaw)
	return Cut(s, MaxName), err
}

// Array is a JSON array decoded one element at a time. It keeps the first most
// elements, and Over says there was one more, which it does not decode: the
// elements after the bound cost a scan and nothing else.
type Array[T any] struct {
	Items []T
	Over  bool
}

func ArrayOf[T any](field string, data []byte, most int, element func([]byte) (T, error)) (Array[T], error) {
	var a Array[T]
	switch {
	case jsonscan.Null(data):
		return a, nil
	case data[0] != '[':
		return a, jsonscan.NotA(field, "an array")
	}
	for value := range jsonscan.Elements(data) {
		if len(a.Items) == most {
			a.Over = true
			return a, nil
		}
		item, err := element(value)
		if err != nil {
			return a, err
		}
		a.Items = append(a.Items, item)
	}
	return a, nil
}
