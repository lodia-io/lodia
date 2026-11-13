// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.lodia.io/lodia/internal/jsonscan"
)

// The administrator of a tenant can have any server answer, so nothing a tenant
// sends is decoded unless Health keeps the field it fills and the raw token is
// within the bound below, checked before the decode (docs/review-checklist.md,
// item 10). A bound counts the bytes of a string between its quotes, in which a
// character may be escaped as \uXXXX, six bytes. The bounds are:
//   - maxNameRaw: a name of maxName bytes with every character escaped, 1,518
//     bytes, rounded up;
//   - maxVersionRaw: a resourceVersion is a revision of at most 19 digits;
//   - maxTimeRaw: a time with microseconds and an offset is 32 bytes;
//   - maxWordRaw: LoadBalancer, the longest word Health compares, with every
//     character escaped, 72 bytes; a field Health only compares with a word (a
//     condition's type and status, a Status's status, a failure policy, a
//     Service's type, a reclaim policy, a phase) is read with jsonscan.Word,
//     and a token past this bound is none of the words; a Status's reason and
//     a cause's reason, words under 40 bytes, are read within it too;
//   - maxHolderRaw: a holderIdentity is a name, an underscore and a UUID, at
//     most 290 bytes;
//   - maxKubeletRaw: maxKubeletVersion bytes with every character escaped, 384;
//   - maxNumberRaw: a number kept is an int32, at most 11 bytes;
//   - maxMessageRaw: a Status's message past it is replaced by a sentence.
//
// A token past its bound, but for a word, fails the read, or stops a list of
// nodes, and the error names the field and the bound and nothing of the value.
const (
	maxNameRaw    = 2048
	maxVersionRaw = 64
	maxTimeRaw    = 64
	maxWordRaw    = 72
	maxHolderRaw  = 512
	maxKubeletRaw = 6 * maxKubeletVersion
	maxNumberRaw  = 11
	maxMessageRaw = 1024
)

// cut keeps a copy of the first most+1 bytes of s: a string over most is never
// kept, and its length is all the readers ask of it. A slice of s would hold all
// of s, which is up to three times its raw bytes.
func cut(s string, most int) string {
	return strings.Clone(s[:min(len(s), most+1)])
}

func objectName(field string, value []byte) (string, error) {
	s, err := jsonscan.Text(field, value, maxNameRaw)
	return cut(s, maxName), err
}

// optionalText decodes a string field that null leaves nil.
func optionalText(field string, value []byte, most int) (*string, error) {
	if jsonscan.Null(value) {
		return nil, nil
	}
	s, err := jsonscan.Text(field, value, most)
	return &s, err
}

// timeOf reads a time from a string token within maxTimeRaw with the layout of
// the API's own type, metav1.Time's RFC 3339 or metav1.MicroTime's, whose
// UnmarshalJSON would hand any token, an object included, to encoding/json;
// null is the zero time.
func timeOf(field string, value []byte, layout string) (time.Time, error) {
	switch {
	case jsonscan.Null(value):
		return time.Time{}, nil
	case value[0] != '"':
		return time.Time{}, jsonscan.NotA(field, "a time")
	}
	text, err := jsonscan.Text(field, value, maxTimeRaw)
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(layout, text)
	if err != nil {
		return time.Time{}, jsonscan.NotA(field, "a time")
	}
	return t.Local(), nil
}

func optionalMicroTime(field string, value []byte) (*metav1.MicroTime, error) {
	if jsonscan.Null(value) {
		return nil, nil
	}
	t, err := timeOf(field, value, metav1.RFC3339Micro)
	return &metav1.MicroTime{Time: t}, err
}

// present says only that a field is there and not null: the webhook's URL and
// the APIService's Service are read for that, not for their content.
type present bool

func (p *present) set(value []byte) {
	*p = present(!jsonscan.Null(value))
}

// array is a JSON array decoded one element at a time. It keeps the first most
// elements, and over says there was one more, which it does not decode: the
// elements after the bound cost a scan and nothing else.
type array[T any] struct {
	items []T
	over  bool
}

func arrayOf[T any](field string, data []byte, most int, element func([]byte) (T, error)) (array[T], error) {
	var a array[T]
	switch {
	case jsonscan.Null(data):
		return a, nil
	case data[0] != '[':
		return a, jsonscan.NotA(field, "an array")
	}
	for value := range jsonscan.Elements(data) {
		if len(a.items) == most {
			a.over = true
			return a, nil
		}
		item, err := element(value)
		if err != nil {
			return a, err
		}
		a.items = append(a.items, item)
	}
	return a, nil
}
