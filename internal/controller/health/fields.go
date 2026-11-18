// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.lodia.io/lodia/internal/jsonscan"
)

// The bounds below are Health's own, beside the ones every reader of a tenant
// shares (tenantclient's fields.go):
//   - maxTimeRaw: a time with microseconds and an offset is 32 bytes;
//   - maxHolderRaw: a holderIdentity is a name, an underscore and a UUID, at
//     most 290 bytes;
//   - maxKubeletRaw: maxKubeletVersion bytes with every character escaped, 384.
const (
	maxTimeRaw    = 64
	maxHolderRaw  = 512
	maxKubeletRaw = 6 * maxKubeletVersion
)

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
