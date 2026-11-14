// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"strings"
	"testing"
)

func TestATimeFieldHoldingAnObjectOrAnArrayIsNotATimeWhateverItsSize(t *testing.T) {
	const size = 4 << 20
	for _, c := range []struct {
		name  string
		value string
	}{
		{"an object", `{"a":1}`},
		{"an array", `[1]`},
		{"a number", `5`},
		{"an object of 4 MiB", `{"a":"` + strings.Repeat("\xff", size) + `"}`},
		{"an array of 4 MiB", `["` + strings.Repeat("\xff", size) + `"]`},
	} {
		t.Run(c.name, func(t *testing.T) {
			lease := []byte(`{"spec":{"renewTime":` + c.value + `}}`)
			condition := []byte(`{"type":"Ready","lastTransitionTime":` + c.value + `}`)
			var leaseErr, conditionErr error
			got := allocated(func() {
				_, leaseErr = leaderLeaseFrom(lease)
				_, conditionErr = nodeConditionOf(condition)
			})
			if leaseErr == nil || leaseErr.Error() != "spec.renewTime is not a time" || conditionErr == nil || conditionErr.Error() != "status.conditions.lastTransitionTime is not a time" {
				t.Errorf("a time that is %s read as %v and %v, want a failed read that names the field", c.name, leaseErr, conditionErr)
			}
			if limit := uint64(size / 16); got > limit {
				t.Errorf("the reads allocated %d bytes, want at most %d", got, limit)
			}
		})
	}
}

func TestATimeIsParsedWithTheLayoutOfItsKind(t *testing.T) {
	for _, c := range []struct {
		name  string
		read  func() error
		valid bool
	}{
		{"a condition's time without a fraction", func() error {
			_, err := nodeConditionOf([]byte(`{"type":"Ready","lastTransitionTime":"2026-11-13T10:00:00Z"}`))
			return err
		}, true},
		{"a condition's time with a fraction and an offset", func() error {
			_, err := nodeConditionOf([]byte(`{"type":"Ready","lastTransitionTime":"2026-11-13T10:00:00.5+02:00"}`))
			return err
		}, true},
		{"a Lease's time with microseconds", func() error {
			_, err := leaderLeaseFrom([]byte(`{"spec":{"renewTime":"2026-11-13T10:00:00.000001Z"}}`))
			return err
		}, true},
		{"a Lease's time without microseconds", func() error {
			_, err := leaderLeaseFrom([]byte(`{"spec":{"renewTime":"2026-11-13T10:00:00Z"}}`))
			return err
		}, false},
		{"a condition's time of letters", func() error {
			_, err := nodeConditionOf([]byte(`{"type":"Ready","lastTransitionTime":"yesterday"}`))
			return err
		}, false},
		{"a condition's time that is null", func() error {
			_, err := nodeConditionOf([]byte(`{"type":"Ready","lastTransitionTime":null}`))
			return err
		}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			if err := c.read(); (err == nil) != c.valid {
				t.Errorf("the read returned %v, want a valid time: %t", err, c.valid)
			}
		})
	}
}
