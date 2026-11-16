// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"fmt"
	"slices"
	"time"
)

// stage is a step on the way to Ready, at the time an object records it.
type stage struct {
	at   time.Time
	what string
}

// timeline is one line a stage, in the order of their times, each with its
// time from created; stages at the same time keep the order given.
func timeline(created time.Time, stages []stage) []string {
	stages = slices.Clone(stages)
	slices.SortStableFunc(stages, func(a, b stage) int { return a.at.Compare(b.at) })
	lines := make([]string, len(stages))
	for i, s := range stages {
		lines[i] = fmt.Sprintf("%5.0fs %s", s.at.Sub(created).Seconds(), s.what)
	}
	return lines
}
