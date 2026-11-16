// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestTheTimelineGoesByTimeWhateverOrderTheConditionsArriveIn(t *testing.T) {
	created := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	at := func(seconds int) time.Time { return created.Add(time.Duration(seconds) * time.Second) }
	got := timeline(created, []stage{
		{at(44), "Ready=True Ready"},
		{at(9), "etcd pod team-gate-etcd-0 Ready"},
		{at(2), "CertificatesReady=True Issued"},
		{at(9), "DatastoreReady=True Ready"},
		{at(31), "APIServerReady=True Ready"},
	})
	want := []string{
		"    2s CertificatesReady=True Issued",
		"    9s etcd pod team-gate-etcd-0 Ready",
		"    9s DatastoreReady=True Ready",
		"   31s APIServerReady=True Ready",
		"   44s Ready=True Ready",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}
}

func TestStagesAtTheSameTimeKeepTheOrderTheyAreGivenIn(t *testing.T) {
	created := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	var stages []stage
	var want []string
	for i := range 16 {
		stages = append(stages,
			stage{created.Add(time.Duration(16-i) * time.Second), fmt.Sprintf("later %02d", 15-i)},
			stage{created, fmt.Sprintf("at creation %02d", i)})
		want = append(want, fmt.Sprintf("    0s at creation %02d", i))
	}
	for i := range 16 {
		want = append(want, fmt.Sprintf("%5ds later %02d", i+1, i))
	}
	if got := timeline(created, stages); !slices.Equal(got, want) {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}
}
