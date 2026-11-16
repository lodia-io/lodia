// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
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
