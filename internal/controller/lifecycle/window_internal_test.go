// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"strings"
	"testing"
	"time"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func withWindow(begin, end, zone string) *lodiav1alpha1.ControlPlane {
	return &lodiav1alpha1.ControlPlane{Spec: lodiav1alpha1.ControlPlaneSpec{Maintenance: &lodiav1alpha1.Maintenance{
		Window: &lodiav1alpha1.MaintenanceWindow{Begin: begin, End: end, TimeZone: zone},
	}}}
}

func instant(t *testing.T, value string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return at
}

func TestTheWindowOpensAndNextOpens(t *testing.T) {
	for _, c := range []struct {
		name             string
		begin, end, zone string
		at               string
		open             bool
		next             string
	}{
		{"UTC by default, a second before it opens", "02:00", "04:00", "", "2026-11-10T01:59:59Z", false, "2026-11-10T02:00:00Z"},
		{"UTC by default, as it opens", "02:00", "04:00", "", "2026-11-10T02:00:00Z", true, ""},
		{"UTC by default, its last second", "02:00", "04:00", "", "2026-11-10T03:59:59Z", true, ""},
		{"UTC by default, as it ends it next opens the day after", "02:00", "04:00", "", "2026-11-10T04:00:00Z", false, "2026-11-11T02:00:00Z"},
		{"UTC named", "02:00", "04:00", "UTC", "2026-11-10T02:30:00Z", true, ""},
		{"a zone east of UTC, as it opens", "02:00", "04:00", "Europe/Berlin", "2026-11-10T01:00:00Z", true, ""},
		{"a zone east of UTC, at its UTC hours", "02:00", "04:00", "Europe/Berlin", "2026-11-10T03:00:00Z", false, "2026-11-11T01:00:00Z"},
		{"a zone half an hour off the hour", "02:00", "04:00", "Asia/Kolkata", "2026-11-09T20:30:00Z", true, ""},
		{"a zone west of UTC opens on its own date", "23:00", "23:30", "America/New_York", "2026-11-10T23:00:00Z", false, "2026-11-11T04:00:00Z"},
		{"across midnight, before midnight", "22:00", "02:00", "", "2026-11-10T23:30:00Z", true, ""},
		{"across midnight, after midnight", "22:00", "02:00", "", "2026-11-11T01:59:59Z", true, ""},
		{"across midnight, as it ends", "22:00", "02:00", "", "2026-11-11T02:00:00Z", false, "2026-11-11T22:00:00Z"},
		{"across midnight, a second before it opens", "22:00", "02:00", "", "2026-11-10T21:59:59Z", false, "2026-11-10T22:00:00Z"},
		{"ending at midnight, its last second", "22:00", "00:00", "", "2026-11-10T23:59:59Z", true, ""},
		{"ending at midnight, at midnight", "22:00", "00:00", "", "2026-11-11T00:00:00Z", false, "2026-11-11T22:00:00Z"},
		{"the day before Berlin skips an hour, a window inside it next opens two days on", "02:15", "02:45", "Europe/Berlin", "2026-03-28T02:00:00Z", false, "2026-03-30T00:15:00Z"},
		{"inside the hour Berlin skips in spring, it does not open that day", "02:15", "02:45", "Europe/Berlin", "2026-03-29T00:59:59Z", false, "2026-03-30T00:15:00Z"},
		{"inside the hour Berlin skips in spring, nor at the jump", "02:15", "02:45", "Europe/Berlin", "2026-03-29T01:00:00Z", false, "2026-03-30T00:15:00Z"},
		{"ending inside the skipped hour, open before the jump", "01:30", "02:30", "Europe/Berlin", "2026-03-29T00:59:59Z", true, ""},
		{"ending inside the skipped hour, it ends at the jump", "01:30", "02:30", "Europe/Berlin", "2026-03-29T01:00:00Z", false, "2026-03-29T23:30:00Z"},
		{"beginning inside the skipped hour, it opens at the jump", "02:30", "03:30", "Europe/Berlin", "2026-03-29T00:59:59Z", false, "2026-03-29T01:00:00Z"},
		{"beginning inside the skipped hour, open after the jump", "02:30", "03:30", "Europe/Berlin", "2026-03-29T01:29:59Z", true, ""},
		{"inside the hour Berlin repeats in autumn, open the first time", "02:15", "02:45", "Europe/Berlin", "2026-10-25T00:15:00Z", true, ""},
		{"inside the hour Berlin repeats in autumn, closed the second time", "02:15", "02:45", "Europe/Berlin", "2026-10-25T01:15:00Z", false, "2026-10-26T01:15:00Z"},
		{"ending inside the repeated hour, it ends the first time", "01:00", "02:30", "Europe/Berlin", "2026-10-25T00:29:59Z", true, ""},
		{"ending inside the repeated hour, closed the second time", "01:00", "02:30", "Europe/Berlin", "2026-10-25T01:10:00Z", false, "2026-10-26T00:00:00Z"},
		{"inside the hour New York skips in spring", "02:00", "02:59", "America/New_York", "2026-03-08T06:59:00Z", false, "2026-03-09T06:00:00Z"},
		{"inside the hour New York repeats in autumn, open the first time", "01:15", "01:45", "America/New_York", "2026-11-01T05:15:00Z", true, ""},
		{"inside the hour New York repeats in autumn, closed the second time", "01:15", "01:45", "America/New_York", "2026-11-01T06:15:00Z", false, "2026-11-02T06:15:00Z"},
	} {
		t.Run(c.name, func(t *testing.T) {
			w, err := windowOf(withWindow(c.begin, c.end, c.zone))
			if err != nil {
				t.Fatal(err)
			}
			open, next := w.state(instant(t, c.at))
			if open != c.open {
				t.Errorf("open is %t at %s, want %t", open, c.at, c.open)
			}
			if !c.open && !next.Equal(instant(t, c.next)) {
				t.Errorf("it next opens at %s, want %s", next.UTC().Format(time.RFC3339), c.next)
			}
		})
	}
}

func TestAZoneTheDatabaseLacksIsRefused(t *testing.T) {
	for _, zone := range []string{"Mars/Olympus_Mons", "Local", "../etc/passwd"} {
		if _, err := windowOf(withWindow("02:00", "04:00", zone)); err == nil || !strings.Contains(err.Error(), "spec.maintenance.window.timeZone "+zone) {
			t.Errorf("zone %q gives %v, want an error that names it", zone, err)
		}
	}
	if w, err := windowOf(&lodiav1alpha1.ControlPlane{}); w != nil || err != nil {
		t.Errorf("no window gives %+v, %v", w, err)
	}
}
