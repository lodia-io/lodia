// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"fmt"
	"time"
	_ "time/tzdata"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// window is spec.maintenance.window: daily, from begin up to end, in minutes
// after midnight in zone, across midnight when end is not after begin.
type window struct {
	begin, end int
	zone       *time.Location
}

// windowOf reads cp's window, nil when it has none, and fails on a zone the
// operator's zone database lacks. "Local" names the operator's own zone, not
// one of the database's.
func windowOf(cp *lodiav1alpha1.ControlPlane) (*window, error) {
	if cp.Spec.Maintenance == nil || cp.Spec.Maintenance.Window == nil {
		return nil, nil
	}
	spec := cp.Spec.Maintenance.Window
	zone, err := time.UTC, error(nil)
	switch spec.TimeZone {
	case "":
	case "Local":
		err = fmt.Errorf("spec.maintenance.window.timeZone %s is not in the operator's zone database", spec.TimeZone)
	default:
		if zone, err = time.LoadLocation(spec.TimeZone); err != nil {
			err = fmt.Errorf("spec.maintenance.window.timeZone %s is not in the operator's zone database", spec.TimeZone)
		}
	}
	if err != nil {
		return nil, err
	}
	return &window{begin: minutes(spec.Begin), end: minutes(spec.End), zone: zone}, nil
}

// minutes reads HH:MM, which the schema's pattern guarantees.
func minutes(clock string) int {
	return (int(clock[0]-'0')*10+int(clock[1]-'0'))*60 + int(clock[3]-'0')*10 + int(clock[4]-'0')
}

// state says whether w is open at now and, when it is not, when it opens next.
func (w *window) state(now time.Time) (open bool, next time.Time) {
	local := now.In(w.zone)
	today := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC)
	for day := -1; day <= 2; day++ {
		start, end := w.span(today.AddDate(0, 0, day))
		switch {
		case !start.Before(end):
		case !now.Before(start) && now.Before(end):
			open = true
		case start.After(now) && next.IsZero():
			next = start
		}
	}
	return open, next
}

// span is the window that opens on the local date of date, a UTC midnight: it
// opens when the clock first shows begin and closes when it first shows end
// after that, so a window inside an hour daylight saving skips is empty, and
// one inside an hour it repeats opens once.
func (w *window) span(date time.Time) (start, end time.Time) {
	start = w.first(date, w.begin)
	if w.end > w.begin {
		return start, w.first(date, w.end)
	}
	return start, w.first(date.AddDate(0, 0, 1), w.end)
}

// first is the first instant of the local date of date whose time of day is
// minute or later: the earlier of two instants when daylight saving repeats
// that time, and the end of the jump when it skips it.
func (w *window) first(date time.Time, minute int) time.Time {
	wall := date.Add(time.Duration(minute) * time.Minute)
	var found time.Time
	for _, probe := range []time.Time{wall.Add(-24 * time.Hour), wall.Add(24 * time.Hour)} {
		_, offset := probe.In(w.zone).Zone()
		t := wall.Add(-time.Duration(offset) * time.Second)
		if wallClock(t.In(w.zone)).Equal(wall) && (found.IsZero() || t.Before(found)) {
			found = t
		}
	}
	if !found.IsZero() {
		return found
	}
	_, after := wall.Add(24 * time.Hour).In(w.zone).Zone()
	_, jump := wall.Add(-time.Duration(after) * time.Second).In(w.zone).ZoneBounds()
	return jump
}

// wallClock is the date and time t shows in its zone, as a UTC instant.
func wallClock(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}
