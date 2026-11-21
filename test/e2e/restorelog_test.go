// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import "testing"

func TestTheBaseSnapshotOfARestoreIsNamedByItsLastRevision(t *testing.T) {
	for name, c := range map[string]struct {
		message string
		last    int64
		named   bool
	}{
		"a full snapshot":             {`time="2026-11-21T14:00:00Z" level=info msg="Restoring from base snapshot: Backup-1763733600/Full-00000000-00001234-1763733600"`, 1234, true},
		"a compressed one":            {`msg="Restoring from base snapshot: Backup-1763733600/Full-00000000-00001234-1763733600.gz"`, 1234, true},
		"a final one":                 {`msg="Restoring from base snapshot: Backup-1763733600/Full-00000000-00001234-1763733600.final"`, 1234, true},
		"a start past zero":           {`msg="Restoring from base snapshot: Backup-1763733600/Full-00000100-00001234-1763733600"`, 1234, true},
		"a delta snapshot":            {`msg="Restoring from base snapshot: Backup-1763733600/Incr-00001235-00001300-1763733660"`, 0, false},
		"another line of the restore": {`msg="Applying first delta snapshot Backup-1763733600/Incr-00001235-00001300-1763733660"`, 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			if last, named := baseRevision(c.message); last != c.last || named != c.named {
				t.Errorf("baseRevision(%q) = %d, %t, want %d, %t", c.message, last, named, c.last, c.named)
			}
		})
	}
}
