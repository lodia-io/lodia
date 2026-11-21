// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"regexp"
	"strconv"
)

var baseSnapshot = regexp.MustCompile(`Restoring from base snapshot: \S*Full-\d+-(\d+)-\d+`)

// baseRevision is the last revision of the full snapshot a restore's log line
// names, Full-<start>-<last>-<time> in etcd-backup-restore's naming
// (https://github.com/gardener/etcd-backup-restore/blob/v0.45.3/pkg/types/snapstore.go#L118-L121).
func baseRevision(message string) (int64, bool) {
	m := baseSnapshot.FindStringSubmatch(message)
	if m == nil {
		return 0, false
	}
	last, err := strconv.ParseInt(m[1], 10, 64)
	return last, err == nil
}
