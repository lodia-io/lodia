// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	apiLoopRate    = 10
	apiLoopTimeout = 5 * time.Second
)

// endedWithoutStatus are the errors the tenant API server's etcd client gives
// a call etcd ended without a gRPC status (test/e2e/README.md, gate G2).
var endedWithoutStatus = []string{
	"rpc error: code = Internal desc = server closed the stream without sending trailers",
	"rpc error: code = Unknown desc = unexpected HTTP status code received from server: 200 (OK); malformed header: missing HTTP content-type",
}

type loopTally struct {
	OK      int64 `json:"ok"`
	Failed  int64 `json:"failed"`
	Empty   int64 `json:"empty"`
	Retried int64 `json:"retried"`
}

type loopCounts struct {
	Time   time.Time `json:"time"`
	Fresh  loopTally `json:"fresh"`
	Reused loopTally `json:"reused"`
}

type loopRead struct {
	where         string
	before, after loopCounts
	log           string
}

type lostRequest struct {
	where, mode, err string
}

// judgeLoops reports what each loop answered over a step and what fails it;
// one failed request passes only when tolerated names its message.
func judgeLoops(reads []loopRead, tolerated []string) (report, faults []string) {
	var lost []lostRequest
	for _, r := range reads {
		seconds := r.after.Time.Sub(r.before.Time).Seconds()
		expected := apiLoopRate * seconds
		for _, mode := range []struct {
			name, key     string
			before, after loopTally
		}{{"new connections", "fresh", r.before.Fresh, r.after.Fresh}, {"reused connections", "reused", r.before.Reused, r.after.Reused}} {
			ok, failed, empty, retried := mode.after.OK-mode.before.OK, mode.after.Failed-mode.before.Failed, mode.after.Empty-mode.before.Empty, mode.after.Retried-mode.before.Retried
			report = append(report, fmt.Sprintf("%s on %s, over %.0fs: %d answered of %.0f sent at %d per second, %d failed, %d found the kubernetes EndpointSlice empty, %d followed a Retry-After", r.where, mode.name, seconds, ok, expected, apiLoopRate, failed, empty, retried))
			if empty != 0 || float64(ok) < 0.95*expected {
				faults = append(faults, fmt.Sprintf("%s on %s: %d found the EndpointSlice empty; %d answered, want at least 95%% of %.0f", r.where, mode.name, empty, ok, expected))
			}
			errs := failures(r.log, mode.key, mode.before.Failed, mode.after.Failed)
			if int64(len(errs)) != failed {
				faults = append(faults, fmt.Sprintf("%s on %s: %d failed, and the log holds %d of them", r.where, mode.name, failed, len(errs)))
			}
			for _, err := range errs {
				lost = append(lost, lostRequest{r.where, mode.name, err})
			}
		}
	}
	lines := make([]string, len(lost))
	for i, l := range lost {
		lines[i] = fmt.Sprintf("%s on %s: %s", l.where, l.mode, l.err)
	}
	switch {
	case len(lost) == 0:
	case len(lost) == 1 && tolerable(lost[0].err, tolerated):
		report = append(report, "the step tolerates its one failed request, a call etcd ended without a status: "+lines[0])
	case len(tolerated) == 0:
		faults = append(faults, fmt.Sprintf("%d requests failed; the step tolerates none:\n%s", len(lost), strings.Join(lines, "\n")))
	default:
		faults = append(faults, fmt.Sprintf("%d requests failed; the step tolerates one, answered 500 with one of %q:\n%s", len(lost), tolerated, strings.Join(lines, "\n")))
	}
	return report, faults
}

// failures returns the errors of the failures numbered after from and up to
// to on one kind of connection, as test/apiloop prints them.
func failures(log, mode string, from, to int64) []string {
	var errs []string
	for line := range strings.Lines(log) {
		rest, ok := strings.CutPrefix(strings.TrimSuffix(line, "\n"), "failed ")
		if !ok {
			continue
		}
		fields := strings.SplitN(rest, " ", 3)
		if len(fields) != 3 || fields[1] != mode {
			continue
		}
		number, err, ok := strings.Cut(fields[2], ": ")
		if n, parseErr := strconv.ParseInt(number, 10, 64); ok && parseErr == nil && n > from && n <= to {
			errs = append(errs, err)
		}
	}
	return errs
}

func tolerable(err string, tolerated []string) bool {
	quoted, ok := strings.CutPrefix(err, `status 500 Internal Server Error, Retry-After "", body `)
	if !ok {
		return false
	}
	body, unquoteErr := strconv.Unquote(quoted)
	if unquoteErr != nil {
		return false
	}
	var status metav1.Status
	if json.Unmarshal([]byte(body), &status) != nil {
		return false
	}
	return slices.Contains(tolerated, status.Message)
}
