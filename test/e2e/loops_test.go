// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

const unknownFromCI = `status 500 Internal Server Error, Retry-After "", body "{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"rpc error: code = Unknown desc = unexpected HTTP status code received from server: 200 (OK); malformed header: missing HTTP content-type\",\"code\":500}\n"`

func answered(status, retryAfter, message string, code int) string {
	body := fmt.Sprintf(`{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":%q,"code":%d}`+"\n", message, code)
	return fmt.Sprintf("status %s, Retry-After %q, body %q", status, retryAfter, body)
}

type lost struct {
	mode string
	n    int64
	err  string
}

func step(where string, failedBefore int64, failures ...lost) loopRead {
	start := time.Date(2026, 11, 13, 13, 13, 48, 0, time.UTC)
	r := loopRead{
		where:  where,
		before: loopCounts{Time: start, Fresh: loopTally{OK: 1000, Failed: failedBefore}, Reused: loopTally{OK: 1000, Failed: failedBefore}},
		after:  loopCounts{Time: start.Add(30 * time.Second), Fresh: loopTally{OK: 1300, Failed: failedBefore}, Reused: loopTally{OK: 1300, Failed: failedBefore}},
	}
	var log strings.Builder
	if failedBefore > 0 {
		fmt.Fprintf(&log, "failed %s fresh %d: %s\n", start.Add(-time.Minute).Format(time.RFC3339Nano), failedBefore, answered("500 Internal Server Error", "", "etcdserver: leader changed", 500))
	}
	for _, f := range failures {
		if f.mode == "fresh" {
			r.after.Fresh.Failed++
		} else {
			r.after.Reused.Failed++
		}
		if f.n > 0 {
			fmt.Fprintf(&log, "failed %s %s %d: %s\n", start.Add(time.Second).Format(time.RFC3339Nano), f.mode, f.n, f.err)
		}
	}
	r.log = log.String()
	return r
}

func TestTheEtcdStepToleratesOneRequestInEachLoopLostToACallEtcdEndedWithoutAStatusAndTheAPIServerStepNone(t *testing.T) {
	internal := answered("500 Internal Server Error", "", endedWithoutStatus[0], 500)
	unknown := answered("500 Internal Server Error", "", endedWithoutStatus[1], 500)
	if unknown != unknownFromCI {
		t.Fatalf("the Unknown error as test/apiloop prints it is\n%s\nnot as run 43430828031 logged it:\n%s", unknown, unknownFromCI)
	}
	outside, inside := "outside the tenant", "inside the tenant"
	later := step(inside, 0)
	later.log += fmt.Sprintf("failed %s fresh 1: context deadline exceeded\n", later.after.Time.Add(time.Second).Format(time.RFC3339Nano))
	cases := []struct {
		name      string
		reads     []loopRead
		tolerated []string
		want      string
		tolerates int
	}{
		{"no request lost in the etcd step", []loopRead{step(outside, 1), step(inside, 0)}, endedWithoutStatus, "", 0},
		{"no request lost in the API server step", []loopRead{step(outside, 1), step(inside, 0)}, nil, "", 0},
		{"a failure after the step's counts", []loopRead{step(outside, 1), later}, nil, "", 0},
		{"server closed the stream without sending trailers", []loopRead{step(outside, 1, lost{"reused", 2, internal}), step(inside, 0)}, endedWithoutStatus, "", 1},
		{"missing HTTP content-type", []loopRead{step(outside, 1, lost{"fresh", 2, unknownFromCI}), step(inside, 0)}, endedWithoutStatus, "", 1},
		{"one in each loop", []loopRead{step(outside, 1, lost{"reused", 2, internal}), step(inside, 0, lost{"reused", 1, unknown})}, endedWithoutStatus, "", 2},
		{"another error", []loopRead{step(outside, 1), step(inside, 0, lost{"fresh", 1, answered("500 Internal Server Error", "", "etcdserver: leader changed", 500)})}, endedWithoutStatus, "inside the tenant: 1 requests failed; the step tolerates one in each loop", 0},
		{"a timeout", []loopRead{step(outside, 1, lost{"fresh", 2, "context deadline exceeded"}), step(inside, 0)}, endedWithoutStatus, "outside the tenant: 1 requests failed; the step tolerates one in each loop", 0},
		{"the message answered 503", []loopRead{step(outside, 1, lost{"fresh", 2, answered("503 Service Unavailable", "", endedWithoutStatus[0], 503)}), step(inside, 0)}, endedWithoutStatus, "outside the tenant: 1 requests failed; the step tolerates one in each loop", 0},
		{"the message with a Retry-After", []loopRead{step(outside, 1, lost{"fresh", 2, answered("500 Internal Server Error", "1", endedWithoutStatus[0], 500)}), step(inside, 0)}, endedWithoutStatus, "outside the tenant: 1 requests failed; the step tolerates one in each loop", 0},
		{"two in one loop", []loopRead{step(outside, 1, lost{"fresh", 2, internal}, lost{"reused", 2, unknown}), step(inside, 0)}, endedWithoutStatus, "outside the tenant: 2 requests failed; the step tolerates one in each loop", 0},
		{"two in one loop and one in the other", []loopRead{step(outside, 1, lost{"reused", 2, internal}), step(inside, 0, lost{"fresh", 1, unknown}, lost{"reused", 1, internal})}, endedWithoutStatus, "inside the tenant: 2 requests failed; the step tolerates one in each loop", 1},
		{"one in the API server step", []loopRead{step(outside, 1, lost{"fresh", 2, internal}), step(inside, 0)}, nil, "outside the tenant: 1 requests failed; the step tolerates none", 0},
		{"one the log does not hold", []loopRead{step(outside, 1, lost{"fresh", 0, ""}), step(inside, 0)}, endedWithoutStatus, "1 failed, and the log holds 0 of them", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			report, faults := judgeLoops(c.reads, c.tolerated)
			switch got := strings.Join(faults, "\n"); {
			case c.want == "" && got != "":
				t.Fatalf("want the step to pass, got %s", got)
			case c.want != "" && !strings.Contains(got, c.want):
				t.Fatalf("want a fault with %q, got %q", c.want, got)
			}
			tolerated := len(slices.DeleteFunc(slices.Clone(report), func(line string) bool {
				return !strings.HasPrefix(line, "the step tolerates the loop's one failed request")
			}))
			if tolerated != c.tolerates || len(report) < 2*len(c.reads) {
				t.Fatalf("the report tolerates %d failed requests, want %d, in %d lines for %d loops: %q", tolerated, c.tolerates, len(report), len(c.reads), report)
			}
		})
	}
}

func TestAStepWithFewerAnswersThanTheRateSendsOrAnEmptySliceFails(t *testing.T) {
	slow := step("outside the tenant", 0)
	slow.after.Reused.OK = slow.before.Reused.OK + 284
	empty := step("inside the tenant", 0)
	empty.after.Fresh.Empty = 1
	for name, r := range map[string]loopRead{"284 of 300 answered": slow, "one empty slice": empty} {
		t.Run(name, func(t *testing.T) {
			if _, faults := judgeLoops([]loopRead{r}, endedWithoutStatus); len(faults) != 1 {
				t.Fatalf("want one fault, got %q", faults)
			}
		})
	}
}
