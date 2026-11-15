// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

const (
	OwnProcessEnv = "LODIA_HEALTH_TEST_IN_A_PROCESS_OF_ITS_OWN"

	mostGoroutinesOfATestRunAlone = 8
)

func inAProcessOfItsOwn(t *testing.T) (ranApart bool) {
	t.Helper()
	if os.Getenv(OwnProcessEnv) == t.Name() {
		return false
	}
	command := exec.Command(os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.count=1", "-test.v")
	command.Env = append(os.Environ(), OwnProcessEnv+"="+t.Name())
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "--- PASS: "+t.Name()+" ") {
		t.Fatalf("%s did not pass in a process of its own: %v\n%s", t.Name(), err, output)
	}
	return true
}

func TestATestInAProcessOfItsOwnRunsBesideNoGoroutineOfTheSuite(t *testing.T) {
	if inAProcessOfItsOwn(t) {
		return
	}
	got := runtime.NumGoroutine()
	t.Logf("the test runs beside %d goroutines in all", got-1)
	if got > mostGoroutinesOfATestRunAlone {
		t.Errorf("the test runs among %d goroutines, want at most %d: the test's own, the runtime's and the testing package's", got, mostGoroutinesOfATestRunAlone)
	}
}
