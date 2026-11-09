// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package testenv_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"go.lodia.io/lodia/internal/testenv"
)

func TestOnlyTestFilesImportIt(t *testing.T) {
	out, err := exec.CommandContext(t.Context(), "go", "list", "-f", `{{.ImportPath}} {{join .Imports " "}}`, "../../...").Output()
	if err != nil {
		t.Fatal(err)
	}
	var seen int
	for line := range strings.Lines(string(out)) {
		fields := strings.Fields(line)
		seen++
		if slices.Contains(fields[1:], "go.lodia.io/lodia/internal/testenv") {
			t.Errorf("%s imports internal/testenv outside its test files", fields[0])
		}
	}
	if seen == 0 {
		t.Fatal("go list found no package")
	}
}

func TestAnAPIServerPastItsTimeLeavesItsGoroutines(t *testing.T) {
	dumps := startAndStop(t, time.Nanosecond)
	if len(dumps) != 1 {
		t.Fatalf("kept %v, want one file", dumps)
	}
	got, err := os.ReadFile(dumps[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "SIGQUIT: quit") || strings.Count(string(got), "\ngoroutine ") < 2 {
		t.Fatalf("no goroutines in %d bytes", len(got))
	}
}

func TestAnAPIServerThatStopsInTimeLeavesNothing(t *testing.T) {
	if dumps := startAndStop(t, time.Minute); len(dumps) != 0 {
		t.Fatalf("kept %v", dumps)
	}
}

func startAndStop(t *testing.T, after time.Duration) []string {
	t.Helper()
	if !testenv.Available() {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	dir := t.TempDir()
	t.Setenv("LODIA_APISERVER_DUMPS", dir)
	_, stop, err := testenv.StartDumping(t, &envtest.Environment{UseExistingCluster: new(false)}, after)
	if err != nil {
		t.Fatal(err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	dumps, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil {
		t.Fatal(err)
	}
	return dumps
}
