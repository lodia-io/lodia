// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package testenv_test

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
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
