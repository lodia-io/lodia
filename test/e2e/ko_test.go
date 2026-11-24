// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"slices"
	"strings"
	"testing"
)

func TestKoRunsWithoutTheVariablesItRefusesBesidePlatform(t *testing.T) {
	t.Setenv("GOARCH", "amd64")
	t.Setenv("GOOS", "linux")
	t.Setenv("GOARM", "")
	t.Setenv("GOARCHIVE", "x")
	env := koEnvironment("lodia-e2e-x", "v1")
	for _, kv := range env {
		if name, _, _ := strings.Cut(kv, "="); name == "GOOS" || name == "GOARCH" || name == "GOARM" {
			t.Errorf("ko runs with %s", kv)
		}
	}
	for _, want := range []string{"GOARCHIVE=x", "KO_DOCKER_REPO=kind.local", "KIND_CLUSTER_NAME=lodia-e2e-x", "VERSION=v1"} {
		if !slices.Contains(env, want) {
			t.Errorf("ko runs without %s", want)
		}
	}
}
