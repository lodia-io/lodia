// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"os"
	"slices"
	"strings"
)

// koEnvironment is the environment ko runs in, building into the kind cluster
// named cluster. ko refuses --platform beside GOOS, GOARCH or GOARM, even set
// empty (https://github.com/google/ko/blob/v0.19.1/pkg/commands/resolver.go#L79-L85).
func koEnvironment(cluster, version string) []string {
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool {
		name, _, _ := strings.Cut(kv, "=")
		return name == "GOOS" || name == "GOARCH" || name == "GOARM"
	})
	return append(env, "KO_DOCKER_REPO=kind.local", "KIND_CLUSTER_NAME="+cluster, "VERSION="+version)
}
