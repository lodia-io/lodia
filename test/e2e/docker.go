// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"context"
	"fmt"
	"os/exec"
)

// containerLabel names the label every container the suite starts carries,
// with the kind cluster it started it for as its value. The Makefile removes
// the containers by the same label.
const containerLabel = "io.lodia.e2e.cluster"

func docker(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput() //nolint:gosec // the arguments are the suite's own
	if err != nil {
		return "", fmt.Errorf("docker %s: %w\n%s", args[0], err, out)
	}
	return string(out), nil
}
