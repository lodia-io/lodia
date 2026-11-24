// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"slices"
	"strings"
)

// containerLabel names the label every container the suite starts carries,
// with the kind cluster it started it for as its value. The Makefile removes
// the containers by the same label.
const containerLabel = "io.lodia.e2e.cluster"

var (
	containerID   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	containerName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)
)

func docker(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput() //nolint:gosec // the arguments are the suite's own
	if err != nil {
		return "", fmt.Errorf("docker %s: %w\n%s", args[0], err, out)
	}
	return string(out), nil
}

func dockerStdout(ctx context.Context, args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // the arguments are the suite's own
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("docker %s: %w\n%s", args[0], err, &stderr)
	}
	return stdout.String(), nil
}

// suiteContainers lists the containers the suite started for the kind
// cluster, each as format prints it, and keeps the words that match
// wellFormed: a warning docker prints is not a container.
func suiteContainers(ctx context.Context, cluster, format string, wellFormed *regexp.Regexp) ([]string, error) {
	out, err := dockerStdout(ctx, "ps", "--all", "--no-trunc", "--format", format, "--filter", "label="+containerLabel+"="+cluster)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(strings.Fields(out), func(word string) bool { return !wellFormed.MatchString(word) }), nil
}

func removeSuiteContainers(ctx context.Context, cluster string) error {
	ids, err := suiteContainers(ctx, cluster, "{{.ID}}", containerID)
	if err != nil || len(ids) == 0 {
		return err
	}
	_, err = docker(ctx, append([]string{"rm", "--force", "--volumes"}, ids...)...)
	return err
}

func nodeRunArgs(cluster, name, dns, image string) []string {
	args := []string{"run", "--detach", "--tty", "--name", name, "--hostname", name, "--network", "kind", "--label", containerLabel + "=" + cluster,
		"--privileged", "--security-opt", "seccomp=unconfined", "--security-opt", "apparmor=unconfined",
		"--cgroupns=private", "--tmpfs", "/tmp", "--tmpfs", "/run", "--volume", "/var",
		"--volume", "/lib/modules:/lib/modules:ro", "--restart=on-failure:1", "--init=false"}
	if dns != "" {
		args = append(args, "--dns", dns)
	}
	return append(args, image)
}

func dnsCreateArgs(cluster, name, image string) []string {
	return []string{"create", "--name", name, "--network", "kind", "--label", containerLabel + "=" + cluster, "--restart=on-failure:3", image, "-conf", "/Corefile"}
}
