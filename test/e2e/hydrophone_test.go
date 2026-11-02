// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && (conformance || restore)

package e2e

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/clientcmd"
)

const hydrophone = "sigs.k8s.io/hydrophone@v0.8.0"

func runHydrophone(ctx context.Context, t *testing.T, access *tenantAccess, outputDir string, args ...string) {
	t.Helper()
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(access.kubeconfig, kubeconfig); err != nil {
		t.Fatal(err)
	}
	args = append([]string{"run", hydrophone, "--kubeconfig", kubeconfig, "--output-dir", outputDir, "--parallel", "4", "--disable-progress-status"}, args...)
	run := exec.CommandContext(ctx, "go", args...)
	run.Stdout, run.Stderr = os.Stdout, os.Stderr
	start := time.Now()
	err := run.Run()
	t.Logf("go %s ran for %s", strings.Join(args, " "), time.Since(start).Round(time.Second))
	if err != nil {
		t.Fatal(err)
	}
}
