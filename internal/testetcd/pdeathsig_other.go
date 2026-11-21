// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package testetcd

import "os/exec"

// Only Linux ends etcd with the test binary; CONTRIBUTING.md says how to find
// a leftover elsewhere.
func endWithTheTestBinary(*exec.Cmd) {}
