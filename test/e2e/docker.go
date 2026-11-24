// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

// containerLabel names the label every container the suite starts carries,
// with the kind cluster it started it for as its value. The Makefile removes
// the containers by the same label.
const containerLabel = "io.lodia.e2e.cluster"
