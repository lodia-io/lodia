// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Command minors prints the minors Lodia runs, for make restore-keys.
package main

import (
	"fmt"

	"go.lodia.io/lodia/internal/versions"
)

func main() {
	for _, minor := range versions.Minors() {
		fmt.Println(minor)
	}
}
