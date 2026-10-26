// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package crds holds the CustomResourceDefinitions of the lodia.io API, which
// make generate writes from the types in v1alpha1.
package crds

import "embed"

// FS holds each CustomResourceDefinition as <group>_<plural>.yaml.
//
//go:embed *.yaml
var FS embed.FS
