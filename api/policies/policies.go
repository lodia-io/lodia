// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package policies holds the ValidatingAdmissionPolicies of the lodia.io API
// and their bindings, which the chart installs and make generate copies into
// it.
package policies

import "embed"

// FS holds the policies of each resource with their bindings as
// <resource>.yaml.
//
//go:embed *.yaml
var FS embed.FS
