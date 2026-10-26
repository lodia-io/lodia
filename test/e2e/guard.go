// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package e2e is Lodia's end-to-end suite. It runs only against a kind
// cluster named on the command line (docs/decisions/e2e-on-a-named-cluster.md);
// the suite itself is behind the e2e build tag.
package e2e

import (
	"errors"
	"fmt"
	"strings"

	"k8s.io/client-go/tools/clientcmd"
)

// ContextPrefix starts the name of every context the suite agrees to use:
// kind names a cluster's context kind-<name>, and make e2e names clusters
// lodia-e2e-<something>.
const ContextPrefix = "kind-lodia-e2e"

// RequireNamedCluster refuses any kubeconfig but one with a single context
// whose name starts with ContextPrefix, so the suite never reaches a cluster
// through someone's current context.
func RequireNamedCluster(path string) error {
	if path == "" {
		return errors.New("E2E_KUBECONFIG is not set: the e2e suite runs only against a cluster named on the command line; make e2e creates one")
	}
	cfg, err := clientcmd.LoadFromFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if len(cfg.Contexts) != 1 {
		return fmt.Errorf("%s has %d contexts; the e2e suite needs exactly one", path, len(cfg.Contexts))
	}
	if _, ok := cfg.Contexts[cfg.CurrentContext]; !ok {
		return fmt.Errorf("%s: current context %q does not exist", path, cfg.CurrentContext)
	}
	if !strings.HasPrefix(cfg.CurrentContext, ContextPrefix) {
		return fmt.Errorf("context %q does not start with %q: the e2e suite runs only against its own kind cluster", cfg.CurrentContext, ContextPrefix)
	}
	return nil
}
