// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func kubeconfig(t *testing.T, current string, contexts ...string) string {
	t.Helper()
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["c"] = &clientcmdapi.Cluster{Server: "https://127.0.0.1:6443"}
	cfg.AuthInfos["u"] = &clientcmdapi.AuthInfo{Token: "t"}
	for _, name := range contexts {
		cfg.Contexts[name] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u"}
	}
	cfg.CurrentContext = current
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*cfg, path); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRequireNamedCluster(t *testing.T) {
	cases := []struct {
		name, path, want string
	}{
		{"no kubeconfig", "", "E2E_KUBECONFIG is not set"},
		{"missing file", filepath.Join(t.TempDir(), "absent"), "read "},
		{"two contexts", kubeconfig(t, "kind-lodia-e2e-local", "kind-lodia-e2e-local", "segla-wg"), "needs exactly one"},
		{"someone's cluster", kubeconfig(t, "segla-wg", "segla-wg"), "does not start with"},
		{"another kind cluster", kubeconfig(t, "kind-lodia-hardway", "kind-lodia-hardway"), "does not start with"},
		{"current context absent", kubeconfig(t, "kind-lodia-e2e-x", "kind-lodia-e2e-y"), "does not exist"},
		{"the e2e cluster", kubeconfig(t, "kind-lodia-e2e-local", "kind-lodia-e2e-local"), ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := RequireNamedCluster(c.path)
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("want accepted, got %v", err)
			case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
				t.Fatalf("want an error with %q, got %v", c.want, err)
			}
		})
	}
}
