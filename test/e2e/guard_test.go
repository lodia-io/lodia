// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func kubeconfig(t *testing.T, current string, contexts ...string) string {
	t.Helper()
	return kubeconfigFor(t, "https://127.0.0.1:6443", current, contexts...)
}

func kubeconfigFor(t *testing.T, server, current string, contexts ...string) string {
	t.Helper()
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["c"] = &clientcmdapi.Cluster{Server: server}
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

func withoutHelmKubeVariables(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); strings.HasPrefix(name, helmKubePrefix) {
			t.Setenv(name, "")
		}
	}
}

func TestRequireNamedCluster(t *testing.T) {
	withoutHelmKubeVariables(t)
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

func TestRequireNamedClusterRefusesTheVariablesHelmLaysOverItsKubeconfig(t *testing.T) {
	withoutHelmKubeVariables(t)
	cases := []struct {
		name, want string
		env        map[string]string
	}{
		{"the context", "sets HELM_KUBECONTEXT:", map[string]string{"HELM_KUBECONTEXT": "segla-wg"}},
		{"the API server and a token", "sets HELM_KUBEAPISERVER, HELM_KUBETOKEN:", map[string]string{"HELM_KUBETOKEN": "t", "HELM_KUBEAPISERVER": "https://192.0.2.1"}},
		{"a variable set empty", "", map[string]string{"HELM_KUBECONTEXT": ""}},
		{"variables of helm that name no cluster", "", map[string]string{"HELM_DEBUG": "true", "HELM_NAMESPACE": "x"}},
		{"none", "", nil},
	}
	path := kubeconfig(t, "kind-lodia-e2e-local", "kind-lodia-e2e-local")
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for name, value := range c.env {
				t.Setenv(name, value)
			}
			err := RequireNamedCluster(path)
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("want accepted, got %v", err)
			case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
				t.Fatalf("want an error with %q, got %v", c.want, err)
			}
		})
	}
}
