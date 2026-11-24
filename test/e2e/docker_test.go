// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func fakeDocker(t *testing.T, script string) (calls string) {
	t.Helper()
	bin, calls := t.TempDir(), filepath.Join(t.TempDir(), "calls")
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte("#!/bin/sh\necho \"$*\" >> "+calls+"\n"+script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	return calls
}

func dockerCalls(t *testing.T, calls string) []string {
	t.Helper()
	log, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(log)), "\n")
}

func TestTheContainersTheSuiteRemovesAreTheIDsDockerPrintsOnStdout(t *testing.T) {
	cases := []struct {
		name, ps, want string
		wantErr        bool
	}{
		{"a warning on stderr", `echo "WARNING: container deadbeef0123 is slow" >&2; echo abcdef012345; echo 0123456789ab`, "rm --force --volumes abcdef012345 0123456789ab", false},
		{"words on stdout that are no ID", `echo "WARNING: the daemon is slow"; echo --all; echo abcdef012345; echo ABCDEF012345`, "rm --force --volumes abcdef012345", false},
		{"a warning only", `echo "WARNING: the daemon is slow" >&2`, "", false},
		{"no container", `:`, "", false},
		{"a failed listing", `echo "Cannot connect to the Docker daemon" >&2; exit 1`, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			calls := fakeDocker(t, "case \"$1\" in ps) "+c.ps+" ;; esac\n")
			err := removeSuiteContainers(t.Context(), "lodia-e2e-x")
			if (err != nil) != c.wantErr {
				t.Fatalf("got %v, want an error: %v", err, c.wantErr)
			}
			want := []string{"ps --all --format {{.ID}} --filter label=io.lodia.e2e.cluster=lodia-e2e-x"}
			if c.want != "" {
				want = append(want, c.want)
			}
			if got := dockerCalls(t, calls); !slices.Equal(got, want) {
				t.Fatalf("docker was called as %q, want %q", got, want)
			}
		})
	}
}

func TestEveryContainerTheSuiteStartsCarriesTheLabelOfItsCluster(t *testing.T) {
	for name, args := range map[string][]string{
		"a node":            nodeRunArgs("lodia-e2e-x", "lodia-e2e-x-tenant-node-1", "", "kindest/node:v1"),
		"a node with a DNS": nodeRunArgs("lodia-e2e-x", "lodia-e2e-x-tenant-node-1", "192.0.2.1", "kindest/node:v1"),
		"the DNS container": dnsCreateArgs("lodia-e2e-x", "lodia-e2e-x-dns", "coredns:v1"),
	} {
		if !strings.Contains(" "+strings.Join(args, " ")+" ", " --label io.lodia.e2e.cluster=lodia-e2e-x ") {
			t.Errorf("%s starts with %q, which carries no label of its cluster", name, args)
		}
	}
}
