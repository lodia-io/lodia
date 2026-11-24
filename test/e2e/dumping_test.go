// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build !e2e

package e2e

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

const (
	dumpingChild  = "E2E_DUMPING_CHILD"
	dumpingMarker = "E2E_DUMPING_MARKER"
	nodeConsole   = `case "$1" in ps) echo e2e-node ;; logs) echo "console of $2" ;; exec) exit 126 ;; esac` + "\n"
)

func dumpsInto(t *testing.T) string {
	t.Helper()
	previous := dumpDir
	dumpDir = t.TempDir()
	dumped.Clear()
	t.Cleanup(func() {
		dumpDir = previous
		dumped.Clear()
	})
	return dumpDir
}

func clusterToDump(t *testing.T) string {
	t.Helper()
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	return kubeconfigFor(t, server.URL, "kind-lodia-e2e-x", "kind-lodia-e2e-x")
}

func awaitDump(t *testing.T, name, calls string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, started := dumped.Load(name); started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no dump %s was started", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
	console := filepath.Join(dumpDir, name, "containers", "e2e-node", "console.log")
	for {
		if _, err := os.Stat(console); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the dump %s holds no %s", name, console)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for !slices.ContainsFunc(dockerCalls(t, calls), func(call string) bool { return strings.HasPrefix(call, "exec ") }) {
		if time.Now().After(deadline) {
			t.Fatalf("the dump %s ran no journalctl", name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestADumpReplacesWhatAnEarlierRunLeftThere(t *testing.T) {
	dumps := dumpsInto(t)
	fakeDocker(t, nodeConsole)
	cfg := envconf.NewWithKubeConfig(clusterToDump(t))
	stale := filepath.Join(dumps, "failure", "stale.txt")
	if err := write(stale, []byte("an earlier run")); err != nil {
		t.Fatal(err)
	}
	dumpCluster(t.Context(), cfg, "failure")
	if _, err := os.Stat(stale); err == nil {
		t.Error("the dump left a file of an earlier run")
	}
	console := filepath.Join(dumps, "failure", "containers", "e2e-node", "console.log")
	if got, err := os.ReadFile(console); err != nil || string(got) != "console of e2e-node\n" {
		t.Fatalf("the console is %q, %v", got, err)
	}
	later := filepath.Join(dumps, "failure", "later.txt")
	if err := write(later, []byte("written after the dump")); err != nil {
		t.Fatal(err)
	}
	dumpCluster(t.Context(), cfg, "failure")
	if _, err := os.Stat(later); err != nil {
		t.Errorf("a second dump of the same name replaced the first: %v", err)
	}
}

func TestTheDumpBeforeTheTimeoutIsArmedWhereTheTimeoutIsGiven(t *testing.T) {
	dumpsInto(t)
	calls := fakeDocker(t, nodeConsole)
	cfg := envconf.NewWithKubeConfig(clusterToDump(t))
	dumpBeforeTimeout(cfg, dumpMargin+100*time.Millisecond)
	awaitDump(t, "timeout", calls)
}

func TestNoDumpBeforeTheTimeoutIsArmedWhereGoTestHasNoTimeout(t *testing.T) {
	dumps := dumpsInto(t)
	fakeDocker(t, nodeConsole)
	dumpBeforeTimeout(envconf.NewWithKubeConfig(clusterToDump(t)), 0)
	time.Sleep(300 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(dumps, "timeout")); err == nil {
		t.Error("a dump was written")
	}
}

func TestADumpEndsWithItsContextWhereTheAPIServerDoesNotAnswer(t *testing.T) {
	fakeDocker(t, ":\n")
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	dir := t.TempDir()
	done := make(chan error, 1)
	go func() { done <- Dump(ctx, &rest.Config{Host: server.URL}, "lodia-e2e-x", dir) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v, want the deadline of the context", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the dump outlived its context")
	}
}

func TestTheEnvironmentDumpsOnceAtTheFirstFailedStepBeforeTheTeardown(t *testing.T) {
	for _, c := range []struct {
		scenario string
		teardown bool
	}{{"assessments", true}, {"setup", false}} {
		t.Run(c.scenario, func(t *testing.T) {
			dumps := t.TempDir()
			marker := filepath.Join(t.TempDir(), "marker")
			calls := fakeDocker(t, nodeConsole)
			child := exec.Command(os.Args[0], "-test.run=^TestDumpingChild$")
			child.Env = append(os.Environ(), "E2E_DUMPS="+dumps, "E2E_KUBECONFIG="+clusterToDump(t), dumpingChild+"="+c.scenario, dumpingMarker+"="+marker)
			out, err := child.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(out), "--- FAIL: TestDumpingChild") {
				t.Fatalf("the child ended with %v, want the failure of its steps:\n%s", err, out)
			}
			console := filepath.Join(dumps, "failure", "containers", "e2e-node", "console.log")
			if got, err := os.ReadFile(console); err != nil || string(got) != "console of e2e-node\n" {
				t.Errorf("the console is %q, %v", got, err)
			}
			if _, err := os.Stat(filepath.Join(dumps, "install")); err == nil {
				t.Error("a dump of a failed install was written")
			}
			listings := 0
			for _, call := range dockerCalls(t, calls) {
				if strings.HasPrefix(call, "ps ") {
					listings++
				}
			}
			if listings != 1 {
				t.Errorf("the failed steps dumped %d times, want once", listings)
			}
			if c.teardown {
				if seen, err := os.ReadFile(marker); err != nil || string(seen) != "dumped" {
					t.Errorf("the teardown found %q, %v, want the dump written before it", seen, err)
				}
			}
		})
	}
}

func TestDumpingChild(t *testing.T) {
	scenario := os.Getenv(dumpingChild)
	if scenario == "" {
		t.Skip("runs as a child of TestTheEnvironmentDumpsOnceAtTheFirstFailedStepBeforeTheTeardown")
	}
	testenv := dumping{env.NewWithConfig(envconf.NewWithKubeConfig(os.Getenv("E2E_KUBECONFIG")))}
	feature := features.New(scenario)
	switch scenario {
	case "assessments":
		feature = feature.
			Assess("fails", func(ctx context.Context, t *testing.T, _ *envconf.Config) context.Context {
				t.Error("the first assessment fails")
				return ctx
			}).
			Assess("fails too", func(ctx context.Context, t *testing.T, _ *envconf.Config) context.Context {
				t.Error("the second assessment fails")
				return ctx
			}).
			Teardown(func(ctx context.Context, t *testing.T, _ *envconf.Config) context.Context {
				seen := "not dumped"
				if _, err := os.Stat(filepath.Join(dumpDir, "failure")); err == nil {
					seen = "dumped"
				}
				if err := os.WriteFile(os.Getenv(dumpingMarker), []byte(seen), 0o600); err != nil {
					t.Fatal(err)
				}
				return ctx
			})
	case "setup":
		feature = feature.Setup(func(ctx context.Context, t *testing.T, _ *envconf.Config) context.Context {
			t.Fatal("the setup fails")
			return ctx
		})
	}
	testenv.Test(t, feature.Feature())
}
