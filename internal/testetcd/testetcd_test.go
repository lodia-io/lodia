// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package testetcd

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"go.lodia.io/lodia/internal/versions"
)

func dial(t *testing.T, endpoint string) *clientv3.Client {
	t.Helper()
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

type failing struct {
	testing.TB
	message  string
	cleanups []func()
}

func (f *failing) Helper() {}

func (f *failing) Failed() bool { return f.message != "" }

func (f *failing) Cleanup(cleanup func()) { f.cleanups = append(f.cleanups, cleanup) }

func (f *failing) Errorf(format string, args ...any) { f.message += fmt.Sprintf(format, args...) }

func (f *failing) Logf(string, ...any) {}

func (f *failing) Fatalf(format string, args ...any) {
	f.message += fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func (f *failing) Fatal(args ...any) { f.Fatalf("%s", fmt.Sprint(args...)) }

func startFailing(t *testing.T, cfg Config) string {
	t.Helper()
	return runFailing(t, func(f testing.TB) { Start(f, cfg) })
}

func runFailing(t *testing.T, run func(testing.TB)) string {
	t.Helper()
	f := &failing{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(f)
	}()
	<-done
	for _, cleanup := range slices.Backward(f.cleanups) {
		cleanup()
	}
	return f.message
}

func TestAMemberRunsTheEtcdWrapperRunsAndAnswersAClient(t *testing.T) {
	t.Parallel()
	m := Start(t, Config{})
	c := dial(t, m.ClientURL)
	status, err := c.Status(t.Context(), m.ClientURL)
	if err != nil {
		t.Fatal(err)
	}
	if want := strings.TrimPrefix(versions.EtcdVersion, "v"); status.Version != want {
		t.Errorf("the member is etcd %s, want %s", status.Version, want)
	}
	if _, err := c.Put(t.Context(), "/k", "v"); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(t.Context(), "/k")
	if err != nil || len(got.Kvs) != 1 || string(got.Kvs[0].GetValue()) != "v" {
		t.Errorf("the member answers %v, %v, want the value written", got, err)
	}
}

func TestThreeMembersFormOneClusterWhoseMembersHaveClientURLs(t *testing.T) {
	t.Parallel()
	cluster := StartCluster(t, 3, nil)
	c := dial(t, cluster.Members[0].ClientURL)
	list, err := c.MemberList(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Members) != 3 {
		t.Fatalf("the cluster has %d members, want 3", len(list.Members))
	}
	for _, m := range list.Members {
		if len(m.GetClientURLs()) != 1 {
			t.Errorf("member %s advertises %v, want one client URL", m.GetName(), m.GetClientURLs())
		}
	}
}

func TestAStoppedMemberAnswersNoMore(t *testing.T) {
	t.Parallel()
	m := Start(t, Config{})
	m.Stop()
	m.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if _, err := dial(t, m.ClientURL).Status(ctx, m.ClientURL); err == nil {
		t.Error("a stopped member answered")
	}
}

func TestAPreparedMemberJoinsAsALearner(t *testing.T) {
	t.Parallel()
	cluster := StartCluster(t, 1, func(_ int, cfg *Config) { cfg.NoStrictReconfigCheck = true })
	learner := cluster.Prepare("learner", Config{NoStrictReconfigCheck: true})
	c := dial(t, cluster.Members[0].ClientURL)
	if _, err := c.MemberAddAsLearner(t.Context(), []string{learner.PeerURL}); err != nil {
		t.Fatal(err)
	}
	learner.Join()
	status, err := c.Status(t.Context(), learner.ClientURL)
	if err != nil {
		t.Fatal(err)
	}
	if !status.IsLearner {
		t.Errorf("the member that joined answers %+v, want a learner", status)
	}
	if len(cluster.Members) != 2 {
		t.Errorf("the cluster holds %d members after the join, want 2", len(cluster.Members))
	}
}

func TestAMemberRemovedFromItsClusterMayExitWithoutFailingTheTest(t *testing.T) {
	t.Parallel()
	cluster := StartCluster(t, 2, func(_ int, cfg *Config) { cfg.NoStrictReconfigCheck = true })
	c := dial(t, cluster.Members[0].ClientURL)
	list, err := c.MemberList(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range list.Members {
		if m.GetName() == cluster.Members[1].Name {
			if _, err := c.MemberRemove(t.Context(), m.GetID()); err != nil {
				t.Fatal(err)
			}
		}
	}
	select {
	case <-cluster.Members[1].proc.done:
	case <-time.After(30 * time.Second):
		t.Fatal("the member removed from its cluster did not exit")
	}
}

func TestAClientPortThatAnotherEtcdHoldsIsPickedAgain(t *testing.T) {
	t.Parallel()
	holder := Start(t, Config{})
	_, heldPort, _ := net.SplitHostPort(strings.TrimPrefix(holder.ClientURL, "http://"))
	cluster := newCluster(t)
	picked := 0
	cluster.port = func() (string, error) {
		picked++
		if picked == 1 {
			return heldPort, nil
		}
		return freePort()
	}
	m := cluster.Prepare("m0", Config{})
	cluster.Members = []*Member{m}
	cluster.boot(cluster.Members, "new", cluster.initialCluster(cluster.Members))
	if picked != 2 || m.attempts != 2 || m.ClientURL == holder.ClientURL {
		t.Errorf("the member started at %s after %d ports picked and %d attempts, want a second port, not %s", m.ClientURL, picked, m.attempts, holder.ClientURL)
	}
	if _, err := dial(t, holder.ClientURL).Put(t.Context(), "/held", "by the first"); err != nil {
		t.Fatal(err)
	}
	got, err := dial(t, m.ClientURL).Get(t.Context(), "/held")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Kvs) != 0 {
		t.Errorf("the member holds %v, a key of the etcd that held its first port", got.Kvs)
	}
}

func TestWithoutTheBinaryTheTestFailsWithHowToGetIt(t *testing.T) {
	t.Setenv(BinaryEnv, "")
	message := startFailing(t, Config{})
	for _, want := range []string{BinaryEnv, "make test", "go run ./test/etcdbinary work/etcd", versions.EtcdVersion} {
		if !strings.Contains(message, want) {
			t.Errorf("the failure %q does not say %q", message, want)
		}
	}
}

func TestAMemberThatExitsAtOnceFailsTheTestWithItsLog(t *testing.T) {
	script := filepath.Join(t.TempDir(), "etcd")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho 'the log of a member' >&2\nexit 3\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(BinaryEnv, script)
	message := startFailing(t, Config{})
	for _, want := range []string{"exit status 3", "the log of a member"} {
		if !strings.Contains(message, want) {
			t.Errorf("the failure %q does not say %q", message, want)
		}
	}
}

func TestAMemberOfAnotherVersionFailsTheTestWithBothVersions(t *testing.T) {
	t.Parallel()
	message := runFailing(t, func(f testing.TB) {
		cluster := newCluster(f)
		cluster.wanted = "v3.5.0"
		m := cluster.Prepare("m0", Config{})
		cluster.Members = []*Member{m}
		cluster.boot(cluster.Members, "new", cluster.initialCluster(cluster.Members))
	})
	for _, want := range []string{"is etcd " + strings.TrimPrefix(versions.EtcdVersion, "v"), "want 3.5.0"} {
		if !strings.Contains(message, want) {
			t.Errorf("the failure %q does not say %q", message, want)
		}
	}
}

func TestAMemberKilledBeforeTheTestEndsFailsIt(t *testing.T) {
	t.Parallel()
	message := runFailing(t, func(f testing.TB) {
		m := Start(f, Config{})
		if err := m.proc.cmd.Process.Kill(); err != nil {
			f.Fatal(err)
		}
		<-m.proc.done
	})
	for _, want := range []string{"etcd m0 died before the test ended", "signal: killed"} {
		if !strings.Contains(message, want) {
			t.Errorf("the failure %q does not say %q", message, want)
		}
	}
}
