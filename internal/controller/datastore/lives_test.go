// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/etcddruid"
	"go.lodia.io/lodia/internal/testetcd"
)

var (
	asEtcdDruid = testetcd.Config{Name: "dev-etcd-0", PeerURL: "https://dev-etcd-0.dev-etcd-peer.tenants.svc:2380", Token: "etcd-cluster"}
	asSidecar   = testetcd.Config{Name: "default", PeerURL: "http://localhost:2380", Token: "etcd-cluster"}
)

func drain(t *testing.T, l *datastore.Latches) {
	t.Helper()
	go func() {
		for {
			select {
			case <-datastore.Told(l):
			case <-t.Context().Done():
				return
			}
		}
	}()
}

func underLoad(t *testing.T, member *testetcd.Member, d time.Duration, pass func()) {
	t.Helper()
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{member.ClientURL}, DialTimeout: 5 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	for w := range 2 {
		wg.Go(func() {
			for i := 0; ctx.Err() == nil; i++ {
				_, _ = c.Put(ctx, fmt.Sprintf("/registry/w%d/k%d", w, i%200), "v")
			}
		})
	}
	wg.Go(func() {
		for i := 0; ctx.Err() == nil; i++ {
			lease, err := c.Grant(ctx, 60)
			if err != nil {
				continue
			}
			_, _ = c.Put(ctx, fmt.Sprintf("/registry/leased/k%d", i%20), "v", clientv3.WithLease(lease.ID))
			_, _ = c.Revoke(ctx, lease.ID)
		}
	})
	wg.Go(func() {
		for ctx.Err() == nil {
			_, _ = c.Txn(ctx).If(clientv3.Compare(clientv3.Value("/registry/w0/k1"), "=", "never")).Then(clientv3.OpPut("/registry/never", "x")).Commit()
		}
	})
	wg.Go(func() {
		for ctx.Err() == nil {
			time.Sleep(300 * time.Millisecond)
			if s, err := c.Status(ctx, member.ClientURL); err == nil && s.Header.GetRevision() > 50 {
				_, _ = c.Compact(ctx, s.Header.GetRevision()-50)
			}
		}
	})
	for end := time.Now().Add(d); time.Now().Before(end); {
		pass()
	}
	cancel()
	wg.Wait()
}

func status(t *testing.T, member *testetcd.Member) *clientv3.StatusResponse {
	t.Helper()
	s, err := etcdClient(t, member.ClientURL).Status(t.Context(), member.ClientURL)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func saveSnapshot(t *testing.T, member *testetcd.Member) string {
	t.Helper()
	stream, err := etcdClient(t, member.ClientURL).Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	file := filepath.Join(t.TempDir(), "full.db")
	out, err := os.Create(file)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, stream); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestTheReleaseBinaryTellsARestoreThroughDeltasAndAnEmptyStartAndNothingElse(t *testing.T) {
	t.Parallel()
	member := testetcd.Start(t, asEtcdDruid)
	cp, etcd := sampledControlPlane("lives", "uid-lives")
	key := client.ObjectKeyFromObject(cp)
	r, _ := sampler(cp, etcd)
	r.EtcdClient = func(context.Context, *etcddruid.Store) (*clientv3.Client, error) {
		return clientv3.New(clientv3.Config{Endpoints: []string{member.ClientURL}, DialTimeout: 5 * time.Second})
	}
	drain(t, r.Latches)
	pass := func() { samplePass(t, r, key) }

	bootstrapped := status(t, member)
	if etcddruid.RestoredFromSnapshots(bootstrapped.Header.GetClusterId()) || bootstrapped.RaftTerm != 2 {
		t.Fatalf("a member bootstrapped under etcd-druid's names answers cluster %x at term %d, want another cluster than a restore's at term 2", bootstrapped.Header.GetClusterId(), bootstrapped.RaftTerm)
	}
	underLoad(t, member, 2*time.Second, pass)
	member.Restart(asEtcdDruid)
	underLoad(t, member, 2*time.Second, pass)
	if got := datastore.Latched(r.Latches, key); got != nil {
		t.Fatalf("a life under writes, leases, failed compares, compactions and a restart told %+v, want nothing", got)
	}

	member.StartEmpty(asEtcdDruid)
	if emptied := status(t, member); emptied.Header.GetClusterId() != bootstrapped.Header.GetClusterId() {
		t.Fatalf("a member started empty under etcd-druid's names answers cluster %x, want the bootstrapped life's %x", emptied.Header.GetClusterId(), bootstrapped.Header.GetClusterId())
	}
	pass()
	if got := datastore.Latched(r.Latches, key); got == nil || got.From != lodiav1alpha1.MemberRestoreFromEmpty {
		t.Fatalf("an empty start after the bootstrapped life told %+v, want it told as Empty", got)
	}

	full := saveSnapshot(t, member)
	underLoad(t, member, time.Second, pass)
	member.Restore(full, asSidecar)
	member.Restart(asSidecar)
	replay := etcdClient(t, member.ClientURL)
	for i := range 20 {
		if _, err := replay.Put(t.Context(), fmt.Sprintf("/registry/delta/k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := replay.MemberUpdate(t.Context(), status(t, member).Header.GetMemberId(), []string{asEtcdDruid.PeerURL}); err != nil {
		t.Fatal(err)
	}
	member.Restart(asEtcdDruid)
	restored := status(t, member)
	if !etcddruid.RestoredFromSnapshots(restored.Header.GetClusterId()) || restored.Header.GetMemberId() != 0x8e9e05c52164694d || restored.RaftTerm != 3 {
		t.Fatalf("a member restored through deltas answers cluster %x, member %x at term %d, want a restore's cluster, member 8e9e05c52164694d at term 3", restored.Header.GetClusterId(), restored.Header.GetMemberId(), restored.RaftTerm)
	}
	pass()
	told := datastore.Latched(r.Latches, key)
	if told == nil || told.Member != asEtcdDruid.Name || told.From != lodiav1alpha1.MemberRestoreFromSnapshots {
		t.Fatalf("the restore told %+v, want %s restored from snapshots", told, asEtcdDruid.Name)
	}
	underLoad(t, member, 2*time.Second, pass)
	if got := datastore.Latched(r.Latches, key); *got != *told {
		t.Fatalf("the restored life under load told %+v after %+v, want nothing more", got, told)
	}

	member.StartEmpty(asEtcdDruid)
	pass()
	if got := datastore.Latched(r.Latches, key); got == nil || got.From != lodiav1alpha1.MemberRestoreFromEmpty {
		t.Errorf("an empty start after the restore told %+v, want it told as Empty", got)
	}
}

func TestASendNobodyTakesEndsWithThePassAndTheNextPassSendsAgain(t *testing.T) {
	t.Parallel()
	cp, _ := sampledControlPlane("unread", "uid-unread")
	key := client.ObjectKeyFromObject(cp)
	r, _ := sampler(cp)
	datastore.Tell(r.Latches, key, cp.UID, lodiav1alpha1.MemberRestoreStatus{Member: "dev-etcd-0", From: lodiav1alpha1.MemberRestoreFromSnapshots})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()
		_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a pass whose send nobody takes did not end with its deadline")
	}
	sent := make(chan types.NamespacedName, 1)
	go func() {
		select {
		case e := <-datastore.Told(r.Latches):
			sent <- client.ObjectKeyFromObject(e.Object)
		case <-t.Context().Done():
		}
	}()
	samplePass(t, r, key)
	select {
	case got := <-sent:
		if got != key {
			t.Errorf("the next pass sent %s, want %s", got, key)
		}
	case <-time.After(5 * time.Second):
		t.Error("the next pass sent nothing, want its latch sent again")
	}
}
