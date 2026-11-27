// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid_test

import (
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"go.lodia.io/lodia/internal/etcddruid"
	"go.lodia.io/lodia/internal/testetcd"
)

func TestOnlyTheSidecarsNamesGiveTheClusterIDOfARestore(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name          string
		cfg           testetcd.Config
		fromSnapshots bool
	}{
		{"the sidecar's", testetcd.Config{Name: "default", PeerURL: "http://localhost:2380", Token: "etcd-cluster"}, true},
		{"etcd-druid's", testetcd.Config{Name: "dev-etcd-0", PeerURL: "https://dev-etcd-0.dev-etcd-peer.team-roll.svc:2380", Token: "etcd-cluster"}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			member := testetcd.Start(t, c.cfg)
			client, err := clientv3.New(clientv3.Config{Endpoints: []string{member.ClientURL}, DialTimeout: 5 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			status, err := client.Status(t.Context(), member.ClientURL)
			if err != nil {
				t.Fatal(err)
			}
			if got := etcddruid.RestoredFromSnapshots(status.Header.GetClusterId()); got != c.fromSnapshots {
				t.Errorf("a member bootstrapped under %s names runs under cluster ID %x, which tells a restore from snapshots: %t, want %t", c.name, status.Header.GetClusterId(), got, c.fromSnapshots)
			}
		})
	}
}
