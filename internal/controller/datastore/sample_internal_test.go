// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestTallyTakesTheLargestSizeAndTheLeaderAQuorumNames(t *testing.T) {
	voting := []*etcdserverpb.Member{{ID: 1}, {ID: 2}, {ID: 3}}
	answer := func(leader uint64, size int64) *clientv3.StatusResponse {
		return &clientv3.StatusResponse{Leader: leader, DbSize: size}
	}
	for _, c := range []struct {
		name       string
		answers    []*clientv3.StatusResponse
		wantLeader bool
		wantSize   int64
	}{
		{"the largest size answered in the middle", []*clientv3.StatusResponse{answer(1, 10), answer(1, 30), answer(1, 20)}, true, 30},
		{"a quorum naming no leader", []*clientv3.StatusResponse{answer(0, 10), answer(0, 10), answer(1, 10)}, false, 10},
		{"a quorum naming one leader and a member another", []*clientv3.StatusResponse{answer(1, 10), answer(2, 10), answer(1, 10)}, true, 10},
	} {
		t.Run(c.name, func(t *testing.T) {
			for range 64 {
				got := tally(voting, c.answers)
				if got.HasLeader != c.wantLeader || got.DBSize == nil || *got.DBSize != c.wantSize {
					t.Fatalf("tally gives %+v with size %v, want leader %t and size %d", got, got.DBSize, c.wantLeader, c.wantSize)
				}
			}
		})
	}
}
