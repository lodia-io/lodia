// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid

// restoredClusterID is the cluster ID of every member the backup-restore
// sidecar restores from the store's snapshots. etcd-druid passes the sidecar
// no name, initial cluster or token
// (etcd-druid@v0.39.2 internal/component/statefulset/builder.go#L430-L523),
// so the restore runs under the sidecar's defaults, default,
// http://localhost:2380 and etcd-cluster
// (etcd-backup-restore@v0.45.3 pkg/types/restorer.go#L20-L23, #L69-L77), and
// etcd derives a member's ID from its peer URLs and the token, and the cluster
// ID from its members' IDs, alone
// (etcd@v3.5.27 server/etcdserver/api/membership/member.go#L52-L77,
// cluster.go#L86-L100).
const restoredClusterID = 0xcdf818194e3a8c32

// RestoredFromSnapshots says whether a member whose data life began under
// clusterID was restored from the store's snapshots. Every other life is a
// bootstrap under etcd-druid's names, an empty start included: with no store,
// or none with a snapshot, the sidecar only removes the data directory
// (etcd-backup-restore@v0.45.3 pkg/initializer/initializer.go#L134-L145,
// #L201-L278).
func RestoredFromSnapshots(clusterID uint64) bool {
	return clusterID == restoredClusterID
}
