---
paths:
  - "internal/**"
  - "cmd/**"
---

# etcd-druid

- Only `internal/etcddruid` names etcd-druid in production code: its types, annotations, labels, ports, task kinds, Lease names, StatefulSet and source lines; a controller calls the engine and gets Lodia's values back, and `internal/etcddruid/seam_test.go` fails on an import elsewhere ([decision](../../docs/decisions/etcd-druid-behind-a-seam.md)).
- A test that writes what etcd-druid writes calls `internal/etcddruid/druidtest`, and a new assumption about etcd-druid gets a row in the decision's table.
- A test that talks to an etcd starts it with `internal/testetcd`, etcd's release binary at `versions.EtcdVersion`, and never an embedded etcd, which keeps `go.etcd.io/etcd/server/v3` out of `go.mod` ([decision](../../docs/decisions/etcd-druid-behind-a-seam.md)).
