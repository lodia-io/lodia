---
paths:
  - "internal/**"
  - "cmd/**"
---

# etcd-druid

- Only `internal/etcddruid` names etcd-druid in production code: its types, annotations, labels, ports, task kinds, Lease names, StatefulSet and source lines; a controller calls the engine and gets Lodia's values back, and `internal/etcddruid/seam_test.go` fails on an import elsewhere ([decision](../../docs/decisions/etcd-druid-behind-a-seam.md)).
- A test that writes what etcd-druid writes calls `internal/etcddruid/druidtest`, and a new assumption about etcd-druid gets a row in the decision's table.
