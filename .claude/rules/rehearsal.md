---
paths:
  - "internal/controller/workloads/rehearsal*.go"
---

# The rehearsal of a minor step

- The copy is `rendering.apiServer()` with the changes of the [decision](../../docs/decisions/rehearse-a-minor-step.md) alone, never a render of its own, so that every change to the API server's render reaches it.
- The copy carries no label anything selects but its own NetworkPolicy and the Datastore controller's etcd NetworkPolicy, has no Service and no route, and its NetworkPolicy admits the operator's namespace to the API server's port alone.
- The sweep reads the copy and the tenant through `internal/tenantclient`, and decodes every answer with `internal/jsonscan`, each array counted (`tenant-reads.md`).
