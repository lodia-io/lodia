---
paths:
  - "internal/controller/**"
  - "internal/live/**"
---

# Conditions while a controller waits

- A writer that waits for another condition at a new generation leaves its own at the generation it last described; only a wait at the generation it already describes is False, `WaitingFor<X>` ([decision](../../docs/decisions/independent-controllers.md)).
- A renderer held by `status.target.operatorRevision` reports what runs, at the generation its condition describes, as a wait does; Workloads' `WaitingForTarget` replaces a condition at the generation it describes unless that condition reports availability, and Tenant keeps `AddonsApplied` but for a refused creation and for True over a `WaitingFor*` reason.
- Every wait of Tenant, held or not, keeps a condition that describes an earlier generation, as the first rule says, and an `AddonsApplied` that is `Refused`, `ObjectsControlledElsewhere` or `Applying`: a lost access undoes no observation, and a held pass writes True over a wait at the generation it names.
- A writer that decides from its own condition or status field while it waits, or keeps one while its calls to the tenant fail, reads it through `internal/live` and compares its status apply with that copy, not the cache's; its ClusterRole needs `get` on `controlplanes`.
- A writer whose status apply carries the `resourceVersion` it read, and whose watch requeues every update of its object, needs no live read: a pass on a stale cache conflicts, or skips a write that the next pass makes.
- Every status apply goes through `internal/statusapply`: a `ControlPlane`'s reads the object live once it finds a change, so its ClusterRole needs `get` on `controlplanes`, and `Backup` and `KubeconfigRequest` apply under the `resourceVersion` they read.
