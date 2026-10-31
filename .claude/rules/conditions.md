---
paths:
  - "internal/controller/**"
  - "internal/live/**"
---

# Conditions while a controller waits

- A writer that waits for another condition at a new generation leaves its own at the generation it last described; only a wait at the generation it already describes is False, `WaitingFor<X>` ([decision](../../docs/decisions/independent-controllers.md)).
- A writer that decides from its own condition while it waits, or keeps one while its calls to the tenant fail, reads it through `internal/live` and compares its status apply with that copy, not the cache's; its ClusterRole needs `get` on `controlplanes`.
- A writer whose status apply carries the `resourceVersion` it read needs no live read: a pass on a stale cache conflicts and writes nothing.
