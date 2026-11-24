# Review checklist

Every pull request is reviewed against these checks.
Each names a failure that hosted control planes run into; a change that fails one is reworked, not excused.

1. **One writer per field.**
   Every status field, condition, label, annotation and shared name has one writer, declared in `api/v1alpha1`, or in `internal/contract` when only Lodia's controllers share it.
   Two writers overwrite each other.
2. **Controllers meet only through the API.**
   A controller reads what another publishes in status, or an object named in `api/v1alpha1` or `internal/contract`; it never calls another controller.
   Hidden coupling breaks when either side changes.
3. **No code for a case that cannot happen.**
   No branch for an unreachable state, such as a migration from a release nobody runs.
   Code that nothing exercises rots.
4. **Derived state follows its inputs.**
   Every certificate, kubeconfig, Secret and rendered template knows its inputs; a change to any of them reissues it and restarts its consumers.
   Otherwise a rotation or an address change leaves stale credentials in use.
5. **Rendering is deterministic.**
   The same inputs render byte-identical objects.
   An unstable order, of a map or a set, rolls every tenant or loops forever.
6. **Accepted means applied.**
   A spec field either takes effect, with a test, or CEL refuses it at admission, and nothing that varies per cluster is hard-coded.
   A setting that is ignored, or refused only during a reconcile, breaks the cluster long after it was written.
7. **Other owners' data is left alone.**
   Server-side apply with Lodia's field manager, selectors on Lodia's own labels, and no deletion by a broad selector.
   Labels and fields another owner set survive every pass.
8. **Every operation works in every state.**
   Rotation, upgrade, restore and deletion are defined while paused, mid-rollout, mid-rotation and with a dependency already gone, and deletion always completes.
   Each pass survives a write failing midway, a restart between writes, a transient API error, an object outside the cache, a predecessor's leftover and an expired deadline.
9. **Status tells the truth.**
   Each condition reason has a test that derives it from observed state, nothing is Ready or Succeeded on empty input, and metrics are tested like status.
   A green condition over missing data is worse than a red one.
10. **Bounded and isolated.**
    Every derived name and object has a computed maximum that CEL enforces, per-tenant state is bounded whatever the tenant's untrusted administrator writes ([who-is-trusted](decisions/who-is-trusted.md)), and every external call has a timeout.
    One tenant's failure never blocks another's reconcile.
11. **Upgrading Lodia is safe.**
    A change to what Lodia renders reaches tenants through their maintenance rules, never all at once ([upgrades-and-maintenance-windows](decisions/upgrades-and-maintenance-windows.md)).
    Until the first real user's tenant is created, a change to `v1alpha1` may be incompatible and keeps no alias, conversion or migration, since no user has stored anything (check 3).
    From that tenant's creation until the maintainers freeze `v1alpha1` ([one-controlplane-per-cluster](decisions/one-controlplane-per-cluster.md#consequences)), a change may still be incompatible, and it comes with what carries that tenant's stored objects to the new shape.
    From the freeze on, objects stored by the previous release reconcile without a manual step, and `v1alpha1` changes only by addition.
