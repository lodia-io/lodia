# Upgrades follow the version skew policy step by step; changes Brygga initiates roll in each tenant's maintenance window, a bounded number of tenants at a time

Proposed 2026-10-23; not decided yet. Not built.

## Decision

**Order of a Kubernetes upgrade** (Lifecycle drives it through
`status.target`, one minor at a time even if `spec.version` is further ahead):

1. Validation in the schema: `spec.version` is a semver, the minor never
   decreases.
2. A `Backup` named `pre-upgrade-<version>` completes (skipped when the
   `DatastoreClass` has no backup store, and said so in `status`).
3. kube-apiserver rolls; the step is done when every replica is `Ready` and
   answers `/readyz` with the new version.
4. kube-controller-manager and kube-scheduler roll; done when each has a
   leader and answers its health endpoint.
5. In the tenant: kube-proxy, CoreDNS and konnectivity-agent move to the
   versions of the new minor; done when their rollouts complete.
6. `status.kubernetes.version` becomes the new version. Nodes are the
   owner's: `status.nodes` reports how many run an older kubelet, and nothing
   forces them.

A step that fails stops the upgrade and sets `Progressing=False` with the
step and the error; the upgrade resumes when the cause is gone, and a
`restoreFrom` the pre-upgrade backup is the documented rollback.

**Who waits for the window:**

| change | when it rolls |
|---|---|
| the user edits `spec` | now |
| `maintenance.autoUpdate.kubernetesPatch` finds a newer patch | in the window |
| a new Brygga release renders a different pod template | in the window |
| a certificate renewal that needs a restart | in the window, or now if the certificate expires before the next window |
| a credential rotation the user started | now; the stages wait for the user |

A window is `spec.maintenance.window` (`begin`, `end`, `timeZone`); a
`ControlPlane` without one rolls Brygga-initiated changes at any time. Across
all tenants, Lifecycle lets at most N (operator configuration, default 5)
tenants roll Brygga-initiated changes at once, oldest pending first; a tenant
that waits says so in `status.target.reason`.

**Mechanism:** Workloads renders with the running operator but does not
change a pod template whose `brygga.segla.io/operator-version` annotation is
older until Lifecycle advances `status.target.operatorVersion` for that
tenant; a user's `spec` change advances it at once, so the user's rollout also
carries the pending template changes.

## Why

- The [version skew policy](https://kubernetes.io/releases/version-skew-policy/)
  allows the controller manager and scheduler to lag the API server by one
  minor and kubelets by three, never the other way round; the order above is
  the only safe one, and in-tenant components cannot be newer than the API
  server.
- A snapshot before every minor upgrade is the only rollback etcd allows: a
  minor downgrade of the API server is unsupported.
- In the solutions we studied, a new operator release can re-render every
  cluster it manages at once; one bug or one changed default then becomes a
  fleet-wide rollout in the middle of a business day. Gardener answers with
  maintenance time windows and a per-seed cap on concurrent reconciliations
  ([shoot maintenance](https://github.com/gardener/gardener/blob/master/docs/usage/shoot/shoot_maintenance.md));
  the same two levers are enough here.
- Automatic patch updates in the window keep tenants on supported versions
  without a platform on top polling for releases.

## Rejected

- Rendering every tenant immediately on operator upgrade — simplest code,
  fleet-wide risk.
- One operator version per tenant (the HyperShift control-plane-operator
  shape) — the strongest isolation of renderer versions, at the cost of a pod
  per tenant and a second upgrade path.
- Automatic minor upgrades — a minor can break workloads (removed APIs); the
  owner opts in by editing `spec.version`.

## Consequences

- Lifecycle is the only controller that knows the time and the fleet; it
  holds an in-memory semaphore, which is correct because only the leader
  runs controllers.
- In the `Compact` layout steps 3 and 4 are one rollout of the single pod:
  the API server, controller manager and scheduler change version together,
  which the skew policy allows ([compact-layout](compact-layout.md)).
- An upgrade does not start while Health holds the controller manager, and
  one in progress waits at step 4 until the hold ends
  ([controller-manager-hold](controller-manager-hold.md)).
- `status.target` becomes part of the API that users read:
  `target.version`, `target.operatorVersion`, `target.reason`,
  `target.scheduledFor`.
- An operator release note states which tenants will roll (all with a changed
  template) and what changes, because the roll is deferred, not hidden.

## Code

none yet.
