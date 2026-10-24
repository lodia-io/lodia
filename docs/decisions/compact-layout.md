# A ControlPlane can run Compact: every control plane component in one pod, for development and free tiers

Decided 2026-10-25. Not built.

## Decision

- `spec.layout` is `Split` (the default, a Deployment per component,
  [component-per-deployment](component-per-deployment.md)) or `Compact`. It is
  immutable in `v1alpha1`.
- `Compact` renders one Deployment `<cp>-control-plane` with one replica and
  the containers `kube-apiserver`, `kube-controller-manager`,
  `kube-scheduler`, `konnectivity-server` and `brygga-signer`. The controller
  manager and the scheduler reach the API on `https://127.0.0.1:6443`, which
  the serving certificate carries as a SAN.
- A schema rule refuses `Compact` together with `highAvailability` or
  `apiServer.replicas` above 1.
- The datastore stays independent of the layout:

| layout | datastore | pods per tenant |
|---|---|---|
| `Split`, single | dedicated | 4 |
| `Split`, HA | dedicated | 9 |
| `Compact` | dedicated | 2: the control plane and a one-member etcd |
| `Compact` | shared | 1 |

- Everything else is the same in both layouts: PKI and every rotation, the
  objects inside the tenant, backups of a dedicated etcd, hibernation, and the
  controller manager hold, which takes a lease and scales nothing
  ([controller-manager-hold](controller-manager-hold.md)).

## Why

- **Density is pod count as much as memory.** A kubelet runs at most 110
  pods by default (`maxPods`,
  [kubelet configuration](https://kubernetes.io/docs/reference/config-api/kubelet-config.v1beta1/)):
  four pods per tenant cap a node at about 27 tenants, one pod at up to 110
  if memory allows. Every pod also takes an address from the management
  cluster's pod range.
- **Memory follows the processes, not the pods.** The same processes run in
  either layout; the larger saving is a shared datastore and hibernation, and
  `Compact` lets those reach one pod per tenant.
- **One renderer, two shapes.** Only the Workloads controller knows the
  layout; the other five controllers do not care how many pods there are, so
  `Compact` adds no second code path to PKI, Datastore, Tenant, Health or
  Lifecycle.

## Rejected

- `Compact` as the default — every restart-only change restarts the whole
  control plane, and there is no high availability.
- An embedded store inside the one pod (SQLite through kine, or an etcd
  container) to make a dedicated store fit in one pod — a second storage path
  beside etcd-druid; `Compact` with a dedicated etcd is two pods with
  etcd-druid's backups.
- Switching the layout in place — every component moves and the API is down
  for the switch; a new `ControlPlane` restored from a `Backup` does the same
  thing explicitly.

## Consequences

- In `Compact`, a restart-only change (audit policy, egress or admission
  configuration, the etcd CA, the konnectivity certificate, the scheduler
  configuration) restarts every component of that control plane; renewals of
  reloaded certificates still restart nothing.
- A single replica means the API is down for the seconds a pod restart or an
  upgrade takes; the maintenance window matters more than in `Split`.
- Moving a tenant from `Compact` to `Split` is a new `ControlPlane` restored
  from a `Backup` (dedicated), or a migration (shared), documented in
  [operations](../operations.md).
- The phase 3 gate runs a `Compact` tenant on a shared `Datastore` as one pod
  through the conformance smoke subset and the controller manager hold
  ([roadmap](../roadmap.md)).

## Code

none yet.
