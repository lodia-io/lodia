# Every tenant gets its own etcd, run by etcd-druid; a shared etcd is an opt-in for density

Proposed 2026-10-23; not decided yet — the phase 0 spike confirms or
overturns it. Not built.

## Decision

- **Dedicated by default.** `spec.datastore.dedicated` makes the Datastore
  controller create one `Etcd` resource (`druid.gardener.cloud/v1alpha1`) in
  the tenant's namespace: 1 member, or 3 with `highAvailability`, built from
  the `DatastoreClass` (quota, compaction, storage, backup store and
  schedules, defragmentation, placement). Brygga's PKI controller issues the
  etcd CA and the server, peer and client certificates; etcd-druid takes them
  as Secrets, because it does not generate TLS itself
  ([securing etcd clusters](https://gardener.github.io/etcd-druid/usage/securing-etcd-clusters.html)).
- **The engine is etcd-druid**, installed beside Brygga (the chart depends on
  its Helm chart), run with `enableEtcdSpecAutoReconcile: true` so a spec
  change reconciles without an annotation, and with its optional components
  webhook off ([configuration](https://gardener.github.io/etcd-druid/deployment/configure-etcd-druid.html)).
  What Brygga gets from it, per tenant:
  - full and delta snapshots to S3, GCS, Azure Blob, Swift and other stores,
    with garbage collection;
  - automatic restore of a single member that lost its data, and re-adding a
    lost member of three as a learner;
  - scaling 1 → 3 learner-first and back;
  - `replicas: 0` that keeps the volumes and takes a full snapshot first,
    which is hibernation;
  - defragmentation on a schedule
  ([managing](https://gardener.github.io/etcd-druid/usage/managing-etcd-clusters.html),
  [recovering](https://gardener.github.io/etcd-druid/usage/recovering-etcd-clusters.html)).
- **Brygga adds what etcd-druid leaves to its consumer:** the NetworkPolicy
  that admits only the tenant's API server and the members themselves
  ([etcd-druid#505](https://github.com/gardener/etcd-druid/issues/505)), the
  ServiceMonitor for the etcd and sidecar metrics, and the `Backup` object as
  an on-demand snapshot through the sidecar.
- **Shared, opt-in.** `spec.datastore.shared` names a `Datastore`: an etcd
  several tenants use. Brygga creates per tenant an etcd user named after the
  `ControlPlane`'s UID, authenticated by the CN of a client certificate (the
  API server has no other way to log in to etcd), and a role granting
  read-write on the prefix `/<prefix>/<uid>/`, whose range end is computed by
  the etcd client library, never by hand. The shared etcd must run with
  `--auth-token jwt`; the simple token is not meant for production
  ([RBAC](https://etcd.io/docs/v3.6/op-guide/authentication/rbac/),
  [auth design](https://etcd.io/docs/v3.6/learning/design-auth-v3/)).
- **The defaults** of the `DatastoreClass` Brygga ships: quota 4 GiB,
  periodic compaction every 15 minutes (the API server also compacts every 5),
  delta snapshots every 5 minutes, a full snapshot daily inside the tenant's
  window, exponential garbage collection, defragmentation followers first and
  leader last, requests that start from Gardener's for its shoots (etcd 30m /
  150M, sidecar 10m / 40Mi) until phase 1 measures our own
  ([Gardener's etcd component](https://raw.githubusercontent.com/gardener/gardener/master/pkg/component/etcd/etcd/etcd.go),
  [maintenance](https://etcd.io/docs/v3.6/op-guide/maintenance/)).

## Why

- **Failure stays with one tenant.** With a store per tenant, a slow disk, a
  runaway controller filling the quota, a defragmentation or a lost quorum
  touches one cluster. etcd has only a cluster-wide quota; in a shared store
  one tenant's growth is everyone's `NOSPACE`
  ([quota](https://etcd.io/docs/v3.6/op-guide/maintenance/)).
- **Backups and restores are per tenant.** A snapshot of a dedicated etcd is
  exactly one cluster; restoring it touches nobody else.
- **Isolation in a shared store rests on code that must be right every
  time:** the prefix range of every role and the mapping from names to
  prefixes. A wrong range end or two names normalised to one prefix exposes
  one tenant's Secrets to another. Using UIDs and the client library's range
  end removes both, but only a dedicated store removes the class of bug.
- **The people who run hosted control planes at scale chose the same:**
  Gardener and HyperShift run an etcd per control plane.
- **etcd-druid is the most complete engine available today.** Of the etcd
  operators we compared, it is the only one with scheduled full and delta
  backups, automatic single-member restore, learner-based scaling and
  hibernation; it is released about monthly and runs Gardener's fleet.

| | etcd-druid | own StatefulSet + etcd-backup-restore | etcd-io/etcd-operator | Cozystack etcd-operator |
|---|---|---|---|---|
| maturity | production at Gardener, v0.39 | the components are; the chart is an example | pre-release v0.2.0 | early alpha |
| backups | full + delta, scheduled | the same sidecar | none yet (roadmap v0.4) | one-shot snapshot objects |
| restore | automatic for one member; quorum loss by hand | automatic for one member | none | at bootstrap only |
| scaling | 1 → 3 and back | static 1 or 3 | up and down | up and down |
| hibernation | `replicas: 0`, snapshot first | by hand | no | `replicas: 0` |

Sources: [etcd-druid releases](https://github.com/gardener/etcd-druid/releases),
[etcd-backup-restore getting started](https://github.com/gardener/etcd-backup-restore/blob/master/docs/deployment/getting_started.md),
[etcd-operator roadmap](https://github.com/etcd-io/etcd-operator/blob/main/docs/roadmap.md),
[Cozystack etcd-operator](https://github.com/cozystack/etcd-operator).

## Rejected

- A shared etcd by default — the density is real, the isolation is not:
  no per-tenant quota, one outage for every tenant on it.
- etcd-io/etcd-operator — the official home of an etcd operator and the one
  to watch, but pre-release and without backups; re-evaluated when its
  backups land.
- Our own StatefulSet with the etcd-backup-restore sidecar — the fallback if
  the spike rejects etcd-druid; it covers one member well and three only
  statically.
- kine with SQL or NATS — a second storage semantics with no auth API, so
  isolation means a database per tenant; nothing it offers outweighs that
  for us.

## Consequences

- etcd-druid becomes a dependency with its own release cadence and image
  registry (Gardener's public Artifact Registry, amd64 and arm64); Brygga's
  version table pins the druid chart and its images, and the operator
  configuration's registry mirror applies to them too.
- The etcd version is the one etcd-druid's wrapper runs, on the 3.5 line
  today, while kubeadm pins 3.6 for Kubernetes 1.34 and 1.35; both lines are
  supported by those Kubernetes releases. Moving to 3.6 follows etcd-druid.
- A restore from snapshot starts a new etcd cluster and, as etcd-druid does
  it, keeps the snapshot's revisions: clients with caches must relist. The
  Datastore controller rolls the tenant's API server after any restore it
  detects, and the in-place restore runbook uses
  `etcdutl snapshot restore --bump-revision --mark-compacted`, which ends
  every watch so informers relist
  ([recovery](https://etcd.io/docs/v3.6/op-guide/recovery/)). Making the
  sidecar bump revisions itself is owed upstream work.
- Permanent loss of quorum is recovered by hand
  ([runbook](../operations.md#lost-etcd-quorum)), as etcd-druid documents.
- Snapshots hold the tenant's Secrets as the API server stored them, so
  encryption at rest is on by default (`secretbox`), and the encryption key is
  backed up next to the snapshots; bucket-side encryption is recommended on
  top.
- The `Datastore` kind keeps a `driver` field reserved, so kine or another
  store can be added later without a new API version.

## Code

none yet.
