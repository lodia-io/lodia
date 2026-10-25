# Every tenant gets its own etcd, run by etcd-druid; a shared etcd is an opt-in for density

Decided 2026-10-24. The phase 0 spike found etcd-druid fit
([etcd spike](../etcd-spike-2026-10.md)). The Datastore controller is built
for the dedicated case with one member: the `Etcd` from the class recorded at
creation and the newest certificate generation, the finalizer with the
reclaim policy, `VolumesExist` and `DatastoreReady`; three members,
hibernation, class edits held for the maintenance window, the final snapshot
and the backups' retention after deletion, restore and the shared store are
not.

## Decision

- **Dedicated by default.** `spec.datastore.dedicated` makes the Datastore
  controller create one `Etcd` resource (`druid.gardener.cloud/v1alpha1`) in
  the tenant's namespace: 1 member, or 3 with `highAvailability`, built from
  the `DatastoreClass` (quota, compaction, storage, backup store and
  schedules, defragmentation, placement). Brygga's PKI controller issues the
  etcd CA and the server, peer and client certificates and the backup-restore
  sidecar's serving certificate; etcd-druid takes them as Secrets, because it
  does not generate TLS itself
  ([securing etcd clusters](https://gardener.github.io/etcd-druid/usage/securing-etcd-clusters.html)),
  and the `Etcd` names the newest complete generation of them, never one
  below the generation it already names, so a rotation is one roll of the
  members
  ([staged-credential-rotation](staged-credential-rotation.md)).
  Client, peer and backup-restore TLS are set together, and the sidecar's
  certificate also names the client Service, through which etcd-druid calls
  it ([etcd spike](../etcd-spike-2026-10.md), findings 1 and 2).
- **The engine is etcd-druid**, installed beside Brygga (Brygga's chart
  vendors etcd-druid's at a tag, since it is not released as a versioned
  chart), run with `enableEtcdSpecAutoReconcile: true` so a spec
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
  - defragmentation on a schedule, which the sidecar skips below 3 GiB
    unless more than 1 GiB is free
  ([managing](https://gardener.github.io/etcd-druid/usage/managing-etcd-clusters.html),
  [recovering](https://gardener.github.io/etcd-druid/usage/recovering-etcd-clusters.html)).
- **Brygga adds what etcd-druid leaves to its consumer:** the NetworkPolicy
  that admits only the tenant's API server, the members themselves and
  etcd-druid, which calls the members on 2379 and their sidecars on 8080
  ([etcd-druid#505](https://github.com/gardener/etcd-druid/issues/505)); the
  ServiceMonitor for the etcd and sidecar metrics; the `Backup` object as an
  on-demand snapshot through the sidecar; the failures of etcd-druid's
  `EtcdOpsTask`s in its own condition; and deleting the volumes and snapshots
  of a deleted `ControlPlane`, which etcd-druid keeps
  ([etcd spike](../etcd-spike-2026-10.md), findings 2, 4 and 6).
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
- Our own StatefulSet with the etcd-backup-restore sidecar — it covers one
  member well and three only statically.
- kine with SQL or NATS — a second storage semantics with no auth API, so
  isolation means a database per tenant; nothing it offers outweighs that
  for us.

## Consequences

- etcd-druid becomes a dependency with its own release cadence and image
  registry (Gardener's public Artifact Registry, amd64 and arm64); Brygga's
  version table pins the vendored chart and its images, and the operator
  configuration's registry mirror applies to them too.
- The etcd version is the one etcd-druid's wrapper runs: etcd 3.5.27 with
  etcd-druid v0.39.0, while kubeadm defaults to 3.6.6 for Kubernetes 1.35 and
  3.7.0 for 1.37 and accepts an external etcd from 3.5.24
  ([1.35](https://github.com/kubernetes/kubernetes/blob/v1.35.0/cmd/kubeadm/app/constants/constants.go#L326-L329),
  [1.37](https://github.com/kubernetes/kubernetes/blob/v1.37.0/cmd/kubeadm/app/constants/constants.go#L326-L330)).
  `spec.etcd.image` and `spec.backup.image` let the version table pin newer
  builds; moving to 3.6 follows etcd-druid.
- A restore from snapshot starts a new etcd cluster and, as etcd-druid does
  it, keeps the snapshot's revisions: clients with caches must relist. The
  spike measured it: until it restarted, the API server's cache listed 19
  objects lost with the data and missed 16 of 17 written afterwards. The
  Datastore controller rolls the tenant's API server after any restore it
  detects, and the in-place restore runbook uses
  `etcdutl snapshot restore --bump-revision --mark-compacted`, which ends
  every watch so informers relist
  ([recovery](https://etcd.io/docs/v3.6/op-guide/recovery/)). Making the
  sidecar bump revisions itself is owed upstream work.
- Permanent loss of quorum is recovered by hand
  ([runbook](../operations.md#lost-etcd-quorum)), as etcd-druid documents.
- A tenant's data never outlives it into another `ControlPlane` by name. The
  backup prefix is the class's prefix plus `<namespace>/<name>/<uid>`, because
  etcd-backup-restore restores an empty member from the newest snapshot under
  its prefix
  ([design](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/docs/proposals/design.md#L27)).
  The volumes keep their names (`<cp>-etcd-<cp>-etcd-<n>`) and a StatefulSet
  re-attaches a claim of that name
  ([stateful_pod_control.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/statefulset/stateful_pod_control.go#L381-L405)),
  whatever its labels, so when such claims of a retained `<cp>-etcd` exist
  before a new `ControlPlane`'s first `Etcd`, Datastore stops with
  `DatastoreReady=False`, `VolumesExist`,
  until someone deletes them; starting from old data is a restore. The same
  holds for an `Etcd` `<cp>-etcd` whose controller reference is not the
  `ControlPlane`'s UID, an orphan or a predecessor's not yet collected:
  Datastore neither adopts it, which a server-side apply with
  `ForceOwnership` would do, nor deletes it.
- Every edit of a class that reaches the `Etcd` rolls its members, a single
  member with an outage, so a class edit is a Brygga-initiated change for each
  tenant on it and rolls in that tenant's maintenance window
  ([upgrades-and-maintenance-windows](upgrades-and-maintenance-windows.md));
  the `Etcd` records the class generation it was rendered from. The storage
  class and size apply when the `Etcd` is created and not after: the first is
  immutable in the `Etcd`
  ([etcd.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/api/core/v1alpha1/etcd.go#L627-L638)),
  the second a StatefulSet field that cannot change in place
  ([validation.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/apis/apps/validation/validation.go#L262)).
- The class is resolved once, when the `Etcd` is created, and recorded on it
  (`brygga.segla.io/datastore-class`, beside
  `brygga.segla.io/datastore-class-generation`); from then on the `Etcd`
  follows the recorded class, its `reclaimPolicy` included, the way a default
  StorageClass is written into a claim when the claim is created
  ([storage classes](https://kubernetes.io/docs/concepts/storage/storage-classes/#default-storageclass)).
  A default that moves therefore re-renders no running etcd and never turns a
  `Retain` into a `Delete`.
- A recorded class that is deleted stops only its own edits. The `Etcd` keeps
  every field the class gave it, the way a dynamically provisioned
  PersistentVolume keeps in its own spec the reclaim policy and mount options
  its StorageClass gave it
  ([storage classes](https://kubernetes.io/docs/concepts/storage/storage-classes/#reclaim-policy)),
  and it keeps following the certificate generations and reporting its
  readiness: with the default `pki.leafLifetime` of a year
  ([operator configuration](../README.md#installation)), PKI issues the next
  generation after eight months, two thirds of it
  ([staged-credential-rotation](staged-credential-rotation.md)), and the
  current one expires at a year. `DatastoreClassNotFound` is therefore a
  reason only before the `Etcd` exists. A class whose credentials copy
  cannot be made — its Secret missing, another owner holding
  `<cp>-etcd-backup`, the API server refusing the copy — is held the same
  way, whole, with a note in `DatastoreReady`'s message, since the rest of an
  edit could add a store whose copy does not exist; only the copy stops, never
  the certificate generations.
- The claims carry the `ControlPlane`'s UID as a label and the class as an
  annotation, since they outlive the `Etcd`: deletion reclaims only this
  `ControlPlane`'s claims, under the class each records, and keeps them when
  that class is gone. While the `Etcd` exists, the deletion labels the claims
  no pass has labelled yet, such as one the StatefulSet created seconds
  before; they can only be its own, since `VolumesExist` kept the `Etcd` from
  being created while anything else stood there. A claim that appears after
  the last such pass stays unlabelled and is kept, the safe side.
- Snapshots hold the tenant's Secrets as the API server stored them, so
  encryption at rest is on by default (`secretbox`), and the encryption key is
  backed up next to the snapshots; bucket-side encryption is recommended on
  top.
- A class's credentials reach every tenant's prefix on that class, and
  `<cp>-etcd-backup` puts them into every tenant namespace: whoever reads
  Secrets in one tenant namespace reads every tenant's snapshots on the
  class, and the encryption keys backed up next to them. Until the
  credentials are scoped to one tenant's prefix, two rules bound it: Secrets
  in tenant namespaces are readable by Brygga and the platform's
  administrators only, and a class per trust boundary.
- etcd-druid keeps the volumes and the snapshots of a deleted `Etcd`: the
  StatefulSet retains its claims and nothing removes the prefix in the
  bucket ([etcd spike](../etcd-spike-2026-10.md), finding 6). Under
  `storage.reclaimPolicy: Delete` the Datastore controller deletes the claims
  itself, finding them by the name the StatefulSet gives them,
  `<template>-<StatefulSet>-<ordinal>` with the template named after the
  `Etcd`
  ([stateful_set_utils.go](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/controller/statefulset/stateful_set_utils.go#L116-L119),
  [builder.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/builder.go#L233)),
  not by the labels it copies onto them, which a claim made by hand lacks;
  the snapshots' retention after deletion is phase 2.
- The `Etcd` names the CA's key, `dataKey: ca.crt`, on all three TLS blocks:
  etcd-druid defaults to `ca.crt` for the members and to `bundle.crt` for its
  own calls to the backup-restore sidecar
  ([builder.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/builder.go#L468),
  [snapshot.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/compaction/snapshot.go#L87)).
- The `Datastore` kind keeps a `driver` field reserved, so kine or another
  store can be added later without a new API version.

## Code

- `internal/controller/datastore` — the controller: `datastore.go` holds the
  reconcile, the class lookup, the newest complete certificate generation,
  the claims, found by name, the credentials copy, the finalizer and the
  status; `etcd.go` renders the `DatastoreClass` into the `Etcd`
  ([the table](../README.md#a-tenant-namespace)), applies it and reads
  etcd-druid's report. Registered as `datastore` in `cmd/brygga/main.go`.
- `internal/controller/datastore/datastore_test.go` — envtest with
  etcd-druid v0.39.0's CRDs from its `crds` package, the controller running
  as a user bound to exactly the rules in
  [Identity and tenancy](../README.md#identity-and-tenancy):
  `TestFinalizerAndEtcdFromClass` (the finalizer before the first `Etcd`, the
  mapping table, generation 1 in all three TLS blocks),
  `TestConditionsFromState` (Unknown on the first visit, an API error leaves
  the status alone), `TestEtcdRefused` (an admission policy refuses the
  `Etcd`), `TestPaused`, `TestNewestCompleteGeneration`,
  `TestGenerationsMoveForward`, `TestDatastoreClassNotFound`,
  `TestBackupCredentialsCopied`, `TestHeldClass`,
  `TestBackupCopyFollowsItsEvents`, `TestPredecessorsCopy`,
  `TestDatastoreReadyFollowsEtcdStatus`,
  `TestVolumesExist`, `TestForeignEtcd`, `TestStorageAtCreation`,
  `TestRecordedClass`, `TestClassGone`, `TestDeletionBeforeLabelling` and
  `TestDeletion`. `datastoretest` stands in for PKI there.
- `test/e2e/datastore_test.go` — `TestDatastoreOnKind`: etcd-druid v0.39.0
  from its chart at the tag, the PKI and Datastore controllers in the test
  process, a `ControlPlane` to `DatastoreReady=True` with the member running
  on PKI's generation 1, a member Secret of generation 1 deleted and PKI's
  generation 2, under the same CA, rolling the member, deletion under
  `Delete`.
