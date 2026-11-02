# A ControlPlane restored from a Backup replaces the Backup's source, and runs no controller manager until it is released

Decided 2026-10-27.
The `Backup`, its seal and its copy are built; the restore is not yet: phase 2's wave 1b (backups) builds it ([roadmap](../roadmap.md)).

## Decision

**A restore replaces.**
A `ControlPlane` with `spec.restoreFrom` starts its etcd from a `Backup` of another `ControlPlane`, its source, and takes the source's place: the same objects, their UIDs, the volumes and load balancers they name.
Cloning beside a live source is left to tools that restore through the API, such as Velero, which give every object a new UID ([restore.go](https://github.com/vmware-tanzu/velero/blob/v1.18.4/pkg/restore/restore.go#L2416-L2435)).
The restored `ControlPlane` gets new CAs, a new ServiceAccount key and a new encryption key first in its configuration, so the source's nodes and tokens are not trusted, and nodes join it anew; a restore that keeps the source's CAs is phase 3's.

**Inert until released.**
The controller manager runs only once `spec.restoreFrom.release` is `Released` and no `ControlPlane` with the Backup's source UID exists in the namespace.
`release` may be `Released` when the `ControlPlane` is created, for a source that is already gone ([the field](../api/controlplane.md#spec)).
Until then Workloads renders the controller manager at 0 replicas in the Split layout, and its pod without that container in the Compact one; `APIServerReady` counts a component Lodia runs at 0 on purpose as available, its message saying the controller manager is held, so `AccessReady`, Tenant and Health, which wait on it, go on.
The API server serves the restored data, and the scheduler runs, harmless without nodes.
Health reports `ControllersLeading=False`, reason `AwaitingRelease`, or `SourceExists` while the source does ([reasons](../api/controlplane.md#conditions-and-their-reasons)).

**The release goes through published conditions.**
Once `release` is `Released` and no `ControlPlane` with the source's UID exists, Workloads scales the controller manager up only when `AddonsApplied` is True for that generation: Tenant applies the cluster's objects first, and no controller calls another.
Were the held controller manager unavailable instead, `AccessReady` would wait on `APIServerReady`, Tenant on `AccessReady`, and the release on Tenant: a deadlock.

**The seal.**
Before it copies, the backup reconciler of the Datastore controller puts the key `/lodia.io/backups/<Backup UID>` (`BackupSealKey`) into the source's etcd, asks etcd-druid for a full snapshot, and copies only once the full snapshot Lease `<cp>-etcd-full-snap` has reached that key's revision: an `EtcdCopyBackupsTask` with `maxBackups: 1` and no `maxBackupAge`, from the source's prefix to `<class prefix>/<namespace UID>/backups/<Backup UID>`, which nothing writes again.
Deltas do not count: the copy takes the newest full snapshot with the deltas after it, and nothing from deltas alone ([miscellaneous.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/miscellaneous/miscellaneous.go#L196-L214)), which is all a store moved through `<cp>-etcd-backup` holds until its next scheduled full snapshot, and a copy that finds nothing succeeds ([copier.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/snapshot/copier/copier.go#L121-L125)).
A finished `EtcdOpsTask` names no revision ([etcdopstask.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/api/core/v1alpha1/etcdopstask.go#L137-L161)), so the reconciler asks again until the Lease shows the seal, each time no sooner after the last task finished than the `Backup` has run, at most an hour, since each costs the source a full snapshot; a Lease created before the source's `Etcd` is a predecessor's and counts for nothing.
The copy holds the source's data at least up to the seal, but for one race with etcd-druid's compaction ([Consequences](#consequences)).
The `Backup` records where the copy is, the seal's revision, and the source's name, UID, version and network ([fields](../api/controlplane.md#backup)), and outlives its source.

**The restore checks before it starts anything.**
The Datastore controller copies only from the store the `Backup` records, and only while it equals the class's store; then it requires the seal key in the restored etcd.
Otherwise it reports `DatastoreReady=False`, `RestoreFailed`, cleans up nothing and lets no API server start ([the other reasons](../api/controlplane.md#conditions-and-their-reasons)).

**One transaction unbinds the data from the source.**
Before any API server reads the data, one conditional transaction keyed `/lodia.io/restores/<ControlPlane UID>` (`RestoreMarkerKey`) deletes the Nodes, the Leases in `kube-system` and `kube-node-lease`, the CertificateSigningRequests, PodCertificateRequests, bootstrap token Secrets and `extension-apiserver-authentication`, and creates the key.
etcd compares a missing key as one created at revision 0 ([apply.go](https://github.com/etcd-io/etcd/blob/v3.5.27/server/etcdserver/apply.go#L562-L585)), so the transaction is sent on every pass and does nothing once done; no status says whether it ran.
Before it, the controller asserts that it deletes fewer than 10240 keys, the limit at which etcd-backup-restore could no longer replay the transaction from a delta snapshot ([restorer.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/types/restorer.go#L27)).
VolumeAttachments stay: deleting them would leave the cloud's attachments behind, which the restored cluster then cannot detach.
The keys are the API server's own under its default prefix, Nodes as `minions` ([default_storage_factory_builder.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/kubeapiserver/default_storage_factory_builder.go#L45-L53)), so `apiServer.extraArgs` refuses `etcd-prefix` and `etcd-servers-overrides`, and an envtest per supported minor pins them.

**Durable before ready.**
A full snapshot follows the transaction, and `DatastoreReady` turns True only once a snapshot Lease shows it in the store: a member that loses its volume afterwards restores the cleaned data, not the source's.
Datastore then writes `status.datastore.restore` ([field](../api/controlplane.md#status)).

**The encryption key stays in the management cluster.**
When it seals, the backup reconciler writes `<backup>.encryption-config` (`BackupEncryptionConfigSecretName`), owned by the `Backup`, immutable, with every key of the source's encryption configuration, and never puts a key in the bucket, whose credentials sit in every tenant namespace on the class.
PKI copies those providers once into the restored `ControlPlane`'s configuration, behind a new key of its own, and never reads the `Backup` again; while it has not, the finalizer `lodia.io/restore-source` keeps the `Backup`, and PKI reports `CertificatesReady=False`, `WaitingForBackup`.
PKI adopts no Secret under its names for a restored `ControlPlane`: one that exists is `SecretsExist`.
The platform backs up Lodia's Secrets, as it must for the CAs; a KMS provider is the long-term path, its key wrapped beside the snapshots.

**A management cluster comes back only with its UIDs.**
Datastore refuses to start an empty etcd when the cluster CA PKI adopted was issued before the `ControlPlane` existed, `DatastoreReady=False`, `CAPredatesControlPlane`: that is an identity whose data is lost, and old kubelets would follow it to an empty cluster.
Only a backup that keeps UIDs and status, an etcd snapshot of the management cluster, recovers one.
A `Backup` carries its own UID in the annotation `lodia.io/sealed-uid` once it is `Completed`, since an annotation survives such a restore and a UID does not; one that carries another UID, or the annotation without its `Completed` status, has lost the status that says where its copy is, and is `SealLost` for good instead of being sealed again from a source that may be empty.

**What platforms do.**
They point nothing that holds credentials outside the cluster at a restored `ControlPlane` before its release: a cloud controller manager, CSI controllers, external-dns, Cluster API infrastructure.
Lodia's Cluster API provider reports it uninitialized until then.

**Not built: an authorization of its own.**
Creating a `ControlPlane` with `restoreFrom` reads the `Backup`'s data.
A ValidatingAdmissionPolicy could require a `restore` verb on the named `Backup`, but `backupRef` is local to the namespace, so whoever creates a `ControlPlane` there already reaches the namespace's data; the docs say so, and `todo.md` keeps the policy.

## Why

- **The properties that make a copy dangerous are the ones a replacement needs.**
  The same UIDs, volume handles, load balancer names and DNS owner that let a restore take over the source's world make a clone beside it destructive: a cloud controller manager names load balancers after Service UIDs ([cloud.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/cloud-provider/cloud.go#L89-L98)).
  One meaning has to be chosen, and every etcd-level product chose replace.
- **Everyone deactivates the source before activating the copy.**
  Gardener stops the source's API server and tears it down before it restores ([control_plane_migration.md](https://github.com/gardener/gardener/blob/v1.154.0/docs/operations/control_plane_migration.md#L26)), and its proposal for an unreachable source wanted the control plane in the source "deactivated before it is activated in the destination" ([GEP-17](https://github.com/gardener/enhancements/blob/08d31385137a519c06396f34dee8bd06386dbc7c/geps/0017-shoot-control-plane-migration-bad-case/README.md#L5)); HyperShift deletes the HostedCluster first ([same-cluster-restore.md](https://github.com/openshift/hypershift/blob/v0.1.86/docs/content/how-to/disaster-recovery/scenarios/same-cluster-restore.md#L70-L83)); `clusterctl move` pauses the source ([move.md](https://github.com/kubernetes-sigs/cluster-api/blob/v1.14.3/docs/book/src/clusterctl/commands/move.md#L31-L39)).
  Lodia sees a source on its own management cluster, so it checks cheaply; for one it cannot see, the owner's release is the assertion.
- **The inert phase still pays.**
  The owner can check the copy while the source is intact, as Stork keeps a migrated application scaled to 0 until it is activated ([migration.go](https://github.com/libopenstorage/stork/blob/v25.1.0/pkg/migration/controllers/migration.go#L212-L215)), and it covers a source Lodia cannot see.
- **The release is spec, not a token or a status.**
  A server-side apply drops an annotation its applier stops declaring, the schema's CEL cannot read annotations, and a status is lost on a move or a restore of the management cluster; a one-way field in spec fails a sync loudly instead.
- **A seal key proves identity, not only completeness.**
  A revision alone proves a gap-free chain up to it of some etcd; the key proves this `Backup`'s, and that the copy reached it.
- **Segla's own uses are replacements:** rollback, recovery from data loss and, in phase 3, a region that comes back.

## Rejected

- A clone beside a live source with Lodia's own guards — the list of what acts outside a cluster has no end, and the safe order is the operator's checklist, not a controller's.
- Fencing across management clusters — Gardener built owner checks for it and removed them as "very difficult to manage in a multi-node ETCD setup" ([#6302](https://github.com/gardener/gardener/issues/6302)).
- Holding the controller manager through the lease, as the hold does — it fails open by design and races the controller manager's first start.
- A release annotation in the style of `lodia.io/maintain-requested-at` — see Why.
- Guarding the cleanup with a status field — a status is lost with a move, a stale cache or an operator rollback; the key in etcd is not.
- A fixed marker key — a `Backup` of the restored cluster would carry it, and restoring from that `Backup` would skip the cleanup.
- Detecting a restore and rolling the API server — a restored `ControlPlane` is new, so no client of the source holds its revisions ([dedicated-etcd-by-default](dedicated-etcd-by-default.md)).

## Consequences

- The restored cluster's `kube-system` UID, the cluster ID some tools use, is the source's.
- Every `Backup` takes a full snapshot of its source's etcd, which counts toward the class's `retention.fullSnapshots` ([what it keeps](../README.md#a-tenant-namespace)).
- Until etcd-backup-restore orders snapshot sets by revision, a `Completed` `Backup` may lack its seal: its copier takes the set whose full snapshot was saved last ([miscellaneous.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/miscellaneous/miscellaneous.go#L175-L177)), which an etcd-druid compaction Job running across the `Backup`'s full snapshot saves below the seal; the restore's seal check refuses such a `Backup` (`todo.md`).
- `docs/operations.md` gets the replace checklist and a Velero recipe for a clone with the restore (`todo.md`).
- The phase 2 gate restores a `Backup` ([roadmap](../roadmap.md)).

## Code

- `api/v1alpha1` — `RestoreFrom` with `Release`, `BackupStatus` with its `Ready` condition, `RestoreStatus`, `SnapshotLocation` and `BackupSnapshotLocation` with `StoreSecretFields`, the names `BackupEncryptionConfigSecretName`, `BackupSealKey` and `RestoreMarkerKey`, the annotation `SealedUIDAnnotation`, the finalizer `RestoreSourceFinalizer` and the reasons in `conditions.go`; `test/api/validation_test.go` holds their rules.
- `internal/controller/datastore/backup.go` — the backup reconciler: the seal, the snapshot it waits for, the copy, the encryption keys and the mark; `etcdclient.go` dials a tenant's etcd for it.
- `test/e2e/restore_test.go` — the gate's `TestRestoreGate`, so far up to a `Completed` `Backup` on a real etcd-druid and S3 store.
