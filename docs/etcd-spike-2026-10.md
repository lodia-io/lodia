# The etcd spike — etcd-druid under a tenant's API server, October 2026

A working doc: the phase 0 spike of [dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md),
run on 2026-10-24. What stays true moves into that decision, `docs/README.md`
and `docs/operations.md`; then this file is deleted. How to run it again is at
the end; its files are in [`hack/etcd-spike/`](../hack/etcd-spike/).

## What it says for the decision

etcd-druid v0.39.0 does everything the decision relies on, on a plain kind
cluster with Brygga's own CA on every line: scheduled full and delta
snapshots to an S3-compatible store, a single member restored from them after
losing its data, scale-out learner first and scale-in, a member of three that
lost its volume re-added as a learner, and hibernation that snapshots before
it stops. Each finding below is work the Datastore and PKI controllers take
on; none of them is a reason for the fallback, our own StatefulSet with the
etcd-backup-restore sidecar, which would have to do the same work and more.

Versions: etcd-druid v0.39.0, etcd-wrapper v0.8.2 (etcd 3.5.27),
etcd-backup-restore v0.45.1, Kubernetes 1.35.8 in kind v0.33.0, RustFS 1.0.3
as the store. The load: a ConfigMap written and read back through the API
server once a second (`hack/etcd-spike/load.sh`); a *failed round* is one
whose write or read failed within its 3 s timeout.

## Results

| scenario | took | failed rounds at the API server |
|---|---|---|
| a fresh `Etcd` of one member, every line TLS | ready in 26 s, its first full snapshot in the bucket by then; `BackupReady` at 96 s | — |
| scale 1 → 3 | 95 s from the pre-scale snapshot to the third member ready; 106 s end to end on a second run | 0 |
| one member of three loses its volume | back in 42 s: removed, re-added as a learner with a new ID, promoted | 0 |
| scale 3 → 1 | 21 s; the removed members' volumes deleted | 0 |
| the single member restarts | — | 12 (21 s) |
| the single member loses its data | restored in 33 s from the full snapshot and 4 deltas | 13 (22 s); see finding 5 for what was lost |
| hibernation: `replicas: 0`, then 1 | asleep in 11 s (a full snapshot first, the volume kept); awake in 30 s, the volume reused, no restore | 21 (30 s, the sleep included) |
| a certificate rotated by a new Secret name, one member | rolled in 30 s | 11 (19 s) |
| the same, three members | rolled one by one in 60 s | 0 |

Measured per container with `kubectl top`, under the load:

| container | one member | three members, each |
|---|---|---|
| etcd (etcd-wrapper) | 12m CPU, 45 Mi | 18–22m, 28–42 Mi |
| backup-restore sidecar | 4m, 22 Mi | 2m, 21–23 Mi |
| etcd-druid (one per management cluster) | 2m, 18 Mi | — |
| kube-apiserver | 25m, 175 Mi | — |

At about 1,100 ConfigMaps a full snapshot is 74 KB gzipped and a delta of one
minute about 5 KB. Sizes and times at gigabytes are phase 4's to measure.

## Findings

1. **`backup.tls` is required once `clientUrlTls` is set.** etcd-druid
   probes the etcd-wrapper over HTTPS only when `spec.backup.tls` is set
   ([builder.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/builder.go#L612-L620)),
   while the wrapper serves HTTPS whenever client TLS is on
   ([readycheck.go](https://github.com/gardener/etcd-wrapper/blob/v0.8.2/internal/app/readycheck.go#L127-L128)).
   With client TLS alone the member never turns ready, though every
   condition of the `Etcd` says `True`. Brygga sets client, peer and
   backup-restore TLS together, as Gardener does.
2. **The sidecar's serving certificate names the client Service.**
   etcd-druid calls the sidecar from outside the pod, through
   `<name>-client.<namespace>.svc:8080`, for the full snapshot it takes before
   a scale-out or hibernation and for compaction
   ([ondemandsnapshot.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/etcdopstask/handler/ondemandsnapshot/ondemandsnapshot.go#L103),
   [compaction](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/compaction/snapshot.go#L122)).
   The wrapper reaches it as `<name>-local`, a host alias of `127.0.0.1`
   ([builder.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/builder.go#L211-L217)).
   The certificate needs both; with only `-local` the scale-out waited until
   it was fixed, over ten minutes.
3. **A Secret changed in place is not picked up.** After the sidecar's
   Secret was updated, the sidecar served the old certificate for as long as
   it was watched (over three minutes), and etcd-druid did not roll the
   pod: its only pod checksum is the ConfigMap's
   ([constants.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/common/constants.go#L11)).
   Pointing the `Etcd` at a new Secret name rolls the members: 19 s without
   the API for a single member, no failed round with three. The PKI controller
   rotates etcd's certificates by writing a new Secret and changing the
   reference, and a single-member tenant takes that roll in its maintenance
   window.
4. **A blocked operation is visible only on its `EtcdOpsTask`.** While the
   pre-scale snapshot failed (finding 2), the `Etcd` showed
   `ScaleOperationComplete=False ScalingOut` with no `lastErrors`; the cause
   was in the `EtcdOpsTask` `presync-snapshot-update-3-0` and in
   etcd-druid's log. The Datastore controller reads `EtcdOpsTask`s into its
   condition.
5. **A restore keeps the snapshot's revisions, and the API server must
   restart after it.** The single member came back at revision 1417, the
   last delta's, after clients had seen 1440. The 19 writes acknowledged
   after that delta (21 s of them) were gone, as the delta period bounds.
   Until it restarted, the API server's watch cache still listed those 19
   ConfigMaps, which a GET answered with NotFound, and listed only 1 of the
   17 written after the restore; revisions 1418–1440 now named other writes.
   A rollout of the API server fixed both. This is the decision's
   consequence, now measured: the Datastore controller rolls the API server
   after any restore, until the sidecar can bump revisions
   ([recovery](https://etcd.io/docs/v3.6/op-guide/recovery/)).
6. **Deleting an `Etcd` keeps its volumes and its snapshots.** The
   StatefulSet retains claims on delete and on scale
   (`persistentVolumeClaimRetentionPolicy: Retain`, no owner references),
   and nothing removes the prefix in the bucket. Scale-in is the exception:
   etcd-druid deletes the removed members' claims itself
   ([scale-in proposal](https://github.com/gardener/etcd-druid/blob/v0.39.0/docs/proposals/08-scale-in.md)).
   `reclaimPolicy: Delete` and `retention.afterDelete` of a `DatastoreClass`
   are Brygga's to carry out.
7. **Scheduled defragmentation skips small databases.** The sidecar
   defragments only above 3 GiB or with more than 1 GiB free
   ([etcdutil.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/etcdutil/etcdutil.go#L34-L35),
   [the check](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/etcdutil/etcdutil.go#L460)),
   so a database this small is never defragmented on schedule. Under the
   4 GiB quota Brygga ships, a scheduled run happens near the quota or after
   a large delete.
8. **The chart installs with plain helm from the tag, but is not published.**
   At v0.39.0, `charts/` already holds the CRDs (the copy step in
   [prepare-helm-charts](https://github.com/gardener/etcd-druid/blob/v0.39.0/docs/deployment/prepare-helm-charts.md)
   is for the development tree), the webhook's PKI is needed only with the
   webhook on, and `helm upgrade --install` with `image.tag: v0.39.0` was up
   in 22 s. The chart's version is `0.1.0` and it is released nowhere, so
   Brygga's chart cannot depend on it by version: Brygga vendors it at a tag.
9. **etcd-druid runs etcd 3.5.27.** The `UpgradeEtcdVersion` gate, beta and
   on since v0.39, selects etcd-wrapper v0.8.2 and etcd-backup-restore
   v0.45.1
   ([images.yaml](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/images/images.yaml),
   [feature gates](https://github.com/gardener/etcd-druid/blob/v0.39.0/docs/deployment/feature-gates.md)),
   both built on etcd 3.5.27. kubeadm defaults to etcd 3.6.6 for
   Kubernetes 1.35 and 3.7.0 for 1.37, and accepts an external etcd from
   3.5.24 ([1.35](https://github.com/kubernetes/kubernetes/blob/v1.35.0/cmd/kubeadm/app/constants/constants.go#L326-L329),
   [1.37](https://github.com/kubernetes/kubernetes/blob/v1.37.0/cmd/kubeadm/app/constants/constants.go#L326-L330)).
   etcd released 3.5.35, 3.6.16 and 3.7.3 on 2026-10-22
   ([releases](https://github.com/etcd-io/etcd/releases)). `spec.etcd.image`
   and `spec.backup.image` override the images, so Brygga's version table
   can pin them.
10. **The object store.** MinIO's repository is archived and its community
    edition ships as source only
    ([minio/minio](https://github.com/minio/minio)); etcd-druid's local guide
    uses LocalStack, which now needs an auth token
    ([manage-s3-emulator](https://github.com/gardener/etcd-druid/blob/v0.39.0/docs/deployment/getting-started-locally/manage-s3-emulator.md)).
    The spike used RustFS 1.0.3 (Apache-2.0,
    [rustfs/rustfs](https://github.com/rustfs/rustfs)) through
    `store.endpointOverride` and `s3ForcePathStyle`; the sidecar reads
    `accessKeyID`, `secretAccessKey` and `region` from the Secret
    ([s3_snapstore.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/snapstore/s3_snapstore.go#L283-L310)).

## DatastoreClass → Etcd

How the Datastore controller renders each field of
[`DatastoreClassSpec`](../api/v1alpha1/datastore_types.go) into etcd-druid's
`Etcd` ([etcd.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/api/core/v1alpha1/etcd.go),
[store.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/api/core/v1alpha1/store.go)):

| DatastoreClass | Etcd | note |
|---|---|---|
| `etcd.quotaBackendBytes` | `spec.etcd.quota` | |
| `etcd.autoCompaction.mode`, `.retention` | `spec.sharedConfig.autoCompactionMode`, `.autoCompactionRetention` | shared with the sidecar's embedded etcd |
| `etcd.defragmentation.schedule` | `spec.etcd.defragmentationSchedule` | runs only above the thresholds of finding 7; the order across members was not observed |
| `etcd.resources` | `spec.etcd.resources` | the sidecar's own are `spec.backup.resources`; the class has no field for them, Brygga renders a default |
| `storage.className` | `spec.storageClass` | immutable in the `Etcd` |
| `storage.size` | `spec.storageCapacity` | |
| `storage.reclaimPolicy` | no equivalent | the claims stay (finding 6); `Delete` is Brygga deleting them |
| `backup.store.provider` | `spec.backup.store.provider` | Brygga's four are among etcd-druid's |
| `backup.store.bucket` | `spec.backup.store.container` | |
| `backup.store.prefix` | `spec.backup.store.prefix` | required by etcd-druid; Brygga adds the tenant below the class's prefix |
| `backup.store.region` | the credentials Secret, key `region` | not an `Etcd` field |
| `backup.store.endpoint` | `spec.backup.store.endpointOverride` | plus `s3ForcePathStyle` in the Secret for most S3-compatible stores |
| `backup.store.secretRef` | `spec.backup.store.secretRef` | mounted as a volume, so it must sit in the tenant's namespace: Brygga copies the class's Secret there |
| `backup.fullSnapshotSchedule` | `spec.backup.fullSnapshotSchedule` | |
| `backup.deltaSnapshotPeriod` | `spec.backup.deltaSnapshotPeriod` | bounds what a restore loses (finding 5) |
| `backup.retention.days` | `spec.backup.garbageCollectionPolicy: LimitBased` with `maxBackupsLimitBasedGC` | etcd-druid counts full snapshots, not days; one full snapshot a day makes them equal |
| `backup.retention.afterDelete` | no equivalent | the prefix stays (finding 6); Brygga removes it after the period |
| `placement.nodeSelector` | `spec.schedulingConstraints.affinity` | etcd-druid has no node selector; Brygga writes required node affinity |
| `placement.tolerations` | `spec.schedulingConstraints.tolerations` | |
| `placement.priorityClassName` | `spec.priorityClassName` | |
| — (`ControlPlane`) | `spec.replicas` | 1, or 3 with `highAvailability`; 0 while hibernating |
| — (PKI controller) | `spec.etcd.clientUrlTls`, `spec.etcd.peerUrlTls`, `spec.backup.tls` | all three (findings 1–3) |

## What assumes Gardener

| what | where | what Brygga does |
|---|---|---|
| every line TLS or none | finding 1 | sets all three TLS blocks |
| certificates rotate by new Secret names: Gardener's secrets manager writes immutable Secrets named after a checksum of their config ([manager.go](https://github.com/gardener/gardener/blob/2a9e7795749d1122a171caf69aabd6426db10777/pkg/utils/secrets/manager/manager.go#L405), [generate.go](https://github.com/gardener/gardener/blob/ac0343dcd77a3c14635bb9006f276ffc2cc30906/pkg/utils/secrets/manager/generate.go#L343)) | finding 3 | the PKI controller rotates the same way |
| NetworkPolicies come from the consumer | [etcd-druid#505](https://github.com/gardener/etcd-druid/issues/505) | admits the API server and the members on 2379 and 2380, etcd-druid on 2379 and 8080 |
| compaction Jobs carry Gardener's network-policy labels (`networking.gardener.cloud/to-dns`, `to-private-networks`, `to-public-networks`) | [reconciler.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/compaction/reconciler.go#L720-L722) | its NetworkPolicy lets compaction Jobs reach DNS and the store |
| images from Gardener's Artifact Registry | [images.yaml](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/images/images.yaml) | pins them in the version table, under the registry mirror |
| deployed through skaffold and Gardener's own charts | finding 8 | vendors the chart at a tag |
| `EtcdCopyBackupsTask`, for Gardener's control plane migration between seeds | the CRD in `charts/crds` | unused |
| the `etcdComponentProtection` webhook, with Gardener's exempt service accounts | the chart's values | off, the chart's default |

## Open questions

- How the Datastore controller learns that a restore happened, to roll the
  API server (finding 5): the sidecar logs it, but no condition of the
  `Etcd` said so in this run.
- In which order the sidecars defragment three members (finding 7 kept
  them from running here).
- Restore, scale and snapshot times at gigabytes, and etcd-druid at a few
  hundred `Etcd` objects — phase 4.

## How to run it again

From the repository root, on a laptop with Docker (about 4 GB for the
spike), helm and the hard way's tools; the hard way may run beside it on its
own cluster and port. Every command
takes the spike's kubeconfig through `mk` and `tk`, never your current
context.

```bash
source hack/etcd-spike/env.sh
go tool kind create cluster --name "$MGMT" --image "$KIND_NODE_IMAGE" --config "$ES/kind.yaml" --kubeconfig "$WORK/management.kubeconfig"
source hack/etcd-spike/env.sh                     # again, for MGMT_IP
bash hack/hard-way/pki.sh && bash "$ES/pki.sh"    # the hard way's PKI, then etcd-druid's certificates from its etcd CA
bash hack/hard-way/kubeconfigs.sh
git clone --depth 1 --branch "$DRUID_VERSION" https://github.com/gardener/etcd-druid "$WORK/etcd-druid"

mk create namespace "$CP"
mk -n "$CP" create secret generic "$CP-etcd-ca" --from-file=ca.crt="$PKI/etcd-ca.crt"
for s in server peer client; do mk -n "$CP" create secret tls "$CP-etcd-$s" --cert="$PKI/etcd-druid-$s.crt" --key="$PKI/etcd-druid-$s.key"; done
mk -n "$CP" create secret tls "$CP-etcd-backup-restore" --cert="$PKI/etcd-druid-backup.crt" --key="$PKI/etcd-druid-backup.key"
mk -n "$CP" create secret generic "$CP-etcd-backup" --from-literal=accessKeyID=spike-access \
  --from-literal=secretAccessKey=spike-secret-key --from-literal=region=us-east-1 --from-literal=s3ForcePathStyle=true
mk -n "$CP" create secret generic "$CP-pki" --from-file="$PKI"
render "$ES/rustfs.yaml" | mk apply -f -
helm --kubeconfig "$WORK/management.kubeconfig" upgrade --install etcd-druid "$WORK/etcd-druid/charts" \
  -n etcd-druid --create-namespace -f "$ES/druid-values.yaml" --wait
mk apply -f "https://github.com/kubernetes-sigs/metrics-server/releases/download/$METRICS_SERVER_VERSION/components.yaml"
mk -n kube-system patch deployment metrics-server --type json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'

render "$ES/etcd.yaml" | mk apply -f -
mk -n "$CP" wait --for=jsonpath='{.status.ready}'=true "etcd/$CP-etcd" --timeout=300s
render "$ES/apiserver.yaml" | mk apply -f -
kubectl --kubeconfig "$WORK/super-admin.conf" apply -f "$HW/tenant/00-rbac.yaml"
bash "$ES/load.sh" > "$WORK/load.log" &           # leave it running through the scenarios
```

The scenarios, each timed with `date -u +%T` before and after; `fails FROM TO`
counts the failed rounds between two times:

```bash
fails() { awk -v a="$1" -v b="$2" '$1>=a && $1<=b && $2=="FAIL" {n++; if(!f) f=$1; l=$1} END {print n+0, "failed rounds", f, l}' "$WORK/load.log"; }
scale() { mk -n "$CP" patch etcd "$CP-etcd" --type merge -p "{\"spec\":{\"replicas\":$1}}"; }

scale 3                                             # scale out; scale 1 scales in
mk -n "$CP" wait --for=condition=ScaleOperationComplete "etcd/$CP-etcd" --timeout=600s

mk -n "$CP" delete pvc "$CP-etcd-$CP-etcd-2" --wait=false; mk -n "$CP" delete pod "$CP-etcd-2"   # one of three loses its volume
mk -n "$CP" wait --for=condition=AllMembersReady "etcd/$CP-etcd" --timeout=600s

mk -n "$CP" delete pvc "$CP-etcd-$CP-etcd-0" --wait=false                                           # the single member loses its data
mk -n "$CP" delete pod "$CP-etcd-0" --grace-period=0 --force
mk -n "$CP" wait --for=jsonpath='{.status.ready}'=true "etcd/$CP-etcd" --timeout=600s
tk -n default get configmap load-<n>                # a write from after the last delta: NotFound
tk -n default get configmaps | grep "load-<n> "     # the API server's cache still lists it, until:
mk -n "$CP" rollout restart "deployment/$CP-apiserver"

scale 0                                             # hibernate; scale 1 wakes it

bash "$ES/pki.sh" etcd-druid-backup                 # a certificate rotated the Gardener way
mk -n "$CP" create secret tls "$CP-etcd-backup-restore-2" --cert="$PKI/etcd-druid-backup.crt" --key="$PKI/etcd-druid-backup.key"
mk -n "$CP" patch etcd "$CP-etcd" --type json \
  -p '[{"op":"replace","path":"/spec/backup/tls/serverTLSSecretRef/name","value":"'"$CP"'-etcd-backup-restore-2"}]'

mk top pods -A --containers
```

Clean up with `kill %1`, `go tool kind delete cluster --name "$MGMT"
--kubeconfig "$WORK/management.kubeconfig"` and `rm -rf "$WORK"`.
