# The etcd spike — etcd-druid under a tenant's API server, October 2026

A working doc: the phase 0 spike of
[dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md), run on
2026-10-24. What stays true moves into that decision, `docs/README.md` and
`docs/operations.md`; then this file is deleted. Its files are in
[`hack/etcd-spike/`](../hack/etcd-spike/).

Moved on 2026-10-25, with the Datastore controller: the **DatastoreClass →
Etcd** table into [`README.md`](README.md#a-tenant-namespace), where the code
is checked against it; what
findings 1, 3 and 6 made the controller do into
[the decision](decisions/dedicated-etcd-by-default.md) and
[`operations.md`](operations.md); installing etcd-druid from its chart at the
tag, as **How to run it again** does, into the e2e suite
([`test/e2e`](../test/e2e/README.md)). The findings stay as the spike's
record; the open questions stay open.

## For the decision

etcd-druid v0.39.0 did everything the decision relies on (Results). The
findings are work for the Datastore and PKI controllers, not a reason for the
fallback: our own StatefulSet with the etcd-backup-restore sidecar would need
the same work and more.

Versions: etcd-druid v0.39.0, etcd-wrapper v0.8.2 (etcd 3.5.27),
etcd-backup-restore v0.45.1, Kubernetes 1.35.8 in kind v0.33.0, RustFS 1.0.3
as the store. The load: a ConfigMap written and read back through the API
server once a second (`hack/etcd-spike/load.sh`); a *failed round* is one
whose write or read failed within its 3 s timeout.

## Results

| scenario | took | failed rounds |
|---|---|---|
| a new `Etcd` of one member, every line TLS | 26 s to ready, the first full snapshot taken | — |
| scale 1 → 3 | 106 s | 0 |
| one member of three loses its volume | 42 s, re-added as a learner | 0 |
| scale 3 → 1 | 21 s, the removed members' volumes deleted | 0 |
| the single member restarts | — | 12 (21 s) |
| the single member loses its data | 33 s, restored from a full snapshot and 4 deltas | 13 (22 s); the writes since the last delta lost (finding 5) |
| hibernation: `replicas: 0`, then 1 | 11 s to sleep (a full snapshot first, the volume kept), 30 s to wake (the volume reused) | 21 (30 s, the sleep included) |
| a certificate renewed under a new Secret name, one member | 30 s, the pod rolled | 11 (19 s) |
| the same, three members | 60 s, rolled one by one | 0 |

Per container under the load (`kubectl top`):

| container | one member | three members, each |
|---|---|---|
| etcd (etcd-wrapper) | 12m CPU, 45 Mi | 18–22m, 28–42 Mi |
| backup-restore sidecar | 4m, 22 Mi | 2m, 21–23 Mi |
| etcd-druid, one per management cluster | 2m, 18 Mi | — |
| kube-apiserver | 25m, 175 Mi | — |

At about 1,100 ConfigMaps a full snapshot is 74 KB gzipped, a one-minute
delta about 5 KB.

## Findings

1. **`backup.tls` is required once `clientUrlTls` is set.** etcd-druid
   probes the etcd-wrapper over HTTPS only when `spec.backup.tls` is set
   ([builder.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/builder.go#L612-L620)),
   while the wrapper serves HTTPS whenever client TLS is on
   ([readycheck.go](https://github.com/gardener/etcd-wrapper/blob/v0.8.2/internal/app/readycheck.go#L127-L128)).
   With client TLS alone the member never turns ready, though every
   condition of the `Etcd` says `True`.
2. **The sidecar's certificate names the client Service.** etcd-druid calls
   the sidecar through `<name>-client.<namespace>.svc:8080` for the full
   snapshot before a scale-out or hibernation and for compaction
   ([ondemandsnapshot.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/etcdopstask/handler/ondemandsnapshot/ondemandsnapshot.go#L103),
   [snapshot.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/compaction/snapshot.go#L122));
   the wrapper calls it as `<name>-local`, a host alias of `127.0.0.1`
   ([builder.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/builder.go#L211-L217)).
   With only `-local` in the certificate the scale-out stalled for over ten
   minutes, until the certificate was fixed.
3. **A Secret changed in place is not picked up.** The sidecar kept serving
   its old certificate for the three minutes it was watched after the update,
   and etcd-druid did not roll the pod: its only pod checksum is the
   ConfigMap's
   ([constants.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/common/constants.go#L11)).
   Pointing the `Etcd` at a new Secret name rolls the members (Results). The
   PKI controller renews etcd's certificates under new names, a
   single-member tenant in its maintenance window.
4. **A blocked operation shows only on its `EtcdOpsTask`.** While the
   pre-scale snapshot failed (finding 2), the `Etcd` said
   `ScaleOperationComplete=False ScalingOut` with no `lastErrors`; the error
   was on the `EtcdOpsTask` `presync-snapshot-update-3-0` and in etcd-druid's
   log. The Datastore controller reads `EtcdOpsTask`s into its condition.
5. **A restore keeps the snapshot's revisions, so the API server restarts
   after it.** The single member came back at revision 1417, the last
   delta's, after clients had seen 1440; the 19 writes acknowledged in the
   21 s after that delta were lost. Until the API server restarted, its
   watch cache listed those 19 ConfigMaps, which a GET answered with
   NotFound, and only 1 of the 17 written after the restore, whose revisions
   1418–1440 reused numbers clients had already seen. A rollout of the API
   server fixed both. The Datastore controller rolls it after every restore,
   until the sidecar can bump revisions
   ([recovery](https://etcd.io/docs/v3.6/op-guide/recovery/)).
6. **Deleting an `Etcd` keeps its volumes and snapshots.** The StatefulSet
   retains its claims (`persistentVolumeClaimRetentionPolicy: Retain`, no
   owner references) and nothing removes the prefix in the bucket; only
   scale-in deletes the removed members' claims
   ([scale-in proposal](https://github.com/gardener/etcd-druid/blob/v0.39.0/docs/proposals/08-scale-in.md)).
   `reclaimPolicy: Delete` and `retention.afterDelete` of a `DatastoreClass`
   are Lodia's code.
7. **Scheduled defragmentation skips small databases.** The sidecar
   defragments only above 3 GiB or with more than 1 GiB free
   ([constant](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/etcdutil/etcdutil.go#L34-L35),
   [check](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/etcdutil/etcdutil.go#L460)),
   so under the 4 GiB quota Lodia ships a scheduled run happens near the
   quota or after a large delete.
8. **The chart installs with plain helm from the tag but is not released.**
   At v0.39.0 `charts/` already holds the CRDs (the copy step in
   [prepare-helm-charts](https://github.com/gardener/etcd-druid/blob/v0.39.0/docs/deployment/prepare-helm-charts.md)
   is for the development tree), and the webhook's PKI is needed only with
   the webhook on. The chart's version is `0.1.0` and it is published
   nowhere, so Lodia vendors it at a tag.
9. **etcd-druid runs etcd 3.5.27.** The `UpgradeEtcdVersion` gate, beta and
   on since v0.39, selects etcd-wrapper v0.8.2 and etcd-backup-restore
   v0.45.1, both on etcd 3.5.27
   ([images.yaml](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/images/images.yaml),
   [feature gates](https://github.com/gardener/etcd-druid/blob/v0.39.0/docs/deployment/feature-gates.md)).
   kubeadm defaults to 3.6.6 for Kubernetes 1.35 and 3.7.0 for 1.37 and
   accepts an external etcd from 3.5.24
   ([1.35](https://github.com/kubernetes/kubernetes/blob/v1.35.0/cmd/kubeadm/app/constants/constants.go#L326-L329),
   [1.37](https://github.com/kubernetes/kubernetes/blob/v1.37.0/cmd/kubeadm/app/constants/constants.go#L326-L330));
   etcd's latest are 3.5.35, 3.6.16 and 3.7.3, from 2026-10-22
   ([releases](https://github.com/etcd-io/etcd/releases)).
   `spec.etcd.image` and `spec.backup.image` let Lodia's version table pin
   newer builds.
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

What each field of the `DatastoreClass` becomes in the `Etcd` is the table in
[`README.md`](README.md#a-tenant-namespace), where the Datastore controller's
tests check it.

## What assumes Gardener

| what | where | what Lodia does |
|---|---|---|
| every line TLS or none | finding 1 | sets all three TLS blocks |
| certificates renewed under new names: Gardener's secrets manager writes immutable Secrets named after a checksum of their config ([manager.go](https://github.com/gardener/gardener/blob/2a9e7795749d1122a171caf69aabd6426db10777/pkg/utils/secrets/manager/manager.go#L405), [generate.go](https://github.com/gardener/gardener/blob/ac0343dcd77a3c14635bb9006f276ffc2cc30906/pkg/utils/secrets/manager/generate.go#L343)) | finding 3 | renews the same way |
| NetworkPolicies come from the consumer | [etcd-druid#505](https://github.com/gardener/etcd-druid/issues/505) | the Datastore controller's `<cp>-etcd` ([README](README.md#a-tenant-namespace)) |
| compaction Jobs carry Gardener's network-policy labels (`networking.gardener.cloud/to-dns`, `to-private-networks`, `to-public-networks`) | [reconciler.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/compaction/reconciler.go#L720-L722) | lets compaction Jobs reach DNS and the store |
| images in Gardener's Artifact Registry | [images.yaml](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/images/images.yaml) | pins them in the version table, under the registry mirror |
| deployed through skaffold | finding 8 | vendors the chart at a tag |
| `EtcdCopyBackupsTask`, for Gardener's control plane migration between seeds | `charts/crds` | leaves it unused |
| the `etcdComponentProtection` webhook, exempting Gardener's service accounts | the chart's values | keeps it off, the chart's default |

## Open questions

- How the Datastore controller learns that a restore happened, to roll the
  API server (finding 5): the sidecar logs it; no condition of the `Etcd`
  said so.
- In which order the sidecars defragment three members (finding 7 kept them
  from running here).
- Restore, scale and snapshot times at gigabytes, and etcd-druid at a few
  hundred `Etcd` objects — phase 4.

## How to run it again

From the repository root, on a laptop with Docker (about 4 GB for the
spike), helm and the hard way's tools; the hard way may run beside it on its
own cluster and port. Every command reaches the spike's cluster through `mk`
and `tk`, never your current context.

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

bash "$ES/pki.sh" etcd-druid-backup                 # a certificate renewed under a new Secret name
mk -n "$CP" create secret tls "$CP-etcd-backup-restore-2" --cert="$PKI/etcd-druid-backup.crt" --key="$PKI/etcd-druid-backup.key"
mk -n "$CP" patch etcd "$CP-etcd" --type json \
  -p '[{"op":"replace","path":"/spec/backup/tls/serverTLSSecretRef/name","value":"'"$CP"'-etcd-backup-restore-2"}]'

mk top pods -A --containers
```

Clean up with `kill %1`, `go tool kind delete cluster --name "$MGMT"
--kubeconfig "$WORK/management.kubeconfig"` and `rm -rf "$WORK"`.
