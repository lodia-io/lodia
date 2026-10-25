# Operations

How Brygga is run once installed: everyday tasks for a tenant, what breaks
and how it shows, the runbooks, the alerts, capacity, upgrades of Brygga
itself and the support policy. Installation is in
[`README.md`](README.md#installation).

## Everyday tasks

| task | how |
|---|---|
| Get access as a person | create a `KubeconfigRequest` ([example](api/examples/kubeconfigrequest.yaml)); read the Secret named in its `status.secretRef`; it expires on its own |
| Get access for automation | read the Secret `<cp>-admin-kubeconfig`; revoke every copy by deleting the ClusterRoleBinding `brygga:cluster-admins` in the tenant (Brygga recreates it on the next apply only if the `ControlPlane` still asks for it) |
| Join a node | with an admin kubeconfig: `kubeadm token create --print-join-command --kubeconfig admin.kubeconfig`; run the printed `kubeadm join …` on the machine; it needs outbound 443 to the endpoint and nothing inbound |
| Upgrade Kubernetes | set `spec.version`; watch `status.kubernetes.upgrade` and `Progressing`; Brygga goes one minor at a time; nodes follow at the owner's pace |
| Get patch updates automatically | `spec.maintenance.autoUpdate.kubernetesPatch: true` and a `maintenance.window` |
| Rotate client identities (yearly, or after a leaked kubeconfig) | `kubectl annotate controlplane <cp> brygga.segla.io/operation=rotate-client-ca-start`; it completes on its own after one kubelet signing duration (30 days), nodes untouched |
| Rotate the ServiceAccount key | `…=rotate-serviceaccount-key-start`; when `status.credentials.rotation.serviceAccountKey.phase` is `Prepared` and no client holds an old token, `…=rotate-serviceaccount-key-complete` |
| Rotate the etcd encryption key, the etcd CA, the front-proxy CA | `…=rotate-etcd-encryption-key-start`, `…=rotate-etcd-ca-start`, `…=rotate-front-proxy-ca-start`; each completes on its own |
| Rotate the trust anchor (the cluster CA) | `…=rotate-ca-start`; when the phase is `Prepared`, add the new bundle (`<cp>-ca-bundle`) to every node's `/etc/kubernetes/pki/ca.crt` and kubelet kubeconfig, then `…=rotate-ca-complete` |
| Hibernate and wake | `spec.hibernation.enabled: true` / `false`, or `spec.hibernation.schedules` |
| Take a snapshot now | create a `Backup` ([example](api/examples/backup.yaml)); its `status.location` is the object in the bucket |
| Clone or recover a cluster | create a new `ControlPlane` with `spec.restoreFrom.backupRef`; nodes of the old cluster can join it with a new token |
| Stop Brygga touching a cluster | `spec.paused: true`; everything keeps running, nothing is reconciled |
| Turn the controller manager hold off for a cluster | `spec.health.holdControllerManager: false`; a hold in progress is released at once |
| Delete a cluster | delete the `ControlPlane`; the finalizer takes a last snapshot when a backup store exists; volumes and snapshots follow the class's reclaim and retention |

## What breaks, and how it shows

| symptom | look at |
|---|---|
| `Ready=False`, reason `WaitingForDatastore` | `DatastoreReady` reason; the etcd resource's status; the etcd pods' logs (`etcdserver: request timed out`, `apply request took too long` mean slow disks) |
| `DatastoreReady=False`, `QuorumLost` | fewer than two of three members up; the [lost quorum](#lost-etcd-quorum) runbook |
| API calls fail with `etcdserver: mvcc: database space exceeded` | the [space quota](#etcd-space-quota) runbook; `status.datastore.dbSizeBytes` against the class quota |
| `APIServerReady=False`, `RollingOut` for long | the kube-apiserver Deployment's events; a new replica failing `/readyz` (often a flag in `extraArgs` that the new minor removed) |
| `CAValidityAcceptable=False`, `CAExpiring` | a CA expires within a year; the message names it and when; start a [CA rotation](#forced-ca-rotation). `Ready` stays True: every certificate is still valid |
| `CertificatesReady=False`, `CAExpired` | a CA is past its `NotAfter`, and every certificate it signed with it; nothing is issued under it until a [CA rotation](#forced-ca-rotation) |
| `CertificatesReady=False`, `CAMissing` | a CA's key Secret `<cp>-cakey-<ca>` is missing or unreadable while its bundle exists; restore it from a backup (it is found by name, labels or not) or rotate the CA; PKI never replaces it on its own |
| `AccessReady=False`, `Binding` | a ClusterRoleBinding of Brygga, the admin group or the kubelet client is missing in the tenant; PKI recreates it with a ten-minute `system:masters` certificate on its next pass |
| `Reachable=False`, `NodesSilent`; in the tenant, the Lease `kube-system/kube-controller-manager` held by `brygga` | most nodes cannot reach the API, so Health holds the controller manager; the [nodes cut off](#nodes-cut-off-from-the-control-plane) runbook |
| `Reachable=False`, `HoldBlocked` | nodes are silent but another component holds the controller manager's lease (`status.health.controllerManagerHold.blockedBy`), for example a platform's own watchdog; run one of the two, not both |
| `Reachable=False`, `ExternalProbeFailed`, nodes fine | the public endpoint from the management cluster's side: Gateway listener, route status, certificate SANs |
| `AddonsAvailable=False`, `RolloutPending` | the DaemonSet or Deployment inside the tenant; a node that cannot pull images; a CNI missing (pods of CoreDNS stay Pending until a CNI is installed) |
| `kubectl logs` / `exec` hang, everything else works | the konnectivity path: agents connected to every server (`konnectivity_network_proxy_server_ready_backend_connections`), the agent's DaemonSet, outbound 443 for the konnectivity hostname |
| `Progressing=False`, `WaitingForWindow` for long | expected outside the window; `status.target.scheduledFor` says when |
| `Progressing=False`, `FleetLimit` | other tenants are rolling; the operator's `fleet.maxConcurrentRollouts` |
| `Progressing=False`, `Failed` | `status.kubernetes.upgrade.step` and `message`; the [stuck upgrade](#stuck-upgrade) runbook |
| Nothing changes after editing `spec` | `metadata.generation` against `status.observedGeneration`; `spec.paused`; the operator's leader and its logs |

## Runbooks

### Lost etcd quorum

1. Confirm: `DatastoreReady=False`, reason `QuorumLost`; fewer than two
   members `Ready`.
2. If members are down because of their node: bring the node back, or delete
   the pod so the StatefulSet reschedules it with its volume; quorum returns
   when two members agree. A single member that lost its data is restored by
   the backup sidecar on its own; a lost member of three is re-added as a
   learner by etcd-druid.
3. If quorum is lost for good (two of three volumes gone), follow etcd-druid's
   manual procedure: suspend its reconciliation with the annotations it
   documents, scale the StatefulSet to 0, delete the PVCs and member leases,
   set the cluster to one member, scale to 1 so it restores from the newest
   snapshots, re-enable reconciliation, and let Brygga scale back to 3
   ([recovering etcd clusters](https://gardener.github.io/etcd-druid/usage/recovering-etcd-clusters.html)).
   Alternatively, create a new `ControlPlane` with `restoreFrom` the latest
   `Backup` and move the nodes (new token, `kubeadm join`).
4. Afterwards: the API server rolls once (Datastore detects the restore);
   check `status.datastore.lastBackup` is moving again.

### etcd space quota

1. Confirm the `NOSPACE` alarm in the member's logs or metrics
   (`etcd_server_quota_backend_bytes` against `etcd_mvcc_db_total_size_in_bytes`).
2. Find what grew: usually Events or a controller writing large objects
   inside the tenant; `kubectl get --raw /metrics | grep apiserver_storage_objects`
   through the admin kubeconfig.
3. Raise the class quota temporarily (`quotaBackendBytes`) or delete the
   offending objects; then compaction and defragmentation run, and the alarm
   is disarmed with `etcdctl alarm disarm`.

### Restore a tenant from backup

The supported path is a new `ControlPlane` with `spec.restoreFrom`: its
etcd starts from the snapshot, the API server comes up on the restored data,
and nodes join it fresh.

Restoring in place under a running cluster follows the Kubernetes order:
stop every API server, restore every etcd member from the same snapshot,
start the API servers, then restart the controller manager and scheduler
([operating etcd](https://kubernetes.io/docs/tasks/administer-cluster/configure-upgrade-etcd/)).
With Brygga: set `spec.paused: true`, scale the API server Deployment to 0,
restore with `etcdutl snapshot restore --bump-revision 1000000000
--mark-compacted` so revisions keep moving forward and every watch ends and
relists ([recovery](https://etcd.io/docs/v3.6/op-guide/recovery/)), then
unpause; Brygga rolls the API server, the controller manager and the
scheduler.

### Stuck upgrade

1. `status.kubernetes.upgrade` says the step and the message.
2. API server step failing: a removed flag or feature gate in `extraArgs` or
   `featureGates` is the usual cause; fix `spec`, the step retries.
3. In-tenant step failing: an addon image not pullable from the tenant's
   nodes (air gap, registry mirror missing).
4. Rollback of a minor: create a new `ControlPlane` with `restoreFrom` the
   `pre-upgrade-<version>` backup and the old `spec.version`; a downgrade in
   place is not supported by Kubernetes.

### Forced CA rotation

- **A kubeconfig or client certificate leaked:** rotate the client CA and
  complete it at once (`rotate-client-ca-start`, then
  `rotate-client-ca-complete`); every client certificate Brygga holds is
  already re-issued, and kubelets whose certificates are still under the old
  CA re-bootstrap with a new token (`kubeadm token create`, then on the node
  `kubeadm reset` and `kubeadm join`), or wait for the automatic completion
  after 30 days if the leak allows it.
- **The cluster CA key leaked or the CA expires:** rotate the trust anchor.
  Start it, add the new bundle to every node (`/etc/kubernetes/pki/ca.crt`
  and the certificate authority in `/etc/kubernetes/kubelet.conf`), complete
  it. Between start and complete both CAs are trusted; completing before the
  nodes have the bundle cuts off every node that trusts only the old CA.

### Nodes cut off from the control plane

1. Confirm: `Reachable=False`, reason `NodesSilent`;
   `status.health.controllerManagerHold` says since when and how many nodes
   are silent. Workloads on the nodes keep running; the tenant's controllers do
   not (no new ReplicaSets, no node IPAM) until the hold ends.
2. Find the broken path: the Gateway listener and routes, DNS of the API
   hostname, the network between the nodes and the Gateway. Kubelets log
   their failed lease renewals.
3. When the nodes reach the API again, Health releases the lease two minutes
   later and the controller manager starts afresh; no node was tainted, no pod
   evicted.
4. If the nodes really are gone (a site lost for good), turn the hold off
   with `spec.health.holdControllerManager: false`, so the controller manager
   can mark them and move their pods to the nodes that remain
   ([controller-manager-hold](decisions/controller-manager-hold.md)).

### Operator down

Tenants keep running: their pods, their certificates (valid for a year) and
their etcds do not depend on Brygga being up. Changes to `ControlPlane`
objects are accepted by the API (no webhooks) and applied when Brygga is
back. Renewal and backups pause; the `BryggaOperatorDown` alert fires first.

## Alerts shipped in the chart

| alert | fires when | means |
|---|---|---|
| `BryggaOperatorDown` | no ready operator replica for 5 m | nothing is reconciled; tenants keep running |
| `BryggaControlPlaneNotReady` | `Ready=False` for 10 m, not hibernated | a tenant's API or a part of it is down; the reason names which |
| `BryggaCertificateExpiring` | any certificate within 14 days of expiry | renewal failed; look at `CertificatesReady` |
| `BryggaBackupStale` | the newest snapshot older than two periods | the backup sidecar or the bucket credentials |
| `BryggaEtcdSpaceNearQuota` | the database above 80% of the quota | compaction is not keeping up, or the tenant writes too much |
| `BryggaEtcdNoLeader` | a dedicated etcd without a leader for 1 m | quorum or network inside the management cluster |
| `BryggaControllerManagerHeld` | the hold active for 15 m | most nodes cannot reach the control plane; the tenant's workloads run, its controllers wait |
| `BryggaUpgradeFailed` | `Progressing=False`, reason `Failed` | the stuck upgrade runbook |
| `BryggaReconcileErrors` | a controller's error rate above 5% for 15 m | the operator logs, by controller |

Every alert links to its runbook section here.

## Capacity

Brygga's targets, to be replaced by measurements in phase 1
([roadmap](roadmap.md)): a single idle control plane within 1 GiB of memory
requests and 0.5 CPU, a highly available one within 2.5 GiB and 1.5 CPU,
etcd included. No primary source publishes the idle footprint of a small
etcd member; Gardener's requests for a shoot's etcd (30m / 150M, sidecar
10m / 40Mi) are the starting point, and etcd 3.6 roughly halved memory under
load compared with 3.5
([Gardener's etcd component](https://raw.githubusercontent.com/gardener/gardener/master/pkg/component/etcd/etcd/etcd.go),
[etcd 3.6](https://etcd.io/blog/2025/announcing-etcd-3.6/)). A management cluster sized for N tenants needs the sum of
their requests on its control plane nodes, plus room to roll one
Deployment of every tenant (the fleet limit bounds how many roll at once),
plus etcd volumes with low fsync latency.

The operator: two replicas; memory grows with the number of tenants it
caches (label-scoped, metadata-only for Secrets); the phase 4 gate is 200
tenants under 2 GiB.

## Upgrading Brygga

`helm upgrade`. The chart upgrades the CRDs first (served versions are only
added, never removed, within a major), then the operator. New pod templates
roll into tenants in their maintenance windows, N at a time; the release
notes list what changes per component. Rolling back the chart is safe while
the CRDs did not add a stored version.

## Support policy

| what | supported |
|---|---|
| Tenant Kubernetes versions | the minors the Kubernetes project maintains, today 1.35, 1.36 and 1.37; a minor leaves Brygga's version table one release after it leaves upstream support |
| Management cluster | a maintained minor with the CEL features the CRDs use: 1.35 or newer today |
| Brygga versions | until 1.0, the latest minor gets fixes; from 1.0, the two latest minors |
| API versions | `v1alpha1` may change between minors until `v1beta1`; from `v1beta1`, changes are additive and conversion needs no webhook |
