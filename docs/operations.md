# Operations

How Lodia is run once installed.
Installation is in [`README.md`](README.md#installation).

## Everyday tasks

| task | how |
|---|---|
| Get access as a person | create a `KubeconfigRequest` for a key of your own ([steps](#access-as-a-person)); its `status.kubeconfig` works with that key until `status.expirationTimestamp` |
| Get access for automation | grant `get` on the Secret `<cp>-admin-kubeconfig` by `resourceNames`, never on the namespace's Secrets, which hold the class's backup credentials in `<cp>-etcd-backup`; read it; every copy is revoked by the client CA rotation (phase 3), not by deleting the ClusterRoleBinding `lodia:cluster-admins` in the tenant, which PKI applies again on its next pass |
| Join a node | with an admin kubeconfig: `kubeadm token create --print-join-command --kubeconfig admin.kubeconfig`; run the printed `kubeadm join …` on the machine; it needs outbound 443 to the endpoint and nothing inbound |
| Upgrade Kubernetes | set `spec.version`; watch `status.kubernetes.upgrade` and `Progressing`; nodes follow at the owner's pace, at most three minors behind; Lodia goes one minor at a time, through a preflight and a `Backup` ([upgrades-and-maintenance-windows](decisions/upgrades-and-maintenance-windows.md)) |
| Get patch updates automatically | `spec.maintenance.autoUpdate.kubernetesPatch: true` and a `maintenance.window` ([the field](api/controlplane.md#spec)) |
| Roll pending maintenance now | `kubectl annotate controlplane <cp> lodia.io/maintain-requested-at="$(date -u +%FT%TZ)" --overwrite`; `status.maintenance.lastHandled` shows the value handled ([the annotation](api/controlplane.md#spec)) |
| Rotate client identities (yearly, or after a leaked kubeconfig) | phase 3, not built: `kubectl annotate controlplane <cp> lodia.io/operation=rotate-client-ca-start`; it completes on its own after one kubelet signing duration (30 days), nodes untouched |
| Rotate the ServiceAccount key | phase 3, not built: `…=rotate-serviceaccount-key-start`; when `status.credentials.rotation.serviceAccountKey.phase` is `Prepared` and no client holds an old token, `…=rotate-serviceaccount-key-complete` |
| Rotate the etcd encryption key, the etcd CA, the front-proxy CA, the tunnel CA in `gateway` mode | phase 3, not built: `…=rotate-etcd-encryption-key-start`, `…=rotate-etcd-ca-start`, `…=rotate-front-proxy-ca-start`, `…=rotate-tunnel-ca-start`; each completes on its own |
| Rotate the trust anchor (the cluster CA) | phase 3, not built: `…=rotate-ca-start`; when the phase is `Prepared`, add the new bundle (`<cp>-ca-bundle`) to every node's `/etc/kubernetes/pki/ca.crt` and kubelet kubeconfig, then `…=rotate-ca-complete` |
| Hibernate and wake | phase 3, not built: `spec.hibernation.enabled: true` / `false`, or `spec.hibernation.schedules` |
| Take a sealed copy | create a `Backup` ([example](api/examples/backup.yaml)); it is `Ready`, `Completed`, once sealed, and `status.snapshots` says where the copy is |
| Replace a cluster from a `Backup` | create a new `ControlPlane` with `spec.restoreFrom.backupRef`; go through the [replace checklist](#replace-a-cluster-from-a-backup), or create the new one `Released` if the source is gone already; nodes join it anew with a new token ([restore-replaces-the-source](decisions/restore-replaces-the-source.md)) |
| Clone a cluster beside its source | through the API with Velero ([recipe](#clone-a-cluster-beside-its-source)), not with `restoreFrom`, which replaces its source |
| Stop Lodia touching a cluster | `spec.paused: true`; everything keeps running, nothing is reconciled but `KubeconfigRequest`s, which write nothing to the `ControlPlane`; deleting a paused `ControlPlane` waits until it is unpaused; in `gateway` mode [the tunnel's client certificate limits how long](#operator-down) |
| Turn the controller manager hold off for a cluster | `spec.health.holdControllerManager: false`; a hold in progress is released at once |
| Change a `DatastoreClass` | edit it; [what the class refuses and what an edit rolls](README.md#a-tenant-namespace); the edit reaches each tenant's `Etcd` in its window, within the fleet limit, or at once on `lodia.io/maintain-requested-at`, and `Progressing` says when; the storage fields, the reclaim policy, `retention.afterDelete`, the credentials' `secretRef` and the region roll nothing ([dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md#consequences)) |
| Rotate a backup store's credentials | create a new Secret with the new keys and point the class's `backup.store.secretRef` at it: the new keys reach every tenant on the class at once, and the class edit, which rolls nothing, reaches each `Etcd` in its window; the keys that name the store ([which](api/controlplane.md#backup)) stay as they were, since a `Backup` taken meanwhile fails on a change of them; an application credential of another Swift project, or keys of another tenant of the store, are a store move that no `Backup` sees ([the two cases](api/controlplane.md#backup)), so make that change while no `Backup` is in progress, and start the next one only once that tenant's sidecar has uploaded with the new keys, as below, since a `Backup`'s full snapshot then goes to the store its copy reads; revoke the old keys only once every tenant's sidecar has uploaded with the new ones, since the kubelet updates its files some time after `<cp>-etcd-backup` changes ([secrets](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/concepts/configuration/secret.md#L549-L553)): in the `backup-restore` container of a `<cp>-etcd` pod, `Updated the snapstore object with new credentials` is followed by `Successfully saved delta snapshot` or `Successfully saved full snapshot` ([snapshotter.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/snapshot/snapshotter/snapshotter.go#L313-L326), [the copy](README.md#a-tenant-namespace)) |
| Keep tenants off each other's hostnames on a shared Gateway | bind each `ControlPlane`'s hostnames to its namespace with [a policy](#hostnames-on-a-shared-gateway) |
| Delete a cluster | delete the `ControlPlane` ([what deletion does](README.md#deletion)); `kubectl get pvc -L lodia.io/control-plane-uid` shows whose each claim is; a lifecycle rule of the bucket expires old prefixes, but never `<prefix>/<namespace UID>/backups/<Backup UID>` while that `Backup` exists: a restore of an emptied copy fails its seal check |

### Hostnames on a shared Gateway

Only Lodia writes `TLSRoute`s in the namespaces a tenant listener admits: another route there can take a tenant's hostname, and on Envoy Gateway two routes with one hostname stop the shared listener taking any tenant's change ([api-exposure](decisions/api-exposure.md)).
A hostname belongs to the `ControlPlane` that named it first on its Gateway, so a stranger's `ControlPlane` that names a running tenant's hostname gets no route, but takes the name once that tenant is deleted or recreated, and holds the replacement at `HostnameConflict`.
Where namespaces have owners who do not trust each other, bind every hostname to `*.<namespace>.<zone>` with this policy, the zone in its ConfigMap:

```yaml
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: controlplane-hostnames
spec:
  failurePolicy: Fail
  paramKind:
    apiVersion: v1
    kind: ConfigMap
  matchConstraints:
    resourceRules:
    - apiGroups: [lodia.io]
      apiVersions: [v1alpha1]
      operations: [CREATE]
      resources: [controlplanes]
  variables:
  - name: hostnames
    expression: >-
      has(object.spec.endpoint.gateway) ? [
        object.spec.endpoint.gateway.hostnames.apiServer,
        object.spec.endpoint.gateway.hostnames.konnectivity,
        object.spec.endpoint.gateway.hostnames.tunnel] : []
  validations:
  - expression: "variables.hostnames.all(h, h.endsWith('.' + object.metadata.namespace + '.' + params.data.zone))"
    messageExpression: "'every hostname of endpoint.gateway must end in .' + object.metadata.namespace + '.' + params.data.zone"
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: controlplane-hostnames
spec:
  policyName: controlplane-hostnames
  validationActions: [Deny]
  paramRef:
    name: controlplane-hostnames
    namespace: lodia-system
    parameterNotFoundAction: Deny
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: controlplane-hostnames
  namespace: lodia-system
data:
  zone: k8s.example.com
```

It checks creation alone: `endpoint` is immutable, and a refused update would also refuse the removal of a finalizer.

### Access as a person

The key stays on your machine; the request carries only its public half.
For `prod-eu` in `team-a`, as cluster admin for an hour:

```sh
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out prod-eu.key
openssl req -new -key prod-eu.key -subj /CN=prod-eu -out prod-eu.csr
user=$(kubectl auth whoami -o jsonpath='{.status.userInfo.username}')
request=$(kubectl create -n team-a -o name -f - <<EOF
apiVersion: lodia.io/v1alpha1
kind: KubeconfigRequest
metadata: {generateName: prod-eu-}
spec:
  controlPlaneRef: {name: prod-eu}
  user: "$user"
  access: ClusterAdmin
  request: $(base64 < prod-eu.csr | tr -d '\n')
EOF
)
kubectl wait -n team-a "$request" --for=condition=Ready --timeout=1m
kubectl get -n team-a "$request" -o jsonpath='{.status.kubeconfig}' | base64 -d > prod-eu.kubeconfig
kubectl config set-credentials "lodia:user:$user" --client-key=prod-eu.key --embed-certs --kubeconfig=prod-eu.kubeconfig
kubectl get nodes --kubeconfig=prod-eu.kubeconfig
```

Without `access` the certificate carries no group, and the tenant's RBAC on `lodia:user:<user>` decides.
`ClusterAdmin` needs `create` on `controlplanes/adminkubeconfig` for that `ControlPlane`; a request that is not `Ready` says why in its condition ([reasons](api/controlplane.md#kubeconfigrequest)).
The request is deleted at its deadline; deleting it earlier revokes nothing, the certificate works until `status.expirationTimestamp`.

## The platform under the gateway mode

What `endpoint.gateway` asks of the platform, beyond the [prerequisites](README.md#installation).

### Gateway implementations

The core of Gateway API's `GATEWAY-TLS` conformance profile is `TLSRoute` in passthrough ([profiles.go](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/conformance/utils/suite/profiles.go#L99-L110), [the test's listener](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/conformance/tests/tlsroute-simple-same-namespace.yaml#L34-L35)), which is all Lodia uses.
These reports, kept in the Gateway API repository at v1.6.3, each the newest of its implementation, pass that core:

| implementation | report (Gateway API version) |
|---|---|
| agentgateway | [v1.1.0](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/conformance/reports/v1.5/agentgateway-agentgateway/v1.1.0-report.yaml) (v1.5.1) |
| Cilium | [v1.20.0-pre.2](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/conformance/reports/v1.5/cilium/experimental-v1.20.0-pre.2-default-report.yaml) (v1.5.1) |
| Envoy Gateway | [v1.8.0](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/conformance/reports/v1.5/envoy-gateway/experimental-v1.8.0-default-report.yaml) (v1.5.1) |
| Istio | [1.28](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/conformance/reports/v1.4.0/istio-istio/1.28.0-default-report.yaml) (v1.4.0) |
| kgateway | [v2.3.0-beta.3](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/conformance/reports/v1.5/kgateway/v2.3.0-beta.3-report.yaml) (v1.5.1) |
| NGINX Gateway Fabric | [v2.6.0](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/conformance/reports/v1.5/nginx-nginx-gateway-fabric/experimental-2.6.0-default-report.yaml) (v1.5.1) |
| Traefik | [v3.7](https://github.com/kubernetes-sigs/gateway-api/blob/v1.6.3/conformance/reports/v1.5/traefik-traefik/experimental-v3.7.0-default-report.yaml) (v1.5.1) |

Lodia's gates run on Envoy Gateway v1.9.3 ([the gate](../test/e2e/README.md#the-gateway-gate)).

### What a tenant's cluster must allow

- **Pod Security:** `kube-system` has to admit `apiserver-proxy`, whose `hostNetwork` and `NET_ADMIN` the Baseline level forbids ([pod-security-standards](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/concepts/security/pod-security-standards.md#baseline)), so a namespace label enforcing `baseline` or `restricted` there stops in-cluster API access.
- **NetworkPolicy:** a tenant's egress policies must allow pods the advertise address and 6443, `240.0.0.1:6443` by default, which is what connections to the `kubernetes` Service resolve to ([api-exposure](decisions/api-exposure.md)).
- **Windows and virtual-kubelet nodes:** they run no `apiserver-proxy`, so a pod there gets no in-cluster API path through `kubernetes.default`.
- **HTTP egress proxies:** `apiserver-proxy` dials the tunnel directly, so a node that reaches the Gateway only through an HTTP proxy gets no in-cluster API path; support for that is undecided.

### Audit

Requests through the tunnel reach the API server from `127.0.0.1`, since the sidecar dials it on loopback, and external requests from the Gateway's address, so audit `sourceIPs` and the request log carry no client address ([request.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/audit/request.go#L55-L59)).
In gateway mode requests are attributed by identity: a bound ServiceAccount token carries its pod and node into the user's extra fields, which the audit event records ([util.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/authentication/serviceaccount/util.go#L40-L47)).
Where audit must rest on the source address, use `loadBalancer` mode.

### Platform matrix

The in-cluster path, `kubernetes` to `240.0.0.1:6443` through `apiserver-proxy` and the tunnel:

| platform | how it was checked | result |
|---|---|---|
| kindnet and kube-proxy, Envoy Gateway v1.9.3, Kubernetes v1.37.0 | `TestGatewayGate`, every pull request ([the gate](../test/e2e/README.md#the-gateway-gate)) | passes |
| Cilium v1.20.4, `kubeProxyReplacement=true`, tunnel routing, no kube-proxy (`addons.kubeProxy.enabled: false`), Kubernetes v1.37.0, kernel 6.12.54-linuxkit | once by hand on kind, repeated by no CI job: `helm upgrade --install cilium oci://quay.io/cilium/charts/cilium --version 1.20.4 --set kubeProxyReplacement=true,k8sServiceHost=<api host>,k8sServicePort=443,dnsPolicy=Default,ipam.mode=kubernetes,operator.replicas=1`; `dnsPolicy=Default` lets the agent resolve the API hostname before CoreDNS runs | socket load balancing on, `cilium-dbg service list` holds `10.32.0.1:443 => 240.0.0.1:6443` (`10.32.0.1` the tenant's `kubernetes` ClusterIP), and a pod's loop to `https://kubernetes.default` answered 600 new-connection and 600 reused-connection requests in a minute, none failed |

## What breaks, and how it shows

| symptom | look at |
|---|---|
| `DatastoreReady=False`, `DatastoreClassNotFound` | before the `Etcd` exists: the `DatastoreClass` that `spec.datastore.dedicated.className` names, or that the claims of a previous `Etcd` record, is missing, or no class is annotated `lodia.io/is-default-class: "true"`; the message says which. Deleting a class that running `Etcd`s record never shows here ([dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md)) |
| `DatastoreReady=False`, `DatastoreClassAmbiguous` | before the `Etcd` exists, without `className`: more than one class is annotated `lodia.io/is-default-class: "true"`; the message names them. Remove the annotation from all but one |
| `DatastoreReady=False`, `VolumesExist` | claims `<cp>-etcd-<cp>-etcd-<n>` of a previous `ControlPlane` of that name, without this one's `lodia.io/control-plane-uid` label, or an `Etcd` `<cp>-etcd` that another `ControlPlane` or nobody controls; the message names them. To start empty, delete them (an `Etcd` with its claims); to keep the data, keep them, and this `ControlPlane` waits. A new `ControlPlane` takes the old cluster's data only from a `Backup` of it, through `restoreFrom`, and waits here too until the claims are deleted |
| `DatastoreReady=False`, `ObjectsControlledElsewhere` | a NetworkPolicy `<cp>-etcd` that another owner keeps, named in the message: a previous `ControlPlane`'s goes with the garbage collector; anyone else's needs deleting, or its controller reference removing, and Lodia then applies its own |
| `DatastoreReady=False`, `Refused` | the API server refuses the `Etcd` or the NetworkPolicy `<cp>-etcd` Lodia renders; the refusal is in Lodia's log (`controller="datastore"`). An admission policy on `Etcd`s: let it admit Lodia's. An etcd-druid whose CRD moved: the version Lodia pins. A `DatastoreClass` stored before a rule one of its values breaks: validation ratcheting keeps an unchanged field on update ([ratcheting](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#validation-ratcheting)), so a value the `Etcd` API refuses shows here, while one only the backup sidecar refuses (a day of week 7, a descending range, a zero quota) is accepted and fails the member at its next roll. After an upgrade of Lodia, check each class by creating a copy under another name with `--dry-run=server`: a create is validated whole, ratcheting being for updates, and a dry run goes through validation and admission without being stored ([dry run](https://kubernetes.io/docs/reference/using-api/api-concepts/#dry-run)); fix what it refuses |
| `DatastoreReady=Unknown`, `Reconciling`, or a `DatastoreReady` whose `observedGeneration` lags the `ControlPlane`'s generation | the Datastore controller's passes end in an error of the API server; the error is in Lodia's log (`controller="datastore"`), never in the condition |
| `DatastoreReady`'s message notes `DatastoreClass <c> generation <g> waits for status.target.datastoreClass` | an edit of the class, or a class deleted and created again under its name, which `<g>` then names by its UID, waits for Lifecycle's grant in the tenant's maintenance window, which `Progressing` names; nothing is broken |
| `DatastoreReady=True`, `SecretNotFound`, `ObjectsControlledElsewhere` or `Refused`, and its message notes `DatastoreClass <c> is held:` | the class's credentials copy cannot be made, and the `Etcd` keeps the class fields it has while it still takes new certificate generations: restore the class's Secret the message names (`SecretNotFound`); or wait while the garbage collector removes a predecessor's `<cp>-etcd-backup`, which another owner controls, and delete it if its owner is something else (`ObjectsControlledElsewhere`); or read Lodia's log when the API server refuses the copy (`Refused`). Meanwhile Lifecycle grants no change in the tenant's window, and one in its slot is `MaintenanceFailed`. A class edit made meanwhile applies once the copy can be made and Lifecycle grants it |
| `DatastoreReady`'s message notes `DatastoreClass <c>'s quota <q> is capped at` | the class's quota needs more than half the `Etcd`'s volume, which never grows, so the `Etcd`'s quota is half its volume ([dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md)) and the rest of the class edit applies. Nothing is broken: keep it, or lower the class's `quotaBackendBytes` to at most half the volume the message names |
| `DatastoreReady`'s message notes `highAvailability waits for Etcd <cp>-etcd to settle` | added `highAvailability` waits while a member of the `Etcd` is not available or runs an older spec, or etcd-druid has not completed a scale ([what the class becomes](README.md#a-tenant-namespace)): the StatefulSet's pods and events name the member |
| `DatastoreReady`'s message notes `removing highAvailability waits for etcd-druid to list member <cp>-etcd-0` | etcd-druid does not list the member it keeps as ready ([what the class becomes](README.md#a-tenant-namespace)): look at the pod `<cp>-etcd-0` and its lease of that name; adding `highAvailability` back cancels the removal at once, since the `Etcd` still has its members |
| With `highAvailability`, a replica of a component or an etcd member stays `Pending`, and its rollout never completes | too few nodes the pods may use: each component needs as many as it has replicas, the API server `apiServer.replicas`, and the etcd three, with `Zone` in as many zones, at most three ([how they spread](README.md#a-tenant-namespace)); the pod's `FailedScheduling` event reads `didn't match pod topology spread constraints` ([plugin.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/scheduler/framework/plugins/podtopologyspread/plugin.go#L41)). A `Pending` etcd member also halts the etcd's roll: the members it has not reached keep the old certificate generation ([stateful_set_control.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/statefulset/stateful_set_control.go#L863-L873)). Add nodes |
| `DatastoreReady=False`, `Provisioning`, `Secrets Etcd <cp>-etcd mounts do not exist` | someone removed etcd-druid's finalizer from Secrets the `Etcd` names and deleted them; the members run on what they mounted until they restart: the [runbook](#a-secret-the-etcd-mounts-is-gone) |
| `DatastoreReady=False`, `Provisioning` for long | the message says what is awaited: a complete generation of the etcd certificates that this `ControlPlane` controls (PKI; one another `ControlPlane` controls never counts), the class's credentials copy, or etcd-druid's report; then the `Etcd`'s status — its `lastErrors`, which `DatastoreReady` does not read, are the only place a persistent failure of etcd-druid's other objects (the client Service, the ConfigMap, the leases) shows at a generation it has observed — its conditions, etcd-druid's log, and the tenant's `EtcdOpsTask`s, where a blocked operation shows ([etcd spike](etcd-spike-2026-10.md), finding 4) |
| An etcd member that does not come back after a roll: `status.datastore.members.ready` short of `total`, `DatastoreReady=False` with one member, the StatefulSet's pod missing | the StatefulSet's events (`kubectl describe statefulset <cp>-etcd`): `FailedCreate` means pod admission refused the new pod after the old one was deleted, for a `priorityClassName` that names no PriorityClass (or one deleted since) or a LimitRange or ResourceQuota in the namespace, which no schema can check; with one member it is an outage until fixed, and every certificate generation rolls the member |
| `DatastoreReady=False`, `QuorumLost` | no member ready: one member while it restarts, as every certificate generation and class edit rolls it, or two of three down, which leaves the third without quorum; the [lost quorum](#lost-etcd-quorum) runbook when it lasts |
| API calls fail with `etcdserver: mvcc: database space exceeded` | the [space quota](#etcd-space-quota) runbook; [`LodiaEtcdAlarm`](#lodiaetcdalarm) on `NOSPACE`, and before it [`LodiaEtcdSpaceNearQuota`](#lodiaetcdspacenearquota) on the metric `lodia_controlplane_datastore_db_size_bytes` against `lodia_controlplane_datastore_quota_bytes` ([metrics-are-part-of-the-api](decisions/metrics-are-part-of-the-api.md)) |
| `Progressing=True`, `RollingOut` for long, or `APIServerReady=True`, `ProgressDeadlineExceeded` | the events of the Deployment the message names, or of the etcd's StatefulSet, and the `Etcd`'s `status.lastErrors` when its updated members are not reported; a new replica failing `/readyz` (often a flag in `extraArgs` that the new minor removed); the old replicas keep serving meanwhile |
| a `Backup` `Snapshotting` for long | its message: an etcd that refuses the seal at its quota ([runbook](#etcd-space-quota)), or the full snapshot Lease `<cp>-etcd-full-snap` short of the seal; then the `EtcdOpsTask` `backup-<Backup UID>`, which etcd-druid rejects while another task of the `Etcd` runs ([reconcile_task.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/etcdopstask/reconcile_task.go#L101-L117)), such as its own snapshot before a roll ([statefulset.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/statefulset.go#L273-L300)), and the source's `BackupReady` |
| a `Backup` `Failed` or `SealLost` | final: the message says why; the `EtcdCopyBackupsTask` `backup-<Backup UID>` of a failed copy stays for its `status.lastError`; create another `Backup` |
| `DatastoreReady=False`, `WaitingForBackup` or `BackupNotFound`; `CertificatesReady=False`, `WaitingForBackup` | the `Backup` `restoreFrom.backupRef` names is not sealed yet or does not exist, the message saying which; the restore starts once it is sealed, `Ready`, `Completed`. A sealed `Backup` being deleted still restores: its finalizer keeps it and its keys until every restore that names it is complete, while one deleted before it is `Completed` goes at once. A restore without the keys holds it only while they exist ([Backup](api/controlplane.md#backup)): one created after a foreground deletion let them go reads `BackupNotFound` when the `Backup` is gone; delete that `ControlPlane` and restore another `Backup`. Never delete a `Backup` a restore names with `--cascade=orphan`: its keys `<backup>.encryption-config` lose their controller reference ([garbagecollector.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/controller/garbagecollector/garbagecollector.go#L683-L719)), which PKI requires, and `CertificatesReady`'s message names them until a person puts that reference back; the `Backup` waits for that restore meanwhile |
| `CertificatesReady=False`, `SecretsExist` | a restored `ControlPlane` adopts no Secret under PKI's names: the message names those that exist; delete them |
| `DatastoreReady=False`, `Restoring` for long | the message names the step: the copy, the `EtcdCopyBackupsTask` `restore-<ControlPlane UID>` and its `status.lastError`, made again a minute after it fails; an etcd that refuses the cleanup at its quota ([runbook](#etcd-space-quota)); the full snapshot after the cleanup, the `EtcdOpsTask` `restore-<ControlPlane UID>` and the Lease `<cp>-etcd-full-snap`; or an etcd that does not answer yet. No API server starts meanwhile |
| `DatastoreReady=False`, `RestoreFailed` | the message says which ([reason](api/controlplane.md#conditions-and-their-reasons)): the class no longer names the store the `Backup` was sealed in, which pointing the class back at that store mends; or the `Backup` will never complete, its `Ready` reason being final, which another `Backup` mends; or the `Backup` is `ObjectsControlledElsewhere` until a person deletes the Secret it names; or, for good, the restored etcd lacks the `Backup`'s seal key, or the cleanup would delete 10240 keys or more. Nothing is cleaned up and no API server starts; delete the `ControlPlane` and restore another `Backup` |
| `DatastoreReady=False`, `BackupIncompatible` | the `Backup`'s Kubernetes minor or network (`status.kubernetesVersion`, `status.network`) differs from the `ControlPlane`'s, or the class's etcd quota is below the one the `Backup` was sealed with (`status.seal.quotaBackendBytes`), the message naming which; create the `ControlPlane` again with the `Backup`'s version and network, or on a class with at least that quota |
| `DatastoreReady=False`, `CAPredatesControlPlane` | the etcd is empty while the cluster CA PKI adopted is older than the `ControlPlane`: Secrets came back, the data did not. Restore the management cluster from a backup that keeps UIDs, or delete `<cp>-ca-bundle` and `<cp>-cakey-cluster` for a new identity |
| `ControllersLeading=False`, `AwaitingRelease` or `SourceExists` | a restored `ControlPlane` waits for `spec.restoreFrom.release: Released`, the message listing what will act outside the cluster then, or for its source `ControlPlane` to be deleted |
| `APIServerReady=False`, `Unavailable` | a component without a ready replica, named in the message: its pods' events and logs |
| `ControllersLeading=False`, `ControllerManagerNotLeading` or `SchedulerNotLeading` | the component's pods and logs; its leader Lease inside the tenant, `kube-system/kube-controller-manager` or `kube-system/kube-scheduler`, names the last holder |
| `CAValidityAcceptable=False`, `CAExpiring` | a CA expires within a year; the message names it and when; a [CA rotation](#forced-ca-rotation) mends it, from phase 3 |
| `CertificatesReady=False`, `CAExpired` | a CA is past its `NotAfter`, and every certificate it signed with it; nothing is issued under it until a [CA rotation](#forced-ca-rotation), from phase 3 |
| `CertificatesReady=False`, `CAMissing` | a CA's key Secret `<cp>-cakey-<ca>` is missing or unreadable while its bundle exists; restore it from a backup without its `metadata.ownerReferences` (it is found by name, labels or not), or a [CA rotation](#forced-ca-rotation) mends it, from phase 3; PKI never replaces it on its own |
| `CertificatesReady=False`, `SecretsControlledElsewhere` | the message names each Secret under PKI's names that another owner keeps, and the owner; PKI writes no Secret until they are gone. A previous `ControlPlane` of the same name: the garbage collector deletes its Secrets, except those an `Etcd` still references, on which etcd-druid holds its finalizer ([runbook](#a-secret-the-etcd-mounts-is-gone)). Any other owner: release the Secret or delete it; PKI's names are its own |
| `CertificatesReady=False`, `SecretUnreadable` | `<cp>-encryption-config` or `<cp>-sa-keys` cannot be read; restore it from a backup. PKI never replaces it ([A tenant namespace](README.md#a-tenant-namespace)) |
| `CertificatesReady=False`, `Issuing` | a Secret PKI has to change could not be written; the message names it and Lodia's log (`controller="pki"`) has the error; PKI retries with backoff |
| `AccessReady=False`, `Refused` | the tenant refuses the ClusterRoleBinding the message names, of Lodia, the admin group or the kubelet client: an admission policy in the tenant; Lodia's log (`controller="pki"`) has the answer |
| `AccessReady=False`, `Binding` | the tenant has not confirmed the bindings since its API server became ready; Lodia's log has the error. PKI recreates a missing binding with a ten-minute `system:masters` certificate on its next pass |
| `ControllerManagerHeld=True`, `NodesSilent`; in the tenant, the Lease `kube-system/kube-controller-manager` held by `lodia` | most nodes cannot reach the API, so Health holds the controller manager; the [nodes cut off](#nodes-cut-off-from-the-control-plane) runbook |
| `ControllerManagerHeld=False`, `HoldBlocked` | nodes are silent but another component holds the controller manager's lease, named in the message, for example a platform's own watchdog; run one of the two, not both; or the API server refused the write that would take it, which the message names |
| `ControllerManagerHeld=False`, `TooManyNodes` | the tenant has more Nodes, or more Leases of `kube-node-lease`, than `health.maxNodes`, or a list of them went on past the pages that many fill and one more, or held a page of more items than its limit, or a Node of more than 64 conditions, or a string of a field longer than its bound ([capacity](#capacity)), a `continue` token of more than 4,096 bytes among them, or a response listing them was over 8 MiB, which the message names; Lodia counts none of its nodes, holds no controller manager there and ends a hold at once, and `status.nodes.truncated` is true when it is the Nodes that stopped. A tenant's administrator creates both kinds of object freely: have the surplus deleted, or raise `health.maxNodes` ([settings](README.md#installation)) |
| `Reachable=False`, `ExternalProbeFailed`, nodes fine | the path the message names: `api:` the public endpoint, its Gateway listener, route status and certificate SANs; `tunnel:` the route `<cp>-tunnel`, the sidecar's `<cp>-tunnel-serving` and the tunnel's client certificate; `konnectivity:` the route `<cp>-konnectivity` and `<cp>-konnectivity-server`'s SANs |
| `Reachable=False`, `InternalProbeFailed` | the API server does not answer `/readyz` on its in-namespace Service `<cp>-kube-apiserver`, or the operator cannot reach it; `APIServerReady`, the Deployment `<cp>-kube-apiserver` and the Service's endpoints say which |
| Many tenants turn `Reachable=False` slowly behind one Gateway | a Gateway that does not answer costs a Health worker the 5-second timeout for each tenant, per pass, so a round over `N` tenants takes `N`/2 seconds on the 10 workers and the third failed probe comes that much later; tenants on other Gateways wait behind them, as they did behind a hung public probe |
| `Reachable=False`, `WaitingForCertificates`, naming `<cp>-tunnel-client` | PKI has not issued it yet, or another owner keeps it ([the name](README.md#a-tenant-namespace)); Tenant delivering it to nodes is a separate path |
| `AddonsApplied=False`, `Refused` | the tenant refuses the object the message names: an admission policy in the tenant, or `lodia:operator` without access there (`AccessReady`), which the inventory `kube-system/lodia-inventory` shows first; Lodia's log (`controller="tenant"`) has the answer |
| `AddonsApplied=False`, `ObjectsControlledElsewhere` | the message names an object that exists and is not Lodia's, another owner's under a name Lodia renders, such as one's own CoreDNS, and its field managers: delete it, and Lodia applies its own within the apply period unless the tool a field manager names applies it again, or disable the addon |
| `AddonsApplied=False`, `Applying` | the message names it: an inventory `kube-system/lodia-inventory` that cannot be read (delete it, and every object is applied again), or a tenant that does not answer, while a refused object or another owner's keeps its own reason; Lodia's log (`controller="tenant"`) has the answer |
| `AddonsAvailable=False`, `RolloutPending` | the DaemonSet or Deployment inside the tenant; a node that cannot pull images; a CNI missing (pods of CoreDNS stay Pending until a CNI is installed) |
| `kubectl logs` / `exec` hang, everything else works | the konnectivity path: agents connected to every server (`konnectivity_network_proxy_server_ready_backend_connections`), the agent's DaemonSet, outbound 443 for the konnectivity hostname |
| `Progressing=False`, `WaitingForWindow` for long | expected outside the window; `status.target.scheduledFor` says when it opens, and `lodia.io/maintain-requested-at` rolls the change now |
| `Progressing=False`, `FleetLimit` | other tenants hold the slots, `status.maintenance.slotSince` present whatever their `Progressing` reads: they roll a change Lodia started, or failed one and keep their slots ([`LodiaMaintenanceHalted`](#lodiamaintenancehalted)); the operator's `fleet.maxConcurrentRollouts`; `status.target.scheduledFor` orders the queue |
| `Progressing=False`, `Failed` or `MaintenanceFailed` | `status.kubernetes.upgrade.step` and `Progressing`'s message; the [stuck upgrade](#stuck-upgrade) runbook |
| `Progressing=False`, `WaitingForNodes` | a kubelet more than three minors older than the next minor, `status.nodes.oldestKubelet`: upgrade the nodes first; or `status.nodes` is absent, Health not having listed the nodes yet: the tenant's API must answer it; or `status.nodes.truncated`, the message saying the nodes could not all be counted: the tenant's Nodes are beyond `health.maxNodes`, a page, a Node's conditions, a field's bound or a response of 8 MiB, as `ControllerManagerHeld` `TooManyNodes` says |
| `Progressing=False`, `TimeZoneUnsupported` | `spec.maintenance.window.timeZone` is not in the operator's zone database; an IANA name such as `Europe/Berlin` |
| `Progressing=True`, `LatestGenerationNotObserved` for more than a few seconds | the first condition the message names has not taken the change up: its reason, which describes the generation it names, when it is False; otherwise what its controller waits on ([independent-controllers](decisions/independent-controllers.md)), which `Ready` names when that is not True; its controller's log (`controller="<name>"`) only when that is True at the new generation |
| Nothing changes after editing `spec` | `metadata.generation` against `status.observedGeneration`; `spec.paused`; the operator's leader and its logs |
| A `Warning` event regarding a `ControlPlane`, a `Backup` or a `KubeconfigRequest` | each transition of a condition, a new status or reason, is an event regarding its object for the API server's `--event-ttl`, an hour by default ([options.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controlplane/apiserver/options/options.go#L129)), from `lodia.io/<controller>`, with the condition type as `action`, the new reason as `reason` and the message as `note` ([metrics-are-part-of-the-api](decisions/metrics-are-part-of-the-api.md)); the condition says what holds now. A failure is `Warning` whatever it replaces: `Failed`, `MaintenanceFailed`, `RestoreFailed`, `BackupFailed`, `ProgressDeadlineExceeded`, `SealLost`, `HoldBlocked`, `RequestInvalid`, `PolicyNotEnforced`. A refusal (`Unsupported`, `<Capability>Unsupported`, `Refused`, `ObjectsControlledElsewhere`, `SecretsControlledElsewhere`, `SecretsExist`, every `<Kind>NotFound`, `DatastoreClassAmbiguous`, `VolumesExist`, `BackupIncompatible`, `CAPredatesControlPlane`, `RouteNotAccepted`, `HostnameConflict`, `TooManyNodes`) or a loss (`QuorumLost`, `Unavailable`, `ControllerManagerNotLeading`, `SchedulerNotLeading`, `NodesSilent`, `ExternalProbeFailed`, `InternalProbeFailed`, `CAMissing`, `CAExpired`, `SecretUnreadable`, `CAExpiring`) is `Warning` when it replaces True, or a reason that is neither a first pass (`Reconciling`, `LatestGenerationNotObserved`), a wait (`WaitingFor*`, `Provisioning`, `Restoring`, `RolloutPending`, `Deleting`, `Applying`, `Binding`, `Issuing`, `AwaitingRelease`, `SourceExists`) nor another of these, and `Normal` otherwise, so a tenant's way up and a held restore record no `Warning`; on a running tenant `Issuing` (a lost kubelet signer, a Secret write that keeps failing) and `Applying` (an inventory that cannot be read) count as waits too, so the refusal or loss after them is `Normal`, and the condition and the alerts carry it. A restore's release records one: `APIServerReady` `Unavailable` over True, while the controller manager that has just started has no ready replica. A formed etcd whose `Etcd` is deleted or whose Secrets are missing reads `Provisioning`, a wait, so that loss and a `QuorumLost` after it are `Normal`, carried by `DatastoreReady` and `LodiaControlPlaneNotReady` |

## Runbooks

### Lost etcd quorum

1. Confirm: `DatastoreReady=False`, reason `QuorumLost`; no member `Ready`.
2. If members are down because of their node: bring the node back, or delete the pod so the StatefulSet reschedules it with its volume; quorum returns when two members agree.
   A single member that lost its data is restored by the backup sidecar on its own; a lost member of three is re-added as a learner by etcd-druid.
3. If quorum is lost for good (two of three volumes gone), follow etcd-druid's manual procedure: suspend its reconciliation with the annotations it documents, scale the StatefulSet to 0, delete the PVCs and member leases, set the cluster to one member, scale to 1 so it restores from the newest snapshots, re-enable reconciliation, and let Lodia scale back to 3 ([recovering etcd clusters](https://gardener.github.io/etcd-druid/usage/recovering-etcd-clusters.html)).
   Alternatively, create a new `ControlPlane` with `restoreFrom` the latest `Backup` and move the nodes (new token, `kubeadm join`).
4. Afterwards: roll the API server (`kubectl rollout restart deployment <cp>-kube-apiserver`), so that its watch cache lists the restored data: nothing detects a restore, and the sidecar keeps the snapshot's revisions until it bumps them (`todo.md`).
   Check that `BackupReady` is True again.

### A Secret the Etcd mounts is gone

etcd-druid holds its finalizer `druid.gardener.cloud/etcd-druid` on every Secret an `Etcd` references until none does ([reconciler.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/secret/reconciler.go#L44-L101)), so the certificate generation the `Etcd` names and `<cp>-etcd-backup` stay, terminating at worst, as long as it names them.
Never remove that finalizer.
If someone did and the Secrets are gone, a member that starts again cannot: the kubelet retries until the Secrets exist ([secrets](https://kubernetes.io/docs/concepts/configuration/secret/#using-a-secret)).

1. A gone `<cp>-etcd-backup` comes back at the next pass, from the class's Secret, while the class and its Secret exist: recreate a deleted class, and restore its Secret if it is missing (until then the class is held and the copy is not made).
2. For a gone certificate generation, delete the `Etcd` `<cp>-etcd`, not the `ControlPlane`.
   etcd-druid deletes the StatefulSet alone ([statefulset.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/statefulset.go#L347-L363); it deletes claims only on a scale-in, [pvccleanup.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/pvccleanup.go#L22-L58)), and a StatefulSet keeps its claims by default ([claim retention](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#persistentvolumeclaim-retention)).
   The Datastore controller creates the `Etcd` again on the newest complete generation and the class the claims record, and the new StatefulSet re-attaches the claims `<cp>-etcd-<cp>-etcd-<n>` by name, so the members start on their data and fresh mounts.
   The new `Etcd` takes the smallest claim's size and caps the class's quota at half of it ([dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md)).
   Until then the condition says what is missing: a recorded class that was deleted is `DatastoreClassNotFound` (recreate it), and a class whose Secret is missing is `Provisioning`, `cannot be applied yet` (restore it).
   Do it even when `DatastoreReady` turns True again by itself: PKI numbers generations from the Secrets present and may have issued the gone number again with new content, and the kubelet does not watch an immutable Secret ([immutable Secrets](https://kubernetes.io/docs/concepts/configuration/secret/#secret-immutable)), so a running member keeps the files it mounted.
   A single member is down until it starts again.

### etcd space quota

1. Confirm the `NOSPACE` alarm in the member's logs or metrics (`etcd_server_quota_backend_bytes` against `etcd_mvcc_db_total_size_in_bytes`).
2. Find what grew: usually Events or a controller writing large objects inside the tenant; `kubectl get --raw /metrics | grep apiserver_storage_objects` through the admin kubeconfig.
3. Raise the class quota temporarily (`quotaBackendBytes`), or delete the offending objects.
   The schema wants the class's `storage.size` at least twice the new quota, the `Etcd` caps it at half of the member's volume ([dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md)), and the raise reaches every tenant on the class.
   Then compaction and defragmentation run, and the alarm is disarmed with `etcdctl alarm disarm`.

A restore stopped at its cleanup (`Restoring`, `etcd refuses the cleanup`) runs no API server, so step 2 does not apply: raise the quota as in step 3 and disarm the alarm, and Datastore sends the cleanup again within a minute.

### Restore a tenant from backup

The supported path is a new `ControlPlane` with `spec.restoreFrom`, which replaces the `Backup`'s source: the [replace checklist](#replace-a-cluster-from-a-backup).
A clone beside a live source goes [through Velero](#clone-a-cluster-beside-its-source).

Restoring in place under a running cluster follows the Kubernetes order: stop every API server, restore every etcd member from the same snapshot, start the API servers, then restart the controller manager and scheduler ([operating etcd](https://kubernetes.io/docs/tasks/administer-cluster/configure-upgrade-etcd/)).
With Lodia: set `spec.paused: true`, scale the API server Deployment to 0, restore with `etcdutl snapshot restore --bump-revision 1000000000 --mark-compacted` so revisions keep moving forward and every watch ends and relists ([recovery](https://etcd.io/docs/v3.6/op-guide/recovery/)), then unpause; Lodia rolls the API server, the controller manager and the scheduler.

### Replace a cluster from a Backup

A `ControlPlane` with `restoreFrom` takes its source's place with the source's objects, their UIDs, volumes and load balancers ([restore-replaces-the-source](decisions/restore-replaces-the-source.md)).
Until the release no controller manager runs, so most deletes wait for a finalizer and take effect once controllers run again: neutralize first, then release.
`ControllersLeading`'s message, `AwaitingRelease`, counts what still acts outside the cluster ([reasons](api/controlplane.md#conditions-and-their-reasons)).
In `gateway` mode a new `ControlPlane` with its source's hostnames holds no route while the source exists (`RoutesReady=False`, `HostnameConflict`), so the admin kubeconfig's server reaches the source; reach the new one through its Service, which serves the same certificate:

```sh
kubectl -n team-a get secret <cp>-admin-kubeconfig -o jsonpath='{.data.kubeconfig}' | base64 -d > admin.kubeconfig
kubectl -n team-a port-forward service/<cp>-kube-apiserver 6443:443 &
kubectl --kubeconfig admin.kubeconfig --server https://127.0.0.1:6443 --tls-server-name <hostnames.apiServer> get namespaces
```

With the new `ControlPlane`'s admin kubeconfig:

1. Confirm that no other `ControlPlane` names the source's hostnames on its Gateway, in any namespace: one older than the new `ControlPlane` takes them once the source is gone (`kubectl get controlplanes -A -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name} {.spec.endpoint.gateway.hostnames}{"\n"}{end}'`).
2. Stop the source first: delete its `ControlPlane`, for which the release waits (`SourceExists`), or stop it wherever Lodia cannot see it.
3. Delete the controllers that act outside the cluster with credentials of their own, such as a cloud controller manager, CSI controllers and external-dns, and their credentials: a cloud controller manager's route controller, for one, deletes every route of its cluster name that none of its nodes has ([route_controller.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/cloud-provider/controllers/route/route_controller.go#L383-L406)).
4. Set every PersistentVolume's reclaim policy to `Retain`: deleting a `Delete` one deletes its disk ([persistent volumes](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/concepts/storage/persistent-volumes.md#L201-L210)), and the CSI provisioner drops its finalizer from a `Retain` one ([controller.go](https://github.com/kubernetes-csi/external-provisioner/blob/v6.4.0/vendor/sigs.k8s.io/sig-storage-lib-external-provisioner/v13/controller/controller.go#L1215-L1218)).
5. Never delete a PersistentVolume with the `Delete` policy, nor a namespace, whose deletion deletes everything in it ([namespaces](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/tasks/administer-cluster/namespaces.md#L127-L137)).
6. Never change a Service's type: a Service that no longer asks for a load balancer has it deleted ([controller.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/cloud-provider/controllers/service/controller.go#L374-L392)).
7. Before deleting a Service of type `LoadBalancer`, remove its finalizer `service.kubernetes.io/load-balancer-cleanup`, or its load balancer goes with it ([controller.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/cloud-provider/controllers/service/controller.go#L539-L552)).
8. Delete the pending PersistentVolumeClaims: the CSI provisioner names a volume after its claim's UID ([controller.go](https://github.com/kubernetes-csi/external-provisioner/blob/v6.4.0/pkg/controller/controller.go#L483-L498)), and the driver answers with the volume of that name that exists ([spec.md](https://github.com/container-storage-interface/spec/blob/v1.12.0/spec.md#L777-L778)), the one it made for the source.
9. Set every VolumeSnapshotContent's `deletionPolicy` to `Retain`, or deleting its VolumeSnapshot deletes the snapshot ([volume snapshots](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/concepts/storage/volume-snapshots.md#L120-L125)).
10. Delete the webhooks that call a URL, a service outside the cluster that sees every write they match ([URL](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/reference/access-authn-authz/extensible-admission-controllers.md#L962-L971)), and those with `failurePolicy: Fail`, which refuse the writes they match while their Service has no node to run on ([failure policy](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/reference/access-authn-authz/extensible-admission-controllers.md#L1123-L1130)): the release waits for Tenant's writes.
11. Suspend the CronJobs ([suspension](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/concepts/workloads/controllers/cron-jobs.md#L145-L154)).
12. Set `spec.restoreFrom.release: Released`.

### Clone a cluster beside its source

A clone restores through the API, as Velero does, which gives every object a new UID and drops its owner references ([restore.go](https://github.com/vmware-tanzu/velero/blob/v1.18.4/pkg/restore/restore.go#L2416-L2435)).

1. Create a `ControlPlane` without `restoreFrom`, and join its nodes.
2. Install Velero with its node agent (`--use-node-agent`, [file system backup](https://github.com/vmware-tanzu/velero/blob/v1.18.4/site/content/docs/v1.18/file-system-backup.md#L54-L58)) in the source and in the clone, both on the same bucket, the clone's backup location `ReadOnly` ([migration](https://github.com/vmware-tanzu/velero/blob/v1.18.4/site/content/docs/v1.18/migration-case.md#L49-L62)).
3. In the source: `velero backup create <name> --include-cluster-resources=true --exclude-namespaces kube-system,kube-public,kube-node-lease --exclude-resources certificatesigningrequests.certificates.k8s.io,podcertificaterequests.certificates.k8s.io --default-volumes-to-fs-backup`.
   The excluded namespaces hold the source's bootstrap tokens, Leases and `extension-apiserver-authentication`, and Lodia writes the clone's own; with namespaces excluded, cluster-scoped objects need `--include-cluster-resources=true` ([resource filtering](https://github.com/vmware-tanzu/velero/blob/v1.18.4/site/content/docs/v1.18/resource-filtering.md#L60-L74)), and the source's certificate requests stay behind.
   Every volume goes with its data, so the clone gets volumes of its own: a PersistentVolume backed up without its data and with the `Retain` policy is restored as it was, on the source's disk ([restore.go](https://github.com/vmware-tanzu/velero/blob/v1.18.4/pkg/restore/restore.go#L1528-L1559)).
4. In the clone, once `velero backup describe <name>` finds it: `velero restore create --from-backup <name>`.
   Velero skips what exists, Lodia's objects among them ([restore-reference.md](https://github.com/vmware-tanzu/velero/blob/v1.18.4/site/content/docs/v1.18/restore-reference.md#L61)), and never restores Nodes or VolumeAttachments ([restore_controller.go](https://github.com/vmware-tanzu/velero/blob/v1.18.4/pkg/controller/restore_controller.go#L66-L95)).
5. Controllers that act outside the cluster run in the clone at once: give them the clone's own credentials and identity, such as another external-dns owner ID, or leave them out of the restore.

### Stuck upgrade

1. `status.kubernetes.upgrade.step` says the step, `Progressing`'s message what failed; the upgrade resumes when the cause is gone.
2. `Backup` step failing: the `Ready` message of the `Backup` `<cp>-pre-upgrade-v<minor>-<ControlPlane UID>`.
   A failed `Backup` is final: delete it, and Lifecycle takes it again.
   A class without a backup store skips it, and `Progressing`'s message says so.
   A failure named elsewhere holds the target at this step even with the `Backup` `Completed`, and the target moves once the cause is gone: if that took long, delete the `Backup` first, so that the upgrade starts from a fresh one.
3. API server step failing: a removed flag or feature gate in `extraArgs` or `featureGates` is the usual cause; fix `spec`, the step retries.
4. In-tenant step failing: an object the tenant refuses, named in `AddonsApplied`; the addons' pods roll on the owner's nodes afterwards, and an image they cannot pull (air gap, registry mirror missing) shows in `AddonsAvailable`, not here.
5. `MaintenanceFailed`: a new operator revision the tenant failed in its slot of the fleet, `Progressing`'s message saying what; the tenant keeps the slot, through an edit of its spec or a pause, and takes the next release at once, and the slot is free once the cause is gone and `Ready` has been True for the settling time.
6. Rollback of a minor: create a new `ControlPlane` with `restoreFrom` the `Backup` `<cp>-pre-upgrade-v<minor>-<ControlPlane UID>` and the old `spec.version`; it replaces the cluster, so delete the old one, or create the new one `Released` once the old one is gone ([restore-replaces-the-source](decisions/restore-replaces-the-source.md)).
   A downgrade in place is not supported by Kubernetes.

Pre-upgrade `Backup`s have no owner, so they outlive their `ControlPlane`, one per minor per tenant, and deleting them is the owner's ([restore-replaces-the-source](decisions/restore-replaces-the-source.md)).

### Forced CA rotation

Phase 3, not built: no controller reads `lodia.io/operation`, so the steps below do nothing yet.

- **A kubeconfig or client certificate leaked:** rotate the client CA and complete it at once (`rotate-client-ca-start`, then `rotate-client-ca-complete`); every client certificate Lodia holds is already re-issued, and kubelets whose certificates are still under the old CA re-bootstrap with a new token (`kubeadm token create`, then on the node `kubeadm reset` and `kubeadm join`), or wait for the automatic completion after 30 days if the leak allows it.
- **A tunnel client certificate leaked:** rotate the tunnel CA (`rotate-tunnel-ca-start`), which also restarts the sidecars to cut the sessions already open ([staged-credential-rotation](decisions/staged-credential-rotation.md)).
- **The cluster CA key leaked or the CA expires:** rotate the trust anchor.
  Start it, add the new bundle to every node (`/etc/kubernetes/pki/ca.crt` and the certificate authority in `/etc/kubernetes/kubelet.conf`), complete it.
  Between start and complete both CAs are trusted; completing before the nodes have the bundle cuts off every node that trusts only the old CA.

### Nodes cut off from the control plane

1. Confirm: `ControllerManagerHeld=True`, reason `NodesSilent`; `status.health.controllerManagerHold` says since when and how many nodes are silent.
   Workloads on the nodes keep running; the tenant's controllers do not (no new ReplicaSets, no node IPAM) until the hold ends.
2. Find the broken path: the Gateway listener and routes, DNS of the API hostname, the network between the nodes and the Gateway.
   Kubelets log their failed lease renewals.
3. When the nodes reach the API again, Health releases the lease two minutes later and the controller manager starts afresh; no node was tainted, no pod evicted.
4. If the nodes really are gone (a site lost for good), turn the hold off with `spec.health.holdControllerManager: false`, so the controller manager can mark them and move their pods to the nodes that remain ([controller-manager-hold](decisions/controller-manager-hold.md)).

### Operator down

Tenants keep running: their pods, their certificates (valid for a year) and their etcds do not depend on Lodia being up.
The one limit is in `gateway` mode: the tunnel's client certificate has 30 days after its renewal time while the operator is down, the `ControlPlane` is paused, or PKI is stopped by a Secret another owner keeps, by one a restored `ControlPlane` does not control (`SecretsExist`), by an earlier failed write, or by a lost or expired tunnel CA; then every pod of that tenant loses its API.
Changes to `ControlPlane` objects are accepted by the API (no webhooks) and applied when Lodia is back.
Renewal and backups pause; the [`LodiaOperatorDown`](#lodiaoperatordown) alert fires first.

## Scraping a tenant's etcd

Lodia's own metrics come from the operator ([scraping it](README.md#scraping-the-operator)); scraping a tenant's components is the platform's choice.
The etcd's NetworkPolicy admits only what dials the members ([A tenant namespace](README.md#a-tenant-namespace)), and policies add up ([network policies](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/concepts/services-networking/network-policies.md#L75-L78)), so a platform that scrapes the members admits its scraper with a policy of its own, here a Prometheus of prometheus-operator, by the label it gives its pods ([operator.go](https://github.com/prometheus-operator/prometheus-operator/blob/v0.94.2/pkg/prometheus/server/operator.go#L1698-L1706)):

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: <cp>-etcd-metrics
  namespace: <the ControlPlane's namespace>
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/managed-by: etcd-druid
      app.kubernetes.io/part-of: <cp>-etcd
      app.kubernetes.io/name: <cp>-etcd
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: <the Prometheus's namespace>
          podSelector:
            matchLabels:
              app.kubernetes.io/name: prometheus
      ports:
        - port: 2379
        - port: 8080
```

Before applying it, weigh both ports ([metrics-are-part-of-the-api](decisions/metrics-are-part-of-the-api.md#decision)):

- etcd serves its metrics on the client port alone, behind client certificates, and Lodia turns on no etcd authentication, so the certificate a scraper needs reads every key of the tenant;
- the backup-restore sidecar serves `/metrics` on 8080 beside the endpoints that take snapshots and start initialization, which the policy opens to the scraper too.

## Alerts shipped in the chart

The chart's PrometheusRule, behind `metrics.rules.enabled` and off by default ([what the Prometheus needs](README.md#alerting-on-the-operator)), holds the rules of [`charts/lodia/files/rules/lodia.yaml`](../charts/lodia/files/rules/lodia.yaml), on the metrics of [metrics-are-part-of-the-api](decisions/metrics-are-part-of-the-api.md) and controller-runtime's; [`test/rules/lodia_test.yaml`](../test/rules/lodia_test.yaml) tests each on both sides of what it reads.
`critical` is a tenant's API, its etcd or its nodes' link down, or nothing reconciling; `warning` waits for working hours.
Every alert on a tenant leaves out a `ControlPlane` being deleted, but `LodiaControlPlaneDeletionStuck`, and every alert on its conditions or counts leaves out a paused one, whose conditions and counts stand as they were.
Every alert keeps firing for 4 minutes after its condition ends, so that it outlasts the failovers of a `helm upgrade` of Lodia, which move every series to the other pod; an alert still pending at a failover starts its wait again.

### LodiaOperatorDown

Critical, after 5 minutes without a replica that leads: `leader_election_master_status{name="lodia.io"}` is 1 in the leader alone, so no replica scraped fires it too.
Look first at `up{job="lodia"}` and the Lease `lodia.io`: a holder whose `renewTime` moves leads and reconciles, so `up` 0 for its pod is a scrape that fails, whose last error Prometheus shows on the target, and no `up` for its pod is a target Prometheus does not have, since it writes `up` at each scrape of a target ([scrape.go](https://github.com/prometheus/prometheus/blob/v3.15.1/scrape/scrape.go#L2297-L2310)): it selects no ServiceMonitor `<release>`, that ServiceMonitor's selector misses the Service `<release>-metrics`, the Service leaves the pod out ([what scraping needs](README.md#scraping-the-operator)), or, under `arbitraryFSAccessThroughSMs.deny`, prometheus-operator rejects the ServiceMonitor for its `bearerTokenFile` ([resource_selector.go](https://github.com/prometheus-operator/prometheus-operator/blob/v0.94.2/pkg/prometheus/resource_selector.go#L273-L277)) and records a `Warning` event `InvalidConfiguration` on it ([resource_selector.go](https://github.com/prometheus-operator/prometheus-operator/blob/v0.94.2/pkg/prometheus/resource_selector.go#L155)), which `metrics.serviceMonitor.scrapeClass` avoids.
Otherwise no replica leads: nothing is reconciled, and tenants keep running ([operator down](#operator-down)); look at the operator's pods in the release namespace, their logs and events, and the Lease's last holder.

### LodiaControlPlaneNotReady

Critical, after 10 minutes of `Ready` not True, but while it reads `Deleting`, which [`LodiaControlPlaneDeletionStuck`](#lodiacontrolplanedeletionstuck) watches, `Restoring` within an hour of `Ready`'s `lastTransitionTime`, a restore its owner asked for, or while a restored `ControlPlane` is held for its release: `AwaitingRelease`, `SourceExists`, or `HostnameConflict` while `ControllersLeading` reads one of the two, a held restore in `gateway` mode whose source keeps the hostnames ([replace a cluster](#replace-a-cluster-from-a-backup)).
A restore reads `Restoring` until it is recorded, after the cleanup and a full snapshot of the cleaned data ([reasons](api/controlplane.md#conditions-and-their-reasons)), so one that never ends fires 70 minutes after `Ready` turned False.
A restore fires on `WaitingForBackup`, which PKI also reads for a failed `Backup`.
In `gateway` mode, a restore whose source keeps the hostnames reads `HostnameConflict` from its start, since `RoutesReady` comes first in `Ready`, while `ControllersLeading` reads `WaitingForAccess` until `AccessReady` is True, so it fires unless its hold is written within 10 minutes.
A held restore whose certificates, etcd or endpoint fail reads that failure in `Ready`, and fires.
`Ready`'s reason and message name the first input that is not True: its row in [what breaks](#what-breaks-and-how-it-shows).
A hibernated tenant joins the exceptions with hibernation, in phase 3.

### LodiaControlPlaneDeletionStuck

Warning, after 30 minutes of `Ready` reading `Deleting`, but for a `ControlPlane` paused before its deletion, which deletes nothing until it is unpaused; one paused after its deletion began fires, and waits for its unpause too.
The Datastore controller deletes the `Etcd` `<cp>-etcd`, waits for etcd-druid to let it go and reclaims the claims before it removes its finalizer `lodia.io/datastore` ([deletion](README.md#deletion)): look at the `Etcd`'s finalizers and `status.lastErrors`, etcd-druid's log and Lodia's (`controller="datastore"`).
Then the garbage collector deletes what the `ControlPlane` owns, and an object of its namespace left with a `deletionTimestamp` names the finalizer it waits for.

### LodiaRedundancyLost

Warning, after 15 minutes of a component with at least one ready replica and fewer than it runs, or of an etcd with a quorum of members ready and fewer than it has: the control plane serves, and one more loss is an outage, which `LodiaControlPlaneNotReady` names.
Look at the component's pods or the member's, and their nodes; a replica that stays `Pending` and a member that does not come back after a roll have their rows in [what breaks](#what-breaks-and-how-it-shows).

### LodiaRolloutStuck

Warning, after an hour of a roll that does not finish while the tenant holds no slot and runs no upgrade, which [`LodiaMaintenanceSlotHeldLong`](#lodiamaintenanceslotheldlong) and [`LodiaUpgradeStalled`](#lodiaupgradestalled) watch: `APIServerReady` reads `ProgressDeadlineExceeded`, or `status.datastore.members.updated` is absent or below `ready`.
Until it finishes, the replicas and members it has not reached run on the certificates they started with.
Look at the events of the Deployment `APIServerReady`'s message names, or of the StatefulSet `<cp>-etcd`; an absent `updated` means etcd-druid does not report the `Etcd`: its `status.lastErrors` and etcd-druid's log.

### LodiaCertificateExpiring

Warning, after an hour of `status.credentials.notAfter`, the earliest expiry among the certificates PKI writes, its CAs included, within 14 days, a paused tenant included, since PKI renews nothing while it is paused.
Renewal failed, which `CertificatesReady` names; or a CA that PKI does not renew is in its last 14 days, which `CertificatesReady` does not name until it has expired and [`LodiaCAExpiring`](#lodiacaexpiring) has named for a year: a [CA rotation](#forced-ca-rotation) mends it, from phase 3; or the tenant is paused: unpause it.

### LodiaCAExpiring

Warning, after an hour of `CAValidityAcceptable=False`: a CA is in its last year, named in the message with when it expires.
A [CA rotation](#forced-ca-rotation) mends it, from phase 3.

### LodiaBackupStale

Warning, after 5 minutes of `BackupReady` reading `BackupFailed`, which etcd-druid reports once the last delta snapshot is three delta snapshot periods old ([check_backup_ready.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/health/condition/check_backup_ready.go#L101-L120)), or `Unknown` for more than three periods since its last transition, as a store that never worked reads: 20 minutes after the last snapshot at the class's default period.
A class without a backup store (`NoBackupStore`), a shared datastore (`Unsupported`) and a class whose `deltaSnapshotPeriod` is below a second, which takes no delta snapshots, never fire it.
Look at the backup sidecar's log in the etcd's pods and at the class's bucket credentials; etcd-druid's message is in the condition.

### LodiaEtcdSpaceNearQuota

Warning, after 15 minutes of the etcd's largest member database above 80% of its quota: compaction does not keep up, or the tenant writes too much.
Act before the quota: the [space quota](#etcd-space-quota) runbook.

### LodiaEtcdNoLeader

Critical, on two samples in a row, a minute apart, in which no quorum of the etcd's voting members names one leader; one sample never fires it, and a new or restored etcd is sampled only once it has formed.
The tenant's API cannot write: the [lost quorum](#lost-etcd-quorum) runbook, and the network between the members inside the management cluster.

### LodiaEtcdAlarm

Critical, on an alarm armed in two samples in a row.
`NOSPACE`: API calls fail with `etcdserver: mvcc: database space exceeded` until the alarm is disarmed: the [space quota](#etcd-space-quota) runbook.
`CORRUPT`, armed when two members' hashes of one revision differ ([corrupt.go](https://github.com/etcd-io/etcd/blob/v3.5.27/server/etcdserver/corrupt.go#L184-L273)), fails every write ([corrupt.go](https://github.com/etcd-io/etcd/blob/v3.5.27/server/etcdserver/corrupt.go#L432-L464)) while reads still answer ([v3_server.go](https://github.com/etcd-io/etcd/blob/v3.5.27/server/etcdserver/v3_server.go#L131)): [restore](#restore-a-tenant-from-backup) the tenant.
etcd compares those hashes only when its config turns a check on ([server.go](https://github.com/etcd-io/etcd/blob/v3.5.27/server/etcdserver/server.go#L2670-L2700)), and etcd-druid's config turns none on ([etcdconfig.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/configmap/etcdconfig.go#L42-L64)), so it does not arm on Lodia's etcds today.

### LodiaControllerManagerHeld

Critical, after 15 minutes of `ControllerManagerHeld=True`: most nodes cannot reach the control plane, and the tenant's workloads run while its controllers wait: the [nodes cut off](#nodes-cut-off-from-the-control-plane) runbook.

### LodiaControllerManagerHoldBlocked

Critical, after a minute of `ControllerManagerHeld` reading `HoldBlocked`: most nodes are silent and Lodia cannot hold the controller manager, so nothing stops the evictions the hold exists for ([controller-manager-hold](decisions/controller-manager-hold.md)).
Mend the nodes' path, as for [nodes cut off](#nodes-cut-off-from-the-control-plane), and what the message names: its row in [what breaks](#what-breaks-and-how-it-shows).

### LodiaUpgradeFailed

Warning, at once, on `Progressing=False` with `Failed` or `MaintenanceFailed`: the [stuck upgrade](#stuck-upgrade) runbook.

### LodiaUpgradeStalled

Warning, once `status.kubernetes.upgrade.startedAt` is more than 2 hours ago, whatever its step, while `Progressing` reads neither `Failed`, `MaintenanceFailed` nor `Paused`.
An upgrade that neither ends nor fails waits on its `Backup` step, a `Backup` `Snapshotting` behind a full etcd for one, or on Tenant: `status.kubernetes.upgrade.step` and the [stuck upgrade](#stuck-upgrade) runbook.

### LodiaMaintenanceHalted

Warning, after 15 minutes of as many tenants as `fleet.maxConcurrentRollouts` holding a slot of the fleet, `status.maintenance.slotSince`, with a reason other than `Maintaining` and `TimeZoneUnsupported`, whose changes roll or settle.
`MaintenanceFailed` and `Paused` free nothing without a person, and `Unsupported` holds its slot for good over a failed change, so changes Lodia starts are halted across the fleet.
The [stuck upgrade](#stuck-upgrade) runbook for each tenant that holds a slot.

### LodiaMaintenanceSlotHeldLong

Warning, once a tenant's slot was taken more than an hour ago, a paused tenant included, but for `MaintenanceFailed`, which [`LodiaUpgradeFailed`](#lodiaupgradefailed) names.
A change Lodia started neither finishes nor fails: a tenant API that stops answering mid-rollout, or a Tenant that waits after the grant on a cause the grant did not remove, stays `Maintaining` with its slot ([upgrades-and-maintenance-windows](decisions/upgrades-and-maintenance-windows.md#decision)).
Look at `Progressing`'s message and the conditions it names.

### LodiaReconcileErrors

Warning, after 15 minutes of a controller's reconcile errors above 5% of its reconciles, both over 15 minutes: its errors are in Lodia's log (`controller="<name>"`), never in a condition.

### LodiaHealthSamplingLate

Warning, once a pass of `health-nodes` started more than 10 seconds after its due time in the last 10 minutes: the hold's margin keeps 10 seconds for a delay in Health's queue, and a longer one can let the tenant's controller manager mark a silent node before the hold makes it exit ([controller-manager-hold](decisions/controller-manager-hold.md)).
Look at the operator's CPU and its throttling, and at `controller_runtime_active_workers{controller="health-nodes"}` against `controller_runtime_max_concurrent_reconciles`.

### LodiaKubernetesEndOfSupport

Warning, once the end-of-support date of the minor of `status.kubernetes.version` has been within 30 days, or past, for an hour, a paused tenant included.
The date is the version table's, the end of life that [kubernetes.io/releases](https://kubernetes.io/releases/) gives the minor, and `Progressing`'s message names it from 60 days before it.
Set `spec.version` to a patch of the next minor ([upgrades](README.md#upgrades-and-windows)); Lodia forces no upgrade at the date ([upgrades-and-maintenance-windows](decisions/upgrades-and-maintenance-windows.md)).
A tenant that reports no version, or one whose minor the table lacks, has no series and never fires it.

## Capacity

One idle control plane, as the nightly conformance run measures it before the suite starts ([`TestConformance`](../test/e2e/README.md#nightly-conformance-and-the-idle-control-plane)): a `ControlPlane` of one replica at Kubernetes v1.37.0 with a dedicated etcd of one member, two nodes joined, the konnectivity agents and CoreDNS running and no workload, idle for 3 minutes.
CPU is the rate of the kubelet's `container_cpu_usage_seconds_total` over the next 5 minutes and memory its `container_memory_working_set_bytes` at their end, both from the management kubelet's `/metrics/resource` ([resource_metrics.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/kubelet/metrics/collectors/resource_metrics.go#L51-L63)).
Measured on 2026-10-26 by nightly run [40360016124](https://github.com/lodia-io/lodia/actions/runs/40360016124) at e5686f8, on GitHub's `ubuntu-24.04` runner for private repositories ([runners](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)): amd64, 2 CPUs and 7.8 GiB, kernel 6.17.0-1022-azure, in kind v0.33.0 with its node image v1.37.0 (containerd 2.3.4) and etcd-druid v0.39.0.

| pod | container | CPU (millicores) | memory (MiB) |
|---|---|---:|---:|
| API server | `kube-apiserver` | 25.9 | 227.7 |
| | `konnectivity-server` | 5.6 | 11.5 |
| | `lodia-signer` | 0.1 | 10.2 |
| controller manager | `kube-controller-manager` | 9.3 | 55.4 |
| scheduler | `kube-scheduler` | 6.3 | 23.9 |
| etcd | `etcd` | 13.3 | 37.5 |
| | `backup-restore` | 1.8 | 26.2 |
| sum | | 62.3 | 392.5 |

The sum replaces phase 0's target for a single idle control plane, 0.5 CPU and 1 GiB with etcd.
CPU varies between runs more than memory: run [40334709578](https://github.com/lodia-io/lodia/actions/runs/40334709578) on the same runner measured 43.3 millicores and 387.7 MiB.
The nightly now runs on the public runner, 4 CPUs and 15.6 GiB, where run [40371428547](https://github.com/lodia-io/lodia/actions/runs/40371428547) measured 55.4 millicores and 422.9 MiB; the table stays run 40360016124's.
A highly available control plane's target stays 1.5 CPU and 2.5 GiB.
A management cluster sized for N tenants needs the sum of their requests on its control plane nodes, plus room to roll one Deployment of every tenant (the fleet limit bounds how many roll at once), plus etcd volumes with low fsync latency.

The operator: two replicas; memory grows with the number of tenants it caches (label-scoped, metadata-only for Secrets); the phase 4 gate is 200 tenants under 2 GiB.
Health reads a tenant as JSON, and the tenant's administrator can have any server answer, an APIService naming a Service of their own included ([review checklist](review-checklist.md), item 10).
So what a read holds is fixed by the constants below and not by what a tenant sends:
- no object and no array of an answer reaches `encoding/json`, whose decoding multiplies the bytes it is given (an empty object becomes a struct, an invalid byte in a string three bytes, the key of a skipped field is unquoted and folded all the same): `internal/jsonscan` walks the body, which `json.Valid` has checked, and every value it hands over is a slice of it;
- a time is parsed from its string token with the layout of `metav1.Time` or `metav1.MicroTime`, and not through their `UnmarshalJSON`, which hands any token, an object or an array included, to `encoding/json` ([time.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/apis/meta/v1/time.go#L100-L110)); `TestNothingIsPassedToEncodingJSONButAScalarToken` fails on a call of an `UnmarshalJSON` in the package;
- a scalar is decoded, by `encoding/json` or `strconv`, only when Health keeps the field it fills and the raw size of its token is within that field's bound, checked before the decode; a field Health only compares with a word (a condition's `type` and `status`, a Status's `status`, a `failurePolicy`, a Service's `type`, a PersistentVolume's reclaim policy, a claim's phase) is compared on its raw token, and a string past its bound is none of the words and is neither decoded nor a stop nor a failed read, while a token that is no string fails the read at any size;
- a key is matched on its raw bytes and with its case, as Kubernetes' own decoder reads one ([json.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/runtime/serializer/json/json.go#L271)); a key with an escape is unquoted only up to 256 bytes, and any longer one is skipped without a copy;
- an error names the field and its bound and nothing of the value: a time that does not parse is "not a time", not the text of the parser's error; a failed answer whose body is no Status within these bounds is the error client-go builds for its status code, which quotes up to 2,048 bytes of a text body ([request.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/request.go#L1339-L1363)), with its code, reason and `Retry-After` and a message of Health's own, `the answer is no Status Health can read`; a request that fails before the head of its answer is read, at a deadline or a reset, is net/http's error with its verb and path and none of its query, which carries the `continue` token of a later page, up to 36,883 bytes (`TestARequestThatFailsBeforeItsAnswerCarriesNoByteOfTheTokenItAsksWith`);
- the body of an answer is cut at 8 MiB, `maxResponseBytes`, and its status line and headers at 64 KiB, `maxResponseHeaderBytes`, where `net/http` reads up to 10 MiB of them ([transport.go](https://github.com/golang/go/blob/go1.26.10/src/net/http/transport.go#L329-L334)); a kube-apiserver's are under 512 bytes (352 bytes of the biggest of eight answers on envtest of Kubernetes 1.35.0, 511 as an HTTP/2 header list), and over HTTP/2 the limit is the same figure and 320 bytes, counting 32 bytes more for each field ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L83-L98));
- a request is sent once, `MaxRetries(0)`: client-go sends a request again up to ten times on a 429 or a 5xx that carries `Retry-After` ([request.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/request.go#L167)), and the next pass is Health's retry.

The raw bound of a field counts the bytes of a string between its quotes, in which every character may be escaped as `\uXXXX`, six bytes.
A token of the wrong kind, a field that is no string where Health keeps a string or a time that is no string, fails the read at any size, and a failed list of the Nodes leaves the last good one standing.
A string of a field Health keeps, past its bound, stops a list of the Leases or of the Nodes (`TooManyNodes`), makes a kind of a held restore "could not be listed", and fails any other read; a stop inside a page counts the items of that page read before it.
A word field's string past its bound is none of the words, and neither a stop nor a failed read.

| field | raw bound |
|---|---|
| a name, a namespace | 2,048 bytes, 253 decoded: a Lease or a Node with a longer name is not counted, and only a held restore's tally counts such an object, without naming it |
| a `resourceVersion`, a time | 64 bytes; the hold's patch carries the `resourceVersion` it read decoded and `json.Marshal`-escaped ([encode.go](https://github.com/golang/go/blob/go1.26.10/src/encoding/json/encode.go#L205-L216)): up to 192 bytes of invalid UTF-8, or 384 of `<`, `>` and `&`, which escape as six |
| a Status's `reason`, a cause's `reason` | 126 bytes, `RequestEntityTooLarge` with every character escaped, the longest of the 19 reasons that `IsNotFound` and, through `retry.RetryOnConflict`, `IsConflict` compare a reason with ([errors.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/api/errors/errors.go#L47-L67), [util.go](https://github.com/kubernetes/client-go/blob/v0.37.1/util/retry/util.go#L103-L105)), where `NamespaceTerminating`, the cause `apistatus.Refused` looks for, is 120 (`TestEveryReasonAnErrorIsComparedWithIsReadAsItselfWithEveryCharacterEscapedAndMaxReasonRawIsTheLongest`, `TestANamespaceBeingDeletedIsTheCauseRefusedLooksForWhateverTheEscapes`): a longer string makes the answer no Status Health can read |
| a condition's `type` and `status`, a Status's `status`, a `failurePolicy`, a Service's `type`, a PersistentVolume's reclaim policy, a claim's phase | 72 bytes, `LoadBalancer` with every character escaped, the longest word that a field read with `jsonscan.Word` is compared with, the reasons above being longer (`TestEveryWordAWordFieldIsComparedWithIsReadAsItselfWithEveryCharacterEscapedAndMaxWordRawIsTheLongest`): a longer string is none of the words and is not decoded |
| a `holderIdentity` | 512 bytes |
| a kubelet version | 384 bytes, 64 decoded: a longer version is not kept |
| a number | 11 bytes, the longest int32 |
| a `continue` token | 4,096 raw bytes, which Health sends back decoded with the next request: up to 12,288 bytes when every byte is invalid UTF-8, three decoded for one, and client-go builds its query again for each use of the request ([request.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/request.go#L571-L582)), escaping every byte outside the unreserved set as three, so the query is at most 36,883 bytes (`TestWhatHealthSendsBackOfATokenAndAVersionAtTheirRawBoundsIsPinned`) |
| a Status's `message` | 1,024 bytes; a longer one is replaced by a sentence that says so |

A webhook's `url` and an APIService's `service` are read for being there and never decoded.
A condition whose `type` is not `Ready` keeps nothing but being counted, whatever the length of its type: a Node's writer chooses it, and `ValidateNode` and `ValidateNodeUpdate` check no condition ([validation.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/apis/core/validation/validation.go#L7607-L7738)), so the API server keeps one of 316 bytes, the length the API's own convention for a condition's type allows ([types.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/apis/meta/v1/types.go#L1649-L1653)).
Each array it keeps is read one element at a time, up to a bound:

| array | bound | past it |
|---|---|---|
| items of the Leases of `kube-node-lease` | 500, the page's `limit` | the list stops, `TooManyNodes` |
| items of the Nodes | 100, the page's `limit` | the list stops, `TooManyNodes` |
| a Node's `conditions` | 64 | the list stops at that Node, `TooManyNodes` |
| items of each list of a held restore | 500 | the kind could not be listed |
| a webhook configuration's `webhooks` | 256 | the kind could not be listed |
| a failed request's `details.causes` | 64 | the rest are dropped |

A pass of `health-nodes` holds one decoded page of each list at a time: a page becomes its Lease versions or Node summaries before the next is read.
A list of webhook configurations counts one configuration at a time and keeps no webhook.

What one read holds at most is the larger of its two phases, the body being read and after it.
Its terms are of three kinds, and a figure is the terms and the margins added and rounded up:
- what Health's code bounds, argued exactly from the code and its constants: the body under `maxResponseBytes` in `io.ReadAll`'s chunks and the slice it returns, what the types keep, the summaries a pass builds and the reads in flight at once;
- the transport's and the connection's buffers whose size grows with a limit the client sets, each the most its limit allows, with its source line: the stream window, the frame size, the header list, the names a head announces and the trailers, the hpack decoder's buffer and the gzip reader;
- a margin of 2 MiB for each connection, for everything else that the standard library, x/net and the runtime allocate on the way and no term itemizes: the readers and writers of 4 KiB, TLS's records, the request's encoder, which Health's longest query of 36,883 bytes leaves at 0.13 to 0.14 MiB (`TestAnHTTP2ConnectionThatSentTheLongestQueryHealthSendsKeepsItsEncoderWithinTheMargin`), and the structures of a stream and of a response.
  It is a margin and no derived term: the measures below confirm that the terms and the margin hold everything they see.

The phases, each term with the source of what runs in it:
- while the body is read, up to 8 MiB:
  - `io.ReadAll`'s chunks and the slice it returns, 20.4 MiB.
    client-go reads the body with `io.ReadAll` ([request.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/request.go#L1159)).
    go1.26.10's starts with a chunk of 512 bytes, then 256, and grows each next chunk by half ([io.go](https://github.com/golang/go/blob/go1.26.10/src/io/io.go#L712), [L717](https://github.com/golang/go/blob/go1.26.10/src/io/io.go#L717), [L747](https://github.com/golang/go/blob/go1.26.10/src/io/io.go#L747)): the sizes do not depend on what the reader returns, and the 25th chunk is 2.7 MiB and the 26th 4.1 MiB.
    It closes a chunk once less than a sixteenth of it is free ([L742](https://github.com/golang/go/blob/go1.26.10/src/io/io.go#L742)), so a chunk is closed between 15/16 full and full, and it allocates the slice of the whole body while every chunk is alive ([L734](https://github.com/golang/go/blob/go1.26.10/src/io/io.go#L734)).
    A reader that fills every read closes the 24th chunk at 5.5 MiB of body and holds, at the cut, 5.5 MiB of closed chunks, the open chunk of 2.7 MiB and the slice of 8 MiB: 16.3 MiB.
    A reader that returns each chunk just past 15/16 full, as a body that arrives in pieces does, closes the 25th chunk at 7.75 MiB, before the cut, and opens the 26th: 8.3 MiB of closed chunks, 4.1 MiB open and the slice, 20.4 MiB.
    No chunk closes earlier than that, and the 27th, which opens at 11.6 MiB of body at the soonest, is past the cut, so 20.4 MiB is the most below it.
    `TestIoReadAllHoldsAtMostItsChunksAndTheSliceItReturnsAtTheCut` reads 1 to 8 MiB through the real `io.ReadAll` with each reader, finds the chunks' heap within 256 KiB of the sizes it offered, 16.27 and 20.38 MiB at 8 MiB, and nothing above 20.4 MiB.
    One read of each of the 47 shapes below, over HTTP/1.1, reaches 20.4 MiB of heap in use in every run.
  - Over HTTP/2, 4 MiB and 32 KiB of body the reader has not read yet.
    The transport buffers a stream's window, 4 MiB ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L46-L49)), and wastes at most twice its largest chunk of 16 KiB ([databuffer.go](https://github.com/golang/net/blob/v0.60.0/http2/databuffer.go#L11-L19)).
    A body longer than the cap leaves a full window buffered when the read is cut; `TestAnHTTP2StreamBuffersItsWindowOfABodyNobodyReadsAndNoMore` holds the buffer of a body nobody reads to that window.
    Over HTTP/1.1 the unread body is in the socket and not in the heap.
  - The headers' map, up to 2.5 MiB over HTTP/1.1 and 0.5 MiB over HTTP/2.
    net/http reads a response's head up to `maxResponseHeaderBytes` into a map of one slot for each name and a shared slice of strings for the values ([reader.go](https://github.com/golang/go/blob/go1.26.10/src/net/textproto/reader.go#L516-L603)).
    It takes its lines from `bufio.Reader.ReadLine`, which ends a line at a line feed with or without a carriage return ([reader.go](https://github.com/golang/go/blob/go1.26.10/src/net/textproto/reader.go#L60-L81), [bufio.go](https://github.com/golang/go/blob/go1.26.10/src/bufio/bufio.go#L405-L438)), so a field line is 3 bytes at the least, a name of one byte, a colon and a line feed, and a name of a field is a token: 77 characters make 77 names of one byte and 5,929 of two ([reader.go](https://github.com/golang/go/blob/go1.26.10/src/net/textproto/reader.go#L686-L712)).
    A field costs a slot of 40 bytes and a control byte, up to 16/7 of that after the table grows at a load of 7/8 and splits at 1,024 slots ([group.go](https://github.com/golang/go/blob/go1.26.10/src/internal/runtime/maps/group.go#L20), [table.go](https://github.com/golang/go/blob/go1.26.10/src/internal/runtime/maps/table.go#L19)), 94 bytes; its key and its value are strings of up to 16 bytes each, and its element of the shared slice 16: 142 bytes.
    `TestTheLeastAnHTTP1FieldLineAddsToAHeadIsFieldLineLessMapSlotSinceTheMapGrowsInSteps` holds the 48 bytes of the key, the value and the slice's element: a head of 150 to 158 field lines of 16 bytes grows by 48 at the least with each line, the map's slot being allocated in steps that no one line shows.
    Each map of the derivation costs that for each key and one table of 1,024 slots, 41,984 bytes, more, since a table that has just split holds fewer than half of its slots; `TestAMapOfHeadersHoldsAtMostASlotOfMapSlotBytesForEachKeyAndOneTableMore` fills 100 maps of 1,995 keys and of 21,930, and the most was 196,880 and 1,693,872 bytes, against 229,514 and 2,103,404.
    A `Trailer` header of a chunked answer makes a key of the response's `Trailer` for each name it lists, in a map of its own, and a copy of the name ([fixTrailer](https://github.com/golang/go/blob/go1.26.10/src/net/http/transfer.go#L767-L806)): a slot of 94 bytes and the copy of 16, 110 bytes.
    The value of the `Trailer` line is a string of the head's map, up to the bytes of the head.
    A name costs the head 2 bytes at the least, itself and its comma, and a field line 3: per byte of the head the dearest are the names of one byte, at most 256, one for each byte, then the field lines of one byte, at most 77, then the names of two bytes, which a head can pay as many of as it holds bytes (a name of two bytes costs 3 for its 110).
    So the most is 256 names of one byte, 77 field lines of one byte and the names of two bytes that the other 64,793 bytes of the head buy, 28,160 + 10,934 + 2,375,744 bytes, two tables of 83,968 and the value of 65,536: 2,564,342 bytes, 2.45 MiB, and the term is 2.5 MiB.
    `TestAnHTTP1HeadOfFieldLinesEndingInALineFeedIsHeldAtMostHeadersHTTP1` reads a head of 65,534 bytes of 13,634 field lines ended by a line feed, 13,636 keys, and finds 1.0 to 1.05 MiB; `TestAnHTTP1HeadOfNamesAndFieldLinesTogetherWithTheTrailerValueIsHeldAtMostHeadersHTTP1` reads a head of 65,535 bytes of 51 field lines of one byte and a `Trailer` of 195 names of one byte and 21,636 of two, and finds 1.58 MiB.
    Over HTTP/2 the limit is the bound and 320 bytes, counted as name, value and 32 bytes a field ([config.go](https://github.com/golang/net/blob/v0.60.0/http2/config.go#L118-L125)), so at most 1,995 fields: 0.3 MiB of map, and 80 KB of decoded fields and the hpack table.
  - Over HTTP/2, the names a head announces and the trailers, up to 1.0 MiB.
    The framer reads each HEADERS frame under two budgets of 65,856 bytes, the limit and 320 bytes: one for every field but `trailer`, counted as name, value and 32 bytes, and one for the names of the `trailer` fields, counted as each name's length and 32 bytes ([frame.go](https://github.com/golang/net/blob/v0.60.0/http2/frame.go#L1714-L1760)).
    A field costs 33 bytes at the least, so a frame has at most 1,995 fields of each budget.
    The head makes a key of the response's `Trailer` for each announced name, with a copy of it ([handleResponse](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L2060-L2068)): a slot of 94 bytes and the copy of 16, 110 bytes, 219,450 bytes for 1,995 names.
    An answer may end in a HEADERS frame of trailers, which the transport keeps in a map of its own, then in the response's `Trailer` ([processTrailers](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L2196-L2218), [copyTrailers](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L2492-L2501)).
    Each of its fields costs a slot in both maps, a copy of its name and a slice of one value, 220 bytes, 438,900 bytes for 1,995 fields.
    Its `trailer` fields, which the budget of the names counts, are appended to the values of one key: 48 bytes each, a slice element grown by up to a half and the string, 95,760 bytes for 1,995.
    The decoded fields of the frame, 40 bytes each ([HeaderField](https://github.com/golang/net/blob/v0.60.0/http2/hpack/hpack.go#L36-L42), `TestTheSizesTheTermsAreBuiltFromAreTheSourcesOnThisPlatform`), 159,600 bytes for the 3,990 of both budgets, are held until the frame is processed, and each of the two maps may hold a table of 1,024 slots more, 83,968 bytes.
    That is 997,678 bytes, 0.95 MiB: the term is 1.0 MiB.
    The server may send the frame once the reader has read to within a window of the end of the body, so at the cut of an answer of up to 12 MiB the trailers are held beside the full stream buffer.
    `TestAnHTTP2AnswersAnnouncedNamesAndTrailersAreHeldBesideItsHeadersAndNoMoreThanTrailersHTTP2` reads 1,914 header names, then the same with 1,861 names announced and 1,861 trailers, and finds 0.57 to 0.61 MiB more, where the parts of that many add to 0.67 MiB; the measures below send them at the limit.
    A Go server declares no `Trailer` field among the trailers it sends ([declareTrailer](https://github.com/golang/go/blob/go1.26.10/src/net/http/h2_bundle.go#L6724-L6734), [ValidTrailerHeader](https://github.com/golang/go/blob/go1.26.10/src/vendor/golang.org/x/net/http/httpguts/guts.go#L20-L50)), so no measure reaches the `trailer` fields of a trailers frame, which the term counts from the framer.
  - Over HTTP/1.1, the trailers of a chunked answer, up to 0.3 MiB.
    net/http reads them once the body has ended, and only when a CRLF CRLF is within the 4,096 bytes of its reader's buffer ([readTrailer](https://github.com/golang/go/blob/go1.26.10/src/net/http/transfer.go#L894-L951)), so a block of lines ended by a line feed, the last by a CRLF, holds at most 77 field lines of 3 bytes and the others of 4 within the 4,093 bytes that precede its last 3: 1,042 field lines.
    It reads them into a map of its own, which `mergeSetHeader` copies into the map of the names the head announced ([L953-L959](https://github.com/golang/go/blob/go1.26.10/src/net/http/transfer.go#L953-L959)): a slot in each, a copy of the name and a slice of one value, 220 bytes, 229,240 bytes for 1,042 fields, and two tables of 1,024 slots more.
    That is 313,208 bytes, 0.30 MiB.
    The read has them before `io.ReadAll` allocates its slice, and the response keeps them while it does.
    `TestAnHTTP1ChunkedAnswersTrailersEndingInALineFeedAreHeldAtItsEndAtMostTrailersHTTP1` reads the 1,036 field lines that 4,093 bytes carry after a head that announces one name and finds 0.11 MiB once the copy's source is garbage.
  - The gzip reader, 219 KiB, over both protocols.
    A transport that sets no `Accept-Encoding` asks for gzip and reads a gzip answer through a reader of its own, taken from a pool and put back when the read ends ([net/http transport.go](https://github.com/golang/go/blob/go1.26.10/src/net/http/transport.go#L2438-L2439), [L3120-L3151](https://github.com/golang/go/blob/go1.26.10/src/net/http/transport.go#L3120-L3151), [x/net transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L2802-L2830)).
    client-go leaves compression on (`TestTheTenantClientAsksForGzipAndReadsItOverEveryProtocol`), and a kube-apiserver compresses an answer over 128 KiB, which it does by default since 1.16 ([writers.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/endpoints/handlers/responsewriters/writers.go#L152-L155), [kube_features.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/features/kube_features.go#L321-L324)), so the Node lists of an honest tenant come through it; the stream is the tenant's to choose, since the aggregator passes an APIService's `Content-Encoding` back.
    Over HTTP/2 the stream's buffer holds compressed bytes, at most its window, while the cap counts the decompressed ones.
    What a `gzip.Reader` holds, at go1.26.10 on 64-bit, is:
    - the reader, 664 bytes, which takes 704, and a `bufio.Reader` of 88 bytes, which takes 96, with its buffer of 4,096, since the body is no `io.ByteReader` ([gunzip.go](https://github.com/golang/go/blob/go1.26.10/src/compress/gzip/gunzip.go#L74-L114));
    - flate's decompressor, 4,392 bytes, which takes 4,864, with two Huffman decoders whose tables of 512 entries are in it, and `bits` and `codebits`, 2,528 and 152 bytes, which take 2,688 and 160 ([inflate.go](https://github.com/golang/go/blob/go1.26.10/src/compress/flate/inflate.go#L104-L109), [L267-L300](https://github.com/golang/go/blob/go1.26.10/src/compress/flate/inflate.go#L267-L300), [L807-L817](https://github.com/golang/go/blob/go1.26.10/src/compress/flate/inflate.go#L807-L817)); the fixed decoder is a package variable and no heap;
    - the window, 32 KiB ([dict_decoder.go](https://github.com/golang/go/blob/go1.26.10/src/compress/flate/dict_decoder.go#L39-L43));
    - the link tables of the last dynamic block, each Huffman decoder keeping one of 64 entries of 4 bytes for each 9-bit prefix that leads to a longer code, in a slice of 24 bytes a prefix, and nothing of an earlier block, since `init` and `Reset` clear the decoder first ([inflate.go](https://github.com/golang/go/blob/go1.26.10/src/compress/flate/inflate.go#L122-L124), [L167-L187](https://github.com/golang/go/blob/go1.26.10/src/compress/flate/inflate.go#L167-L187), [L786-L795](https://github.com/golang/go/blob/go1.26.10/src/compress/flate/inflate.go#L786-L795));
      the code is complete (L167), so a prefix has two leaves or more, one of 15 bits seven or more under it, and the codes of 9 bits or fewer, which fill the other prefixes, need as many leaves as 512 less the long prefixes has bits set: of 286 symbols (L23) at most 137 prefixes, 8,768 entries of any longest length, and of 30 distances at most 8, 512 entries (`TestACompleteCodeHasAtMostAsManyLongPrefixesAsTheGzipTermCounts`);
      the 145 tables take 256 bytes each and their slices 3,456 and 192: 40,768 bytes;
    - the header of the first member, kept for the reader's life: the extra field up to 65,535 bytes, a large object of 65,536, and the name and the comment up to 511 bytes of Latin-1 each, which `readString` turns into runes and then into a string, and the runtime allocates a string's UTF-8 length and 3 bytes ([gunzip.go](https://github.com/golang/go/blob/go1.26.10/src/compress/gzip/gunzip.go#L141-L170), [L199-L224](https://github.com/golang/go/blob/go1.26.10/src/compress/gzip/gunzip.go#L199-L224), [string.go](https://github.com/golang/go/blob/go1.26.10/src/runtime/string.go#L278)): 1,022 and 3 bytes, which take 1,152 (`TestTheSizesTheTermsAreBuiltFromAreTheSourcesOnThisPlatform`), 67,840 bytes;
    - the header of a later member, read while the first and the tables are held and dropped when it is read ([gunzip.go](https://github.com/golang/go/blob/go1.26.10/src/compress/gzip/gunzip.go#L262-L290)): the same and a slice of runes of 2,048 bytes, 69,888 bytes;
    - the transport's wrapper, 48 bytes ([gzipReader](https://github.com/golang/go/blob/go1.26.10/src/net/http/transport.go#L3120)).
    That is 223,920 bytes, 218.7 KiB: the term is 219 KiB.
    `TestTheSizesTheTermsAreBuiltFromAreTheSourcesOnThisPlatform` holds each of these sizes, and the copies of a name and a value that the HTTP/1.1 head's term counts, by allocating that part alone, 100 to 10,000 times up to 8 MiB: the least of 20 runs is the size, since allocations by other goroutines only add to a run.
    `TestAGzipReaderAllocatesExactlyTheSumOfItsPartsAndTheRunesItConvertsOnEachWorstStream` reads one byte through a `gzip.Reader` and finds 45,376 bytes allocated for a stream of `gzip.Writer`, 86,144 for a code of 137 and 8 tables, and 158,080 with the extra field, the name and the comment: each its parts, and for the last the 4,096 bytes of the two slices of runes that the name and the comment pass through on their way to a string.
    `TestAGzipReaderThatReadsALaterMembersHeaderAllocatesExactlyGzipPartsAndTheRunesOfItsThreeEarlierConversions` finds 230,064 bytes for the crafted stream, a second member's header and the transport's wrapper: `gzipParts` and the three slices of runes that are garbage by the time the last string is made.
    `TestTheTenantClientsHoldAGzipReaderOfTheWorstStreamUnderGzipReader` reads the crafted stream through the tenant's clients, 152 to 155 KiB of live heap over both protocols.
    `TestTheCraftedGzipStreamsMakeTheTablesAndTheHeaderTheirPartsAssume` holds the crafted streams to what the sum assumes: 145 tables and two slices of them, an extra field of 65,535 bytes, a name and a comment of 1,022.
    The whole readers are measured as the parts are: the bytes the process allocated while one was built and read, the least of 20 runs of 100 to 10,000 rounds, which another goroutine can only add to, so the sums are equal and no tolerance is left.
    That measure counts garbage with what is held, and the only garbage the readers make is the slices of runes.
    A reader that is reset keeps nothing of the stream it read before, which an allocation count cannot show: `TestAGzipReaderThatIsResetHoldsNothingOfTheStreamItReadBefore` watches the extra field, the name, the comment, the buffered reader and its buffer and the 147 slices of the link tables of the crafted stream through weak pointers, and finds none alive after a `Reset` to another stream and two collections.
  - The connection of each read in flight, apart from the read's own buffers, and the margin.
    Every read in flight is counted with a connection of its own, over HTTP/1.1 and over HTTP/2, and each connection has the buffers below once.
    Over HTTP/1.1 each read in flight has a connection of its own, and nothing of net/http's reader and writer of 4 KiB each ([transport.go](https://github.com/golang/go/blob/go1.26.10/src/net/http/transport.go#L1991)) is itemized.
    Over HTTP/2 the reads of a pass share a connection only while it takes another stream, and two things stop that:
    - client-go leaves x/net's `StrictMaxConcurrentStreams` off, and nothing in client-go, apimachinery or Lodia sets it, so a connection with as many streams as the server's `SETTINGS_MAX_CONCURRENT_STREAMS` takes no new request ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L665-L686)): the pool has none to give ([client_conn_pool.go](https://github.com/golang/net/blob/v0.60.0/http2/client_conn_pool.go#L53-L82)), net/http dials another and sends the request again ([transport.go](https://github.com/golang/go/blob/go1.26.10/src/net/http/transport.go#L720-L722)), and x/net keeps it beside the first ([client_conn_pool.go](https://github.com/golang/net/blob/v0.60.0/http2/client_conn_pool.go#L151-L158)).
    - A connection that has received a GOAWAY takes no new request while its streams finish ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L702-L708)), so the next page of one list goes on a new connection while a page of the other is in flight on the old one.
      A Go server sends a GOAWAY after a response with `Connection: close` ([h2_bundle.go](https://github.com/golang/go/blob/go1.26.10/src/net/http/h2_bundle.go#L6785-L6796)), which a kube-apiserver sets at random under `--goaway-chance` ([goaway.go](https://github.com/kubernetes/apiserver/blob/v0.37.1/pkg/server/filters/goaway.go#L66-L72)).
      Lodia sets that flag to 0.001 when it runs more than one API server replica (`internal/controller/workloads/render.go`), and a replica that shuts down sends a GOAWAY to every client ([api-exposure](decisions/api-exposure.md)).
    A kube-apiserver allows 100 streams unless `--http2-max-streams-per-connection` says otherwise ([secure_serving.go](https://github.com/kubernetes/apiserver/blob/v0.37.1/pkg/server/secure_serving.go#L191-L199)), and `apiServer.extraArgs` does not refuse that flag (`api/v1alpha1/controlplane_types.go`): the first case needs the flag, the second only Lodia's own GOAWAY.
    `TestTheTwoListsOfAPassAreOnConnectionsOfTheirOwnOverHTTP2WhenTheServerAllowsOneStream` shows the first against net/http's HTTP/2 server.
    `TestAGoawayAfterAPageOfLeasesPutsTheNextPageOnASecondConnectionWhileTheNodeListIsInFlightOnTheFirst` shows the second against a raw HTTP/2 server that holds the first page of Leases until the Node list is in, writes the GOAWAY before the page's body, and answers the Node list once the second page is in, and asserts the connection of each request: the first page and the Node list on one, the second page on another.
    Each connection has, once:
    - a frame buffer of 16 KiB.
      client-go builds the transport with `http2.ConfigureTransports` and leaves `MaxReadFrameSize` at 0 ([cache.go](https://github.com/kubernetes/client-go/blob/v0.37.1/transport/cache.go#L147), [http.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/net/http.go#L131-L194)), which x/net clips up to the minimum frame size, 16,384 bytes, and sends in its settings ([config.go](https://github.com/golang/net/blob/v0.60.0/http2/config.go#L68-L92), [frame.go](https://github.com/golang/net/blob/v0.60.0/http2/frame.go#L419), [transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L510), [L531](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L531)).
      A frame of more is refused by its header, before its payload is read ([frame.go](https://github.com/golang/net/blob/v0.60.0/http2/frame.go#L511-L527)); the framer keeps a buffer of the largest frame it has read ([frame.go](https://github.com/golang/net/blob/v0.60.0/http2/frame.go#L455-L460)).
      `TestAnHTTP2ConnectionReadsNoFrameOverFrameSize` reads a frame of 16,384 bytes and not one of 16,385.
    - the hpack decoder's buffer, 256 KiB.
      The transport decodes every head of a connection with the connection's own `hpack.Decoder` ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L515)), which copies a field that a frame ends inside into `saveBuf`, a `bytes.Buffer` it keeps for the connection's life and only resets ([hpack.go](https://github.com/golang/net/blob/v0.60.0/http2/hpack/hpack.go#L243-L281), [L234-L241](https://github.com/golang/net/blob/v0.60.0/http2/hpack/hpack.go#L234-L241)).
      It saves up to 2 × (65,856 + 8) = 131,728 bytes of a field it has not finished, and refuses a string past 65,856 bytes ([hpack.go](https://github.com/golang/net/blob/v0.60.0/http2/hpack/hpack.go#L487)); every frame adds 16,384 bytes at the most, and a `bytes.Buffer` grows to the larger of what it needs and twice its capacity ([buffer.go](https://github.com/golang/go/blob/go1.26.10/src/bytes/buffer.go#L247-L270)), so it doubles through 32,768, 65,536 and 131,072 to 262,144 at the frame that takes it past 131,072, and no further, since an unfinished field and the frame that follows it take 131,728 + 16,384 = 148,112 bytes (`TestTheHpackBufferIsWhatABytesBufferGrowsToByFramesWhileAFieldIsUnfinished`).
      The head is then past the bound, and the stream fails with `errResponseHeaderListSize` while the connection keeps serving ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L2035-L2036), [L2002-L2012](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L2002-L2012)), so every later read on it holds the buffer.
      `TestAnHTTP2ConnectionKeepsHpacksBufferOfAFieldRunningAcrossFramesForItsLifeAndNoMoreThanHpackBuffer` sends a head of nine frames whose one field, a name and a value of 65,856 bytes, runs across all of them, and finds the connection 255,008 to 261,984 bytes heavier than after nine frames of table updates, and serving the next read.
    - a margin of 2 MiB.
- after it, the body, 8 MiB, and what the types keep, 2 MiB.
  The most is 1.85 MiB, 1.83 measured and 1.90 under the race detector, a page of 100 Nodes of 64 Ready conditions with every string at its raw bound and invalid UTF-8: a decoded string is cut to a copy of its kept length, which `TestACutStringHoldsNoneOfTheStringItWasCutFrom` holds, so a name takes 256 bytes, a kubelet version 80, a condition's status 224 and its type 8, and the conditions' slice 4 KiB a Node.
  The other kinds keep at most 0.4 MiB, and a failed answer's Status, a message of 1,024 raw bytes, a reason and 64 causes of 126, 0.03 MiB (`TestWhatTheTypesKeepAtTheRawBoundsIsUnderKeptAtMost`).

So a read holds 28.5 MiB over HTTP/2 (20.4 + 4.03 + 0.5 + 1.0 + 0.22, then 2.27 for its connection: 0.02, 0.25 and the margin of 2) and 25.5 MiB over HTTP/1.1 (20.4 + 2.5 + 0.3 + 0.22, then the margin of 2) while its body is read, and 10 MiB after: 28.5 MiB, since a tenant's server may answer either way.
The memory of a pass follows from the reads it has in flight at once, each with a connection of its own: 28.5 MiB over HTTP/2, of which the connection's 2.27 MiB, and 25.5 MiB over HTTP/1.1, of which the margin's 2:
- a pass of `health-nodes` reads the controller manager's Lease, then lists the Leases and the Nodes at once, then sends the hold's write, whose patch carries the read's `resourceVersion`, 384 bytes at most, and reads its answer: two reads at once, 56.9 MiB over HTTP/2 and 50.9 MiB over HTTP/1.1, and the summaries the pass builds beside the sampling, up to `health.maxNodes` Lease versions and Node summaries, 4 MiB (3.72 MiB measured, with names of 253 bytes: a Lease version takes up to 112 bytes of map slot, its name of 256 and 24 bytes of the slice it is built from, a Node summary 72 bytes, 256 and 80, 3.8 MiB for 5,000 of each; `TestWhatAPassBuildsBesideTheSamplingIsUnderSummariesAtMost`): 60.9 MiB over HTTP/2 and 54.9 MiB over HTTP/1.1;
- a pass of `health` over a held restore lists seven kinds at once, each with a stream, a buffer, a head and a connection of its own: seven reads, 198.9 MiB over HTTP/2 and 177.9 over HTTP/1.1;
- the 100 workers of `health-nodes` hold 100 passes at once, 5.94 GiB at most, and the 10 workers of `health`, by default, 10 held restores, 1.95 GiB.

At the default `GOGC` the heap in use may grow to about twice the live heap, so the operator's memory limit and `GOMEMLIMIT`, which are set when the operator is measured (`todo.md`, "From the chart"), start from about twice these figures.

Five tests measure the live heap and the heap in use at `GOGC=5`, so that the heap in use stays near the live heap, and run only when `LODIA_MEASURE_HEAP` is set.
They answer over HTTP/1.1 and over HTTP/2 with TLS, as a kube-apiserver does.
The first three fail when a read or a pass holds more heap in use than its structural figure, or ends in another outcome than the shape expects:
- `TestMeasureTheLiveHeapOfEveryReadOverAnAmplifyingResponse` answers each read with a response of 8 MiB in 47 shapes: nothing but empty objects where the read keeps an array, a body padded with bytes the types skip, a key of letters, of escapes and of invalid UTF-8, and a field past its bound in each of those;
- `TestMeasureTheLiveHeapOfAPassWhoseEveryReadIsAtItsWorst` runs a pass of `health-nodes`, with its Lease read, both lists answered together by a server that waits until both requests are in, and its renewal, and a pass of `health` over a held restore, the seven lists answered as they come and together, every answer a body of 8 MiB;
- `TestMeasureTheLiveHeapOfAReadWhoseAnswerIsCutAtTheBoundWithHeadersAtTheirs` reads an answer of 16 MiB, which the cap cuts at 8 MiB, 100 times in a process, with the headers of a plain answer, with distinct names filling the bound, about 13,600 field lines ended by a line feed over HTTP/1.1 and as many as the HTTP/2 limit allows over HTTP/2, and with a head that announces names filling it, 51 field lines of one byte and about 21,800 names in a chunked head over HTTP/1.1 and as many as the budget allows over HTTP/2, which also takes trailers of distinct names filling the same limit, after a body of 12 MiB so that they arrive before the read is cut, and with the body gzipped, of bytes that do not compress, so that the transport reads it through a decompressor;
- `TestMeasureTheLiveHeapOfAPassOverAPageOf128000EmptyWebhooks` runs a pass over a held restore whose webhook lists answer 500 configurations of 256 empty webhooks;
- `TestMeasureTheLiveHeapOfPassesOverTenantsPaddedToTheirBounds` runs passes over a tenant padded to its bounds, with every page of Leases and of Nodes just under 8 MiB and the default `health.maxNodes`, one pass and 100 at once on 100 tenants.

The figure they compare is the heap in use.
The live heap is what the last collection marked, objects allocated during it included, so it can read above anything alive at once: in 1,200 cut answers over HTTP/2 with the headers and the trailers at their bound it read up to 38.3 MiB, where the heap in use did not pass 23.2 MiB.

```
GOTOOLCHAIN=go1.26.10 LODIA_MEASURE_HEAP=1 KUBEBUILDER_ASSETS="$(go tool setup-envtest use 1.35.0 -p path)" go test -count=1 -v -run TestMeasureTheLiveHeap ./internal/controller/health/
```

These are measurements, not worst cases: the sampler reads the heap when it can, and a peak it misses is a peak it does not report, so each figure is a floor of what the read or the pass held, and the structural figures above are the ceiling.
The runs below are on go1.26.10, `go version go1.26.10 darwin/arm64`, on 16 cores of an Apple M4 Max; each is a process of its own, `-count=1`, and each cut shape runs 100 times in it.

| the measure | runs | live heap | heap in use | structural bound |
|---|---|---|---|---|
| one read, the most of 47 shapes, over HTTP/1.1 | 12 | 8.0 to 16.0 MiB | 20.3 to 20.4 MiB | 25.5 MiB |
| the same, over HTTP/2 | 12 | 8.5 to 21.9 MiB | 20.9 to 22.2 MiB | 28.5 MiB |
| a read whose answer of 16 MiB is cut, over HTTP/1.1 | 1,200 | 12.4 to 21.6 MiB | 20.4 MiB | 25.5 MiB |
| the same, headers at their bound | 1,200 | 13.5 to 33.4 MiB | 21.4 to 21.6 MiB | 25.5 MiB |
| the same, headers at their bound, gzipped | 1,200 | 13.5 to 29.5 MiB | 21.5 to 21.6 MiB | 25.5 MiB |
| the same, announced names at their bound | 1,200 | 9.9 to 25.9 MiB | 17.9 MiB | 25.5 MiB |
| the same, announced names at their bound, gzipped | 1,200 | 14.1 to 28.4 MiB | 22.0 MiB | 25.5 MiB |
| a read whose answer of 16 MiB is cut, over HTTP/2 | 1,200 | 22.1 to 29.1 MiB | 22.3 to 24.1 MiB | 28.5 MiB |
| the same, headers at their bound | 1,200 | 22.3 to 25.0 MiB | 22.6 to 23.3 MiB | 28.5 MiB |
| the same, trailers at their bound | 1,200 | 22.0 to 38.3 MiB | 22.3 to 23.2 MiB | 28.5 MiB |
| the same, announced names at their bound | 1,200 | 22.2 to 25.0 MiB | 22.5 to 23.4 MiB | 28.5 MiB |
| the same, gzipped | 1,200 | 21.9 to 27.2 MiB | 22.4 to 24.3 MiB | 28.5 MiB |
| one pass of `health-nodes`, the Lease read, both lists answered together and the renewal, over HTTP/1.1 | 12 | 8.1 to 24.2 MiB | 16.1 to 32.5 MiB | 54.9 MiB |
| the same, over HTTP/2 | 12 | 0.0 to 33.2 MiB | 9.2 to 41.4 MiB | 60.9 MiB |
| one pass of `health` over a held restore, seven answers as they come, over HTTP/1.1 | 12 | 24.9 to 73.7 MiB | 49.7 to 114.0 MiB | 177.9 MiB |
| the same, over HTTP/2 | 12 | 56.0 to 87.9 MiB | 107.2 to 143.9 MiB | 198.9 MiB |
| the same, its seven lists answered together, over HTTP/1.1 | 12 | 57.1 to 73.6 MiB | 81.6 to 113.9 MiB | 177.9 MiB |
| the same, over HTTP/2 | 12 | 63.8 to 143.7 MiB | 119.8 to 144.1 MiB | 198.9 MiB |
| one pass of `health` over 500 configurations of 256 empty webhooks, named with 253 bytes, one list or both | 12 | 0.0 to 1.1 MiB or 0.0 to 3.1 MiB | 0.8 to 3.3 MiB or 1.6 to 5.3 MiB | |
| the same, named with 2,048 bytes | 12 | 0.0 to 2.2 MiB or 2.0 to 4.6 MiB | 0.2 to 3.8 MiB or 4.3 to 6.8 MiB | |
| one pass over a tenant padded to its bounds, 62 responses | 12 | 8.7 to 24.6 MiB | 20.2 to 32.7 MiB | 54.9 MiB |
| 100 passes at once on 100 tenants | 12 | 1.14 to 1.34 GiB, 11.7 to 13.7 MiB a pass | 1.26 to 1.52 GiB | 5.4 GiB |

The 100 passes at once open 200 connections to one listener, and two runs of the measures started together failed in it, passes ending in an unexpected EOF or in a pass that did not stop, from the loopback server, whose listen queue is 128 (`kern.ipc.somaxconn`); the twelve above ran one at a time and all passed.
The closest any measure came to its bound is 3.4 MiB, the cut answer with announced names at their bound and a gzipped body over HTTP/1.1.
No run put a pass at the ceiling of its reads, since their chunks do not all peak together.
A first pass of `health-nodes` over 5,000 Nodes and their 5,000 Leases, the most `health.maxNodes` counts by default, took 313, 295 and 293 ms in three runs on go1.26.10, and a pass over 5,001 Nodes, which stops at the bound, 295, 287 and 286 ms (`TestATenantOfMaxNodesNodesIsCountedWholeAtTheOperatorsTimeout`, on envtest, on 16 cores of an Apple M4 Max, one run at a time).
The race detector slows it tenfold, to 2.90, 2.86 and 2.96 s for 5,000 Nodes and 2.83, 2.82 and 2.87 s for 5,001, in three runs of `TestATenantOfMaxNodesNodesIsCountedWholeUnderTheRaceDetector`, which counts the same tenant with a `Timeout` of a minute, against the operator's of 5 s.
That first test is built without the race detector, and `make test` runs it in a step of its own.
Between passes it keeps at most `health.maxNodes` Lease versions and Node summaries per tenant ([review checklist](review-checklist.md), item 10).
The tenant's administrator can have any server answer the lists, so the pass bounds every string it keeps itself:
- a Lease or a Node named with more than 253 bytes is not kept; no real object has such a name ([validation.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/validation/validation.go#L190-L191));
- a Lease version keeps a digest of its `resourceVersion`, 64 bits, whatever the form of the version;
- a Node summary keeps a kubelet version only up to 64 bytes and a Ready status only if it is `True`, `False` or `Unknown`: the tenant's administrator writes both, and [`ValidateNodeUpdate`](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/apis/core/validation/validation.go#L7666-L7738) checks neither.

With every string at its bound a Lease version takes a name of 253 bytes and a map slot of 48 bytes (key header, digest and time), and a Node summary 72 bytes, a name of 253 bytes and a kubelet version of 64.
A tenant at the default `health.maxNodes` of 5,000 keeps 5,000 of each, 3.4 MiB of live heap (3.43 to 3.44) as `TestWhatASamplingKeepsRestsOnItsOwnBoundsAndNotOnWhatTheServerAnswers` measures in three runs with names of 253 bytes and versions at their raw bounds: what the lists answer does not change it.
Each tenant has two clients, `health-nodes`' and `health`'s, and each keeps connections between passes, up to these against a server that allows one stream:
- Over HTTP/2, twice the reads in flight at once and one more: 5 for `health-nodes`, which has two, and 15 for `health` over a held restore, which has seven.
  x/net adds a connection to its pool only when none in it can take a request ([client_conn_pool.go](https://github.com/golang/net/blob/v0.60.0/http2/client_conn_pool.go#L151-L158)), and a connection takes none while its streams, its reserved slots and its reset streams that no PING has answered fill the server's limit ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L665-L686), [L729-L731](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L729-L731)).
  A read holds one stream or slot and none once it has returned: client-go reads and closes the body before it returns ([request.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/request.go#L1102), [with_retry.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/with_retry.go#L340-L354)), and closing waits until the stream has left the connection ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L2309-L2318)).
  So a pool grows only when every connection in it is busy with a read, and a server that answers leaves at most the reads in flight and one more: 3 and 8.
  A read that a deadline cuts before the server sent any frame leaves its connection with a reset stream, which counts against the limit until a PING is answered ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L1326), [L1356-L1386](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L1356-L1386), [L2711-L2714](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L2711-L2714)), so a server that answers none makes every later read dial again.
  `tenant.do` closes the client's idle connections after a read fails before its answer or while its body comes, and x/net closes a connection that holds no stream or reserved slot, reset streams apart ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L785-L801)); a connection with a read in flight stays.
  The stream of a cut read leaves its connection after the read has returned, in the goroutine that sent the request ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L1073-L1076), [L1087-L1090](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L1087-L1090)), so the close can find that connection busy and leave it, at most one for each read in flight, until a later read fails or the connection has been idle for 90 s ([http.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/net/http.go#L123-L124)).
  Those connections take no request, so they come beside the reads in flight and the one more.
- Over HTTP/1.1, twice the reads in flight: 4 and 14.
  net/http dials for a read only when no connection is idle ([transport.go](https://github.com/golang/go/blob/go1.26.10/src/net/http/transport.go#L1546-L1549)), and a dial goes on when the read that began it is cut or served by an idle connection meanwhile ([L1524-L1529](https://github.com/golang/go/blob/go1.26.10/src/net/http/transport.go#L1524-L1529)), so each read in flight has a connection and at most one dial that adds another.
  A read cut by its deadline closes its connection ([transport.go](https://github.com/golang/go/blob/go1.26.10/src/net/http/transport.go#L2206-L2211)), and no more than 25 stay idle ([cache.go](https://github.com/kubernetes/client-go/blob/v0.37.1/transport/cache.go#L45), [L151](https://github.com/kubernetes/client-go/blob/v0.37.1/transport/cache.go#L151)).

None of this bounds a client against a server that sends a GOAWAY on an idle connection and keeps it open: x/net drops the connection from its pool at the GOAWAY ([transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L2502-L2514)), no close of idle connections reaches it, and it stays until it has been idle for 90 s, a connection for each read in that time (`todo.md`).

By the rule of the derivation each connection takes a margin of 2 MiB, and over HTTP/2 its frame buffer and its hpack decoder's buffer as well, 2.27 MiB.
A tenant at rest keeps at most 4 MiB of summaries and 20 connections over HTTP/2, 49.4 MiB: 11.4 MiB for the connections of `health-nodes` and 34.0 for those of `health`; over HTTP/1.1 it keeps 4 MiB and 18 connections, 40.0 MiB.
`TestPassesWhoseReadsADeadlineCutKeepAtMostConnectionsKeptWhateverTheNumberOfPasses` makes a server that allows one stream and never answers a PING let a deadline cut the reads of 20 passes of each kind of read, each of the two clients' through its own code, and finds at most the bounds above open after each: with no close it finds 4 connections after 4 passes of one read and 17 after 7 passes of seven.
`TestPassesAgainstAServerThatAllowsOneStreamKeepAtMostTheConnectionsOfHonestPasses` runs 40 passes of each client against a server that answers and allows one stream and finds at most the reads in flight and one more over HTTP/2 and twice the reads over HTTP/1.1.
`TestPassesOfAnHonestTenantWithNoFailedReadDialNoConnectionAfterTheFirst` and `TestAReadAnsweredWithAStatusLeavesTheConnectionsOpen` show that passes whose reads succeed or are answered with a Status close nothing, and `TestAnAnswerThatFailsWhileItsBodyComesClosesTheIdleConnectionsOfTheClient` that a stream reset or a body past `maxResponseBytes` closes the idle connections.

Three pools of the standard library and x/net outlive the reads, and the figures do not count them: net/http's and x/net's `gzipPool` and hpack's `bufPool`.
A `sync.Pool` drops an item once two garbage collections have passed since it was put ([pool.go](https://github.com/golang/go/blob/go1.26.10/src/sync/pool.go#L257-L281)).
A gzip reader goes back to its pool reset to an empty reader ([transport.go](https://github.com/golang/go/blob/go1.26.10/src/net/http/transport.go#L3146-L3152), [x/net transport.go](https://github.com/golang/net/blob/v0.60.0/http2/transport.go#L2827-L2833)), which drops the header, the link tables and the buffered reader and keeps the reader's 704 bytes, flate's 4,864 with its lengths, 2,848, and its window of 32 KiB: 41,184 bytes for each read that ended and was not followed by another before two collections.
A buffer of hpack's goes back reset but with its capacity ([hpack.go](https://github.com/golang/net/blob/v0.60.0/http2/hpack/hpack.go#L508-L522)), at most 131,072 bytes, since a Huffman string is refused past 65,856 bytes and a `bytes.Buffer` doubles from 64.
The operator's pods set no requests or limits yet.

## Upgrading Lodia

`helm upgrade`.
The chart upgrades the CRDs first (served versions are only added, never removed, within a major), then the operator.
A tenant renders with the new release once Lifecycle grants it the operator revision: in its window, `fleet.maxConcurrentRollouts` tenants at a time, each keeping its slot until it has rolled out and stayed `Ready` for the settling time, or at once when its owner edits its spec or sets a new `lodia.io/maintain-requested-at` ([the rules](decisions/upgrades-and-maintenance-windows.md#decision)).
A release that breaks tenants stops once as many are `MaintenanceFailed` as the limit, and the next release, a rollback included, reaches them at once.
The release notes list what changes per component.
Rolling back the chart is safe while the CRDs did not add a stored version; to the tenants it is one more revision.
A `ControlPlane` stored with an `extraArgs` flag the new schema refuses keeps that map while it is unchanged, and the next change to the map must drop the flag.

## Support policy

| what | supported |
|---|---|
| Tenant Kubernetes versions | the minors the Kubernetes project maintains, today 1.35, 1.36 and 1.37; a minor leaves Lodia's version table with the first release after it leaves upstream support, and stays renderable for one more release, only as the source of an upgrade; tenants are warned in `Progressing`'s message from 60 days before its end-of-support date, and by [`LodiaKubernetesEndOfSupport`](#lodiakubernetesendofsupport) from 30 days before |
| Management cluster | a maintained minor with the CEL features the CRDs use: 1.35 or newer today |
| Lodia versions | until 1.0, the latest minor gets fixes; from 1.0, the two latest minors |
| API versions | `v1alpha1` may change between minors until `v1beta1`; from `v1beta1`, changes are additive and conversion needs no webhook |
