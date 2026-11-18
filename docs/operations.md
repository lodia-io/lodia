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
| Rotate client identities (yearly, or after a leaked kubeconfig) | phase 3, not built: an operation object starts it ([one-controlplane-per-cluster](decisions/one-controlplane-per-cluster.md)); it completes on its own after one kubelet signing duration (30 days), nodes untouched |
| Rotate the ServiceAccount key | phase 3, not built: start it; when `status.credentials.rotation.serviceAccountKey.phase` is `Prepared` and no client holds an old token, complete it |
| Rotate the etcd encryption key, the etcd CA, the front-proxy CA, the tunnel CA in `gateway` mode | phase 3, not built: start each; each completes on its own |
| Rotate the trust anchor (the cluster CA) | phase 3, not built: start it; when the phase is `Prepared`, add the new bundle (`<cp>-ca-bundle`) to every node's `/etc/kubernetes/pki/ca.crt` and kubelet kubeconfig, then complete it |
| Hibernate and wake | phase 3, not built: `spec.hibernation.enabled: true` / `false`, or `spec.hibernation.schedules` |
| Take a sealed copy | create a `Backup` ([example](api/examples/backup.yaml)); it is `Ready`, `Completed`, once sealed, and `status.snapshots` says where the copy is |
| Replace a cluster from a `Backup` | create a new `ControlPlane` with `spec.restoreFrom.backupRef`; go through the [replace checklist](#replace-a-cluster-from-a-backup), or create the new one `Released` if the source is gone already; nodes join it anew with a new token ([restore-replaces-the-source](decisions/restore-replaces-the-source.md)) |
| Clone a cluster beside its source | through the API with Velero ([recipe](#clone-a-cluster-beside-its-source)), not with `restoreFrom`, which replaces its source |
| Stop Lodia touching a cluster | `spec.paused: true`; everything keeps running, nothing is reconciled but `KubeconfigRequest`s, which write nothing to the `ControlPlane`; deleting a paused `ControlPlane` waits until it is unpaused; in `gateway` mode [the tunnel's client certificate limits how long](#operator-down) |
| Turn the controller manager hold off for a cluster | `spec.health.holdControllerManager: false`; a hold in progress is released at once |
| Change a `DatastoreClass` | edit it; [what the class refuses and what an edit rolls](README.md#a-tenant-namespace); the edit reaches each tenant's `Etcd` in its window, within the fleet limit, or at once on `lodia.io/maintain-requested-at`, and `Progressing` says when; the storage fields, the reclaim policy, the credentials' `secretRef` and the region roll nothing ([dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md#consequences)) |
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
| `DatastoreReady=False`, `DatastoreClassNotFound` | before the `Etcd` exists: the `DatastoreClass` that `spec.datastore.className` names, or that the claims of a previous `Etcd` record, is missing, or no class is annotated `lodia.io/is-default-class: "true"`; the message says which. Deleting a class that running `Etcd`s record never shows here ([dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md)) |
| `DatastoreReady=False`, `DatastoreClassAmbiguous` | before the `Etcd` exists, without `datastore.className`: more than one class is annotated `lodia.io/is-default-class: "true"`; the message names them. Remove the annotation from all but one |
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
| Many tenants turn `Reachable=False` slowly behind one Gateway | a Gateway that does not answer costs a Health worker the 5-second timeout for each tenant, per pass, so a round over `N` tenants takes `N`/20 seconds on the 100 workers of `controllers.health.workers` and the third failed probe comes that much later; tenants on other Gateways wait behind them, as they did behind a hung public probe |
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

Lodia never grows the volume of a running `Etcd`, and a class's `storage.size` reaches only an `Etcd` created without claims; growing a tenant's volume is not built.

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

Phase 3, not built: the operation objects that start and complete a rotation do not exist yet ([one-controlplane-per-cluster](decisions/one-controlplane-per-cluster.md)).

- **A kubeconfig or client certificate leaked:** rotate the client CA and complete it at once; every client certificate Lodia holds is already re-issued, and kubelets whose certificates are still under the old CA re-bootstrap with a new token (`kubeadm token create`, then on the node `kubeadm reset` and `kubeadm join`), or wait for the automatic completion after 30 days if the leak allows it.
- **A tunnel client certificate leaked:** rotate the tunnel CA, which also restarts the sidecars to cut the sessions already open ([staged-credential-rotation](decisions/staged-credential-rotation.md)).
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
A class without a backup store (`NoBackupStore`) and a class whose `deltaSnapshotPeriod` is below a second, which takes no delta snapshots, never fire it.
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

The operator: two replicas; memory grows with the number of tenants it caches (label-scoped, metadata-only for Secrets) and reads, as the synthetic fleet measures ([Scale](#scale)).
Health reads a tenant as JSON, and the tenant's administrator can have any server answer, an APIService naming a Service of their own included ([review checklist](review-checklist.md), item 10).
So what a read holds is fixed by the constants below and not by what a tenant sends:
- no object and no array of an answer reaches `encoding/json`, whose decoding multiplies the bytes it is given (an empty object becomes a struct, an invalid byte in a string three bytes, the key of a skipped field is unquoted and folded all the same): `internal/jsonscan` walks the body, which `json.Valid` has checked, and every value it hands over is a slice of it;
- a time is parsed from its string token with the layout of `metav1.Time` or `metav1.MicroTime`, and not through their `UnmarshalJSON`, which hands any token, an object or an array included, to `encoding/json` ([time.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/apis/meta/v1/time.go#L100-L110)); `TestNothingIsPassedToEncodingJSONButAScalarToken` fails on a call of an `UnmarshalJSON` in the package;
- a scalar is decoded, by `encoding/json` or `strconv`, only when Health keeps the field it fills and the raw size of its token is within that field's bound, checked before the decode; a field Health only compares with a word (a condition's `type` and `status`, a Status's `status`, a `failurePolicy`, a Service's `type`, a PersistentVolume's reclaim policy, a claim's phase) is compared on its raw token, and a string past its bound is none of the words and is neither decoded nor a stop nor a failed read, while a token that is no string fails the read at any size;
- a key is matched on its raw bytes and with its case, as Kubernetes' own decoder reads one ([json.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/runtime/serializer/json/json.go#L271)); a key with an escape is unquoted only up to 256 bytes, and any longer one is skipped without a copy;
- an error names the field and its bound and nothing of the value: a time that does not parse is "not a time", not the text of the parser's error; a failed answer whose body is no Status within these bounds is the error client-go builds for its status code, which quotes up to 2,048 bytes of a text body ([request.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/request.go#L1339-L1363)), with its code, reason and `Retry-After` and a message of Lodia's own, `the answer is no Status Lodia can read`; a request that fails before the head of its answer is read, at a deadline or a reset, is net/http's error with its verb and path and none of its query, which carries the `continue` token of a later page, up to 36,883 bytes (`TestARequestThatFailsBeforeItsAnswerCarriesNoByteOfTheTokenItAsksWith`);
- the body of an answer is cut at 8 MiB, `tenantclient.MaxResponseBytes`, and its status line and headers at 64 KiB, `tenantclient.MaxResponseHeaderBytes`, where `net/http` reads up to 10 MiB of them ([transport.go](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L329-L334)); a kube-apiserver's are under 512 bytes (352 bytes of the biggest of eight answers on envtest of Kubernetes 1.35.0);
- a request is sent once, `MaxRetries(0)`: client-go sends a request again up to ten times on a 429 or a 5xx that carries `Retry-After` ([request.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/request.go#L167)), and the next pass is Health's retry.

The raw bound of a field counts the bytes of a string between its quotes, in which every character may be escaped as `\uXXXX`, six bytes.
A token of the wrong kind, a field that is no string where Health keeps a string or a time that is no string, fails the read at any size, and a failed list of the Nodes leaves the last good one standing.
A string of a field Health keeps, past its bound, stops a list of the Leases or of the Nodes (`TooManyNodes`), makes a kind of a held restore "could not be listed", and fails any other read; a stop inside a page counts the items of that page read before it.
A word field's string past its bound is none of the words, and neither a stop nor a failed read.

| field | raw bound |
|---|---|
| a name, a namespace | 2,048 bytes, 253 decoded: a Lease or a Node with a longer name is not counted, and only a held restore's tally counts such an object, without naming it |
| a `resourceVersion`, a time | 64 bytes; the hold's patch carries the `resourceVersion` it read decoded and `json.Marshal`-escaped ([encode.go](https://github.com/golang/go/blob/go1.26.11/src/encoding/json/encode.go#L205-L216)): up to 192 bytes of invalid UTF-8, or 384 of `<`, `>` and `&`, which escape as six |
| a Status's `reason`, a cause's `reason` | 126 bytes, `RequestEntityTooLarge` with every character escaped, the longest of the 19 reasons that `IsNotFound` and, through `retry.RetryOnConflict`, `IsConflict` compare a reason with ([errors.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/api/errors/errors.go#L47-L67), [util.go](https://github.com/kubernetes/client-go/blob/v0.37.1/util/retry/util.go#L103-L105)), where `NamespaceTerminating`, the cause `apistatus.Refused` looks for, is 120 (`TestEveryReasonAnErrorIsComparedWithIsReadAsItselfWithEveryCharacterEscapedAndMaxReasonRawIsTheLongest`, `TestANamespaceBeingDeletedIsTheCauseRefusedLooksForWhateverTheEscapes`): a longer string makes the answer no Status Lodia can read |
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
- what Health's code bounds, argued exactly from the code and its constants: the body under `tenantclient.MaxResponseBytes` in `io.ReadAll`'s chunks and the slice it returns, what the types keep, the summaries a pass builds and the reads in flight at once;
- the transport's buffers whose size grows with a limit the client sets, with its source line: the head, its names and the trailers that follow it, which are one term;
  each is the most its limit allows but the tables of a map of more than 896 keys, which depend on the map's random seed, so that the term is a count at a stated chance, and no deterministic most of it exists: the directory of a map doubles with each split of keys that share a prefix of their hash, up to its 64 bits, and which keys share one is the seed's;
- a margin of 2 MiB for each connection, for everything else that the standard library and the runtime allocate on the way and no term itemizes: the readers and writers of 4 KiB, TLS's records, the request's writer and the structures of a response.
  It is a margin and no derived term: the measures below confirm that the terms and the margin hold everything they see.

The phases, each term with the source of what runs in it:
- while the body is read, up to 8 MiB:
  - `io.ReadAll`'s chunks and the slice it returns, 20.4 MiB.
    client-go reads the body with `io.ReadAll` ([request.go](https://github.com/kubernetes/client-go/blob/v0.37.1/rest/request.go#L1159)).
    go1.26.11's starts with a chunk of 512 bytes, then 256, and grows each next chunk by half ([io.go](https://github.com/golang/go/blob/go1.26.11/src/io/io.go#L712), [L717](https://github.com/golang/go/blob/go1.26.11/src/io/io.go#L717), [L747](https://github.com/golang/go/blob/go1.26.11/src/io/io.go#L747)): the sizes do not depend on what the reader returns, and the 25th chunk is 2.7 MiB and the 26th 4.1 MiB.
    It closes a chunk once less than a sixteenth of it is free ([L742](https://github.com/golang/go/blob/go1.26.11/src/io/io.go#L742)), so a chunk is closed between 15/16 full and full, and it allocates the slice of the whole body while every chunk is alive ([L734](https://github.com/golang/go/blob/go1.26.11/src/io/io.go#L734)).
    A reader that fills every read closes the 24th chunk at 5.5 MiB of body and holds, at the cut, 5.5 MiB of closed chunks, the open chunk of 2.7 MiB and the slice of 8 MiB: 16.3 MiB.
    A reader that returns each chunk just past 15/16 full, as a body that arrives in pieces does, closes the 25th chunk at 7.75 MiB, before the cut, and opens the 26th: 8.3 MiB of closed chunks, 4.1 MiB open and the slice, 20.4 MiB.
    No chunk closes earlier than that, and the 27th, which opens at 11.6 MiB of body at the soonest, is past the cut, so 20.4 MiB is the most below it.
    `TestIoReadAllHoldsAtMostItsChunksAndTheSliceItReturnsAtTheCut` reads 1 to 8 MiB through the real `io.ReadAll` with each reader, finds the chunks' heap within 256 KiB of the sizes it offered, 16.27 and 20.38 MiB at 8 MiB, and nothing above 20.4 MiB.
    One read of each of the 47 shapes below reaches 20.5 to 20.6 MiB of heap in use in every run.
  - The head, the names it announces and the trailers that follow it, up to 3.5 MiB.
    net/http reads a response's head up to `tenantclient.MaxResponseHeaderBytes` into a map of one slot for each name and a shared slice of strings for the values, `make(MIMEHeader, hint)`, where `hint` is the count of lines in the bytes the reader has buffered, at most 1,000 ([reader.go](https://github.com/golang/go/blob/go1.26.11/src/net/textproto/reader.go#L516-L603), [L623-L645](https://github.com/golang/go/blob/go1.26.11/src/net/textproto/reader.go#L623-L645)).
    The tenant sets the hint by how it writes its first 4,096 bytes, and a line that repeats a key counts in it as a line of a key of its own does, so the argument takes the worst hint, 0 to 1,000, for the map and for the slice of values, whatever the keys the head has.
    `fixTrailer` makes a map of its own for the names a `Trailer` line lists, with no hint, and a copy of each name that is not already as it would be written ([fixTrailer](https://github.com/golang/go/blob/go1.26.11/src/net/http/transfer.go#L767-L806)).
    Each `Trailer` line is a value of its own, a string `string(bytes.TrimLeft(v, " \t"))` of the line ([reader.go L572](https://github.com/golang/go/blob/go1.26.11/src/net/textproto/reader.go#L572)) in the slice of values of the key, and `fixTrailer` reads every one ([transfer.go L786](https://github.com/golang/go/blob/go1.26.11/src/net/http/transfer.go#L786)): the names that are no copies point into these strings.
    A string of more than 32,768 bytes is a large object that takes whole pages of 8 KiB, so a tenant that splits the names at a comma into the lines that take most, 8 bytes of head for each line after the first, pays for the strings of the size classes just past each piece and for a slice of the values.
    The count reads the value on up to eight lines, each string in its size class and the slice that holds them, and takes the most over them: nine to sixteen lines hold no more (`TestMoreTrailerLinesThanTheCountReadsHoldNoMoreThanTheMostItFinds`, which holds the count of each of the two to its bytes).
    `TestTheTrailerValueTheCountSplitsIsTheMostOfEveryLengthOfItsPiecesOnEveryNumberOfLines` holds the most the strings of a value take on up to eight lines to a search over every length of every piece, for each length of the value to 2,500 bytes, a piece of one byte among them, and on two lines for every 211th length to 65,536.
    The trailers of a chunked answer are read once the body has ended, and only when a CRLF CRLF is within the 4,096 bytes of the reader's buffer ([readTrailer](https://github.com/golang/go/blob/go1.26.11/src/net/http/transfer.go#L897-L954)), so a block of lines ended by a line feed, the last by a CRLF, holds at most 77 field lines of 3 bytes and the others of 4 within the 4,093 bytes that precede its last 3: 1,042 field lines.
    They are read into a map of their own, which `mergeSetHeader` copies into the map of the names the head announced ([L956-L962](https://github.com/golang/go/blob/go1.26.11/src/net/http/transfer.go#L956-L962)): one map then holds the names the head announced and the names of the trailers, none of which the head announced in the worst case, beside the map the trailers were read into, its slice of values and the copies of their names.
    While that map splits it holds the table it splits beside the two it makes, and the directory it replaces when the split doubles the directory ([table.go L1179-L1220](https://github.com/golang/go/blob/go1.26.11/src/internal/runtime/maps/table.go#L1179-L1220), [map.go L363-L395](https://github.com/golang/go/blob/go1.26.11/src/internal/runtime/maps/map.go#L363-L395)): the last split of the names' map, the one that is made while the trailers' read is alive, holds a table of 49,184 bytes and the directory it replaces at once, which the count adds at the depth of the map it counts: 128 bytes for a map of 17 to 32 tables and 256 for one of 33 to 64, up to 49,440 bytes in all, which the count's names' maps reach from 21,739 keys at 1e-14 and from 19,891 at 1e-24, up to 22,956 keys (`TestTheTableASplitHoldsIsTheOneOfTheNamesMapTheTrailersAreMergedInto`).
    A head and its trailers are therefore one term, and no bound of the trailers alone is given.
    The head takes its lines from `bufio.Reader.ReadLine`, which ends a line at a line feed with or without a carriage return ([reader.go](https://github.com/golang/go/blob/go1.26.11/src/net/textproto/reader.go#L60-L81), [bufio.go](https://github.com/golang/go/blob/go1.26.11/src/bufio/bufio.go#L405-L438)), so a field line is 3 bytes at the least, a name of one byte, a colon and a line feed, and a name of a field is a token: 77 characters make 77 names of one byte and 5,929 of two ([reader.go](https://github.com/golang/go/blob/go1.26.11/src/net/textproto/reader.go#L686-L712)).
    A name a `Trailer` lists is any bytes between commas: 256 names of one byte and 65,536 of two, and a name costs the head 2 bytes at the least, itself and its comma.
    The argument prices these, and there are fewer: textproto folds the case of a name, so that 77 token characters make 51 keys of one byte and 2,601 of two, and a `Trailer` value refuses control bytes ([reader.go L726-L738](https://github.com/golang/go/blob/go1.26.11/src/net/textproto/reader.go#L726-L738)) and loses the spaces and tabs around a name, so that its names of one byte are 195.
    The count takes the names that cost least and not the names that exist, so it errs on the safe side.
    It prices the shortest head, too, 48 bytes besides the lines and the names, each of which the source needs:
    - `HTTP/1.1 200` and a line feed, 13 bytes: `ReadResponse` cuts the line at the space after the protocol and takes the code from the three bytes before the next space, if there is one, so that no reason is needed ([response.go L168-L183](https://github.com/golang/go/blob/go1.26.11/src/net/http/response.go#L168-L183)), and a line feed with no carriage return ends a line ([reader.go L60-L81](https://github.com/golang/go/blob/go1.26.11/src/net/textproto/reader.go#L60-L81));
    - `Transfer-Encoding:chunked` and a line feed, 26 bytes: trailers are read only of a chunked answer, which takes one `Transfer-Encoding` value, `chunked`, of a head of HTTP/1.1 ([transfer.go L631-L656](https://github.com/golang/go/blob/go1.26.11/src/net/http/transfer.go#L631-L656), [L545](https://github.com/golang/go/blob/go1.26.11/src/net/http/transfer.go#L545), [fixTrailer](https://github.com/golang/go/blob/go1.26.11/src/net/http/transfer.go#L767-L806)), and a field line needs no space after its colon;
    - `Trailer:`, 8 bytes, whose value the names follow ([fixTrailer](https://github.com/golang/go/blob/go1.26.11/src/net/http/transfer.go#L767-L806)), each name with its comma but the last, since `foreachHeaderElement` takes the elements between commas and drops empty ones ([server.go L1531-L1545](https://github.com/golang/go/blob/go1.26.11/src/net/http/server.go#L1531-L1545));
    - a line feed that ends the `Trailer` line and one that ends the head, 2 bytes ([reader.go L546-L549](https://github.com/golang/go/blob/go1.26.11/src/net/textproto/reader.go#L546-L549)); and no `Content-Type`, so that the header map has two keys besides its lines.
    Those are 13 + 26 + 8 + 2 = 49 bytes, and the count prices a comma after every name, which the last needs none of: 48 bytes besides the lines and the names, as `headFixed` takes them.
    Round 14's head, `HTTP/1.1 200 ` with a space after the code, parses alike and is a byte longer, and so is a `Trailer:A,B,` with a comma after the last name.
    The most a map of `k` keys takes, by go1.26.11's `internal/runtime/maps` and the allocator's size classes:
    - A slot is a key of 16 bytes and an element of 24, and a group 8 slots and a control word of 8 bytes, 328 bytes ([group.go](https://github.com/golang/go/blob/go1.26.11/src/internal/runtime/maps/group.go#L124-L126)); a map of 8 keys or fewer is one group, 352 bytes, the size class that holds it.
    - A table holds 7/8 of its slots and doubles when it is full, at 8, 14, 28, 56, 112, 224 and 448 keys, up to 1,024 slots ([table.go](https://github.com/golang/go/blob/go1.26.11/src/internal/runtime/maps/table.go#L19), [L115-L135](https://github.com/golang/go/blob/go1.26.11/src/internal/runtime/maps/table.go#L115-L135), [L1145-L1170](https://github.com/golang/go/blob/go1.26.11/src/internal/runtime/maps/table.go#L1145-L1170)).
      A table takes the size class of its groups ([sizeclasses.go](https://github.com/golang/go/blob/go1.26.11/src/internal/runtime/gc/sizeclasses.go#L98)): 704, 1,408, 2,688, 5,376, 10,880 and 21,760 bytes for 16 to 512 slots, and 49,152 for 1,024, whose 41,984 bytes are a large object that takes whole pages of 8 KiB ([mcache.go](https://github.com/golang/go/blob/go1.26.11/src/runtime/mcache.go#L246-L249)).
      With its structure of 32 bytes and a directory entry, a map of 449 keys takes 49,240 bytes, 109.7 a key, the dearest a key a single table takes.
    - A table of 1,024 slots splits at its 897th key into two of 1,024 ([table.go](https://github.com/golang/go/blob/go1.26.11/src/internal/runtime/maps/table.go#L1179-L1225)): a map of 897 keys takes 98,432 bytes, 109.7 a key again.
    - Which table a key goes to is the hash's, and a map's seed is random for each map ([map.go](https://github.com/golang/go/blob/go1.26.11/src/internal/runtime/maps/map.go#L269)), so the tenant cannot place its keys and the tables of a map of more than 896 keys are a random number.
      The map only grows, so a table with the keys of a prefix of `j` bits of the hash has split exactly when 897 keys or more have that prefix, and the tables are the leaves of that tree.
      The `k` keys fall on the 2^j prefixes of a depth as one multinomial, whose counts are negatively associated, so that the chance that `m + 1` given prefixes each have 897 keys or more is at most the product of their chances, `q^(m+1)`, with `q` the exact tail of one prefix, and the chance that any `m + 1` of the 2^j prefixes do is at most the union bound `C(2^j, m + 1) q^(m+1)`.
      The count takes for each depth the least `m` for which that bound is at most the chance stated, 1e-24 for the term, and at most `k / 897`.
      A map has one table more than those it has split, 49,152 bytes with its structure of 32, and its directory is 8 bytes for each entry of its depth.
      The chance that a map has more tables than counted at some depth is at most 1e-24 for each depth, 5e-24 for a map of up to 22,956 keys, whose depths past the fifth have no chance above 1e-80, and what it costs is one table, 0.047 MiB.
    - A map made with a hint of more than 8 starts with `hint * 8 / 7` slots, in tables of at most 1,024 ([map.go](https://github.com/golang/go/blob/go1.26.11/src/internal/runtime/maps/map.go#L264-L330)): a hint of 1,000, which a head of 1,000 lines or more gets, makes two tables of 1,024 slots, 98,432 bytes, for a head of a few keys, and a hint of 897 or more starts with two tables.
    `TestAMapOfHeadersHoldsAtMostWhatItsTablesTakeAtEverySplitPointAndOneKeyOnEachSideOfIt` finds every key count at which the bytes counted change, with no hint and with the hint of the keys it gets, the empty map and the maps of one and of eight keys among them.
    Under 897 keys the tables are no random number, and it holds the bytes counted exactly, by the bytes a map allocates, the least of 5 runs: with the hint of its keys what its tables take, with no hint that and the tables it grew through.
    From 897 keys it reads nine maps of that many keys and finds the median of their heap at most the bytes counted, within 2 KiB: the count is a bound, and a map's tables vary with its seed.
    The 2 KiB is room for what the heap in use varies by between two reads in a process with no new map in it: in 1,200 pairs of reads, with the race detector and without, the second was level with the first in 42%, 16 to 784 bytes above it in 56% and about 7,000 bytes below it in 2%, so that the median of nine moves only when five of its trials take such a step at once, while a table of 1,024 slots takes 49,152.
    `TestTheSizeClassesTheMapsAreBuiltFromAreTheAllocators` holds the size classes the sizes are rounded to by allocating each of them and one byte either side.
    The rest of a head's bytes are the keys' copies, up to 16 bytes each, an element of the slice of values, 16 bytes, which `make([]string, hint)` allocates once for the first `hint` lines and a line allocates after them, and the strings of the `Trailer` value in their size classes.
    `TestTheLeastAnHTTP1FieldLineAddsToAHeadIsItsCopiesAndAnElementOfTheSliceSinceTheMapGrowsInSteps` holds the 48 bytes of the key, the value and the slice's element: a head of 150 to 158 field lines of 16 bytes grows by 48 at the least with each line, the map's tables being allocated in steps that no one line shows.
    `TestTheSizesTheTermsAreBuiltFromAreTheSourcesOnThisPlatform` holds the copy of a name and of a value, 16 bytes each, and an element of the slice of values, 16, by allocating each alone, the least of 5 runs of 100 to 10,000 rounds, which allocations by other goroutines can only add to.
    Every test of this derivation that counts allocations or reads the heap runs in a `go test` of its own, a step of `make test` that sets `LODIA_HEALTH_TEST_WITHOUT_ENVTEST`, under the race detector like the package's other tests: `TestMain` then starts no envtest and no manager, the step runs those tests alone with `-run`, and the package's main run leaves them out with `-skip`.
    The step fails unless each of them printed its `--- PASS:` line there, read from `go test -v` beside its exit status, since a test binary that exits before its tests, or a guard that skips them all, prints `ok`.
    Outside the step each of them skips and names the step, so that a plain `go test` of the package passes without them and says why (`TestTheStepWithoutEnvtestRunsExactlyTheTestsThatSkipOutsideItAndTheMainRunLeavesThemOut` reads the Makefile and the source and finds the list the step and the main run share, and the tests that skip outside the step, the same).
    They are 28: 25 count allocations or read the heap, and three hold the quiet of the process they share and the guard that waits for it.
    The suite's process has the manager's goroutines, 42 of them, which allocate up to 3,400 times in 20 ms when the binary starts, so that what a test counts there is not its own.
    `TestATestOfTheStepWithoutEnvtestRunsBesideNoGoroutineOfTheSuite` finds the step's process beside 1 goroutine, with the race detector and without it, and `TestATallyBuildsNoNameForAnObjectThatDoesNotQualify` 6 allocations counting the names in ascending order and 8 in descending under the race detector, 6 and 6 without it.
    The tests share that process one after another, so that each body starts only when the goroutines of the earlier tests have ended (`TestAStepStartsABodyOnlyWhenTheGoroutinesOfEarlierTestsHaveEnded` holds the wait and `TestAGuardSkipsOutsideTheStepAndFailsInItWhenAnEarlierTestLeftGoroutinesBeside` the guard that skips outside the step and fails inside it when one is left), and the tests that open a tenant client close its idle connections when they end.
    `allocatedBy` reads at one proc, as `testing.AllocsPerRun` does ([allocs.go L24](https://github.com/golang/go/blob/go1.26.11/src/testing/allocs.go#L24)): at more, the runtime's own mark workers can take a `sudog` of 112 bytes from the heap between its two readings ([mgc.go L1018](https://github.com/golang/go/blob/go1.26.11/src/runtime/mgc.go#L1018), [proc.go L488](https://github.com/golang/go/blob/go1.26.11/src/runtime/proc.go#L488)), which a build of 75 KB or more reads as a byte too many a round.
    At one proc only the build allocates between the readings, and every run reads alike: `ReadMemStats` flushes every P's cache before it reads ([mstats.go L382](https://github.com/golang/go/blob/go1.26.11/src/runtime/mstats.go#L382)), which clears the tiny allocator's block ([mcache.go L326-L328](https://github.com/golang/go/blob/go1.26.11/src/runtime/mcache.go#L326-L328)), and in 224,320 readings none differed within a call; the least of 5 runs is a margin for an allocation of the runtime's own between two readings, which none of those readings showed (`TestAllocatedByRunsItsBuildsAtOneProcAndRestoresTheProcsAfterIt`).
    The trailers' own read is its map at the hint of 1,000, its slice of values, and a copy of each of the names: `TestABlockOfTrailersOfNamesNoAllocatorSharesIsReadInAllocationsEqualToItsParts` reads blocks of 292 and of 1,100 names of 12 bytes through `textproto` and finds the bytes it allocates equal to those parts, the least of 5 runs: 31,384 bytes for 292 names, a map of 21,848, a slice of 4,864 and copies of 4,672, and for 1,100, which has the reader's hint of 1,000 and 100 names past it, the map of two tables, the slice of 1,000 and 100 values more, and the copies.
    Each part is seen at its own size, so a part left out of the sum fails the test.
    The names are of 12 bytes so that each copy takes a block of 16 bytes of its own, in both builds: the tiny allocator packs objects of less than 16 bytes into blocks of 16 ([malloc.go](https://github.com/golang/go/blob/go1.26.11/src/runtime/malloc.go#L1286)), and a name of one byte is no copy at all, since the runtime keeps one string for each byte ([string.go](https://github.com/golang/go/blob/go1.26.11/src/runtime/string.go#L159)).
    The 1,036 names of a block that fills the 4,093 bytes are 51 of one byte and 985 of two, which take 1,979 bytes where the term counts 16 for each of its 1,042 names, 16,672, and the race detector turns the packing off ([malloc.go](https://github.com/golang/go/blob/go1.26.11/src/runtime/malloc.go#L1340-L1345)), so that the 985 take 15,760 and the same read allocates 13,792 bytes more under it: the term counts the larger.
    `TestABlockOfTrailersIsMergedIntoAMapOfOneNameInAllocationsEqualToItsParts` merges the 292 names into a map of one name as `mergeSetHeader` does and finds the bytes it allocates, 43,016, equal to the tables the map grows through, 704 to 21,760 bytes, with their structures.
    The term is the count of the head's parts, which the tests hold, and room to spare.
    The count itemizes, for any head of up to 64 KiB, what its names, field lines and `Trailer` lines leave of it, at the most over every number of names, field lines, value lines and keys of the block of trailers:
    - the header map, its tables and directory, two keys more than its lines, and its slice of values, at the worst hint;
    - the map of the names the head announced and the trailers carry, its tables and directory, and the table its last split holds besides the two it makes with the directory it replaces;
    - the copies of the names, 16 bytes each;
    - the strings of the `Trailer` value, each in its size class, and the slice of them;
    - the trailers' own read, its map, its slice of values and the copies of its names;
    - the slices that the keys repeated by lines keep, in the head and in the block of trailers.
    The tables of each map are counted at the chance stated, 1e-24 for each depth: the seed of a map is random, so the number of tables a map of more than 896 keys has is a chance, and the count takes the least number that the chance of more is at most 1e-24 for, by the union bound above.
    An object of more than 512 bytes that holds pointers takes a header of 8 bytes before the allocator picks its size class ([mallocgcSmallScanHeader](https://github.com/golang/go/blob/go1.26.11/src/runtime/malloc.go#L1594-L1622), [MinSizeForMallocHeader](https://github.com/golang/go/blob/go1.26.11/src/internal/runtime/gc/malloc.go#L47), [roundupsize](https://github.com/golang/go/blob/go1.26.11/src/runtime/msize.go#L16-L36)), and each part of the count is priced so:
    - the slice of values, in the header map and in the trailers' read, is such an object: `make([]string, 896)` takes 16,384 bytes, where the 14,336 of its elements fill a size class and the header takes the next, so that the worst hint is 896;
    - the slice that a repeated key keeps is such an object past 512 bytes: 32 repeats make 33 values, which `append` has grown into a slice of capacity 71, 1,136 bytes of elements in an object of 1,152;
    - a table of groups is such an object, and it takes the size class that it takes without the header, for each of its sizes of 16 to 1,024 slots, so that the tables above stand;
    - the directory, 256 bytes at the most, the slice that holds the `Trailer` value's strings, 128 at the most, and the element of a key past the hint, 16, are not past 512, and the copies and the strings of the value hold no pointer.
    `TestASliceOfStringsTakesTheSizeClassOfItsBytesAndTheAllocatorsHeaderPastFiveHundredAndTwelve` allocates slices of strings at every size class and one string either side of it and finds the bytes the allocator takes equal to the count's, `TestASliceOfPointersTakesTheSizeClassOfItsBytesAndTheAllocatorsHeaderOfEightBytes` does so with slices of pointers, whose elements of 8 bytes tell a header of 8 from one of 16, `TestATableOfGroupsTakesTheSameSizeClassWithTheHeaderAndWithoutIt` holds the tables, and `TestTheWorstHintIsTheDearestOfEveryHintATenantCanSetByTheBytesTheAllocatorTakesForTheSlice` finds the worst hint by the bytes of the real `make` at the first and the last hint of each size class, and not by the function the count uses.
    A line that repeats a key appends a value to that key's slice: textproto gives a key's first value a slot of the shared slice, with a capacity of one, and appends every later one ([reader.go L588-L600](https://github.com/golang/go/blob/go1.26.11/src/net/textproto/reader.go#L588-L600)), so the key keeps the slice `append` grew ([growslice](https://github.com/golang/go/blob/go1.26.11/src/runtime/slice.go#L178), [nextslicecap](https://github.com/golang/go/blob/go1.26.11/src/runtime/slice.go#L326-L356): the capacity doubles below 256 and grows by a quarter and 192 elements past it), in the size class of its bytes with the header.
    A key repeated once keeps 32 bytes, twice 64, 32 times 1,152 and 1,023 times 24,576, where 16 bytes a value are 528 and 16,384: a line of 3 bytes buys up to 36 bytes of slice, 12 for each byte of head, where a line of a key of its own, 4 bytes, buys 32 and, at the steps of its map's tables, a table of 49,184 bytes.
    The count charges those tables only at their steps, so that between two steps a line that repeats buys more, and the count reads those shapes and does not argue them away.
    `TestAKeyRepeatedByLinesKeepsTheSliceAppendGrewToInTheBytesTheAllocatorTakesForIt` reads 1 to 1,400 repeats of a key through textproto, and up to 21,845, and finds the slice each keeps in the bytes the count holds, and each growth of it in the bytes the allocator takes.
    The block of trailers is read for every number of keys of its own, 1 to 1,042, the first 77 of one byte, 3 bytes a line, the others of two, 4 bytes, with the bytes they leave spent on lines that repeat them, each key repeated as often as is best, a choice held to a search over every number of repeats of each key (`TestTheMostRepeatedLinesBuyForTheKeysOfABlockIsTheBestChoiceOfHowOftenToRepeatEachOfThem`), a block repeating fewer keys than the 77 of one byte (`TestNoBlockRepeatsAsManyKeysAsThereAreKeysOfOneByteSoTheCostOfTheLastOfThemIsNone`).
    Its lines are of 4 bytes at the most, so that its 4,093 bytes hold 1,022 of them at the least and the reader's hint is 1,000 whatever the keys it has: the trailers' own read is its map at that hint, its slice of values and the copies of its keys (`TestABlockThatRepeatsTheKeysTheCountPicksKeepsTheSlicesTheCountAddsAndFillsItsBytesWithMoreThanAThousandLines` reads blocks through textproto and finds the slices of their repeated keys in the bytes the count adds).
    The head's lines that repeat a key are read to the head's 64 KiB, one-byte keys at 3 bytes a line and two-byte keys at 4, for every number of names and of lines of the rest, and the head the search finds equals the head found by every number of repeated bytes, at the same names and repeated bytes where lines that repeat keys win (`TestTheDearestHeadByTheBoundsOfItsRepeatedBytesIsTheDearestByEveryChoiceOfThem`), and what they buy is held to a second search by steps to 12,000 bytes, where the head repeats keys of all 77 of one byte (`TestAHeadThatRepeatsKeysBuysTheSameAsAnotherSearchByStepsWhenItSpendsEnoughBytesToUseEveryKeyOfOneByte`).
    `TestTheTailOfABinomialIsTheExactSumTheArgumentQuotes` holds the tail of one prefix to the exact sums, `TestTheCountOfPrefixesThatHoldAMapIsTheUnionBoundNotTheBinomialQuantile` the count to the union bound, one more than the quantile of a binomial at 16 prefixes of a chance of 0.1, and `TestTheDearestHeadIsTheMixTheArgumentNamesAndItsPartsAreTheOnesItQuotes` and `TestTheTermHoldsTheCountAtAChanceOfOneInAMillionAndAtOneInASeptillion` the parts of the count to their bytes at three chances.
    The count of these parts is 2.80 MiB at 1e-24 a depth, which `TestTheTermOfTheHeadAndItsTrailersIsTheCountAtOneInASeptillionADepthAndRoomToSpare` holds to that hundredth of a MiB, and to the term less a slack of 0.6 MiB.
    The term is 0.7 MiB over it.
    That is slack, room for the parts and the shapes of head that the count may miss, and no part of the count.
    What a head's read allocates besides the itemized parts is not itemized and falls under the margin of 2 MiB for each connection, as everything else that no term itemizes does.
    The count is a bound and not what a tenant reaches, since it takes the cheapest names and every table that can split.
    The heaviest heads the tests read through Health's client are held at 2.2 MiB at the most, each under the term:
    - `TestTheMostAHeadOfDistinctNamesAndFieldLinesWithItsTrailersHoldsIsFoundByScanningItsAnnouncedNames` reads the heads of distinct canonical names, from 0 announced names to the most a head carries, 21,865, in steps of 200, 110 heads, the rest of each head field lines, each followed by a block of 1,036 trailers none of whose names it announced, and finds each held at most its parts and the parts at most the term;
      the most held in 50 runs without the race detector was 2.053 to 2.061 MiB, at 13,800 to 15,200 announced names, on a plateau of about 2.03 MiB from 14,200 to 16,400 names, and 2.165 to 2.176 in 40 runs under it, which is 0.74 MiB under the count without the race detector and 0.63 MiB under it with the race detector;
    - `TestTheHeadTheScanFindsMostWithItsTrailersIsHeldAtMostItsPartsAndTheTerm` reads the head of 14,600 names and the 4,899 lines the rest of its 64 KiB buys from that plateau, which holds 2.035 MiB, 2.147 under the race detector, within 0.1 MiB of the most either way, and the gated measure of a cut answer reads the same head;
    - `TestAnHTTP1HeadWhoseBlockOfTrailersRepeatsKeysIsHeldAtMostItsPartsAndHeadAndTrailersHTTP1` reads a head of announced names and field lines that fills its 64 KiB, with a block of 200 trailers of which 30 are repeated 32 times, 1,160 lines, whose 30 slices of 1,152 bytes the response keeps, and finds it held at 1.32 to 1.37 MiB in 20 runs and 1.40 to 1.45 in 10 under the race detector, at most its parts;
    - `TestAnHTTP1HeadWhoseTrailerValueIsOnSeveralLinesIsHeldAtMostItsPartsAndHeadAndTrailersHTTP1` reads a head of announced names and field lines that fills its 64 KiB with the names on 5 `Trailer` lines, then the full block of 1,036 trailers, and finds it held at 1.39 to 1.48 MiB in 20 runs and 1.49 to 1.55 in 10 under the race detector, at most its parts;
    - `TestAnHTTP1HeadThatAnnouncesTheNamesOfTheDearestCountWithAFullBlockOfTrailersNoneOfThemAnnouncedIsHeldAtMostHeadAndTrailersHTTP1` reads the same on one `Trailer` line and finds it held at 1.34 to 1.45 MiB in 20 runs and 1.49 to 1.58 in 11 under the race detector, at most its parts;
    - `TestAnHTTP1HeadOfFieldLinesEndingInALineFeedIsHeldAtMostHeadAndTrailersHTTP1` reads a head of 65,534 bytes of 13,634 field lines ended by a line feed, 13,636 keys, held at 1.00 to 1.09 MiB in 20 runs and 1.17 to 1.22 in 11 under the race detector, and `TestAnHTTP1HeadOfNamesAndFieldLinesTogetherWithTheTrailerValueIsHeldAtMostHeadAndTrailersHTTP1` one of 51 field lines of one byte and a `Trailer` of 195 names of one byte and 21,636 of two, held at 1.58 MiB and 1.63 under it, each at most its parts.
  - No decompressor.
    The tenant clients set `DisableCompression`, so that net/http asks for no gzip and decodes none ([transport.go](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L2845-L2862)), and no gzip reader, which net/http's pool keeps past the read, is made.
    `TestTheTenantClientsAskForNoCompression` finds no `Accept-Encoding` in a pass of either client, and `TestAGzipAnswerSentUnaskedIsAFailedReadThatKeepsNothingOfItsResponse` sends a gzip answer nobody asked for, a Lease and one of 9 MiB, and finds each read fail, as no JSON or at the cut, and its response unreachable after a collection.
  - The connection of each read in flight, and the margin.
    A read in flight has a connection of its own, and nothing of net/http's reader and writer of 4 KiB each ([transport.go](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L1991)) is itemized: they are in the margin of 2 MiB.
    The tenant clients speak HTTP/1.1 only: `tenantclient.New` leaves `h2` out of `NextProtos`, and client-go configures HTTP/2 only for a transport whose `NextProtos` allows it ([http.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/net/http.go#L196-L208), [L131-L144](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/net/http.go#L131-L144)).
    `TestTheTenantClientsSpeakHTTP1EvenWhenTheServerOffersHTTP2First` reads with both clients from a TLS server that offers `h2` and finds every request HTTP/1.1.
    Each client's transport has `MaxConnsPerHost` equal to the most reads its pass has in flight at once: 1 for `health-nodes`, whose two lists go one after the other, and 7 for `health`, the lists of a held restore.
    net/http counts every connection of a host, dialing, in use and idle, and blocks a dial past the cap until a connection closes or is given back ([transport.go](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L214-L219), [L1594-L1621](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L1594-L1621), [L1663-L1710](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L1663-L1710)), so a client has at most that many connections to its host open at any time, whatever its server does.
    The cap counts each host on its own, so that a client that followed a redirect would have a cap for every host the tenant names, 17 connections for `health`'s cap of 7 against five hosts, and would send Health's client certificate to each.
    The clients follow no redirect: `CheckRedirect` is `noRedirects`, as the prober's is, and the redirected read is a failed read that names no byte of the `Location`.
    `TestTheTenantClientsFollowNoRedirectAndNameNoByteOfItsLocation` has a server redirect every read to five other hosts and finds one request to the server, none to the others, and no more connections across the six than the cap.
    `TestTheReadPastTheCapWaitsForAConnectionToBeGivenBackAndFailsAtItsDeadlineWhenNoneIs` holds the cap's reads in flight and finds the next read, the 3rd and the 8th, wait without reaching the server, fail at its deadline when no connection comes back, and take the connection given back when one does.
    A pass leaves no connection open when the server closes it after every answer or when its deadline cuts its reads, with the cap or without.
    net/http ends the connection of an answer with `Connection: close` in the read loop, which exits once the answer is read ([transport.go](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L2373-L2381), [L2291-L2294](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L2291-L2294)), and that of a read the request's end cuts before its answer in `persistConn.roundTrip`, which calls `cancelRequest`, and `closeLocked` counts the connection down and closes it ([L2954-L2963](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L2954-L2963), [L2206-L2211](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L2206-L2211), [L2997-L3014](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L2997-L3014)); the read loop then exits on the closed connection.
    `TestAServerThatClosesAfterEveryAnswerLeavesNoConnectionOpenAfterAPass` and `TestAServerThatNeverAnswersLeavesNoConnectionOpenAfterAPassItsDeadlineCut` run 40 passes of each client against each and find none open after each, and every read answered by the first.
    The cap is held by the tests that make a read wait on it.
    An honest pass waits on the cap for no connection of its own:
    - the reads of a pass in flight at once are at most the cap.
      `health-nodes` reads the controller manager's Lease, the two lists and the Lease again for its write one after the other; `health` runs the seven lists at once or its two leader Leases one after the other.
      A list reads its pages one after another: `listPages` asks for the next after it has read the last, and does not use `pager.ListPager`, which fetches the next while it hands the last over.
      The reconcilers run controller-runtime v0.25.2's priority queue, which neither sets otherwise ([controller.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/controller/controller.go#L279-L286)), and it hands a key out once until `Done`: its `locked` set holds the keys handed out, skips them in `Get` and drops them in `Done` ([priorityqueue.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/controller/priorityqueue/priorityqueue.go#L150-L152), [L406-L412](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/controller/priorityqueue/priorityqueue.go#L406-L412), [L492-L495](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/controller/priorityqueue/priorityqueue.go#L492-L495)), so a reconciler has one request of a control plane at a time, and each reconciler has clients of its own.
      `TestAPassNeedsNoMoreConnectionsThanItsCapWhenTheCapIsLifted` lifts the cap and finds 1 and 7 reads in flight at most and as many sockets open, 40 passes of each client, and `TestAPassAgainstAKubeAPIServerFailsNoReadUnderTheCap` reads 40 passes of each client from a real kube-apiserver under the cap and fails none.
    - a read's connection is given back before the reader sees the end of its body.
      The read loop puts the connection in the idle pool, and only then lets the body's end return ([transport.go](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L2316-L2320), [L2417-L2430](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L2417-L2430), [L2455-L2464](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L2455-L2464)); a request that waits is handed a connection as it is put back ([L1097-L1113](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L1097-L1113)).
      So the next page or the next read of a pass takes the connection the read before it gave back: `TestThePagesOfAListTakeTheConnectionsOfThePagesBeforeThem` finds, over the six pages of the two lists, 1 dial and 5 reused connections, and a second pass over a held restore reusing 1 and dialing 6.
    - a wait can still come where the count holds a connection that no read uses: a connection that is going, or a dial that is still in flight.
      An answer with `Connection: close` ends its connection in the read loop after the reader has seen the end of the body, and net/http counts it down when the loop closes it ([transport.go](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L2293-L2294), [L2997-L3014](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L2997-L3014)); a server that closes a connection it kept idle does the same once the client notices.
      A read that needs a connection then waits until the count is handed on ([L1677-L1695](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L1677-L1695)), which the read loop does as it exits, and at the latest until its own deadline, since `getConn` returns once the request's context ends ([L1583-L1589](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L1583-L1589)).
      net/http hands a connection that comes back to a read that is still dialing and lets that dial go on, counted until it ends, when its connection goes to the first read that waits for one ([L1097-L1099](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L1097-L1099), [L1639-L1659](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L1639-L1659)): a read can find the cap reached by the dial of another read and wait for it.
      `tenantclient.Options.dialContext` bounds a dial by `Timeout`, as the prober's `dial` does, since net/http dials on a context detached from the request's ([L1524-L1529](https://github.com/golang/go/blob/go1.26.11/src/net/http/transport.go#L1524-L1529)), where client-go's dialer lets it run 30 s ([cache.go](https://github.com/kubernetes/client-go/blob/v0.37.1/transport/cache.go#L122)), and the transport's `TLSHandshakeTimeout`, which client-go sets to 10 s ([cache.go](https://github.com/kubernetes/client-go/blob/v0.37.1/transport/cache.go#L149)), is `Timeout` too: a place a dial holds is held for its connect and its handshake, each within `Timeout`, two Timeouts at most.
      A read that waits for a place can therefore fail at its deadline behind a dial that is slow, whether the dial ends in a connection or not, where a dial of its own would have been quick; and after a dial that fails it dials only then.
      `TestADialHeldPastTimeoutGivesItsPlaceBackAtTimeoutAndAReadBehindItDialsAfterwards` and `TestAHandshakeHeldPastTimeoutGivesItsPlaceBackAtTimeoutAndAReadBehindItDialsAfterwards` hold the cap's dials, and its handshakes with a server that never answers, past a `Timeout` of 500 ms, and find a read behind them fail at its deadline while they are held and begin its dial once they end at `Timeout`, not before; `TestTheTenantClientsBoundTheirHandshakeByTheTimeoutOfTheirReconciler` finds each reconciler give its client its own `Timeout`.
      `TestAReadPastTheCapWaitsForADialWhoseReadTookAnotherConnectionAndTakesTheConnectionOfThatDial` holds a dial whose read took another connection and finds the read past the cap fail at its deadline while the dial is held, and reach the server on the dial's connection when it ends, with no more connections open than the cap.
      Measured against a real kube-apiserver with `MaxConnsPerHost` set to 0, 40 passes of `health` had at most 7 sockets open in each of 40 runs without the race detector and in 38 of 40 under it, and 8 and 10 in the other two, beside a burner of 8 threads at a load average of 38 and 31: a read of an honest tenant can wait on the cap for a slow dial, and never for a connection that another read holds.
    Each connection takes the margin of 2 MiB once, and a client has at most its cap of them: the connections of a pass are those of its reads, and a tenant at rest has one of each client idle, never both.
    A client keeps one connection idle between passes, `MaxIdleConnsPerHost` of 1, `tenantclient.IdleConnections`, and closes the others as they are given back, for 90 s at most, client-go's `IdleConnTimeout` ([http.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/net/http.go#L123-L124)): every pass of a tenant at rest reuses it, a held restore's pass dials six, and `TestAPassLeavesOneConnectionIdleWhateverItHadInFlight` finds one connection open after each pass of either client.
    The fleet measures what an idle connection holds, 46 to 48 KiB of live heap and stacks, and what a handshake more costs, 0.6 ms of CPU ([Scale](#scale)).
    A kube-apiserver's `--goaway-chance`, which Lodia sets to 0.001 when it runs more than one replica (`internal/controller/workloads/render.go`), sets `Connection: close` on an HTTP/2 request only ([goaway.go](https://github.com/kubernetes/apiserver/blob/v0.37.1/pkg/server/filters/goaway.go#L67)), so it closes no connection of Health's.
    Over HTTP/1.1 a connection stays with the replica of a highly available tenant that it reached, through the Service, until it closes: when it has been idle for 90 s ([http.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/net/http.go#L123-L124)), when the replica ends it or when a read on it fails.
    A read on such a connection reads what a read on any replica would: Health's reads ask for no `resourceVersion`, and a Get or a List with none is a most recent read, consistent whichever replica serves it ([api-concepts](https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions); a Get with none is served from etcd, [delegator.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/cacher/delegator.go#L109-L114)).
    What the connections change is where the reads go: to the replicas they reached when they were dialed, which `--goaway-chance` does not move, as it moves an HTTP/2 client such as a kubelet ([README](README.md)).
- after it, the body, 8 MiB, and what the types keep, 2 MiB.
  The most is 1.85 MiB, 1.83 measured and 1.90 under the race detector, a page of 100 Nodes of 64 Ready conditions with every string at its raw bound and invalid UTF-8: a decoded string is cut to a copy of its kept length, which `TestACutStringHoldsNoneOfTheStringItWasCutFrom` holds, so a name takes 256 bytes, a kubelet version 80, a condition's status 224 and its type 8, and the conditions' slice 4 KiB a Node.
  The other kinds keep at most 0.4 MiB, and a failed answer's Status, a message of 1,024 raw bytes, a reason and 64 causes of 126, 0.03 MiB (`TestWhatTheTypesKeepAtTheRawBoundsIsUnderKeptAtMost`).

So a read holds 25.9 MiB while its body is read (20.4 + 3.5, then the margin of 2) and 10 MiB after: 25.9 MiB.
The memory of a pass follows from the reads it has in flight at once, each with a connection of its own, 25.9 MiB, of which the margin's 2:
- a pass of `health-nodes` reads the controller manager's Lease, then lists the Leases and then the Nodes, then sends the hold's write, whose patch carries the read's `resourceVersion`, 384 bytes at most, and reads its answer: one read at a time, 25.9 MiB, and the summaries the pass builds beside the sampling, up to `health.maxNodes` Lease versions and Node summaries, 4 MiB (3.72 MiB measured, with names of 253 bytes: a Lease version takes up to 112 bytes of map slot, its name of 256 and 24 bytes of the slice it is built from, a Node summary 72 bytes, 256 and 80, 3.8 MiB for 5,000 of each; `TestWhatAPassBuildsBesideTheSamplingIsUnderSummariesAtMost`): 29.9 MiB;
- a pass of `health` over a held restore lists seven kinds at once, each with a head and a connection of its own: seven reads, 181.3 MiB, and at most 10 such passes list at once, whatever the workers, 1.78 GiB;
- any other pass of `health` first runs its probes at once, three heads of at most 3.5 MiB, the probes' transports cutting them at 64 KiB as the clients' do, and five connections, the tunnel's two TLS sessions counted as two, 20.5 MiB, and then reads its two leader Leases one after the other, one read, 25.9 MiB: 25.9 MiB;
- a pass of PKI sends its requests one after another, so it holds one read at a time, 25.9 MiB; a pass of Tenant does too and holds beside its read the inventory it decodes, whose text and the copy of it that `json.Valid` takes are 1 MiB each, `maxInventoryRaw`, and whose entries, up to 256 of five names of 253 bytes and the copies of the list a pass makes, are 0.4 MiB: 28.3 MiB; their 10 workers each by default hold 20 passes at once, 0.53 GiB, beside Health's below;
- the 100 workers of `health-nodes`, by default, hold 100 passes at once, 2.92 GiB at most, and the 100 workers of `health` 10 held restores and 90 other passes, 4.05 GiB: 6.97 GiB in all; `controllers.<name>.workers` scales each, by 29.9 MiB and 25.9 MiB a worker.

At the default `GOGC` the heap in use may grow to about twice the live heap; the operator's memory limit and `GOMEMLIMIT` are in [Scale](#scale).

Five tests measure the live heap and the heap in use at `GOGC=5`, so that the heap in use stays near the live heap, and a sixth the bytes of a page of Nodes; they run only when `LODIA_MEASURE_HEAP` is set.
They answer over TLS and HTTP/1.1, as the tenant clients speak it, so that the records of TLS and its buffers are in what they measure and the margin of 2 MiB for each connection has to cover them.
They read the heap beside envtest's manager, not in the step without it, and the heap they read is the process's: the manager's heap at the base is in what a measure subtracts, and what the rest of the process allocates or frees after the base moves a reading either way.
In two runs of the suite's process with the manager idle, read as the measures read it, at `GOGC=5`, in 40 windows of 3 s each, the heap in use rose 0.14 MB in the first window, while the manager started, and did not move within the others; after the collection that ends a window it stood from 2,008 bytes under its base to 3,936 over it, but in the 40th and last window of both runs, about two minutes after the manager started, 119,048 and 121,480 bytes, 0.11 and 0.12 MiB, under it.
The three measures held to a bound of the heap in use, the first three below, are held to a figure that has at least 4.25 MiB of room over what they read, which covers 0.14 MB more or 0.12 MiB less; the other two only report what they read and fail on their outcomes.
The first three fail when a read or a pass holds more heap in use than its structural figure, or ends in another outcome than the shape expects:
- `TestMeasureTheLiveHeapOfEveryReadOverAnAmplifyingResponse` answers each read with a response of 8 MiB in 47 shapes: nothing but empty objects where the read keeps an array, a body padded with bytes the types skip, a key of letters, of escapes and of invalid UTF-8, and a field past its bound in each of those;
- `TestMeasureTheLiveHeapOfAPassWhoseEveryReadIsAtItsWorst` runs a pass of `health-nodes`, with its Lease read, its two lists and its renewal, and a pass of `health` over a held restore, the seven lists answered as they come and together, every answer a body of 8 MiB;
- `TestMeasureTheLiveHeapOfAReadWhoseAnswerIsCutAtTheBoundWithHeadersAtTheirs` reads an answer of 16 MiB, which the cap cuts at 8 MiB, 100 times in a process, with the headers of a plain answer, with distinct names filling the bound, about 13,600 field lines ended by a line feed, and with the heaviest head the scan of the argument above found, 14,600 announced names and the field lines the rest of the 64 KiB buys in a chunked head, and, with that head, an answer of 8 MiB read whole, a full block of 1,036 trailers after it, which the cap does not cut and which ends in the Lease it decodes;
- `TestMeasureTheLiveHeapOfAPassOverAPageOf128000EmptyWebhooks` runs a pass over a held restore whose webhook lists answer 500 configurations of 256 empty webhooks;
- `TestMeasureTheLiveHeapOfPassesOverTenantsPaddedToTheirBounds` runs passes over a tenant padded to its bounds, with every page of Leases and of Nodes just under 8 MiB and the default `health.maxNodes`, one pass and 100 at once on 100 tenants;
- `TestMeasureTheBytesOfAPageOf100KubeletNodesWithAndWithoutGzip` lists 100 of the repository's kubelet Nodes from envtest's kube-apiserver with and without `Accept-Encoding: gzip` and logs the bytes of each, which [the decision](decisions/tenant-clients-speak-http1.md) gives as the cost of asking for no compression.
  Its command is the second below, and it gives 750,385 bytes with 21,338 to 25,647 gzipped in twelve runs, 29 to 35 times fewer, since the size gzip reaches follows the names and UIDs the API server draws.
  The uncompressed size carries the name of the test binary, which each Node's `managedFields` names as its manager: `go test`'s binary, `health.test`, gives 750,385 bytes, and a binary of a name five bytes longer 750,885.

The figure they compare is the heap in use.
The live heap is what the last collection marked, objects allocated during it included, so it can read above anything alive at once: in 1,200 cut answers with the headers at their bound it read up to 29.6 MiB, where the heap in use did not pass 21.6 MiB.

```
GOTOOLCHAIN=go1.26.11 LODIA_MEASURE_HEAP=1 KUBEBUILDER_ASSETS="$(go tool setup-envtest use 1.35.0 -p path)" go test -count=1 -v -run TestMeasureTheLiveHeap ./internal/controller/health/
```

```
GOTOOLCHAIN=go1.26.11 LODIA_MEASURE_HEAP=1 KUBEBUILDER_ASSETS="$(go tool setup-envtest use 1.35.0 -p path)" go test -count=1 -v -run '^TestMeasureTheBytesOfAPageOf100KubeletNodesWithAndWithoutGzip$' ./internal/controller/health/
```

These are measurements, not worst cases: the sampler reads the heap when it can, and a peak it misses is a peak it does not report, so each figure is a floor of what the read or the pass held, and the structural figures above are the ceiling.
The runs below are on go1.26.11, `go version go1.26.11 darwin/arm64`, on 16 cores of an Apple M4 Max; each is a process of its own, `-count=1`, and each cut shape runs 100 times in it.

| the measure | runs | live heap | heap in use | structural bound |
|---|---|---|---|---|
| one read, the most of 47 shapes | 12 | 8.1 to 20.4 MiB | 20.5 to 20.6 MiB | 25.9 MiB |
| a read whose answer of 16 MiB is cut | 1,200 | 12.5 to 28.5 MiB | 20.5 MiB | 25.9 MiB |
| the same, headers at their bound | 1,200 | 13.6 to 29.6 MiB | 21.6 MiB | 25.9 MiB |
| the same, the heaviest head | 1,200 | 16.9 to 26.3 MiB | 18.2 to 18.3 MiB | 25.9 MiB |
| a read of an answer of 8 MiB whole, the heaviest head and a full block of trailers | 1,200 | 10.5 to 18.6 MiB | 18.7 to 18.8 MiB | 25.9 MiB |
| one pass of `health-nodes`, the Lease read, the two lists one after the other and the renewal | 3 | 12.5 MiB | 20.5 MiB | 29.9 MiB |
| one pass of `health` over a held restore, seven answers as they come | 12 | 46.2 to 118.3 MiB | 78.4 to 131.0 MiB | 181.3 MiB |
| the same, its seven lists answered together | 12 | 73.6 to 98.5 MiB | 105.6 to 130.5 MiB | 181.3 MiB |
| one pass of `health` over 500 configurations of 256 empty webhooks, named with 253 bytes, one list or both | 12 | 1.3 to 1.7 MiB or 0.9 to 2.8 MiB | 2.6 to 3.1 MiB or 2.6 to 4.8 MiB | |
| the same, named with 2,048 bytes | 12 | 1.5 to 2.2 MiB or 3.7 to 4.6 MiB | 2.9 to 3.5 MiB or 5.3 to 6.5 MiB | |
| one pass over a tenant padded to its bounds, 62 responses | 3 | 5.5 to 21.0 MiB | 13.5 to 22.8 MiB | 29.9 MiB |
| 100 passes at once on 100 tenants | 3 | 0.85 to 0.87 GiB, 8.7 to 8.9 MiB a pass | 0.90 to 0.93 GiB | 2.92 GiB |

The 100 passes at once open 100 connections to one listener, whose listen queue is 128 (`kern.ipc.somaxconn`); the runs above ran one at a time.
The closest any measure came to its bound is 4.25 MiB, the cut answer with the headers at their bound, at 21.65 MiB of 25.9.
No run put a pass at the ceiling of its reads, since their chunks do not all peak together.
A first pass of `health-nodes` over 5,000 Nodes and their 5,000 Leases, the most `health.maxNodes` counts by default, took 305, 302 and 295 ms in three runs on go1.26.11, and a pass over 5,001 Nodes, which stops at the bound, 303, 305 and 306 ms (`TestATenantOfMaxNodesNodesIsCountedWholeAtTheOperatorsTimeout`, on envtest, on 16 cores of an Apple M4 Max, one run at a time).
The race detector slows it tenfold, to 2.86, 2.96 and 2.85 s for 5,000 Nodes and 2.82, 2.83 and 2.82 s for 5,001, in three runs of `TestATenantOfMaxNodesNodesIsCountedWholeUnderTheRaceDetector`, which counts the same tenant with a `Timeout` of a minute, against the operator's of 5 s.
That first test is built without the race detector, and `make test` runs it in a step of its own.
Between passes it keeps at most `health.maxNodes` Lease versions and Node summaries per tenant ([review checklist](review-checklist.md), item 10).
The tenant's administrator can have any server answer the lists, so the pass bounds every string it keeps itself:
- a Lease or a Node named with more than 253 bytes is not kept; no real object has such a name ([validation.go](https://github.com/kubernetes/apimachinery/blob/v0.37.1/pkg/util/validation/validation.go#L190-L191));
- a Lease version keeps a digest of its `resourceVersion`, 64 bits, whatever the form of the version;
- a Node summary keeps a kubelet version only up to 64 bytes and a Ready status only if it is `True`, `False` or `Unknown`: the tenant's administrator writes both, and [`ValidateNodeUpdate`](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/apis/core/validation/validation.go#L7666-L7738) checks neither.

With every string at its bound a Lease version takes a name of 253 bytes and a map slot of 48 bytes (key header, digest and time), and a Node summary 72 bytes, a name of 253 bytes and a kubelet version of 64.
A tenant at the default `health.maxNodes` of 5,000 keeps 5,000 of each, 3.4 MiB of live heap (3.43 to 3.44) as `TestWhatASamplingKeepsRestsOnItsOwnBoundsAndNotOnWhatTheServerAnswers` measures in three runs with names of 253 bytes and versions at their raw bounds: what the lists answer does not change it.
A tenant has two clients of Health's, `health-nodes`' and `health`'s, and each keeps one connection idle between passes: 2 in all, each 2 MiB by the rule of the derivation.
Tenant's client and PKI's keep one each for the 90 s after a pass, which each makes every 10 minutes.
A tenant at rest keeps at most 4 MiB of summaries, 4 MiB of connections and 0.1 MiB of the copies of its access Secrets and of the clients Tenant and PKI keep, 8.1 MiB, and 4 MiB of connections more for 90 s of every 10 minutes.
The copies of its five Secrets hold 13,766 to 13,948 bytes on envtest's PKI (`TestMeasureWhatTheCopiesOfATenantsAccessSecretsHold`, 200 tenants, three runs, live heap after a collection) and each of the two clients 10,560 to 10,606 bytes, 10.3 KiB, the kubeconfig it was built from, 2,148 to 2,156 bytes, included (`TestMeasureWhatAKeptClientOfATenantHolds`, 200 tenants, three runs): 34 KiB in all, 10.0 MiB at 300 tenants, which the figure rounds up with room for the bundle that holds two CAs in a rotation.
The connections of a pass are those 2 and the ones it dials: the figure of a pass counts them with its reads, and the figure at rest counts them idle, so that no connection is counted twice.

## Scale

`test/fleet` runs the operator, every controller as `operator.Setup` registers them and in a process of its own, against envtest's API server and a fleet of gateway `ControlPlane`s on one Gateway, every connection to a tenant dialed to stand-ins through `Inputs.Dial`, and reports what the operator asks of the API server, how often it passes each tenant and what it holds.
One knob sets the size of the fleet, the other the share of its tenants that hang in one phase:

```
make fleet FLEET_TENANTS=1000 FLEET_HANGING=0.1
```

A run needs a checkout, the Go of `go.mod` and setup-envtest's assets of Kubernetes 1.35.0, which `go tool setup-envtest` fetches on the first run, 157 MB; it needs no Docker and no kube context, since envtest starts a kube-apiserver and an etcd of its own and nothing reads a kubeconfig.
It writes the report, the operator's log and the stand-ins' log to `work/fleet/`.
At 300 tenants its process tree held 4.4 GiB at the most, the kube-apiserver 2.6 GiB of it, and took up to 13 cores while it created the fleet and in the wave and about one at rest, on 16 cores of an Apple M4 Max with `GOMAXPROCS=8`.
At 1,000 tenants on Linux the kube-apiserver held 6.0 GiB at the most, etcd 0.9 GiB and the operator 1.6 GiB, so a run of 1,000 wants 16 GiB of memory; it took 26 minutes on 16 vCPUs.
`nightly-fleet` runs 300 tenants every night on GitHub's `ubuntu-24.04` runner, 4 CPUs, 16 GB and 14 GB of disk ([runners](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)), 3.6 times the memory the tree takes, and any number on demand; a run of 1,000 is by hand.

| stand-in | for | leaves out |
|---|---|---|
| one TLS listener that tells the tenant and the path from the name a handshake asks for, and presents the certificate PKI wrote for that path | each tenant's API server behind its Service and the Gateway, lodia-tunnel and konnectivity-server | the network and the Gateway's listener; HTTP/2, which PKI's and Tenant's clients would speak to a kube-apiserver |
| leader Leases renewed every 2 s, three node Leases renewed every 10 s and three kubelet Nodes | the tenant's controller manager, scheduler and kubelets | tenants of many nodes |
| a store of what PKI and Tenant apply, under the field manager that applies it, with every Deployment and DaemonSet rolled out, and the discovery envtest serves | the tenant's API server and controllers | validation, admission and the merges of several field managers |
| Deployments and StatefulSets whose status follows their spec | the management cluster's controllers and kubelets | pods |
| an etcd-druid that serves a new `Etcd`, and a request to reconcile one, with a ready status, and writes new timestamps into every status every 15 s | etcd-druid | the etcd's pods, volumes and snapshots |
| a Gateway that accepts every route | the Gateway's implementation | `attachedRoutes` and the data path |
| one etcd member that every sample reads, after the two Secret reads of `datastore.DialEtcd` | each tenant's etcd | TLS to etcd |
| the operator as its ServiceAccount, bound to the chart's ClusterRoles, under the API server's priority and fairness | the operator's identity | leader election and the scrape of Lodia's collector |

A run creates the fleet and waits until every `ControlPlane` is Ready; it measures the fleet at rest over a minute, one period of the etcd sampler, once the idle connections of the creation have closed, and fails if that minute holds a read of a Secret, then the idle connections, one `ControlPlane` updated and every one at once, a batch of gateway tenants beside the routes of the fleet, with the operator stopped the lists that a gateway tenant's pass, a slot's count and an operator's first grant and first route write take, a restart, a wave of a new revision with a slot for every tenant, the most a wave writes at once, a share of the tenants hanging on every path, a tenth of them, at most 100, answering their lists of Leases and Nodes at Health's bounds, and 10 minutes of a wave at the default slots.
Between them it waits until the operator has settled: for 5 s, no controller but the periodic ones has a request ready in its queue and the operator sends no write, so that the passes of each tenant's timers, Health's, the etcd sampler's, and Tenant's and PKI's every 10 minutes, which are picked up as they fall due and write nothing, do not keep a large fleet from settling.
The requests of a change, one update, every update, a restart and the wave at the default slots, are given also less what the tenants of the fleet send at rest in as long, by the rate of a tenant at rest times the tenants the fleet has then, one for one tenant's namespace, since the periodic reads of the window are not the change's; the subtraction is made for each kind of request, and a kind the window sent less of than its rest nets nothing, not less than nothing.
The operator serves what it counts, its requests by verb and resource, its passes by controller, its heap and its CPU, and the test reads its resident set with `ps`.
The runs below are on go1.26.11, one at a time: on Linux, a VM of 16 vCPUs and 64 GB, `n2-standard-16`, Ubuntu 24.04, with `GOMAXPROCS=8`, 1 run of 100 tenants and 2 of 1,000; on 16 cores of an Apple M4 Max with `GOMAXPROCS=8`, 2 runs of 300; and on the nightly runner, 2 runs of 300 at its 4 CPUs.
A window of a few seconds can hold more of the periodic reads of Secrets than their average at rest: the 18 to 20 requests of one update on the runner and at 1,000 tenants on Linux, and the runner's 5,400 to 5,760 of every update, hold 2 and 359 such reads more.
Each nightly run writes its report into the summary of its workflow run.

| | 100 tenants, Linux | 300 tenants, the Mac | 300 tenants, the runner | 1,000 tenants, Linux |
|---|---|---|---|---|
| every `ControlPlane` Ready after the creation | 27 s | 42 to 45 s | 1 min 59 s to 2 min 17 s | 4 min 39 s to 4 min 42 s |
| requests to the API server at rest | not measured | not measured | 0.0 to 0.1 a second over a minute, watches only, and no read of a Secret (the `fleet` job at 22e953b and 4f97d79) | not measured |
| passes at rest | 20 a second of `health` and of `health-nodes`, 1.7 of `datastore-sample` | 60, and 5.0 | 60, and 5.0 | 200, and 16.7 |
| a tenant passed by `health` and by `health-nodes` at rest | every 5.0 s, 5.02 s at the most | every 5.0 s, 5.02 to 5.05 s at the most | every 5.0 s, 5.05 to 5.06 s at the most | every 5.0 s, 5.04 to 5.06 s at the most |
| the operator at rest: resident set, heap in use, live heap, stacks | 171 MiB, 85 MiB, 38 MiB, 10 MiB | 346 to 398 MiB, 198 to 219 MiB, 94 to 97 MiB, 18 to 20 MiB | 343 MiB, 223 to 225 MiB, 92 to 94 MiB, 19 to 20 MiB | 877 to 886 MiB, 641 to 684 MiB, 288 to 297 MiB, 40 to 42 MiB |
| idle connections at rest, one for each client of each tenant, and what each holds of live heap and stacks | 200; 49 KiB | 600; 42 to 48 KiB | 600; 53 KiB | 2,000; 26 to 43 KiB |
| a handshake more, when every answer closes its connection, in the operator's CPU | 1.43 ms | 0.51 to 0.65 ms | 0.81 to 0.87 ms | 1.68 to 1.70 ms |
| a `ControlPlane`, as JSON | 10,604 bytes, 4,712 of status and 5,209 of `managedFields` | the same | the same | the same |
| one `ControlPlane` updated, a label | one pass of PKI, Lifecycle, Workloads and the Datastore controller; 18 requests besides the reads of the window at rest | the same | the same, 18 to 20 | the same, 18 to 20 |
| every `ControlPlane` updated at once: settled in, requests besides those at rest | 2 s, 1,821 | 3 s, 5,400 | 11 s, 5,400 to 5,760 | 22 s, 18,002 to 18,004 |
| a batch of gateway tenants beside the routes of the fleet: every route Accepted, every one Ready, the median and the most | 5 beside 300 routes: 0.5 s; 2.5 s | 15 beside 900: 0.5 and 1.0 s; 2.5 and 3.0 s | 15 beside 900: 4.1 to 4.2 and 5.2 to 5.6 s; 8.7 to 8.8 and 9.8 s | 50 beside 3,000: 10.0 to 10.6 and 11.5 to 12.2 s; 15.2 to 15.8 and 17.8 to 17.9 s |
| a list of every `ControlPlane` from the cache with copies, a gateway tenant's pass | 0.74 ms, 0.9 MiB allocated | 0.64 to 0.66 ms, 2.8 MiB | 1.03 to 1.50 ms, 2.8 MiB | 8.85 to 9.73 ms, 9.2 MiB |
| the same without copies, a slot's count | 0.03 ms, 0.1 MiB allocated | 0.02 ms, 0.2 MiB | 0.02 to 0.04 ms, 0.2 MiB | 0.23 to 0.25 ms, 0.7 MiB |
| an uncached list of every `ControlPlane`, an operator's first grant | 58 ms, 1.1 MiB on the wire, 4.3 MiB allocated | 67 to 68 ms, 3.2 MiB, 13.2 MiB | 103 to 106 ms, 3.2 MiB, 13.2 MiB | 585 to 592 ms, 10.7 MiB, 43.7 MiB |
| an uncached list of every `TLSRoute`, an operator's first route write on a Gateway | 37 ms, 315 routes, 0.6 MiB on the wire, 2.8 MiB allocated | 37 to 38 ms, 945 routes, 1.7 MiB, 8.4 MiB | 58 to 67 ms, 945 routes, 1.7 MiB, 8.4 MiB | 351 to 354 ms, 3,150 routes, 5.6 MiB, 28.1 MiB |
| a restart: settled in, requests besides those at rest, heap in use at the most | 3 s, 2,878, 118 MiB | 3 s, 8,598 to 8,602, 243 MiB | 16 s, 7,927 to 7,942, 272 to 273 MiB | 43 to 44 s, 24,298 to 24,422, 911 to 927 MiB |
| a wave with a slot for every tenant: every tenant on the revision and Ready, settled | 9 s, 54 s | 14 to 23 s, 59 s to 1 min 8 s | 1 min 5 s to 1 min 7 s, 1 min 50 s to 1 min 52 s | 2 min 26 s to 2 min 27 s, 3 min 12 s |
| the wave's status writes, and its most in a second | 915, 114 | 2,938 to 2,994, 344 to 357 | 2,839 to 2,848, 62 to 67 | 9,822 to 9,863, 109 to 116 |
| the wave's peak: resident set, heap in use, live heap | 228 MiB, 139 MiB, 78 MiB | 450 to 483 MiB, 339 to 349 MiB, 186 to 195 MiB | 553 to 580 MiB, 416 to 420 MiB, 231 to 246 MiB | 1,443 to 1,511 MiB, 1,190 to 1,260 MiB, 646 to 671 MiB |
| a wave at the default 5 slots, from its second minute to its tenth: tenants granted a minute, requests a second besides those at rest | 5.0, 11.9 | 5.0, 12.8 to 13.5 | 5.0, 10.6 to 11.4 | 5.0, 11.0 to 11.1 |
| a tenth of the tenants hanging: an honest tenant passed by `health`, by `health-nodes`, the median and the most | 11 of 105: 5.0 and 5.1 s; 5.0 s | 32 of 315: 5.0 s; 5.0 s | 32 of 315: 5.0 and 5.1 to 5.2 s; 5.0 and 5.1 s | 105 of 1,050: 5.0 and 5.5 to 5.6 s; 5.0 and 5.2 to 5.3 s |
| a tenth of the tenants at Health's bounds: resident set, heap in use at the most | 385 MiB, 244 MiB | 804 to 877 MiB, 233 to 374 MiB | 533 to 548 MiB, 344 to 382 MiB | 1,590 to 1,596 MiB, 978 to 1,282 MiB |

What the numbers answer:
- **Secret reads.**
  Tenant, `health`, `health-nodes` and `DialEtcd` read the access Secrets from memory for as long as their `resourceVersion` stays ([decision](decisions/one-client-for-a-tenant.md)), so that a tenant at rest sends the API server no read of a Secret: the runner's 300 tenants sent it none over a minute at rest, in 0.0 to 0.1 requests a second in all, watches only.
  PKI's passes over each tenant every 10 minutes read: its two lists of the namespace's Secrets without the cache, the labelled ones whole and every one's metadata, about 3.3 a second at 1,000 tenants, none of which the minute at rest held.
- **What a change costs.**
  One `ControlPlane` updated costs one pass each of PKI, Lifecycle, Workloads and the Datastore controller, 18 requests to the API server besides the reads of its window at rest and PKI's three bindings in the tenant; `health` and `health-nodes` pass only on what they read, and Tenant only on its inputs.
  Every `ControlPlane` updated at once costs the same 18 for each, 18,000 at 1,000 tenants, settled in 22 s.
  A restart costs each tenant a pass of every controller and of each periodic one as it starts, and 23 to 27 requests besides those of as long at rest, of which 4 to 8 reads of Secrets; it settled in 43 to 44 s at 1,000 tenants.
  A wave costs each tenant about 29 passes of PKI, Lifecycle, Workloads, the Datastore controller and Tenant, and 9.4 status writes; with a slot for every tenant the 1,050 were on the new revision in 2 min 26 s to 2 min 27 s and free of their slots 45 to 46 s later, the settling.
- **A gateway tenant's pass.**
  Its list of every `ControlPlane` from the cache with copies took 8.9 to 9.7 ms and allocated 9.2 MiB at 1,050 tenants, 9 KiB a tenant; Workloads' mappers of route, Gateway and `ControlPlane` events take the same list for each event ([todo](../todo.md)).
- **Route writes.**
  A route's write reads the route under its name from the API server and the routes that may hold its hostname from the cache's index, and waits until the cache holds what it wrote, one at a time on a Gateway ([api-exposure](decisions/api-exposure.md)): an operator lists every `TLSRoute` from the API server for its first write on a Gateway, and again after a write whose outcome is unknown or that its cache did not show within 10 seconds, 351 to 354 ms for 3,150 routes.
  Its cost does not grow with the fleet, but the writes of one Gateway still go one after another: a batch of 50 tenants, 150 writes beside 3,000 routes, had every route Accepted 11.5 to 12.2 s after it began, about 77 to 81 ms a write, and the creation of 1,000 tenants, 3,000 writes, every one Ready 4 min 39 s to 4 min 42 s after it began, about 93 to 94 ms a write with the API server busier ([todo](../todo.md)).
- **A slot of the fleet.**
  A grant counts the fleet from the cache without copies, 0.23 to 0.25 ms at 1,050 tenants, and waits until the cache holds it; an operator lists every `ControlPlane` from the API server for its first grant, and again after a grant whose outcome is unknown or that its cache did not show within 10 seconds, 585 to 592 ms, 10.7 MiB on the wire and 43.7 MiB allocated at 1,050 tenants ([upgrades-and-maintenance-windows](decisions/upgrades-and-maintenance-windows.md)).
  With a slot for every tenant the wave of 1,050 granted them in 2 min 26 s to 2 min 27 s, one list of the fleet in all.
  At the default 5 slots the wave granted 5 tenants a minute at every size, each slot freed at the end of its settling, 45 s, so a wave of 1,000 tenants takes about 3.3 hours; the 1,000 tenants that waited were passed by Lifecycle every 30 s, 35 passes a second, each counting the fleet from the cache, which sends the API server nothing: the wave's 11.0 to 11.1 requests a second besides those of the 1,050 tenants at rest, 10.6 to 13.5 at 100 and 300 tenants, held no list of the fleet: they were the writes and reads of the 5 tenants rolling at a time, 0.9 status writes a second among them.
- **Tenants that do not answer.**
  A pass of `health` over a tenant that hangs holds its worker for one `Timeout`: its probes run at once, and once that of the in-namespace Service fails it reads nothing more in the tenant.
  Its 100 workers by default hold the hanging tenants of a tenth of the fleet, 50 on average and 100 when they hang together: with 105 of 1,050 hanging an honest tenant was passed every 5.0 s at the median and 5.5 to 5.6 s at the most, within the 10 s of a `health.period` and a `Timeout` that Lifecycle's settling counts for each pass.
  A pass of `health-nodes` over a tenant that hangs holds its worker for the `Timeout` of its read of the controller manager's Lease, and comes back a `health.period` after it ends, as a pass of `health` does, so its 100 workers by default hold the same: 50 hanging tenants on average and 100 when they hang together.
  With 105 of 1,050 hanging it sampled an honest tenant every 5.0 s at the median and 5.2 to 5.3 s at the most, within the 12.6 s the hold keeps for a delay in Health's queue ([controller-manager-hold](decisions/controller-manager-hold.md)).
  `controllers.health.workers` and `controllers.health-nodes.workers` set them ([README](README.md)); each worker costs 25.9 MiB and 29.9 MiB of the worst case below.
- **The status.**
  A `ControlPlane` is 10.6 KB of JSON, half of it `managedFields`, under a hundredth of etcd's limit on an object; at rest the controllers write no status, since each writes only a change, and a wave writes 9.4 statuses a tenant, 109 to 116 in its busiest second with every tenant in a slot and about one a second at the default 5 slots.
  Splitting `status` into an object of its own would spare the watchers of the `ControlPlane` a copy of 10.6 KB for each of those writes and cost a second object, its writes and its ownership for every tenant: keep `status` where it is, and look again when a field of it grows with a tenant's nodes or objects.

The operator's memory:
- At rest on Linux the operator holds about 0.64 MiB of heap in use for each tenant beside about 20 MiB of its own, and 0.79 MiB of resident set beside about 90 MiB; macOS keeps memory the heap gave back in the resident set, which makes its figure larger.
- An idle connection holds 26 to 53 KiB of live heap and stacks, measured by the drop when every answer closes its connection, and a handshake more costs 0.5 to 1.7 ms of CPU.
  Each tenant client keeps one connection idle between passes and closes the others as they are given back, for client-go's 90 s at most: 2 for a tenant at rest, about 0.1 MiB, where 9 were possible before, and a held restore's pass dials six, 3 to 10 ms of CPU each pass; keeping none would cost every tenant two handshakes every 5 s, about 0.7 of a CPU at 1,000 tenants by the measure there; Tenant's client and PKI's keep one each for 90 s after a pass every 10 minutes.
  By the rule of [Capacity](#capacity), 2 MiB for each connection, a tenant at rest keeps at most 8.1 MiB, and 12.1 MiB for 90 s of every 10 minutes.
  The runner's live heap at rest at 300 tenants was 104.7 MiB in the `fleet` job at 22e953b and 111.9 MiB at 4f97d79, beside the 91.6 and 93.8 MiB of the two runs of the runner's column above, which were taken before Tenant and PKI kept a client for each tenant and the access Secrets' copies were kept.
  Two runs of nearly the same code spread by 7.2 MiB, so a point sample of the live heap cannot attribute the rise.
  The two structural measures of [Capacity](#capacity), `TestMeasureWhatTheCopiesOfATenantsAccessSecretsHold` and `TestMeasureWhatAKeptClientOfATenantHolds`, put the copies and the clients at 34 KiB a tenant, 10.0 MiB at 300 tenants.
- Each pass of `health-nodes` reads its two lists one after the other, which halves the worst case of 100 passes at once at Health's bounds: 2.92 GiB, 29.9 MiB a pass, where the two lists at once make 5.45 GiB; the 100 passes measured 0.90 to 0.93 GiB of heap in use ([Capacity](#capacity)).
  It costs a tenant of 5,000 Nodes about 25 ms every 30 s: a first pass over it took 295 to 305 ms, and 270 to 285 ms with the two lists at once, three runs each.
  Fewer workers would cost every tenant the samples that hanging tenants delay, and a shared bound on responses in flight would still leave the heads and the margins of every read in flight, 5.5 MiB each, unbounded, and let as many slow tenants as it has places stall every other read.
- The chart's requests fit about 100 tenants, by the run of 100 on Linux above: 512 MiB of memory, over a resident set of 171 MiB at rest and 385 MiB at the most with a tenth of the tenants at Health's bounds, and 300 millicores, over 0.23 of a CPU at rest; it sets no CPU limit.
  Its memory limit, 10 GiB, covers what does not grow with the fleet, Health's worst case at its default workers, 6.97 GiB, and Tenant's and PKI's, 0.53 GiB ([Capacity](#capacity)), beside the resident set of those 100 tenants at its most, 7.9 GiB in all, with more than a quarter of room; `GOMEMLIMIT`, 9 GiB, is a GiB under the limit, for what the Go runtime does not count, and above that sum.
  A platform with more tenants sets `resources` and `goMemLimit` by the table below, from the runs above, the resident set at its most with a tenth of the tenants at Health's bounds and the CPU at rest with room, and the limit over Health's worst case, Tenant's and PKI's and that resident set with at least a quarter of room; one that raises `controllers.health-nodes.workers`, `controllers.health.workers`, `controllers.tenant.workers` or `controllers.pki.workers` raises the limit by 29.9, 25.9, 28.3 or 25.9 MiB for each worker.
  The room is the limit over that sum less one, from the highest resident set of a row's runs: 27.0% at 100 tenants (385 MiB), 31.6% at 300 (the Mac's 877 MiB; 35.3% for the runner's 642.6 MiB, the higher of its two runs at 22e953b and 4f97d79) and 32.5% at 1,000 (1,596 MiB).

| tenants | memory request | CPU request | memory limit | `goMemLimit` |
|---|---|---|---|---|
| about 100, the chart's | 512 MiB | 300m | 10 GiB | 9GiB |
| 300 | 1 GiB | 750m | 11 GiB | 10GiB |
| 1,000 | 2.5 GiB | 3 | 12 GiB | 11GiB |

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
| API versions | `v1alpha1` may change incompatibly until the maintainers declare it frozen, and only by addition from then on; a later version keeps its shape and converts with no webhook ([one-controlplane-per-cluster](decisions/one-controlplane-per-cluster.md#consequences)) |
