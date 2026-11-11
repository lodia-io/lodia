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
| `DatastoreReady`'s message ends with `DatastoreClass <c> generation <g> waits for status.target.datastoreClass` | an edit of the class, or a class deleted and created again under its name, which `<g>` then names by its UID, waits for Lifecycle's grant in the tenant's maintenance window, which `Progressing` names; nothing is broken |
| `DatastoreReady=True`, `SecretNotFound`, `ObjectsControlledElsewhere` or `Refused`, and its message notes `DatastoreClass <c> is held:` | the class's credentials copy cannot be made, and the `Etcd` keeps the class fields it has while it still takes new certificate generations: restore the class's Secret the message names (`SecretNotFound`); or wait while the garbage collector removes a predecessor's `<cp>-etcd-backup`, which another owner controls, and delete it if its owner is something else (`ObjectsControlledElsewhere`); or read Lodia's log when the API server refuses the copy (`Refused`). Meanwhile Lifecycle grants no change in the tenant's window, and one in its slot is `MaintenanceFailed`. A class edit made meanwhile applies once the copy can be made and Lifecycle grants it |
| `DatastoreReady`'s message notes `DatastoreClass <c>'s quota <q> is capped at` | the class's quota needs more than half the `Etcd`'s volume, which never grows, so the `Etcd`'s quota is half its volume ([dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md)) and the rest of the class edit applies. Nothing is broken: keep it, or lower the class's `quotaBackendBytes` to at most half the volume the message names |
| `DatastoreReady`'s message notes `highAvailability waits for Etcd <cp>-etcd to settle` | added `highAvailability` waits while a member of the `Etcd` is not available or runs an older spec, or etcd-druid has not completed a scale ([what the class becomes](README.md#a-tenant-namespace)): the StatefulSet's pods and events name the member |
| `DatastoreReady`'s message notes `removing highAvailability waits for etcd-druid to list member <cp>-etcd-0` | etcd-druid does not list the member it keeps as ready ([what the class becomes](README.md#a-tenant-namespace)): look at the pod `<cp>-etcd-0` and its lease of that name; adding `highAvailability` back cancels the removal at once, since the `Etcd` still has its members |
| With `highAvailability`, a replica of a component or an etcd member stays `Pending`, and its rollout never completes | too few nodes the pods may use: each component needs as many as it has replicas, the API server `apiServer.replicas`, and the etcd three, with `Zone` in as many zones, at most three ([how they spread](README.md#a-tenant-namespace)); the pod's `FailedScheduling` event reads `didn't match pod topology spread constraints` ([plugin.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/scheduler/framework/plugins/podtopologyspread/plugin.go#L41)). A `Pending` etcd member also halts the etcd's roll: the members it has not reached keep the old certificate generation ([stateful_set_control.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/statefulset/stateful_set_control.go#L863-L873)). Add nodes |
| `DatastoreReady=False`, `Provisioning`, `Secrets Etcd <cp>-etcd mounts do not exist` | someone removed etcd-druid's finalizer from Secrets the `Etcd` names and deleted them; the members run on what they mounted until they restart: the [runbook](#a-secret-the-etcd-mounts-is-gone) |
| `DatastoreReady=False`, `Provisioning` for long | the message says what is awaited: a complete generation of the etcd certificates that this `ControlPlane` controls (PKI; one another `ControlPlane` controls never counts), the class's credentials copy, or etcd-druid's report; then the `Etcd`'s status — its `lastErrors`, which `DatastoreReady` does not read, are the only place a persistent failure of etcd-druid's other objects (the client Service, the ConfigMap, the leases) shows at a generation it has observed — its conditions, etcd-druid's log, and the tenant's `EtcdOpsTask`s, where a blocked operation shows ([etcd spike](etcd-spike-2026-10.md), finding 4) |
| An etcd member that does not come back after a roll: `status.datastore.members.ready` short of `total`, `DatastoreReady=False` with one member, the StatefulSet's pod missing | the StatefulSet's events (`kubectl describe statefulset <cp>-etcd`): `FailedCreate` means pod admission refused the new pod after the old one was deleted, for a `priorityClassName` that names no PriorityClass (or one deleted since) or a LimitRange or ResourceQuota in the namespace, which no schema can check; with one member it is an outage until fixed, and every certificate generation rolls the member |
| `DatastoreReady=False`, `QuorumLost` | no member ready: one member while it restarts, as every certificate generation and class edit rolls it, or two of three down, which leaves the third without quorum; the [lost quorum](#lost-etcd-quorum) runbook when it lasts |
| API calls fail with `etcdserver: mvcc: database space exceeded` | the [space quota](#etcd-space-quota) runbook; from wave 2, observability, the metric `lodia_controlplane_datastore_db_size_bytes` against `lodia_controlplane_datastore_quota_bytes` ([metrics-are-part-of-the-api](decisions/metrics-are-part-of-the-api.md)) |
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
| `Progressing=False`, `FleetLimit` | other tenants hold the slots, `status.maintenance.slotSince` present whatever their `Progressing` reads: they roll a change Lodia started, or failed one and keep their slots (`LodiaMaintenanceHalted`, from the observability wave); the operator's `fleet.maxConcurrentRollouts`; `status.target.scheduledFor` orders the queue |
| `Progressing=False`, `Failed` or `MaintenanceFailed` | `status.kubernetes.upgrade.step` and `Progressing`'s message; the [stuck upgrade](#stuck-upgrade) runbook |
| `Progressing=False`, `WaitingForNodes` | a kubelet more than three minors older than the next minor, `status.nodes.oldestKubelet`: upgrade the nodes first; or `status.nodes` is absent, Health not having listed the nodes yet: the tenant's API must answer it |
| `Progressing=False`, `TimeZoneUnsupported` | `spec.maintenance.window.timeZone` is not in the operator's zone database; an IANA name such as `Europe/Berlin` |
| `Progressing=True`, `LatestGenerationNotObserved` for more than a few seconds | the first condition the message names has not taken the change up: its reason, which describes the generation it names, when it is False; otherwise what its controller waits on ([independent-controllers](decisions/independent-controllers.md)), which `Ready` names when that is not True; its controller's log (`controller="<name>"`) only when that is True at the new generation |
| Nothing changes after editing `spec` | `metadata.generation` against `status.observedGeneration`; `spec.paused`; the operator's leader and its logs |

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
Renewal and backups pause; the `LodiaOperatorDown` alert fires first.

## Alerts shipped in the chart

From phase 2's wave 2, behind a value of `metrics.*` and off by default, written on the metrics of [metrics-are-part-of-the-api](decisions/metrics-are-part-of-the-api.md):

| alert | fires when | means |
|---|---|---|
| `LodiaOperatorDown` | no operator replica scraped for 5 m | nothing is reconciled; tenants keep running |
| `LodiaControlPlaneNotReady` | `Ready` not True for 10 m, not hibernated | a tenant's API or a part of it is down; the reason names which |
| `LodiaRedundancyLost` | a component with fewer ready replicas than it runs, or an etcd with fewer ready members than it has, for 15 m | the control plane serves, and one more loss is an outage; the component's pods or the member's, and their nodes |
| `LodiaCertificateExpiring` | `status.credentials.notAfter` within 14 days, for 1 h | renewal failed; look at `CertificatesReady` |
| `LodiaCAExpiring` | `CAValidityAcceptable=False` for 1 h | a CA is in its last year, named in the message; rotate it |
| `LodiaBackupStale` | `BackupReady=False` for 15 m, but for `NoBackupStore` | the backup sidecar or the bucket credentials; etcd-druid's message is in the condition |
| `LodiaEtcdSpaceNearQuota` | the database above 80% of the quota for 15 m | compaction is not keeping up, or the tenant writes too much |
| `LodiaEtcdNoLeader` | a dedicated etcd without a leader for 1 m | quorum or network inside the management cluster |
| `LodiaControllerManagerHeld` | the hold active for 15 m | most nodes cannot reach the control plane; the tenant's workloads run, its controllers wait |
| `LodiaUpgradeFailed` | `Progressing=False`, reason `Failed`, at once | the stuck upgrade runbook |
| `LodiaMaintenanceHalted` | as many tenants whose change Lodia started failed in their slots as `fleet.maxConcurrentRollouts`, the slot read from `status.maintenance.slotSince`, not from `Progressing`'s reason, since a slot is also held under `Unsupported`, which ranks above `MaintenanceFailed`, and `TimeZoneUnsupported` | changes Lodia starts are halted across the fleet; the stuck upgrade runbook for each |
| `LodiaKubernetesEndOfSupport` | a tenant's minor within 30 days of its end-of-support date in the version table | upgrade it a minor; `Progressing`'s message says the same |
| `LodiaReconcileErrors` | a controller's error rate above 5% for 15 m | the operator logs, by controller |

Every alert links to its runbook section here.

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
| Tenant Kubernetes versions | the minors the Kubernetes project maintains, today 1.35, 1.36 and 1.37; a minor leaves Lodia's version table with the first release after it leaves upstream support, and stays renderable for one more release, only as the source of an upgrade; tenants are warned in `Progressing`'s message from 60 days before its end-of-support date, and in a metric and an alert from wave 2's observability |
| Management cluster | a maintained minor with the CEL features the CRDs use: 1.35 or newer today |
| Lodia versions | until 1.0, the latest minor gets fixes; from 1.0, the two latest minors |
| API versions | `v1alpha1` may change between minors until `v1beta1`; from `v1beta1`, changes are additive and conversion needs no webhook |
