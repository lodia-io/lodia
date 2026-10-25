# ControlPlane — field reference (v1alpha1, draft)

`brygga.segla.io/v1alpha1`, namespaced. One object per tenant cluster. The
Go types will be generated from this reference, and the two change together.
Examples: [`examples/`](examples/).

## spec

| field | type | default | rules |
|---|---|---|---|
| `version` | string | required | `v<major>.<minor>.<patch>`; the minor never decreases (CEL transition rule); the patch may |
| `layout` | `Split` \| `Compact` | `Split` | immutable; `Compact` puts every control plane component in one pod and excludes `highAvailability` and `apiServer.replicas` above 1 ([compact-layout](../decisions/compact-layout.md)) |
| `highAvailability.failureTolerance` | `Node` \| `Zone` | absent = single | sets replicas (2/2/2, etcd 3) and the spread topology key (`kubernetes.io/hostname` or `topology.kubernetes.io/zone`) |
| `datastore` | union | required | exactly one of `dedicated`, `shared`; immutable in v1alpha1 (migration is a later operation) |
| `datastore.dedicated.className` | string | the default `DatastoreClass` | |
| `datastore.shared.name` | string | | a `Datastore` |
| `endpoint` | union | required | exactly one of `gateway`, `loadBalancer`, `nodePort`; immutable in v1alpha1 |
| `endpoint.gateway.parentRef` | `{name, namespace, sectionName?}` | | the Gateway with a TLS passthrough listener |
| `endpoint.gateway.hostnames.apiServer` | hostname | required with gateway | the public API hostname; a SAN of the serving certificate |
| `endpoint.gateway.hostnames.konnectivity` | hostname | required with gateway | where agents connect |
| `endpoint.gateway.hostnames.tunnel` | hostname | absent = no in-cluster proxy | the TLS-terminated tunnel that apiserver-proxy on nodes dials ([api-exposure](../decisions/api-exposure.md)) |
| `endpoint.loadBalancer.{class, sourceRanges, annotations}` | | | a Service of type LoadBalancer with ports 443 (API) and 8132 (konnectivity) |
| `endpoint.nodePort` | `{}` | | a Service of type NodePort; the endpoint is the first management node address |
| `endpoint.certSANs` | []string | | extra SANs on the API serving certificate |
| `network.serviceCIDRs` | []CIDR | required | immutable; the first address of the first CIDR is the `kubernetes` Service, the tenth is CoreDNS |
| `network.podCIDRs` | []CIDR | required | immutable; the controller manager allocates node CIDRs from it |
| `network.clusterDomain` | string | `cluster.local` | immutable |
| `apiServer.replicas` | int | 1, or 2 with HA | ≥ 1; ≥ 2 with HA |
| `apiServer.extraArgs` | map | | flags Brygga owns are rejected by CEL (`etcd-servers`, `service-account-*`, `client-ca-file`, `egress-selector-config-file`, …); the list is in the schema |
| `apiServer.authentication` | object | no JWT | rendered into the structured `AuthenticationConfiguration`; `jwt[]` as upstream; the `anonymous` section is Brygga's (only `cluster-info` and the health paths) and cannot be set |
| `apiServer.advertiseAddress` | IP | `172.20.0.1` | gateway mode only; the address `apiserver-proxy` owns on every node; must not overlap the tenant's node, pod or service ranges |
| `serviceAccount.issuer` | URL | `https://<public API host>` | the tokens' issuer and OIDC discovery URL; `serviceAccount.publicDiscovery: true` binds discovery to unauthenticated users for federation |
| `apiServer.audit.policyRef` | `{name}` | none | a ConfigMap in the namespace, key `policy.yaml`; the log goes to the pod's stdout unless `audit.webhookRef` names a Secret with a kubeconfig |
| `apiServer.encryption.provider` | `None` \| `secretbox` \| `aescbc` \| `kms` | `secretbox` | keys are generated and rotated by PKI; `kms` takes `kms.endpoint` and `kms.name` |
| `apiServer.admission.configRef` | `{name}` | none | a ConfigMap with an `AdmissionConfiguration` |
| `apiServer.featureGates` | map | | |
| `apiServer.resources` | ResourceRequirements | operator defaults | |
| `controllerManager.{extraArgs, featureGates, resources}` | | | same rules; `cluster-cidr`, `service-cluster-ip-range`, signing and SA flags are Brygga's |
| `scheduler.{extraArgs, featureGates, resources, configRef}` | | | `configRef` names a ConfigMap with a `KubeSchedulerConfiguration` whose client connection Brygga overrides |
| `addons.konnectivity.version` | string | from the version table | pin only to hold a version across a Kubernetes upgrade |
| `addons.konnectivity.agent.mode` | `DaemonSet` \| `Deployment` | `DaemonSet` | `Deployment` takes `replicas`; `hostNetwork` defaults false |
| `addons.kubeProxy.{enabled, mode}` | | `true`, `iptables` | `enabled: false` for CNIs that replace kube-proxy |
| `addons.coreDNS.{enabled, replicas}` | | `true`, 2 | |
| `placement.{nodeSelector, tolerations, priorityClassName}` | | operator defaults | applies to every control plane pod of this tenant; etcd placement comes from the `DatastoreClass` |
| `maintenance.window.{begin, end, timeZone}` | `HH:MM`, `HH:MM`, IANA zone | absent = any time | end may be before begin (crosses midnight) |
| `maintenance.autoUpdate.kubernetesPatch` | bool | `false` | newest patch of the running minor, applied in the window |
| `hibernation.enabled` | bool | `false` | an explicit override of the schedules |
| `hibernation.schedules[]` | `{start, end, timeZone}` | | cron expressions; `start` puts the control plane to sleep, `end` wakes it |
| `restoreFrom.backupRef` | `{name, namespace?}` | | set only at creation (CEL: cannot be added or changed later); the datastore starts from that snapshot, from phase 2 on ([roadmap](../roadmap.md)): until then nothing reads it, and a `ControlPlane` created with it starts empty |
| `health.holdControllerManager` | bool | `true` | hold the controller manager's leader lease while most nodes are silent ([controller-manager-hold](../decisions/controller-manager-hold.md)); `false` releases a hold in progress |
| `paused` | bool | `false` | Brygga stops reconciling but keeps watching; `Progressing=False`, reason `Paused`; the finalizer stays, so a paused `ControlPlane`'s deletion waits until it is unpaused |

## status

| field | written by | meaning |
|---|---|---|
| `observedGeneration` | Lifecycle | the last `metadata.generation` every controller has fully processed |
| `conditions[]` | each controller its own | `CertificatesReady`, `CAValidityAcceptable`, `DatastoreReady`, `APIServerReady`, `AccessReady`, `AddonsApplied`, `AddonsAvailable`, `Reachable`, `Progressing`, `Hibernated`, `Ready`; each with `reason`, `message`, `observedGeneration` |
| `target.version` | Lifecycle | the Kubernetes version being rolled now; one minor at a time towards `spec.version` |
| `target.operatorVersion` | Lifecycle | the Brygga version whose templates this tenant's pods are rendered with |
| `target.hibernated` | Lifecycle | the desired sleep state |
| `target.rotation.{clientCA, ca, serviceAccountKey, etcdEncryptionKey, etcdCA, frontProxyCA}` | Lifecycle | `Idle` \| `Preparing` \| `Prepared` \| `Completing` |
| `target.reason`, `target.scheduledFor` | Lifecycle | why the target lags `spec`: a closed window, the fleet limit, a failed step |
| `kubernetes.version` | Lifecycle | the version every component runs |
| `kubernetes.upgrade.{from, to, step, startedAt, message}` | Lifecycle | present while an upgrade runs or is stuck |
| `endpoint.{host, port}` | Workloads | the public endpoint, published as soon as the Service has an address; PKI puts it into the serving certificate and the kubeconfigs |
| `endpoint.internal` | Workloads | the in-namespace Service, `https://<name>-kube-apiserver.<ns>.svc:443` |
| `credentials.adminKubeconfigSecretRef` | PKI | `<name>-admin-kubeconfig` |
| `credentials.caBundleSecretRef` | PKI | `<name>-ca-bundle`: the CAs a client must trust now (two during a rotation) |
| `credentials.rotation.<credential>.{phase, lastInitiated, lastCompleted}` | PKI | the stages per credential (the same keys as `target.rotation`), as Gardener reports them |
| `datastore.{kind, name, members, dbSizeBytes}` | Datastore | `Dedicated` or `Shared` |
| `datastore.lastBackup.{time, kind, revision}` | Datastore | the newest full or delta snapshot |
| `components.{apiServer, controllerManager, scheduler}.{replicas, ready}` | Workloads | from the Deployments |
| `nodes.{total, ready, outdatedKubelets, sampledAt}` | Health | from a periodic list of nodes and leases in the tenant |
| `health.controllerManagerHold.{since, silentNodes, countedNodes, blockedBy}` | Health | present while Brygga holds the controller manager's leader lease; `blockedBy` names a foreign holder Brygga left alone |

## Conditions and their reasons

A controller writes its conditions `Unknown` with reason `Reconciling` on its
first visit, at the start of its first pass; a reader counts a condition only
while its `observedGeneration` is the object's generation
([independent-controllers](../decisions/independent-controllers.md)).

| condition | True | False reasons |
|---|---|---|
| `CertificatesReady` | every certificate, key and kubeconfig exists, verifies, and is valid for more than its renewal margin or until its CA expires (`Issued`) | `Issuing` (a Secret PKI has to change is not written yet: the message names it, the error is in Brygga's log), `WaitingForEndpoint` (`loadBalancer` and `nodePort`, until Workloads publishes `status.endpoint`), `CAMissing` (a CA's key is missing or unreadable), `CAExpired` (a CA is past its `NotAfter`); in both, the message names the CA and nothing it signs is written until it is restored or rotated; `SecretsControlledElsewhere` (a Secret under one of PKI's names has another owner, which the message names: a previous `ControlPlane` of the same name until the garbage collector deletes it, anything else until a person releases it; PKI writes no Secret meanwhile); `SecretUnreadable` (the encryption configuration or the ServiceAccount key cannot be read, and PKI never replaces it); `EncryptionProviderUnsupported` (`kms`) |
| `CAValidityAcceptable` | every CA is valid for more than a year (`Valid`); not part of `Ready`, since a CA in its last year, or a rotation under way, leaves every certificate valid | `CAExpiring`, the message naming each CA and its `NotAfter`, as Gardener's `CACertificateValiditiesAcceptable` ([shoot status](https://github.com/gardener/gardener/blob/v1.154.0/docs/usage/shoot/shoot_status.md#L105-L109)); `Unknown`, `CAMissing`, while a CA's key cannot be read |
| `DatastoreReady` | the etcd (or the tenant's prefix in a shared store) accepts writes | `Provisioning`, `QuorumLost`, `Restoring`, `DatastoreClassNotFound`, `DatastoreNotFound`, `VolumesExist`, `EtcdRefused` (the API server refuses the `Etcd` Brygga renders), `Deleting`, `Hibernated` |
| `APIServerReady` | all API server replicas answer `/readyz`; controller manager and scheduler have leaders | `WaitingForCertificates`, `WaitingForDatastore`, `RollingOut`, `Unhealthy`, `Hibernated` |
| `AccessReady` | the tenant's ClusterRoleBindings for `brygga:operator`, `brygga:cluster-admins` and `brygga:kubelet-api-admins` exist, checked with `brygga:operator` (`Bound`) | `WaitingForAPIServer`, `Binding` (the tenant refused the bindings, or has not confirmed them since its API server became ready; while it does not answer, a True stays, since reaching it is `Reachable`'s to report) |
| `AddonsApplied` | the inventory is applied and pruned | `WaitingForAPIServer`, `Applying` |
| `AddonsAvailable` | kube-proxy, CoreDNS and the agents report ready; not part of `Ready`, since their pods need the owner's nodes | `RolloutPending` |
| `Reachable` | the public endpoint answers `/readyz` and nodes' leases are fresh | `ExternalProbeFailed`, `NodesSilent` (the controller manager is held), `HoldBlocked` (nodes silent, another holder keeps the lease), `Hibernated` |
| `Progressing` | an upgrade, rotation stage, hibernation transition or deferred rollout is running | `Idle`, `WaitingForWindow`, `FleetLimit`, `Failed`, `Paused` |
| `Hibernated` | every component is scaled to zero and the data is kept | `Awake`, `FallingAsleep`, `WakingUp` |
| `Ready` | the six readiness conditions are True and the control plane is awake | the first False one's reason |
