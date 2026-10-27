# ControlPlane — field reference (v1alpha1, draft)

`lodia.io/v1alpha1`, namespaced. One object per tenant cluster. The
Go types will be generated from this reference, and the two change together.
Examples: [`examples/`](examples/).

The name is a DNS-1035 label of at most 32 characters (CEL), since the
names Lodia and etcd-druid derive from it have to fit Kubernetes' limits.
The tightest is etcd-druid's label on the etcd's Role,
`druid.gardener.cloud-etcd-<name>-etcd`
([role.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/role/role.go#L154)),
and a label value holds at most 63 characters
([syntax](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set));
the Service `<name>-kube-apiserver` needs a name that starts with a letter on
management clusters before 1.36
([RelaxedServiceNameValidation](https://github.com/kubernetes/kubernetes/blob/v1.36.0/pkg/features/kube_features.go#L1881-L1884)).

## spec

| field | type | default | rules |
|---|---|---|---|
| `version` | string | required | `v<major>.<minor>.<patch>`; the minor never decreases (CEL transition rule); the patch may |
| `layout` | `Split` \| `Compact` | `Split` | immutable; `Compact` puts every control plane component in one pod and excludes `highAvailability` and `apiServer.replicas` above 1 ([compact-layout](../decisions/compact-layout.md)) |
| `highAvailability.failureTolerance` | `Node` \| `Zone` | absent = single | sets replicas (2/2/2, etcd 3) and the spread topology key (`kubernetes.io/hostname` or `topology.kubernetes.io/zone`); phase 2 (until then `APIServerReady=False`, `Unsupported`) |
| `datastore` | union | required | exactly one of `dedicated`, `shared`; immutable in v1alpha1 (migration is a later operation) |
| `datastore.dedicated.className` | string | the default `DatastoreClass` | |
| `datastore.shared.name` | string | | a `Datastore` |
| `endpoint` | union | required | exactly one of `gateway`, `loadBalancer`, `nodePort`; immutable in v1alpha1 |
| `endpoint.gateway.parentRef` | `{name, namespace, sectionName?}` | | the Gateway with a TLS passthrough listener |
| `endpoint.gateway.hostnames.apiServer` | hostname | required with gateway | the public API hostname; a SAN of the serving certificate |
| `endpoint.gateway.hostnames.konnectivity` | hostname | required with gateway | where agents connect |
| `endpoint.gateway.hostnames.tunnel` | hostname | absent = no in-cluster proxy | the TLS-terminated tunnel that apiserver-proxy on nodes dials ([api-exposure](../decisions/api-exposure.md)) |
| `endpoint.loadBalancer.{class, sourceRanges, annotations}` | | | a Service of type LoadBalancer with ports 443 and 6443 (API) and 8132 (konnectivity); 6443 is the `kubernetes` Service's endpoint inside the tenant ([api-exposure](../decisions/api-exposure.md)) |
| `endpoint.nodePort` | `{}` | | a Service of type NodePort; the endpoint is the first Ready management node's InternalIP by name, kept while any node still reports it, Ready or not |
| `endpoint.certSANs` | []hostname | | extra names on the API serving certificate |
| `network.serviceCIDRs` | []CIDR | required | immutable; the first address of the first CIDR is the `kubernetes` Service, the tenth is CoreDNS |
| `network.podCIDRs` | []CIDR | required | immutable; the controller manager allocates node CIDRs from it |
| `network.clusterDomain` | hostname | `cluster.local` | immutable |
| `apiServer.replicas` | int | 1, or 2 with HA | ≥ 1; ≥ 2 with HA; above 1 from phase 2 (until then `APIServerReady=False`, `Unsupported`) |
| `apiServer.extraArgs` | map | | every flag Lodia sets is refused by CEL, with `anonymous-auth` and the ServiceAccount key flags, which Lodia's configuration excludes; `feature-gates` comes from `featureGates`. An entry stored before the schema refused its flag stays valid until it changes ([ratcheting](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#validation-ratcheting)), but never replaces the value Lodia sets. One flag merges instead: `enable-admission-plugins`, whose entries follow Lodia's `NodeRestriction`; their order does not matter to the API server ([admission.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/server/options/admission.go#L108-L112)). The list is in the schema |
| `apiServer.authentication` | object | no JWT | rendered into the structured `AuthenticationConfiguration`; `jwt[]` as upstream (from phase 2; until then `APIServerReady=False`, `Unsupported`); the `anonymous` section is Lodia's (only `cluster-info` and the health paths) and cannot be set |
| `apiServer.advertiseAddress` | IP | `172.20.0.1` | gateway mode only (in another mode `APIServerReady=False`, `Unsupported`); the address `apiserver-proxy` owns on every node; must not overlap the tenant's node, pod or service ranges |
| `serviceAccount.issuer` | URL | `https://<public API host>` | the tokens' issuer and OIDC discovery URL; `serviceAccount.publicDiscovery: true` binds discovery to unauthenticated users for federation, from phase 2 (until then `APIServerReady=False`, `Unsupported`) |
| `apiServer.audit.policyRef` | `{name}` | none | a ConfigMap in the namespace, key `policy.yaml`; the log goes to the pod's stdout unless `audit.webhookRef` names a Secret with a kubeconfig; from phase 2 (until then `Unsupported`) |
| `apiServer.encryption.provider` | `None` \| `secretbox` \| `aescbc` \| `kms` | `secretbox` | keys are generated and rotated by PKI; `kms` takes `kms.endpoint` and `kms.name` |
| `apiServer.admission.configRef` | `{name}` | none | a ConfigMap with an `AdmissionConfiguration`; from phase 2 (until then `Unsupported`) |
| `apiServer.featureGates` | map | | |
| `apiServer.resources` | ResourceRequirements | operator defaults | |
| `controllerManager.{extraArgs, featureGates, resources}` | | | the same rule, the signing and ServiceAccount key flags refused too; `controllers` merges: Lodia's `*,bootstrap-signer-controller,token-cleaner-controller` first, then the tenant's entries, and since the controller manager takes the first entry that names a controller, a tenant entry disables any controller but those two ([helper.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/controller-manager/app/helper.go#L60-L80)) |
| `scheduler.{extraArgs, featureGates, resources, configRef}` | | | `extraArgs` by the same rule, none merged; `configRef` names a ConfigMap with a `KubeSchedulerConfiguration` whose client connection Lodia overrides; from phase 2 (until then `Unsupported`) |
| `addons.konnectivity.version` | string | from the version table | pin only to hold a version across a Kubernetes upgrade |
| `addons.konnectivity.agent.mode` | `DaemonSet` \| `Deployment` | `DaemonSet` | `Deployment` takes `replicas`; `hostNetwork` defaults false |
| `addons.kubeProxy.{enabled, mode}` | | `true`, `iptables` | `enabled: false` for CNIs that replace kube-proxy |
| `addons.kubeProxy.healthzBindAddress` | IP address | loopback | where kube-proxy serves its health port, 10256, which Tenant appends; beyond loopback for cloud load balancers |
| `addons.kubeProxy.conntrack.maxPerCore` | int ≥ 0 | kube-proxy's, 32768 ([defaults.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/proxy/apis/config/v1alpha1/defaults.go#L81-L83)) | kube-proxy's `conntrack.maxPerCore`, the connections tracked per CPU core; 0 leaves the node's `nf_conntrack_max` as it is ([types.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/kube-proxy/config/v1alpha1/types.go#L101-L104)), which a node in a container needs: kind sets 0 for its nodes, since `nf_conntrack_max` is global and affects other network namespaces ([config.go](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/pkg/cluster/internal/kubeadm/config.go#L633-L636)) |
| `addons.coreDNS.{enabled, replicas}` | | `true`, 2 | |
| `placement.{nodeSelector, tolerations, priorityClassName}` | | operator defaults | applies to every control plane pod of this tenant; etcd placement comes from the `DatastoreClass` |
| `maintenance.window.{begin, end, timeZone}` | `HH:MM`, `HH:MM`, IANA zone | absent = any time | end may be before begin (crosses midnight) |
| `maintenance.autoUpdate.kubernetesPatch` | bool | `false` | newest patch of the running minor, applied in the window |
| `hibernation.enabled` | bool | unset, the schedules decide | an explicit override of the schedules |
| `hibernation.schedules[]` | `{start, end, timeZone}` | | cron expressions; `start` puts the control plane to sleep, `end` wakes it |
| `restoreFrom.backupRef` | `{name}` | | set only at creation (CEL: cannot be added or changed later); the datastore starts from that snapshot, from phase 2 on ([roadmap](../roadmap.md)): until then nothing reads it, and a `ControlPlane` created with it starts empty |
| `health.holdControllerManager` | bool | `true` | hold the controller manager's leader lease while most nodes are silent ([controller-manager-hold](../decisions/controller-manager-hold.md)); `false` releases a hold in progress |
| `paused` | bool | `false` | Lodia stops reconciling but keeps watching; `Progressing=False`, reason `Paused`; the finalizer stays, so a paused `ControlPlane`'s deletion waits until it is unpaused |

A hostname is a lowercase DNS name whose labels are at most 63 characters,
and not digits and dots alone, which read as an IP address.

## status

| field | written by | meaning |
|---|---|---|
| `observedGeneration` | Lifecycle | the newest `metadata.generation` that every readiness condition (`CertificatesReady`, `DatastoreReady`, `APIServerReady`, `AccessReady`, `AddonsApplied`, `Reachable`) has been reported for |
| `conditions[]` | each controller its own | `CertificatesReady`, `CAValidityAcceptable`, `DatastoreReady`, `APIServerReady`, `AccessReady`, `AddonsApplied`, `AddonsAvailable`, `Reachable`, `ControllerManagerHeld`, `Progressing`, `Hibernated`, `Ready`; each with `reason`, `message`, `observedGeneration` |
| `target.version` | Lifecycle | the Kubernetes version being rolled now; one minor at a time towards `spec.version`; until upgrades are built (phase 2) it follows `spec.version` within its minor only |
| `target.operatorVersion` | Lifecycle | the Lodia version whose templates this tenant's pods are rendered with |
| `target.hibernated` | Lifecycle | the desired sleep state |
| `target.rotation.{clientCA, ca, serviceAccountKey, etcdEncryptionKey, etcdCA, frontProxyCA}` | Lifecycle | `Idle` \| `Preparing` \| `Prepared` \| `Completing` |
| `target.reason`, `target.scheduledFor` | Lifecycle | why the target lags `spec`: `Unsupported` (a `spec.version` this release does not run, or a minor upgrade); `WaitingForWindow` until `scheduledFor`, `FleetLimit`, `Failed` (phase 2) |
| `kubernetes.version` | Lifecycle | the version every component runs |
| `kubernetes.upgrade.{from, to, step, startedAt, message}` | Lifecycle | present while an upgrade runs or is stuck |
| `endpoint` | Workloads | written whole once the public address exists, absent until then; each address a host (an IP address, an IPv6 one without brackets, or a hostname) and a port |
| `endpoint.apiServer.{host, port}` | Workloads | the API server's public endpoint; PKI puts it into the serving certificate and the kubeconfigs, Tenant into `cluster-info`, `kubeadm-config` and kube-proxy's kubeconfig, and Health probes it |
| `endpoint.konnectivity.{host, port}` | Workloads | where konnectivity agents reach the konnectivity server: the API server's host in `loadBalancer` and `nodePort` modes with its own port, `hostnames.konnectivity` in `gateway` mode; PKI names the host in the konnectivity server's certificate, and Tenant gives it to the agents |
| `endpoint.inCluster.{host, port}` | Workloads | where the tenant's pods reach the API server, an IP address (CEL): the balancer's IP and 6443 in `loadBalancer` mode, the node's InternalIP and the node port of 6443 in `nodePort` mode; the endpoint of the tenant's `kubernetes` Service, which Tenant writes as its Endpoints and EndpointSlice, since the API server runs with `--endpoint-reconciler-type=none` ([api-exposure](../decisions/api-exposure.md)) |
| `credentials.adminKubeconfigSecretRef` | PKI | `<name>-admin-kubeconfig` |
| `credentials.caBundleSecretRef` | PKI | `<name>-ca-bundle`: the CAs a client must trust now (two during a rotation) |
| `credentials.rotation.<credential>.{phase, lastInitiated, lastCompleted}` | PKI | the stages per credential (the same keys as `target.rotation`), as Gardener reports them |
| `datastore` | Datastore | written whole once the store exists, absent until then |
| `datastore.endpoints[].{host, port}` | Datastore | where the API server reaches etcd: for a dedicated etcd, etcd-druid's client Service; Workloads builds `--etcd-servers` from it |
| `datastore.class.{name, generation}` | Datastore | the `DatastoreClass` a dedicated etcd was built from, and the generation of it the `Etcd` runs; Lifecycle reads it in phase 2 |
| `datastore.members.{total, ready}` | Datastore | the members the `Etcd` asks for and those ready |
| `datastore.dbSizeBytes`, `datastore.lastBackup.{time, kind, revision}` | Datastore | phase 2, with backups and metrics |
| `nodes.{total, ready, outdatedKubelets, sampledAt}` | Health | from a periodic list of nodes and leases in the tenant |
| `health.controllerManagerHold.{since, silentNodes, countedNodes}` | Health | present while Lodia holds the controller manager's leader lease (`ControllerManagerHeld`) |

## Conditions and their reasons

Every condition type has one writer, and every reason is a constant in
`api/v1alpha1/conditions.go`, named by one rule: `Reconciling` is a writer's
own first pass, `Unknown`, at the start of its first visit; `WaitingFor<X>`
is an input awaited, `X` a condition type without `Ready` or a status field;
`Unsupported` is what this release does not build, the field named in the
message; `<Capability>Unsupported` is an environment that cannot serve the
spec; `<Kind>NotFound` is a named object that does not exist; `Refused` is
the API server a controller writes to refusing one of its objects, the
object named in the message; `ObjectsControlledElsewhere` is an object under
the writer's names that another owner keeps, the object and its owner named
in the message. A reader counts a condition only while its
`observedGeneration` is the object's generation
([independent-controllers](../decisions/independent-controllers.md)). A
reason with a phase beside it is not written before that phase.

| condition | writer | True, and its reason | False or Unknown reasons |
|---|---|---|---|
| `CertificatesReady` | PKI | every certificate, key and kubeconfig exists, verifies, and is valid for more than its renewal margin or until its CA expires (`Issued`) | `Reconciling`; `WaitingForEndpoint` (until Workloads publishes `status.endpoint`); `Issuing` (a Secret PKI has to change is not written yet: the message names it, the error is in Lodia's log); `CAMissing` (a CA's key is missing or unreadable), `CAExpired` (a CA is past its `NotAfter`): in both, the message names the CA and nothing it signs is written until it is restored or rotated; `SecretsControlledElsewhere` (a Secret under one of PKI's names has another owner, which the message names: a previous `ControlPlane` of the same name until the garbage collector deletes it, anything else until a person releases it; PKI writes no Secret meanwhile); `SecretUnreadable` (the encryption configuration or the ServiceAccount key cannot be read, and PKI never replaces it); `Unsupported` (`apiServer.encryption.provider: kms`) |
| `CAValidityAcceptable` | PKI | every CA is valid for more than a year (`Valid`); not part of `Ready`, since a CA in its last year, or a rotation under way, leaves every certificate valid | `Reconciling`; `CAExpiring`, the message naming each CA and its `NotAfter`, as Gardener's `CACertificateValiditiesAcceptable` ([shoot status](https://github.com/gardener/gardener/blob/v1.154.0/docs/usage/shoot/shoot_status.md#L105-L109)); `Unknown`, `CAMissing`, while a CA's key cannot be read |
| `AccessReady` | PKI | the tenant's ClusterRoleBindings for `lodia:operator`, `lodia:cluster-admins` and `lodia:kubelet-api-admins` exist, checked with `lodia:operator` (`Bound`) | `Reconciling`; `WaitingForAPIServer`; `Refused` (the tenant refuses a binding, which the message names); `Binding` (the tenant has not confirmed the bindings since its API server became ready; while it does not answer, a True stays, since reaching it is `Reachable`'s to report) |
| `DatastoreReady` | Datastore | the etcd accepts writes on the certificate generation the `Etcd` names (`Ready`) | `Reconciling`; `WaitingForCertificates` (no complete generation of the etcd certificates this `ControlPlane` controls); `Provisioning` (the etcd is on its way, the message says what is awaited); `DatastoreClassNotFound` (the class `className` names, or the one this `ControlPlane`'s claims record, does not exist, or no class is the default); `DatastoreClassAmbiguous` (more than one class is the default, the message naming them); `VolumesExist`; `Refused` (the API server refuses the `Etcd` Lodia renders, the message naming it and its certificate and class generations, or, once the etcd is otherwise ready, the etcd's NetworkPolicy `<cp>-etcd`); `ObjectsControlledElsewhere` (once the etcd is otherwise ready: another owner keeps the NetworkPolicy `<cp>-etcd`, which the message names with that owner: a previous `ControlPlane` of the same name until the garbage collector deletes it, anything else until a person releases it; the `Etcd` still takes each certificate generation); `Deleting`; `Unsupported` (`datastore.shared`; and `highAvailability`, whose etcd runs one member and so is never True); `QuorumLost`, `Restoring` (phase 2); `DatastoreNotFound`, `Hibernated` (phase 3) |
| `APIServerReady` | Workloads | each of the three Deployments has observed its generation and has every replica updated and ready (`Ready`) | `Reconciling`; `WaitingForEndpoint` (the load balancer has no address yet, or no management node is Ready); `WaitingForTarget`; `WaitingForCertificates` (also while a Secret a component reads once lacks PKI's data hash); `WaitingForDatastore` (also until `status.datastore.endpoints` exists); `RollingOut`; `Unsupported` (a field this release does not render, named in the message: `endpoint.gateway` until phase 2, the `Compact` layout, `highAvailability`, `apiServer.replicas` above 1, `apiServer.advertiseAddress` outside gateway mode, JWT authentication, an audit policy, an admission or a scheduler configuration, `serviceAccount.publicDiscovery`; the endpoint stays published but in gateway mode); `GatewayUnsupported` (phase 2: the Gateway lacks a feature the mode needs, [api-exposure](../decisions/api-exposure.md)); `LoadBalancerHostnameUnsupported` (a load balancer that reports a host name only); `Refused` (the management API server refuses one of Workloads' objects, which the message names: a quota on load balancers, an admission policy; the pass is retried with backoff); `ObjectsControlledElsewhere` (an object under one of Workloads' names has another owner, which the message names: a previous `ControlPlane` of the same name until the garbage collector deletes it, anything else until a person releases it; Workloads writes no Deployment, ConfigMap or NetworkPolicy meanwhile, nor the Service when it is the one held); `Hibernated` (phase 3) |
| `AddonsApplied` | Tenant | the inventory is applied and pruned for this generation (`Applied`) | `Reconciling`; `WaitingForAccess`, `WaitingForTarget`, `WaitingForEndpoint`; `Refused` (the tenant refuses an object, or the read of the inventory, which the message names); `Applying` (an apply not finished: the message names an object that exists and is not Lodia's, an inventory that cannot be read, or a tenant that never answered) |
| `AddonsAvailable` | Tenant | kube-proxy, CoreDNS and the agents report ready (`Available`); not part of `Ready`, since their pods need the owner's nodes | `Reconciling`; `WaitingForAccess`, `WaitingForTarget`, `WaitingForEndpoint`; `RolloutPending`; `Applying`, `Refused` (no rollout read yet: the reason and the message of what stopped the pass) |
| `Reachable` | Health | the public endpoint answers `/readyz` (`Reachable`); phase 2 adds the internal probe and the leader leases of the controller manager and the scheduler, renewed by any holder, Lodia's hold included | `Reconciling`; `WaitingForEndpoint`; `WaitingForCertificates` (`<cp>-ca-bundle` is not written yet); `ExternalProbeFailed` (three probes in a row); `Hibernated` (phase 3) |
| `ControllerManagerHeld` | Health, phase 2 | Lodia holds the controller manager's leader lease while most nodes are silent and the API answers (`NodesSilent`); not part of `Ready`, since the nodes are cut off, not the control plane | `NotHeld`; `HoldBlocked` (most nodes are silent and another holder keeps the lease, named in the message) |
| `Progressing` | Lifecycle | an upgrade, rotation stage, hibernation transition or deferred rollout is running (phase 2) | `Idle`; `Paused`; `Unsupported` (the minor of `spec.version` is not one Lodia runs, or `spec.version` changes the minor, until upgrades are built; the target stays); `WaitingForWindow`, `FleetLimit`, `Failed` (phase 2) |
| `Hibernated` | Lifecycle, phase 3 | every component is scaled to zero and the data is kept | `Awake`, `FallingAsleep`, `WakingUp` |
| `Ready` | Lifecycle | `CertificatesReady`, `DatastoreReady`, `APIServerReady`, `AccessReady`, `AddonsApplied` and `Reachable` are True for this generation (`Ready`) | `Deleting` while the `ControlPlane` is being deleted; otherwise the status, reason and message of the first of the six, in that order, that is not True, or `Unknown`, `LatestGenerationNotObserved` (kstatus's reason for it, [generic.go](https://github.com/kubernetes-sigs/cli-utils/blob/v0.37.2/pkg/kstatus/status/generic.go#L89-L94)) while that one is missing or reported for an older generation |
