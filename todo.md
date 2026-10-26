# todo

Capture inbox. Findings that fall outside the current task land here and are
triaged into the Notion **Tasks** database at the end of a task. Nothing here
is a decision — a decision is a file in `docs/decisions/`.

## From the design (2026-10-23)

- List the Gateway implementations that report `TLSRouteModeTerminate`,
  `TLSRouteModeMixed` and `ListenerSet` support today (Envoy Gateway, Istio,
  Cilium, NGINX Gateway Fabric, Traefik), for the install docs.
- Check whether kube-apiserver reloads `--proxy-client-cert-file` and whether
  konnectivity-server reloads `--cluster-cert`; each proven reload removes a
  rollout from certificate renewal.
- Decide the deletion policy of a dedicated etcd's backups after a
  `ControlPlane` is deleted; Gardener keeps backups for a configurable
  retention period. etcd-druid keeps them when an `Etcd` is deleted, so the
  policy is Brygga's code (etcd spike, finding 6). The volumes follow the
  class's `storage.reclaimPolicy` since the Datastore controller (2026-10-25).
- Measure, not assume, the idle footprint of one control plane in phase 1 and
  replace the planning numbers in `docs/operations.md`.
- etcd-druid's restore keeps the snapshot's revisions (no
  `--bump-revision`); propose the bump upstream in etcd-backup-restore, and
  until then roll the tenant's API server after any restore Brygga detects.
  The etcd spike measured the stale cache (finding 5); how the Datastore
  controller detects a restore is still open.
- etcd-druid's scale at ~1000 `Etcd` objects with its default
  `concurrentSyncs: 3` is not documented; measure in phase 4 or ask the
  maintainers.

## From the skeleton (2026-10-24)

- kube-api-linter on `api/`, as `releases-and-support` lists; it is not in
  `.golangci.yml` yet.
- Before the repository goes public: an address for conduct reports
  (`CODE_OF_CONDUCT.md` names only the maintainers), private vulnerability
  reporting switched on (`SECURITY.md` points at it), and `GOVERNANCE.md` and
  `ADOPTERS.md` from the CNCF template.
- The workflow's actions are pinned by commit with nothing to bump them;
  Dependabot or Renovate for `github-actions` and `gomod`.

## From brygga-signer (2026-10-23)

- The signer's socket is mode 0600 and owned by the signer's user (65532 on
  distroless `nonroot`); kube-apiserver can dial it only as that user or as
  root. When the Workloads controller runs the API server as non-root, both
  containers need the same user.
- `.ko.yaml` pins the base image by digest with nothing to bump it; add it to
  the Renovate configuration with the other base images.
- The signer logs a failed load at error level when it reads a Secret volume
  while the kubelet removes the previous version; the next event reloads the
  complete set. If that noise matters, retry once before logging.

## From the etcd spike (2026-10-24)

- Upstream for etcd-druid: the etcd container's readiness probe takes its
  scheme from `backup.tls` while the wrapper serves HTTPS whenever client TLS
  is on; and a failing pre-scale snapshot shows only on its `EtcdOpsTask`, not
  on the `Etcd` (findings 1 and 4).
- Upstream for etcd-backup-restore: the sidecar keeps serving its first
  serving certificate after the Secret changes (finding 3).

## From deciding the design (2026-10-25)

- `KubeconfigRequest`'s schema refuses only `system:masters`;
  [short-lived-kubeconfigs](docs/decisions/short-lived-kubeconfigs.md) now
  refuses every user and group under `system:` and every user under
  `brygga:`. The rule and its envtest cases come with the controller that
  answers requests.
- `loadBalancer` mode with a load balancer that reports only a host name
  (AWS NLB and CLB): `--advertise-address` takes an IP, so the `kubernetes`
  endpoint needs another source there; decide before that mode is supported
  on such a provider.
- client-go reloads a kubeconfig's CA file only with `ClientsAllowCARotation`,
  on by default since 1.36
  ([known_features.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/client-go/features/known_features.go#L115-L117)):
  on 1.35 a trust anchor rotation restarts every component whose kubeconfig
  names the CA file; plan it into the phase 3 rotation.
- Argo CD has no generic health check for custom resources: the chart ships
  a Lua health check for `ControlPlane` that reads `status.observedGeneration`
  and `Ready`
  ([health checks](https://github.com/argoproj/argo-cd/blob/v3.5.6/docs/operator-manual/health.md#L61-L83));
  Flux reads the same through kstatus, where a permanently failing object
  stays InProgress until a timeout unless it reports `Stalled`
  ([kstatus](https://github.com/kubernetes-sigs/cli-utils/blob/v0.37.2/pkg/kstatus/status/status.go#L102-L183)).

## From the Datastore controller (2026-10-25)

- Nothing reads `spec.restoreFrom` until restore is built in phase 2: a
  `ControlPlane` created with it starts on an empty etcd. When restore lands,
  test the `VolumesExist` remedy end to end: the claims kept, a `Backup` of
  the old cluster, a new `ControlPlane` with `restoreFrom` that serves the old
  data.
- The backup-restore sidecar's `spec.backup.resources` have no field in the
  `DatastoreClass` and stay etcd-druid's default (50m / 128Mi at v0.39.0);
  the operator configuration's `defaults.resources` should cover them once
  phase 1 measures.
- The chart's ClusterRole takes the Datastore controller's rules from the
  table in `docs/README.md` (Identity and tenancy), which `datastoreRules` in
  `internal/controller/datastore/datastore_test.go` enforces; the chart and
  the test should read one list.
- The shared `Placement` and `resources` types get the rules of pod
  validation with Workloads, which renders them for the `ControlPlane` in
  phase 1: the class's `placement` and `etcd.resources` have none yet. Until
  then a class value the apps API refuses in etcd-druid's StatefulSet shows as
  `DatastoreReady=False`, since etcd-druid returns before it stamps
  `observedGeneration`
  ([reconciler.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/controller/etcd/reconciler.go#L127-L144)):
  the running member stays, later certificate generations stop reaching it
  until the class is fixed, and the reason is in the `Etcd`'s
  `status.lastErrors`.
- No schema can check what pod admission refuses in a pod the StatefulSet
  API accepted: a `priorityClassName` that names no PriorityClass, or one
  deleted later, and a LimitRange or ResourceQuota in the tenant namespace.
  The rolling update deletes the old pod first and then cannot create the new
  one
  ([stateful_set_control.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/statefulset/stateful_set_control.go#L748-L760)),
  so a single member goes down at its next roll, and every certificate
  generation is a roll. `docs/operations.md` names the symptom.
- A class edit that reaches the `Etcd` rolls its members at once; the decision
  wants it in the tenant's maintenance window, which Lifecycle gates in phase
  2. The `Etcd` records the class generation it was rendered from for that.
- `<cp>-etcd-backup` stays when a class drops its backup store; harmless, the
  `ControlPlane` garbage-collects it.
- Whether etcd-backup-restore reads rotated store credentials without a
  restart: etcd-druid mounts `<cp>-etcd-backup` and puts no checksum of it in
  the pod template
  ([builder.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/builder.go#L219-L226)),
  so a new copy rolls nothing; the rotation in `docs/operations.md` revokes
  the old keys once every copy holds the new ones, which is safe only if the
  sidecar re-reads them.
- Backup credentials scoped to one tenant's prefix, so a tenant namespace's
  `<cp>-etcd-backup` reaches only that tenant's snapshots and not every
  tenant's on the class (`docs/README.md`, Identity and tenancy).
- The e2e suite runs the PKI and Datastore controllers in the test process
  and needs `git` and `helm`; the suite against Brygga's image and chart comes with the
  chart, and `make e2e` could check for the tools.
- The cache keeps metadata only for Secrets because this controller watches
  them as `PartialObjectMetadata`; nothing stops a future controller from
  starting a typed Secret informer. A test or a cache transform should
  enforce the rule.

## From the PKI controller (2026-10-25)

- A changed `apiServer.encryption.provider` puts the new provider first with
  a new key and keeps the previous ones readable, but nothing rewrites the
  stored Secrets or drops the old provider, and with two API server replicas
  one may write with the new key before the other has reloaded the file (up
  to a minute). That is the etcd encryption key rotation of
  [staged-credential-rotation](docs/decisions/staged-credential-rotation.md);
  until it is built, document the change as unsupported, or refuse it in the
  schema.
- `apiServer.encryption.provider: kms` is accepted by the schema; PKI issues
  everything else and reports `CertificatesReady` False with `Unsupported`.
  Refuse it in CEL until the provider is built.
- The kubelet signer narrows nothing: whoever holds it can sign any identity
  the API server accepts. A signer outside the controller manager, serving
  only kubelet CSRs (those for `kubernetes.io/kube-apiserver-client-kubelet`,
  [types.go](https://github.com/kubernetes/api/blob/v0.37.1/certificates/v1/types.go#L155-L158)),
  would make the narrowing true; decide before the client CA rotation is
  built.
- Brygga reaches a tenant's API server through its in-namespace Service and
  ignores `HTTPS_PROXY` for that connection; the chart's documentation should
  say so for sites that set a proxy on every pod.
- The e2e suite installs the CRDs and applies the examples; nothing runs the
  operator against kind yet. The phase 1 gate adds that, and with it a check
  that a `ControlPlane` reaches `CertificatesReady` on a real cluster.

## From the PKI review (2026-10-25)

- No PKI-level test pins one clock per pass: `leaf()` issuing at
  `time.Now()` passes the whole suite; a test that delays one apply past a
  second boundary would pin it.
- `TestAPredecessorsSecretsAreNeitherReadNorWritten` passes with the
  metadata lookup disabled, since the labelled predecessor Secrets mask its
  unlabelled CA key; give that path its own case.
- In a blocked pass the CAs are never loaded: a forbidden `brygga:operator`
  then reports "client CA missing", and the bind error replaces the bounded
  requeue with exponential backoff. With `APIServerReady` True and
  `<cp>-brygga-kubeconfig` missing or foreign, `AccessReady` says
  "APIServerReady is not True for this generation", which the state
  contradicts.
- `readEncryptionConfig` decodes leniently, while kube-apiserver decodes
  strictly and with defaults
  ([config.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/server/options/encryptionconfig/config.go#L140-L146),
  [loadConfig](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/server/options/encryptionconfig/config.go#L519-L536));
  with `kms` requested, an unreadable configuration is reported as
  `Unsupported`, not `SecretUnreadable`.
- The previous etcd generation is kept and adopted on completeness alone,
  even when it does not chain to the current etcd CA (a restored
  predecessor's generation, for instance); keep it only if it chains.

## From the contracts audit (2026-10-25)

- Phase 1, wave 3: the etcd NetworkPolicy belongs to the Datastore
  controller, which knows etcd-druid's labels and ports: it admits the API
  server's pods by Workloads' pod label, defined in `api/v1alpha1`, and
  etcd-druid's namespace is a setting of the Datastore controller, not of
  Workloads.
- Phase 2: one `status.target.version` cannot order an upgrade's steps,
  since Workloads and Tenant both render from it; a target per step, decided
  with a status field that says which version each component runs.
- Phase 2: `status.endpoint.tunnel`, the tunnel listener's host and port in
  gateway mode, absent without a tunnel, for Tenant's apiserver-proxy and
  PKI's certificate.
- Phase 2: `status.health` is present only while Brygga holds the controller
  manager; a blocked hold is reported in `HoldBlocked`'s message, and
  `nodes.outdatedKubelets` compares against the tenant's own `/version`.
- Phase 2: `Backup` and `KubeconfigRequest` get a named status writer, and
  `Backup` reports conditions instead of a `phase`.
- Phase 2: the default `advertiseAddress`, `172.20.0.1`, becomes a constant
  that Workloads and Tenant both read.
- Phase 3: `target.rotation.*`, which `Target` lacks, gets a stage enum of
  its own instead of sharing PKI's observed `RotationPhase`, and
  `rotation.ca` becomes `clusterCA`.
- Phase 3: `Hibernated`, with Lifecycle as its writer, and the handled
  `brygga.segla.io/operation` value recorded in `status`.

## From the Tenant controller (2026-10-25)

- A kind node container needs kubelet settings a real node does not
  (`cgroupRoot: /kubelet`, `failSwapOn: false`, eviction thresholds at 0%),
  and kube-proxy in a container cannot raise `nf_conntrack_max`, which the
  hard way avoided with `conntrack.maxPerCore: 0`; the phase 1 gate joins
  with kubeadm's kubelet patches and decides kube-proxy's conntrack setting.
- Nothing watches `<cp>-ca-bundle`: a rotated cluster CA reaches
  `cluster-info` within the apply period; the CA rotation of phase 3 should
  bring a Tenant pass.
- `addons.konnectivity.version` pins the agents' image here; the server's
  is Workloads'.
- A tenant's own admission webhook that fails with a 5xx counts as no
  answer, so a True `AddonsApplied` stays while it keeps an object out.
- The one-minute requeue while the rollout is pending runs the full apply,
  not only the rollout read.
- Every pass builds a new tenant client, transport and RESTMapper.
- `--agent-identifiers=ipv4=$(HOST_IP)` names an agent by its node's IPv4
  address only; IPv6 and dual-stack nodes need `ipv6=`.
- `AddonsAvailable` is not read again while applies are refused: the
  rollout read before stays.
- `--count-server-leases` on the agents is proven only by the wave 3 e2e.
- CoreDNS's replicas are Brygga's: an autoscaler is set back on every
  apply; a later phase lets one own them.

## From the Workloads controller (2026-10-25)

- `highAvailability` (replicas, spread, PodDisruptionBudgets), more than one
  API server replica, JWT authentication, an audit policy, an admission and a
  scheduler configuration, `serviceAccount.publicDiscovery` and
  `apiServer.advertiseAddress` outside gateway mode are not rendered: they
  turn `APIServerReady` False with `Unsupported` until phase 2 builds them.
- The NetworkPolicies isolate each component's ingress alone; the
  default-deny set and egress, the README's arrows, come in phase 2, and
  etcd's with the Datastore controller.
- `--signer-image` is an operator flag until the operator configuration and
  the chart render it.
- The control plane pods set no securityContext: give them one that passes
  Pod Security `restricted`, the API server and the signer under one user
  (From brygga-signer, above).
- Workloads lists the management nodes uncached on every pass of a
  `nodePort` `ControlPlane`; an informer on Nodes, which carry no
  control-plane label and so need a cache rule of their own, would serve
  them from memory.
- In `nodePort` mode the Service takes a node port for 443 as well, which
  nothing dials: the endpoint names the node ports of 6443 and 8132, and
  in-namespace clients use the cluster IP.
- Wave 3's gate checks the tenant API from inside the cluster with a token:
  anonymous requests reach only the health paths and `cluster-info`.
- The e2e asserts `APIServerReady` alone, with the operator in the test
  process outside kind; wave 3 runs it in the cluster.
- Workloads finds another owner's objects under its names in the cache,
  which holds only labelled ones. An unlabelled object it applied for a
  predecessor is taken over silently, with the
  `OwnerReferencesPermissionEnforcement` plugin as without it, now that
  Workloads may delete (before, the plugin refused the apply); one that only
  refers to another `ControlPlane` is applied over once and then held; one
  with another controller is refused by validation, which `APIServerReady`
  reports as `Refused`. Reading the targets uncached, as PKI reads its
  Secrets, would see them all.
