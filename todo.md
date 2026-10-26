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
  policy is Lodia's code (etcd spike, finding 6). The volumes follow the
  class's `storage.reclaimPolicy` since the Datastore controller (2026-10-25).
- etcd-druid's restore keeps the snapshot's revisions (no
  `--bump-revision`); propose the bump upstream in etcd-backup-restore, and
  until then roll the tenant's API server after any restore Lodia detects.
  The etcd spike measured the stale cache (finding 5); how the Datastore
  controller detects a restore is still open.
- etcd-druid's scale at ~1000 `Etcd` objects with its default
  `concurrentSyncs: 3` is not documented; measure in phase 4 or ask the
  maintainers.

## From the skeleton (2026-10-24)

- kube-api-linter on `api/`, as `releases-and-support` lists; it is not in
  `.golangci.yml` yet.
- The workflow's actions are pinned by commit with nothing to bump them;
  Dependabot or Renovate for `github-actions` and `gomod`, leaving the API
  module's `k8s.io` floor alone (releases-and-support).

## From lodia-signer (2026-10-23)

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
  `lodia:`. The rule and its envtest cases come with the controller that
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
- `make e2e` could check for Docker and `helm` before it creates its
  cluster.
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
- Lodia reaches a tenant's API server through its in-namespace Service and
  ignores `HTTPS_PROXY` for that connection; the chart's documentation should
  say so for sites that set a proxy on every pod.

## From the PKI review (2026-10-25)

- No PKI-level test pins one clock per pass: `leaf()` issuing at
  `time.Now()` passes the whole suite; a test that delays one apply past a
  second boundary would pin it.
- `TestAPredecessorsSecretsAreNeitherReadNorWritten` passes with the
  metadata lookup disabled, since the labelled predecessor Secrets mask its
  unlabelled CA key; give that path its own case.
- In a blocked pass the CAs are never loaded: a forbidden `lodia:operator`
  then reports "client CA missing", and the bind error replaces the bounded
  requeue with exponential backoff. With `APIServerReady` True and
  `<cp>-lodia-kubeconfig` missing or foreign, `AccessReady` says
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

- Phase 2: one `status.target.version` cannot order an upgrade's steps,
  since Workloads and Tenant both render from it; a target per step, decided
  with a status field that says which version each component runs, and
  `status.kubernetes.version` written from it: Cluster API creates no worker
  while a control plane reports no version
  ([preflight](https://github.com/kubernetes-sigs/cluster-api/blob/v1.14.3/core/reconcilers/machineset/machineset_preflight.go#L174-L181),
  [provisioning](https://github.com/kubernetes-sigs/cluster-api/blob/v1.14.3/internal/contract/controlplane.go#L277-L293)).
- Phase 2: `status.endpoint.tunnel`, the tunnel listener's host and port in
  gateway mode, absent without a tunnel, for Tenant's apiserver-proxy and
  PKI's certificate.
- Phase 2: `nodes.outdatedKubelets` compares against the tenant's own
  `/version`.
- Phase 2: `Backup` and `KubeconfigRequest` get a named status writer, and
  `Backup` reports conditions instead of a `phase`.
- Phase 2: the default `advertiseAddress`, `172.20.0.1`, becomes a constant
  that Workloads and Tenant both read.
- Phase 3: `target.rotation.*`, which `Target` lacks, gets a stage enum of
  its own instead of sharing PKI's observed `RotationPhase`, and
  `rotation.ca` becomes `clusterCA`.
- Phase 3: `Hibernated`, with Lifecycle as its writer, and the handled
  `lodia.io/operation` value recorded in `status`.

## From the Tenant controller (2026-10-25)

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
- CoreDNS's replicas are Lodia's: an autoscaler is set back on every
  apply; a later phase lets one own them.

## From the Workloads controller (2026-10-25)

- `highAvailability` (replicas, spread, PodDisruptionBudgets), more than one
  API server replica, JWT authentication, an audit policy, an admission and a
  scheduler configuration, `serviceAccount.publicDiscovery` and
  `apiServer.advertiseAddress` outside gateway mode are not rendered: they
  turn `APIServerReady` False with `Unsupported` until phase 2 builds them.
- The NetworkPolicies isolate each component's ingress alone; the
  default-deny set and egress, the README's arrows, come in phase 2.
- The control plane pods set no securityContext: give them one that passes
  Pod Security `restricted`, the API server and the signer under one user
  (From lodia-signer, above).
- Workloads lists the management nodes uncached on every pass of a
  `nodePort` `ControlPlane`; an informer on Nodes, which carry no
  control-plane label and so need a cache rule of their own, would serve
  them from memory.
- In `nodePort` mode the Service takes a node port for 443 as well, which
  nothing dials: the endpoint names the node ports of 6443 and 8132, and
  in-namespace clients use the cluster IP.
- Workloads finds another owner's objects under its names in the cache,
  which holds only labelled ones. An unlabelled object it applied for a
  predecessor is taken over silently, with the
  `OwnerReferencesPermissionEnforcement` plugin as without it, now that
  Workloads may delete (before, the plugin refused the apply); one that only
  refers to another `ControlPlane` is applied over once and then held; one
  with another controller is refused by validation, which `APIServerReady`
  reports as `Refused`. Reading the targets uncached, as PKI reads its
  Secrets, would see them all.

## From the etcd NetworkPolicy (2026-10-26)

- Phase 2, with three members: etcd-druid removes a member in a scale-in
  through the client Service
  ([memberremoval.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/statefulset/memberremoval.go#L31-L39),
  [client.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/client/etcd/client.go#L82-L95)),
  so the etcd's policy admits etcd-druid's namespace to the client port then,
  and the members to each other's backup-restore port: a sidecar that is not
  the backup leader forwards snapshot requests to the leader's
  ([httpAPI.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/server/httpAPI.go#L281-L288)),
  which Gardener opens between members for the same reason
  ([etcd.go](https://github.com/gardener/gardener/blob/v1.154.0/pkg/component/etcd/etcd/etcd.go#L404-L411)).
- The etcd's policy admits no metrics scraper, as the controller manager's
  and the scheduler's do not: a ServiceMonitor for the etcd and sidecar
  metrics needs its scraper admitted, the phase 2 arrow from the metrics
  scrapers to the health ports.
- The README's paragraph on what the processes accept names
  etcd-backup-restore v0.41.2 as the sidecar etcd-druid v0.39.0 runs; with
  the `UpgradeEtcdVersion` gate, beta, on by default and set in etcd-druid's
  chart too, it runs etcd-wrapper v0.8.2 and etcd-backup-restore v0.45.1
  ([image.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/utils/image.go#L39-L44),
  [values.yaml](https://github.com/gardener/etcd-druid/blob/v0.39.0/charts/values.yaml#L22-L24),
  the etcd spike's finding 9): its sources should be read again at v0.45.1.
- The Datastore controller reads its previous `DatastoreReady` from the
  cache: a pass that starts before the cache holds the previous pass's
  write applies a transition a second time, moving `lastTransitionTime` by
  the gap, a second in envtest, where a test that compared the
  ControlPlane's resourceVersion right after a transition to True caught it
  once in fifteen runs.

## From the chart (2026-10-26)

- etcd-druid's CRDs come from its chart's `crds/` directory, which Helm
  applies only when it installs a release (`docs/README.md`, Installation):
  a bump of the dependency needs its CRDs applied by hand first; decide
  whether the chart templates them or the release notes carry the step.
- The operator's pods set no requests or limits until the operator is
  measured.

## From the phase 1 gate (2026-10-26)

- The e2e suite reaches a tenant's API through port-forwards to the API
  server pod, one per connection (`forward` in `test/e2e/gate_test.go`),
  only because a kind node's address is not routable from macOS, where the
  suite also runs. Real clients, and kubeadm on a joining node, use the
  tenant's endpoint, and Lodia never relies on a port-forward. Remove it
  once the suite's tenant clients run where the endpoint is routable: on
  Linux CI it already is; on macOS, from a container on kind's network, for
  instance.

## From #29's review (2026-10-26)

- The idle table leaves out a control plane pod that is not Running with
  every container ready (`pods` in `test/e2e/conformance_test.go`, through
  `running` in `datastore_test.go`): the sum can come out short, and
  `pods[0]` panics when none is left. Leave out only terminating pods and
  fail on the rest.
- A `CONFORMANCE_FOCUS` that selects no spec passes; hydrophone's
  `--extra-ginkgo-args --fail-on-empty=true` would fail it
  ([suite.go](https://github.com/onsi/ginkgo/blob/v2.32.0/internal/suite.go#L527-L530)).
- `--conformance-image` repeats what hydrophone v0.8.0 derives from the
  tenant's server version
  ([root.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/cmd/root.go#L205-L219));
  drop it.
- `openTenant` builds its client with `clientcmd.NewDefaultClientConfig`,
  which attaches the default loading rules (`$KUBECONFIG`,
  `~/.kube/config`), though nothing reads or writes a kubeconfig through
  them; `NewNonInteractiveClientConfig` attaches none.
- The local serial run's one failure needs no chasing: the pod generation
  spec's 20 s wait for `observedGeneration`
  ([pods.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/e2e/node/pods.go#L531))
  ran out while that network pulled `nginx:1.27.0-2` in 28.8 s, and the
  spec passed with the images pulled beforehand and in CI.
- Measure a highly available control plane's idle footprint; phase 2's
  scope and gate do not list it.
