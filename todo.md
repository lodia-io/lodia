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

- The names the controllers share — the finalizer
  `brygga.segla.io/controlplane`, the condition `DatastoreReady`, the label
  `brygga.segla.io/etcd-certificates` and the annotation
  `brygga.segla.io/is-default-class` — are constants of
  `internal/controller/datastore`; move them to `api/v1alpha1` when PKI and
  Lifecycle need them.
- The backup-restore sidecar's `spec.backup.resources` have no field in the
  `DatastoreClass` and stay etcd-druid's default (50m / 128Mi at v0.39.0);
  the operator configuration's `defaults.resources` should cover them once
  phase 1 measures.
- The chart's ClusterRole takes the Datastore controller's rules from the
  table in `docs/README.md` (Identity and tenancy), which `datastoreRules` in
  `internal/controller/datastore/datastore_test.go` enforces; the chart and
  the test should read one list.
- The `DatastoreClass` schema checks `fullSnapshotSchedule` and
  `defragmentation.schedule` only for length; etcd-druid's CRD wants standard
  five-field cron, so a bad schedule fails the `Etcd` apply: the error is in
  the controller's log and `DatastoreReady` keeps what it said before. A
  pattern on the class, or a reason of its own.
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
- The e2e suite runs the Datastore controller in the test process and needs
  `git` and `helm`; the suite against Brygga's image and chart comes with the
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
  everything else and reports `CertificatesReady` False with
  `EncryptionProviderUnsupported`. Refuse it in CEL until the provider is
  built.
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
