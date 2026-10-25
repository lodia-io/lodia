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
- Decide the deletion policy of a dedicated etcd's volumes (`Delete` or
  `Retain`) and of its backups after a `ControlPlane` is deleted; Gardener
  keeps backups for a configurable retention period. etcd-druid keeps both
  when an `Etcd` is deleted, so either policy is Brygga's code (etcd spike,
  finding 6).
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

## From the PKI controller (2026-10-25)

- A certificate is valid from the second it is issued; a node whose clock
  runs behind refuses a freshly renewed serving certificate until it catches
  up. kubeadm sets a leaf's `NotBefore` to its CA's
  ([`NewSignedCert`](https://github.com/kubernetes/kubernetes/blob/v1.35.0/cmd/kubeadm/app/util/pkiutil/pki_helpers.go));
  decide whether to backdate, and by how much, before nodes join.
- A changed `apiServer.encryption.provider` puts the new provider first with
  a new key and keeps the previous ones readable, but nothing rewrites the
  stored Secrets or drops the old provider, and with two API server replicas
  one may write with the new key before the other has reloaded the file (up
  to a minute). That is the etcd encryption key rotation of
  [staged-credential-rotation](docs/decisions/staged-credential-rotation.md);
  until it is built, document the change as unsupported, or refuse it in the
  schema.
- `apiServer.encryption.provider: kms` is accepted by the schema and turns
  `CertificatesReady` False with `Issuing` and a message; refuse it in CEL
  until the provider is built.
- Brygga reaches a tenant's API server through its in-namespace Service and
  ignores `HTTPS_PROXY` for that connection; the chart's documentation should
  say so for sites that set a proxy on every pod.
- The e2e suite installs the CRDs and applies the examples; nothing runs the
  operator against kind yet. The phase 1 gate adds that, and with it a check
  that a `ControlPlane` reaches `CertificatesReady` on a real cluster.
