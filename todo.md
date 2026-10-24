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
