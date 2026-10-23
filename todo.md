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
  keeps backups for a configurable retention period.
- Measure, not assume, the idle footprint of one control plane in phase 1 and
  replace the planning numbers in `docs/operations.md`.
- etcd-druid's restore keeps the snapshot's revisions (no
  `--bump-revision`); propose the bump upstream in etcd-backup-restore, and
  until then roll the tenant's API server after any restore Brygga detects.
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
