# Lodia is an Apache-2.0 project with the Go module go.lodia.io/lodia and the API group lodia.io

Decided 2026-10-23 as Brygga (`github.com/segla-io/brygga`, API group
`brygga.segla.io`); renamed to Lodia on 2026-10-26, before any cluster ran
it.

## Decision

- **Name:** Lodia, from Ukrainian «лодія», an old word for a boat from the
  time of Kyivan Rus': the vessel beside Segla, which is Swedish for
  sailing.
- **Home:** the repository is `github.com/segla-io/brygga` for now and moves
  to the `lodia-io` organization. Issues, discussions and pull requests live
  there; CI runs on GitHub Actions.
- **Go module:** `go.lodia.io/lodia`, a path of the project's own domain, so
  it stays when the repository moves; `go.lodia.io` serves the `go-import`
  page that points it at the repository
  ([remote import paths](https://pkg.go.dev/cmd/go#hdr-Remote_import_paths)).
- **License:** Apache License 2.0, the license of the Kubernetes ecosystem
  and of every neighbour project we studied. Contributions are accepted under
  the Developer Certificate of Origin (DCO sign-off), not a CLA.
- **API group:** `lodia.io`, first version `v1alpha1`. Labels,
  annotations and finalizers use the same prefix
  (`lodia.io/control-plane`, `lodia.io/operation`,
  `lodia.io/datastore` as the finalizer, named after the controller
  that holds it).
- **Field managers:** `lodia/<controller>` (`lodia/pki`, `lodia/workloads`,
  …), one per controller, so server-side apply ownership shows who wrote a
  field.

## Why

- A name and an API group cannot change once an object exists in someone's
  cluster; the rename came while nothing ran it, the API at `v1alpha1`.
- The hosted control plane projects and the tooling around them (Kamaji,
  Gardener, HyperShift, Cluster API, etcd-druid) are on GitHub; being next to
  them is where contributors and issues find us.
- Apache-2.0 carries a patent grant and is what the CNCF requires of projects
  it hosts; it does not stop Segla from selling a platform on top.
- `lodia.io` is the project's own domain: it says who maintains the project,
  as `kamaji.clastix.io` and `core.gardener.cloud` do, without tying it to
  the company that started it.

## Rejected

- A GitLab-first repository with a GitHub mirror — issues and pull requests
  would split between two places.
- MIT — no patent grant. AGPL — keeps clouds from reselling it as a service,
  but also keeps out the CNCF and part of the users we want.
- A group under the company's domain, `brygga.segla.io`, the first choice —
  the project would carry Segla's name in every object; it has a domain of
  its own now.
- The repository path as the Go module path — every import would change when
  the repository moves.

## Consequences

- The repository carries `LICENSE`, and every source file a short Apache-2.0
  header; `CONTRIBUTING.md`, `SECURITY.md` and `CODE_OF_CONDUCT.md` arrive with
  the first code.
- Images and the Helm chart are published to `ghcr.io/lodia-io/lodia` and
  `oci://ghcr.io/lodia-io/charts/lodia`.
- Before the repository goes public, `go.lodia.io/lodia?go-get=1` serves its
  `go-import` page (`todo.md`).

## Code

none: a policy.
