# Brygga is an Apache-2.0 project at github.com/segla-io/brygga with the API group brygga.segla.io

Decided 2026-10-23.

## Decision

- **Name:** Brygga. Swedish for a ship's bridge (the place a vessel is steered
  from) and for brewing coffee; a nod to Segla, Swedish for sailing.
- **Home:** `github.com/segla-io/brygga`, beside the already public
  `segla-io/agent`. The Go module path is the repository path. Issues,
  discussions and pull requests live there; CI runs on GitHub Actions.
- **License:** Apache License 2.0, the license of the Kubernetes ecosystem
  and of every neighbour project we studied. Contributions are accepted under
  the Developer Certificate of Origin (DCO sign-off), not a CLA.
- **API group:** `brygga.segla.io`, first version `v1alpha1`. Labels,
  annotations and finalizers use the same prefix
  (`brygga.segla.io/control-plane`, `brygga.segla.io/operation`,
  `brygga.segla.io/datastore` as the finalizer, named after the controller
  that holds it).
- **Field managers:** `brygga/<controller>` (`brygga/pki`, `brygga/workloads`,
  …), one per controller, so server-side apply ownership shows who wrote a
  field.

## Why

- A name and an API group cannot change once an object exists in someone's
  cluster; both were chosen before the first line of code.
- The hosted control plane projects and the tooling around them (Kamaji,
  Gardener, HyperShift, Cluster API, etcd-druid) are on GitHub; being next to
  them is where contributors and issues find us.
- Apache-2.0 carries a patent grant and is what the CNCF requires of projects
  it hosts; it does not stop Segla from selling a platform on top.
- `brygga.segla.io` says who maintains the project, as `kamaji.clastix.io` and
  `core.gardener.cloud` do, and the domain is already ours.

## Rejected

- A GitLab-first repository with a GitHub mirror — issues and pull requests
  would split between two places.
- MIT — no patent grant. AGPL — keeps clouds from reselling it as a service,
  but also keeps out the CNCF and part of the users we want.
- A neutral group such as `brygga.io` — needs a domain nobody owns yet and
  hides who runs the project.

## Consequences

- The repository carries `LICENSE`, and every source file a short Apache-2.0
  header; `CONTRIBUTING.md`, `SECURITY.md` and `CODE_OF_CONDUCT.md` arrive with
  the first code.
- Images and the Helm chart are published to `ghcr.io/segla-io/brygga` and
  `oci://ghcr.io/segla-io/charts/brygga`.

## Code

none: a policy.
