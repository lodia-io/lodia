# Lodia

**The hosted control plane for nodes you do not own, run by a team too small to watch a fleet.**

Lodia runs Kubernetes control planes as pods on a management cluster, one per tenant cluster, and lets worker nodes anywhere join them with `kubeadm`.
Each control plane is an ordinary set of Deployments with an etcd of its own, described by one `ControlPlane` object; six independent controllers keep it certified, stored, running, reachable and up to date, and keep the tenant's workloads alive when the path to its nodes breaks.

```sh
helm install lodia oci://ghcr.io/lodia-io/charts/lodia --version 0.1.0-alpha.1 -n lodia-system --create-namespace
```

[Installation](docs/README.md#installation) lists what the management cluster needs first and how to verify the release.

Measured, each by its test:

- A minor upgrade fails no request: two loops of 20 requests a second, one against the public endpoint and one from a pod in the tenant, record zero failures ([G1](test/e2e/README.md#gate-g1-a-minor-upgrade), every pull request).
- Each minor step is rehearsed on a copy of the tenant first, and a tenant that sets a feature gate the next minor removed stops there with its API server untouched (the same gate).
- With every node cut off from the control plane for 10 minutes, no node is tainted and every pod keeps its UID ([G4](test/e2e/README.md#nightly-gate-g4-nodes-cut-off-from-the-gateway), every night).
- An operator upgrade reaches a tenant only inside its maintenance window ([G5](test/e2e/README.md#gate-g5-an-operator-upgrade-in-windows), every pull request).
- One idle control plane with an etcd of its own takes about 60 millicores and 400 MiB ([Capacity](docs/operations.md#capacity), every night).
- One operator brings a synthetic fleet of 1,000 tenants to `Ready` within 5 minutes of its creation ([Scale](docs/operations.md#scale)).

**Status: phase 3 of the [roadmap](docs/roadmap.md); phase 2's gate is built, and the first release is `v0.1.0-alpha.1`.**
The six controllers are built, high availability included, and the Helm chart in [`charts/lodia`](charts/lodia) installs them.

- [`docs/README.md`](docs/README.md) — what Lodia is: the objects, the controllers, the shape of a tenant namespace, networking, lifecycle, installation and operations.
- [`docs/api/`](docs/api/) — the `ControlPlane` field reference and example manifests.
- [`docs/decisions/`](docs/decisions/) — why it is built this way, one decision per file; [project-home](docs/decisions/project-home.md) has the name's origin.
- [`docs/analysis.md`](docs/analysis.md) — what we learned from the hosted control plane solutions that came before, and where the bar is.
- [`docs/roadmap.md`](docs/roadmap.md) — the phases, and how each one is verified before the next starts.
- [`docs/operations.md`](docs/operations.md) — upgrading and running Lodia; the runbooks.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — building and testing, and what CI runs on every pull request and every night.
- [`CLAUDE.md`](CLAUDE.md) — how to work on the repository.
- [`todo.md`](todo.md) — findings waiting for triage.

Licensed under the [Apache License 2.0](LICENSE).
