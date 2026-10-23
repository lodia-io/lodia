# Brygga

Brygga runs Kubernetes control planes as pods on a management cluster, one
per tenant cluster, and lets worker nodes anywhere join them with `kubeadm`.
Each control plane is an ordinary set of Deployments (or a single pod in the
compact layout) with an etcd of its own or a prefix in a shared one,
described by one `ControlPlane` object; six independent controllers keep it
certified, stored, running, reachable and up to date, and keep the tenant's
workloads alive when the path to its nodes breaks.

*Brygga* is Swedish for a ship's bridge, the place a vessel is steered from,
and for brewing a pot of coffee.

**Status: design.** Nothing is built yet; the documents below are the plan
and the reasons behind it.

- [`docs/README.md`](docs/README.md) — what Brygga is: the objects, the
  controllers, the shape of a tenant namespace, networking, lifecycle,
  installation and operations.
- [`docs/api/`](docs/api/) — the `ControlPlane` field reference and example
  manifests.
- [`docs/decisions/`](docs/decisions/) — why it is built this way, one
  decision per file.
- [`docs/analysis.md`](docs/analysis.md) — what we learned from the hosted
  control plane solutions that came before, and where the bar is.
- [`docs/roadmap.md`](docs/roadmap.md) — the phases, and how each one is
  verified before the next starts.
- [`docs/operations.md`](docs/operations.md) — installing, upgrading and
  running Brygga; the runbooks.
- [`CLAUDE.md`](CLAUDE.md) — how to work on the repository.
- [`todo.md`](todo.md) — findings waiting for triage.

Licensed under the [Apache License 2.0](LICENSE).
