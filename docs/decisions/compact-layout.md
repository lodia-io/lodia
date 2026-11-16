# Lodia has no Compact layout

Decided 2026-11-12; it withdraws the `Compact` layout decided on 2026-10-25, which was never built.

## Decision

Every control plane runs a Deployment per component ([component-per-deployment](component-per-deployment.md)); no layout puts every component in one pod.

## Why

`Compact` was to run a tenant's whole control plane in one pod.
It would save pods and no memory, since the same processes run in one pod as in four, and how many pods a node holds is the management cluster's `maxPods`; and it would cost a second shape of what Workloads renders, with an upgrade step of its own.

## Consequences

- `spec.layout` leaves `v1alpha1` with the other fields no controller reads ([roadmap](../roadmap.md)); until then the schema accepts `Compact` and Workloads refuses it with `Unsupported`.

## Code

- `internal/controller/workloads/workloads.go` — refuses `layout: Compact` with `Unsupported`.
