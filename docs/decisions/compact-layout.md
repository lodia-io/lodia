# Lodia has no Compact layout: every control plane runs a Deployment per component

Decided 2026-11-12; it withdraws the `Compact` layout decided on 2026-10-25, which was never built.

## Decision

- Every control plane runs the layout of [component-per-deployment](component-per-deployment.md); no layout puts every component in one pod.
- `spec.layout: Compact` leaves `v1alpha1` with the other fields no controller reads ([roadmap](../roadmap.md)); until then the schema accepts it and Workloads refuses it with `Unsupported` ([reasons](../api/controlplane.md#conditions-and-their-reasons)).

## Why

- **It saves pods, not memory.**
  The same processes run in one pod as in four, and the API server alone is 227.7 of an idle tenant's 392.5 MiB ([Capacity](../operations.md#capacity)).
- **The pod limit is a setting of the management cluster.**
  A kubelet runs 110 pods by default, and its `maxPods` sets another limit ([kubelet configuration](https://kubernetes.io/docs/reference/config-api/kubelet-config.v1beta1/)).
- **It costs a second shape of what Workloads renders:** a second render path, an upgrade step of its own, in which the API server, the controller manager and the scheduler change version in one rollout, and a held restore's pod without its controller manager.
- A layout can come back as an added field or value, which is a compatible change.

## Rejected

- The `Compact` layout: one Deployment with every component and one replica, a pod per tenant on a shared store and two with a dedicated etcd — for the reasons above.

## Consequences

- A tenant takes four pods of the management cluster, nine when highly available ([component-per-deployment](component-per-deployment.md)); how many tenants a node holds is the management cluster's `maxPods` and pod range.
- The controller manager always runs in a Deployment of its own, which a restore holds at 0 replicas and the hold stops through its lease ([restore-replaces-the-source](restore-replaces-the-source.md), [controller-manager-hold](controller-manager-hold.md)).

## Code

- `internal/controller/workloads/workloads.go` — refuses `layout: Compact` with `Unsupported`.
