# One ControlPlane object describes one tenant cluster; four small objects describe storage classes, shared stores, access and snapshots

Proposed 2026-10-23; not decided yet. Not built.

## Decision

Five custom resources in `brygga.segla.io/v1alpha1`:

| kind | scope | one per | holds |
|---|---|---|---|
| `ControlPlane` | namespaced | tenant cluster | the whole desired state of a control plane: version, availability, datastore choice, endpoint, network, component settings, addons, maintenance, hibernation |
| `DatastoreClass` | cluster | kind of dedicated etcd | the template a dedicated etcd is built from: members, storage, quota, backup store and schedules, defragmentation |
| `Datastore` | cluster | shared etcd pool | endpoints and credentials of an etcd that several tenants share by key prefix |
| `KubeconfigRequest` | namespaced | request | a short-lived kubeconfig for a person; the result is a Secret that expires |
| `Backup` | namespaced | snapshot | an on-demand full snapshot of a tenant's etcd, named so a new `ControlPlane` can be restored from it |

Rules that hold across all of them:

- `spec` is what the user wants, `status` is what Brygga did. Status is a
  subresource; `status.conditions` follow the Kubernetes API conventions
  (`metav1.Condition` with `observedGeneration`); `status.observedGeneration`
  is set only after a full successful pass.
- Everything a `ControlPlane` creates in its namespace carries an
  `ownerReference` to it and the label `brygga.segla.io/control-plane: <name>`;
  deleting the `ControlPlane` garbage-collects it. The dedicated etcd's
  volumes and its backups follow the `DatastoreClass` reclaim policy.
- A `ControlPlane` has one finalizer, `brygga.segla.io/controlplane`; deletion
  takes a final snapshot when backups are configured, then lets garbage
  collection run.
- Imperative operations that are not desired state — starting or completing a
  credential rotation — are requested through the annotation
  `brygga.segla.io/operation`, which Brygga removes when it has recorded the
  request in `status`. The same pattern Gardener uses with
  `gardener.cloud/operation`
  ([credentials rotation](https://github.com/gardener/gardener/blob/master/docs/usage/shoot-operations/shoot_credentials_rotation.md)).
- Validation and defaults live in the CRD schema (CEL and defaults), not in a
  webhook ([no-admission-webhooks](no-admission-webhooks.md)).

The field reference is [`docs/api/controlplane.md`](../api/controlplane.md);
examples are in [`docs/api/examples/`](../api/examples/).

## Why

- A platform that creates a cluster for a customer wants to write one object
  and watch one `Ready` condition. Spreading a cluster across several objects
  (one per component) moves the orchestration to every caller.
- Storage, access and snapshots have their own lifecycles and their own
  owners: a platform administrator defines a `DatastoreClass` once; a person
  asks for a kubeconfig many times; a snapshot outlives the control plane it
  was taken from.
- `DatastoreClass` mirrors `StorageClass`: the operator of the management
  cluster decides what a dedicated etcd looks like, the tenant only names a
  class.

## Rejected

- One object per component (`APIServer`, `ControllerManager`, …) — flexible,
  but every caller re-implements the ordering.
- Putting etcd settings inline in every `ControlPlane` — the same storage and
  backup settings repeated per tenant drift apart; a class is edited once.
- A `JoinToken` resource — a bootstrap token is a Secret in the tenant
  cluster that any admin kubeconfig can create with `kubeadm token create`;
  Brygga documents the command instead of wrapping it.

## Consequences

- The Go types, the CRD YAML and `docs/api/controlplane.md` change together.
- `v1alpha1` may change incompatibly between minor releases of Brygga until
  `v1beta1`; the roadmap says when.

## Code

none yet.
