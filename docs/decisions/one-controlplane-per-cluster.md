# One ControlPlane object describes one tenant cluster; three small objects describe storage classes, access and snapshots

Decided 2026-10-25.
The four kinds are built as types and CRDs; the six controllers act on `ControlPlane`, the Datastore controller also on `DatastoreClass` and `Backup`, PKI on `KubeconfigRequest`.

## Decision

Four custom resources in `lodia.io/v1alpha1`:

| kind | scope | one per | holds |
|---|---|---|---|
| `ControlPlane` | namespaced | tenant cluster | the whole desired state of a control plane: version, availability, datastore choice, endpoint, network, component settings, addons, maintenance and, from phase 3, hibernation |
| `DatastoreClass` | cluster | kind of dedicated etcd | the template a dedicated etcd is built from: members, storage, quota, backup store and schedules, defragmentation |
| `KubeconfigRequest` | namespaced | request | a short-lived kubeconfig for a person's own key, in its status; the request is deleted when it expires |
| `Backup` | namespaced | snapshot | an on-demand full snapshot of a tenant's etcd, named so a new `ControlPlane` can be restored from it |

Rules that hold across all of them:

- `spec` is what the user wants, `status` is what Lodia did.
  Status is a subresource; `status.conditions` follow the Kubernetes API conventions (`metav1.Condition` with `observedGeneration`); `status.observedGeneration` is set only after a full successful pass.
- Everything a `ControlPlane` creates in its namespace carries an `ownerReference` to it and the label `lodia.io/control-plane: <name>`; deleting the `ControlPlane` garbage-collects it.
  The dedicated etcd's volumes follow the `DatastoreClass` reclaim policy; its snapshots stay, since nothing deletes them yet ([dedicated-etcd-by-default](dedicated-etcd-by-default.md)).
- A `ControlPlane` has one finalizer, `lodia.io/datastore`, which the Datastore controller holds ([deletion](../README.md#deletion)).
- Imperative operations that are not desired state — starting or completing a credential rotation — are objects of their own, as a `Backup` and a `KubeconfigRequest` are: a right of their own in RBAC, CEL validation, and a status that records who asked and what happened.
  Whether there is one kind per operation or one for all is for the design of the first rotation ([roadmap](../roadmap.md)).
- Validation and defaults live in the CRD schema (CEL and defaults), not in a webhook ([no-admission-webhooks](no-admission-webhooks.md)).

The field reference is [`docs/api/controlplane.md`](../api/controlplane.md); examples are in [`docs/api/examples/`](../api/examples/).

## Why

- A platform that creates a cluster for a customer wants to write one object and watch one `Ready` condition.
  Spreading a cluster across several objects (one per component) moves the orchestration to every caller.
- Storage, access and snapshots have their own lifecycles and their own owners: a platform administrator defines a `DatastoreClass` once; a person asks for a kubeconfig many times; a snapshot outlives the control plane it was taken from.
- `DatastoreClass` mirrors `StorageClass`: the operator of the management cluster decides what a dedicated etcd looks like, the tenant only names a class.

## Rejected

- One object per component (`APIServer`, `ControllerManager`, …) — flexible, but every caller re-implements the ordering.
- Putting etcd settings inline in every `ControlPlane` — the same storage and backup settings repeated per tenant drift apart; a class is edited once.
- An operation annotation on the `ControlPlane` — whoever may edit the object may start any operation, a server-side apply drops an annotation its applier stops declaring, the schema's CEL cannot read one, and the record of a handled value in `status` is lost on a restore of the management cluster, the reasons the restore's release is a field ([restore-replaces-the-source](restore-replaces-the-source.md#why)).
- A `JoinToken` resource — a bootstrap token is a Secret in the tenant cluster that any admin kubeconfig can create with `kubeadm token create`; Lodia documents the command instead of wrapping it.

## Consequences

- The Go types, the CRD YAML and `docs/api/controlplane.md` change together.
- `v1alpha1` may change incompatibly until the maintainers declare it frozen, and changes only by addition from then on; a later version keeps its shape ([no-admission-webhooks](no-admission-webhooks.md#consequences)).

## Code

- `api/v1alpha1/controlplane_types.go`, `datastore_types.go` and `access_types.go` — the four kinds; `api/crds/` is generated from them.
