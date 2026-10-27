# Objects inside a tenant cluster are applied with an inventory and pruned; Lodia keeps no informers in tenant clusters

Decided 2026-10-25. Built for phase 1, without the apiserver-proxy.

## Decision

- The Tenant controller applies its objects to a tenant cluster with
  server-side apply under the field manager `lodia/tenant`, records their
  identities in an inventory ConfigMap
  (`kube-system/lodia-inventory`), and deletes what the previous inventory
  names and the current render no longer contains, when Lodia applied it.
- Lodia leaves another owner's objects as they are: the rule of
  [independent-controllers](independent-controllers.md) carried into the
  tenant, where no object has a `ControlPlane` for controller, with
  `managedFields` as the signal. Before its first apply of an object no
  inventory names, the controller reads it; one that exists without
  `lodia/tenant` among its managers is another owner's, stays as it is, and
  `AddonsApplied` names it: delete it, or disable the addon. The `kubernetes`
  Service's Endpoints and EndpointSlice have no other owner: they belong to
  the control plane in every cluster, and the controller takes them over
  from whoever wrote them, such as the lease reconciler of an API server
  that ran before `--endpoint-reconciler-type=none`
  ([api-exposure](api-exposure.md)). The prune reads
  an entry that left the render by group and kind, at the version the tenant
  serves, and deletes it under the UID and resourceVersion it read only when
  `lodia/tenant` applied it; any other entry, and one already gone or of a
  kind no longer served, only leaves the inventory. The label
  `app.kubernetes.io/managed-by` is no signal: PKI's bindings carry it too.
- It applies on every change of the `ControlPlane` or of `status.target`, and
  again on a timer (every 10 minutes by default), so a deleted or hand-edited
  object comes back within that period.
- `AddonsApplied` says the inventory is applied and pruned. The readiness of
  in-tenant workloads (kube-proxy, CoreDNS, konnectivity-agent,
  apiserver-proxy) is read with direct GETs at apply time and on the timer,
  and reported in `AddonsAvailable`, which `Ready` does not wait for: their
  pods need the owner's nodes and CNI, which a new cluster does not have yet.
- Lodia opens no watch into a tenant cluster. The only long-lived
  connections into tenants are the konnectivity tunnels the API servers own.
- Fields a tenant's own tools add to Lodia's objects (labels and
  annotations, for instance) are left to their owners: Lodia applies only
  the fields it renders, and server-side apply keeps the rest. A field Lodia
  renders stays Lodia's: CoreDNS's replica count is
  `addons.coreDNS.replicas`, and an autoscaler that changes it is set back
  on the next apply.

## Why

- A thousand tenants with informers each means a thousand watch connections,
  caches and reconnect storms in the operator for objects that change
  rarely. Gardener manages the same kind of objects with a periodic apply and
  a prune list (`ManagedResource` in gardener-resource-manager,
  [docs](https://github.com/gardener/gardener/blob/master/docs/concepts/resource-manager.md));
  the model scales to thousands of clusters there.
- Prune by inventory is how `kubectl apply --prune` and `kpt live apply`
  remove what a render dropped; without it, disabling an addon leaves its
  objects behind forever.
- Server-side apply gives one owner per field, so a tenant's own tooling and
  Lodia stop overwriting each other.
- A hand-edited inventory, or an owner's own CoreDNS under kubeadm's names,
  must not make Lodia delete or take over what it never wrote; the tenant's
  objects carry no owner reference Lodia could ask, and `managedFields`
  record who applied each one.

## Rejected

- A manager with informers in every tenant — immediate drift repair, at the
  cost of memory and connections that grow with tenants.
- An agent inside every tenant that pulls desired state — immediate repair
  and no connection from the operator at all, but one more component to
  version, and the node agents of the platforms on top already fill that
  role.

## Consequences

- Drift repair is not instant: a deleted CoreDNS Deployment returns within
  the apply period, not within seconds. The period is configurable per
  operator; a platform that needs faster repair runs its own agent.
- The inventory is the contract for prune: an object applied by Lodia
  without an inventory entry would leak, so the entry is written in the same
  pass as the object.
- Another owner's object under a name Lodia renders keeps that object out
  until the owner deletes it, after which Lodia applies its own within the
  apply period, or disables the addon.

## Code

- `internal/controller/tenant` — `inventory.go` holds the inventory: an
  object is named in it before its first write and the entries that left
  the render are pruned before it is rewritten, so a pass that stops
  anywhere leaks nothing; an object at another API version is the same
  entry; `foreign` finds another owner's objects before the first apply, the
  `kubernetes` Service's endpoints aside (`kubernetesEndpoints`), and
  `prune` deletes only what `lodia/tenant` applied; an inventory it cannot
  read stops the pass with `AddonsApplied` False until someone deletes it.
  `tenant.go` holds the pass and the conditions, `render.go` and
  `manifests/` the objects.
- `internal/controller/tenant/tenant_test.go` — the objects on a tenant
  kube-apiserver, prune and restore of CoreDNS, a deleted DaemonSet coming
  back, an object the tenant refuses that leaks nothing, a tenant that does
  not answer, a prune that deletes only what Lodia applied, another owner's
  CoreDNS neither taken over nor pruned, the `kubernetes` endpoints taken
  over from the lease reconciler, an inventory read the tenant refuses.
