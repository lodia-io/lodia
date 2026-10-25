# Objects inside a tenant cluster are applied with an inventory and pruned; Brygga keeps no informers in tenant clusters

Decided 2026-10-25. Built for phase 1, without the apiserver-proxy.

## Decision

- The Tenant controller applies its objects to a tenant cluster with
  server-side apply under the field manager `brygga/tenant`, records their
  identities in an inventory ConfigMap
  (`kube-system/brygga-inventory`), and deletes anything in the previous
  inventory that the current render no longer contains.
- It applies on every change of the `ControlPlane` or of `status.target`, and
  again on a timer (every 10 minutes by default), so a deleted or hand-edited
  object comes back within that period.
- `AddonsApplied` says the inventory is applied and pruned. The readiness of
  in-tenant workloads (kube-proxy, CoreDNS, konnectivity-agent,
  apiserver-proxy) is read with direct GETs at apply time and on the timer,
  and reported in `AddonsAvailable`, which `Ready` does not wait for: their
  pods need the owner's nodes and CNI, which a new cluster does not have yet.
- Brygga opens no watch into a tenant cluster. The only long-lived
  connections into tenants are the konnectivity tunnels the API servers own.
- Fields a tenant may legitimately set on Brygga's objects (replica counts of
  CoreDNS by an autoscaler, for instance) are left to their owners: Brygga
  applies only the fields it renders, and server-side apply keeps the rest.

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
  Brygga stop overwriting each other.

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
- The inventory is the contract for prune: an object applied by Brygga
  without an inventory entry would leak, so the entry is written in the same
  pass as the object.

## Code

- `internal/controller/tenant` — `inventory.go` holds the inventory: an
  object is named in it before its first write and the entries that left
  the render are pruned before it is rewritten, so a pass that stops
  anywhere leaks nothing; an object at another API version is the same
  entry; an inventory it cannot read stops the pass with `AddonsApplied`
  False until someone deletes it. `tenant.go` holds the pass and the
  conditions, `render.go` and `manifests/` the objects.
- `internal/controller/tenant/tenant_test.go` — the objects on a tenant
  kube-apiserver, prune and restore of CoreDNS, a deleted DaemonSet coming
  back, an object the tenant refuses that leaks nothing, a tenant that does
  not answer.
