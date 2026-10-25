# Six controllers share one object and communicate only through status conditions

Decided 2026-10-25. The manager and PKI are built; the other five
controllers are not.

## Decision

One operator binary, one controller-runtime manager, six controllers that all
watch `ControlPlane`:

| controller | owns | writes condition | waits on |
|---|---|---|---|
| **PKI** | CAs, leaf certificates, kubeconfigs, ServiceAccount keys, encryption keys; renewal and staged rotation; the tenant's bindings for Brygga, the admin group and the kubelet client | `CertificatesReady`, `AccessReady` | `status.endpoint`; `APIServerReady` for the bindings |
| **Datastore** | the tenant's etcd (dedicated through etcd-druid, or a user and prefix in a shared store), backups, defragmentation; the only controller that talks to etcd | `DatastoreReady` | nothing |
| **Workloads** | Deployments, Services, PodDisruptionBudgets, NetworkPolicies, ConfigMaps, Gateway routes in the tenant namespace | `APIServerReady` | `CertificatesReady`, `DatastoreReady` |
| **Tenant** | objects inside the tenant cluster: kubeadm bootstrap objects, kube-proxy, CoreDNS, konnectivity-agent, apiserver-proxy, RBAC | `AddonsApplied`, `AddonsAvailable` | `AccessReady` |
| **Health** | probes the API from outside and inside, samples node leases; holds the controller manager's leader lease inside the tenant while most nodes cannot reach the API ([controller-manager-hold](controller-manager-hold.md)) | `Reachable` | `AccessReady` |
| **Lifecycle** | the pace: which version to run now, whether to hibernate, which rotation stage is active, when a tenant's maintenance window is open and how many tenants roll at once | `status.target` and `Progressing` | the others' conditions |

Rules:

- A controller never calls another controller and never writes another
  controller's objects. It reads `spec` and `status.target`, writes its own
  objects with server-side apply under its own field manager, and reports
  through its own condition. Blocked work is a condition with a reason, not a
  retry loop.
- The public address is known only once Workloads' Service has one (a load
  balancer's, a node's), so Workloads publishes `status.endpoint` from the
  Service before it waits on anything, and PKI and Tenant read it: the
  serving certificate's addresses, the kubeconfigs' server, `cluster-info`.
- Each controller has its own work queue, worker count and rate limiter; a
  failure in one tenant's datastore does not delay certificate renewal in
  another.
- The cache holds Brygga's own kinds and, of every other kind, only objects
  with the label `brygga.segla.io/control-plane` (a default label selector);
  it strips `managedFields` and keeps metadata only for Secrets and
  ConfigMaps; a controller reads a payload, or an object someone else created
  for a tenant, with the uncached
  API reader when it needs it. Tests run the client with
  `ReaderFailOnMissingInformer`, so an accidental cluster-wide informer fails
  a test instead of filling memory
  ([controller-runtime cache](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/cache)).
- `status.conditions` is a map list keyed by `type`, and each controller
  applies only its own condition through the status subresource with its own
  field manager, so six writers never conflict on the list; one condition
  type has exactly one writer, and an envtest case enforces it.
- A condition counts only while its `observedGeneration` is the object's
  generation; a controller waiting on an older one treats it as Unknown. Each
  controller applies every condition it owns on every generation, Unknown on
  its first visit, as the API conventions ask
  ([conventions](https://github.com/kubernetes/community/blob/a434040a92edf60f10a29879026845d0ed891c9a/contributors/devel/sig-architecture/api-conventions.md#L378-L386)),
  with `lastTransitionTime` moving only when the status does and a message
  that depends on nothing but the state, so a pass that changes nothing
  writes nothing.
- Each controller is registered under its own name (`pki`, `datastore`, …)
  with its own `MaxConcurrentReconciles` and a reconcile timeout; the
  priority queue that controller-runtime enables by default puts resyncs
  behind real changes
  ([priority queue design](https://github.com/kubernetes-sigs/controller-runtime/blob/main/designs/priorityqueue.md)).
- `Ready` on the `ControlPlane` is computed by Lifecycle from the others:
  `CertificatesReady ∧ DatastoreReady ∧ APIServerReady ∧ AccessReady ∧ AddonsApplied ∧ Reachable`,
  and `Hibernated` when asleep. Nothing in it waits for the tenant's nodes:
  the owner joins them once the control plane is `Ready`, so
  `AddonsAvailable`, the addons' pods on those nodes, is reported beside it.

```mermaid
flowchart LR
  CP[("ControlPlane<br/>spec · status")]
  PKI -->|CertificatesReady, AccessReady| CP
  DS[Datastore] -->|DatastoreReady| CP
  WL[Workloads] -->|APIServerReady| CP
  TN[Tenant] -->|AddonsApplied, AddonsAvailable| CP
  HL[Health] -->|Reachable| CP
  LC[Lifecycle] -->|status.target, Progressing, Ready| CP
  CP -. watch .-> PKI
  CP -. watch .-> DS
  CP -. watch .-> WL
  CP -. watch .-> TN
  CP -. watch .-> HL
  CP -. watch .-> LC
```

## Why

- In the solutions we studied, a common shape is one reconcile per cluster
  that runs every step in sequence; any failing step stops the rest, and a
  slow dependency (a datastore connection, a webhook) slows everything behind
  it. Independent queues keep failures local and make each controller small
  enough to test alone.
- Conditions are the Kubernetes way to express "this part is done" to other
  parts without coupling them, and they are what a platform on top watches
  anyway.
- Server-side apply with one field manager per controller makes ownership
  visible in `managedFields` and lets several controllers write the same
  object (each its own condition in the `ControlPlane`'s status, for
  instance) without overwriting each other.

## Rejected

- One reconcile with ordered handlers — simple to read, but every failure is
  global and the function grows without bound.
- One operator process per tenant (the HyperShift shape) — strong isolation
  and per-tenant versions of the renderer, at the cost of one more pod per
  tenant; maintenance windows give most of the benefit
  ([upgrades-and-maintenance-windows](upgrades-and-maintenance-windows.md)).
- Informers inside every tenant cluster — see
  [tenant-resources-by-inventory](tenant-resources-by-inventory.md).

## Consequences

- Two or more controllers may hold a `ControlPlane` in a partially ready state
  for a while; `status` says exactly which part and why.
- Two controllers know about time: Lifecycle (windows, the fleet limit,
  rotation stages) and Health (how long nodes have been silent); the other
  four are pure functions of `spec` and `status.target`.
- The layout (`Split` or `Compact`) is known to Workloads alone
  ([compact-layout](compact-layout.md)).
- Metrics come per controller: queue depth, reconcile duration and errors,
  labelled by controller and tenant.

## Code

- `cmd/brygga/main.go` — the manager with the cache rules above, and PKI
  registered as `pki` with ten workers.
- `internal/controller/pki` — reads `spec` and `status.endpoint`, writes its
  Secrets with server-side apply as `brygga/pki`, applies `CertificatesReady`
  and `AccessReady` alone through the status subresource (`Unknown` on its
  first visit, then with one fixed message per reason, so a pass that changes
  nothing writes nothing), counts `APIServerReady` only at the current
  generation, and requeues at the earliest renewal time in whole seconds.
  `reconciler_test.go` runs it under a manager whose cache is the one
  `main.go` builds, with `ReaderFailOnMissingInformer`, and holds the first
  visit and the stale `APIServerReady` cases.
