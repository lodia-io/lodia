# Each control plane component runs in its own Deployment; etcd in its own StatefulSet

Decided 2026-10-25. Not built.

## Decision

A tenant namespace holds:

| workload | containers | replicas: single | replicas: HA | notes |
|---|---|---|---|---|
| Deployment `<name>-kube-apiserver` | `kube-apiserver`, `konnectivity-server`, `brygga-signer` | 1 | 2 | the sidecars talk to the API server over Unix sockets, so they share its pod; one PodDisruptionBudget, spread across nodes (or zones) |
| Deployment `<name>-kube-controller-manager` | `kube-controller-manager` | 1 | 2 | leader election; reaches the API through the namespace Service |
| Deployment `<name>-kube-scheduler` | `kube-scheduler` | 1 | 2 | leader election; same path to the API |
| etcd (a `StatefulSet` managed by etcd-druid) | `etcd`, `backup-restore` | 1 | 3 | own CA, quota, continuous snapshots to object storage |

This is the `Split` layout, the default; `Compact` puts every component in
one pod for development clusters and free tiers
([compact-layout](compact-layout.md)).

Four pods for a single control plane, nine for a highly available one. Each
Deployment has its own readiness: the API server on `/readyz`, the two
controllers on their secure health endpoints; none waits on another through
init containers.

```mermaid
flowchart TB
  subgraph ns["namespace team-a · ControlPlane prod-eu · HA"]
    direction LR
    subgraph kas["Deployment kube-apiserver ×2"]
      a1["kube-apiserver :6443"] ---|UDS| k1["konnectivity-server :8132"]
    end
    kcm["Deployment kube-controller-manager ×2<br/>leader election"]
    sch["Deployment kube-scheduler ×2<br/>leader election"]
    etcd[("etcd ×3<br/>+ backup-restore")]
    svc["Service kube-apiserver :443→6443"]
    kcm --> svc
    sch --> svc
    svc --> a1
    a1 -->|mTLS :2379| etcd
  end
```

## Why

- A change to one component should roll only that component. With separate
  Deployments, a controller-manager flag, a scheduler profile or a renewed
  controller certificate restarts one pod with a leader election failover and
  leaves the API server alone; the API server rolls only for its own changes.
- A rolling update of the API server with two replicas and a PDB keeps the API
  up during upgrades; with every component in one pod, the smallest change
  restarts the API server too.
- The API server reloads its serving certificate and client CA from disk, so a
  routine certificate renewal is no rollout at all once nothing else lives in
  its pod.
- This is the shape Gardener and HyperShift use for their hosted control
  planes
  ([Gardener](https://github.com/gardener/gardener/blob/master/docs/development/high-availability-of-components.md),
  [HyperShift](https://github.com/openshift/hypershift/blob/main/ARCHITECTURE.md)).

## Rejected

- One pod with every component — fewest pods, simplest wiring, but one
  rollout for any change and no independent scaling.
- konnectivity-server as its own Deployment — the API server reaches it over
  a Unix socket (the egress selector's `UDS` transport); a separate pod would
  need TCP with its own TLS and one more hop.
- Three API server replicas in HA by default — two already give zero-downtime
  rollouts and node-failure tolerance; the third is memory for nothing
  unless load asks for it (`spec.apiServer.replicas` can raise it).

## Consequences

- Secrets and ConfigMaps whose change requires a restart (audit policy,
  egress selector and admission configuration, the etcd CA, the konnectivity
  server certificate, the scheduler configuration) are hashed into the pod
  template of the component that mounts them, and only that component rolls.
  Files that are reloaded are not hashed: serving certificates, client CA
  bundles, the kubelet client certificate, the encryption and authentication
  configuration, certificates referenced from kubeconfigs, and the
  ServiceAccount keys behind the signer sidecar.
- Holding the controller manager while nodes cannot reach the API does not
  touch its replicas: Health takes its leader lease
  ([controller-manager-hold](controller-manager-hold.md)), which works the
  same when every component shares one pod.
- Resource requests are per component and per tenant; the defaults are in the
  operator configuration and overridable per `ControlPlane`.

## Code

none yet.
