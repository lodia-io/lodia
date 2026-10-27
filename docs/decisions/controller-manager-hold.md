# While most of a tenant's nodes are silent, Lodia holds its controller manager's leader lease

Decided 2026-10-25.
Not built: the hold and its timings come with phase 2's wave 2 (Health), and `ControllersLeading` with wave 1b (HA) ([roadmap](../roadmap.md)).

## Decision

The Health controller protects a tenant's workloads from an outage between the control plane and its nodes:

- **It watches from inside.**
  Every 5 seconds, Health's period, for every awake `ControlPlane`, it reads the `Lease`s in `kube-node-lease` through the in-namespace Service, never through the public endpoint the nodes use, and the tenant's `Node`s every 30 seconds, since nodes are large objects and the set it counts changes slowly ([KEP-589](https://github.com/kubernetes/enhancements/blob/7eef794bb549a50c6b08c457556ff0eac98a4c6b/keps/sig-node/589-efficient-node-heartbeats/README.md#L31-L49)).
- **A node is silent** once its lease has not changed for 20 seconds by the operator's clock, not by `renewTime`: a node anywhere keeps its own time.
  The clock lives in memory: a lease seen for the first time counts as renewed then, and so does every lease after a read that failed, since the kubelets could not renew meanwhile either, and an operator that restarts counts afresh.
- **A node counts** if it has a lease and is `Ready`, or if the controller manager marked it `Unknown` less than 5 minutes before the hold began.
  A node its kubelet reports `NotReady` does not count, and a cluster with one counted node is never held: its silence cannot be told from a dead machine.
- **The hold.**
  When 60% or more of the counted nodes are silent and the API server answers, Health writes the Lease `kube-system/kube-controller-manager` inside the tenant (`LeaderElectionNamespace` and `ControllerManagerLease` in `api/v1alpha1/names.go`) with holder `lodia` (`HoldHolderIdentity`) and a 60-second duration, and renews it every round.
  The controller manager's leader fails its next renewal, gives up within its renew deadline (10 seconds) and exits; restarted, it waits on a lease that keeps being renewed.
  Nothing marks the nodes, nothing taints them, nothing evicts their pods.
- **The release.**
  Once fewer than 60% have been silent for 2 minutes, Health releases the lease the way client-go steps down (no holder, a 1-second duration); the controller manager takes it within seconds and starts afresh, counting its grace periods from its own start.
- **It never fights.**
  Health takes the lease only when it has expired or when its holder's identity starts with the name of a pod Lodia runs for this `ControlPlane` (a leader's identity is `<pod name>_<uuid>`).
  Any other holder, such as a platform's own watchdog, is left alone and named in the message of `ControllerManagerHeld=False`, reason `HoldBlocked`.
- **It fails safe.**
  A Lodia that stops renewing gives the lease back after 60 seconds, to upstream behaviour; a Lodia that restarts keeps a hold it finds with holder `lodia`.
  A paused `ControlPlane` gets no writes, so its hold lapses within those 60 seconds.
- **Settings.**
  On by default, off for one `ControlPlane` with `spec.health.holdControllerManager: false` ([the field](../api/controlplane.md#spec)).
  The thresholds and the period are [operator configuration](../README.md#installation).
- **Lodia owns the timings the hold races.**
  The hold works only if the controller manager exits before its node lifecycle controller marks the first silent node.
  Workloads renders that controller's grace period and the controller manager's leader election from constants in `api/v1alpha1/names.go`, and the schema refuses every one of them in `controllerManager.extraArgs`:

  | flag | value | constant |
  |---|---|---|
  | `--node-monitor-grace-period` | 60s | `NodeMonitorGracePeriod` |
  | `--node-monitor-period` | 5s | `NodeMonitorPeriod` |
  | `--leader-elect-lease-duration` | 15s | `ControllerManagerLeaseDuration` |
  | `--leader-elect-renew-deadline` | 10s | `ControllerManagerRenewDeadline` |
  | `--leader-elect-retry-period` | 2s | `ControllerManagerRetryPeriod` |
  | `--leader-elect-resource-lock`, `-resource-name`, `-resource-namespace` | `leases`, `kube-controller-manager`, `kube-system` | `ControllerManagerLease`, `LeaderElectionNamespace` |

  The operator refuses to start when its Health thresholds break the invariant below.
- **While Lodia holds the lease, `ControllersLeading` stays True**, reason `Held`: the hold is Lodia's own, so `Ready` does not drop for it.

**The margin.**
A kubelet renews its lease every quarter of its 40-second duration, 10 seconds, with up to 4% jitter ([kubelet.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubelet/kubelet.go#L1155-L1165), [defaults.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/kubelet/apis/config/v1beta1/defaults.go#L150), [controller.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/component-helpers/apimachinery/lease/controller.go#L153)), so the last silent node renewed at most 10.4 seconds after the first.
Health sees a renewal within its period, 5 seconds, and calls a node silent 20 seconds after it saw the last change; the leader then fails its renewals for its renew deadline, counted from one retry period after its last success, 10 to 12 seconds ([leaderelection.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go#L279-L310)), and exits.
The first node is marked no sooner than the grace period after its last renewal.
The margin is the grace period less 10.4 + 20 + 5 + 12 seconds:

| grace period | margin |
|---|---|
| 40 s (Gardener's default, [defaults_shoot.go](https://github.com/gardener/gardener/blob/v1.154.0/pkg/apis/core/v1beta1/defaults_shoot.go#L117-L119)) | −7.4 s: the first node can be marked and tainted before the exit |
| 50 s (upstream's default, [defaults.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/nodelifecycle/config/v1alpha1/defaults.go#L46)) | 2.6 s, before any delay in Health's queue, the API or a failover of the operator |
| 60 s (Lodia's) | 12.6 s |

A grace period of 60 seconds marks a node that really died 10 seconds later than upstream does, and its pods are evicted 300 seconds after that, as before.

It does not tell whose side broke: 60% of a cluster silent at once is a cut from the control plane, the provider's or the customer's, and there is nowhere to evict to either way.
One dead machine of two is 50%, and its pods are evicted as upstream does.

## Why

- **A hosted control plane turns the provider's outage into the tenant's.**
  Remote nodes reach their API server over a network the provider runs; when that path breaks, every kubelet stops renewing its lease at once while the pods keep running.
  After the node monitor grace period the controller manager marks the nodes `Unknown` and taints them; 300 seconds later the default toleration runs out and their pods are evicted ([taint-based evictions](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/#taint-based-evictions)).
- **Kubernetes' own safeguard has a hole.**
  The node lifecycle controller stops evicting when every node goes silent at once, but a taint added before that mode stays ([kubernetes#131481](https://github.com/kubernetes/kubernetes/issues/131481)).
  On Segla's platform on 2026-10-20, cutting both nodes of a cluster from the control plane cost one of them every pod that was not a DaemonSet's; that incident is where this design comes from.
- **Gardener protects its clusters the same way.**
  Its dependency-watchdog scales the controller manager to zero when node leases expire while the API server answers; 60% is its default `nodeLeaseFailureFraction` ([dependency-watchdog configuration](https://github.com/gardener/dependency-watchdog/blob/master/docs/deployment/configure.md)).
- **The lease instead of the replicas.**
  Scaling to zero needs the controller manager in a Deployment of its own; in the [Compact layout](compact-layout.md) it shares the API server's pod.
  Taking its leader lease stops it in every layout, rolls nothing and needs nothing upstream.
- **It is proven.**
  Segla's platform has run this exact hold since 2026-10-20: a lease written over the leader made it exit 10–11 seconds later, three times out of three; with two nodes cut off for 13 minutes the lease was taken 25 seconds in, the controller manager exited 12 seconds later, no node was tainted and all 15 pods kept their UIDs; with one node of two cut there was no hold, and that node's pods moved to the other after the toleration.

## Rejected

- Scaling the controller manager to zero, as Gardener's dependency-watchdog does ([prober.go](https://github.com/gardener/dependency-watchdog/blob/v1.8.0/internal/prober/prober.go#L118-L168), [scale.go](https://github.com/gardener/dependency-watchdog/blob/v1.8.0/internal/prober/scaler/scale.go#L22-L31)) — it calls a lease expired three quarters of the grace period after its `renewTime`, 30 seconds with the 40 seconds Gardener runs, and Gardener probes every 30 seconds with 20% jitter ([prober.go](https://github.com/gardener/dependency-watchdog/blob/v1.8.0/internal/prober/prober.go#L274-L278), [config.go](https://github.com/gardener/dependency-watchdog/blob/v1.8.0/internal/prober/config.go#L26), [access.go](https://github.com/gardener/gardener/blob/v1.154.0/pkg/component/nodemanagement/dependencywatchdog/access.go#L31)), so nodes are still marked and tainted, which wakes Cluster API's MachineHealthChecks; it fights Workloads over the replicas of a Deployment; and the Compact layout has no Deployment of the controller manager to scale.
- Longer tolerations (`--default-unreachable-toleration-seconds`) — pods would sit that long on a machine that really died.
- A longer node monitor grace period instead of the hold — the eviction comes later, but it comes.
- Sampling every 2 seconds to keep upstream's 50-second grace period — a margin of 5.6 seconds for two and a half times the reads in every tenant.
- Turning off the node lifecycle controller — no eviction from a dead node at all.

## Consequences

- While a hold lasts, no controller of the controller manager runs in that tenant: no new ReplicaSets, no garbage collection, no node IPAM; changes wait for the release (on Segla's platform, a Deployment created during a hold got its ReplicaSet 5 seconds after the release).
- A hold shows in `ControllerManagerHeld`, outside `Ready`, `status.health.controllerManagerHold` and an alert ([reasons](../api/controlplane.md#conditions-and-their-reasons), [alerts](../operations.md#alerts-shipped-in-the-chart)).
- A platform that already runs such a watchdog turns it off before relying on Lodia's; Segla's bosun steps down when Segla moves to Lodia.
- The phase 2 gate tests the hold ([roadmap](../roadmap.md)).

## Code

none yet.
