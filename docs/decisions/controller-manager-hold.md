# While most of a tenant's nodes are silent, Lodia holds its controller manager's leader lease

Decided 2026-10-25. Not built.

## Decision

The Health controller protects a tenant's workloads from an outage between
the control plane and its nodes:

- **It watches from inside.** Every 5 seconds, for every awake
  `ControlPlane`, it reads the tenant's `Node`s and their `Lease`s in
  `kube-node-lease` through the in-namespace Service, never through the
  public endpoint the nodes use.
- **A node is silent** once its lease has not changed for 20 seconds by the
  operator's clock, not by `renewTime`: a node anywhere keeps its own time. A
  lease seen for the first time counts as renewed then, and so does every
  lease after a read that failed: the kubelets could not renew meanwhile
  either.
- **A node counts** if it has a lease and is `Ready`, or if the controller
  manager marked it `Unknown` less than 5 minutes before the hold began. A
  node its kubelet reports `NotReady` does not count, and a cluster with one
  counted node is never held: its silence cannot be told from a dead machine.
- **The hold.** When 60% or more of the counted nodes are silent and the API
  server answers, Health writes the Lease `kube-system/kube-controller-manager`
  inside the tenant with holder `lodia` and a 60-second duration, and renews
  it every round. The controller manager's leader fails its next renewal,
  gives up within its renew deadline (10 seconds) and exits; restarted, it
  waits on a lease that keeps being renewed. Nothing marks the nodes, nothing
  taints them, nothing evicts their pods.
- **The release.** Once fewer than 60% have been silent for 2 minutes,
  Health releases the lease the way client-go steps down (no holder, a
  1-second duration); the controller manager takes it within seconds and
  starts afresh, counting its grace periods from its own start.
- **It never fights.** Health takes the lease only when it has expired or
  when its holder's identity starts with the name of a pod Lodia runs for
  that controller manager (a leader's identity is `<pod name>_<uuid>`). Any
  other holder, such as a platform's own watchdog, is left alone and named in
  the message of `ControllerManagerHeld=False`, reason `HoldBlocked`.
- **It fails safe.** A Lodia that stops renewing gives the lease back after
  60 seconds, to upstream behaviour; a Lodia that restarts keeps a hold it
  finds with holder `lodia`.
- **Settings.** On by default; `spec.health.holdControllerManager: false`
  turns it off for one `ControlPlane` and releases a hold in progress. The
  thresholds (20 s, 60%, 2 min) are operator configuration.

It does not tell whose side broke: 60% of a cluster silent at once is a cut
from the control plane, the provider's or the customer's, and there is
nowhere to evict to either way. One dead machine of two is 50%, and its pods
are evicted as upstream does.

## Why

- **A hosted control plane turns the provider's outage into the tenant's.**
  Remote nodes reach their API server over a network the provider runs; when
  that path breaks, every kubelet stops renewing its lease at once while the
  pods keep running. After the node monitor grace period the controller
  manager marks the nodes `Unknown` and taints them; 300 seconds later the
  default toleration runs out and their pods are evicted
  ([taint-based evictions](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/#taint-based-evictions)).
- **Kubernetes' own safeguard has a hole.** The node lifecycle controller
  stops evicting when every node goes silent at once, but a taint added before
  that mode stays
  ([kubernetes#131481](https://github.com/kubernetes/kubernetes/issues/131481)).
  On Segla's platform on 2026-10-20, cutting both nodes of a cluster from the
  control plane cost one of them every pod that was not a DaemonSet's; that
  incident is where this design comes from.
- **Gardener protects its clusters the same way.** Its dependency-watchdog
  scales the controller manager to zero when node leases expire while the API
  server answers; 60% is its default `nodeLeaseFailureFraction`
  ([dependency-watchdog configuration](https://github.com/gardener/dependency-watchdog/blob/master/docs/deployment/configure.md)).
- **The lease instead of the replicas.** Scaling to zero needs the controller
  manager in a Deployment of its own; in the
  [Compact layout](compact-layout.md) it shares the API server's pod. Taking
  its leader lease stops it in every layout, rolls nothing and needs nothing
  upstream.
- **It is proven.** Segla's platform has run this exact hold since
  2026-10-20: a lease written over the leader made it exit 10–11 seconds later,
  three times out of three; with two nodes cut off for 13 minutes the lease was
  taken 25 seconds in, the controller manager exited 12 seconds later, no node
  was tainted and all 15 pods kept their UIDs; with one node of two cut there
  was no hold, and that node's pods moved to the other after the toleration.

## Rejected

- Scaling the controller manager to zero — impossible in the Compact layout,
  and a change to a Deployment in the Split one.
- Longer tolerations (`--default-unreachable-toleration-seconds`) — pods
  would sit that long on a machine that really died.
- A longer node monitor grace period — the eviction comes later, but it comes.
- Turning off the node lifecycle controller — no eviction from a dead node at
  all.

## Consequences

- While a hold lasts, no controller of the controller manager runs in that
  tenant: no new ReplicaSets, no garbage collection, no node IPAM; changes wait
  for the release (on Segla's platform, a Deployment created during a hold got
  its ReplicaSet 5 seconds after the release).
- `ControllerManagerHeld=True` with reason `NodesSilent`, outside `Ready`
  since the API answers; `status.health.controllerManagerHold` says since
  when and how many nodes, and the alert `LodiaControllerManagerHeld` fires
  after 15 minutes.
- A platform that already runs such a watchdog turns it off before relying on
  Lodia's; Segla's bosun steps down when Segla moves to Lodia.
- The phase 2 gate cuts both workers of a tenant from the control plane and
  checks that no node is tainted and no pod is replaced
  ([roadmap](../roadmap.md)).

## Code

none yet.
