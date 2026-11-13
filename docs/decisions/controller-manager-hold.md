# While most of a tenant's nodes are silent, Lodia holds its controller manager's leader lease

Decided 2026-10-25.
Built; gate G4 tests it every night ([roadmap](../roadmap.md)).

## Decision

The Health controller protects a tenant's workloads from an outage between the control plane and its nodes:

- **It watches from inside.**
  Every 5 seconds, Health's period, for every awake `ControlPlane`, it reads the `Lease`s in `kube-node-lease` through the in-namespace Service, never through the public endpoint the nodes use, and the tenant's `Node`s every 30 seconds, since nodes are large objects and the set it counts changes slowly ([KEP-589](https://github.com/kubernetes/enhancements/blob/7eef794bb549a50c6b08c457556ff0eac98a4c6b/keps/sig-node/589-efficient-node-heartbeats/README.md#L31-L49)).
  Both lists page, 500 Leases and 100 Nodes to a page, with no `resourceVersion` set: the first page is a most recent read and each next one continues it at the same version ([api-concepts.md](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/reference/using-api/api-concepts.md#L1628-L1629)).
  A list stops once it has read more than `health.maxNodes` items, 5,000 by default, the most a cluster supports ([cluster-large.md](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/setup/best-practices/cluster-large.md#L12-L16)), or when a response is longer than 8 MiB or a field of it longer than its bound, since the tenant's administrator writes these objects.
  It also reads no more pages than `health.maxNodes` items fill and one more, 11 pages of Leases and 51 of Nodes by default, and stops at a further one: a tenant's API server fills every page of a list with no selector ([store.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L840-L862)), so a list still going by then comes from a server of the administrator's own, which an APIService naming a Service puts in place of the API server's ([handler_proxy.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/kube-aggregator/pkg/apiserver/handler_proxy.go#L215-L219)) and whose pages may hold no item.
  Health reads every response from a tenant with a scanner that decodes only the scalars it keeps, each within a bound of its raw size, checked before the decode, and each array one element at a time, up to its bound; no object or array reaches `encoding/json`, which would multiply a response the administrator can have any server write, and no error or message carries a byte of the tenant's beyond those bounds ([operations](../operations.md#capacity)).
  Every request goes out once, and the headers of a response are cut at 64 KiB: client-go would send it again up to ten times on a 429 or a 5xx, and the next pass is the retry.
  A page of more items than its `limit`, a Node of more than 64 `conditions`, a field it keeps longer than its bound or a `continue` token of more than 4,096 raw bytes, which Health sends back decoded with the next request, up to 12,288 bytes in a query of 36,883, stops a list like the bounds above, with `TooManyNodes` naming the page, the Node, the field or the token; an API server's token is the base64 of a small JSON of the next key and the revision, 403 bytes for names of 253 bytes at revision 1,234 and 423 at the largest revision, so 4,096 is 9.7 times the longest ([continue.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/continue.go#L83-L93)); a webhook configuration of more than 256 `webhooks`, a field of a held restore's list longer than its bound, or a list of a held restore of more than a page, makes that kind "could not be listed" in the message of `ControllersLeading`.
  A field Health only compares with a word, a condition's `type` among them, is compared on its raw token, and one past its bound is none of the words and no stop: a Node's writer chooses the type, and the API server accepts one of any length ([validation.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/apis/core/validation/validation.go#L7607-L7738)).
  A stop inside a page counts the items of that page that precede it.
  A tenant whose list stopped is not counted: its Lease clock is forgotten, `ControllerManagerHeld` reads `TooManyNodes` naming the bound, a hold in place ends at once and none is taken, and a pass back under the bounds counts afresh.
  `status.nodes.truncated` is true when the Node list stopped, since `status.nodes` counts Nodes; a stop of the Lease list leaves `status.nodes` as the last Node list has it ([the field](../api/controlplane.md#status)).
- **A node is silent** once its lease has not changed for 20 seconds by the operator's clock, not by `renewTime`: a node anywhere keeps its own time.
  The clock lives in memory: a lease seen for the first time counts as renewed then, and so does every lease after a read that failed, since the kubelets could not renew meanwhile either, and an operator that restarts counts afresh.
  A failover of the operator inside a cut can therefore delay a hold past the grace period, the known limit of keeping the clock in memory.
- **A node counts** if it has a lease and is `Ready`, or if the controller manager marked it `Unknown` less than 5 minutes before the hold began.
  A node its kubelet reports `NotReady` does not count, and a cluster with one counted node is never held: its silence cannot be told from a dead machine.
- **The hold.**
  When 60% or more of the counted nodes are silent and the API server answers, Health writes the Lease `kube-system/kube-controller-manager` inside the tenant (`LeaderElectionNamespace` and `ControllerManagerLease` in `api/v1alpha1/names.go`) with holder `lodia` (`HoldHolderIdentity`) and a 60-second duration, and renews it every round.
  The controller manager's leader fails its next renewal, gives up within its renew deadline (10 seconds) and exits; restarted, it waits on a lease that keeps being renewed.
  Nothing marks the nodes, nothing taints them, nothing evicts their pods.
- **The release.**
  Once fewer than 60% have been silent for 2 minutes, Health releases the lease the way client-go steps down (no holder, a 1-second duration); the controller manager takes it within seconds and starts afresh, counting its grace periods from its own start.
  A pass counts towards those 2 minutes only once the clock has run `silentAfter`, and a failed read of the kubeconfig or of the node Leases, or a return to 60% or more, clears the count.
  A hold has no end of its own while `AccessReady` stays not True, but for a tenant beyond a bound (row 5).
- **It never fights.**
  Health takes the lease only when it has expired or when its holder's identity starts with the name of a pod Lodia runs for this `ControlPlane` (a leader's identity is `<pod name>_<uuid>`).
  Any other holder, such as a platform's own watchdog, is left alone and named in the message of `ControllerManagerHeld=False`, reason `HoldBlocked`.
- **It fails safe.**
  A Lodia that stops renewing gives the lease back after 60 seconds, to upstream behaviour; a Lodia that restarts keeps a hold it finds with holder `lodia`.
  A failed read of the node leases restarts the clock, and a lease held by `lodia` is still renewed, but for a tenant beyond a bound (row 5), whose hold is released.
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

  The operator refuses to start when:
  - the margin below is under 10 seconds, kept for a delay in Health's queue or the API: `silentAfter`, the period, 10.4 seconds, the renew deadline, the retry period and those 10 seconds add up to more than the grace period;
  - `silentAfter` is not above 10.4 seconds plus the period, which would call a healthy node silent;
  - `holdFraction`, rounded to thousandths, is not above one half or is above one;
  - the period, `releaseAfter` or `failuresBeforeFalse` is not positive.
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

**The rows.**
A pass reads the Lease first, with a consistent GET, then takes the first row that matches.
Its write is a JSON merge patch of the Lease's `spec` that carries the `metadata.resourceVersion` it read: the API server answers a Lease that moved since with a conflict ([store.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go#L796-L810)), and every field of the Lease that a row does not write stays as it is.
A Lease that does not exist is created whole.
"Held" is holder `lodia` on the Lease just read, "below" includes fewer than 2 counted nodes, "free" is no Lease or no holder, and "expired" is no change for its `leaseDurationSeconds` by the operator's clock.

| # | when | condition | write |
|---|---|---|---|
| 1 | the read failed, `AccessReady` not True | `WaitingForAccess` | none |
| 2 | the read failed | kept, nothing applied | none |
| 3 | disabled, held | `Disabled` | release at once |
| 4 | disabled | `Disabled` | none |
| 5 | a list stopped at a bound, held | `TooManyNodes`, naming the bound | release at once |
| 6 | a list stopped at a bound | `TooManyNodes`, naming the bound | none |
| 7 | `AccessReady` not True, held | `NodesSilent` | renew |
| 8 | `AccessReady` not True | `WaitingForAccess` | none |
| 9 | the Lease list failed, held | `NodesSilent` | renew |
| 10 | the Lease list failed | `NotHeld`, saying the Leases could not be listed | none |
| 11 | no Node list of this run, held | `NodesSilent` | renew |
| 12 | no Node list of this run | `NotHeld`, saying the nodes are not listed yet | none |
| 13 | at or above the fraction, held | `NodesSilent` | renew, `acquireTime` kept |
| 14 | at or above, the Lease free or expired, or held by a pod of this `ControlPlane` | `NodesSilent` | take: holder `lodia`, 60 s, `acquireTime` and `renewTime` now, transitions + 1 |
| 15 | at or above | `HoldBlocked`, naming the holder | none |
| 16 | below, held, for less than `releaseAfter` | `NodesSilent` | renew, or the Lease would lapse at 60 s |
| 17 | below, held, for `releaseAfter` | `NotHeld` | release: no holder, 1 s, as client-go does |
| 18 | below | `NotHeld` | none |

Whether a list stopped is what its last outcome says, and a list that fails keeps the outcome it had: a stop known from an earlier pass still ends a hold in row 5 when `AccessReady` is not True or the Lease list fails.
With the read of the controller manager's Lease failed nothing can be released, so rows 1 and 2 answer before the stop.
A pass with no Lease list behind it, or with a list stopped at a bound, restarts the clock and clears the count towards the release, whichever row follows: rows 1, 2 and 5-10, rows 3 and 4 when the list failed, was stopped or `AccessReady` is not True, and a missing or foreign `<cp>-lodia-kubeconfig`.
Rows 13-15 clear the count, and rows 11 and 12 neither grow nor clear it.
A write that is refused or fails changes the row's condition:

| the write | then |
|---|---|
| succeeded | the row's condition |
| a conflict | the Lease is read again and **the row is selected again** from what it says, so a retry never writes over a holder row 14 does not allow; when the retries run out, as "failed otherwise" |
| a take refused (`apistatus.Refused`: a webhook, `Forbidden`) | `HoldBlocked`, naming the refusal; the next pass tries again |
| a renewal or a release refused | `NodesSilent`, the Lease still `lodia`'s; the next pass tries again, and a refused renewal lapses at 60 s if it lasts |
| failed otherwise (a timeout, a 5xx, `NotFound`) | kept, nothing applied; the next pass reads what the Lease says |
| a create meets `AlreadyExists` | nothing applied; the next pass reads it |
| row 14's pod list failed | kept, nothing applied, no take |

`NodesSilent` is therefore written only after a write that held the Lease, or with the Lease read as `lodia`'s.
`status.health.controllerManagerHold` goes with it, its counts absent on any row that has no list to count from.

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

- `internal/controller/workloads` — `render.go` renders the controller manager's timings and the scheduler's lease from the constants in `api/v1alpha1/names.go`; `workloads_test.go` holds the flags.
- `internal/operator` — `config.go` loads Health's settings from the operator's configuration file and refuses the ones that break the margin and a `health.maxNodes` that is not positive; `config_test.go` holds every refusal.
- `internal/controller/health` — `health.go` reports `ControllersLeading` from the two leader Leases; `health_test.go` holds a Lease that stops renewing.
  `nodes.go` samples the node Leases and the Nodes on a queue of its own, pages both lists and stops them at the bounds, and reports `status.nodes` and `ControllerManagerHeld`; `nodes_test.go` holds the counting rules, and `lists_internal_test.go` the pages, the stops at `health.maxNodes`, at the pages bound and at a response over 8 MiB, with the bytes read, a page of more items than its limit, a Node of 65 conditions, a `continue` token of 4,097 bytes, a page that fails, and the heap the sampling keeps over names and versions at their bounds.
  `read.go` holds the types a tenant's responses are read into and the requests, which ask for JSON and go out once, and `fields.go` the raw bound of each field and the reader of an array; `read_internal_test.go` holds the array bounds and the failed requests' Status, `bounds_internal_test.go` every kept field read at its bound and a byte past it, a key of any kind, an error and its log that carry nothing of the value, and `projection_internal_test.go` that no object or array of an answer reaches `encoding/json`.
  `internal/jsonscan` is the scanner, with its tests against a `json.Decoder` and two fuzz tests.
  `bounded.go` is the transport's cap on a response and on its headers, for both reconcilers; `requests_internal_test.go` holds a request sent once whatever the answer says of sending it again, and the headers at the bound and a byte past it, over HTTP/1.1 and HTTP/2; `bounded_internal_test.go` holds a body of exactly the bound and one a byte over, chunked and with a length, and `bounded_test.go` a held restore's list of a kind whose response is over, a tenant of more than a page counted whole, a pass over lists of pages with no item and the requests it sends, a page or a Node beyond its bound, a held restore's lists beyond theirs, the stops with their condition and status, a hold released at once and one never taken, rows 1 and 2 before the stop, and the clock afresh once back under the bounds; `large_test.go` holds a tenant of 5,000 Nodes counted whole at the operator's timeout, `decoded_internal_test.go` that no decoded page is reachable when the next one is requested, and `measure_internal_test.go` what every read does with a response of empty objects and, when `LODIA_MEASURE_HEAP` is set, the live heap of each and of passes over tenants padded to their bounds ([operations](../operations.md#capacity)).
  `hold.go` selects the row of the table above and writes the Lease; `hold_internal_test.go` holds the patch each write sends, and `hold_test.go` every row, each write's outcomes, a conflict on every try and the fields of the Lease the hold does not write.
- `test/e2e/hold_test.go` — gate G4: a highly available tenant behind the Gateway on a kind cluster, its two nodes cut off for 10 minutes, then one of them ([the gate](../../test/e2e/README.md#nightly-gate-g4-nodes-cut-off-from-the-gateway)).
