# A minor step is rehearsed on a copy of the API server before its Backup

Decided 2026-11-20.
Built; Workloads' second reconciler, `rehearsal`, runs the copy.

## Decision

**The step.**
A minor step of every tenant goes preflight, `Rehearsal`, `Backup`, `APIServer`, `ControllerManager`, `Scheduler`, `Addons` ([upgrades-and-maintenance-windows](upgrades-and-maintenance-windows.md)).
`Rehearsal` is recorded in `status.kubernetes.upgrade.step` as `Backup` is; a patch step has neither.
The rehearsal writes nothing in the tenant, so it needs no rollback point before it, and a `Backup` taken before a rehearsal that fails for days would be stale.
No field lets a tenant skip it: the operator's `fleet.maxConcurrentRehearsals: 0` turns rehearsals off, and the step then goes from the preflight to `Backup`.

**Who does what.**
Lifecycle records the step and reads the verdict.
Workloads' second reconciler, `rehearsal`, with a queue and field manager of its own, renders the copy as Workloads renders the API server, creates and deletes its Pod and NetworkPolicy, sweeps it, and writes `Rehearsed` and `status.rehearsal.version`, the version of the step the verdict is about, since a chain's steps share a generation.
The Datastore controller's etcd NetworkPolicy admits the copy's pods to the client port, so it keeps the one say over who reaches the etcd.

**The copy.**
One Pod `<cp>-rehearsal-<suffix>` in the tenant namespace, named by `generateName`, owned by the `ControlPlane` and labelled with it and `app.kubernetes.io/name: rehearsal`, which nothing else selects; no Service and no route.
A copy that is being deleted, as one on a node that is gone stays for good, counts neither toward the limit below nor against the next copy, which takes a name of its own.
Its API server is the render of the tenant's at the step's version, with only these changes:

| the render | the copy |
|---|---|
| `--etcd-servers` the client Service, with its TLS flags and mounts | `http://127.0.0.1:2379`, an etcd in the pod |
| `--etcd-compaction-interval` at its default or the tenant's | `0`, so that a sweep's continue tokens hold across its passes |
| the webhook admission plugins on | off: in the disable list and out of the enable list, both read as kube-apiserver reads them, a CSV record whose quotes are not part of a name ([admission.go](https://github.com/kubernetes/apiserver/blob/v0.37.1/pkg/server/options/admission.go#L108-L116)) |
| konnectivity-server and, in `gateway` mode, lodia-tunnel | none; lodia-signer kept |
| `--peer-ca-file`, `--peer-advertise-ip` and `--peer-advertise-port` a tenant sets in `extraArgs` | none: with `--peer-ca-file` the mixed-version proxy would dial the tenant's API servers, whose identity Leases the snapshot holds, directly ([peer.go](https://github.com/kubernetes/kubernetes/blob/v1.37.2/pkg/controlplane/apiserver/peer.go#L66-L86)) |
| startup, liveness and readiness probes, a preStop sleep | a readiness probe on `/readyz` every second |
| `terminationMessagePolicy: File` | `FallbackToLogsOnError`, so that the exit's reason lands in the pod's status ([types.go](https://github.com/kubernetes/kubernetes/blob/v1.37.2/staging/src/k8s.io/api/core/v1/types.go#L3279-L3280)) |
| a Deployment and its spread constraints | one Pod, `restartPolicy: OnFailure`, `activeDeadlineSeconds: 900`, `terminationGracePeriodSeconds: 5` |

Its init container `snapshot` takes a fresh snapshot of the tenant's etcd with `etcdctl` through the client Service, with the API server's etcd client and CA, and retries until the etcd's NetworkPolicy admits it; `restore` restores it with `etcdutl` into an `emptyDir` of twice the class's quota; the sidecar `etcd` serves it on loopback at the class's `quotaBackendBytes` and member resources, from etcd's own image at the version etcd-druid's wrapper runs, 3.5.27, a constant of the build outside the operator revision, since the copy is no object that lasts.
The API server mounts the tenant's Secrets as its own pod does, but etcd's, which only `snapshot` mounts, and konnectivity's.
Its NetworkPolicy `<cp>-rehearsal` admits the operator's namespace to port 6443 and nothing else.

**Webhooks.**
A copy reaches no webhook: its egress is konnectivity's socket, which it lacks.
With the webhook plugins on, each of the next minor's start writes would wait the webhook's whole timeout, 10 s by default and at most 30 s ([types.go](https://github.com/kubernetes/api/blob/v0.37.2/admissionregistration/v1/types.go#L907-L908)), and a webhook that fails closed would kill the copy with a message that names the webhook, not the minor.
So the copy does not check the next minor's start writes against the tenant's webhooks, which the real step does through konnectivity, and does not compile their match conditions; `ValidatingAdmissionPolicy` and `MutatingAdmissionPolicy` run in process and stay on.

**The sweep.**
Through the copy: aggregated discovery of `/api` and `/apis`; the `APIService`s, for those that name a Service; the CustomResourceDefinitions, 5 a page, for their conversion; then every resource that lists, at every version the copy serves, 500 a page, as a `Table` with `includeObject=None` ([types.go](https://github.com/kubernetes/kubernetes/blob/v1.37.2/staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/types.go#L1570-L1575)), the rows counted and nothing decoded.
Through the tenant: its aggregated discovery, and for each resource that fails through the copy, the same list.
Each read goes through `internal/tenantclient`, one at a time, the copy's at the pod's address with the API server's name, and is read with `internal/jsonscan` ([one-client-for-a-tenant](one-client-for-a-tenant.md)).
A pass reads for at most 45 s of its minute, a read at most 30 s, the time a conversion webhook may take ([webhook.go](https://github.com/kubernetes/kubernetes/blob/v1.37.2/staging/src/k8s.io/apiserver/pkg/util/webhook/webhook.go#L36-L38)), and the next pass comes at once with the sweep kept in memory by the copy's UID; a read through the copy or the tenant whose error is transient, no answer or none within 30 s, or kube-apiserver's 429, 503, 504 or `ServerTimeout`, which ask for a retry, is sent again 5 s later, within the copy's 15 minutes, and a restarted operator sweeps a copy from its start.

**The verdict.**

| finding | `Rehearsed` |
|---|---|
| the copy's kube-apiserver exits on its own, or runs and is not ready 5 minutes after the pod's creation | False `APIServerFailed`, its termination message quoted |
| a resource lists through the tenant and not through the copy; a resource the tenant serves that a CRD in the copy's own etcd serves at that version and the copy does not; a read through the copy that still fails on a connection the copy accepted, or the tenant's list of a resource the copy failed that still fails, when the copy's 15 minutes are over | False `ListFailed` |
| the copy did not run its API server within 15 minutes of its creation: not scheduled, an image not pulled, the snapshot, the restore or etcd failing; or it gave no verdict within them, a ready copy whose node is gone among them; or its node ended it, at its `activeDeadlineSeconds`, by an eviction or at its memory limit, whatever ran before | False `CopyFailed`, the pod's last reason, the init container's message or the sweep's last failed read |
| the management cluster refuses the Pod or the NetworkPolicy, or another owner keeps the NetworkPolicy under its name | False `Refused` or `ObjectsControlledElsewhere` |
| a resource that fails through both, through the tenant for a reason that is not transient | reported with each side's error: it fails before the step too |
| a group-version or resource only one side serves | reported: what the next minor removes or adds |
| a group-version whose `APIService` names a Service, one the copy's discovery marks stale | reported: not read through a copy |
| a CRD a webhook converts | reported: not read |
| none of the above | True `Passed` |

The message names the version, the objects and resources listed and the time since the pod's creation, then the first five findings of each kind and a count of the rest; each transition is an event.
The verdict stays on the `ControlPlane` until the next step's rehearsal, and the copy and its NetworkPolicy are deleted once it is written.
A failed verdict runs again at once on a new generation, the owner's fix among them, and otherwise at the first of every 10 minutes from `Rehearsed`'s `lastTransitionTime` after the last run ended, or after the operator started, so that a restart neither runs it at once nor loses the wait; `Refused` and `ObjectsControlledElsewhere`, about the copy's objects on the management cluster, run again sooner, on the controller's backoff and within a minute.
While it runs again at the same generation the verdict stays, so that `Progressing` does not flip.
A paused `ControlPlane` starts no copy, and one already running ends at its deadline.

**How many.**
At most `fleet.maxConcurrentRehearsals` copies run at once, 5 by default, apart from the fleet's slots, which an owner's upgrade does not take; the rehearsal counts them from the cache, which holds the copies' Pods and no other, under a fence of its own ([`internal/fence`](../../internal/fence/fence.go)).
A step without a free place is `Rehearsed` Unknown `RehearsalLimit`, and looks again every 30 seconds; the waiting tenants have no order among them.

## Why

- A tenant that sets a gate or a flag the next minor removed does not break at the step today: the new replica does not start, the old ones serve, and the upgrade stands `Failed` at `APIServer` after a surge replica; the rehearsal says so before the step, and says what a table of removed gates cannot.
- Experiment X4 ran the copy beside a tenant on kind at 1.35 → 1.36 and 1.36 → 1.37: snapshot, restore and etcd's start took 0.2 to 1.5 s together for 1.4 to 24.6 MiB, the kubelet's six containers 5.1 to 6.7 s, the sweep of 10,714 objects 106 to 140 ms, and a copy peaked at 0.23 to 0.30 GiB for 38 to 61 s; the tenant answered all 12,044 requests of its 14 copies, and its etcd saw no writer but its own.
- A gate 1.37 removed, `APIServerTracing`, stopped the copy 0.8 s after its API server started, with `unrecognized feature gate: APIServerTracing` as the container's termination message.
- Beside a webhook that fails open at 2 s the copy was ready after 13 to 26 s instead of 5.4 s; beside one that fails closed it exited after 75 s with `PostStartHook "rbac/bootstrap-roles" failed`.
- Every count that differed between the copy and the tenant was the copy's own start writes, an API one side serves alone, or the aggregated API, and a read of the tenant at the snapshot's revision matched the copy line for line; an object the next minor cannot decode fails its list, so a list error is the finding and counts are not compared.
- An object's annotations may hold 256 KiB ([objectmeta.go](https://github.com/kubernetes/kubernetes/blob/v1.37.2/staging/src/k8s.io/apimachinery/pkg/api/validation/objectmeta.go#L39)), so 500 objects' metadata can pass the 8 MiB a tenant read is cut at; a `Table` without objects cannot.
- Without the webhook plugins the copy measures the minor and not its own reach, and nothing else of the rehearsal's truth is lost: a list passes no admission.

## Rejected

- The step after the `Backup`, as the plan's sketch had it: the `Backup` would be stale after a rehearsal that fails for long.
- An `Etcd` of etcd-druid for the copy: a StatefulSet with a claim per member, certificates and a NetworkPolicy for a minute's copy, not measured; a plain etcd restored in 5 to 21 ms.
- The pre-upgrade `Backup`'s snapshot: gzip, the store's credentials wherever it is fetched, once in three with a delta `etcdutl` does not apply, no rehearsal for a class without a store, and a `Backup` before the rehearsal; that a `Backup` restores is proven by a restore drill of its own.
- A snapshot taken by the operator: it would carry the tenant's data through the operator.
- Keys of the copy's own: they changed no result and no time in X4, and would need certificates minted and delivered for each copy; the encryption configuration has to be the tenant's.
- Comparing counts: a paged read of the whole live tenant and the snapshot's revision carried out of the pod, for no decision.
- The report in a ConfigMap or in status lists: an object and a name to publish, or an API to freeze, for what a person reads once.
- A seventh controller: it would export Workloads' render for one caller; Lifecycle running the copy would end its being a pure function.
- A field per tenant to skip the rehearsal: a field added now is frozen with whatever it means; the operator's switch unblocks a tenant a rehearsal wrongly stops, for the length of its step.

## Consequences

- The rehearsal does not prove that the next minor's start writes pass the tenant's webhooks: a webhook that refuses them still stops the step at `APIServer`.
- The rehearsal does not prove that the bucket's `Backup` restores, only that the live data snapshots, restores and reads at the next minor.
- A copy holds the tenant's Secrets and a copy of its data on the node's disk for its minute; reaching them takes `pods/exec` on the management cluster or its node ([who-is-trusted](who-is-trusted.md)).
- A management cluster whose CNI does not enforce NetworkPolicy admits anyone to the copy, as to the tenant's own API server.

## Code

- `internal/controller/workloads/rehearsalcopy.go` — `rehearsalCopy`, the copy as the API server's render with the changes above, and `copyPolicy`; `rehearsalcopy_internal_test.go` holds the golden files `testdata/render/rehearsal-*.yaml` of a `nodePort`, a highly available and a `gateway` tenant, every other flag, the resources and the placement kept, and the policy.
- `internal/controller/workloads/rehearsal.go` — `RehearsalReconciler`: the step it rehearses, the place under the fence, the verdict from the pod, the deletion; `rehearsalsweep.go` — the sweep and its projections; `rehearsalsweep_internal_test.go` runs the sweep against stub servers: each transient error read again on both sides, a page late by 30 s among them, the tenant's expired token, a CRD resource the copy lacks stopping the step only while its own CRD serves it, each side's error reported, a sweep that goes on across passes with its continue token, and a `ControlPlane` gone or being deleted taking its sweeps and its verdict time along.
  `rehearsal_test.go` runs it in Workloads' suite as `lodia-workloads`, with the test standing in for the kubelet and a second envtest API server behind the tenant's serving certificate for the copy: the copy and its policy, `Passed` with its report and the reads it made, two tenants under a limit of 1, an exit with `unrecognized feature gate`, the failed verdict kept while it runs again and its wait across a restart, a NetworkPolicy another owner keeps and a Pod the API server refuses tried again sooner, another owner's Pod with the copy's labels neither judged nor deleted, a new generation and one after a failed verdict rehearsed at once, a chain's second minor rehearsed at the generation the first passed at, `CopyFailed` at the deadline and unscheduled, a ready copy whose node is gone `CopyFailed` 20 minutes, an hour and a day after its creation and, while it is being deleted, neither counted nor in the next copy's way, a copy its node ended at its deadline, by an eviction or at its memory limit `CopyFailed`, a transient answer of the copy read again, a tenant's 503 that does not hide what the copy cannot read, a list that still fails through the copy at the deadline `ListFailed`, a sweep cut by its pass's 45 s that goes on from its continue token, an API server not ready at 5 minutes, a copy that does not answer read again, an object the copy cannot decode and one neither can, and an aggregated API and a webhook-converted CRD reported and not read.
- `internal/controller/lifecycle/decide.go` — `rehearse`, the step from the preflight to `Rehearsal` and, on `Passed` for the step's version at the object's generation, to `Backup` in the same write, and back to `Rehearsal` from a `Backup` step whose generation moved; `decide_internal_test.go` holds the waits, the verdicts of another version and an older generation, `Failed`, `settle`, a limit of 0 and a patch step, and `upgrade_test.go` the `Backup` that comes in the pass the rehearsal passes and a spec edited at the `Backup` step, rehearsed before the target moves.
- `internal/controller/datastore/networkpolicy.go` — the copy's rule on the etcd's client port.
- `internal/versions` — `Step`, the version of a minor step, and `EtcdImage`.
- `internal/operator` — `fleet.maxConcurrentRehearsals`, which `setup_internal_test.go` checks reaches Lifecycle and the rehearsal, `controllers.rehearsal.workers`, and the cache of the copies' Pods.
