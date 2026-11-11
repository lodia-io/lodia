# Upgrades follow the version skew policy step by step; changes Lodia initiates roll in each tenant's maintenance window, a bounded number of tenants at a time

Decided 2026-10-25.
Built: the steps of a minor upgrade below, Lifecycle driving them, the operator revision, the holds of Workloads, Tenant and the Datastore controller, the stamps on the Deployments and the `Etcd`, and Lifecycle's grant of the running revision by the rules below: windows, the fleet, maintenance on request and the settling time.
Not built yet: patch updates, an edit of the `DatastoreClass` in the window and the warnings before end of support, which phase 2's wave 2 builds next, and, with its observability, the alerts `LodiaMaintenanceHalted` and one for a slot held long ([roadmap](../roadmap.md)).

## Decision

**One target; each renderer follows what runs.**
Lifecycle writes one `status.target.version`: the version the API server rolls to now, one minor at a time towards `spec.version`; a spec several minors ahead goes through the newest patch of each minor between, as the version table names it.

- Workloads renders the API server, and konnectivity-server beside it, at `target.version`, and the controller manager and the scheduler at `status.components.apiServer.version`, the version every API server replica runs, or at `target.version` while none is reported.
  The `Compact` layout runs all three in one pod at `target.version`.
- Tenant renders every object inside the tenant at the oldest of the three components' versions.
- Each reports what it rolled out in full, in `status.components` and `status.addons` ([status](../api/controlplane.md#status)).
- `status.kubernetes.version` is the oldest of those four.
  It moves only once the last step is done, and it is the version Cluster API reads as the control plane's: the minimum, "more specifically the API server version" ([control-plane.md](https://github.com/kubernetes-sigs/cluster-api/blob/v1.14.3/docs/book/src/developer/providers/contracts/control-plane.md#L455-L457)).

**A minor step.**

1. Preflight, once every part reports the current target (what rolled before it, a patch downgrade among them, finishes first): the next minor is in the version table (else `Unsupported`); `DatastoreReady` at the object's generation (else `WaitingForDatastore`); every kubelet at most three minors older than the next minor, from `status.nodes.oldestKubelet` (else `WaitingForNodes`, also while `status.nodes` is absent); no hold of the controller manager (else `ControllerManagerHeld`).
2. `status.kubernetes.upgrade.step` becomes `Backup`, and the `Backup` `<cp>-pre-upgrade-v<minor>-<ControlPlane UID>` (`PreUpgradeBackupName`) completes; a tenant steps to a minor once, so a `Backup` named for the minor does not change with the patch the step goes to, and the name's UID keeps a predecessor's from standing in for it, since a `Backup` outlives its source.
   A failed `Backup` is `Failed`, and deleting it takes another; it has no owner reference, since it must outlive the tenant.
   The target moves once it completes, or once it reports that the class has no backup store, which `Progressing` then says, unless the upgrade stands `Failed` (below); `step` becomes `APIServer` in the same write.
3. The API server rolls; the step is done when every replica runs the new template, updated and ready, since `/readyz` reports no version.
4. The controller manager and the scheduler roll.
5. In the tenant, kube-proxy, CoreDNS and konnectivity-agent move to the new minor's versions; done when they are applied.
   Their pods roll on the owner's nodes and are reported in `AddonsAvailable`, not awaited.
6. `status.kubernetes.version` becomes the new version.
   Nodes are the owner's: `status.nodes` reports how many run an older kubelet, and nothing forces them.

A patch step has no preflight and no `Backup`: the target moves at once, keeping the `startedAt` of a change under way.
No step waits for a leader: a hold of the controller manager blocks only the start of an upgrade.
A step that fails — a Deployment past its progress deadline (`APIServerReady`, `ProgressDeadlineExceeded`), an object the API server refuses — stops the upgrade, `Progressing=False`, `Failed`, with the step in `status.kubernetes.upgrade`; it resumes when the cause is gone.
Restoring the pre-upgrade `Backup` into a new `ControlPlane` at the old version is the documented rollback, and it replaces the cluster ([restore-replaces-the-source](restore-replaces-the-source.md)).

**Patch updates.**
Lodia never writes `spec`: `maintenance.autoUpdate.kubernetesPatch` moves `target.version` ([the field](../api/controlplane.md#spec)).
`spec.version` takes any patch of a minor in the table: `latest` is what updates move to, not an allowlist.

**End of support.**
Each minor in the version table has an end-of-support date, and a minor dropped from the table stays renderable for one more release, as the source of an upgrade.
Tenants nearing the date are warned in a metric, an alert and `Progressing`'s message.
A forced upgrade at the end of support, as Gardener forces the next minor of an expired version ([reconciler.go](https://github.com/gardener/gardener/blob/v1.154.0/pkg/controllermanager/controller/shoot/maintenance/reconciler.go#L696-L720)), can come later as an operator policy, off by default.

**The operator revision.**
An operator release renders differently, and so does an operator with another configuration.
`operatorRevision` is `<build version>+<digest>`.
The build version covers what is fixed at build: the version table, the registry and the default resources.
The digest covers what a render reads at run time: the images of `lodia-signer` and `lodia-tunnel`, etcd-druid's version, and the namespaces of etcd-druid and of the operator, which each etcd's NetworkPolicy renders.
Whether the management cluster serves Gateway API is left out: a tenant waiting for it must not then wait for a window.
The configuration's `versions` and `registry` keys join the digest when something reads them ([operator configuration](../README.md#installation)).
Revisions are compared for equality only, so a rollback of the operator is one more revision.
Workloads, Tenant and the Datastore controller render with the running revision only while `status.target.operatorRevision` equals it ([what they hold until then](../api/controlplane.md#status)).
Workloads stamps the revision on its Deployments and the Datastore controller on the `Etcd`, in the annotation `lodia.io/operator-revision`, never in a pod template, so a revision that changes nothing rolls nothing.
Until the grant Workloads writes nothing that exists but the deletions [`target.operatorRevision`'s row](../api/controlplane.md#status) names, and creates what is missing: it keeps no inventory, so an object a new release adds is created at once.
Until the grant Tenant writes nothing that exists inside the tenant but the tunnel's renewed client certificate, neither the inventory nor a prune; it creates only what the stored inventory lists and the tenant lacks, so an object a new release adds waits for the grant, and a listed one the release no longer renders stays as it is.
Tenant stamps nothing: `status.addons.operatorRevision` is the revision of a pass that applied in full.
Until the grant the Datastore controller applies the `Etcd` it finds, with only a new generation of the etcd certificates, and writes the etcd's NetworkPolicy only when it is missing; it creates a missing `Etcd` from the full render, stamped with the running revision.
etcd-druid's automatic spec reconcile is off, and the Datastore controller asks for each reconcile ([dedicated-etcd-by-default](dedicated-etcd-by-default.md)): for each generation and stamp of the `Etcd` until a reconcile that started after the request has served it, and for a stamp it has not asked under before only once etcd-druid's Deployment runs the version the operator was given and no pod of an older one terminates, since an older etcd-druid would roll the members to its own render.
So `status.datastore.operatorRevision` names a revision only once etcd-druid, at the version that revision names, has reconciled the `Etcd` under it in a reconcile that started after the request, and every member runs that spec, wherever `DeploymentReplicaSetTerminatingReplicas`, on by default from Kubernetes 1.35, lets the Deployment count a terminating pod ([A tenant namespace](../README.md#a-tenant-namespace)).
A hand edit is not reverted while a tenant waits; storing the last render, as Gardener's ManagedResources do, stays a later option with the same contracts.

**The grant.**
Lifecycle grants the running revision, while it is pending, by these rules in their order:

1. A new value of `lodia.io/maintain-requested-at` is recorded in `status.maintenance.lastHandled` on every pass but a paused or deleting one's, pending or not; a value over 256 characters is not handled, and removing the annotation leaves the record, so the same value set again does nothing.
2. A revision that does not run the minor of the target or of a reported version is granted on no path, since a renderer would write `WaitingForTarget` and take a running tenant down: `Unsupported`; a tenant in a slot keeps it, so the release after reaches it at once.
3. At once, with no slot: no target yet; an edit of `spec` not taken up, its generation ahead of `Progressing`'s; an upgrade its owner started; a new value of the annotation; a tenant in a slot, which keeps it, so that a later release, or a rollback, reaches a tenant a bad release left failed; and three kinds of tenant only the grant brings back to `Ready`, since a held renderer cannot: `AddonsApplied` not True with no `status.addons.version` or with `Refused`, `ObjectsControlledElsewhere` or `Applying`, `APIServerReady` False, `WaitingForTarget`, at the object's generation, and a restored `ControlPlane` whose controller manager is not released.
4. Not while `Unsupported` or `Failed` stands: the owner's to fix.
5. Not while `TimeZoneUnsupported` stands; outside the window, `WaitingForWindow`, with `target.scheduledFor` its next start.
6. Not while `Ready` is not True, at whatever generation each input describes, `AddonsApplied` left out while it waits with `AccessReady` True: `Ready`'s reason, or `WaitingForReady` while `Ready` is Unknown, whose reasons say something else under `Progressing` False.
   A held Tenant keeps `AddonsApplied` at the generation it describes for the whole hold, so `Ready` at the object's generation would never grant an edit Tenant had not taken up when the release started.
   Nor while a cause that fails a change stands on an input of `Ready`, `ProgressDeadlineExceeded` on an available component: that cause's reason, since the change would fail in the write that grants it, for a cause it did not bring.
7. A slot in the order of the queue: `Maintaining`, in the same write as the grant; none, `target.scheduledFor` when the tenant became eligible, which queues it, and `FleetLimit` unless a reason ranked above it applies.

A change Lodia started keeps its origin in `status.maintenance.slotSince`, written in the same write as the grant: the tenant holds its slot while it is present, whatever `Progressing` says, so a reason ranked above the slot's does not free it; a grant on request is the owner's.
Only the change settling and deletion end a slot: an owner's edit rolls in the slot and settles it as any other change, and a paused tenant keeps counting as a used slot, since a tenant a bad release left failed would otherwise leave the fleet on an edit or a pause, and rule 6 would keep the next release waiting for a `Ready` only that release brings.

**Who waits for the window:**

| change | when it rolls |
|---|---|
| the owner edits `spec` | now, and the pending revision with it |
| `maintenance.autoUpdate.kubernetesPatch` finds a newer patch | in the window |
| a new operator revision renders something else | in the window |
| an edit of the tenant's `DatastoreClass` that reaches its `Etcd` | in the window: a class is shared, and the edit rolls every member on it |
| a certificate renewal that needs a restart | now, or at the grant on a tenant Workloads holds; in the window with phase 3's rotations (`todo.md`) |
| a credential rotation the owner started | now; the stages wait for the owner |

**Windows.**
`spec.maintenance.window` ([the field](../api/controlplane.md#spec)) is read with the zone database embedded in the operator (`time/tzdata`); a window inside a repeated hour opens once.
Without a window Lodia-initiated changes roll at any time, still within the fleet limit.

**The fleet.**
Across all tenants, at most `fleet.maxConcurrentRollouts` (operator configuration, 5 by default) roll a Lodia-initiated change at once.
A tenant holds a slot while `status.maintenance.slotSince` is present, whatever `Progressing` reads: a failed rollout Lodia started keeps its slot, so that many failures halt maintenance across the fleet, as unavailable pods halt a Deployment's rollout, which the alert `LodiaMaintenanceHalted` reports from the observability wave.
Tenants get slots in the order of `status.target.scheduledFor`, then namespace and name: a tenant is queued while its `scheduledFor` is at or before now, whatever `Progressing` says, since a wait for a slot can sit under a reason ranked above `FleetLimit`, and a pass grants when the tenants that hold a slot and those queued before it are fewer than the limit, counted from the cache and, before the grant is written, again from the API server under a lock, so that two passes never take one slot.
An owner's change takes no slot.
A change Lodia started holds its slot while it rolls: a part not at the target, a replica or member not updated, a renderer that reports neither the granted revision nor the running one.
A Tenant that waits after the grant, `AddonsApplied` False with a `WaitingFor*` reason while `AccessReady` is True, keeps its earlier `status.addons`, so the change stays `Maintaining` with its slot until the wait ends: such a wait names no cause of the change.
Once it has rolled out, `status.maintenance.settlingSince` records when, and the slot is held for a settling time S after the later of that and `Ready`'s last transition: `health.failuresBeforeFalse` × (`health.period` + Health's timeout) + one etcd-druid status sync ([values.yaml](https://github.com/gardener/etcd-druid/blob/v0.39.0/charts/values.yaml#L63)), 45 seconds at the defaults.
Then the slot is free while `Ready` is True, and the change is `MaintenanceFailed` while it is not; a refusal, an object kept elsewhere, `Unsupported` or `ProgressDeadlineExceeded` among `Ready`'s inputs fails it at once.
A canary, one tenant first for each new revision, can come later without a change of contract.

**Maintenance on request.**
The annotation `lodia.io/maintain-requested-at: <token>` on a `ControlPlane` grants its pending maintenance now ([the field](../api/controlplane.md#spec)).
Lodia never removes the annotation, so a GitOps tool that keeps it does not fight Lodia; it works like the `kubectl.kubernetes.io/restartedAt` that `kubectl rollout restart` writes ([objectrestarter.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/kubectl/pkg/polymorphichelpers/objectrestarter.go#L41)).

**`Progressing` says where a change stands**, by the reasons and order in the [field reference](../api/controlplane.md#conditions-and-their-reasons); it is the one home of the reason a target lags `spec`.

## Why

- The [version skew policy](https://kubernetes.io/releases/version-skew-policy/) allows the controller manager and scheduler to lag the API server by one minor and kubelets by three, never the other way round; the order above is the only safe one, and in-tenant components cannot be newer than the API server.
- A renderer that follows what runs keeps the skew by construction: kubeadm upgrades the addons only once every API server runs the new version ([addons.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/cmd/kubeadm/app/cmd/phases/upgrade/addons.go#L62-L73)), and HyperShift rolls a component only once its dependencies are available at the desired version ([status.go](https://github.com/openshift/hypershift/blob/v0.1.86/support/controlplane-component/status.go#L59-L101)).
  A bug in Lifecycle's sequencing then cannot break the skew, and the current step is derived from what runs on every pass, but for `Backup`, which is recorded: while the `Backup` is taken what runs is identical to a finished step, so nothing would derive it, and the preflight would run again.
- A snapshot before every minor upgrade is the only rollback etcd allows: a minor downgrade of the API server is unsupported.
- In the solutions we studied, a new operator release can re-render every cluster it manages at once; one bug or one changed default then becomes a fleet-wide rollout in the middle of a business day.
  Gardener answers with maintenance time windows ([shoot_maintenance.md](https://github.com/gardener/gardener/blob/v1.154.0/docs/usage/shoot/shoot_maintenance.md#L11-L20)) and a cap on the shoots each gardenlet reconciles at once ([types.go](https://github.com/gardener/gardener/blob/v1.154.0/pkg/apis/config/gardenlet/v1alpha1/types.go#L315-L319)); the same two levers are enough here.
  Counting a failed rollout in its slot makes the cap a brake on a bad release too.
  Without the settling time a revision that changes no template is reported within seconds and frees its slot before a probe can fail, so a release that breaks tenants would reach all of them.
- Every renderer is held, not only pod templates: a re-rendered NetworkPolicy, kube-proxy configuration or `Etcd` is as fleet-wide as a pod template.
- Automatic patch updates in the window keep tenants on supported versions without a platform on top polling for releases; the version table, not `spec`, carries them, since a GitOps tool would revert a written `spec`.

## Rejected

- Rendering every tenant immediately on operator upgrade — simplest code, fleet-wide risk.
- One operator version per tenant (the HyperShift control-plane-operator shape) — the strongest isolation of renderer versions, at the cost of a pod per tenant and a second upgrade path.
- Automatic minor upgrades — a minor can break workloads (removed APIs); the owner opts in by editing `spec.version`.
- A target per step — a central state machine for what each renderer can derive from what runs.
- Waiting at step 4 for the controller manager and the scheduler to have a leader — Cluster API's control plane checks their pods and etcd between steps, not a leader ([preflight.go](https://github.com/kubernetes-sigs/cluster-api/blob/v1.14.3/controlplane/kubeadm/reconcilers/kubeadmcontrolplane/preflight.go#L170-L178)), and a hold would stall an upgrade under way.
- Comparing operator versions as older and newer, or stamping them in pod templates — a rollback has an older version, and a stamp in a template rolls every tenant on every release.
- `status.target.reason` — it repeated `Progressing`'s reason.
- Lifecycle writing `spec.version` for a patch update, as Gardener's maintenance writes the shoot's — `spec` has one writer, its owner.

## Consequences

- Lifecycle is the only controller that knows the maintenance windows and the fleet; it counts the tenants rolling a Lodia-initiated change from their `status` (`status.maintenance.slotSince`), not in memory, so a restarted operator neither forgets nor double-counts a rollout in progress.
- In the `Compact` layout steps 3 and 4 are one rollout of the single pod: the API server, controller manager and scheduler change version together, which the skew policy allows ([compact-layout](compact-layout.md)).
- `status.target` is part of the API users read: `target.version`, `target.operatorRevision` and `target.scheduledFor`, with `Progressing`.
- An operator release note states which tenants will roll (all with a changed render) and what changes, because the roll is deferred, not hidden.

## Code

- `api/v1alpha1` — `Target`, `KubernetesStatus` with `UpgradeStep`, `ComponentStatus` and `Rendered`, `MaintenanceStatus`, the window's rule, the annotations `lodia.io/operator-revision` and `lodia.io/maintain-requested-at`, `PreUpgradeBackupName` and the reasons of `Progressing`; `test/api` holds the rules.
- `cmd/lodia`, `.ko.yaml`, `internal/operator` — the build version, set by ko from `VERSION` (every build of `cmd/lodia` sets it), and `Inputs.Revision`, which adds the digest; the operator refuses to start on an empty version or a revision longer than the field's 128 characters.
- `internal/controller/lifecycle` — `decide` holds the steps above, and `maintain` the grant, its rules and the settling time; `window` reads the window with `time/tzdata`; the reconciler gathers its facts, the `ControlPlane`, the `Backup` of the minor step and, while a revision is pending, the fleet from the cache, decides on the cached object, on a change decides again on the object read live, and before a write that takes a slot decides once more from the fleet the API server lists, under a semaphore that honours the pass's deadline; it requeues at the window's next start, at most an hour ahead, every 30 seconds while `FleetLimit`, and at the end of the settling time.
- `internal/operator` — `fleet.maxConcurrentRollouts`, at least 1, and the settling time from Health's configuration.
- `internal/controller/workloads` — `status.components.*.version`, the tag of each component's own image once every replica runs it, and the controller manager and the scheduler rendered at `components.apiServer.version`; the hold in `apply`, which, while `target.operatorRevision` is not `Reconciler.Revision`, writes only an object neither the cache nor the API server holds; the stamp on the Deployments, which `status.components.*.operatorRevision` reports under the rule of `version`.
- `internal/controller/tenant` — the render at the oldest component's version, and `status.addons`, the version and the running revision of a pass that applied it in full; the hold in `hold` and `heldInventory`, which, while `target.operatorRevision` is not `Reconciler.Revision`, apply only an object the stored inventory lists and the tenant lacks, and report what runs.
- `internal/controller/datastore` — the hold in `copied` and `applyNetworkPolicy`, which, while `target.operatorRevision` is not `Reconciler.Revision`, apply the stored `Etcd`'s spec on the newest certificate generation and a missing NetworkPolicy, and report what runs, and in `storeMoved`, which keeps the credentials copy of the store the held `Etcd` names; the stamp on the `Etcd`; `requestReconcile`, which asks etcd-druid for each reconcile under the record `lodia.io/etcd-reconcile-requested` until `served` says a reconcile under another run ID has taken it, `inFlight`, its wait while such a reconcile runs, and `druidPending`, its wait for etcd-druid's Deployment; `status.datastore.operatorRevision` from `rolledOut`.
- `internal/versions` — `Oldest`, `Latest`, `Next` and `MinorsAhead`, the version arithmetic of a step.
