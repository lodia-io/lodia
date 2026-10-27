# Upgrades follow the version skew policy step by step; changes Lodia initiates roll in each tenant's maintenance window, a bounded number of tenants at a time

Decided 2026-10-25.
Not built: phase 2's wave 2, upgrades, builds it ([roadmap](../roadmap.md)).
Until then Lifecycle moves `status.target.version` within a minor only, and every renderer renders with the running operator at once.

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

1. Preflight: the next minor is in the version table (else `Unsupported`); `DatastoreReady` (else `WaitingForDatastore`); every kubelet at most three minors older than the next minor, from `status.nodes.oldestKubelet` (else `WaitingForNodes`); no hold of the controller manager (else `ControllerManagerHeld`).
2. The `Backup` `<cp>-pre-upgrade-<version>-<ControlPlane UID>` (`PreUpgradeBackupName`) completes; a `Backup` outlives its source, so the UID keeps a predecessor's of the same name from standing in for it.
   The target moves once it completes, or once it reports that the class has no backup store, which `Progressing` then says.
3. The API server rolls; the step is done when every replica runs the new template, updated and ready, since `/readyz` reports no version.
4. The controller manager and the scheduler roll.
5. In the tenant, kube-proxy, CoreDNS and konnectivity-agent move to the new minor's versions; done when they are applied.
   Their pods roll on the owner's nodes and are reported in `AddonsAvailable`, not awaited.
6. `status.kubernetes.version` becomes the new version.
   Nodes are the owner's: `status.nodes` reports how many run an older kubelet, and nothing forces them.

A patch step skips the `Backup`.
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
`operatorRevision` is the operator's build version and a digest of what it renders from: the version table, the registry, the default resources, the images of `lodia-signer` and `lodia-tunnel`, and etcd-druid's version.
Revisions are compared for equality only, so a rollback of the operator is one more revision.
Workloads, Tenant and the Datastore controller render with the running revision only while `status.target.operatorRevision` equals it ([what they hold until then](../api/controlplane.md#status)).
Each stamps the revision on the objects it renders, in the annotation `lodia.io/operator-revision`, never in a pod template, so a revision that changes nothing rolls nothing.
etcd-druid's automatic spec reconcile is off, and the Datastore controller asks for each reconcile ([dedicated-etcd-by-default](dedicated-etcd-by-default.md)).
Lifecycle grants the running revision when a `ControlPlane` is created, when its spec changes, during an upgrade its owner started, and otherwise in the window, with a free slot of the fleet, while the tenant is `Ready`.
A hand edit is not reverted while a tenant waits; storing the last render, as Gardener's ManagedResources do, stays a later option with the same contracts.

**Who waits for the window:**

| change | when it rolls |
|---|---|
| the owner edits `spec` | now, and the pending revision with it |
| `maintenance.autoUpdate.kubernetesPatch` finds a newer patch | in the window |
| a new operator revision renders something else | in the window |
| an edit of the tenant's `DatastoreClass` that reaches its `Etcd` | in the window: a class is shared, and the edit rolls every member on it |
| a certificate renewal that needs a restart | in the window, or now if the certificate expires before the next window |
| a credential rotation the owner started | now; the stages wait for the owner |

**Windows.**
`spec.maintenance.window` ([the field](../api/controlplane.md#spec)) is read with the zone database embedded in the operator (`time/tzdata`); a window inside a repeated hour opens once.
Without a window Lodia-initiated changes roll at any time, still within the fleet limit.

**The fleet.**
Across all tenants, at most `fleet.maxConcurrentRollouts` (operator configuration, 5 by default) roll a Lodia-initiated change at once.
A tenant holds a slot while `Progressing` is True, `Maintaining`, or False, `MaintenanceFailed`: a failed rollout Lodia started keeps its slot, so that many failures halt maintenance across the fleet, as unavailable pods halt a Deployment's rollout, and the alert `LodiaMaintenanceHalted` fires.
Tenants get slots in the order of `status.target.scheduledFor`, then namespace and name; grants are made one at a time, counted from an uncached list.
An owner's change takes no slot.
A canary, one tenant first for each new revision, can come later without a change of contract.

**Maintenance on request.**
The annotation `lodia.io/maintain-requested-at: <token>` on a `ControlPlane` grants its pending maintenance now ([the field](../api/controlplane.md#spec)).
Lodia never removes the annotation, so a GitOps tool that keeps it does not fight Lodia; it works like the `kubectl.kubernetes.io/restartedAt` that `kubectl rollout restart` writes ([objectrestarter.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/kubectl/pkg/polymorphichelpers/objectrestarter.go#L41)).

**`Progressing` says where a change stands**, by the reasons and order in the [field reference](../api/controlplane.md#conditions-and-their-reasons); it is the one home of the reason a target lags `spec`.

## Why

- The [version skew policy](https://kubernetes.io/releases/version-skew-policy/) allows the controller manager and scheduler to lag the API server by one minor and kubelets by three, never the other way round; the order above is the only safe one, and in-tenant components cannot be newer than the API server.
- A renderer that follows what runs keeps the skew by construction: kubeadm upgrades the addons only once every API server runs the new version ([addons.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/cmd/kubeadm/app/cmd/phases/upgrade/addons.go#L62-L73)), and HyperShift rolls a component only once its dependencies are available at the desired version ([status.go](https://github.com/openshift/hypershift/blob/v0.1.86/support/controlplane-component/status.go#L59-L101)).
  A bug in Lifecycle's sequencing then cannot break the skew, and the current step is derived from what runs on every pass; `status.kubernetes.upgrade` only reports it.
- A snapshot before every minor upgrade is the only rollback etcd allows: a minor downgrade of the API server is unsupported.
- In the solutions we studied, a new operator release can re-render every cluster it manages at once; one bug or one changed default then becomes a fleet-wide rollout in the middle of a business day.
  Gardener answers with maintenance time windows ([shoot_maintenance.md](https://github.com/gardener/gardener/blob/v1.154.0/docs/usage/shoot/shoot_maintenance.md#L11-L20)) and a cap on the shoots each gardenlet reconciles at once ([types.go](https://github.com/gardener/gardener/blob/v1.154.0/pkg/apis/config/gardenlet/v1alpha1/types.go#L315-L319)); the same two levers are enough here.
  Counting a failed rollout in its slot makes the cap a brake on a bad release too.
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

- Lifecycle is the only controller that knows the time and the fleet; it counts the tenants rolling a Lodia-initiated change from their `status` (`Progressing`), not in memory, so a restarted operator neither forgets nor double-counts a rollout in progress.
- In the `Compact` layout steps 3 and 4 are one rollout of the single pod: the API server, controller manager and scheduler change version together, which the skew policy allows ([compact-layout](compact-layout.md)).
- `status.target` is part of the API users read: `target.version`, `target.operatorRevision` and `target.scheduledFor`, with `Progressing`.
- An operator release note states which tenants will roll (all with a changed render) and what changes, because the roll is deferred, not hidden.

## Code

- `api/v1alpha1` — `Target`, `KubernetesStatus` with `UpgradeStep`, `ComponentStatus` and `Rendered`, `MaintenanceStatus`, the window's rule, the annotations `lodia.io/operator-revision` and `lodia.io/maintain-requested-at`, `PreUpgradeBackupName` and the reasons of `Progressing`; `test/api` holds the rules.
- `internal/controller/lifecycle` — the target within a minor, as phase 1 built it.
