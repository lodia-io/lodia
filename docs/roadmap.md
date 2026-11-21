# Roadmap

Five phases.
Each ends with a gate: checks that say the phase is done, automated wherever they can be, and where a gate has a number, a number we measured.
A phase does not start before the previous gate is green, and a gate that fails after it passed is a regression, not a backlog item.
A consumer of a phase's contract may be built in that phase, since it is what tests the contract: the Cluster API provider of phase 3 consumes phase 2's API.

| phase | delivers | gate |
|---|---|---|
| 0 · Foundations | the repository can build, test and ship; one control plane built by hand | `make verify` green in CI; the hand-built control plane runs a pod; the etcd spike has numbers |
| 1 · Walking skeleton | a single-replica `ControlPlane` a real kubeadm node joins | e2e: `Ready` in under 90 seconds, node `Ready`, `kubectl logs` and `exec` through the tunnel, conformance passes |
| 2 · Production | HA, Gateway exposure with in-cluster access, backups with `Backup` and restore, upgrades, maintenance windows, Health, metrics | zero failed API requests during a rollout and an upgrade; restore from backup passes conformance; nodes cut off for 10 minutes lose no pod |
| 3 · Released, hardened, ahead | a first release and an API ready to freeze; control planes hardened for tenants whose administrators are untrusted; rehearsed upgrades, templates, rewind, proven backups, staged rotations, hibernation, a tenant that can move; a Cluster API provider | an upgrade stops at its rehearsal, not at a broken API server; control plane pods reach only their own paths; rotations keep nodes `Ready`; sleep and wake keep data; a restore keeps its nodes |
| 4 · Scale and community | sharding across operator replicas, zone spread, `v1beta1`, docs site, supply-chain badges | OpenSSF Scorecard ≥ 8 |

## Phase 0 · Foundations

Scope:

- Repository: Go module, `make verify` (golangci-lint, unit tests, envtest), GitHub Actions, DCO check, the community files.
- The CRD types for `ControlPlane`, `DatastoreClass`, `KubeconfigRequest` and `Backup` with every CEL rule from [`api/controlplane.md`](api/controlplane.md), and an envtest table of accepted and rejected manifests.
- The e2e harness: a kind cluster named `kind-lodia-e2e-*`, and the refusal to run elsewhere ([decision](decisions/e2e-on-a-named-cluster.md)).
- **The hard way, once:** one tenant control plane assembled by hand in kind, following [`hard-way.md`](https://github.com/lodia-io/lodia/blob/005734e/docs/hard-way.md): certificates with `openssl`, etcd, kube-apiserver with konnectivity-server, controller manager, scheduler, then `kubeadm join` of a container running the kind node image.
  Its *What we learned* table feeds the decisions, its files become the first templates, and it is where the team learns every flag it will automate.
  Run on 2026-10-25: [What we learned](https://github.com/lodia-io/lodia/blob/005734e/docs/hard-way.md#what-we-learned).
- The etcd spike: etcd-druid installed from its Helm chart on a plain kind cluster with `enableEtcdSpecAutoReconcile`, an `Etcd` of one member with our own CA, snapshots to an S3-compatible store.
  Then: scale to three, lose one member's volume, hibernate with `replicas: 0` and wake, delete the data of a single member and watch the sidecar restore it, and a kube-apiserver on top through all of it.
  Measured: idle memory and CPU per member and sidecar, snapshot and restore time, time to scale 1 → 3, what assumes Gardener.
  Run on 2026-10-24 ([etcd-spike-2026-10](etcd-spike-2026-10.md)); it decided [dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md).
- Two smaller spikes on the hand-built control plane: whether the konnectivity agent sends SNI from `--proxy-server-host`, so it can share the Gateway's passthrough listener on 443 ([api-exposure](decisions/api-exposure.md)); and a minimal external ServiceAccount signer on a Unix socket, rotating a key under a running API server ([staged-credential-rotation](decisions/staged-credential-rotation.md)).

Gate:

- `make verify` is green in CI on a pull request.
- The hand-built control plane runs `kubectl run` a pod on its joined node, and `kubectl logs` reaches it through konnectivity.
- [`docs/hard-way.md`](https://github.com/lodia-io/lodia/blob/005734e/docs/hard-way.md) and the spike's numbers are committed.

## Phase 1 · Walking skeleton

Scope: the thinnest path through every controller, single replica only, exposure through `loadBalancer` and `nodePort`.

- PKI: CAs, every leaf certificate and kubeconfig, ServiceAccount keys, the encryption configuration; renewal at two thirds of lifetime.
- Datastore: a dedicated etcd of one member through etcd-druid.
- Workloads: the three Deployments, the Services, PodDisruptionBudgets, NetworkPolicies, the hashes that roll a component when its restart-only files change.
- Tenant: kubeadm bootstrap objects, RBAC, kube-proxy, CoreDNS, konnectivity-agent, the inventory and prune.
- Lifecycle: `status.target` without windows; `Ready`.
- Health: the external probe only.
- Helm chart, images by digest, `lodia-system` namespace.

Gate (the e2e suite in [`test/e2e`](../test/e2e/README.md): the first four are checked on every pull request by `TestPhase1Gate`, the fifth every night by `TestConformance`, which the [`nightly`](../.github/workflows/nightly.yml) workflow runs; the last was measured once, and the nightly records it again without asserting it):

- A `ControlPlane` reaches `Ready` in under 90 seconds on kind.
- A container running the kind node image joins with `kubeadm join --token …` and becomes `Ready`; a pod is scheduled on it and runs; `kubectl logs`, `exec` and `port-forward` work through the tunnel.
- Disabling and re-enabling `addons.coreDNS` removes and restores its objects through the inventory.
- Deleting the `ControlPlane` leaves nothing in the namespace but what the reclaim policy keeps.
- Nightly: the Kubernetes conformance suite passes against a tenant with two joined nodes.
- Measured: idle CPU and memory of one control plane, against the planning numbers in [`operations.md`](operations.md#capacity); the numbers replace the plan.

## Phase 2 · Production

Scope:

- `highAvailability`: replica counts, spread, three etcd members, a load loop in e2e.
- `endpoint.gateway` with TLS passthrough routes and the in-cluster apiserver-proxy ([api-exposure](decisions/api-exposure.md)).
- Backups: continuous snapshots to object storage from the `DatastoreClass`; `Backup`, a sealed copy, which an upgrade takes before each minor; and `restoreFrom`, which replaces the `Backup`'s source and stays inert until released ([restore-replaces-the-source](decisions/restore-replaces-the-source.md)).
- Upgrades in the documented order, with the pre-upgrade backup and per-step gates; automatic patch updates; maintenance windows and the fleet limit; deferred re-render on operator upgrade.
- Health: the internal probe, node lease sampling, the controller manager hold.
- `KubeconfigRequest`.
- Observability: Lodia's metrics per tenant, conditions among them, built from `status`, and controller-runtime's per controller; an event per condition transition; a metrics Service, ServiceMonitor, alert rules and dashboards in the chart, all off by default ([metrics-are-part-of-the-api](decisions/metrics-are-part-of-the-api.md)).

It is built in waves.
The fixes came first: API server rollouts that lose no request, the backup prefix and the encryption key names.
Then the contracts: every field, condition, reason and shared name phase 2 decided, in `api/v1alpha1`, with its writer and the wave that writes it in [`api/controlplane.md`](api/controlplane.md).
Then, in parallel, wave 1b: HA, the Gateway with in-cluster access, backups with restore, and `KubeconfigRequest`s.
Then wave 2: upgrades with maintenance windows, the fleet limit and the operator revision; Health's internal probe, node sampling and controller manager hold; observability.

Done from wave 1b: `highAvailability` with [gate G2](../test/e2e/README.md#gate-g2-a-highly-available-control-plane), backups with `Backup` and restore with [gate G3](../test/e2e/README.md#gate-g3-a-backup-and-a-restore), `KubeconfigRequest`s, and the Gateway with in-cluster access, with its gate on every pull request and G6 and conformance behind the Gateway every night ([gates](../test/e2e/README.md#the-gateway-gate)).

Done from wave 2: Health's internal probe, node sampling and the controller manager hold, with gate G4 every night ([gate](../test/e2e/README.md#nightly-gate-g4-nodes-cut-off-from-the-gateway)); upgrades by minors, with the upgrade part of [gate G1](../test/e2e/README.md#gate-g1-a-minor-upgrade) on every pull request; the operator revision, automatic patch updates and edits of the `DatastoreClass`, granted in maintenance windows within the fleet limit, with [gate G5](../test/e2e/README.md#gate-g5-an-operator-upgrade-in-windows) on every pull request; observability: the metrics, the events, the scraping, the alerts and the dashboards, with [gate G7](../test/e2e/README.md#gate-g7-the-leaders-metrics) on every pull request.

Gate:

- e2e: during a rolling update of the API server and during a minor upgrade (two kind node images), a client loop of 20 requests per second against the public endpoint, and one from a pod in the tenant through `kubernetes.default`, record zero failures.
  The minor upgrade is [`TestMinorUpgradeGate`](../test/e2e/README.md#gate-g1-a-minor-upgrade), on every pull request; a chain of two minors and an upgrade of a highly available tenant run in [`nightly-upgrade`](../test/e2e/README.md#nightly-the-chain-and-a-highly-available-upgrade).
- e2e: delete an API server pod and an etcd pod of an HA control plane; `Ready` never turns False for longer than 5 seconds, and the same two loops record zero failures for the API server pod and at most one each for the etcd pod, a call etcd ended without a status ([gate G2](../test/e2e/README.md#gate-g2-a-highly-available-control-plane)).
- e2e: a `Backup` of A; B, created with `restoreFrom` it, stays inert while it serves A's namespaces and Secrets, and a `Released` B still waits, `SourceExists`; deleting A releases B, a node joins it, and the conformance smoke subset passes; every pull request checks it with [`TestRestoreGate`](../test/e2e/README.md#gate-g3-a-backup-and-a-restore).
- e2e: cut both workers of a tenant off from the Gateway for 10 minutes; within 30 seconds Lodia holds the controller manager's lease and the controller manager exits; no node is tainted and every pod keeps its UID; reconnect, and the lease is released 2 minutes later.
  Cut one worker of two: no hold, and its pods move to the other after the toleration, as upstream does ([controller-manager-hold](decisions/controller-manager-hold.md)).
  `TestControllerManagerHold` checks both every night in [`nightly-hold`](../test/e2e/README.md#nightly-gate-g4-nodes-cut-off-from-the-gateway).
- e2e: an operator upgrade re-renders a tenant only inside its window; outside it, `Progressing` is False, `WaitingForWindow`, and `status.target.scheduledFor` is the window's next start; an edit of the tenant's spec renders it at once.
  [`TestOperatorUpgradeInWindows`](../test/e2e/README.md#gate-g5-an-operator-upgrade-in-windows) checks it on every pull request with an upgrade that changes the `Etcd` and an object in the tenant, and the fleet limit runs in [`nightly-upgrade`](../test/e2e/README.md#nightly-the-fleet-limit).
- e2e: behind the metrics filter, the operator's leader serves a tenant's `Ready`, its etcd's leader and size, and the standby none of Lodia's series; the tenant's turn to `Ready` is an event regarding it.
  [Gate G7](../test/e2e/README.md#gate-g7-the-leaders-metrics), a step of `TestPhase1Gate`, checks both on every pull request.
- Measured: API p99 latency through the Gateway against a direct Service, overhead under 5 ms on kind.
  `TestGatewayOverhead` measures it in [`nightly-gateway`](../test/e2e/README.md#nightly-g6-and-conformance-behind-the-gateway); on 2026-11-08 on Docker Desktop's kernel 6.12.54-linuxkit with 16 CPUs, Δp99 was 0.06 ms through the Gateway and 0.17 ms through the tunnel on reused connections, and 0.11 and 0.99 ms on new ones, with no error.

## Phase 3 · Released, hardened, ahead

Three stages, in this order: a release a platform can pin, then control planes safe for tenants whose administrators are untrusted ([who-is-trusted](decisions/who-is-trusted.md)), then the features that put Lodia ahead.

Scope, to a first release:

- Health's reads of a tenant bounded, whatever its server answers ([controller-manager-hold](decisions/controller-manager-hold.md)).
- Gates that say what they test: G2's bound, G5's changed `Etcd` and object in the tenant, the phase 1 gate's stages ([gates](../test/e2e/README.md)).
- The API before it freezes: every field no controller reads leaves `v1alpha1`, the `Datastore` kind with them, and comes back with the change that builds it; `restoreFrom.release: Held` becomes `Pending`; the names only Lodia's controllers share leave the public module.
- A synthetic fleet of 1,000 `ControlPlane`s on envtest, run on demand and every night, and the operator's memory request and limit set from its numbers.
- One client of a tenant for PKI, Tenant and Health, with Health's bounds on every read, and the Secrets it needs read from the cache.
- Read-your-writes: a controller's next pass sees the status it wrote, whatever its cache's lag.
- Structured authentication: `apiServer.authentication.jwt` as upstream's `JWTAuthenticator`, an issuer's edit applied without a restart.
- The rehearsal of a minor step: before the API server moves, a short-lived copy of the tenant's data under the next minor's API server shows that the step will start and every resource still reads ([rehearse-a-minor-step](decisions/rehearse-a-minor-step.md)); Kubernetes 1.38 joins the version table after it.
- etcd-druid v0.39.2, nightlies that pin how Lodia asks it to reconcile and how a member restores through delta snapshots, and a seam that keeps its types out of the controllers ([dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md)).
- A first release: the release workflow, signed images, the chart at a version, Renovate ([releases-and-support](decisions/releases-and-support.md)).

Scope, to the first production tenant:

- Deleting a `ControlPlane` needs a confirmation, which an admission policy of the chart checks and the Cluster API provider gives ([who-is-trusted](decisions/who-is-trusted.md)).
- Control plane pods locked down: egress denied but for the paths a control plane needs, the `restricted` security context on every container, a memory limit per component.
- Sizing and prerequisites: what one tenant takes of a node's pods, pod range and volumes; a management cluster that runs nothing else and encrypts its data at rest.

Scope, ahead, each feature designed first:

- A credential of its own for the Cluster API provider and for each extension, and a status field that says whether a cluster may act outside itself.
- Operations as objects, and the client CA rotation ([one-controlplane-per-cluster](decisions/one-controlplane-per-cluster.md)).
- Templates: a new cluster from a prepared one, every object under a new UID.
- Rewind: a namespace or a resource type brought back from etcd's history.
- Backups that are proven: a drill per tenant, its result in status.
- No surprise rollouts: a digest of what each tenant renders, a canary, a report before an upgrade.
- What a tenant's own users see: an audit policy, the ServiceAccount issuer's public discovery, a scheduler configuration, the control plane's logs.
- The cheapest idle tenant: hibernation with schedules, a class for small tenants.
- The other rotations: the cluster CA, the ServiceAccount key, the encryption key, and the etcd, front-proxy and tunnel CAs ([staged-credential-rotation](decisions/staged-credential-rotation.md)).
- A second version of the controller manager hold, which today leaves a tenant of one node alone, and its margin per minor.
- Upkeep and adoption: a generated field reference, code the controllers share, a nightly with a DRA driver.
- Last, a tenant that can move: a restore that keeps the tenant's identity, so its nodes stay without joining again, which [restore-replaces-the-source](decisions/restore-replaces-the-source.md) leaves to this phase; then a move between management clusters, and an import.

And through the phase, a Cluster API control plane provider in its own repository.

Not in phase 3:

- A shared `Datastore`, its write lock and migration between stores.
  The API server's compactor writes and watches `compact_rev_key`, a key outside `--etcd-prefix` ([compact.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/compact.go#L35)), so the API servers of tenants that share an etcd by prefix share that key.

Gate (a feature of the last stage adds its check here with its design):

- e2e: a minor upgrade of a tenant that sets a feature gate the next minor removed stops at its rehearsal with the reason, its API server untouched, and a tenant without one steps as it does today; the minor upgrade's gate checks both.
- e2e, every night: etcd-druid upgraded across a patch under a held tenant serves each request of the Datastore controller once and reports its revision once; a one-member tenant whose volume is deleted after a delta snapshot comes back with the key that delta holds.
- e2e: a tenant whose JWT authenticator maps claims by CEL to groups, `uid` and `extra` and validates the user accepts a token of its issuer as that user, and an edit of the issuer applies without a restart of the API server.
- envtest: deleting a `ControlPlane` without the confirmation is refused; the Cluster API provider's e2e still deletes its cluster.
- e2e: the gates pass with egress denied and the `restricted` security context on every control plane pod, and a control plane pod that dials the management network is refused.
- e2e: a Cluster API `Cluster` whose control plane is Lodia's provider reaches `ControlPlaneInitialized`, and a `MachineDeployment` of Cluster API's Docker machines joins it through the kubeadm bootstrap provider.
- e2e: start and complete a CA rotation while two nodes stay `Ready` and a workload keeps calling the API with a ServiceAccount token; the same for the ServiceAccount key and the encryption key; once the client CA rotation completes, an admin kubeconfig of the old client CA no longer works.
- e2e: a `ControlPlane` created from a template reaches `Ready` with the template's objects, each under a new UID.
- e2e: a namespace deleted in a tenant is rewound from etcd's history and its objects are back.
- e2e: a tenant's backup drill restores its newest snapshot without touching the tenant, and its result is in status.
- e2e: an operator upgrade that changes nothing a tenant renders rolls nothing of it.
- e2e: hibernate, wait, wake; every object and Secret is there, nodes rejoin without intervention.
- e2e: a `ControlPlane` restored from a `Backup` with its identity kept: its nodes stay `Ready` without joining again and every pod keeps its UID; a move to another management cluster loses no acknowledged write.
- Measured: the operator with 1,000 `ControlPlane`s on envtest, its memory and how long a spec change takes to reconcile; the numbers set the chart's memory request and limit.

Done: Health's bounded reads, with the tenant clients of [tenant-clients-speak-http1](decisions/tenant-clients-speak-http1.md); the gates that say what they test; structured authentication ([structured-authentication](decisions/structured-authentication.md)), its e2e gate above written and not yet seen in CI.

## Phase 4 · Scale and community

Scope: label-based sharding of tenants across operator replicas; zone spread; `v1beta1`, of `v1alpha1`'s frozen shape; the documentation site; OpenSSF Scorecard; the first adopters.

Gate:

- Scorecard ≥ 8.

## What is deliberately not on the roadmap

- Machines: Lodia joins nodes, it does not create them.
- SQL or NATS stores through kine: later, if users ask.
- A VPN between control plane and nodes: konnectivity is upstream and enough.
- A Lodia agent on nodes.
