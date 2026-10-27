# Roadmap

Five phases. Each ends with a gate: an automated check whose exit code says
the phase is done, a number we measured against a target, and a scenario
someone can watch. A phase does not start before the previous gate is green,
and a gate that fails after it passed is a regression, not a backlog item.

| phase | delivers | gate |
|---|---|---|
| 0 · Foundations | the repository can build, test and ship; one control plane built by hand | `make verify` green in CI; the hand-built control plane runs a pod; the etcd spike has numbers |
| 1 · Walking skeleton | a single-replica `ControlPlane` a real kubeadm node joins | e2e: `Ready` in under 3 minutes, node `Ready`, `kubectl logs` and `exec` through the tunnel, conformance passes |
| 2 · Production | HA, Gateway exposure with in-cluster access, backups with `Backup` and restore, upgrades, maintenance windows, Health, metrics | zero failed API requests during a rollout and an upgrade; restore from backup passes conformance; nodes cut off for 10 minutes lose no pod |
| 3 · Parity | shared etcd, migration by snapshot, staged rotations, hibernation, write lock, the `Compact` layout, a Cluster API provider | rotation keeps nodes `Ready` and tokens valid; sleep and wake keep data; migration freezes writes for seconds, reads never; a `Compact` tenant is one pod |
| 4 · Scale and community | hundreds of tenants per operator, zone spread, `v1beta1`, docs site, supply-chain badges | 200 tenants on one management cluster within the resource budget; OpenSSF Scorecard ≥ 8 |

Segla moves to Lodia in phase 3, through Lodia's Cluster API provider.
Segla keeps Cluster API for the worker pools it will create through it, and
Cluster API's kubeadm bootstrap provider joins a worker only once a control
plane provider reports the control plane initialized
([kubeadmconfig_controller.go](https://github.com/kubernetes-sigs/cluster-api/blob/v1.14.3/bootstrap/kubeadm/reconcilers/kubeadmconfig/kubeadmconfig_controller.go#L351-L367)).
Its `cluster` service then swaps the `KamajiControlPlane` for the
provider's object, its agent drops its own API proxy, and its regional
watchdog steps down for Lodia's controller manager hold. That work is
tracked in Segla's repositories, not here.

## Phase 0 · Foundations

Scope:

- Repository: Go module, `make verify` (golangci-lint, unit tests, envtest),
  GitHub Actions, DCO check, the community files.
- The CRD types for `ControlPlane`, `DatastoreClass`, `Datastore`,
  `KubeconfigRequest` and `Backup` with every CEL rule from
  [`api/controlplane.md`](api/controlplane.md), and an envtest table of
  accepted and rejected manifests.
- The e2e harness: a kind cluster named `kind-lodia-e2e-*`, and the refusal
  to run elsewhere ([decision](decisions/e2e-on-a-named-cluster.md)).
- **The hard way, once:** one tenant control plane assembled by hand in kind,
  following [`hard-way.md`](hard-way.md): certificates with `openssl`, etcd,
  kube-apiserver with konnectivity-server, controller manager, scheduler,
  then `kubeadm join` of a container running the kind node image. Its
  *What we learned* table feeds the decisions, its files become the first
  templates, and it is where the team learns every flag it will automate.
  Run on 2026-10-25: [What we learned](hard-way.md#what-we-learned).
- The etcd spike: etcd-druid installed from its Helm chart on a plain kind
  cluster with `enableEtcdSpecAutoReconcile`, an `Etcd` of one member with
  our own CA, snapshots to an S3-compatible store. Then: scale to three,
  lose one member's volume, hibernate with `replicas: 0` and wake, delete the
  data of a single member and watch the sidecar restore it, and a
  kube-apiserver on top through all of it. Measured: idle memory and CPU per member and sidecar,
  snapshot and restore time, time to scale 1 → 3, what assumes Gardener.
  Run on 2026-10-24 ([etcd-spike-2026-10](etcd-spike-2026-10.md)); it decided
  [dedicated-etcd-by-default](decisions/dedicated-etcd-by-default.md).
- Two smaller spikes on the hand-built control plane: whether the
  konnectivity agent sends SNI from `--proxy-server-host`, so it can share the
  Gateway's passthrough listener on 443
  ([api-exposure](decisions/api-exposure.md)); and a minimal external
  ServiceAccount signer on a Unix socket, rotating a key under a running API
  server ([staged-credential-rotation](decisions/staged-credential-rotation.md)).

Gate:

- `make verify` is green in CI on a pull request.
- The hand-built control plane runs `kubectl run` a pod on its joined node,
  and `kubectl logs` reaches it through konnectivity.
- `docs/hard-way.md` and the spike's numbers are committed.

## Phase 1 · Walking skeleton

Scope: the thinnest path through every controller, single replica only,
exposure through `loadBalancer` and `nodePort`.

- PKI: CAs, every leaf certificate and kubeconfig, ServiceAccount keys, the
  encryption configuration; renewal at two thirds of lifetime.
- Datastore: a dedicated etcd of one member through etcd-druid.
- Workloads: the three Deployments, the Services, PodDisruptionBudgets,
  NetworkPolicies, the hashes that roll a component when its restart-only
  files change.
- Tenant: kubeadm bootstrap objects, RBAC, kube-proxy, CoreDNS,
  konnectivity-agent, the inventory and prune.
- Lifecycle: `status.target` without windows; `Ready`.
- Health: the external probe only.
- Helm chart, images by digest, `lodia-system` namespace.

Gate (the e2e suite in [`test/e2e`](../test/e2e/README.md): the first four
are checked on every pull request by `TestPhase1Gate`, the fifth every night
by `TestConformance`, which the
[`nightly`](../.github/workflows/nightly.yml) workflow runs; the last was
measured once, and the nightly records it again without asserting it):

- A `ControlPlane` reaches `Ready` in under 3 minutes on kind.
- A container running the kind node image joins with
  `kubeadm join --token …` and becomes `Ready`; a pod is scheduled on it and
  runs; `kubectl logs`, `exec` and `port-forward` work through the tunnel.
- Disabling and re-enabling `addons.coreDNS` removes and restores its objects
  through the inventory.
- Deleting the `ControlPlane` leaves nothing in the namespace but what the
  reclaim policy keeps.
- Nightly: the Kubernetes conformance suite passes against a tenant with two
  joined nodes.
- Measured: idle CPU and memory of one control plane, against the planning
  numbers in [`operations.md`](operations.md#capacity); the numbers replace
  the plan.

## Phase 2 · Production

Scope:

- `highAvailability`: replica counts, spread, three etcd members, a load
  loop in e2e.
- `endpoint.gateway` with TLS passthrough routes and the in-cluster
  apiserver-proxy ([api-exposure](decisions/api-exposure.md)).
- Backups: continuous snapshots to object storage from the `DatastoreClass`;
  `Backup`, a sealed copy, which an upgrade takes before each minor; and
  `restoreFrom`, which replaces the `Backup`'s source and stays inert until
  released ([restore-replaces-the-source](decisions/restore-replaces-the-source.md)).
  `Backup` was phase 3's until the upgrades and the restore needed it.
- Upgrades in the documented order, with the pre-upgrade backup and
  per-step gates; automatic patch updates; maintenance windows and the fleet
  limit; deferred re-render on operator upgrade.
- Health: the internal probe, node lease sampling, the controller manager
  hold.
- `KubeconfigRequest`.
- Observability: metrics per controller and tenant, conditions as metrics,
  events, the Grafana dashboard and alert rules in the chart.

It is built in waves. The fixes came first: API server rollouts that lose
no request, the backup prefix and the encryption key names. Then the
contracts: every field, condition, reason and shared name phase 2 decided,
in `api/v1alpha1`, with its writer and the wave that writes it in
[`api/controlplane.md`](api/controlplane.md). Then, in parallel, wave 1b:
HA, the Gateway with in-cluster access, backups with restore, and
`KubeconfigRequest`s. Then wave 2: upgrades with maintenance windows, the
fleet limit and the operator revision; Health's internal probe, node
sampling and controller manager hold; observability.

Gate:

- e2e: during a rolling update of the API server and during a minor upgrade
  (two kind node images), a client loop of 20 requests per second against
  the public endpoint, and one from a pod in the tenant through
  `kubernetes.default`, record zero failures.
- e2e: delete an API server pod and an etcd pod of an HA control plane;
  `Ready` never turns False for longer than Health's period, 5 seconds, and
  the same two loops record zero failures.
- e2e: a `Backup` of A; B, created with `restoreFrom` it, stays inert while
  it serves A's namespaces and Secrets, and a `Released` B still waits,
  `SourceExists`; deleting A releases B, a node joins it, and the
  conformance smoke subset passes.
- e2e: cut both workers of a tenant off from the Gateway for 10 minutes;
  within 30 seconds Lodia holds the controller manager's lease and the
  controller manager exits; no node is tainted and every pod keeps its UID;
  reconnect, and the lease is released 2 minutes later. Cut one worker of two:
  no hold, and its pods move to the other after the toleration, as upstream
  does ([controller-manager-hold](decisions/controller-manager-hold.md)).
- e2e: an operator upgrade re-renders a tenant only inside its window;
  outside it, `Progressing` is False, `WaitingForWindow`, and
  `status.target.scheduledFor` is the window's next start; an edit of the
  tenant's spec renders it at once.
- Measured: API p99 latency through the Gateway against a direct Service,
  overhead under 5 ms on kind.

## Phase 3 · Parity

Scope: shared `Datastore` with a user and prefix per tenant; migration
between stores through a write lock and a snapshot; staged rotation of the
CA, the ServiceAccount key and the etcd encryption key; hibernation with
schedules; the `ValidatingAdmissionPolicy` write lock; the
`Compact` layout; a Cluster API control plane provider in its own repository.

Gate:

- e2e: start and complete a CA rotation while two nodes stay `Ready` and a
  workload keeps calling the API with a ServiceAccount token; the same for
  the ServiceAccount key and the encryption key.
- e2e: hibernate, wait, wake; every object and Secret is there, nodes rejoin
  without intervention.
- e2e: migrate a tenant from a shared `Datastore` to a dedicated etcd; writes
  are refused for at most 30 seconds, reads succeed throughout.
- e2e: a `Compact` `ControlPlane` on a shared `Datastore` runs as one pod; a
  node joins, the conformance smoke subset passes, and the controller manager
  hold works as in `Split` ([compact-layout](decisions/compact-layout.md)).
- e2e: a Cluster API `Cluster` whose control plane is Lodia's provider
  reaches `ControlPlaneInitialized`, and a `MachineDeployment` of Cluster
  API's Docker machines joins it through the kubeadm bootstrap provider.

## Phase 4 · Scale and community

Scope: label-based sharding of tenants across operator replicas; zone
spread; 200 tenants on one test management cluster with kwok-simulated
nodes; `v1beta1` with the lessons of the alpha; the documentation site;
signed images, SBOMs, provenance; OpenSSF Scorecard; the first adopters.

Gate:

- 200 `ControlPlane`s reach `Ready`; the operator stays under 2 GiB of memory
  and reconciles a spec change in under 10 seconds at that scale.
- Scorecard ≥ 8; every image signed and with provenance.

## What is deliberately not on the roadmap

- Machines: Lodia joins nodes, it does not create them.
- SQL or NATS stores through kine: later, if users ask; the `Datastore` kind
  leaves room for a `driver`.
- A VPN between control plane and nodes: konnectivity is upstream and
  enough.
- A Lodia agent on nodes.
