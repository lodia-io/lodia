# End-to-end tests

The suite runs only against a kind cluster whose kubeconfig is named on the
command line: `E2E_KUBECONFIG` points at a file with exactly one context, and
that context's name starts with `kind-lodia-e2e`. Anything else, the current
kube context included, stops it before the first request
([decision](../../docs/decisions/e2e-on-a-named-cluster.md)).

`make e2e` creates the kind cluster `lodia-e2e-local` (`E2E_CLUSTER` changes
the name), runs the suite against it and deletes it.

To keep a cluster between runs:

```sh
mkdir -p work/e2e
go tool kind create cluster --name lodia-e2e-dev --kubeconfig work/e2e/dev.kubeconfig
E2E_KUBECONFIG=work/e2e/dev.kubeconfig make e2e
E2E_KUBECONFIG=$PWD/work/e2e/dev.kubeconfig go test -tags e2e -count=1 ./test/e2e/... -run TestExamplesOnAKindCluster
go tool kind delete cluster --name lodia-e2e-dev --kubeconfig work/e2e/dev.kubeconfig
```

`--kubeconfig` makes kind write that file alone and leave `~/.kube/config`
untouched. `go test` runs in the package directory, so it needs an absolute
`E2E_KUBECONFIG`; `make e2e` resolves a relative one, and runs `go test -v`
so that each scenario's lines are in the log.

Before the first test, the suite installs Lodia as a user does, and needs
Docker and `helm` on the `PATH` for it: it builds `cmd/lodia` and
`cmd/lodia-signer` with ko straight into the kind cluster
(`KO_DOCKER_REPO=kind.local`, as the hard way does), then runs
`helm dependency build` and installs [`charts/lodia`](../../charts/lodia)
into `lodia-system` with those two images. The chart brings the CRDs and
etcd-druid v0.39.0, its dependency; the operator runs in the cluster under
the chart's RBAC, and every test asserts against it. The suite leaves the
chart installed.

- `TestDatastoreOnKind`: PKI issues the etcd certificate generations, and
  generation 2 is the one it issues under the same CA once a member Secret
  of generation 1 is deleted, the roll a tenant sees in production.
- `TestOperatorFromTheChart`: the operator's Deployment is Available, a
  running replica holds the leader Lease `lodia.io`, and a namespace that
  enforces Pod Security `restricted` admits the operator's pod in a dry run.
- `TestExamplesOnAKindCluster`: the CRDs the chart installed accept every
  example in [`docs/api/examples`](../../docs/api/examples).
- `TestAPIServerOnKind`: a `ControlPlane` in `nodePort` mode reports
  `APIServerReady=True`: a real kube-apiserver, controller manager and
  scheduler on PKI's certificates and the Datastore's etcd.
- `TestPhase1Gate`: the per-pull-request part of the
  [phase 1 gate](../../docs/roadmap.md#phase-1--walking-skeleton). A
  `ControlPlane` in `nodePort` mode, at the version of kind's default node
  image, turns `Ready`, and the suite logs the time from its creation, which
  must be under 3 minutes. Anonymous requests reach the health paths and
  `cluster-info` and nothing else: `/version`, which RBAC grants
  unauthenticated users
  ([policy.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go#L383-L388),
  [binding](https://github.com/kubernetes/kubernetes/blob/v1.37.0/plugin/pkg/auth/authorizer/rbac/bootstrappolicy/policy.go#L723)),
  answers 401. A node joins (below) and turns `Ready`; a pod of
  agnhost, pinned by digest, runs on it, and its logs, an `exec` and a
  port-forward reach it through the konnectivity tunnel: before the three
  and after each, the suite reads konnectivity-server's dial count, the
  count of its `konnectivity_network_proxy_server_dial_duration_seconds`
  histogram, observed once per established dial
  ([metrics.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/pkg/server/metrics/metrics.go#L76-L85),
  [server.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/pkg/server/server.go#L1067)),
  on its health port, 8134
  ([server.go](https://github.com/kubernetes-sigs/apiserver-network-proxy/blob/v0.37.0/cmd/server/app/server.go#L585)),
  through a port-forward to the API server pod, and wants each call to add
  one dial. `exec` runs twice, over client-go's SPDY transport and over
  WebSocket, kubectl's default; the port-forward uses SPDY, and kubectl's
  WebSocket port-forward reaches the kubelet through the same transport and
  tunnel
  ([exec](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/registry/core/pod/rest/subresources.go#L208-L230),
  [port-forward](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/registry/core/pod/rest/subresources.go#L293-L306)),
  so the dial count covers it too. Disabling `addons.coreDNS` removes
  CoreDNS's objects from the tenant and from the inventory, and enabling it
  brings them back.
  Deleting the `ControlPlane` leaves in its namespace, of every namespaced
  kind the API server serves, Events aside, which outlive their objects,
  only the etcd's claim, which the class's `Retain` keeps, and what every
  namespace holds: the ConfigMap `kube-root-ca.crt` and the ServiceAccount
  `default`.

A node joins as in [the hard way](../../docs/hard-way.md#11-join-a-node).
The suite reaches the tenant's API server through a port-forward to the API
server pod's secure port, as `<cp>-admin-kubeconfig` with its server set to
the forwarded address, which the serving certificate names (`127.0.0.1`),
and, as the platform, creates a bootstrap token Secret there
([nodes-join-with-kubeadm](../../docs/decisions/nodes-join-with-kubeadm.md)).
Each connection to a forwarded address gets a port-forward of its own:
client-go ends a port-forward, with every connection through it, on the
first forwarding error
([portforward.go](https://github.com/kubernetes/client-go/blob/v0.37.1/tools/portforward/portforward.go#L545-L550)),
and an exec's connection meets one when the API server has closed it before
the client's last write (`write: broken pipe`).
It starts a container of kind's node image, `<cluster>-tenant-node-1`, on
the docker network `kind`, waits for its entrypoint to report the boot as
kind does
([cgroups.go](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/pkg/cluster/internal/providers/common/cgroups.go#L33-L47)),
and runs `kubeadm join <status.endpoint.apiServer> --token …
--discovery-token-ca-cert-hash sha256:…` in it. `--patches` names
[`testdata/kubeadm-patches`](testdata/kubeadm-patches), whose
`kubeletconfiguration` patch, the node's own configuration over the
cluster's
([patches](https://github.com/kubernetes/website/blob/snapshot-initial-v1.37/content/en/docs/setup/production-environment/tools/kubeadm/control-plane-flags.md?plain=1#L185-L205),
[--patches](https://github.com/kubernetes/kubernetes/blob/v1.37.0/cmd/kubeadm/app/cmd/options/generic.go#L104-L114)),
carries the kubelet settings kind gives its nodes
([config.go](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/pkg/cluster/internal/kubeadm/config.go#L596-L615)),
and the `ControlPlane` sets `addons.kubeProxy.conntrack.maxPerCore: 0`.
`--ignore-preflight-errors` names the one check a container fails,
`SystemVerification`: on GitHub's runners it finds the host kernel's
configuration in none of the places it looks, and `modprobe configs` fails
([kernel_validator.go](https://github.com/kubernetes/system-validators/blob/v1.12.1/validators/kernel_validator.go#L191-L245),
the version kubeadm v1.37.0 uses). Ignoring it skips kubeadm's OS and
cgroups validators too, which run under the same name; a check's name is
the finest grain kubeadm ignores by
([checks.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/cmd/kubeadm/app/preflight/checks.go#L573-L588),
[checks_linux.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/cmd/kubeadm/app/preflight/checks_linux.go#L48-L54)).
The CNI is kindnet from the node image's manifest, with the tenant's pod
range.
The suite removes the containers it started when it ends, pass or fail, and
each one before it starts it; `make e2e` and `make conformance` remove them
as well, since a `go test` timeout or a panic never reaches the suite's end.

`make e2e` gives the suite 30 minutes.

## Nightly: conformance and the idle control plane

`TestConformance` checks the last two items of the
[phase 1 gate](../../docs/roadmap.md#phase-1--walking-skeleton). It sits
behind a build tag of its own, `conformance` with `e2e`, so the
per-pull-request suite never builds it;
[`nightly.yml`](../../.github/workflows/nightly.yml) runs it every night, on
demand, and on a pull request that changes the workflow or the test.
`make conformance` runs it as `make e2e` runs the suite: a kind cluster of
its own (`E2E_CLUSTER`), the suite's setup with the operator from the chart,
`TestConformance` alone, then the node containers and the cluster removed.
`CONFORMANCE_FOCUS`, a regular expression, runs only the conformance tests
it matches, to check the plumbing in a quarter of an hour:

```sh
make conformance E2E_CLUSTER=lodia-e2e-conformance
make conformance E2E_CLUSTER=lodia-e2e-conformance CONFORMANCE_FOCUS='Pods should support retrieving logs from the container over websockets'
```

`go test -artifacts` keeps what the test writes in
`work/e2e/_artifacts/test/e2e/TestConformance/<n>/`
([ArtifactDir](https://github.com/golang/go/blob/go1.26.0/src/testing/testing.go#L1325-L1342)):
the table of the idle control plane, `idle-control-plane.txt`, and
hydrophone's `e2e.log` and `junit_01.xml`; the workflow uploads them.

The tenant has the gate's shape, and two containers of kind's node image,
`<cluster>-tenant-node-1` and `-2`, join it as the gate's one does. Once the
`ControlPlane` is `Ready`, both nodes are `Ready` with a ready konnectivity
agent and CoreDNS is available, the test leaves the tenant idle for 3
minutes, then reads the resource metrics of the management kubelets,
`/metrics/resource`, through the management API server's node proxy, twice,
5 minutes apart, for every container of the control plane's pods: the API
server, konnectivity-server and the signer, the controller manager, the
scheduler, etcd and its backup-restore sidecar. CPU is the rate of
`container_cpu_usage_seconds_total`, the container's cumulative CPU time in
core-seconds, between the two samples' timestamps; memory is
`container_memory_working_set_bytes` at the second, the working set:
recently accessed, dirty and kernel memory
([resource_metrics.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/kubelet/metrics/collectors/resource_metrics.go#L51-L63),
[collected](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/kubelet/metrics/collectors/resource_metrics.go#L228-L246),
[types.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/kubelet/pkg/apis/stats/v1alpha1/types.go#L234-L256),
at v1.37.0, the version of kind's node image the management cluster runs).
The table, per container and summed, goes to the test log and the
artifacts; [Capacity](../../docs/operations.md#capacity) states it.

Then hydrophone v0.8.0, one of the two runners the CNCF names for
conformance
([instructions.md](https://github.com/cncf/k8s-conformance/blob/e40cba7bf280b59739c51e314e77b2fb28c6e1d9/instructions.md#L11-L13)),
runs the suite in the tenant, as `go run sigs.k8s.io/hydrophone@v0.8.0`: as a
`go tool` it would raise `k8s.io/utils` and `k8s.io/kube-openapi` in
`go.mod`. Its flags:

- `--conformance`, the focus `\[Conformance\]` with no skip
  ([root.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/cmd/root.go#L145-L149),
  [deploy.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/deploy.go#L96-L104)),
  what a certification run takes
  ([instructions.md](https://github.com/cncf/k8s-conformance/blob/e40cba7bf280b59739c51e314e77b2fb28c6e1d9/instructions.md#L32)):
  every conformance test runs, none is skipped, retried or focused away;
  `CONFORMANCE_FOCUS` replaces it with `--focus`;
- `--conformance-image registry.k8s.io/conformance:<the tenant's version>`;
- `--parallel 4`, four Ginkgo processes
  ([flags.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/types/flags.go#L32),
  [deploy.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/deploy.go#L125-L127)),
  with no skip added
  ([cmd.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/conformance/image/go-runner/cmd.go#L33-L40)):
  the specs marked `Serial` still run alone, on the first process once the
  others have finished
  ([ordering.go](https://github.com/onsi/ginkgo/blob/v2.32.0/internal/ordering.go#L174-L192),
  [suite.go](https://github.com/onsi/ginkgo/blob/v2.32.0/internal/suite.go#L505-L511),
  [ginkgowrapper.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/e2e/framework/ginkgowrapper.go#L396-L398);
  Ginkgo v2.32.0 is the version Kubernetes v1.37.0 builds the conformance
  image with). Kind's own parallel conformance jobs run 25 processes on 4
  CPUs and skip the serial specs instead
  ([kind-presubmits.yaml](https://github.com/kubernetes/test-infra/blob/9b2551f8f32815d93a471123109af4ed36ce827a/config/jobs/kubernetes-sigs/kind/kind-presubmits.yaml#L78-L120),
  [e2e-k8s.sh](https://github.com/kubernetes-sigs/kind/blob/v0.33.0/hack/ci/e2e-k8s.sh#L253-L261),
  [ginkgo-e2e.sh](https://github.com/kubernetes/kubernetes/blob/v1.37.0/hack/ginkgo-e2e.sh#L181-L193)).
  The suite waits far more than it computes: with one process it took 1h57m
  on a 16-CPU workstation, where the kind containers kept a third of a CPU
  busy; with four it took 44 minutes on GitHub's runner of 2 CPUs, and the
  nightly job 65;
- `--kubeconfig`, the admin kubeconfig with the forwarded address, so that
  hydrophone reaches the tenant as the suite does; the conformance pod in
  the tenant uses its in-cluster configuration;
- `--output-dir`, the test's artifact directory, where it downloads
  `e2e.log` and `junit_01.xml` from the pod by exec
  ([download.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/client/download.go#L38-L50));
- `--disable-progress-status`: the progress report execs into the pod every
  30 seconds and stops hydrophone when the log it reads lacks the spec count
  ([logs.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/client/logs.go#L218-L221),
  [stops](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/client/logs.go#L80-L81));
  the streamed log shows the progress.

Hydrophone exits with the conformance container's exit code
([root.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/cmd/root.go#L185-L199)),
which the image's runner sets to 1 when Ginkgo fails
([main.go](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/conformance/image/go-runner/main.go#L43-L45),
[Wait](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/conformance/image/go-runner/main.go#L84-L87)),
and with 1 on an error of its own
([main.go](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/main.go#L27-L29));
the test passes only on 0. `make conformance` gives the test 110 minutes,
and the nightly job has 120.
