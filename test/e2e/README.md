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
  one dial. `exec` and the port-forward use client-go's SPDY transport;
  kubectl's default, WebSocket, reaches the kubelet through the same
  transport and tunnel
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
each one before it starts it; `make e2e` removes them as well, since a
`go test` timeout or a panic never reaches the suite's end.

`make e2e` gives the suite 30 minutes.
