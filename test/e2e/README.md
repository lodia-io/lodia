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

`make e2e` gives the suite 30 minutes.
