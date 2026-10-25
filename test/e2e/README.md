# End-to-end tests

The suite runs only against a kind cluster whose kubeconfig is named on the
command line: `E2E_KUBECONFIG` points at a file with exactly one context, and
that context's name starts with `kind-brygga-e2e`. Anything else, the current
kube context included, stops it before the first request
([decision](../../docs/decisions/e2e-on-a-named-cluster.md)).

`make e2e` creates the kind cluster `brygga-e2e-local` (`E2E_CLUSTER` changes
the name), runs the suite against it and deletes it.

To keep a cluster between runs:

```sh
mkdir -p work/e2e
go tool kind create cluster --name brygga-e2e-dev --kubeconfig work/e2e/dev.kubeconfig
E2E_KUBECONFIG=work/e2e/dev.kubeconfig make e2e
E2E_KUBECONFIG=$PWD/work/e2e/dev.kubeconfig go test -tags e2e -count=1 ./test/e2e/... -run TestExamplesOnAKindCluster
go tool kind delete cluster --name brygga-e2e-dev --kubeconfig work/e2e/dev.kubeconfig
```

`--kubeconfig` makes kind write that file alone and leave `~/.kube/config`
untouched. `go test` runs in the package directory, so it needs an absolute
`E2E_KUBECONFIG`; `make e2e` resolves a relative one, and runs `go test -v`
so that each scenario's lines are in the log.

`TestDatastoreOnKind` needs `git` and `helm` on the `PATH`: it installs
etcd-druid v0.39.0 from its chart at the tag (the chart is not released) into
the namespace `etcd-druid` with
[`hack/etcd-spike/druid-values.yaml`](../../hack/etcd-spike/druid-values.yaml),
the way the [etcd spike](../../docs/etcd-spike-2026-10.md) ran it, and leaves
it installed. It runs the Datastore controller inside the test process
against the cluster, with the cache rules of `cmd/brygga`; the suite against
Brygga's own image and chart comes with the chart.
