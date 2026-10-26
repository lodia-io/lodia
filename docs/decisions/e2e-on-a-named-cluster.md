# End-to-end tests run only against a cluster named on the command line, and never the current kube context

Decided 2026-10-25. The refusal and `make e2e` are built;
kubeadm workers, upgrade tests and conformance come with phase 1.

## Decision

- `make e2e` runs against `E2E_KUBECONFIG=<path>`, or creates its own kind
  cluster (`lodia-e2e-local`, or the name in `E2E_CLUSTER`) and deletes it
  afterwards. The suite refuses to start when the variable is unset, when the
  kubeconfig has more than one context, or when the context name does not
  start with `kind-lodia-e2e`.
- Unit tests use fakes; controller tests use envtest (a real API server and
  etcd, no kubelet, so no garbage collection) for every controller and every
  CEL rule; e2e uses kind with real `kubeadm join` workers, written with
  `sigs.k8s.io/e2e-framework`, whose configuration takes an explicit
  kubeconfig and context ([e2e-framework](https://github.com/kubernetes-sigs/e2e-framework)).
  The guard is ours: neither the kubebuilder scaffold nor the framework
  checks that the context is a kind cluster.
- Upgrades are tested by bumping `spec.version` across kind node images
  (1.35 → 1.36 → 1.37) with workers one minor behind, inside the skew
  policy; kind itself does not upgrade a cluster
  ([version skew policy](https://kubernetes.io/releases/version-skew-policy/)).
- The Kubernetes conformance suite runs through hydrophone against a tenant
  cluster with two joined workers: a focused subset in parallel on every pull
  request, the full `--conformance` run nightly; a release is cut only from a
  commit whose nightly conformance passed
  ([hydrophone flags](https://github.com/kubernetes-sigs/hydrophone/blob/main/docs/flags-cli-ref.md),
  [conformance tests](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/conformance-tests.md)).
- `go test ./...` from the repository root runs unit and envtest tests only;
  e2e lives under `test/e2e` behind a build tag.

## Why

- A test suite that reads the current kube context will one day run against
  production: Segla hit exactly this on 2026-10-20, when an upstream
  project's e2e suite (`UseExistingCluster`) ran against the production
  management cluster because it was the current context. The rule is a
  check, not advice.
- A hosted control plane is only correct if a real kubelet joins it, a pod
  runs, and `kubectl logs` and `exec` reach the node through the tunnel; no
  fake covers that. Conformance is the ecosystem's own definition of "a
  working Kubernetes".

## Rejected

- `UseExistingCluster` with a warning — a warning is advice.
- Mocking the tenant API server in e2e — misses kubeadm, konnectivity, CoreDNS
  and kube-proxy, which is where hosted control planes break.

## Consequences

- CI needs Docker (kind) and about 20 minutes for the e2e gate; conformance
  is nightly, not per change.
- The e2e harness is a product of its own: `test/e2e/README.md` documents how
  to run one scenario locally against a named kind cluster.

## Code

- `test/e2e/guard.go` — `RequireNamedCluster`, the refusal;
  `TestRequireNamedCluster` in `guard_test.go` holds it.
- `test/e2e/suite_test.go` — the suite, behind the `e2e` build tag; before
  any test it builds both images with ko into the kind cluster and installs
  the chart with them.
- `test/e2e/operator_test.go` — the operator from the chart: Available, a
  running replica holding the leader lease, its pod admitted where Pod
  Security `restricted` is enforced.
- `Makefile` — the `e2e` target.
