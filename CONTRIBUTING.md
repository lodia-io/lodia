# Contributing to Lodia

Lodia is designed in the open.
[`docs/README.md`](docs/README.md) says what it does and [`docs/decisions/`](docs/decisions/) why; a pull request that changes behaviour updates them, and one that goes against a decision changes the decision's file first.
Every pull request is reviewed against the [review checklist](docs/review-checklist.md).
Taking part means following the [code of conduct](CODE_OF_CONDUCT.md).

## Build and test

You need Go (the version in `go.mod`), Docker for the end-to-end suite, [golangci-lint](https://golangci-lint.run/) v2.13.2 and [Helm](https://helm.sh/) v4.3.1.
Every other tool runs through `go tool`.

| command | does |
|---|---|
| `make generate` | deepcopy functions and CRDs from `api/`, their copies in the chart, and each controller's ClusterRole from its RBAC markers; commit the result |
| `make verify` | the check that says a change is done: generated files current, the chart linted and rendered, lint, unit and envtest tests |
| `make e2e` | the end-to-end suite on a kind cluster of its own ([how](test/e2e/README.md)) |
| `make conformance` | the Kubernetes conformance suite against a tenant with two nodes, on a kind cluster of its own ([how](test/e2e/README.md#nightly-conformance-and-the-idle-control-plane)) |

CI runs `make verify` and `make e2e` on every pull request, and `make conformance` every night.

## Docs

- Each fact once, in its home, and a link to it elsewhere: the fields, conditions and reasons of `ControlPlane` and `Backup` in [`docs/api/controlplane.md`](docs/api/controlplane.md), runbooks in [`docs/operations.md`](docs/operations.md).
  No history, and no prose that restates the code.
- Every claim about Kubernetes or a neighbour carries a source.
- Decisions are in the handbook format; a behaviour change updates [`docs/README.md`](docs/README.md) and the decision in the same commit.
- Markdown uses one sentence per line.

## Commits

- [Conventional commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`, `chore:`.
- Every commit is signed off: its `Signed-off-by` line certifies the [Developer Certificate of Origin](https://developercertificate.org/).
  `git commit -s` adds the line; the DCO check on a pull request refuses a commit without one.
