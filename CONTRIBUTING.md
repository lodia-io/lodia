# Contributing to Brygga

Brygga is designed in the open. [`docs/README.md`](docs/README.md) says what
it does and [`docs/decisions/`](docs/decisions/) why; a pull request that
changes behaviour updates them, and one that goes against a decision changes
the decision's file first. Taking part means following the
[code of conduct](CODE_OF_CONDUCT.md).

## Build and test

You need Go (the version in `go.mod`), Docker for the end-to-end suite, and
[golangci-lint](https://golangci-lint.run/) v2.13.2. Every other tool runs
through `go tool`.

| command | does |
|---|---|
| `make generate` | deepcopy functions and CRDs from `api/`; commit the result |
| `make verify` | the check that says a change is done: generated files current, lint, unit and envtest tests |
| `make e2e` | the end-to-end suite on a kind cluster of its own ([how](test/e2e/README.md)) |

CI runs `make verify` and `make e2e` on every pull request.

## Commits

- [Conventional commits](https://www.conventionalcommits.org/): `feat:`,
  `fix:`, `docs:`, `chore:`.
- Every commit is signed off: its `Signed-off-by` line certifies the
  [Developer Certificate of Origin](https://developercertificate.org/).
  `git commit -s` adds the line; the DCO check on a pull request refuses a
  commit without one.
