# Contributing to Lodia

Lodia is designed in the open.
[`docs/README.md`](docs/README.md) says what it does and [`docs/decisions/`](docs/decisions/) why; a pull request that changes behaviour updates them in the same commit, and one that goes against a decision changes the decision's file first.
Every pull request is reviewed against the [review checklist](docs/review-checklist.md).
Taking part means following the [code of conduct](CODE_OF_CONDUCT.md).

## Build and test

You need Go (the version in `go.mod`), Docker for the end-to-end suite, [golangci-lint](https://golangci-lint.run/) v2.13.2, [Helm](https://helm.sh/) v4.3.1, promtool from [Prometheus v3.15.1](https://github.com/prometheus/prometheus/releases/tag/v3.15.1) and dashboard-linter [v0.4.0](https://github.com/grafana/dashboard-linter/releases/tag/v0.4.0).
Every other tool runs through `go tool`, except ko, hydrophone, yq and crane, which run through `go run` at a version of their own (`Makefile`).

| command | does |
|---|---|
| `make generate` | deepcopy functions and CRDs from `api/`, their copies in the chart, and each controller's ClusterRole from its RBAC markers; commit the result |
| `make verify` | the check that says a change is done: generated files current, the chart linted and rendered, the release's images built for two platforms with no push and its chart packaged with their digests and rendered, lint, the metrics' golden and the alert rules checked by promtool, the rules' promtool tests, the dashboards linted by dashboard-linter, the metric names they and the rules read checked against what Lodia serves, unit and envtest tests, and the etcd keys a restore's cleanup deletes against each minor Lodia runs (`make restore-keys`) |
| `make e2e` | the end-to-end suite on a kind cluster of its own ([`test/e2e/README.md`](test/e2e/README.md)) |
| `make conformance` | the Kubernetes conformance suite against a tenant with two nodes, on a kind cluster of its own ([`test/e2e/README.md`](test/e2e/README.md#nightly-conformance-and-the-idle-control-plane)) |

The tests of Lodia's calls to etcd run etcd's release binary at `versions.EtcdVersion`, the etcd that etcd-wrapper runs in production, and no embedded etcd.
`make test` and `make fleet` fetch it into `work/etcd` first (`make etcd`); a test run by hand needs `LODIA_ETCD_BINARY="$(go run ./test/etcdbinary work/etcd)"`.
A test binary that `go test`'s timeout or a SIGKILL ended skips its cleanup: on Linux the kernel ends its etcd with it.
On macOS the etcd stays on its port and its `etcd-*` directory stays in `$TMPDIR`: `ps -eo pid,ppid,command | grep -- '--data-dir.*/etcd-'` lists them, a leftover's parent is process 1 and its directory is `lsof -a -p <pid> -d cwd`, `kill` it and remove the directory.

CI runs `make verify`, `make e2e`, `make e2e-ha`, `make e2e-restore`, `make e2e-gateway`, `make e2e-upgrade` and `make e2e-maintenance` on every pull request, and every night `make conformance`, `make e2e-gateway-g6`, `make conformance-gateway`, `make e2e-hold`, `make e2e-upgrade-ha`, and `make e2e-ha`, `make e2e-upgrade` and `make e2e-maintenance` with their nightly tests.

`test/rules/lodia_test.yaml`, promtool's cases of the alert rules, is generated and never edited by hand.
To add or change a case, edit the alert's function in `test/rules/cases_test.go`, run `go test ./test/rules/ -update` and commit the file it rewrites.
`go test ./test/rules/` fails when the committed file differs from what the cases generate.

## Docs

- Each fact once, in its home, and a link to it elsewhere: the fields, conditions and reasons of `ControlPlane` and `Backup` in [`docs/api/controlplane.md`](docs/api/controlplane.md), runbooks in [`docs/operations.md`](docs/operations.md).
  No history, and no prose that restates the code.
- Every claim about Kubernetes or a neighbour carries a source.
- Markdown uses one sentence per line.

## Commits

- [Conventional commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `docs:`, `chore:`.
- Every commit is signed off: its `Signed-off-by` line certifies the [Developer Certificate of Origin](https://developercertificate.org/).
  `git commit -s` adds the line; the DCO check on a pull request refuses a commit without one.

## Releasing

A release is two signed annotated tags at one commit, `v<version>` and `api/v<version>`, which the maintainer cuts; `release.yml` runs on `v<version>` and publishes the images and the chart ([releases-and-support](docs/decisions/releases-and-support.md)).
`make release-check` reads GitHub with `GITHUB_TOKEN`, else with the signed-in GitHub CLI's.

1. Merge the pull request that names `--version <version>` in the install commands of `README.md` and `docs/README.md` and in the upgrade command of `docs/operations.md`.
   It is the last merge before the tag: every nightly must have passed at the commit that gets the tag, and a later merge moves `main` past it.
   Until the release is out, `main`'s docs name a version that does not exist yet.
2. Run `make release-check TAG=v<version> COMMIT=<commit>`, `<commit>` being that merge.
   It names every rule that is not met, each missing nightly among them with the command that dispatches it, `gh workflow run <file> --ref main`.
   Dispatch them and wait for them, about two hours.
   Once `main` has moved past the commit, a dispatch runs at the new head: re-run a red nightly of the commit with `gh run rerun <id>` instead.
3. Tag the commit: `git tag -s -m v<version> v<version> <commit>` and `git tag -s -m api/v<version> api/v<version> <commit>`.
4. Run `make release-check TAG=v<version> COMMIT=<commit>` until it passes, then `git push --atomic origin v<version> api/v<version>`.

The first release stops at the anonymous pull, with the four packages `lodia`, `lodia-signer`, `lodia-tunnel` and `charts/lodia` pushed and private.
Make each public in its package settings (Danger Zone, Change visibility), then re-run the job on the same tag.
The chart is reproducible, so the re-run pushes the chart digest the first run pushed ([releases-and-support](docs/decisions/releases-and-support.md)).
A run that finds any package private stops there again.

A run that fails:

- The tag check refused it on what GitHub says, a `ci.yml` run or a nightly that is not green: re-run the job once that is green, since the run pushed nothing.
  A refusal on the commit's content never reaches the workflow, because `make release-check` refuses it before the push.
- Any other failure: fix it on `main` and cut the next pre-release the same way, `v0.1.0-alpha.2` after `v0.1.0-alpha.1`.
  Its docs pull request also moves the version in the status line of `README.md` and in the first release's entry of `docs/roadmap.md`, which still name the failed tag and which nothing checks.
  The failed tags stay, because the tag ruleset `release-tags` blocks moving and deleting them.
  Delete the failed run's versions from the four packages by hand, with a classic token that has `read:packages` and `delete:packages` ([the endpoint](https://github.com/github/rest-api-description/blob/c9c95fe34ced05fe076001f5c55704742bc8e7f8/descriptions/api.github.com/api.github.com.2022-11-28.yaml#L41855-L41859)), so that no unsigned, unreleased chart stays installable.
  A package whose last tagged version GitHub refuses to delete (`You cannot delete the last tagged version of a package. You must delete the package instead.`) is deleted whole, which is every package after a failed first release.
  Its next push creates it private again, so the next release's run stops at the anonymous pull as the first did: make it public again before that run.
