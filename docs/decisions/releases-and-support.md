# Lodia ships signed multi-arch images built with ko and a Helm chart in GHCR, and supports the Kubernetes minors upstream maintains

Decided 2026-10-25; the notes, the tags and the nightlies of a release, 2026-11-22.
The chart, the release workflow that builds, signs, attests and publishes the images and the chart, Renovate's configuration and kube-api-linter on the API are built; the Scorecard is phase 4's.

## Decision

**Build and publish**

- The three images, `ghcr.io/lodia-io/lodia`, `ghcr.io/lodia-io/lodia-signer` and `ghcr.io/lodia-io/lodia-tunnel`, are built with `ko` for `linux/amd64` and `linux/arm64` on `gcr.io/distroless/static:nonroot`, pinned by digest; `ko` attaches an SPDX SBOM to every image ([ko SBOMs](https://ko.build/features/sboms/), [multi-platform](https://ko.build/features/multi-platform/)).
  The release workflow sets `VERSION`, the operator's build version, to the release tag ([configuration.md](https://github.com/google/ko/blob/v0.19.1/docs/configuration.md#L80-L89)).
- Images and the chart are signed with cosign keyless signing from the release workflow's GitHub OIDC identity, and carry build provenance through GitHub artifact attestations ([cosign](https://docs.sigstore.dev/cosign/signing/overview/), [artifact attestations](https://docs.github.com/en/actions/concepts/security/artifact-attestations)).
- The Helm chart is published to `oci://ghcr.io/lodia-io/charts/lodia` next to the images, and references images by digest.
  A release tags the repository `v<version>` and packages the chart at `<version>`, its `appVersion` too, while `Chart.yaml` keeps `0.0.0-dev` on `main`: the PrometheusRule links each runbook at the tag `v<appVersion>`, or at `main` for an `appVersion` that ends in `-dev` ([Alerting on the operator](../README.md#alerting-on-the-operator)).
  The packaged chart is reproducible: Helm stamps the archive's modification time into the manifest as its creation time ([ocipusher.go](https://github.com/helm/helm/blob/v4.3.1/pkg/pusher/ocipusher.go#L92-L94)), so the packaging sets the modification time of every file and of the archive to the commit's, and a re-run on the same tag pushes the digest the first run pushed.
  The CRDs are templates in the chart, installed and upgraded with it and kept on uninstall ([values](../README.md#installation)), the pattern cert-manager uses because Helm's `crds/` directory is never upgraded ([Helm CRD best practices](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/), [cert-manager chart](https://cert-manager.io/docs/installation/helm/)).
- `release.yml` runs on a pushed `v*` tag: it pushes the images and the chart, pulls all four with no credentials, signs and attests them, verifies every signature and attestation as a user does, and only then creates the GitHub release.
  A release therefore never exists for artifacts that an anonymous user cannot pull or that a user's own command refuses.
  A package that a workflow creates with `GITHUB_TOKEN` takes the visibility of its repository ([workflows](https://github.com/github/docs/blob/d7487f5debb5a5a2a6e65e6f9fa539266f5f8db6/content/packages/managing-github-packages-using-github-actions-workflows/publishing-and-installing-a-package-with-github-actions.md?plain=1#L60-L62)), so `lodia`, `lodia-signer`, `lodia-tunnel` and `charts/lodia` are public from their first push; a run that finds any of them private stops at the anonymous pull ([CONTRIBUTING](../../CONTRIBUTING.md#releasing)).
- The release's notes are the chart's reference and the four digests, then GitHub's generated list of the merged pull requests ([release notes](https://github.com/github/docs/blob/6d56af0c8b62bde11bb3d9df46244fab5f0a00f9/content/repositories/releasing-projects-on-github/automatically-generated-release-notes.md?plain=1#L19)); a version with `-` is a pre-release.
  Each item of the list is a squash-merged pull request's title, a conventional commit, and the list shows no commit footer, so a breaking change is a title with `!`, as `feat!:`.
- Renovate keeps current the Go modules of `.` and `test/monitoring/`, GitHub Actions by digest, the base image of `.ko.yaml` by digest, etcd-druid, and the version table's kube-apiserver `latest` and konnectivity ([Renovate docker](https://docs.renovatebot.com/docker/)).
  It opens its pull requests once a week, early on Monday (UTC), with every minor, patch and digest update in one, so that CI runs once for them; the Go toolchain, a minor of the Kubernetes modules, etcd-druid and each major come in pull requests of their own, and a fix for a GitHub security alert comes at any time ([schedule.preset.ts](https://github.com/renovatebot/renovate/blob/44.257.9/lib/config/presets/internal/schedule.preset.ts#L4), [packageRules](https://github.com/renovatebot/renovate/blob/44.257.9/docs/usage/configuration-options.md?plain=1#L3270), [vulnerabilityAlerts](https://github.com/renovatebot/renovate/blob/44.257.9/lib/config/options/index.ts#L2766-L2773)).
  etcd-druid's chart dependency, its image tag and its Go module move in one pull request, never apart.
  A bump of that chart does not upgrade etcd-druid's CRDs, since Helm applies a chart's `crds/` only when it installs a release ([charts.mdx](https://github.com/helm/helm-www/blob/d869ccd3db838b63cee08bffb8400e1a9221c3ee/docs/topics/charts.mdx?plain=1#L1052-L1056), [Installation](../README.md#installation)); the step that carries them waits for the first pull request that moves etcd-druid past v0.39.2, which Renovate can merge before any release ([todo](../../todo.md#releases-and-tooling)).
  A row of the table stays in its minor, and CoreDNS, which is kubeadm's pin for the minor, moves by hand ([versions.go](../../internal/versions/versions.go)).
  Renovate does nothing until its GitHub App is installed on lodia-io for this repository ([installing](https://github.com/renovatebot/renovate/blob/44.257.9/docs/usage/getting-started/installing-onboarding.md?plain=1#L26-L40)).

**Versions and support**

- Lodia follows semver, independent of the API version.
  Until 1.0, the latest minor gets fixes; from 1.0, the two latest minors.
- The maintainer cuts a release with two signed annotated tags at one commit, pushed together: `v<version>`, and the API module's `api/v<version>`, the tag form Go reads for a module in a subdirectory ([version tags](https://go.dev/ref/mod#vcs-version)).
  `v*` matches only a tag that starts with `v`, which `api/v…` does not ([filter patterns](https://github.com/github/docs/blob/6d56af0c8b62bde11bb3d9df46244fab5f0a00f9/content/actions/reference/workflows-and-actions/workflow-syntax.md?plain=1#L1686)), so only the first one runs `release.yml`.
  The tag ruleset `release-tags` blocks moving and deleting `v*` and `api/v*`, so that the identity a signature carries, `release.yml@refs/tags/<tag>`, names one commit for good.
- `make release-check TAG=v<version>` refuses a tag before it is pushed, and the workflow's first step refuses it again, each naming every rule the tag breaks:
  - the tag is `vMAJOR.MINOR.PATCH[-PRERELEASE]`, with no build metadata, which an image tag cannot carry, and above every published release of its major and minor;
  - its commit is on `origin/main`, and every run of `ci.yml` for the push of the commit concluded `success`;
  - every `.github/workflows/nightly*.yml` at the commit has its newest completed scheduled or dispatched run on `main` at that commit concluded `success`: a run in progress on its first attempt is not counted, a re-run in progress counts as its previous attempt, and a dispatched run counts only at the defaults of its inputs: a nightly whose `workflow_dispatch` has inputs states each in its `run-name` as `${{ inputs.<name> || '<default>' }}`, as `nightly-fleet` does for its size, and the check refuses one that does not and one with an input of `type: boolean` or `type: number`, whose `false` and `0` the `||` turns into the default ([typed inputs](https://github.com/github/docs/blob/6d56af0c8b62bde11bb3d9df46244fab5f0a00f9/content/actions/reference/workflows-and-actions/contexts.md?plain=1#L839), [falsy values](https://github.com/github/docs/blob/6d56af0c8b62bde11bb3d9df46244fab5f0a00f9/content/actions/reference/workflows-and-actions/expressions.md?plain=1#L30)), while an empty string input must run as its default, as the fleet's `${TENANTS:-300}` does ([e2e-on-a-named-cluster](e2e-on-a-named-cluster.md));
  - `v<version>` and `api/v<version>` are tags at the commit;
  - `Chart.yaml` at the commit has version and `appVersion` `0.0.0-dev`;
  - `README.md` and `docs/README.md` at the commit name `--version <version>` in their install commands, and `docs/operations.md` in its upgrade command.
- A release ships a version table: for each Kubernetes minor it supports, the images of kube-apiserver, kube-controller-manager, kube-scheduler, kube-proxy, CoreDNS, konnectivity and etcd, the newest patch, `latest`, which automatic patch updates move to, and the end-of-support date, ahead of which tenants are warned.
  It supports the minors the Kubernetes project maintains, three at any time, about 14 months each ([releases](https://kubernetes.io/releases/)): today 1.35, 1.36 and 1.37, with 1.34 leaving upstream support on 2026-10-27.
  A minor that leaves the table stays renderable for one more release, only as the source of an upgrade ([upgrades-and-maintenance-windows](upgrades-and-maintenance-windows.md)).
- The support matrix in the docs has two columns, *supported* and *tested*, as cert-manager's does: a minor is *tested* when the e2e suite and the nightly conformance run pass on it with kind images ([cert-manager supported releases](https://cert-manager.io/docs/releases/)).
- The management cluster runs any maintained minor; the CRDs use CEL libraries up to semver (1.34), validation ratcheting and `optionalOldSelf` (GA in 1.33), so 1.35 is the floor today ([CEL in Kubernetes](https://kubernetes.io/docs/reference/using-api/cel/)).

**Engineering baseline**

- Two Go modules.
  The operator: kubebuilder v4 layout, controller-runtime v0.25 and `k8s.io/*` v0.37 on Go 1.26 at the start ([controller-runtime releases](https://github.com/kubernetes-sigs/controller-runtime/releases), [kubebuilder v4.16.0](https://github.com/kubernetes-sigs/kubebuilder/releases/tag/v4.16.0)).
  The API, `go.lodia.io/lodia/api`: it requires `k8s.io/api` and `k8s.io/apimachinery` of the oldest Kubernetes minor Lodia supports, v0.35.0 today, and `make test` builds and tests it there.
  A module that imports the types keeps its own `client-go`: Go builds with the highest version any module requires ([minimal version selection](https://go.dev/ref/mod#minimal-version-selection)), and `k8s.io/api` of a newer minor breaks an older `client-go`, as v0.37 dropped `scheduling/v1alpha2`, which client-go v0.36's scheme imports ([register.go](https://github.com/kubernetes/client-go/blob/v0.36.3/kubernetes/scheme/register.go#L69)).
  The floor moves when its minor leaves support, never with Renovate.
- golangci-lint v2 with Segla's shared configuration plus kube-api-linter for the API types ([kube-api-linter](https://github.com/kubernetes-sigs/kube-api-linter)).
- The community files of the CNCF project template (CONTRIBUTING, CODE_OF_CONDUCT, SECURITY, MAINTAINERS, GOVERNANCE, ADOPTERS), DCO sign-off checked on every pull request by the DCO app, OpenSSF Scorecard published from CI ([project template](https://github.com/cncf/project-template), [DCO app](https://github.com/dcoapp/app), [Scorecard](https://github.com/ossf/scorecard)).

## Why

- `ko` builds a Go binary into a minimal image without a Dockerfile, for several architectures at once, and emits an SBOM by default; the neighbour projects that ship Go operators use it.
- Keyless signing removes the signing key as something to protect and rotate; artifact attestations are GitHub's maintained path for provenance, while the older `slsa-github-generator` is no longer actively maintained ([its README](https://github.com/slsa-framework/slsa-github-generator/blob/main/internal/builders/container/README.md)).
- A management cluster that upgrades Lodia must upgrade its CRDs in the same step, or new fields are silently dropped; templated CRDs do that, and `keep` protects every `ControlPlane` from an accidental uninstall.
- Following upstream's support window keeps tenants on versions that still get security fixes, and keeps Lodia's test matrix at three minors.
- The check runs before the push because a pushed tag stays: the ruleset blocks moving and deleting it, so a refused tag never reaches the remote.

## Rejected

- Dockerfile builds with `docker buildx` — slower, and an SBOM is extra work; only needed if an image ever carries non-Go content.
- `chainguard/static` as the base — free only as `latest`, so it cannot be pinned to a released tag without a subscription.
- A separate CRD chart — one more install step for every user; the `crds.enabled` switch covers the GitOps users who manage CRDs themselves.
- Supporting more minors than upstream — every extra minor is a column of e2e and conformance for versions without security fixes.

## Consequences

- The release workflow runs on GitHub Actions with `id-token: write`, builds, signs, attests and pushes; a release is cut only from a commit at which every nightly passed ([e2e-on-a-named-cluster](e2e-on-a-named-cluster.md)).
- Users verify the images and the chart with `cosign verify` and `gh attestation verify`, each pinned to `release.yml` at the release's tag ([Verifying a release](../README.md#verifying-a-release)).
- Adding a Kubernetes minor is a Lodia release: a new row in the version table, kind images in the matrix, the conformance run green.
- Each update of the version table reviews every `@todo` whose undo condition names an upstream release, starting with the API server's `preStop` sleep ([kubernetes#142980](https://github.com/kubernetes/kubernetes/issues/142980), [#143346](https://github.com/kubernetes/kubernetes/pull/143346)) and the etcd-backup-restore `v1/` prefix rule (`todo.md`).

## Code

- `charts/lodia` — the chart ([values](../README.md#installation)): the CRDs as templates of the ones in `api/crds`, which `make generate` copies into `files/crds`, and the admission policies of `api/policies`, which it copies into `files/policies`.
  `make verify` lints and renders it with tags and with digests.
- `.ko.yaml` — the base image, pinned by digest.
- `.github/workflows/release.yml` — the release, through the `release-*` targets of the `Makefile`; `hack/releasecheck/release_test.go` holds its trigger, order, permissions and pins.
- `hack/releasecheck` — the tag check, run as `make release-check` and as the workflow's first step.
- `.github/renovate.json5` — Renovate's configuration; the `verify` job of `ci.yml` validates it.
- `.kube-api-linter.yml` — kube-api-linter's configuration, which `make kube-api-linter` runs on `api/` and `make verify` runs; its `enable` list names the rules it adds to the defaults, its `disable` list the rules it leaves off with their reasons, and its exclusions the findings it accepts with theirs.
