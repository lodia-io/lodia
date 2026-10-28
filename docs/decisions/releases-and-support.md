# Lodia ships signed multi-arch images built with ko and a Helm chart in GHCR, and supports the Kubernetes minors upstream maintains

Decided 2026-10-25.
The chart is built; the release workflow that builds, signs, attests and publishes the images and the chart is not.

## Decision

**Build and publish**

- Images are built with `ko` for `linux/amd64` and `linux/arm64` on `gcr.io/distroless/static:nonroot`, pinned by digest; `ko` attaches an SPDX SBOM to every image ([ko SBOMs](https://ko.build/features/sboms/), [multi-platform](https://ko.build/features/multi-platform/)).
- Images and the chart are signed with cosign keyless signing from the release workflow's GitHub OIDC identity, and carry build provenance through GitHub artifact attestations ([cosign](https://docs.sigstore.dev/cosign/signing/overview/), [artifact attestations](https://docs.github.com/en/actions/concepts/security/artifact-attestations)).
- The Helm chart is published to `oci://ghcr.io/lodia-io/charts/lodia` next to the images, and references images by digest.
  The CRDs are templates in the chart, installed and upgraded with it and kept on uninstall ([values](../README.md#installation)), the pattern cert-manager uses because Helm's `crds/` directory is never upgraded ([Helm CRD best practices](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/), [cert-manager chart](https://cert-manager.io/docs/installation/helm/)).
- Release notes and the changelog come from conventional commits through release-please; `feat!` or `BREAKING CHANGE:` marks a breaking change ([release-please](https://github.com/googleapis/release-please)).
- Renovate keeps Go modules, GitHub Actions (by digest), base images (by digest), the etcd-druid chart and the version table's component images current ([Renovate docker](https://docs.renovatebot.com/docker/)).

**Versions and support**

- Lodia follows semver, independent of the API version.
  Until 1.0, the latest minor gets fixes; from 1.0, the two latest minors.
- A release tags the API module at the same commit and version, `api/v<version>`, the tag form Go reads for a module in a subdirectory ([version tags](https://go.dev/ref/mod#vcs-version)).
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

## Rejected

- Dockerfile builds with `docker buildx` — slower, and an SBOM is extra work; only needed if an image ever carries non-Go content.
- `chainguard/static` as the base — free only as `latest`, so it cannot be pinned to a released tag without a subscription.
- A separate CRD chart — one more install step for every user; the `crds.enabled` switch covers the GitOps users who manage CRDs themselves.
- Supporting more minors than upstream — every extra minor is a column of e2e and conformance for versions without security fixes.

## Consequences

- The release workflow runs on GitHub Actions with `id-token: write`, builds, signs, attests and pushes; a release is cut only from a commit whose nightly conformance passed ([e2e-on-a-named-cluster](e2e-on-a-named-cluster.md)).
- Users verify an image with `cosign verify --certificate-identity-regexp … --certificate-oidc-issuer https://token.actions.githubusercontent.com` and `gh attestation verify`.
- Adding a Kubernetes minor is a Lodia release: a new row in the version table, kind images in the matrix, the conformance run green.
- Each update of the version table reviews every `@todo` whose undo condition names an upstream release, starting with the API server's `preStop` sleep ([kubernetes#142980](https://github.com/kubernetes/kubernetes/issues/142980), [#143346](https://github.com/kubernetes/kubernetes/pull/143346)) and the etcd-backup-restore `v1/` prefix rule (`todo.md`).

## Code

- `charts/lodia` — the chart ([values](../README.md#installation)): the CRDs as templates of the ones in `api/crds`, which `make generate` copies into `files/crds`.
  `make verify` lints and renders it with tags and with digests.
- `.ko.yaml` — the base image, pinned by digest.
