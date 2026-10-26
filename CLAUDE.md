# CLAUDE.md

Lodia is an open-source operator that runs Kubernetes control planes as pods
on a management cluster, one `ControlPlane` object per tenant cluster, with
worker nodes anywhere joining through `kubeadm`. The fact that frames
everything: **controllers never call each other** — each one reads `spec`,
writes its own condition in `status`, and waits only on the conditions it
cannot work without.

## Where truth lives

- `docs/README.md` — what Lodia is and does: objects, controllers, the
  tenant namespace, networking, lifecycle, install, operations. It describes
  the design now and the code once it exists; a change to behaviour updates it
  in the same commit.
- `docs/decisions/` — indexed in `docs/README.md`. Read the relevant ones
  before changing an API, a controller boundary or a security property; a
  decision is changed in its file, never worked around in code.
- `docs/api/` — the field reference and examples; a CRD field changes there
  and in the Go types in the same commit.
- `docs/roadmap.md` — the current phase and its verification gate.
- The platform rules of Segla's handbook apply to how we work
  (https://gitlab.com/segla/handbook/-/blob/main/README.md): decisions in the
  format of `writing/decisions.md`, documentation layout from
  `writing/docs-layout.md`.

## Working from a task

A task's body is its work order (handbook `process/work-orders.md`). Read
**Authority** first; **Out of scope** is binding, findings outside it go to
`todo.md`; a task is not Done until **Landing** is paid: the docs above, in
the same change.

## Commands

- `make generate` — deepcopy functions and the CRDs in `api/crds/` from
  `api/`; commit the result.
- `make verify` — the one check whose exit code says a change is done:
  generated files current, both modules tidy (`.` and `api/`, which is
  `go.lodia.io/lodia/api`), golangci-lint (installed on its own), unit and
  envtest tests. CI runs it on every pull request.
- `make e2e` — the suite in `test/e2e` against `E2E_KUBECONFIG`, or against a
  kind cluster of its own that it deletes afterwards; it refuses any other
  cluster (`docs/decisions/e2e-on-a-named-cluster.md`).

## Rules

- English everywhere in the repository; diagrams are mermaid inside Markdown.
- A claim about Kubernetes, etcd or a neighbour project carries a link to a
  primary source (kubernetes.io, a KEP, the project's docs or code at a
  commit). No source, no claim.
- Other projects are described, never judged: `docs/analysis.md` states what
  each does and cites it; our reasons live in `docs/decisions/`.
- No admission webhooks: validation is CEL in the CRD schema
  (`docs/decisions/no-admission-webhooks.md`).
- Nothing in tests or tooling ever targets the current kube context; e2e
  takes a named kubeconfig or refuses to run.
- Commits follow conventional commits (`feat:`, `fix:`, `docs:`, `chore:`),
  signed, with no generated attribution trailers.
