---
paths:
  - ".github/workflows/**"
  - "test/e2e/**"
---

# Nightly tests

- A nightly test has a workflow of its own: a schedule, `workflow_dispatch`, and `pull_request` with `paths` naming only that workflow file and the test's file ([decision](../../docs/decisions/e2e-on-a-named-cluster.md)).
- Never add controller or library paths to a nightly workflow: `paths` decides for the whole workflow.
  Code that needs coverage before a merge gets a test that a `ci.yml` job runs: an envtest in `verify`, or an e2e gate as in `e2e` and `e2e-ha`.
