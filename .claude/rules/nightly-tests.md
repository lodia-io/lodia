---
paths:
  - ".github/workflows/**"
  - "test/e2e/**"
---

# Nightly tests

- A nightly test has a workflow of its own: a schedule, `workflow_dispatch`, and `pull_request` with `paths` naming only that workflow file and the test's file ([decision](../../docs/decisions/e2e-on-a-named-cluster.md)).
- Never add controller or library paths to a nightly workflow: `paths` decides for the whole workflow.
  Code that needs coverage before a merge gets a job in `ci.yml`, as `e2e` and `e2e-ha` are.
