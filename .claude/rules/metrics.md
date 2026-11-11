---
paths:
  - "internal/metrics/**"
  - "charts/lodia/files/rules/**"
  - "charts/lodia/files/dashboards/**"
  - "test/rules/**"
---

# Metrics

- A metric is API: its name, its labels, their values and its meaning change as a status field does ([decision](../../docs/decisions/metrics-are-part-of-the-api.md)).
- A label's value from a set is a constant in `api/v1alpha1`.
- The collector reads the manager's cache and the samples alone, never the API server.
- Every metric has a line in the golden, `internal/metrics/testdata/metrics.prom`.
- Every rule aggregates `pod` and `instance` away: a failover moves every series to another pod.
- Every dashboard query on a tenant's series aggregates `pod` and `instance` away with `max`, as a rule does, so that the two leaders' series at a failover are one; the operator dashboard, which shows the replicas, keeps `pod`.
- A dashboard passes `dashboard-linter lint --strict`; `charts/lodia/files/dashboards/.lint` excludes the job and instance rules for the fleet and control plane dashboards alone, by title.
- Every alert carries `keep_firing_for: 4m`, which outlasts the two lease handovers of a `helm upgrade`.
- Every alert has a section `### <alert>` in `docs/operations.md`, which its `runbook_url` names.
- Every alert has cases, written in the alert's function in `test/rules/cases_test.go`, and `go test ./test/rules/` fails on an alert with no case where it fires; `go test ./test/rules/ -update` rewrites `test/rules/lodia_test.yaml`, which is never edited by hand.
- A case outside a failover lists the series its alert reads; `complete` in `test/rules/series_test.go` adds the condition types of each tenant the case leaves out.
- The cases of an alert that joins on `namespace` and `controlplane` include a tenant of the same name in a second namespace (`batch.in`) that carries what the join reads, so that a join on `controlplane` alone fails.
