---
paths:
  - "internal/metrics/**"
  - "charts/lodia/files/rules/**"
  - "charts/lodia/files/dashboards/**"
---

# Metrics

- A metric is API: its name, its labels, their values and its meaning change as a status field does ([decision](../../docs/decisions/metrics-are-part-of-the-api.md)).
- A label's value from a set is a constant in `api/v1alpha1`.
- The collector reads the manager's cache and the samples alone, never the API server.
- Every metric has a line in the golden, `internal/metrics/testdata/metrics.prom`.
- Every rule aggregates `pod` and `instance` away: a failover moves every series to another pod.
