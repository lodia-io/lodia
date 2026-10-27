# Lodia's metrics are part of its API

Decided 2026-10-26. Not built: phase 2's wave 2, observability, builds it
([roadmap](../roadmap.md)); the metrics below are its contract.

## Decision

**A metric of Lodia's is a contract like a status field:** its name, its
label names and their values, its unit and its meaning. Names are
`lodia_<object>_<what>[_<unit>]`; every metric of a `ControlPlane` carries
the labels `namespace` and `controlplane`. A label takes an object's name or
a value of a set fixed in `api/v1alpha1`, never an error string or a value
read from a tenant. A metric is renamed only under a new name, and in
`v1alpha1` the old one stays for one minor, with a release note.

- **One collector** in the operator builds them at scrape time from its
  cache, in the leader replica only, as cert-manager builds its certificate
  metrics from its listers
  ([certificate_collector.go](https://github.com/cert-manager/cert-manager/blob/v1.21.3/internal/collectors/certificate_collector.go#L28-L112))
  and HyperShift its hosted clusters' from a list
  ([metrics.go](https://github.com/openshift/hypershift/blob/v0.1.86/hypershift-operator/controllers/hostedcluster/metrics/metrics.go#L237-L267)):
  about 60 series per `ControlPlane`, and no histogram per tenant.
- **controller-runtime's own metrics** stay as they are, labelled by
  `controller` alone
  ([metrics.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/internal/controller/metrics/metrics.go#L28-L109));
  Lodia commits only to that label's values: `pki`, `datastore`, `backup`,
  `workloads`, `tenant`, `health` and `lifecycle`.
- **A condition is a one-hot gauge.**
  `lodia_controlplane_condition{type, status}` is 1 for the condition's
  status and 0 for the other two, and an alert holds its duration on it.
  `lodia_controlplane_condition_reason{type, reason}` is for dashboards and
  alert annotations; an alert selects by a reason only to fire at once
  (`LodiaUpgradeFailed`) or to leave a case out (`LodiaBackupStale`).
- **etcd is sampled, not scraped.** The Datastore controller reads each
  dedicated etcd's `Status` once a minute and keeps it in memory: the
  database size, the quota and whether there is a leader. The etcd's
  NetworkPolicy admits the operator to the client port for it. Scraping the
  etcd and its backup sidecar stays the platform's choice until they serve
  metrics on a listener of their own: etcd-druid's etcd serves its metrics
  on the client port only, behind client certificates
  ([etcdconfig.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/configmap/etcdconfig.go#L42-L63),
  [etcdconfig.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/configmap/etcdconfig.go#L136-L146)),
  and Lodia turns on no etcd authentication, so a certificate that may
  scrape may read every key; the sidecar serves its metrics on 8080 beside
  the endpoints that take snapshots and start initialization
  ([httpAPI.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/server/httpAPI.go#L137-L144)).
- **Events.** Each controller records one event per transition of its
  condition, on `events.k8s.io/v1`, through
  `GetEventRecorder("lodia.io/<controller>")`
  ([recorder.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/recorder/recorder.go#L35-L50)):
  the new reason as `reason`, the condition type as `action`, the message as
  `note`, and `Warning` when the new status is False.
- **The chart** ships a metrics Service, a ServiceMonitor, a PrometheusRule
  with the alerts in [`operations.md`](../operations.md#alerts-shipped-in-the-chart)
  and the dashboards, each behind a value of `metrics.*`, all off by
  default, with no capability check: `helm template` knows only the built-in
  API versions and those passed with `--api-versions`
  ([install.go](https://github.com/helm/helm/blob/v4.3.1/pkg/action/install.go#L340-L348)),
  so a check would drop them silently. The rules and the dashboards read
  only the metrics below and controller-runtime's.

**The metrics**, each beside its source:

| metric | labels besides `namespace` and `controlplane` | value |
|---|---|---|
| `lodia_controlplane_info` | `uid`, `kubernetes_version`, `layout`, `datastore` (`dedicated`, `shared`), `endpoint` (`gateway`, `loadBalancer`, `nodePort`) | 1: `metadata.uid`, `status.kubernetes.version`, `spec.layout`, and the member of `spec.datastore` and of `spec.endpoint` that is set |
| `lodia_controlplane_condition` | `type`, `status` | 1 or 0, one-hot: `status.conditions` |
| `lodia_controlplane_condition_reason` | `type`, `reason` | 1: `status.conditions` |
| `lodia_controlplane_metadata_generation` | | `metadata.generation` |
| `lodia_controlplane_status_observed_generation` | | `status.observedGeneration` |
| `lodia_controlplane_nodes`, `lodia_controlplane_nodes_ready` | | `status.nodes.total`, `status.nodes.ready` |
| `lodia_controlplane_component_replicas`, `lodia_controlplane_component_replicas_ready` | `component` (`apiServer`, `controllerManager`, `scheduler`) | `status.components.<component>.replicas`, `.ready`: lost redundancy, which `APIServerReady` leaves True |
| `lodia_controlplane_datastore_members`, `lodia_controlplane_datastore_members_ready` | | `status.datastore.members.total`, `.ready`: lost redundancy, which `DatastoreReady` leaves True while there is quorum |
| `lodia_controlplane_certificates_expiration_timestamp_seconds` | | `status.credentials.notAfter` |
| `lodia_controlplane_kubernetes_end_of_support_timestamp_seconds` | | the end-of-support date the version table gives the minor of `status.kubernetes.version` ([upgrades-and-maintenance-windows](upgrades-and-maintenance-windows.md)) |
| `lodia_controlplane_datastore_db_size_bytes`, `lodia_controlplane_datastore_quota_bytes`, `lodia_controlplane_datastore_has_leader` | | the Datastore controller's sample of the etcd's `Status` |

## Why

- Platforms build their own rules and dashboards on these names, as on
  `status`; a rename breaks them silently, so it gets the care of an API
  change.
- A tenant label on controller-runtime's reconcile histogram, 40 buckets,
  would be 40 × 7 controllers × 200 tenants, 56,000 series; Lodia's gauges
  are about 60 series per tenant.
- Built at scrape time, the metrics cannot drift from `status`, and a
  restarted operator has them at its first scrape.

## Rejected

- A tenant label on the reconcile metrics — the cost above.
- kube-state-metrics' custom resource state — feature-frozen, to be
  deprecated
  ([customresourcestate-metrics.md](https://github.com/kubernetes/kube-state-metrics/blob/v2.20.0/docs/metrics/extend/customresourcestate-metrics.md#L3-L4)),
  it maps `unknown` to 0 as it maps `false`
  ([customresourcestate-metrics.md](https://github.com/kubernetes/kube-state-metrics/blob/v2.20.0/docs/metrics/extend/customresourcestate-metrics.md#L625)),
  and it is configured in the platform's kube-state-metrics, which Lodia's
  chart does not own.
- A ServiceMonitor on every tenant's etcd by default — the full-data
  certificate and the sidecar's endpoints above.
- `status.datastore.dbSizeBytes` and `status.datastore.lastBackup` — they
  change all the time, and every status write wakes six controllers; the
  size is a metric, and the backups' health is `BackupReady`.

## Consequences

- The alerts in [`operations.md`](../operations.md#alerts-shipped-in-the-chart)
  are written on these metrics, and `make verify` runs `promtool check
  rules`, `promtool test rules` and a lint of the dashboards.
- A golden test pins the collector's output.
- Each controller's ClusterRole gains `create` and `patch` on
  `events.k8s.io` Events.

## Code

none yet.
