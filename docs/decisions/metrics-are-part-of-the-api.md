# Lodia's metrics are part of its API

Decided 2026-10-26.
Built: the collector, served by the leader alone, with every metric below that the table marks served, and the chart's metrics Service and ServiceMonitor.
Not built yet: the other metrics, the events and the chart's rules and dashboards, which phase 2's wave 2, observability, builds next ([roadmap](../roadmap.md)).

## Decision

**A metric of Lodia's is a contract like a status field:** its name, its label names and their values, its unit and its meaning.
Names are `lodia_<object>_<what>[_<unit>]`; every metric of a `ControlPlane` carries the labels `namespace` and `controlplane`.
A label takes an object's name or a value of a set fixed in `api/v1alpha1`, never an error string or a value read from a tenant.
A metric is renamed only under a new name, and in `v1alpha1` the old one stays for one minor, with a release note.

- **One collector** in the operator builds them at scrape time from its cache, as cert-manager builds its certificate metrics from its listers ([certificate_collector.go](https://github.com/cert-manager/cert-manager/blob/v1.21.3/internal/collectors/certificate_collector.go#L28-L112)) and HyperShift its hosted clusters' from a list ([metrics.go](https://github.com/openshift/hypershift/blob/v0.1.86/hypershift-operator/controllers/hostedcluster/metrics/metrics.go#L237-L267)), with no histogram per tenant.
- **The leader alone serves them.**
  An informer of the cache starts with the first source that watches its kind, and a controller holds its sources until it starts ([controller.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/internal/controller/controller.go#L242-L247)), in the leader alone ([controller.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/internal/controller/controller.go#L253-L259)); a list in the standby would start a second watch of each kind ([informer_cache.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/cache/informer_cache.go#L84-L99)), and both replicas would serve the same series.
  So the collector emits nothing until the manager is elected ([manager.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/manager/manager.go#L64-L67)), then lists under a 5-second deadline, below Prometheus' default scrape timeout of 10 seconds ([configuration.md](https://github.com/prometheus/prometheus/blob/v3.15.1/docs/configuration/configuration.md#L61)), since an informer that has not synced holds a list for as long as its context allows ([informers.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/cache/internal/informers.go#L327-L332)), and it emits nothing on a list that fails or runs out.
  Health's histogram and counter go through it too, so the standby serves controller-runtime's metrics alone.
- **controller-runtime's own metrics** stay as they are, labelled by `controller` alone ([metrics.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/internal/controller/metrics/metrics.go#L28-L109)); Lodia commits only to that label's values: `pki`, `kubeconfigrequest`, `datastore`, `backup`, `datastore-sample`, `lifecycle`, `tenant`, `health`, `health-nodes` and `workloads`.
- **A condition is a one-hot gauge.**
  `lodia_controlplane_condition{type, status}` is 1 for the condition's status and 0 for the other two, `Unknown` among them, and an alert holds its duration on it; a type absent from status has no series.
  `lodia_controlplane_condition_reason{type, reason}` is for dashboards and alert annotations; an alert selects by a reason only to fire at once (`LodiaUpgradeFailed`) or to leave a case out (`LodiaBackupStale`).
- **etcd is sampled, not scraped.**
  A controller of the Datastore's, `datastore-sample`, reads each formed dedicated etcd's `Status` once a minute and keeps it in memory: the database size, the leader and the alarms.
  A pass starts a minute after the last one started and has 5 seconds, and its sample is served from its start plus those 5 seconds until the next one is, so each sample stands a minute, and an alert that holds for a minute fires on two samples and never on one.
  A pass whose reads of the client certificate fail, or leave less than a second of its 5 for the etcd's calls, keeps no sample: that failure is the management cluster's, not the etcd's.
  The quota is the `Etcd`'s `spec.etcd.quota`, which etcd-druid renders as the etcd's backend quota ([etcdconfig.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/configmap/etcdconfig.go#L129-L134)).
  Scraping the etcd and its backup sidecar stays the platform's choice until they serve metrics on a listener of their own: etcd-druid's etcd serves its metrics on the client port only, behind client certificates ([etcdconfig.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/configmap/etcdconfig.go#L42-L63), [etcdconfig.go](https://github.com/gardener/etcd-druid/blob/v0.39.0/internal/component/configmap/etcdconfig.go#L136-L146)), and Lodia turns on no etcd authentication, so a certificate that may scrape may read every key; the sidecar serves its metrics on 8080 beside the endpoints that take snapshots and start initialization ([httpAPI.go](https://github.com/gardener/etcd-backup-restore/blob/v0.45.1/pkg/server/httpAPI.go#L137-L144)).
- **Events.**
  Each controller records one event per transition of its condition, on `events.k8s.io/v1`, through `GetEventRecorder("lodia.io/<controller>")` ([recorder.go](https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.2/pkg/recorder/recorder.go#L35-L50)): the new reason as `reason`, the condition type as `action` and the message as `note`.
  Its type follows the reason and the condition it replaces, not the status, since an idle `Progressing` and every wait are False, and a tenant on its way up passes from one loss to the next: `WarningReasons` in `api/v1alpha1` lists the refusals, the failures and the losses; a failure is `Warning` whatever it replaces, a refusal or a loss only when it replaces True or a reason that is neither a first pass, a wait nor another of the list's, and every other reason is `Normal`.
- **The chart** ships a metrics Service, a ServiceMonitor, a PrometheusRule with the alerts in [`operations.md`](../operations.md#alerts-shipped-in-the-chart) and the dashboards, each behind a value of `metrics.*`, all off by default, with no capability check: `helm template` knows only the built-in API versions and those passed with `--api-versions` ([install.go](https://github.com/helm/helm/blob/v4.3.1/pkg/action/install.go#L340-L348)), so a check would drop them silently.
  The ServiceMonitor sets `honorLabels`: prometheus-operator labels every series with the Service's namespace as `namespace` and the port's name as `endpoint` ([promcfg.go](https://github.com/prometheus-operator/prometheus-operator/blob/v0.94.2/pkg/prometheus/promcfg.go#L2156-L2159), [promcfg.go](https://github.com/prometheus-operator/prometheus-operator/blob/v0.94.2/pkg/prometheus/promcfg.go#L2214-L2228)), and Prometheus renames a scraped label that collides to `exported_<name>` ([configuration.md](https://github.com/prometheus/prometheus/blob/v3.15.1/docs/configuration/configuration.md#L349-L369)); so the endpoint's mode is the label `endpoint_mode`.
  The rules and the dashboards read only the metrics below and controller-runtime's.

**The metrics**, each beside its source; a field that is absent gives no series, but where the row says otherwise:

| metric | labels besides `namespace` and `controlplane` | value | served |
|---|---|---|---|
| `lodia_controlplane_info` | `uid`, `kubernetes_version`, `layout`, `datastore` (`dedicated`, `shared`), `endpoint_mode` (`gateway`, `loadBalancer`, `nodePort`) | 1: `metadata.uid`, `status.kubernetes.version`, empty while it is absent, `spec.layout`, and the member of `spec.datastore` and of `spec.endpoint` that is set | yes |
| `lodia_controlplane_condition` | `type`, `status` | 1 or 0, one-hot: each entry of `status.conditions` | yes |
| `lodia_controlplane_condition_reason` | `type`, `reason` | 1: each entry's reason | yes |
| `lodia_controlplane_condition_last_transition_timestamp_seconds` | `type` | each entry's `lastTransitionTime` | yes |
| `lodia_controlplane_metadata_generation`, `lodia_controlplane_status_observed_generation` | | `metadata.generation`, `status.observedGeneration` | yes |
| `lodia_controlplane_nodes`, `lodia_controlplane_nodes_ready`, `lodia_controlplane_nodes_outdated_kubelets` | | `status.nodes.total`, `.ready`, `.outdatedKubelets` | yes |
| `lodia_controlplane_component_replicas`, `lodia_controlplane_component_replicas_ready` | `component` (`apiServer`, `controllerManager`, `scheduler`) | `status.components.<component>.total`, `.ready`: lost redundancy, which `APIServerReady` leaves True | yes |
| `lodia_controlplane_datastore_members`, `lodia_controlplane_datastore_members_ready`, `lodia_controlplane_datastore_members_updated` | | `status.datastore.members.total`, `.ready`, `.updated`: lost redundancy, which `DatastoreReady` leaves True while there is quorum, and a roll that stops | yes |
| `lodia_controlplane_operator_revision_granted` | | 1 when `status.target.operatorRevision` is the running operator's revision, else 0: how far a release has reached | yes |
| `lodia_controlplane_maintenance_slot_since_timestamp_seconds` | | `status.maintenance.slotSince`: the tenant's slot of the fleet | yes |
| `lodia_controlplane_maintenance_scheduled_for_timestamp_seconds` | | `status.target.scheduledFor`: at or before now, the tenant is queued for a slot; later, it waits for its window | yes |
| `lodia_controlplane_upgrade_started_timestamp_seconds` | `step` (`UpgradeStep`) | `status.kubernetes.upgrade.startedAt` and `.step` | yes |
| `lodia_controlplane_certificates_expiration_timestamp_seconds` | | `status.credentials.notAfter` | yes |
| `lodia_controlplane_datastore_quota_bytes` | | the `Etcd`'s `spec.etcd.quota` | yes |
| `lodia_controlplane_datastore_delta_snapshot_period_seconds` | | the `Etcd`'s `spec.backup.deltaSnapshotPeriod`, absent below 1 second or without a store | yes |
| `lodia_controlplane_datastore_db_size_bytes` | | the largest `dbSize` among the members that answered the last sample; absent when none did and wherever `lodia_controlplane_datastore_has_leader` is | yes |
| `lodia_controlplane_datastore_has_leader` | | the last sample: 1 when a quorum of the voting members answered naming one leader, else 0; absent while the `ControlPlane` is paused or being deleted, until its `Etcd` is formed, and after a pass that could not read the cache, or the client certificate in time, until one does | yes |
| `lodia_controlplane_datastore_alarm` | `alarm` (`NOSPACE`, `CORRUPT`) | 1 per alarm armed in the last sample; none wherever `lodia_controlplane_datastore_has_leader` is absent | yes |
| `lodia_controlplane_nodes_silent`, `lodia_controlplane_nodes_counted` | | Health's last Lease list, once its clock has run `silentAfter`: the silent nodes and those that count, the two sides of the hold's fraction ([controller-manager-hold](controller-manager-hold.md)) | with Health's metrics |
| `lodia_controlplane_kubernetes_end_of_support_timestamp_seconds` | | the end-of-support date the version table gives the minor of `status.kubernetes.version` ([upgrades-and-maintenance-windows](upgrades-and-maintenance-windows.md)) | once the version table has the dates |

Of the operator, without a tenant's labels:

| metric | labels | value | served |
|---|---|---|---|
| `lodia_fleet_max_concurrent_rollouts` | | `fleet.maxConcurrentRollouts`, the slots of the fleet | yes |
| `lodia_health_nodes_sample_lag_seconds` | a histogram, buckets 0.25, 0.5, 1, 2.5, 5, 10 and 30 | how much later than its due time a `health-nodes` pass started: the watchdog of the hold's margin | with Health's metrics |
| `lodia_health_probe_failures_total` | `path` (`api`, `tunnel`, `konnectivity`, `internal`) | the probes that failed, whether or not three in a row turned `Reachable` False | with Health's metrics |

## Why

- Platforms build their own rules and dashboards on these names, as on `status`; a rename breaks them silently, so it gets the care of an API change.
- A tenant label on controller-runtime's reconcile histogram, 40 buckets, would be 40 × 10 controllers × 200 tenants, 80,000 series; Lodia's metrics are 94 series per tenant in `gateway` mode and 89 in the others, up to 3 more while a change rolls or waits and 2 while etcd alarms are armed.
- Built at scrape time, the metrics cannot drift from `status`, and a new leader has them at its first scrape once its informers sync.

## Rejected

- A tenant label on the reconcile metrics — the cost above.
- Serving from both replicas and aggregating in every rule — it doubles every series and starts a second watch in the standby.
- kube-state-metrics' custom resource state — feature-frozen, to be deprecated ([customresourcestate-metrics.md](https://github.com/kubernetes/kube-state-metrics/blob/v2.20.0/docs/metrics/extend/customresourcestate-metrics.md#L3-L4)), it maps `unknown` to 0 as it maps `false` ([customresourcestate-metrics.md](https://github.com/kubernetes/kube-state-metrics/blob/v2.20.0/docs/metrics/extend/customresourcestate-metrics.md#L625)), and it is configured in the platform's kube-state-metrics, which Lodia's chart does not own.
- A ServiceMonitor on every tenant's etcd by default — the full-data certificate and the sidecar's endpoints above.
- `status.datastore.dbSizeBytes` and `status.datastore.lastBackup` — they change all the time, and every status write wakes six controllers; the size is a metric, and the backups' health is `BackupReady`.
- Metrics of a `Backup` or a `KubeconfigRequest` — one-shot objects, whose creator waits on `Ready`; continuous backups are `BackupReady`, and a pre-upgrade `Backup` that fails is `Progressing` `Failed`.
- `status.maintenance.settlingSince`, the revisions the renderers report and `status.components.<component>.updated` — the settling is shorter than two scrapes 30 seconds apart, and `Progressing` names its outcome and a renderer behind, `APIServerReady` a component's roll past its progress deadline.
- A list of the cache without its deep copies — it saves a copy of each `ControlPlane` a scrape, and a bug in the collector would write into the cache every controller reads.

## Consequences

- The alerts in [`operations.md`](../operations.md#alerts-shipped-in-the-chart) are written on these metrics.
- A golden test pins the collector's output, and `make verify` runs `promtool check metrics` on it; with the alerts and the dashboards it runs `promtool check rules`, `promtool test rules`, a lint of the dashboards and a test that every metric they read exists.
- Each controller's ClusterRole gains `create` and `patch` on `events.k8s.io` Events with the events.

## Code

- `internal/metrics` — `Collector` builds the metrics served from the manager's cache and the etcd sampler's samples, once elected, under the lists' deadline; `collector_test.go` holds the golden `testdata/metrics.prom` over a tenant of each shape, a standby before and after its election, an informer that has not synced, a recreated `ControlPlane` and two scrapes at once, `etcd_test.go` the quota and the period of an `Etcd` without a sample and an `Etcd` list that does not answer, and `BenchmarkCollect` a scrape at 200 tenants.
- `internal/controller/datastore/sample.go` — `SampleReconciler`, the controller `datastore-sample`, and the samples it serves; `sample_test.go` runs it on embedded etcd: one member, three with members stopped, a learner before and after it starts, a member list that fails, a client that leaves the pass too little time, the client each pass closes, both alarms, the ControlPlanes it forgets, a cache read and a dial that fail, a recreated one, the minute each sample stands, and a member and a Secret read that hang; `sample_internal_test.go` the largest size and the leader a quorum names among synthetic answers.
- `api/v1alpha1/metrics.go` — the label values no other type of the package holds.
- `internal/operator`, `cmd/lodia` — `Metrics` builds the collector on the running revision, `fleet.maxConcurrentRollouts` and the etcd sampler `Setup` returns, and `cmd/lodia` registers it in controller-runtime's registry after `Setup`; `operator_test.go` reads, through `Metrics`, the sample the manager's sampler keeps.
- `Makefile`, `.github/workflows/ci.yml` — `make monitoring`, part of `verify`, runs `promtool check metrics` on the golden; CI installs promtool by its checksum.
- `charts/lodia/templates/metrics-service.yaml`, `servicemonitor.yaml` — the Service and the ServiceMonitor behind `metrics.service.enabled` and `metrics.serviceMonitor.enabled` ([Scraping the operator](../README.md#scraping-the-operator)); `make chart` checks that the defaults render neither, that the ServiceMonitor fails without the Service, honors the labels, makes the job `lodia`, scrapes the Service's port, selects that Service alone, and sends the token file only without `scrapeClass`.