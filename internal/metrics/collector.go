// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package metrics builds Lodia's metrics of each ControlPlane from its status,
// its etcd's samples and Health's counts at scrape time, in the leader alone,
// and serves Health's histogram and counter
// (docs/decisions/metrics-are-part-of-the-api.md).
package metrics

import (
	"context"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/versions"
)

// listTimeout is below Prometheus' default scrape timeout of 10 s: an
// informer that has not synced yet holds a list for as long as its context
// allows.
const listTimeout = 5 * time.Second

var (
	info = perControlPlane("lodia_controlplane_info",
		"1 for every ControlPlane: its UID, status.kubernetes.version, empty while absent, spec.layout, and the members of spec.datastore and spec.endpoint that are set.",
		"uid", "kubernetes_version", "layout", "datastore", "endpoint_mode")
	condition = perControlPlane("lodia_controlplane_condition",
		"1 for the status of each condition in status.conditions and 0 for its other two; a type absent from status has no series.",
		"type", "status")
	conditionReason = perControlPlane("lodia_controlplane_condition_reason",
		"1 for the reason of each condition in status.conditions.",
		"type", "reason")
	conditionLastTransition = perControlPlane("lodia_controlplane_condition_last_transition_timestamp_seconds",
		"The lastTransitionTime of each condition in status.conditions.",
		"type")
	generation = perControlPlane("lodia_controlplane_metadata_generation",
		"metadata.generation.")
	observedGeneration = perControlPlane("lodia_controlplane_status_observed_generation",
		"status.observedGeneration: the newest generation every readiness condition has been reported for.")
	nodes = perControlPlane("lodia_controlplane_nodes",
		"status.nodes.total: the tenant's nodes.")
	nodesReady = perControlPlane("lodia_controlplane_nodes_ready",
		"status.nodes.ready: the tenant's nodes that are Ready.")
	nodesOutdatedKubelets = perControlPlane("lodia_controlplane_nodes_outdated_kubelets",
		"status.nodes.outdatedKubelets: the tenant's nodes whose kubelet runs an older minor than status.kubernetes.version; absent while status.nodes.truncated is true.")
	componentReplicas = perControlPlane("lodia_controlplane_component_replicas",
		"status.components.<component>.total: the replicas the component's Deployment asks for.",
		"component")
	componentReplicasReady = perControlPlane("lodia_controlplane_component_replicas_ready",
		"status.components.<component>.ready: those of them that are ready.",
		"component")
	datastoreMembers = perControlPlane("lodia_controlplane_datastore_members",
		"status.datastore.members.total: the members the etcd asks for.")
	datastoreMembersReady = perControlPlane("lodia_controlplane_datastore_members_ready",
		"status.datastore.members.ready: those of them that are ready.")
	datastoreMembersUpdated = perControlPlane("lodia_controlplane_datastore_members_updated",
		"status.datastore.members.updated: those of them that run the Etcd's current spec and are available; absent while it is.")
	datastoreQuota = perControlPlane("lodia_controlplane_datastore_quota_bytes",
		"The Etcd's spec.etcd.quota: the database size at which etcd arms NOSPACE.")
	datastoreDeltaSnapshotPeriod = perControlPlane("lodia_controlplane_datastore_delta_snapshot_period_seconds",
		"The Etcd's spec.backup.deltaSnapshotPeriod; absent without a backup store and below 1 s, where no delta snapshot is taken.")
	datastoreDBSize = perControlPlane("lodia_controlplane_datastore_db_size_bytes",
		"The largest dbSize among the etcd's members that answered the last sample; absent when none did and wherever lodia_controlplane_datastore_has_leader is.")
	datastoreHasLeader = perControlPlane("lodia_controlplane_datastore_has_leader",
		"The last sample of the etcd: 1 when a quorum of the voting members answered naming one leader, else 0; absent while the ControlPlane is paused or being deleted, until its Etcd is formed, and after a pass that could not read the cache, or the client certificate in time, until one does.")
	datastoreAlarm = perControlPlane("lodia_controlplane_datastore_alarm",
		"1 for each alarm armed in the etcd in the last sample; none wherever lodia_controlplane_datastore_has_leader is absent.",
		"alarm")
	operatorRevisionGranted = perControlPlane("lodia_controlplane_operator_revision_granted",
		"1 when status.target.operatorRevision is the running operator's revision, else 0.")
	slotSince = perControlPlane("lodia_controlplane_maintenance_slot_since_timestamp_seconds",
		"status.maintenance.slotSince: when the change Lodia started took a slot of the fleet, which the tenant holds while this series exists.")
	scheduledFor = perControlPlane("lodia_controlplane_maintenance_scheduled_for_timestamp_seconds",
		"status.target.scheduledFor: when a change Lodia initiates may roll; at or before now the tenant is queued for a slot, later it waits for its window.")
	upgradeStarted = perControlPlane("lodia_controlplane_upgrade_started_timestamp_seconds",
		"status.kubernetes.upgrade.startedAt, labelled with its step: when the version change in progress started.",
		"step")
	certificatesExpiration = perControlPlane("lodia_controlplane_certificates_expiration_timestamp_seconds",
		"status.credentials.notAfter: the earliest expiry among the certificates PKI's Secrets hold for the ControlPlane, its CAs among them. It counts each Secret's own certificate and the newest complete etcd generation, and leaves out the replaced certificates in the CA bundles, the staged kubelet signer and a previous etcd generation.")
	endOfSupport = perControlPlane("lodia_controlplane_kubernetes_end_of_support_timestamp_seconds",
		"The end-of-support date, at 00:00 UTC, that the version table gives the minor of status.kubernetes.version; absent while the version is absent and for a minor outside the table.")
	nodesSilent = perControlPlane("lodia_controlplane_nodes_silent",
		"The nodes that count whose Lease Health has not seen change for health.silentAfter, in its last list of the node Leases. Absent while that list is missing, or a list of the Leases or of the Nodes stopped at a bound, or the tenant's hold is off, before Health's first list of the Nodes, and until Health's clock of the Leases has run health.silentAfter.")
	nodesCounted = perControlPlane("lodia_controlplane_nodes_counted",
		"The nodes that count towards the hold of the controller manager in Health's last list of the node Leases: those with a Lease that are Ready, or turned Unknown less than 5 minutes before the hold began. Absent while lodia_controlplane_nodes_silent is.")
	maxConcurrentRollouts = prometheus.NewDesc("lodia_fleet_max_concurrent_rollouts",
		"fleet.maxConcurrentRollouts: how many tenants may hold a slot of the fleet at once.", nil, nil)
	holdThresholdRatio = prometheus.NewDesc("lodia_health_hold_threshold_ratio",
		"health.holdFraction: the share of the counted nodes that are silent at which Health holds a tenant's controller manager.", nil, nil)

	descs = []*prometheus.Desc{
		info, condition, conditionReason, conditionLastTransition, generation, observedGeneration,
		nodes, nodesReady, nodesOutdatedKubelets, componentReplicas, componentReplicasReady,
		datastoreMembers, datastoreMembersReady, datastoreMembersUpdated,
		datastoreQuota, datastoreDeltaSnapshotPeriod, datastoreDBSize, datastoreHasLeader, datastoreAlarm, operatorRevisionGranted,
		slotSince, scheduledFor, upgradeStarted, maxConcurrentRollouts, holdThresholdRatio,
		certificatesExpiration, endOfSupport, nodesSilent, nodesCounted,
	}

	statuses = []metav1.ConditionStatus{metav1.ConditionTrue, metav1.ConditionFalse, metav1.ConditionUnknown}
)

func perControlPlane(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(name, help, append([]string{"namespace", "controlplane"}, labels...), nil)
}

// NewSampleLag returns the histogram health-nodes observes and the collector
// serves.
func NewSampleLag() prometheus.Histogram {
	return prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "lodia_health_nodes_sample_lag_seconds",
		Help:    "How much later than its due time, the previous pass's end plus the requeue it returned, a pass of health-nodes started: the delay in Health's queue, for which the margin of the controller manager's hold keeps 10 seconds.",
		Buckets: []float64{0.25, 0.5, 1, 2.5, 5, 10, 30},
	})
}

// NewProbeFailures returns the counter Health's probes count into and the
// collector serves, with a series of each path from the start.
func NewProbeFailures() *prometheus.CounterVec {
	failures := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "lodia_health_probe_failures_total",
		Help: "Health's probes that failed, by path, whether or not three failed passes in a row turned Reachable False.",
	}, []string{"path"})
	for _, path := range []string{lodiav1alpha1.MetricProbeAPI, lodiav1alpha1.MetricProbeInternal, lodiav1alpha1.MetricProbeTunnel, lodiav1alpha1.MetricProbeKonnectivity} {
		failures.WithLabelValues(path)
	}
	return failures
}

// Collector builds the metrics from Reader, the manager's cache, once Elected,
// the manager's, is closed, with Revision, the running operator revision,
// MaxConcurrentRollouts, fleet.maxConcurrentRollouts, HoldFraction,
// health.holdFraction in thousandths, EtcdSample, the sample the etcd sampler
// serves of a ControlPlane's etcd, by its name and UID, and NodeCounts, the
// count of a ControlPlane's nodes health-nodes published, by its name and UID.
// It serves SampleLag and ProbeFailures, Health's, which no registry holds, so
// that the standby serves neither. It keeps no state, so scrapes may run at
// once.
type Collector struct {
	Reader                client.Reader
	Elected               <-chan struct{}
	Revision              string
	MaxConcurrentRollouts int
	HoldFraction          int
	EtcdSample            func(types.NamespacedName, types.UID) (datastore.EtcdSample, bool)
	NodeCounts            func(types.NamespacedName, types.UID) (silent, counted int, ok bool)
	SampleLag             prometheus.Histogram
	ProbeFailures         *prometheus.CounterVec
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range descs {
		ch <- desc
	}
	c.SampleLag.Describe(ch)
	c.ProbeFailures.Describe(ch)
}

// Collect emits nothing in a replica that does not lead: the cache's
// informers start with the controllers, in the leader alone, and a list
// would start them in the standby too, which would serve the same series.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	select {
	case <-c.Elected:
	default:
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), listTimeout)
	defer cancel()
	var list lodiav1alpha1.ControlPlaneList
	if err := c.Reader.List(ctx, &list); err != nil {
		logf.Log.WithName("metrics").Error(err, "list the ControlPlanes: this scrape gets none of Lodia's metrics")
		return
	}
	var etcds druidv1alpha1.EtcdList
	if err := c.Reader.List(ctx, &etcds); err != nil {
		logf.Log.WithName("metrics").Error(err, "list the Etcds: this scrape gets none of Lodia's metrics")
		return
	}
	byName := make(map[types.NamespacedName]*druidv1alpha1.Etcd, len(etcds.Items))
	for i := range etcds.Items {
		byName[client.ObjectKeyFromObject(&etcds.Items[i])] = &etcds.Items[i]
	}
	ch <- prometheus.MustNewConstMetric(maxConcurrentRollouts, prometheus.GaugeValue, float64(c.MaxConcurrentRollouts))
	ch <- prometheus.MustNewConstMetric(holdThresholdRatio, prometheus.GaugeValue, float64(c.HoldFraction)/1000)
	c.SampleLag.Collect(ch)
	c.ProbeFailures.Collect(ch)
	for i := range list.Items {
		cp := &list.Items[i]
		c.collect(ch, cp, byName[types.NamespacedName{Namespace: cp.Namespace, Name: lodiav1alpha1.EtcdName(cp.Name)}])
	}
}

func (c *Collector) collect(ch chan<- prometheus.Metric, cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd) {
	emit := func(desc *prometheus.Desc, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, append([]string{cp.Namespace, cp.Name}, labels...)...)
	}
	status := &cp.Status

	var version string
	if status.Kubernetes != nil {
		version = status.Kubernetes.Version
	}
	datastore := lodiav1alpha1.MetricDatastoreDedicated
	if cp.Spec.Datastore.Shared != nil {
		datastore = lodiav1alpha1.MetricDatastoreShared
	}
	endpoint := lodiav1alpha1.MetricEndpointNodePort
	switch {
	case cp.Spec.Endpoint.Gateway != nil:
		endpoint = lodiav1alpha1.MetricEndpointGateway
	case cp.Spec.Endpoint.LoadBalancer != nil:
		endpoint = lodiav1alpha1.MetricEndpointLoadBalancer
	}
	emit(info, 1, string(cp.UID), version, string(cp.Spec.Layout), datastore, endpoint)
	minor, _ := versions.Minor(version)
	if day, ok := versions.EndOfSupport(minor); ok {
		emit(endOfSupport, float64(day.Unix()))
	}

	for _, cond := range status.Conditions {
		for _, s := range statuses {
			emit(condition, oneIf(cond.Status == s), cond.Type, string(s))
		}
		emit(conditionReason, 1, cond.Type, cond.Reason)
		emit(conditionLastTransition, seconds(cond.LastTransitionTime), cond.Type)
	}

	emit(generation, float64(cp.Generation))
	if status.ObservedGeneration > 0 {
		emit(observedGeneration, float64(status.ObservedGeneration))
	}

	if n := status.Nodes; n != nil {
		emit(nodes, float64(n.Total))
		emit(nodesReady, float64(n.Ready))
		if !n.Truncated {
			emit(nodesOutdatedKubelets, float64(n.OutdatedKubelets))
		}
	}
	if silent, counted, ok := c.NodeCounts(client.ObjectKeyFromObject(cp), cp.UID); ok {
		emit(nodesSilent, float64(silent))
		emit(nodesCounted, float64(counted))
	}

	if components := status.Components; components != nil {
		for _, component := range []struct {
			name   string
			status *lodiav1alpha1.ComponentStatus
		}{
			{lodiav1alpha1.MetricComponentAPIServer, components.APIServer},
			{lodiav1alpha1.MetricComponentControllerManager, components.ControllerManager},
			{lodiav1alpha1.MetricComponentScheduler, components.Scheduler},
		} {
			if component.status != nil {
				emit(componentReplicas, float64(component.status.Total), component.name)
				emit(componentReplicasReady, float64(component.status.Ready), component.name)
			}
		}
	}

	if d := status.Datastore; d != nil {
		emit(datastoreMembers, float64(d.Members.Total))
		emit(datastoreMembersReady, float64(d.Members.Ready))
		if d.Members.Updated != nil {
			emit(datastoreMembersUpdated, float64(*d.Members.Updated))
		}
	}
	if etcd != nil && metav1.IsControlledBy(etcd, cp) {
		if quota := etcd.Spec.Etcd.Quota; quota != nil {
			emit(datastoreQuota, float64(quota.Value()))
		}
		if period := etcd.Spec.Backup.DeltaSnapshotPeriod; etcd.Spec.Backup.Store != nil && period != nil && period.Duration >= time.Second {
			emit(datastoreDeltaSnapshotPeriod, period.Seconds())
		}
	}
	if sample, ok := c.EtcdSample(client.ObjectKeyFromObject(cp), cp.UID); ok {
		if sample.DBSize != nil {
			emit(datastoreDBSize, float64(*sample.DBSize))
		}
		emit(datastoreHasLeader, oneIf(sample.HasLeader))
		for _, alarm := range sample.Alarms {
			emit(datastoreAlarm, 1, alarm)
		}
	}

	emit(operatorRevisionGranted, oneIf(status.Target != nil && status.Target.OperatorRevision == c.Revision))
	if m := status.Maintenance; m != nil && m.SlotSince != nil {
		emit(slotSince, seconds(*m.SlotSince))
	}
	if t := status.Target; t != nil && t.ScheduledFor != nil {
		emit(scheduledFor, seconds(*t.ScheduledFor))
	}
	if k := status.Kubernetes; k != nil && k.Upgrade != nil {
		emit(upgradeStarted, seconds(k.Upgrade.StartedAt), string(k.Upgrade.Step))
	}
	if c := status.Credentials; c != nil && c.NotAfter != nil {
		emit(certificatesExpiration, seconds(*c.NotAfter))
	}
}

func oneIf(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func seconds(t metav1.Time) float64 { return float64(t.Unix()) }
