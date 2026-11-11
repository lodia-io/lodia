// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package metrics_test

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	running  = "v1.0.0+6f1c"
	previous = "v0.9.0+2b7e"
)

var (
	now            = time.Date(2026, 11, 11, 8, 0, 0, 0, time.UTC)
	beyondAnyClock = time.Date(2100, 1, 1, 3, 0, 0, 0, time.UTC)
)

func ago(d time.Duration) *metav1.Time { return &metav1.Time{Time: now.Add(-d)} }

var conditionTypes = []string{
	lodiav1alpha1.ConditionCertificatesReady, lodiav1alpha1.ConditionCAValidityAcceptable, lodiav1alpha1.ConditionAccessReady,
	lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ConditionAPIServerReady, lodiav1alpha1.ConditionRoutesReady,
	lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ConditionAddonsAvailable, lodiav1alpha1.ConditionReachable,
	lodiav1alpha1.ConditionControllersLeading, lodiav1alpha1.ConditionControllerManagerHeld, lodiav1alpha1.ConditionBackupReady,
	lodiav1alpha1.ConditionProgressing, lodiav1alpha1.ConditionReady,
}

type state struct {
	status metav1.ConditionStatus
	reason string
}

var healthy = map[string]state{
	lodiav1alpha1.ConditionCertificatesReady:     {metav1.ConditionTrue, lodiav1alpha1.ReasonIssued},
	lodiav1alpha1.ConditionCAValidityAcceptable:  {metav1.ConditionTrue, lodiav1alpha1.ReasonValid},
	lodiav1alpha1.ConditionAccessReady:           {metav1.ConditionTrue, lodiav1alpha1.ReasonBound},
	lodiav1alpha1.ConditionDatastoreReady:        {metav1.ConditionTrue, lodiav1alpha1.ReasonReady},
	lodiav1alpha1.ConditionAPIServerReady:        {metav1.ConditionTrue, lodiav1alpha1.ReasonReady},
	lodiav1alpha1.ConditionRoutesReady:           {metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted},
	lodiav1alpha1.ConditionAddonsApplied:         {metav1.ConditionTrue, lodiav1alpha1.ReasonApplied},
	lodiav1alpha1.ConditionAddonsAvailable:       {metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable},
	lodiav1alpha1.ConditionReachable:             {metav1.ConditionTrue, lodiav1alpha1.ReasonReachable},
	lodiav1alpha1.ConditionControllersLeading:    {metav1.ConditionTrue, lodiav1alpha1.ReasonLeading},
	lodiav1alpha1.ConditionControllerManagerHeld: {metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld},
	lodiav1alpha1.ConditionBackupReady:           {metav1.ConditionTrue, lodiav1alpha1.ReasonBackupSucceeded},
	lodiav1alpha1.ConditionProgressing:           {metav1.ConditionFalse, lodiav1alpha1.ReasonIdle},
	lodiav1alpha1.ConditionReady:                 {metav1.ConditionTrue, lodiav1alpha1.ReasonReady},
}

func conditions(cp *lodiav1alpha1.ControlPlane, changed map[string]state) []metav1.Condition {
	var out []metav1.Condition
	for i, conditionType := range conditionTypes {
		if conditionType == lodiav1alpha1.ConditionRoutesReady && cp.Spec.Endpoint.Gateway == nil {
			continue
		}
		s, ok := changed[conditionType]
		if !ok {
			s = healthy[conditionType]
		}
		out = append(out, metav1.Condition{
			Type: conditionType, Status: s.status, Reason: s.reason, Message: "the message of " + conditionType,
			ObservedGeneration: cp.Generation, LastTransitionTime: *ago(time.Duration(i+1) * time.Minute),
		})
	}
	return out
}

func controlPlane(name string, uid int) *lodiav1alpha1.ControlPlane {
	return &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "tenants", UID: types.UID(fmt.Sprintf("00000000-0000-0000-0000-%012d", uid)), Generation: 1},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   "v1.37.1",
			Layout:    lodiav1alpha1.LayoutSplit,
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint:  lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
}

func count(total, ready int32, updated ...int32) lodiav1alpha1.RolloutCount {
	c := lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: total, Ready: ready}}
	if len(updated) > 0 {
		c.Updated = &updated[0]
	}
	return c
}

func components(apiServer, controllerManager, scheduler lodiav1alpha1.RolloutCount) *lodiav1alpha1.ComponentsStatus {
	return &lodiav1alpha1.ComponentsStatus{
		APIServer:         &lodiav1alpha1.ComponentStatus{RolloutCount: apiServer},
		ControllerManager: &lodiav1alpha1.ComponentStatus{RolloutCount: controllerManager},
		Scheduler:         &lodiav1alpha1.ComponentStatus{RolloutCount: scheduler},
	}
}

func datastore(members lodiav1alpha1.RolloutCount) *lodiav1alpha1.DatastoreStatus {
	return &lodiav1alpha1.DatastoreStatus{Endpoints: []lodiav1alpha1.HostPort{{Host: "etcd-client", Port: 2379}}, Members: members}
}

func steady(cp *lodiav1alpha1.ControlPlane, version string) {
	cp.Status.ObservedGeneration = cp.Generation
	cp.Status.Conditions = conditions(cp, nil)
	cp.Status.Target = &lodiav1alpha1.Target{Version: version, OperatorRevision: running}
	cp.Status.Kubernetes = &lodiav1alpha1.KubernetesStatus{Version: version}
	cp.Status.Components = components(count(1, 1, 1), count(1, 1, 1), count(1, 1, 1))
	cp.Status.Datastore = datastore(count(1, 1, 1))
	cp.Status.Nodes = &lodiav1alpha1.NodesStatus{Count: lodiav1alpha1.Count{Total: 2, Ready: 2}}
}

func gatewayHA(name string, uid int) *lodiav1alpha1.ControlPlane {
	cp := controlPlane(name, uid)
	cp.Generation = 3
	cp.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"}
	cp.Spec.Endpoint = lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
		ParentRef: lodiav1alpha1.ParentReference{Name: "edge", Namespace: "gateways", SectionName: "tls"},
		Hostnames: lodiav1alpha1.GatewayHostnames{APIServer: lodiav1alpha1.Hostname("api." + name + ".example.com"), Konnectivity: lodiav1alpha1.Hostname("konnectivity." + name + ".example.com"), Tunnel: lodiav1alpha1.Hostname("tunnel." + name + ".example.com")},
	}}
	steady(cp, "v1.37.1")
	cp.Status.Conditions = conditions(cp, map[string]state{lodiav1alpha1.ConditionProgressing: {metav1.ConditionTrue, lodiav1alpha1.ReasonMaintaining}})
	cp.Status.Components = components(count(3, 3, 3), count(2, 2, 2), count(2, 2, 1))
	cp.Status.Datastore = datastore(count(3, 3, 3))
	cp.Status.Nodes = &lodiav1alpha1.NodesStatus{Count: lodiav1alpha1.Count{Total: 5, Ready: 4}}
	cp.Status.Maintenance = &lodiav1alpha1.MaintenanceStatus{SlotSince: ago(10 * time.Minute)}
	return cp
}

func golden() []client.Object {
	fresh := controlPlane("fresh", 1)

	edge := gatewayHA("edge", 2)

	restored := controlPlane("restored", 3)
	restored.Spec.Endpoint = lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}
	restored.Spec.RestoreFrom = &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: "edge-before"}, Release: lodiav1alpha1.RestoreHeld}
	steady(restored, "v1.36.4")
	restored.Status.Conditions = conditions(restored, map[string]state{
		lodiav1alpha1.ConditionAddonsAvailable:    {metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending},
		lodiav1alpha1.ConditionControllersLeading: {metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease},
		lodiav1alpha1.ConditionBackupReady:        {metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotsPending},
		lodiav1alpha1.ConditionReady:              {metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease},
	})
	restored.Status.Components = components(count(1, 1, 1), count(0, 0, 0), count(1, 1, 1))
	restored.Status.Nodes = &lodiav1alpha1.NodesStatus{}

	failed := controlPlane("failed", 4)
	failed.Generation = 2
	steady(failed, "v1.37.1")
	failed.Status.Conditions = conditions(failed, map[string]state{lodiav1alpha1.ConditionProgressing: {metav1.ConditionFalse, lodiav1alpha1.ReasonMaintenanceFailed}})
	failed.Status.Datastore = datastore(count(1, 1))
	failed.Status.Maintenance = &lodiav1alpha1.MaintenanceStatus{SlotSince: ago(3 * time.Hour)}

	queued := controlPlane("queued", 5)
	steady(queued, "v1.37.1")
	queued.Status.Conditions = conditions(queued, map[string]state{lodiav1alpha1.ConditionProgressing: {metav1.ConditionFalse, lodiav1alpha1.ReasonFleetLimit}})
	queued.Status.Target.OperatorRevision = previous
	queued.Status.Target.ScheduledFor = ago(20 * time.Minute)

	upgrading := controlPlane("upgrading", 6)
	upgrading.Generation = 4
	steady(upgrading, "v1.36.4")
	upgrading.Status.Conditions = conditions(upgrading, map[string]state{lodiav1alpha1.ConditionProgressing: {metav1.ConditionTrue, lodiav1alpha1.ReasonUpgrading}})
	upgrading.Status.Target.Version = "v1.37.1"
	upgrading.Status.Kubernetes.Upgrade = &lodiav1alpha1.UpgradeStatus{Step: lodiav1alpha1.UpgradeStepAddons, StartedAt: *ago(40 * time.Minute)}
	upgrading.Status.Nodes.OutdatedKubelets = 2

	paused := controlPlane("paused", 7)
	paused.Generation = 5
	paused.Spec.Paused = true
	steady(paused, "v1.37.1")
	paused.Status.ObservedGeneration = 4
	paused.Status.Conditions = conditions(paused, map[string]state{lodiav1alpha1.ConditionProgressing: {metav1.ConditionFalse, lodiav1alpha1.ReasonPaused}})
	paused.Status.Maintenance = &lodiav1alpha1.MaintenanceStatus{SlotSince: ago(2 * time.Hour)}

	starting := controlPlane("starting", 10)
	starting.Status.Components = &lodiav1alpha1.ComponentsStatus{APIServer: &lodiav1alpha1.ComponentStatus{RolloutCount: count(1, 0, 0)}}

	rolling := controlPlane("rolling", 11)
	rolling.Generation = 2
	steady(rolling, "v1.36.4")
	rolling.Status.Conditions = conditions(rolling, map[string]state{lodiav1alpha1.ConditionProgressing: {metav1.ConditionTrue, lodiav1alpha1.ReasonUpgrading}})
	rolling.Status.Target.Version = "v1.37.1"
	rolling.Status.Kubernetes.Upgrade = &lodiav1alpha1.UpgradeStatus{Step: lodiav1alpha1.UpgradeStepAPIServer, StartedAt: *ago(5 * time.Minute)}
	rolling.Status.Components = components(count(3, 2, 2), count(2, 1, 1), count(2, 2, 1))
	rolling.Status.Datastore = datastore(count(3, 2, 1))

	waiting := controlPlane("waiting", 12)
	steady(waiting, "v1.37.1")
	waiting.Status.Conditions = conditions(waiting, map[string]state{lodiav1alpha1.ConditionProgressing: {metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForWindow}})
	waiting.Status.Target.OperatorRevision = previous
	waiting.Status.Target.ScheduledFor = &metav1.Time{Time: beyondAnyClock}

	overdue := controlPlane("overdue", 13)
	overdue.Status.Target = &lodiav1alpha1.Target{Version: "v1.37.1", OperatorRevision: previous, ScheduledFor: ago(24 * time.Hour)}

	shared := controlPlane("shared", 8)
	shared.Spec.Layout = lodiav1alpha1.LayoutCompact
	shared.Spec.Datastore = lodiav1alpha1.DatastoreChoice{Shared: &lodiav1alpha1.SharedDatastore{Name: "pool"}}
	shared.Status.Conditions = conditions(shared, map[string]state{
		lodiav1alpha1.ConditionAccessReady:           {metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAPIServer},
		lodiav1alpha1.ConditionDatastoreReady:        {metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported},
		lodiav1alpha1.ConditionAPIServerReady:        {metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore},
		lodiav1alpha1.ConditionAddonsApplied:         {metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess},
		lodiav1alpha1.ConditionAddonsAvailable:       {metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess},
		lodiav1alpha1.ConditionReachable:             {metav1.ConditionUnknown, lodiav1alpha1.ReasonReconciling},
		lodiav1alpha1.ConditionControllersLeading:    {metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess},
		lodiav1alpha1.ConditionControllerManagerHeld: {metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess},
		lodiav1alpha1.ConditionBackupReady:           {metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported},
		lodiav1alpha1.ConditionReady:                 {metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported},
	})
	shared.Status.Target = &lodiav1alpha1.Target{Version: "v1.37.1", OperatorRevision: running}

	deleting := controlPlane("deleting", 9)
	deleting.DeletionTimestamp = ago(5 * time.Minute)
	deleting.Finalizers = []string{lodiav1alpha1.DatastoreFinalizer}
	steady(deleting, "v1.37.1")
	deleting.Status.Conditions = conditions(deleting, map[string]state{lodiav1alpha1.ConditionReady: {metav1.ConditionFalse, lodiav1alpha1.ReasonDeleting}})

	tenants := []client.Object{fresh, edge, restored, failed, queued, upgrading, paused, starting, rolling, waiting, overdue, shared, deleting}
	return append(tenants, goldenEtcds(tenants)...)
}
