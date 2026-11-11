// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

// The values of the labels of Lodia's metrics that come from a set no type of
// this package holds; the condition types and reasons, Layout and UpgradeStep
// are the values of theirs (docs/decisions/metrics-are-part-of-the-api.md).
const (
	// MetricDatastoreDedicated and MetricDatastoreShared name the member of
	// spec.datastore that is set, in the label datastore.
	MetricDatastoreDedicated = "dedicated"
	MetricDatastoreShared    = "shared"

	// MetricEndpointGateway, MetricEndpointLoadBalancer and
	// MetricEndpointNodePort name the member of spec.endpoint that is set, in
	// the label endpoint_mode.
	MetricEndpointGateway      = "gateway"
	MetricEndpointLoadBalancer = "loadBalancer"
	MetricEndpointNodePort     = "nodePort"

	// MetricComponentAPIServer, MetricComponentControllerManager and
	// MetricComponentScheduler name a member of status.components, in the
	// label component.
	MetricComponentAPIServer         = "apiServer"
	MetricComponentControllerManager = "controllerManager"
	MetricComponentScheduler         = "scheduler"

	// MetricAlarmNoSpace and MetricAlarmCorrupt name an alarm armed in a
	// dedicated etcd, as etcd names it, in the label alarm.
	MetricAlarmNoSpace = "NOSPACE"
	MetricAlarmCorrupt = "CORRUPT"
)
