// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package rules

import (
	"fmt"
	"slices"
)

func cases() []testCase {
	return slices.Concat(
		operatorDown(), controlPlaneNotReady(), controlPlaneDeletionStuck(), redundancyLost(), rolloutStuck(),
		certificateExpiring(), caExpiring(), backupStale(), etcdSpaceNearQuota(), etcdNoLeader(), etcdAlarm(),
		controllerManagerHeld(), controllerManagerHoldBlocked(), upgradeFailed(), upgradeStalled(),
		maintenanceHalted(), maintenanceSlotHeldLong(), reconcileErrors(), healthSamplingLate(),
	)
}

func operatorDown() []testCase {
	const alert = "LodiaOperatorDown"
	leader := func(p pod, values string) batch {
		return operator("leader_election_master_status", values, "name", "lodia.io").on(p)
	}
	return []testCase{
		{
			name:   alert + " fires on a standby's 0 alone",
			series: leader(podB, "0x20"),
			checks: []check{{270, alert, nil}, {300, alert, []alertLabels{{}}}},
		},
		{
			name:   alert + " fires with no series at all",
			series: operator("lodia_fleet_max_concurrent_rollouts", "5x20"),
			checks: []check{{270, alert, nil}, {300, alert, []alertLabels{{}}}},
		},
		{
			name:   alert + " stays silent while the first replica leads",
			series: join(leader(podA, "1x20"), leader(podB, "0x20")),
			checks: []check{{300, alert, nil}, {600, alert, nil}},
		},
		{
			name:   alert + " stays silent while the second replica leads",
			series: join(leader(podA, "0x20"), leader(podB, "1x20")),
			checks: []check{{300, alert, nil}, {600, alert, nil}},
		},
		{
			name:   alert + " stays silent through 210 seconds without a leader between two",
			series: join(leader(podA, "1x10 stale"), leader(podB, "0x17 1x20")),
			checks: []check{{330, alert, nil}, {510, alert, nil}, {540, alert, nil}, {900, alert, nil}},
		},
		{
			name:   alert + " keeps firing for 4 minutes after a leader returns",
			series: leader(podA, "_x20 1x20"),
			checks: []check{
				{300, alert, []alertLabels{{}}}, {600, alert, []alertLabels{{}}}, {810, alert, []alertLabels{{}}},
				{840, alert, nil},
			},
		},
	}
}

func controlPlaneNotReady() []testCase {
	const alert = "LodiaControlPlaneNotReady"
	ready0 := func(cp string) batch { return cond(cp, "Ready", "True", "0x40") }
	return []testCase{
		{
			name: alert + " on each side of its exclusions",
			series: join(
				ready0("unknown"), reason("unknown", "Ready", "Reconciling", "1x40"),
				ready0("conflict"), reason("conflict", "Ready", "HostnameConflict", "1x40"),
				reason("conflict", "ControllersLeading", "Leading", "1x40"),
				ready0("held-ca"), reason("held-ca", "Ready", "CAExpired", "1x40"),
				reason("held-ca", "ControllersLeading", "AwaitingRelease", "1x40"),
				ready0("held"), reason("held", "Ready", "AwaitingRelease", "1x40"),
				reason("held", "ControllersLeading", "AwaitingRelease", "1x40"),
				ready0("source"), reason("source", "Ready", "SourceExists", "1x40"),
				reason("source", "ControllersLeading", "SourceExists", "1x40"),
				ready0("held-conflict"), reason("held-conflict", "Ready", "HostnameConflict", "1x40"),
				reason("held-conflict", "ControllersLeading", "AwaitingRelease", "1x40"),
				ready0("source-conflict"), reason("source-conflict", "Ready", "HostnameConflict", "1x40"),
				reason("source-conflict", "ControllersLeading", "SourceExists", "1x40"),
				ready0("deleting"), reason("deleting", "Ready", "Deleting", "1x40"),
				ready0("restoring"), reason("restoring", "Ready", "Restoring", "1x40"),
				ready0("restore-backup"), reason("restore-backup", "Ready", "WaitingForBackup", "1x40"),
				ready0("restore-conflict"), reason("restore-conflict", "Ready", "HostnameConflict", "1x40"),
				reason("restore-conflict", "ControllersLeading", "WaitingForAccess", "1x40"),
				reason("restore-conflict", "DatastoreReady", "Restoring", "1x40"),
				ready0("paused"), reason("paused", "Ready", "Unavailable", "1x40"), reason("paused", "Progressing", "Paused", "1x40"),
				cond("ready", "Ready", "True", "1x40"), reason("ready", "Ready", "Ready", "1x40"),
				join(
					ready0("unknown"), reason("unknown", "Ready", "Deleting", "1x40"), reason("unknown", "Progressing", "Paused", "1x40"),
					ready0("conflict"), reason("conflict", "Ready", "HostnameConflict", "1x40"),
					reason("conflict", "ControllersLeading", "AwaitingRelease", "1x40"),
				).in("tenants-b"),
			),
			checks: []check{
				{570, alert, nil},
				{600, alert, []alertLabels{
					tenantAlert("unknown"), tenantAlert("conflict"), tenantAlert("held-ca"),
					tenantAlert("restore-backup"), tenantAlert("restore-conflict"),
				}},
			},
		},
		failover(alert, tenantAlert("a"), 900, cond("a", "Ready", "True", "0")),
		{
			name: alert + " starts its for again after a failover drops it pending",
			series: join(
				cond("a", "Ready", "True", "0x10 stale"),
				cond("a", "Ready", "True", "_x18 0x40").on(podB),
			),
			checks: []check{{600, alert, nil}, {1110, alert, nil}, {1140, alert, []alertLabels{tenantAlert("a")}}},
			asIs:   true,
		},
	}
}

func controlPlaneDeletionStuck() []testCase {
	const alert = "LodiaControlPlaneDeletionStuck"
	return []testCase{
		{
			name: alert + " at its for, and never while paused",
			series: join(
				reason("deleting", "Ready", "Deleting", "1x70"),
				reason("paused", "Ready", "Deleting", "1x70"), reason("paused", "Progressing", "Paused", "1x70"),
				reason("ready", "Ready", "Ready", "1x70"),
				join(reason("deleting", "Ready", "Deleting", "1x70"), reason("deleting", "Progressing", "Paused", "1x70")).in("tenants-b"),
			),
			checks: []check{{1770, alert, nil}, {1800, alert, []alertLabels{tenantAlert("deleting")}}},
		},
		failover(alert, tenantAlert("a"), 1800, reason("a", "Ready", "Deleting", "1")),
	}
}

func redundancyLost() []testCase {
	const alert = "LodiaRedundancyLost"
	comp := func(cp, component string, total, ready int) batch {
		return join(
			tenant("lodia_controlplane_component_replicas", cp, fmt.Sprintf("%dx40", total), "component", component),
			tenant("lodia_controlplane_component_replicas_ready", cp, fmt.Sprintf("%dx40", ready), "component", component),
		)
	}
	mem := func(cp string, total, ready int) batch {
		return join(
			tenant("lodia_controlplane_datastore_members", cp, fmt.Sprintf("%dx40", total)),
			tenant("lodia_controlplane_datastore_members_ready", cp, fmt.Sprintf("%dx40", ready)),
		)
	}
	return []testCase{
		{
			name: alert + " on each side of its thresholds and exclusions",
			series: join(
				comp("api", "apiServer", 3, 2), mem("api", 3, 3),
				comp("etcd", "apiServer", 3, 3), mem("etcd", 3, 2),
				comp("single", "apiServer", 1, 0), mem("single", 1, 1),
				comp("quorum", "apiServer", 3, 3), mem("quorum", 3, 1),
				mem("pair", 2, 1),
				comp("held", "controllerManager", 0, 0), mem("held", 1, 1),
				comp("full", "apiServer", 3, 3), comp("full", "scheduler", 2, 2), mem("full", 3, 3),
				comp("paused", "apiServer", 3, 2), mem("paused", 3, 2), reason("paused", "Progressing", "Paused", "1x40"),
				comp("deleting", "apiServer", 3, 2), mem("deleting", 3, 2), reason("deleting", "Ready", "Deleting", "1x40"),
				join(
					comp("api", "apiServer", 3, 2), mem("api", 3, 3),
					reason("api", "Progressing", "Paused", "1x40"), reason("api", "Ready", "Deleting", "1x40"),
				).in("tenants-b"),
			),
			checks: []check{
				{870, alert, nil},
				{900, alert, []alertLabels{tenantAlert("api", "component", "apiServer"), tenantAlert("etcd")}},
			},
		},
		failover(alert, tenantAlert("a", "component", "apiServer"), 900,
			tenant("lodia_controlplane_component_replicas", "a", "3", "component", "apiServer"),
			tenant("lodia_controlplane_component_replicas_ready", "a", "2", "component", "apiServer"),
		),
	}
}

func rolloutStuck() []testCase {
	const alert = "LodiaRolloutStuck"
	threeMembers := func(cp string, ready int, updated ...int) batch {
		b := join(
			tenant("lodia_controlplane_datastore_members", cp, "3x130"),
			tenant("lodia_controlplane_datastore_members_ready", cp, fmt.Sprintf("%dx130", ready)),
		)
		for _, u := range updated {
			b = join(b, tenant("lodia_controlplane_datastore_members_updated", cp, fmt.Sprintf("%dx130", u)))
		}
		return b
	}
	return []testCase{
		{
			name: alert + " on each side of its thresholds and exclusions",
			series: join(
				threeMembers("etcd", 3, 2),
				threeMembers("unreported", 3),
				threeMembers("apiserver", 3, 3), reason("apiserver", "APIServerReady", "ProgressDeadlineExceeded", "1x130"),
				threeMembers("down", 2, 2),
				threeMembers("done", 3, 3), reason("done", "APIServerReady", "Ready", "1x130"),
				threeMembers("slot", 3, 2), tenant(slotMetric, "slot", "0x130"),
				threeMembers("upgrading", 3, 2), tenant(upgradeMetric, "upgrading", "0x130", "step", "APIServer"),
				threeMembers("paused", 3, 2), reason("paused", "Progressing", "Paused", "1x130"),
				threeMembers("deleting", 3, 2), reason("deleting", "Ready", "Deleting", "1x130"),
				join(
					threeMembers("unreported", 3, 3), tenant(slotMetric, "unreported", "0x130"),
					tenant(upgradeMetric, "unreported", "0x130", "step", "APIServer"),
					reason("unreported", "Progressing", "Paused", "1x130"), reason("unreported", "Ready", "Deleting", "1x130"),
				).in("tenants-b"),
			),
			checks: []check{
				{3570, alert, nil},
				{3600, alert, []alertLabels{tenantAlert("etcd"), tenantAlert("unreported"), tenantAlert("apiserver")}},
			},
		},
		failover(alert, tenantAlert("a"), 3600,
			tenant("lodia_controlplane_datastore_members_ready", "a", "3"),
			tenant("lodia_controlplane_datastore_members_updated", "a", "2"),
		),
	}
}

func certificateExpiring() []testCase {
	const alert = "LodiaCertificateExpiring"
	const expiration = "lodia_controlplane_certificates_expiration_timestamp_seconds"
	const soon = 14*86400 + 3600
	return []testCase{
		{
			name: alert + " on each side of 14 days, also while paused",
			series: join(
				tenant(expiration, "expiring", fmt.Sprintf("%dx250", soon)),
				tenant(expiration, "paused", fmt.Sprintf("%dx250", soon)), reason("paused", "Progressing", "Paused", "1x250"),
				tenant(expiration, "deleting", fmt.Sprintf("%dx250", soon)), reason("deleting", "Ready", "Deleting", "1x250"),
				tenant(expiration, "renewed", fmt.Sprintf("%dx250", soon+3600)),
				join(tenant(expiration, "expiring", fmt.Sprintf("%dx250", soon)), reason("expiring", "Ready", "Deleting", "1x250")).in("tenants-b"),
			),
			checks: []check{{7200, alert, nil}, {7230, alert, []alertLabels{tenantAlert("expiring"), tenantAlert("paused")}}},
		},
		failover(alert, tenantAlert("a"), 3600, tenant(expiration, "a", "0")),
	}
}

func caExpiring() []testCase {
	const alert = "LodiaCAExpiring"
	return []testCase{
		{
			name: alert + " at its for, and never while paused",
			series: join(
				cond("expiring", "CAValidityAcceptable", "False", "1x130"),
				cond("valid", "CAValidityAcceptable", "False", "0x130"),
				cond("paused", "CAValidityAcceptable", "False", "1x130"), reason("paused", "Progressing", "Paused", "1x130"),
				cond("deleting", "CAValidityAcceptable", "False", "1x130"), reason("deleting", "Ready", "Deleting", "1x130"),
				join(
					cond("expiring", "CAValidityAcceptable", "False", "1x130"),
					reason("expiring", "Progressing", "Paused", "1x130"), reason("expiring", "Ready", "Deleting", "1x130"),
				).in("tenants-b"),
			),
			checks: []check{{3570, alert, nil}, {3600, alert, []alertLabels{tenantAlert("expiring")}}},
		},
		failover(alert, tenantAlert("a"), 3600, cond("a", "CAValidityAcceptable", "False", "1")),
	}
}

func backupStale() []testCase {
	const alert = "LodiaBackupStale"
	const period = "lodia_controlplane_datastore_delta_snapshot_period_seconds"
	backup := func(cp, status, cause string, seconds ...int) batch {
		b := join(
			cond(cp, "BackupReady", "True", fmt.Sprintf("%dx380", bit(status == "True"))),
			cond(cp, "BackupReady", "False", fmt.Sprintf("%dx380", bit(status == "False"))),
			cond(cp, "BackupReady", "Unknown", fmt.Sprintf("%dx380", bit(status == "Unknown"))),
			reason(cp, "BackupReady", cause, "1x380"),
			tenant(transitionMetric, cp, "0x380", "type", "BackupReady"),
		)
		for _, s := range seconds {
			b = join(b, tenant(period, cp, fmt.Sprintf("%dx380", s)))
		}
		return b
	}
	return []testCase{
		{
			name: alert + " on each side of three periods, and its exclusions",
			series: join(
				backup("failed", "False", "BackupFailed", 300),
				backup("pending", "Unknown", "SnapshotsPending", 300),
				backup("hourly", "Unknown", "SnapshotsPending", 3600),
				backup("ok", "True", "BackupSucceeded", 300),
				backup("no-store", "False", "NoBackupStore"),
				backup("shared", "False", "Unsupported"),
				backup("no-period", "Unknown", "SnapshotsPending"),
				backup("no-deltas-failed", "False", "BackupFailed"),
				backup("paused", "False", "BackupFailed", 300), reason("paused", "Progressing", "Paused", "1x380"),
				backup("deleting", "False", "BackupFailed", 300), reason("deleting", "Ready", "Deleting", "1x380"),
				backup("other-unknown", "True", "BackupSucceeded", 300),
				reason("other-unknown", "AddonsAvailable", "Reconciling", "1x380"),
				join(
					backup("failed", "False", "BackupFailed", 300),
					reason("failed", "Progressing", "Paused", "1x380"), reason("failed", "Ready", "Deleting", "1x380"),
					backup("no-deltas-failed", "True", "BackupSucceeded", 300),
					backup("ok", "Unknown", "SnapshotsPending"),
				).in("tenants-b"),
			),
			checks: []check{
				{270, alert, nil}, {300, alert, []alertLabels{tenantAlert("failed")}},
				{1200, alert, []alertLabels{tenantAlert("failed")}},
				{1230, alert, []alertLabels{tenantAlert("failed"), tenantAlert("pending")}},
				{11100, alert, []alertLabels{tenantAlert("failed"), tenantAlert("pending")}},
				{11130, alert, []alertLabels{tenantAlert("failed"), tenantAlert("pending"), tenantAlert("hourly")}},
			},
		},
		failover(alert, tenantAlert("a"), 300,
			reason("a", "BackupReady", "BackupFailed", "1"),
			tenant(period, "a", "300"),
		),
	}
}

func etcdSpaceNearQuota() []testCase {
	const alert = "LodiaEtcdSpaceNearQuota"
	const size, quota = "lodia_controlplane_datastore_db_size_bytes", "lodia_controlplane_datastore_quota_bytes"
	return []testCase{
		{
			name: alert + " on each side of 80%",
			series: join(
				tenant(size, "near", "801x40"), tenant(quota, "near", "1000x40"),
				tenant(size, "at", "800x40"), tenant(quota, "at", "1000x40"),
			),
			checks: []check{{870, alert, nil}, {900, alert, []alertLabels{tenantAlert("near")}}},
		},
		failover(alert, tenantAlert("a"), 900, tenant(size, "a", "900"), tenant(quota, "a", "1000")),
	}
}

func etcdNoLeader() []testCase {
	const alert = "LodiaEtcdNoLeader"
	const hasLeader = "lodia_controlplane_datastore_has_leader"
	return []testCase{
		{
			name: alert + " on two samples and never on one",
			series: join(
				tenant(hasLeader, "two", "0 0 0 0 1x10"), tenant(hasLeader, "one", "0 0 1x12"),
				tenant(hasLeader, "ok", "1x14"), tenant("lodia_controlplane_datastore_quota_bytes", "unformed", "1000x14"),
			),
			checks: []check{{30, alert, nil}, {60, alert, []alertLabels{tenantAlert("two")}}, {90, alert, []alertLabels{tenantAlert("two")}}},
		},
		failover(alert, tenantAlert("a"), 300, tenant(hasLeader, "a", "0")),
	}
}

func etcdAlarm() []testCase {
	const alert = "LodiaEtcdAlarm"
	const armed = "lodia_controlplane_datastore_alarm"
	return []testCase{
		{
			name: alert + " on two samples and never on one",
			series: join(
				tenant(armed, "two", "1x14", "alarm", "NOSPACE"), tenant(armed, "one", "1 1 stale", "alarm", "NOSPACE"),
			),
			checks: []check{{30, alert, nil}, {60, alert, []alertLabels{tenantAlert("two", "alarm", "NOSPACE")}}},
		},
		failover(alert, tenantAlert("a", "alarm", "NOSPACE"), 300, tenant(armed, "a", "1", "alarm", "NOSPACE")),
	}
}

func controllerManagerHeld() []testCase {
	const alert = "LodiaControllerManagerHeld"
	return []testCase{
		{
			name: alert + " at its for, and never while paused",
			series: join(
				cond("held", "ControllerManagerHeld", "True", "1x40"),
				cond("not-held", "ControllerManagerHeld", "True", "0x40"),
				cond("paused", "ControllerManagerHeld", "True", "1x40"), reason("paused", "Progressing", "Paused", "1x40"),
				cond("deleting", "ControllerManagerHeld", "True", "1x40"), reason("deleting", "Ready", "Deleting", "1x40"),
				join(
					cond("held", "ControllerManagerHeld", "True", "1x40"),
					reason("held", "Progressing", "Paused", "1x40"), reason("held", "Ready", "Deleting", "1x40"),
				).in("tenants-b"),
			),
			checks: []check{{870, alert, nil}, {900, alert, []alertLabels{tenantAlert("held")}}},
		},
		failover(alert, tenantAlert("a"), 900, cond("a", "ControllerManagerHeld", "True", "1")),
	}
}

func controllerManagerHoldBlocked() []testCase {
	const alert = "LodiaControllerManagerHoldBlocked"
	return []testCase{
		{
			name: alert + " at its for, and never while paused",
			series: join(
				reason("blocked", "ControllerManagerHeld", "HoldBlocked", "1x10"),
				reason("not-held", "ControllerManagerHeld", "NotHeld", "1x10"),
				reason("paused", "ControllerManagerHeld", "HoldBlocked", "1x10"), reason("paused", "Progressing", "Paused", "1x10"),
				reason("deleting", "ControllerManagerHeld", "HoldBlocked", "1x10"), reason("deleting", "Ready", "Deleting", "1x10"),
				join(
					reason("blocked", "ControllerManagerHeld", "HoldBlocked", "1x10"),
					reason("blocked", "Progressing", "Paused", "1x10"), reason("blocked", "Ready", "Deleting", "1x10"),
				).in("tenants-b"),
			),
			checks: []check{{30, alert, nil}, {60, alert, []alertLabels{tenantAlert("blocked")}}},
		},
		failover(alert, tenantAlert("a"), 300, reason("a", "ControllerManagerHeld", "HoldBlocked", "1")),
	}
}

func upgradeFailed() []testCase {
	const alert = "LodiaUpgradeFailed"
	return []testCase{
		{
			name: alert + " at once, on either failure",
			series: join(
				reason("failed", "Progressing", "Failed", "1x10"),
				reason("maintenance", "Progressing", "MaintenanceFailed", "1x10"),
				reason("upgrading", "Progressing", "Upgrading", "1x10"),
				reason("deleting", "Progressing", "Failed", "1x10"), reason("deleting", "Ready", "Deleting", "1x10"),
				join(reason("failed", "Progressing", "Failed", "1x10"), reason("failed", "Ready", "Deleting", "1x10")).in("tenants-b"),
			),
			checks: []check{{0, alert, []alertLabels{tenantAlert("failed"), tenantAlert("maintenance")}}},
		},
		failover(alert, tenantAlert("a"), 300, reason("a", "Progressing", "Failed", "1")),
	}
}

func upgradeStalled() []testCase {
	const alert = "LodiaUpgradeStalled"
	return []testCase{
		{
			name: alert + " past 2 hours, across a step, and its exclusions",
			series: join(
				tenant(upgradeMetric, "stalled", "0x244 stale", "step", "Scheduler"),
				tenant(upgradeMetric, "stalled", "_x245 0x20", "step", "Addons"),
				reason("stalled", "Progressing", "Upgrading", "1x265"),
				tenant(upgradeMetric, "failed", "0x265", "step", "Addons"), reason("failed", "Progressing", "Failed", "1x265"),
				tenant(upgradeMetric, "maintenance", "0x265", "step", "Addons"),
				reason("maintenance", "Progressing", "MaintenanceFailed", "1x265"),
				tenant(upgradeMetric, "paused", "0x265", "step", "Addons"), reason("paused", "Progressing", "Paused", "1x265"),
				tenant(upgradeMetric, "deleting", "0x265", "step", "Addons"),
				reason("deleting", "Progressing", "Upgrading", "1x265"), reason("deleting", "Ready", "Deleting", "1x265"),
				join(
					tenant(upgradeMetric, "stalled", "0x265", "step", "Addons"),
					reason("stalled", "Progressing", "Paused", "1x265"), reason("stalled", "Ready", "Deleting", "1x265"),
				).in("tenants-b"),
			),
			checks: []check{
				{7200, alert, nil}, {7230, alert, []alertLabels{tenantAlert("stalled")}},
				{7500, alert, []alertLabels{tenantAlert("stalled")}},
			},
		},
		failover(alert, tenantAlert("a"), 7500, tenant(upgradeMetric, "a", "0", "step", "Addons")),
	}
}

func maintenanceHalted() []testCase {
	const alert = "LodiaMaintenanceHalted"
	const fleet = "lodia_fleet_max_concurrent_rollouts"
	halted := func(name string, fires bool, tenants ...batch) testCase {
		var want []alertLabels
		if fires {
			want = []alertLabels{alertOf()}
		}
		return testCase{
			name:   alert + " at N = 2 " + name,
			series: join(operator(fleet, "2x40"), join(tenants...)),
			checks: []check{{870, alert, nil}, {900, alert, want}},
		}
	}
	return []testCase{
		halted("fires on MaintenanceFailed and Unsupported", true, slot("failed", "MaintenanceFailed", 40), slot("other", "Unsupported", 40),
			reason("other", "Progressing", "TimeZoneUnsupported", "1x40").in("tenants-b")),
		halted("fires on MaintenanceFailed and Paused", true, slot("failed", "MaintenanceFailed", 40), slot("other", "Paused", 40)),
		halted("stays silent on MaintenanceFailed and Maintaining", false, slot("failed", "MaintenanceFailed", 40), slot("other", "Maintaining", 40)),
		halted("stays silent on MaintenanceFailed and TimeZoneUnsupported", false, slot("failed", "MaintenanceFailed", 40), slot("other", "TimeZoneUnsupported", 40)),
		halted("stays silent on one tenant in its slot and one without a slot", false, slot("failed", "MaintenanceFailed", 40), reason("other", "Progressing", "Idle", "1x40")),
		failover(alert, alertOf(), 900,
			operator(fleet, "2"),
			tenant(slotMetric, "failed", "0"), reason("failed", "Progressing", "MaintenanceFailed", "1"),
			tenant(slotMetric, "other", "0"), reason("other", "Progressing", "Unsupported", "1"),
		),
	}
}

func slot(cp, cause string, n int) batch {
	return join(
		reason(cp, "Progressing", cause, fmt.Sprintf("1x%d", n)),
		tenant(slotMetric, cp, fmt.Sprintf("0x%d", n)),
	)
}

func maintenanceSlotHeldLong() []testCase {
	const alert = "LodiaMaintenanceSlotHeldLong"
	return []testCase{
		{
			name: alert + " past an hour, but for MaintenanceFailed",
			series: join(
				slot("maintaining", "Maintaining", 130), slot("paused", "Paused", 130),
				slot("unsupported", "Unsupported", 130), slot("failed", "MaintenanceFailed", 130),
				slot("maintaining", "MaintenanceFailed", 130).in("tenants-b"),
			),
			checks: []check{
				{3600, alert, nil},
				{3630, alert, []alertLabels{tenantAlert("maintaining"), tenantAlert("paused"), tenantAlert("unsupported")}},
			},
		},
		failover(alert, tenantAlert("a"), 3900, tenant(slotMetric, "a", "0")),
	}
}

func reconcileErrors() []testCase {
	const alert = "LodiaReconcileErrors"
	const failures, total = "controller_runtime_reconcile_errors_total", "controller_runtime_reconcile_total"
	datastore := operatorAlert("controller", "datastore")
	reconciles := func(controller string, failed, succeeded int, prefix string, n int) batch {
		return join(
			operator(failures, fmt.Sprintf("%s0+%dx%d", prefix, failed, n), "controller", controller),
			operator(total, fmt.Sprintf("%s0+%dx%d", prefix, failed, n), "controller", controller, "result", "error"),
			operator(total, fmt.Sprintf("%s0+%dx%d", prefix, succeeded, n), "controller", controller, "result", "success"),
		)
	}
	return []testCase{
		{
			name: alert + " on each side of 5%, and without reconciles",
			series: join(
				reconciles("datastore", 6, 94, "", 60), reconciles("pki", 4, 96, "", 60), reconciles("tenant", 0, 0, "", 60),
			),
			checks: []check{
				{900, alert, nil}, {930, alert, []alertLabels{datastore}}, {1500, alert, []alertLabels{datastore}},
			},
		},
		{
			name: alert + " reads 15 minutes of reconciles",
			series: join(
				operator(failures, "0x20 6+6x80", "controller", "datastore"),
				operator(total, "0x20 6+6x80", "controller", "datastore", "result", "error"),
				operator(total, "0+100x20 2094+94x80", "controller", "datastore", "result", "success"),
			),
			checks: []check{{1800, alert, nil}, {2400, alert, []alertLabels{datastore}}},
		},
		{
			name: alert + " fires throughout a failover",
			series: join(
				operator(failures, "0+6x40 stale", "controller", "datastore"),
				operator(total, "0+6x40 stale", "controller", "datastore", "result", "error"),
				operator(total, "0+94x40 stale", "controller", "datastore", "result", "success"),
				reconciles("datastore", 6, 94, "_x48 ", 40).on(podB),
			),
			checks: throughout(alert, datastore, 930, 1200, 1230, 1410, 1440, 1800),
		},
	}
}

func healthSamplingLate() []testCase {
	const alert = "LodiaHealthSamplingLate"
	const lag = "lodia_health_nodes_sample_lag_seconds"
	histogram := func(count, le5, le10 string) batch {
		return join(
			operator(lag+"_bucket", le5, "le", "5.0"),
			operator(lag+"_bucket", le10, "le", "10.0"),
			operator(lag+"_bucket", count, "le", "+Inf"),
			operator(lag+"_count", count),
		)
	}
	late := operatorAlert()
	return []testCase{
		{
			name:   alert + " on one pass more than 10 seconds late, for 10 minutes",
			series: histogram("100x9 101x60", "100x70", "100x70"),
			checks: []check{
				{270, alert, nil}, {300, alert, []alertLabels{late}}, {840, alert, []alertLabels{late}},
				{1080, alert, []alertLabels{late}}, {1800, alert, nil},
			},
		},
		{
			name:   alert + " stays silent on passes 10 seconds late and less",
			series: histogram("100x9 102x60", "100x9 101x60", "100x9 102x60"),
			checks: []check{{300, alert, nil}, {600, alert, nil}},
		},
		{
			name: alert + " stays silent when the histogram starts again at 0 in another pod",
			series: join(
				histogram("105x10 stale", "100x10 stale", "100x10 stale"),
				histogram("_x18 0+1x30", "_x18 0+1x30", "_x18 0+1x30").on(podB),
			),
			checks: silent(alert, 300, 540, 600, 900, 1200),
		},
		{
			name: alert + " fires throughout a failover",
			series: join(
				histogram("100x9 101x2 stale", "100x12 stale", "100x12 stale"),
				histogram("_x20 0x20", "_x20 0x20", "_x20 0x20").on(podB),
			),
			checks: throughout(alert, late, 300, 390, 420, 570, 600, 660),
		},
	}
}
