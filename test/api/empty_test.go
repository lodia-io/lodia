// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var controlPlaneFilled = map[string]any{
	"spec.version":                             "v1.37.1",
	"spec.addons.konnectivity.version":         "v0.34.0",
	"spec.addons.kubeProxy.healthzBindAddress": "::",
	"spec.apiServer.advertiseAddress":          "192.0.2.10",
	"spec.datastore.className":                 "fast-ssd",
	"spec.endpoint.loadBalancer.class":         "example.com/load-balancer",
	"spec.maintenance.window.begin":            "02:00",
	"spec.maintenance.window.end":              "04:00",
	"spec.maintenance.window.timeZone":         "Europe/Kyiv",
	"spec.placement.priorityClassName":         "tenant-control-plane",
	"spec.serviceAccount.issuer":               "https://issuer.example.test",
}

var classFilled = map[string]any{
	"spec.backup.fullSnapshotSchedule":   "0 */24 * * *",
	"spec.backup.store.bucket":           "lodia-backups",
	"spec.backup.store.endpoint":         "https://s3.eu-central-1.amazonaws.com",
	"spec.backup.store.prefix":           "prod",
	"spec.backup.store.region":           "eu-central-1",
	"spec.etcd.autoCompaction.retention": "15m",
	"spec.etcd.defragmentation.schedule": "0 3 * * *",
	"spec.placement.priorityClassName":   "tenant-etcd",
	"spec.storage.className":             "local-nvme",
}

var controlPlaneStatusFilled = map[string]any{
	"addons": map[string]any{"version": "v1.37.1", "operatorRevision": "v0.1.0+abc"},
	"components": map[string]any{
		"apiServer":         map[string]any{"total": int64(1), "ready": int64(1), "version": "v1.37.1", "operatorRevision": "v0.1.0+abc"},
		"controllerManager": map[string]any{"total": int64(1), "ready": int64(1), "version": "v1.37.1", "operatorRevision": "v0.1.0+abc"},
		"scheduler":         map[string]any{"total": int64(1), "ready": int64(1), "version": "v1.37.1", "operatorRevision": "v0.1.0+abc"},
	},
	"datastore": map[string]any{
		"endpoints":        []any{map[string]any{"host": "dev-etcd-client.team-a.svc", "port": int64(2379)}},
		"members":          map[string]any{"total": int64(1), "ready": int64(1)},
		"operatorRevision": "v0.1.0+abc",
		"snapshots":        map[string]any{"provider": "S3", "bucket": "lodia-backups", "prefix": "prod", "endpoint": "https://s3.eu-central-1.amazonaws.com"},
	},
	"kubernetes":  map[string]any{"version": "v1.37.1"},
	"maintenance": map[string]any{"lastHandled": "v0.1.0+abc"},
	"nodes":       map[string]any{"total": int64(2), "ready": int64(2), "oldestKubelet": "v1.37.1"},
	"target":      map[string]any{"version": "v1.37.1", "operatorRevision": "v0.1.0+abc"},
}

var backupStatusFilled = map[string]any{
	"kubernetesVersion": "v1.37.1",
	"snapshots":         map[string]any{"provider": "S3", "bucket": "lodia-backups", "prefix": "prod", "endpoint": "https://s3.eu-central-1.amazonaws.com"},
}

// TestAnEmptyValueIsRefusedWhereLeavingTheFieldOutMeansTheSame refuses "" and {} on every optional string and object whose empty value is no choice of its own.
func TestAnEmptyValueIsRefusedWhereLeavingTheFieldOutMeansTheSame(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	filled := func(t *testing.T, file, name string, fields map[string]any) *unstructured.Unstructured {
		obj := example(t, file, name)
		for path, value := range fields {
			set(t, obj, value, strings.Split(path, ".")...)
		}
		return obj
	}
	type row struct {
		path  string
		empty any
	}
	for _, c := range []struct {
		file   string
		fields map[string]any
		rows   []row
	}{
		{"controlplane-single.yaml", controlPlaneFilled, []row{
			{"spec.version", ""}, {"spec.addons.konnectivity.version", ""}, {"spec.addons.kubeProxy.healthzBindAddress", ""},
			{"spec.apiServer.advertiseAddress", ""}, {"spec.datastore", map[string]any{}}, {"spec.datastore.className", ""},
			{"spec.endpoint", map[string]any{}}, {"spec.endpoint.loadBalancer.class", ""}, {"spec.maintenance.window.begin", ""},
			{"spec.maintenance.window.end", ""}, {"spec.maintenance.window.timeZone", ""}, {"spec.placement.priorityClassName", ""},
			{"spec.serviceAccount.issuer", ""},
		}},
		{"datastoreclass.yaml", classFilled, []row{
			{"spec.backup.fullSnapshotSchedule", ""}, {"spec.backup.store.bucket", ""}, {"spec.backup.store.endpoint", ""},
			{"spec.backup.store.prefix", ""}, {"spec.backup.store.region", ""}, {"spec.etcd.autoCompaction.retention", ""},
			{"spec.etcd.defragmentation.schedule", ""}, {"spec.placement.priorityClassName", ""}, {"spec.storage.className", ""},
		}},
	} {
		t.Run(c.file, func(t *testing.T) {
			expect(t, k8s.Create(context.Background(), filled(t, c.file, "filled", c.fields), client.DryRunAll), "")
			for _, r := range c.rows {
				t.Run(r.path, func(t *testing.T) {
					obj := filled(t, c.file, "empty", c.fields)
					set(t, obj, r.empty, strings.Split(r.path, ".")...)
					expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), emptyRefused(r.path, r.empty))
				})
			}
		})
	}
	for _, c := range []struct {
		file   string
		status map[string]any
		rows   []row
	}{
		{"controlplane-single.yaml", controlPlaneStatusFilled, []row{
			{"status", map[string]any{}}, {"status.addons.operatorRevision", ""}, {"status.addons.version", ""},
			{"status.components.apiServer.operatorRevision", ""}, {"status.components.apiServer.version", ""},
			{"status.components.controllerManager.operatorRevision", ""}, {"status.components.controllerManager.version", ""},
			{"status.components.scheduler.operatorRevision", ""}, {"status.components.scheduler.version", ""},
			{"status.datastore.operatorRevision", ""}, {"status.datastore.snapshots.endpoint", ""}, {"status.kubernetes.version", ""},
			{"status.maintenance.lastHandled", ""}, {"status.nodes.oldestKubelet", ""}, {"status.target.operatorRevision", ""},
			{"status.target.version", ""},
		}},
		{"backup.yaml", backupStatusFilled, []row{
			{"status", map[string]any{}}, {"status.kubernetesVersion", ""}, {"status.snapshots.endpoint", ""},
		}},
		{"kubeconfigrequest.yaml", map[string]any{"kubeconfig": "a2luZDogQ29uZmlnCg=="}, []row{
			{"status", map[string]any{}},
		}},
	} {
		t.Run(c.file+"/status", func(t *testing.T) {
			obj := example(t, c.file, "status-filled")
			if err := k8s.Create(context.Background(), obj); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
			written := obj.DeepCopy()
			set(t, written, c.status, "status")
			expect(t, k8s.Status().Update(context.Background(), written, client.DryRunAll), "")
			for _, r := range c.rows {
				t.Run(r.path, func(t *testing.T) {
					written := obj.DeepCopy()
					set(t, written, c.status, "status")
					set(t, written, r.empty, strings.Split(r.path, ".")...)
					expect(t, k8s.Status().Update(context.Background(), written, client.DryRunAll), emptyRefused(r.path, r.empty))
				})
			}
		})
	}
}

func emptyRefused(path string, empty any) string {
	inBody := path
	if rest, ok := strings.CutPrefix(path, "status"); ok {
		inBody = strings.TrimPrefix(rest, ".")
	}
	if empty == "" {
		return path + `: Invalid value: "": ` + inBody + " in body should be at least 1 chars long"
	}
	return path + ": Invalid value: 0: " + inBody + " in body should have at least 1 properties"
}
