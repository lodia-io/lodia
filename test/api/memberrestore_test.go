// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestTheRecordOfARestoredMemberAndItsReportsOnlyMoveForward(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const (
		earlier = "2026-11-26T10:00:00Z"
		at      = "2026-11-26T10:00:05Z"
		later   = "2026-11-26T10:00:06Z"
	)
	record := func(detectedAt string) map[string]any {
		return map[string]any{"member": "dev-etcd-0", "detectedAt": detectedAt, "from": "Snapshots"}
	}
	component := func(detectedAt string) map[string]any {
		c := map[string]any{"total": int64(1), "ready": int64(1)}
		if detectedAt != "" {
			c["memberRestoreDetectedAt"] = detectedAt
		}
		return c
	}
	components := func(apiServer, controllerManager, scheduler string) map[string]any {
		return map[string]any{"apiServer": component(apiServer), "controllerManager": component(controllerManager), "scheduler": component(scheduler)}
	}
	stored := map[string]any{"memberRestore": record(at), "components": components(at, at, at)}
	without := func(path ...string) map[string]any {
		status := runtime.DeepCopyJSONValue(stored).(map[string]any)
		unstructured.RemoveNestedField(status, path...)
		return status
	}
	with := func(value any, path ...string) map[string]any {
		status := runtime.DeepCopyJSONValue(stored).(map[string]any)
		if err := unstructured.SetNestedField(status, value, path...); err != nil {
			t.Fatal(err)
		}
		return status
	}
	for i, c := range []struct {
		name   string
		status map[string]any
		want   string
	}{
		{"the same", stored, ""},
		{"a later record", with(record(later), "memberRestore"), ""},
		{"a record reported", with(earlier, "memberRestore", "reportedAt"), ""},
		{"the record removed", without("memberRestore"), "memberRestore cannot be removed once set"},
		{"the record moved back", with(record(earlier), "memberRestore"), "memberRestore.detectedAt cannot move back"},
		{"a later report", with(components(later, later, later), "components"), ""},
		{"the components removed", without("components"), "components.apiServer.memberRestoreDetectedAt cannot be removed once set"},
		{"the API server's removed", without("components", "apiServer"), "components.apiServer.memberRestoreDetectedAt cannot be removed once set"},
		{"the API server's report removed", with(components("", at, at), "components"), "components.apiServer.memberRestoreDetectedAt cannot be removed once set"},
		{"the controller manager's report removed", with(components(at, "", at), "components"), "components.controllerManager.memberRestoreDetectedAt cannot be removed once set"},
		{"the scheduler's report removed", with(components(at, at, ""), "components"), "components.scheduler.memberRestoreDetectedAt cannot be removed once set"},
		{"the API server's report moved back", with(components(earlier, at, at), "components"), "memberRestoreDetectedAt cannot move back"},
		{"the controller manager's report moved back", with(components(at, earlier, at), "components"), "memberRestoreDetectedAt cannot move back"},
		{"the scheduler's report moved back", with(components(at, at, earlier), "components"), "memberRestoreDetectedAt cannot move back"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			obj := example(t, "controlplane-single.yaml", fmt.Sprintf("member-restore-%d", i))
			if err := k8s.Create(ctx, obj); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
			obj.Object["status"] = runtime.DeepCopyJSONValue(stored)
			if err := k8s.Status().Update(ctx, obj); err != nil {
				t.Fatal(err)
			}
			obj.Object["status"] = c.status
			expect(t, k8s.Status().Update(ctx, obj), c.want)
		})
	}
}
