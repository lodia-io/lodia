// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant_test

import (
	"os"
	"runtime"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const copiesOfATenantAtMost = 64 << 10

func TestMeasureWhatTheCopiesOfATenantsAccessSecretsHold(t *testing.T) {
	if os.Getenv("LODIA_MEASURE_HEAP") == "" {
		t.Skip("LODIA_MEASURE_HEAP is not set")
	}
	cp := controlPlane(t, "tenant-measure-copies", gatewayMode)
	publishGateway(t, cp)
	reconcilePKI(t, gatewayIssuer(), cp)
	var list corev1.SecretList
	if err := k8s.List(t.Context(), &list, client.InNamespace(cp.Namespace)); err != nil {
		t.Fatal(err)
	}
	held := map[string]*corev1.Secret{}
	for i := range list.Items {
		held[list.Items[i].Name] = &list.Items[i]
	}
	var kept []*corev1.Secret
	for _, name := range []string{"dev-ca-bundle", "dev-lodia-kubeconfig", "dev-tunnel-client", "dev-etcd-client-1", "dev-etcd-ca-1"} {
		secret := held[name]
		if secret == nil {
			t.Fatalf("PKI wrote no Secret %s", name)
		}
		kept = append(kept, secret)
	}
	const tenants = 200
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	copies := make([]*corev1.Secret, 0, tenants*len(kept))
	for range tenants {
		for _, secret := range kept {
			copies = append(copies, secret.DeepCopy())
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(copies)
	held1 := (int64(after.HeapAlloc) - int64(before.HeapAlloc)) / tenants
	t.Logf("the copies of the %d Secrets of a tenant hold %d bytes of live heap, over %d tenants", len(kept), held1, tenants)
	if held1 > copiesOfATenantAtMost {
		t.Errorf("the copies of a tenant hold %d bytes, want at most %d", held1, copiesOfATenantAtMost)
	}
}
