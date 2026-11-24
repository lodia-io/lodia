// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package restore_test

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/restore"
)

const (
	sourceUID   types.UID = "5e1f7c3a-0000-4000-8000-000000000001"
	restoredUID types.UID = "5e1f7c3a-0000-4000-8000-000000000002"
	againUID    types.UID = "5e1f7c3a-0000-4000-8000-000000000003"
)

func controlPlane(name string, uid types.UID, source *lodiav1alpha1.UIDReference) *lodiav1alpha1.ControlPlane {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", UID: uid}}
	if source != nil {
		cp.Status.Restore = &lodiav1alpha1.RestoreStatus{
			Backup:      lodiav1alpha1.UIDReference{Name: "a-1", UID: "5e1f7c3a-0000-4000-8000-000000000009"},
			Source:      *source,
			CompletedAt: metav1.Date(2026, 11, 2, 18, 0, 0, 0, time.UTC),
		}
	}
	return cp
}

func reader(t *testing.T, objects ...client.Object) client.Reader {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func TestTheSourceIsFoundByItsUID(t *testing.T) {
	source := &lodiav1alpha1.UIDReference{Name: "a", UID: sourceUID}
	for _, c := range []struct {
		name    string
		cp      *lodiav1alpha1.ControlPlane
		objects []client.Object
		want    types.UID
	}{
		{
			name:    "the source under another name's restore",
			cp:      controlPlane("b", restoredUID, source),
			objects: []client.Object{controlPlane("a", sourceUID, nil), controlPlane("b", restoredUID, source)},
			want:    sourceUID,
		},
		{
			name:    "a restore under its source's name",
			cp:      controlPlane("a", againUID, source),
			objects: []client.Object{controlPlane("a", againUID, source)},
		},
		{
			name:    "a successor of the source's name beside another restore",
			cp:      controlPlane("b", restoredUID, source),
			objects: []client.Object{controlPlane("a", againUID, source), controlPlane("b", restoredUID, source)},
		},
		{
			name:    "no completed restore",
			cp:      controlPlane("b", restoredUID, nil),
			objects: []client.Object{controlPlane("a", sourceUID, nil), controlPlane("b", restoredUID, nil)},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := restore.Source(t.Context(), reader(t, c.objects...), c.cp)
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case c.want == "" && got != nil:
				t.Fatalf("source %s/%s (UID %s), want none", got.Namespace, got.Name, got.UID)
			case c.want != "" && (got == nil || got.UID != c.want):
				t.Fatalf("source %+v, want UID %s", got, c.want)
			}
		})
	}
}
