// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid

import (
	"context"
	"slices"
	"testing"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type etcdReader struct {
	client.Reader
	etcd *druidv1alpha1.Etcd
}

func (r etcdReader) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if r.etcd == nil {
		return apierrors.NewNotFound(schema.GroupResource{Group: druidv1alpha1.SchemeGroupVersion.Group, Resource: "etcds"}, key.Name)
	}
	r.etcd.DeepCopyInto(obj.(*druidv1alpha1.Etcd))
	return nil
}

func TestHeldEmptyNamesAManagerThatHoldsAnObjectTheApplyEmpties(t *testing.T) {
	const ours = "lodia/datastore"
	entry := func(manager string, operation metav1.ManagedFieldsOperationType, fields string) metav1.ManagedFieldsEntry {
		return metav1.ManagedFieldsEntry{Manager: manager, Operation: operation, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(fields)}
	}
	lodia := entry(ours, metav1.ManagedFieldsOperationApply, `{"f:spec":{"f:sharedConfig":{"f:autoCompactionMode":{}}}}`)
	kubectl := entry("kubectl", metav1.ManagedFieldsOperationApply, `{"f:spec":{"f:sharedConfig":{}}}`)
	for _, c := range []struct {
		name    string
		entries []metav1.ManagedFieldsEntry
		want    []string
	}{
		{"another manager holds it empty", []metav1.ManagedFieldsEntry{lodia, kubectl, entry("etcd-druid", metav1.ManagedFieldsOperationUpdate, `{"f:status":{"f:ready":{}}}`)},
			[]string{"spec.sharedConfig, which field manager kubectl holds"}},
		{"two managers hold it empty", []metav1.ManagedFieldsEntry{lodia, kubectl, entry("argocd", metav1.ManagedFieldsOperationApply, `{"f:spec":{"f:sharedConfig":{}}}`)},
			[]string{"spec.sharedConfig, which field managers argocd and kubectl hold"}},
		{"one manager holds it by apply and by update", []metav1.ManagedFieldsEntry{lodia, kubectl, entry("kubectl", metav1.ManagedFieldsOperationUpdate, `{"f:spec":{"f:sharedConfig":{}}}`)},
			[]string{"spec.sharedConfig, which field manager kubectl holds"}},
		{"Lodia's apply holds nothing in it", []metav1.ManagedFieldsEntry{kubectl}, nil},
		{"only Lodia's update holds fields in it", []metav1.ManagedFieldsEntry{entry(ours, metav1.ManagedFieldsOperationUpdate, `{"f:spec":{"f:sharedConfig":{"f:autoCompactionMode":{}}}}`), kubectl}, nil},
		{"another manager holds a field in it", []metav1.ManagedFieldsEntry{lodia, kubectl, entry("gitops", metav1.ManagedFieldsOperationApply, `{"f:spec":{"f:sharedConfig":{"f:autoCompactionRetention":{}}}}`)}, nil},
		{"no other manager holds it", []metav1.ManagedFieldsEntry{lodia}, nil},
		{"no Etcd", nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			rendered := &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Name: "dev-etcd", Namespace: "team-a"}}
			reader := etcdReader{}
			if c.entries != nil {
				reader.etcd = rendered.DeepCopy()
				reader.etcd.ManagedFields = c.entries
			}
			got, err := (&Engine{}).HeldEmpty(t.Context(), reader, ours, &Store{etcd: rendered})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, c.want) {
				t.Errorf("HeldEmpty names %q, want %q", got, c.want)
			}
		})
	}
}
