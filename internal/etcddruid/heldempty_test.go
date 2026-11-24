// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
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

func refusal(errs ...*field.Error) error {
	errs = append(errs, field.Invalid(nil, nil, "some validation rules were not checked because the object was invalid; correct the existing errors to complete validation"))
	return fmt.Errorf("apply Etcd dev-etcd: %w", apierrors.NewInvalid(schema.GroupKind{Group: druidv1alpha1.SchemeGroupVersion.Group, Kind: "Etcd"}, "dev-etcd", errs))
}

func null(path ...string) *field.Error {
	return field.TypeInvalid(field.NewPath(path[0], path[1:]...), "null", path[len(path)-1]+` in body must be of type object: "null"`)
}

func TestHeldEmptyNamesTheManagersThatHoldAnObjectTheRefusalFindsNull(t *testing.T) {
	const ours = "lodia/datastore"
	entry := func(manager string, operation metav1.ManagedFieldsOperationType, fields string) metav1.ManagedFieldsEntry {
		return metav1.ManagedFieldsEntry{Manager: manager, Operation: operation, FieldsType: "FieldsV1", FieldsV1: metav1.NewFieldsV1(fields)}
	}
	lodia := entry(ours, metav1.ManagedFieldsOperationApply, `{"f:spec":{"f:sharedConfig":{"f:autoCompactionMode":{}},"f:etcd":{"f:resources":{"f:requests":{"f:cpu":{}},"f:limits":{"f:cpu":{}}}}}}`)
	applied := entry("kubectl", metav1.ManagedFieldsOperationApply, `{"f:spec":{"f:sharedConfig":{}}}`)
	sharedConfig := refusal(null("spec", "sharedConfig"))
	schedule := field.Invalid(field.NewPath("spec", "etcd", "defragmentationSchedule"), "0 3 * * 9", "does not match")
	const patch = `the JSON patch [{"op":"remove","path":"/spec/sharedConfig"}] removes it`
	for _, c := range []struct {
		name      string
		entries   []metav1.ManagedFieldsEntry
		refused   error
		want      []string
		explained bool
	}{
		{"another manager holds it by apply", []metav1.ManagedFieldsEntry{lodia, applied, entry("etcd-druid", metav1.ManagedFieldsOperationUpdate, `{"f:status":{"f:ready":{}}}`)}, sharedConfig,
			[]string{"spec.sharedConfig, which the apply of field manager kubectl holds until kubectl applies without it"}, true},
		{"two managers hold it by apply", []metav1.ManagedFieldsEntry{lodia, applied, entry("argocd", metav1.ManagedFieldsOperationApply, `{"f:spec":{"f:sharedConfig":{}}}`)}, sharedConfig,
			[]string{"spec.sharedConfig, which the apply of field manager argocd and the apply of field manager kubectl hold until each applies without it"}, true},
		{"another manager holds it by update", []metav1.ManagedFieldsEntry{lodia, entry("kubectl-patch", metav1.ManagedFieldsOperationUpdate, `{"f:spec":{"f:sharedConfig":{}}}`)}, sharedConfig,
			[]string{"spec.sharedConfig, which the update of field manager kubectl-patch holds until " + patch}, true},
		{"one manager holds it by update and by apply", []metav1.ManagedFieldsEntry{lodia, entry("kubectl", metav1.ManagedFieldsOperationUpdate, `{"f:spec":{"f:sharedConfig":{}}}`), applied}, sharedConfig,
			[]string{"spec.sharedConfig, which the apply of field manager kubectl and the update of field manager kubectl hold until " + patch}, true},
		{"one manager holds it by update at two versions", []metav1.ManagedFieldsEntry{lodia, entry("kubectl-edit", metav1.ManagedFieldsOperationUpdate, `{"f:spec":{"f:sharedConfig":{}}}`), entry("kubectl-edit", metav1.ManagedFieldsOperationUpdate, `{"f:spec":{"f:sharedConfig":{}}}`)}, sharedConfig,
			[]string{"spec.sharedConfig, which the update of field manager kubectl-edit holds until " + patch}, true},
		{"a nested object", []metav1.ManagedFieldsEntry{lodia, entry("kubectl", metav1.ManagedFieldsOperationApply, `{"f:spec":{"f:etcd":{"f:resources":{"f:requests":{}}}}}`)}, refusal(null("spec", "etcd", "resources", "requests")),
			[]string{"spec.etcd.resources.requests, which the apply of field manager kubectl holds until kubectl applies without it"}, true},
		{"a refusal with another cause too", []metav1.ManagedFieldsEntry{lodia, applied}, refusal(null("spec", "sharedConfig"), schedule),
			[]string{"spec.sharedConfig, which the apply of field manager kubectl holds until kubectl applies without it"}, false},
		{"a refusal for another cause while another manager holds an object empty", []metav1.ManagedFieldsEntry{lodia, applied}, refusal(schedule), nil, false},
		{"a refusal of another type at the object", []metav1.ManagedFieldsEntry{lodia, applied}, refusal(field.TypeInvalid(field.NewPath("spec", "sharedConfig"), "x", `spec.sharedConfig in body must be of type object: "string"`)), nil, false},
		{"a refusal without causes", []metav1.ManagedFieldsEntry{lodia, applied}, apierrors.NewForbidden(schema.GroupResource{Group: druidv1alpha1.SchemeGroupVersion.Group, Resource: "etcds"}, "dev-etcd", errors.New("denied by a policy")), nil, false},
		{"Lodia's apply holds nothing in it", []metav1.ManagedFieldsEntry{applied}, sharedConfig, nil, false},
		{"only Lodia's update holds fields in it", []metav1.ManagedFieldsEntry{entry(ours, metav1.ManagedFieldsOperationUpdate, `{"f:spec":{"f:sharedConfig":{"f:autoCompactionMode":{}}}}`), applied}, sharedConfig, nil, false},
		{"another manager holds a field in it", []metav1.ManagedFieldsEntry{lodia, applied, entry("gitops", metav1.ManagedFieldsOperationApply, `{"f:spec":{"f:sharedConfig":{"f:autoCompactionRetention":{}}}}`)}, sharedConfig, nil, false},
		{"no other manager holds it", []metav1.ManagedFieldsEntry{lodia}, sharedConfig, nil, false},
		{"no Etcd", nil, sharedConfig, nil, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			rendered := &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Name: "dev-etcd", Namespace: "team-a"}}
			reader := etcdReader{}
			if c.entries != nil {
				reader.etcd = rendered.DeepCopy()
				reader.etcd.ManagedFields = c.entries
			}
			got, explained, err := (&Engine{}).HeldEmpty(t.Context(), reader, ours, &Store{etcd: rendered}, c.refused)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, c.want) || explained != c.explained {
				t.Errorf("HeldEmpty names %q, explained %t, want %q, %t", got, explained, c.want, c.explained)
			}
		})
	}
}
