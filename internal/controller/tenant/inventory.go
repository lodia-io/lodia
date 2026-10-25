// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/apistatus"
)

const (
	inventoryName = "brygga-inventory"
	inventoryKey  = "objects"
)

// entry is an object in the inventory. Version only addresses it: the same
// object at another version is the same entry.
type entry struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

func (e entry) String() string {
	if e.Namespace == "" {
		return e.Kind + " " + e.Name
	}
	return e.Kind + " " + e.Namespace + "/" + e.Name
}

func compareEntries(a, b entry) int {
	return cmp.Or(cmp.Compare(a.Group, b.Group), cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
}

func entryOf(obj *unstructured.Unstructured) entry {
	gvk := obj.GroupVersionKind()
	return entry{Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind, Namespace: obj.GetNamespace(), Name: obj.GetName()}
}

func contains(entries []entry, e entry) bool {
	return slices.ContainsFunc(entries, func(o entry) bool { return compareEntries(o, e) == 0 })
}

// union keeps the first of each object, sorted.
func union(lists ...[]entry) []entry {
	var out []entry
	for _, list := range lists {
		for _, e := range list {
			if !contains(out, e) {
				out = append(out, e)
			}
		}
	}
	slices.SortFunc(out, compareEntries)
	return out
}

// stale is what previous lists and desired does not; the same object at
// another version is not stale.
func stale(previous, desired []entry) []entry {
	var out []entry
	for _, e := range previous {
		if !contains(desired, e) {
			out = append(out, e)
		}
	}
	return out
}

// appliedByBrygga reads managedFields: the managed-by label is on PKI's
// bindings in the tenant too.
func appliedByBrygga(obj metav1.Object) bool {
	return slices.ContainsFunc(obj.GetManagedFields(), func(f metav1.ManagedFieldsEntry) bool {
		return f.Manager == FieldManager && f.Operation == metav1.ManagedFieldsOperationApply
	})
}

// failure is an object a pass could not read or write, and what the tenant
// answered, empty when it did not.
type failure struct {
	entry   entry
	verdict string
	err     error
}

func (f *failure) Error() string {
	return fmt.Sprintf("%s: %v", f.entry, f.err)
}

func (f *failure) Unwrap() error {
	return f.err
}

func failed(e entry, err error) error {
	f := &failure{entry: e, err: err}
	if apistatus.Refused(err) {
		f.verdict = "is refused by the tenant's API server"
	}
	return f
}

func inventoryEntry() entry {
	return entry{Version: "v1", Kind: "ConfigMap", Namespace: metav1.NamespaceSystem, Name: inventoryName}
}

func readInventory(ctx context.Context, c client.Client) ([]entry, error) {
	cm := &corev1.ConfigMap{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: inventoryName}, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, failed(inventoryEntry(), err)
	}
	var entries []entry
	if err := json.Unmarshal([]byte(cm.Data[inventoryKey]), &entries); err != nil {
		return nil, &failure{entry: inventoryEntry(), verdict: "cannot be read: delete it, and every object is applied again", err: err}
	}
	return entries, nil
}

func writeInventory(ctx context.Context, c client.Client, entries []entry) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	cm := corev1ac.ConfigMap(inventoryName, metav1.NamespaceSystem).
		WithLabels(map[string]string{bryggav1alpha1.ManagedByLabel: bryggav1alpha1.ManagedBy}).
		WithData(map[string]string{inventoryKey: string(data)})
	if err := c.Apply(ctx, cm, client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		return failed(inventoryEntry(), err)
	}
	return nil
}

// foreign returns the objects of the render no inventory lists yet that
// exist without Brygga's field manager: another owner's.
func foreign(ctx context.Context, c client.Client, objects []*unstructured.Unstructured, previous []entry) ([]entry, error) {
	var others []entry
	for _, obj := range objects {
		e := entryOf(obj)
		if contains(previous, e) {
			continue
		}
		existing := &unstructured.Unstructured{}
		existing.SetGroupVersionKind(obj.GroupVersionKind())
		err := c.Get(ctx, client.ObjectKey{Namespace: e.Namespace, Name: e.Name}, existing)
		switch {
		case apierrors.IsNotFound(err):
		case err != nil:
			return nil, failed(e, err)
		case !appliedByBrygga(existing):
			others = append(others, e)
		}
	}
	slices.SortFunc(others, compareEntries)
	return others, nil
}

// prune deletes an entry that left the render if Brygga wrote it, under the
// UID and resourceVersion it read; an entry it did not write, or one gone or
// of a kind no longer served, only leaves the inventory.
func prune(ctx context.Context, c client.Client, e entry) error {
	mapping, err := c.RESTMapper().RESTMapping(schema.GroupKind{Group: e.Group, Kind: e.Kind})
	if meta.IsNoMatchError(err) {
		return nil
	}
	if err != nil {
		return failed(e, err)
	}
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(mapping.GroupVersionKind)
	err = c.Get(ctx, client.ObjectKey{Namespace: e.Namespace, Name: e.Name}, existing)
	switch {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return failed(e, err)
	case !appliedByBrygga(existing):
		return nil
	}
	uid, version := existing.GetUID(), existing.GetResourceVersion()
	err = c.Delete(ctx, existing, client.Preconditions{UID: &uid, ResourceVersion: &version}, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if client.IgnoreNotFound(err) != nil {
		return failed(e, err)
	}
	return nil
}

// applyInventory applies the objects but another owner's, which it returns,
// then prunes what the previous inventory lists and they do not. The
// inventory names an object before Brygga first writes it, so a pass that
// stops anywhere leaves nothing a later pass cannot prune.
func applyInventory(ctx context.Context, c client.Client, objects []*unstructured.Unstructured) ([]entry, error) {
	previous, err := readInventory(ctx, c)
	if err != nil {
		return nil, err
	}
	previous = union(previous)
	others, err := foreign(ctx, c, objects, previous)
	if err != nil {
		return nil, err
	}
	var desired []entry
	var writes []*unstructured.Unstructured
	for _, obj := range objects {
		if e := entryOf(obj); !contains(others, e) {
			desired, writes = append(desired, e), append(writes, obj)
		}
	}
	desired = union(desired)
	stored := previous
	if both := union(desired, previous); !slices.Equal(both, previous) {
		if err := writeInventory(ctx, c, both); err != nil {
			return nil, err
		}
		stored = both
	}
	for _, obj := range writes {
		if err := c.Apply(ctx, client.ApplyConfigurationFromUnstructured(obj), client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
			return nil, failed(entryOf(obj), err)
		}
	}
	for _, e := range stale(previous, desired) {
		if err := prune(ctx, c, e); err != nil {
			return nil, err
		}
	}
	if !slices.Equal(desired, stored) {
		return others, writeInventory(ctx, c, desired)
	}
	return others, nil
}
