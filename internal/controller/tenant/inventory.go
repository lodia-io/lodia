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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// union keeps the first of each object, sorted.
func union(lists ...[]entry) []entry {
	var out []entry
	for _, list := range lists {
		for _, e := range list {
			if !slices.ContainsFunc(out, func(o entry) bool { return compareEntries(o, e) == 0 }) {
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
		if !slices.ContainsFunc(desired, func(d entry) bool { return compareEntries(d, e) == 0 }) {
			out = append(out, e)
		}
	}
	return out
}

// failure is an object a pass could not write or read, and what the tenant
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
	if refused(err) {
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
		return nil, client.IgnoreNotFound(err)
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
		WithLabels(map[string]string{managedByLabel: managedBy}).
		WithData(map[string]string{inventoryKey: string(data)})
	if err := c.Apply(ctx, cm, client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		return failed(inventoryEntry(), err)
	}
	return nil
}

// applyInventory applies the objects, then prunes what the previous
// inventory lists and they do not. The inventory names an object before it
// is first written, so a pass that stops anywhere leaves nothing a later
// pass cannot prune.
func applyInventory(ctx context.Context, c client.Client, objects []*unstructured.Unstructured) error {
	previous, err := readInventory(ctx, c)
	if err != nil {
		return err
	}
	previous = union(previous)
	desired := make([]entry, 0, len(objects))
	for _, obj := range objects {
		desired = append(desired, entryOf(obj))
	}
	desired = union(desired)
	stored := previous
	if both := union(desired, previous); !slices.Equal(both, previous) {
		if err := writeInventory(ctx, c, both); err != nil {
			return err
		}
		stored = both
	}
	for _, obj := range objects {
		if err := c.Apply(ctx, client.ApplyConfigurationFromUnstructured(obj), client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
			return failed(entryOf(obj), err)
		}
	}
	for _, e := range stale(previous, desired) {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(schema.GroupVersionKind{Group: e.Group, Version: e.Version, Kind: e.Kind})
		obj.SetNamespace(e.Namespace)
		obj.SetName(e.Name)
		err := c.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationBackground))
		if client.IgnoreNotFound(err) != nil {
			return failed(e, err)
		}
	}
	if !slices.Equal(desired, stored) {
		return writeInventory(ctx, c, desired)
	}
	return nil
}
