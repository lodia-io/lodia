// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/tenantclient"
)

const (
	inventoryName = "lodia-inventory"
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

func entriesOf(objects []*unstructured.Unstructured) []entry {
	out := make([]entry, 0, len(objects))
	for _, obj := range objects {
		out = append(out, entryOf(obj))
	}
	return out
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

// failure is an object a pass could not read or write, with the reason and
// the verdict to report, empty when the tenant did not answer.
type failure struct {
	entry           entry
	reason, verdict string
	err             error
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
		f.reason, f.verdict = lodiav1alpha1.ReasonRefused, tenantclient.Refused(err)
	}
	return f
}

func inventoryEntry() entry {
	return entry{Version: "v1", Kind: "ConfigMap", Namespace: metav1.NamespaceSystem, Name: inventoryName}
}

func readInventory(ctx context.Context, a tenantAPI) ([]entry, error) {
	body, err := a.read(ctx, inventoryEntry())
	if err != nil {
		return nil, failed(inventoryEntry(), err)
	}
	if body == nil {
		return nil, nil
	}
	stored, err := tenantclient.Decode(body, nil, inventoryFrom)
	if err != nil {
		return nil, &failure{entry: inventoryEntry(), reason: lodiav1alpha1.ReasonApplying, verdict: "cannot be read: delete it, and every object is applied again", err: err}
	}
	return stored.entries, nil
}

func writeInventory(ctx context.Context, a tenantAPI, entries []entry) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	cm := corev1ac.ConfigMap(inventoryName, metav1.NamespaceSystem).
		WithLabels(map[string]string{lodiav1alpha1.ManagedByLabel: lodiav1alpha1.ManagedBy}).
		WithData(map[string]string{inventoryKey: string(data)})
	body, err := json.Marshal(cm)
	if err != nil {
		return err
	}
	if err := a.apply(ctx, inventoryEntry(), body); err != nil {
		return failed(inventoryEntry(), err)
	}
	return nil
}

// foreignObject is an object of the render that exists without Lodia's field
// manager, another owner's, with the field managers it has.
type foreignObject struct {
	entry
	managers managers
}

// owners names the field managers, which tell a person whether deleting the
// object helps: one a tool applies comes back. Of a great many it names the
// first that sort and counts the entries of the others.
func (o foreignObject) owners() string {
	if len(o.managers.names) == 0 {
		return "it has no field manager"
	}
	named := "its field managers are " + strings.Join(o.managers.names, ", ")
	if more := o.managers.more(); more > 0 {
		named += fmt.Sprintf(" and %d more entries", more)
	}
	return named
}

func isForeign(others []foreignObject, e entry) bool {
	return slices.ContainsFunc(others, func(o foreignObject) bool { return compareEntries(o.entry, e) == 0 })
}

// foreign returns the objects of the render no inventory lists yet that
// exist without Lodia's field manager: another owner's.
func foreign(ctx context.Context, a tenantAPI, objects []*unstructured.Unstructured, previous []entry) ([]foreignObject, error) {
	var others []foreignObject
	for _, obj := range objects {
		e := entryOf(obj)
		if contains(previous, e) {
			continue
		}
		existing, err := readEntry(ctx, a, e)
		switch {
		case err != nil:
			return nil, err
		case existing != nil && !existing.applied:
			others = append(others, foreignObject{entry: e, managers: existing.managers})
		}
	}
	slices.SortFunc(others, func(a, b foreignObject) int { return compareEntries(a.entry, b.entry) })
	return others, nil
}

func readEntry(ctx context.Context, a tenantAPI, e entry) (*object, error) {
	existing, err := a.readObject(ctx, e)
	if err != nil {
		return nil, failed(e, err)
	}
	return existing, nil
}

// prune deletes an entry that left the render if Lodia wrote it, under the
// UID and resourceVersion it read; an entry it did not write, or one gone or
// of a kind no longer served, only leaves the inventory.
func prune(ctx context.Context, a tenantAPI, e entry) error {
	existing, err := readEntry(ctx, a, e)
	if err != nil || existing == nil || !existing.applied {
		return err
	}
	if err := a.delete(ctx, e, existing); err != nil && !apierrors.IsNotFound(err) {
		return failed(e, err)
	}
	return nil
}

// applyInventory applies the objects but another owner's, which it returns,
// past each one the tenant refuses, and only once it refused none prunes what
// the previous inventory lists and they do not and writes the inventory of
// what it applied. The inventory names an object before Lodia first writes it,
// so a pass that stops anywhere leaves nothing a later pass cannot prune. It
// returns the refusals it went past, and the failure it stopped at: before
// the first apply any failure, and after it one that is no answer.
func applyInventory(ctx context.Context, a tenantAPI, objects []*unstructured.Unstructured) ([]foreignObject, []*failure, error) {
	previous, err := readInventory(ctx, a)
	if err != nil {
		return nil, nil, err
	}
	previous = union(previous)
	others, err := foreign(ctx, a, objects, previous)
	if err != nil {
		return nil, nil, err
	}
	var desired []entry
	var writes []*unstructured.Unstructured
	for _, obj := range objects {
		if e := entryOf(obj); !isForeign(others, e) {
			desired, writes = append(desired, e), append(writes, obj)
		}
	}
	desired = union(desired)
	stored := previous
	if both := union(desired, previous); !slices.Equal(both, previous) {
		if err := writeInventory(ctx, a, both); err != nil {
			return nil, nil, err
		}
		stored = both
	}
	var refused []*failure
	past := func(err error) error {
		if f, ok := errors.AsType[*failure](err); ok && f.verdict != "" {
			refused = append(refused, f)
			return nil
		}
		return err
	}
	for _, obj := range writes {
		if err := past(apply(ctx, a, obj)); err != nil {
			return others, refused, err
		}
	}
	if len(refused) > 0 {
		return others, refused, nil
	}
	for _, e := range stale(previous, desired) {
		if err := past(prune(ctx, a, e)); err != nil {
			return others, refused, err
		}
	}
	if len(refused) == 0 && !slices.Equal(desired, stored) {
		return others, refused, past(writeInventory(ctx, a, desired))
	}
	return others, refused, nil
}

func apply(ctx context.Context, a tenantAPI, obj *unstructured.Unstructured) error {
	body, err := obj.MarshalJSON()
	if err != nil {
		return failed(entryOf(obj), err)
	}
	if err := a.apply(ctx, entryOf(obj), body); err != nil {
		return failed(entryOf(obj), err)
	}
	return nil
}

// heldInventory reads what a held pass acts on: the stored inventory, the
// objects of the render it lists that the tenant lacks, and whether it is
// applied: it lists something, every object it lists is stored with Lodia's
// apply but those missing ones, and no object of the render it does not list
// is another owner's. Without a render, nil objects, only the inventory is read.
func heldInventory(ctx context.Context, a tenantAPI, objects []*unstructured.Unstructured) ([]entry, []*unstructured.Unstructured, bool, error) {
	listed, err := readInventory(ctx, a)
	if err != nil || objects == nil {
		return union(listed), nil, false, err
	}
	listed = union(listed)
	applied := len(listed) > 0
	var missing []*unstructured.Unstructured
	for _, e := range listed {
		existing, err := readEntry(ctx, a, e)
		if err != nil {
			return nil, nil, false, err
		}
		i := slices.IndexFunc(objects, func(obj *unstructured.Unstructured) bool { return compareEntries(entryOf(obj), e) == 0 })
		switch {
		case existing == nil && i >= 0:
			missing = append(missing, objects[i])
		case existing == nil || !existing.applied:
			applied = false
		}
	}
	others, err := foreign(ctx, a, objects, listed)
	if err != nil {
		return nil, nil, false, err
	}
	return listed, missing, applied && len(others) == 0, nil
}
