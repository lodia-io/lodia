// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAnObjectAtAnotherVersionIsNotStale(t *testing.T) {
	previous := []entry{
		{Group: "apps", Version: "v1beta2", Kind: "DaemonSet", Namespace: "kube-system", Name: "kube-proxy"},
		{Version: "v1", Kind: "ConfigMap", Namespace: "kube-system", Name: "gone"},
	}
	desired := []entry{{Group: "apps", Version: "v1", Kind: "DaemonSet", Namespace: "kube-system", Name: "kube-proxy"}}
	if got, want := stale(previous, desired), previous[1:]; !slices.Equal(got, want) {
		t.Errorf("stale is %v, want %v", got, want)
	}
	if got := union(desired, previous); len(got) != 2 || got[1].Version != "v1" {
		t.Errorf("the union keeps %v, want the desired version of kube-proxy", got)
	}
}

func TestAnotherOwnersObjectIsNamedWithItsFieldManagers(t *testing.T) {
	obj := &metav1.ObjectMeta{ManagedFields: []metav1.ManagedFieldsEntry{
		{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationApply},
		{Manager: "helm", Operation: metav1.ManagedFieldsOperationUpdate},
		{Operation: metav1.ManagedFieldsOperationUpdate},
		{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationUpdate},
	}}
	if got, want := (foreignObject{managers: fieldManagers(obj)}).owners(), "its field managers are helm, kubectl"; got != want {
		t.Errorf("an object with field managers is named %q, want %q", got, want)
	}
	if got, want := (foreignObject{managers: fieldManagers(&metav1.ObjectMeta{})}).owners(), "it has no field manager"; got != want {
		t.Errorf("an object without a field manager is named %q, want %q", got, want)
	}
}
