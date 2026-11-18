// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"slices"
	"testing"
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
	var managers managers
	for _, name := range []string{"kubectl", "helm", "kubectl"} {
		managers.add(name)
	}
	if got, want := (foreignObject{managers: managers}).owners(), "its field managers are helm, kubectl"; got != want {
		t.Errorf("an object with field managers is named %q, want %q", got, want)
	}
	if got, want := (foreignObject{}).owners(), "it has no field manager"; got != want {
		t.Errorf("an object without a field manager is named %q, want %q", got, want)
	}
}
