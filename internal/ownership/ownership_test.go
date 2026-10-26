// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package ownership_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/ownership"
)

func TestControlledElsewhere(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", UID: "self"}}
	ref := func(apiVersion, kind string, controller bool) metav1.OwnerReference {
		return metav1.OwnerReference{APIVersion: apiVersion, Kind: kind, Name: "dev", UID: "other", Controller: new(controller)}
	}
	own := metav1.OwnerReference{APIVersion: "lodia.io/v1alpha1", Kind: "ControlPlane", Name: "dev", UID: "self", Controller: new(true)}
	for _, c := range []struct {
		name      string
		refs      []metav1.OwnerReference
		elsewhere bool
	}{
		{"no owner", nil, false},
		{"this ControlPlane", []metav1.OwnerReference{own}, false},
		{"another ControlPlane as controller", []metav1.OwnerReference{ref("lodia.io/v1alpha1", "ControlPlane", true)}, true},
		{"another ControlPlane, not as controller", []metav1.OwnerReference{own, ref("lodia.io/v1alpha1", "ControlPlane", false)}, true},
		{"a controller of another kind", []metav1.OwnerReference{ref("external-secrets.io/v1", "ExternalSecret", true)}, true},
		{"another kind, not as controller", []metav1.OwnerReference{own, ref("apps/v1", "Deployment", false)}, false},
		{"a ControlPlane of another group, not as controller", []metav1.OwnerReference{ref("example.com/v1", "ControlPlane", false)}, false},
	} {
		obj := &metav1.ObjectMeta{OwnerReferences: c.refs}
		if got := ownership.ControlledElsewhere(obj, cp) != nil; got != c.elsewhere {
			t.Errorf("%s: controlled elsewhere %t, want %t", c.name, got, c.elsewhere)
		}
	}
}
