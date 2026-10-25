// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package ownership holds the rule that no controller uses the content of,
// or writes over, an object another owner keeps
// (docs/decisions/independent-controllers.md).
package ownership

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
)

// ControlledElsewhere returns the owner reference that makes obj another
// owner's: a controller that is not cp, of any kind, or any reference to
// another ControlPlane. A controller-less object is not another owner's.
func ControlledElsewhere(obj metav1.Object, cp *bryggav1alpha1.ControlPlane) *metav1.OwnerReference {
	refs := obj.GetOwnerReferences()
	for i := range refs {
		if refs[i].UID != cp.UID && (ptr.Deref(refs[i].Controller, false) || IsControlPlane(refs[i])) {
			return &refs[i]
		}
	}
	return nil
}

// IsControlPlane says whether ref refers to a ControlPlane.
func IsControlPlane(ref metav1.OwnerReference) bool {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	return err == nil && gv.Group == bryggav1alpha1.GroupVersion.Group && ref.Kind == "ControlPlane"
}
