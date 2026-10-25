// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

// CIDR is an IPv4 or IPv6 network in CIDR notation.
// +kubebuilder:validation:MaxLength=43
// +kubebuilder:validation:XValidation:rule="isCIDR(self)",message="must be a network in CIDR notation"
type CIDR string

// Hostname is a lowercase DNS name: labels of at most 63 characters, and not
// digits and dots alone, which read as an IP address.
// +kubebuilder:validation:MinLength=1
// +kubebuilder:validation:MaxLength=253
// +kubebuilder:validation:XValidation:rule="self.matches('^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?([.][a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?)*$') && !self.matches('^[0-9.]+$')",message="must be a lowercase DNS name"
type Hostname string

// LocalReference names an object in the same namespace.
type LocalReference struct {
	// Name of the object.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// NamespacedReference names an object in a namespace, the same one when the
// namespace is omitted.
type NamespacedReference struct {
	// Name of the object.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Namespace of the object.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace,omitempty"`
}

// SecretReference names a Secret in a namespace.
type SecretReference struct {
	// Name of the Secret.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// Namespace of the Secret.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`
}
