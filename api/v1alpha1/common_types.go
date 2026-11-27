// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

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
	// name of the object.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// UIDReference is an identity record: an object in the same namespace by its
// name and the one object that had that name, since a name can be taken
// again and a UID never. A reader does not follow it to the object, as it
// follows a field named <x>Ref; the object may be gone.
type UIDReference struct {
	// name of the object.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// uid of the object.
	// +required
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=36
	UID types.UID `json:"uid"`
}

// SecretReference names a Secret in a namespace.
type SecretReference struct {
	// name of the Secret.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// namespace of the Secret.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`
}

// LabelValue is a label's value: empty, or at most 63 alphanumerics, dashes,
// underscores and dots that begin and end with an alphanumeric.
// +kubebuilder:validation:MaxLength=63
// +kubebuilder:validation:XValidation:rule="self.matches('^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$')",message="must be a label's value: empty, or alphanumerics, dashes, underscores and dots that begin and end with an alphanumeric"
type LabelValue string

// Toleration is a pod's toleration as pod validation takes it, without the
// operators Lt and Gt.
// +kubebuilder:validation:XValidation:rule="has(self.key) && size(self.key) > 0 || has(self.operator) && self.operator == 'Exists'",message="a toleration without a key has operator Exists"
// +kubebuilder:validation:XValidation:rule="!has(self.tolerationSeconds) || has(self.effect) && self.effect == 'NoExecute'",message="tolerationSeconds is set only with effect NoExecute"
// +kubebuilder:validation:XValidation:rule="!has(self.operator) || self.operator != 'Exists' || !has(self.value) || size(self.value) == 0",message="a toleration with operator Exists has no value"
type Toleration struct {
	// key is the taint's key, a label's name; without it, and with operator
	// Exists, the toleration matches every taint.
	// +optional
	// +kubebuilder:validation:MaxLength=317
	// +kubebuilder:validation:XValidation:rule="size(self) == 0 || self.matches('^([a-z0-9]([-a-z0-9]*[a-z0-9])?([.][a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]$') && (self.contains('/') ? size(self.split('/')[0]) <= 253 && size(self.split('/')[1]) <= 63 : size(self) <= 63)",message="must be a label's name: at most 63 alphanumerics, dashes, underscores and dots that begin and end with an alphanumeric, after a lowercase DNS subdomain of at most 253 characters and a slash"
	Key string `json:"key,omitempty"`

	// operator is Equal, the taint's value equal to value, or Exists, any
	// value; Equal by default.
	// +optional
	// +kubebuilder:validation:Enum=Equal;Exists
	Operator corev1.TolerationOperator `json:"operator,omitempty"`

	// value is the taint's value that operator Equal matches.
	// +optional
	Value LabelValue `json:"value,omitempty"`

	// effect is the taint's effect, NoSchedule, PreferNoSchedule or NoExecute;
	// without it the toleration matches every effect.
	// +optional
	// +kubebuilder:validation:Enum=NoSchedule;PreferNoSchedule;NoExecute
	Effect corev1.TaintEffect `json:"effect,omitempty"`

	// tolerationSeconds is how long a pod stays on a node tainted NoExecute;
	// without it, for as long as the taint.
	// +optional
	TolerationSeconds *int64 `json:"tolerationSeconds,omitempty"`
}

// Resources are a container's requests and limits of cpu, memory and
// ephemeral-storage, as pod validation takes them.
// +kubebuilder:validation:XValidation:rule="!has(self.requests) || !has(self.limits) || self.requests.all(k, !(k in self.limits) || quantity(string(self.requests[k])).compareTo(quantity(string(self.limits[k]))) <= 0)",message="a request cannot exceed its limit"
type Resources struct {
	// requests are what the container is scheduled by.
	// +optional
	// +kubebuilder:validation:MaxProperties=3
	// +kubebuilder:validation:XValidation:rule="self.all(k, k in ['cpu', 'memory', 'ephemeral-storage'])",message="a control plane container takes cpu, memory and ephemeral-storage alone"
	// +kubebuilder:validation:XValidation:rule="self.all(k, !quantity(string(self[k])).isLessThan(quantity('0')))",message="a quantity cannot be negative"
	Requests corev1.ResourceList `json:"requests,omitempty"`

	// limits are the most the container may use.
	// +optional
	// +kubebuilder:validation:MaxProperties=3
	// +kubebuilder:validation:XValidation:rule="self.all(k, k in ['cpu', 'memory', 'ephemeral-storage'])",message="a control plane container takes cpu, memory and ephemeral-storage alone"
	// +kubebuilder:validation:XValidation:rule="self.all(k, !quantity(string(self[k])).isLessThan(quantity('0')))",message="a quantity cannot be negative"
	Limits corev1.ResourceList `json:"limits,omitempty"`
}

// Requirements are r as a container takes them.
func (r *Resources) Requirements() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{Requests: r.Requests.DeepCopy(), Limits: r.Limits.DeepCopy()}
}
