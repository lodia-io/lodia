// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KubeconfigRequest asks for a short-lived kubeconfig. Brygga writes it into
// the Secret named in status and deletes both after expiry.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=kcr
// +kubebuilder:printcolumn:name="ControlPlane",type=string,JSONPath=".spec.controlPlaneRef.name"
// +kubebuilder:printcolumn:name="User",type=string,JSONPath=".spec.user"
// +kubebuilder:printcolumn:name="Expires",type=date,JSONPath=".status.expirationTimestamp"
type KubeconfigRequest struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec KubeconfigRequestSpec `json:"spec"`

	// +optional
	Status KubeconfigRequestStatus `json:"status,omitzero"`
}

// KubeconfigRequestList is a list of KubeconfigRequests.
// +kubebuilder:object:root=true
type KubeconfigRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KubeconfigRequest `json:"items"`
}

// KubeconfigRequestSpec is who asks for access to which ControlPlane, and for how long.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="a KubeconfigRequest cannot change; create another"
type KubeconfigRequestSpec struct {
	// ControlPlaneRef names a ControlPlane in this namespace.
	// +required
	ControlPlaneRef LocalReference `json:"controlPlaneRef"`

	// User is the certificate's common name.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	User string `json:"user"`

	// Groups are the certificate's organizations; brygga:cluster-admins by default.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=128
	// +kubebuilder:validation:XValidation:rule="!self.exists(g, g == 'system:masters')",message="system:masters cannot be requested"
	Groups []string `json:"groups,omitempty"`

	// ExpirationSeconds is the kubeconfig's lifetime.
	// +optional
	// +kubebuilder:default=3600
	// +kubebuilder:validation:Minimum=600
	// +kubebuilder:validation:Maximum=86400
	ExpirationSeconds int64 `json:"expirationSeconds,omitempty"`
}

// KubeconfigRequestStatus is the issued kubeconfig.
type KubeconfigRequestStatus struct {
	// SecretRef names the Secret with the kubeconfig.
	// +optional
	SecretRef *LocalReference `json:"secretRef,omitempty"`

	// ExpirationTimestamp is when the certificate expires.
	// +optional
	ExpirationTimestamp *metav1.Time `json:"expirationTimestamp,omitempty"`

	// Conditions, Issued among them.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=8
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Backup is an on-demand full snapshot of a ControlPlane's etcd, kept in the
// DatastoreClass's store; a new ControlPlane can start from it.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="ControlPlane",type=string,JSONPath=".spec.controlPlaneRef.name"
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
type Backup struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec BackupSpec `json:"spec"`

	// +optional
	Status BackupStatus `json:"status,omitzero"`
}

// BackupList is a list of Backups.
// +kubebuilder:object:root=true
type BackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Backup `json:"items"`
}

// BackupSpec names the ControlPlane to snapshot.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="a Backup cannot change; create another"
type BackupSpec struct {
	// ControlPlaneRef names a ControlPlane in this namespace.
	// +required
	ControlPlaneRef LocalReference `json:"controlPlaneRef"`
}

// BackupStatus is the snapshot's progress and where it went.
type BackupStatus struct {
	// Phase is Pending, Running, Completed or Failed.
	// +optional
	// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed
	Phase string `json:"phase,omitempty"`
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// KubernetesVersion of the ControlPlane when the snapshot was taken.
	// +optional
	// +kubebuilder:validation:MaxLength=16
	KubernetesVersion string `json:"kubernetesVersion,omitempty"`
	// Revision of etcd in the snapshot.
	// +optional
	Revision int64 `json:"revision,omitempty"`
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`
	// Location of the snapshot in the store.
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Location string `json:"location,omitempty"`
}
