// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KubeconfigRequest asks for a short-lived kubeconfig. Lodia writes it into
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

	// Groups are the certificate's organizations; lodia:cluster-admins by default.
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

// Backup is a sealed copy of a ControlPlane's etcd, kept in the
// DatastoreClass's store under a prefix of its own that nothing writes again;
// a new ControlPlane can start from it, and replaces its source. It outlives
// its source.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:validation:XValidation:rule="self.metadata.name.size() <= 235",message="the name must be at most 235 characters, so that the Secret <name>.encryption-config fits"
// +kubebuilder:printcolumn:name="ControlPlane",type=string,JSONPath=".spec.controlPlaneRef.name"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Reason",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].reason"
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

// BackupStatus is the copy's progress, what it was taken of, and where it
// is. The backup reconciler of the Datastore controller writes it.
type BackupStatus struct {
	// Conditions, Ready among them.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=8
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Source is the ControlPlane the Backup was taken of.
	// +optional
	Source *UIDReference `json:"source,omitempty"`

	// KubernetesVersion is the version the source's API server ran when the
	// Backup was sealed, which wrote the data.
	// +optional
	// +kubebuilder:validation:MaxLength=16
	KubernetesVersion string `json:"kubernetesVersion,omitempty"`

	// Network is the source's network, which a ControlPlane restored from the
	// Backup must have too.
	// +optional
	Network *Network `json:"network,omitempty"`

	// Snapshots is where the copy is: the store it was sealed in and the
	// Backup's own prefix there.
	// +optional
	Snapshots *SnapshotLocation `json:"snapshots,omitempty"`

	// Seal is what the copy holds.
	// +optional
	Seal *BackupSeal `json:"seal,omitempty"`

	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
}

// BackupSeal is the seal of a Backup: before the copy, the backup
// reconciler puts the key BackupSealKey into the source's etcd and copies
// only once a snapshot reached its revision, so the copy holds the source's
// data at least up to it, and a restore proves it came from this Backup by
// finding the key.
type BackupSeal struct {
	// Revision of the seal key in the source's etcd.
	// +required
	// +kubebuilder:validation:Minimum=1
	Revision int64 `json:"revision"`
}
