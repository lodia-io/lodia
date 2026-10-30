// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KubeconfigRequest asks for a short-lived kubeconfig for a key of the
// requester's own. Lodia answers in status and deletes the request at its
// deadline.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=kcr
// +kubebuilder:printcolumn:name="ControlPlane",type=string,JSONPath=".spec.controlPlaneRef.name"
// +kubebuilder:printcolumn:name="User",type=string,JSONPath=".spec.user"
// +kubebuilder:printcolumn:name="Access",type=string,JSONPath=".spec.access"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
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

// KubeconfigRequestSpec is who asks for access to which ControlPlane, with
// which key, and for how long.
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="a KubeconfigRequest cannot change; create another"
type KubeconfigRequestSpec struct {
	// ControlPlaneRef names a ControlPlane in this namespace.
	// +required
	ControlPlaneRef LocalReference `json:"controlPlaneRef"`

	// User is the requester's own username, which the admission policy
	// kubeconfigrequests.lodia.io requires; the certificate's user is
	// lodia:user:<user>.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	User string `json:"user"`

	// Access ClusterAdmin puts the certificate in lodia:cluster-admins, for
	// which the admission policy requires create on
	// controlplanes/adminkubeconfig for the ControlPlane; without it the
	// certificate carries the user only.
	// +optional
	Access KubeconfigAccess `json:"access,omitempty"`

	// Request is a PEM PKCS#10 certificate request, of which Lodia uses the
	// public key only.
	// +required
	// +kubebuilder:validation:MaxLength=5120
	// +kubebuilder:validation:XValidation:rule="string(self).startsWith('-----BEGIN CERTIFICATE REQUEST-----') && !string(self).contains('PRIVATE KEY')",message="must be a PEM certificate request, with no private key"
	Request []byte `json:"request"`

	// ExpirationSeconds counts from the request's creation.
	// +optional
	// +kubebuilder:default=3600
	// +kubebuilder:validation:Minimum=600
	// +kubebuilder:validation:Maximum=86400
	ExpirationSeconds int64 `json:"expirationSeconds,omitempty"`
}

// KubeconfigAccess is what a KubeconfigRequest's certificate carries beyond its user.
// +kubebuilder:validation:Enum=ClusterAdmin
type KubeconfigAccess string

// KubeconfigRequestStatus is the issued kubeconfig. The kubeconfig request
// reconciler of the PKI controller writes it.
type KubeconfigRequestStatus struct {
	// Kubeconfig names the public endpoint and the cluster CA bundle and
	// embeds the certificate; it has no key.
	// +optional
	// +kubebuilder:validation:MaxLength=40960
	Kubeconfig []byte `json:"kubeconfig,omitempty"`

	// ExpirationTimestamp is the certificate's NotAfter.
	// +optional
	ExpirationTimestamp *metav1.Time `json:"expirationTimestamp,omitempty"`

	// Conditions, Ready among them.
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

	// KubernetesVersion is the version the source ran when the Backup was
	// sealed: its status.kubernetes.version, or its status.target.version
	// until Lifecycle writes the former. A ControlPlane restored from the
	// Backup runs the same minor.
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
