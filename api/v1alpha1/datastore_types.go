// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DatastoreClass is how a dedicated etcd is built. The class annotated
// lodia.io/is-default-class: "true" serves ControlPlanes that name none.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=dsc
// +kubebuilder:printcolumn:name="Storage",type=string,JSONPath=".spec.storage.className"
// +kubebuilder:printcolumn:name="Backups",type=string,JSONPath=".spec.backup.store.provider"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
type DatastoreClass struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec DatastoreClassSpec `json:"spec"`
}

// DatastoreClassList is a list of DatastoreClasses.
// +kubebuilder:object:root=true
type DatastoreClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DatastoreClass `json:"items"`
}

// DatastoreClassSpec describes a dedicated etcd. Members follow the
// ControlPlane: one, or three with high availability.
type DatastoreClassSpec struct {
	// Etcd configures the members.
	// +optional
	Etcd *EtcdSettings `json:"etcd,omitempty"`

	// Storage is the members' volumes.
	// +required
	Storage EtcdStorage `json:"storage"`

	// Backup is where and how often snapshots go; without it, no backups.
	// +optional
	Backup *EtcdBackup `json:"backup,omitempty"`

	// Placement says where members run.
	// +optional
	Placement *Placement `json:"placement,omitempty"`
}

// EtcdSettings configure etcd members.
type EtcdSettings struct {
	// QuotaBackendBytes is the database size limit, above zero: the backup
	// sidecar refuses to start with zero or less.
	// +optional
	// +kubebuilder:default="4Gi"
	// +kubebuilder:validation:XValidation:rule="quantity(string(self)).isGreaterThan(quantity('0'))",message="must be greater than zero"
	QuotaBackendBytes *resource.Quantity `json:"quotaBackendBytes,omitempty"`

	// AutoCompaction is etcd's own compaction.
	// +optional
	AutoCompaction *AutoCompaction `json:"autoCompaction,omitempty"`

	// Defragmentation runs followers first, the leader last.
	// +optional
	Defragmentation *Defragmentation `json:"defragmentation,omitempty"`

	// Resources of each member.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// AutoCompaction is etcd's compaction mode and retention.
// +kubebuilder:validation:XValidation:rule="!has(self.retention) || self.retention.matches('^[0-9]+$') || ((!has(self.mode) || self.mode == 'periodic') && duration(self.retention) >= duration('0s'))",message="retention must be a number, or in periodic mode a duration"
type AutoCompaction struct {
	// Mode is periodic or revision.
	// +optional
	// +kubebuilder:default=periodic
	// +kubebuilder:validation:Enum=periodic;revision
	Mode string `json:"mode,omitempty"`

	// Retention is a number of hours or a duration in periodic mode, a
	// revision count in revision mode; etcd refuses to start with anything
	// else.
	// +optional
	// +kubebuilder:default="15m"
	// +kubebuilder:validation:MaxLength=32
	Retention string `json:"retention,omitempty"`
}

// Defragmentation is a cron schedule.
type Defragmentation struct {
	// Schedule is a cron expression of five fields separated by single
	// spaces, without lists, whose ranges ascend and whose days of the week
	// run from 1 to 6, since etcd-druid refuses 0 and the backup sidecar 7;
	// */7 selects Sunday.
	// +required
	Schedule CronSchedule `json:"schedule"`
}

// CronSchedule is a cron expression both etcd-druid and the backup sidecar
// accept.
// +kubebuilder:validation:MaxLength=64
// +kubebuilder:validation:Pattern=`^(\*|[1-5]?[0-9]|[1-5]?[0-9]-[1-5]?[0-9]|(?:[1-9]|[1-4][0-9]|5[0-9])\/(?:[1-9]|[1-4][0-9]|5[0-9]|60)|\*\/(?:[1-9]|[1-4][0-9]|5[0-9]|60))\s+(\*|[0-9]|1[0-9]|2[0-3]|[0-9]-(?:[0-9]|1[0-9]|2[0-3])|1[0-9]-(?:1[0-9]|2[0-3])|2[0-3]-2[0-3]|(?:[1-9]|1[0-9]|2[0-3])\/(?:[1-9]|1[0-9]|2[0-4])|\*\/(?:[1-9]|1[0-9]|2[0-4]))\s+(\*|[1-9]|[12][0-9]|3[01]|[1-9]-(?:[1-9]|[12][0-9]|3[01])|[12][0-9]-(?:[12][0-9]|3[01])|3[01]-3[01]|(?:[1-9]|[12][0-9]|30)\/(?:[1-9]|[12][0-9]|3[01])|\*\/(?:[1-9]|[12][0-9]|3[01]))\s+(\*|[1-9]|1[0-2]|[1-9]-(?:[1-9]|1[0-2])|1[0-2]-1[0-2]|(?:[1-9]|1[0-2])\/(?:[1-9]|1[0-2])|\*\/(?:[1-9]|1[0-2]))\s+(\*|[1-7]|[1-6]-[1-7]|[1-6]\/[1-7]|\*\/[1-7])$`
// +kubebuilder:validation:XValidation:rule="self.matches('^[^ ]+ [^ ]+ [^ ]+ [^ ]+ [^ ]+$')",message="must be five fields separated by single spaces"
// +kubebuilder:validation:XValidation:rule="self.split(' ').all(f, !f.split('/')[0].matches('^[0-9]+-[0-9]+$') || int(f.split('/')[0].split('-')[0]) <= int(f.split('/')[0].split('-')[1]))",message="every range must ascend"
// +kubebuilder:validation:XValidation:rule="self.split(' ').size() != 5 || !self.split(' ')[4].matches('^(7|[1-6]-7)$')",message="the day of the week cannot be 7"
type CronSchedule string

// EtcdStorage is the members' persistent volumes.
type EtcdStorage struct {
	// ClassName is the StorageClass; empty means the cluster's default.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	ClassName string `json:"className,omitempty"`

	// Size of each member's volume.
	// +required
	Size resource.Quantity `json:"size"`

	// ReclaimPolicy is what happens to the volumes of a deleted ControlPlane.
	// +optional
	// +kubebuilder:default=Retain
	// +kubebuilder:validation:Enum=Delete;Retain
	ReclaimPolicy string `json:"reclaimPolicy,omitempty"`
}

// EtcdBackup is continuous snapshots to object storage.
type EtcdBackup struct {
	// Store is the bucket.
	// +required
	Store BackupStore `json:"store"`

	// FullSnapshotSchedule is a cron expression as defragmentation.schedule
	// takes it; the hour moves into the tenant's maintenance window.
	// +optional
	// +kubebuilder:default="0 0 * * *"
	FullSnapshotSchedule CronSchedule `json:"fullSnapshotSchedule,omitempty"`

	// DeltaSnapshotPeriod is how often a delta snapshot is taken.
	// +optional
	// +kubebuilder:default="5m"
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="must be a duration"
	DeltaSnapshotPeriod *metav1.Duration `json:"deltaSnapshotPeriod,omitempty"`

	// Retention is how long snapshots are kept.
	// +optional
	Retention *BackupRetention `json:"retention,omitempty"`
}

// BackupStore is an object storage bucket.
type BackupStore struct {
	// +required
	Provider StoreProvider `json:"provider"`

	// Bucket name.
	// +required
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9][a-zA-Z0-9._-]{1,61}[a-zA-Z0-9]$`
	Bucket string `json:"bucket"`

	// @todo: refuses what etcd-backup-restore's parser misreads; remove with todo.md's v1/ prefix item.

	// Prefix inside the bucket; each ControlPlane's snapshots go under
	// <prefix>/<namespace UID>/controlplanes/<ControlPlane UID>. It neither
	// contains v1/ nor ends in v1, which etcd-backup-restore would misread.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:rule="!(self + '/').contains('v1/')",message="must neither contain v1/ nor end in v1: etcd-backup-restore's snapshot parser takes the last v1/ in a key for its layout and would ignore every snapshot under this prefix"
	Prefix string `json:"prefix,omitempty"`

	// Region of the bucket.
	// +optional
	// +kubebuilder:validation:MaxLength=64
	Region string `json:"region,omitempty"`

	// Endpoint for S3-compatible stores.
	// +optional
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:XValidation:rule="self.startsWith('https://') || self.startsWith('http://')",message="must be a URL"
	// +kubebuilder:validation:XValidation:rule="isURL(self)",message="must be a URL"
	Endpoint string `json:"endpoint,omitempty"`

	// SecretRef names the credentials.
	// +required
	SecretRef SecretReference `json:"secretRef"`
}

// StoreProvider is the kind of an object store: S3, GCS, ABS or Swift.
// +kubebuilder:validation:Enum=S3;GCS;ABS;Swift
type StoreProvider string

// BackupRetention is how long snapshots live.
type BackupRetention struct {
	// FullSnapshots is how many full snapshots a running ControlPlane keeps,
	// the newest: days, at the default schedule. Only the newest keeps the
	// delta snapshots after it, since no delta retention period is set, so an
	// older one restores to its own revision.
	// +optional
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3650
	FullSnapshots int32 `json:"fullSnapshots,omitempty"`

	// AfterDelete is how long snapshots outlive their ControlPlane; no
	// release deletes them yet.
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="must be a duration"
	AfterDelete *metav1.Duration `json:"afterDelete,omitempty"`
}

// Datastore is an etcd several tenants share by key prefix. There is no quota
// per tenant in a shared store, and one tenant's load is its neighbours'.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=ds
// +kubebuilder:printcolumn:name="Driver",type=string,JSONPath=".spec.driver"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
type Datastore struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +required
	Spec DatastoreSpec `json:"spec"`
}

// DatastoreList is a list of Datastores.
// +kubebuilder:object:root=true
type DatastoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Datastore `json:"items"`
}

// DatastoreSpec is how to reach the shared store and administer it.
type DatastoreSpec struct {
	// Driver is etcd; the field leaves room for other stores later.
	// +optional
	// +kubebuilder:default=etcd
	// +kubebuilder:validation:Enum=etcd
	Driver string `json:"driver,omitempty"`

	// Endpoints of the members.
	// +required
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=9
	// +kubebuilder:validation:items:MaxLength=512
	// +kubebuilder:validation:items:XValidation:rule="self.startsWith('https://')",message="must be an https URL"
	Endpoints []string `json:"endpoints"`

	// TLS is the store's CA and the root client Lodia creates tenants with.
	// +required
	TLS DatastoreTLS `json:"tls"`

	// Prefix under which every tenant gets its own prefix.
	// +optional
	// +kubebuilder:default="/lodia"
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^/[a-z0-9/-]*$`
	Prefix string `json:"prefix,omitempty"`
}

// DatastoreTLS names the Secrets for a shared store.
type DatastoreTLS struct {
	// CASecretRef holds ca.crt.
	// +required
	CASecretRef SecretReference `json:"caSecretRef"`

	// RootClientSecretRef holds tls.crt and tls.key of a client allowed to
	// manage users and roles.
	// +required
	RootClientSecretRef SecretReference `json:"rootClientSecretRef"`
}
