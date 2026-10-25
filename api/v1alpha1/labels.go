// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

// The labels, annotations and finalizer Brygga's controllers and its users
// share, each with its writer.
const (
	// ControlPlaneLabel marks every object Brygga creates for a control
	// plane; its value is the ControlPlane's name. Every controller writes it
	// on its objects; the operator's cache holds only labelled objects.
	ControlPlaneLabel = "brygga.segla.io/control-plane"

	// ControlPlaneUIDLabel on an etcd claim, written by the Datastore
	// controller, keeps a ControlPlane recreated under the same name from
	// starting on the old one's data.
	ControlPlaneUIDLabel = "brygga.segla.io/control-plane-uid"

	// EtcdCertificatesLabel, written by PKI, marks an etcd certificate
	// Secret with its generation, which the Datastore controller counts.
	EtcdCertificatesLabel = "brygga.segla.io/etcd-certificates"

	// DefaultClassAnnotation "true", set by the platform, makes a
	// DatastoreClass the default.
	DefaultClassAnnotation = "brygga.segla.io/is-default-class"

	// DatastoreClassAnnotation and DatastoreClassGenerationAnnotation, written
	// by the Datastore controller on an Etcd and its claims, record the class
	// and the generation of it the Etcd was rendered from.
	DatastoreClassAnnotation           = "brygga.segla.io/datastore-class"
	DatastoreClassGenerationAnnotation = "brygga.segla.io/datastore-class-generation"

	// DataHashAnnotation, written by PKI on every Secret it writes, is the
	// hex SHA-256 of the JSON of the Secret's data, so that Workloads hashes a
	// restart-only Secret into a pod template from its metadata alone.
	DataHashAnnotation = "brygga.segla.io/data-hash"

	// OperationAnnotation, set by the platform, requests an operation such
	// as a rotation (docs/decisions/one-controlplane-per-cluster.md).
	OperationAnnotation = "brygga.segla.io/operation"

	// DatastoreFinalizer, held by the Datastore controller, keeps a
	// ControlPlane until its etcd is deleted and its claims reclaimed.
	DatastoreFinalizer = "brygga.segla.io/datastore"
)
