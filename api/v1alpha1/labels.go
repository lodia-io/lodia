// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"maps"
	"slices"
)

// The labels, annotations and finalizer Lodia's controllers and its users
// share, each with its writer.
const (
	// ControlPlaneLabel marks every object Lodia creates for a control
	// plane; its value is the ControlPlane's name. Every controller writes it
	// on its objects; the operator's cache holds only labelled objects.
	ControlPlaneLabel = "lodia.io/control-plane"

	// ManagedByLabel: ManagedBy marks every object Lodia writes inside a
	// tenant cluster.
	ManagedByLabel = "app.kubernetes.io/managed-by"
	ManagedBy      = "lodia"

	// ComponentLabel, written by Workloads on the control plane's pods,
	// names their component.
	ComponentLabel = "app.kubernetes.io/name"

	// ControlPlaneUIDLabel on an etcd claim, written by the Datastore
	// controller, keeps a ControlPlane recreated under the same name from
	// starting on the old one's data.
	ControlPlaneUIDLabel = "lodia.io/control-plane-uid"

	// EtcdCertificatesLabel, written by PKI, marks an etcd certificate
	// Secret with its generation, which the Datastore controller counts.
	EtcdCertificatesLabel = "lodia.io/etcd-certificates"

	// DefaultClassAnnotation "true", set by the platform, makes a
	// DatastoreClass the default.
	DefaultClassAnnotation = "lodia.io/is-default-class"

	// DatastoreClassAnnotation, written by the Datastore controller on an
	// Etcd and its claims, records the class the Etcd was rendered from.
	DatastoreClassAnnotation = "lodia.io/datastore-class"

	// DatastoreClassGenerationAnnotation, written by the Datastore controller
	// on an Etcd, records the generation of the class it was rendered from.
	DatastoreClassGenerationAnnotation = "lodia.io/datastore-class-generation"

	// DataHashAnnotation, written by PKI on every Secret it writes, holds
	// DataHash of the Secret's data, so that Workloads hashes a restart-only
	// Secret into a pod template from its metadata alone.
	DataHashAnnotation = "lodia.io/data-hash"

	// OperationAnnotation, set by the platform, requests an operation such
	// as a rotation (docs/decisions/one-controlplane-per-cluster.md).
	OperationAnnotation = "lodia.io/operation"

	// DatastoreFinalizer, held by the Datastore controller, keeps a
	// ControlPlane until its etcd is deleted and its claims reclaimed.
	DatastoreFinalizer = "lodia.io/datastore"
)

// DataHash is the hex SHA-256 over a Secret's keys in order, each key and
// its value prefixed by its length.
func DataHash(data map[string][]byte) string {
	var b []byte
	for _, key := range slices.Sorted(maps.Keys(data)) {
		b = binary.BigEndian.AppendUint64(b, uint64(len(key)))
		b = append(b, key...)
		b = binary.BigEndian.AppendUint64(b, uint64(len(data[key])))
		b = append(b, data[key]...)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
