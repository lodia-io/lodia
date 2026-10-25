// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

// Condition types, each with its one writer.
const (
	ConditionCertificatesReady    = "CertificatesReady"    // PKI
	ConditionAccessReady          = "AccessReady"          // PKI
	ConditionCAValidityAcceptable = "CAValidityAcceptable" // PKI
	ConditionDatastoreReady       = "DatastoreReady"       // Datastore
	ConditionAPIServerReady       = "APIServerReady"       // Workloads
	ConditionAddonsApplied        = "AddonsApplied"        // Tenant
	ConditionAddonsAvailable      = "AddonsAvailable"      // Tenant
	ConditionReachable            = "Reachable"            // Health
	ConditionProgressing          = "Progressing"          // Lifecycle
	ConditionReady                = "Ready"                // Lifecycle
)

// EtcdCertificatesLabel's value is the generation of the etcd certificate
// Secret it marks.
const EtcdCertificatesLabel = "brygga.segla.io/etcd-certificates"
