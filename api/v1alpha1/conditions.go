// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

// Condition types more than one controller reads, each with its one writer.
const (
	ConditionCertificatesReady    = "CertificatesReady"    // PKI
	ConditionAccessReady          = "AccessReady"          // PKI
	ConditionCAValidityAcceptable = "CAValidityAcceptable" // PKI
	ConditionAPIServerReady       = "APIServerReady"       // Workloads
)

// EtcdCertificatesLabel's value is the generation of the etcd certificate
// Secret it marks.
const EtcdCertificatesLabel = "brygga.segla.io/etcd-certificates"
