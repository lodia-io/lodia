// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

// The condition types of a ControlPlane that more than one controller reads;
// each is written by exactly one controller.
const (
	// ConditionCertificatesReady is PKI's: every certificate, key and
	// kubeconfig exists and is valid beyond its renewal time.
	ConditionCertificatesReady = "CertificatesReady"
	// ConditionAccessReady is PKI's: the tenant's bindings for Brygga, the
	// admin group and the kubelet client exist.
	ConditionAccessReady = "AccessReady"
	// ConditionAPIServerReady is Workloads': every API server replica answers.
	ConditionAPIServerReady = "APIServerReady"
)

// EtcdCertificatesLabel marks the five Secrets of one generation of a
// tenant's etcd certificates; its value is the generation number.
const EtcdCertificatesLabel = "brygga.segla.io/etcd-certificates"
