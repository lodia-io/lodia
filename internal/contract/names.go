// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package contract holds the names and values that more than one of Lodia's
// controllers share and no importer relies on, each with its writer; what an
// importer may rely on is in api/v1alpha1
// (docs/decisions/independent-controllers.md).
package contract

import (
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// EtcdCertificateRole is a Secret of one generation of etcd certificates,
// which PKI writes and the Datastore controller names in the Etcd: the CA
// under v1alpha1.CACertKey, the others kubernetes.io/tls Secrets.
type EtcdCertificateRole string

const (
	EtcdCARole            EtcdCertificateRole = "ca"
	EtcdServerRole        EtcdCertificateRole = "server"
	EtcdClientRole        EtcdCertificateRole = "client"
	EtcdPeerRole          EtcdCertificateRole = "peer"
	EtcdBackupRestoreRole EtcdCertificateRole = "backup-restore"
)

// EtcdCertificateRoles are the Secrets of one generation.
func EtcdCertificateRoles() []EtcdCertificateRole {
	return []EtcdCertificateRole{EtcdCARole, EtcdServerRole, EtcdClientRole, EtcdPeerRole, EtcdBackupRestoreRole}
}

// EtcdCertificateSecretName is "<ControlPlane>-etcd-<role>-<generation>".
func EtcdCertificateSecretName(controlPlane string, role EtcdCertificateRole, generation int) string {
	return controlPlane + "-etcd-" + string(role) + "-" + strconv.Itoa(generation)
}

// ParseEtcdCertificateSecretName is the inverse of EtcdCertificateSecretName:
// a name of the ControlPlane's etcd certificates, with a generation above
// zero written without leading zeros, gives its role and generation.
func ParseEtcdCertificateSecretName(controlPlane, name string) (EtcdCertificateRole, int, bool) {
	rest, ok := strings.CutPrefix(name, controlPlane+"-etcd-")
	if !ok {
		return "", 0, false
	}
	for _, role := range EtcdCertificateRoles() {
		if n, ok := strings.CutPrefix(rest, string(role)+"-"); ok {
			if g, err := strconv.Atoi(n); err == nil && g > 0 && strconv.Itoa(g) == n {
				return role, g, true
			}
			return "", 0, false
		}
	}
	return "", 0, false
}

// EtcdName is the Etcd the Datastore controller keeps, whose members PKI's
// etcd certificates name and whose samples the metrics collector serves.
func EtcdName(controlPlane string) string { return controlPlane + "-etcd" }

// BackupSealKey is the etcd key the backup reconciler puts into the
// source's etcd before it copies the Backup, and that the Datastore
// controller requires in the restored etcd.
func BackupSealKey(backup types.UID) string { return "/lodia.io/backups/" + string(backup) }

// RestoreMarkerKey is the etcd key of the Datastore controller's cleanup of
// a restored etcd: its one transaction deletes what bound the data to the
// source unless the key exists, and creates it.
func RestoreMarkerKey(controlPlane types.UID) string {
	return "/lodia.io/restores/" + string(controlPlane)
}

// The values beside the names that one controller renders and another relies
// on.
const (
	// FrontProxyClientCommonName is the common name of the front-proxy client
	// certificate PKI issues, which the API server allows to set request
	// headers.
	FrontProxyClientCommonName = "front-proxy-client"

	// APIServerReadyzPath is the API server's readiness endpoint, which Health
	// probes and Workloads lets anonymous requests read.
	APIServerReadyzPath = "/readyz"

	// ControllerManagerLease and SchedulerLease are the components' leader
	// Leases in LeaderElectionNamespace inside the tenant: Workloads names
	// them in the components' flags, and Health reads them.
	ControllerManagerLease  = "kube-controller-manager"
	SchedulerLease          = "kube-scheduler"
	LeaderElectionNamespace = "kube-system"

	// HoldHolderIdentity is the holder Health writes into
	// ControllerManagerLease while it holds the controller manager.
	HoldHolderIdentity = "lodia"
)

// The controller manager's timings, which Workloads renders as its flags and
// the operator checks Health's thresholds against when it starts.
const (
	NodeMonitorGracePeriod         = 60 * time.Second
	NodeMonitorPeriod              = 5 * time.Second
	ControllerManagerLeaseDuration = 15 * time.Second
	ControllerManagerRenewDeadline = 10 * time.Second
	ControllerManagerRetryPeriod   = 2 * time.Second
)
