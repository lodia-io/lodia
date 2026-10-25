// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"strconv"
	"strings"
)

// The names of the objects in a tenant namespace that one controller writes
// and another reads: each is "<ControlPlane>-<suffix>". A name only its
// writer uses stays in the writer's package.

// Secret is a Secret one controller writes and others read, by its suffix.
type Secret string

// Written by PKI.
const (
	CABundleSecret                    Secret = "ca-bundle"
	ClientCASecret                    Secret = "client-ca"
	FrontProxyCASecret                Secret = "front-proxy-ca"
	EtcdCASecret                      Secret = "etcd-ca"
	KubeletSignerSecret               Secret = "kubelet-signer"
	APIServerServingSecret            Secret = "apiserver-serving"
	APIServerKubeletClientSecret      Secret = "apiserver-kubelet-client"
	APIServerEtcdClientSecret         Secret = "apiserver-etcd-client"
	FrontProxyClientSecret            Secret = "front-proxy-client"
	KonnectivityServerSecret          Secret = "konnectivity-server"
	KonnectivityKubeconfigSecret      Secret = "konnectivity-kubeconfig"
	ControllerManagerKubeconfigSecret Secret = "controller-manager-kubeconfig"
	SchedulerKubeconfigSecret         Secret = "scheduler-kubeconfig"
	BryggaKubeconfigSecret            Secret = "brygga-kubeconfig"
	AdminKubeconfigSecret             Secret = "admin-kubeconfig"
	ServiceAccountKeysSecret          Secret = "sa-keys"
	EncryptionConfigSecret            Secret = "encryption-config"
)

// Written by the Datastore controller.
const EtcdBackupSecret Secret = "etcd-backup"

// PKISecrets are the Secrets PKI writes that others read, the etcd
// certificate generations aside.
func PKISecrets() []Secret {
	return []Secret{
		CABundleSecret, ClientCASecret, FrontProxyCASecret, EtcdCASecret,
		KubeletSignerSecret, APIServerServingSecret, APIServerKubeletClientSecret, APIServerEtcdClientSecret,
		FrontProxyClientSecret, KonnectivityServerSecret, KonnectivityKubeconfigSecret,
		ControllerManagerKubeconfigSecret, SchedulerKubeconfigSecret, BryggaKubeconfigSecret, AdminKubeconfigSecret,
		ServiceAccountKeysSecret, EncryptionConfigSecret,
	}
}

// Name is the Secret's name for a ControlPlane.
func (s Secret) Name(controlPlane string) string { return controlPlane + "-" + string(s) }

// EtcdCertificateRole is a Secret of one generation of etcd certificates,
// which PKI writes and the Datastore controller names in the Etcd.
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
// etcd certificates name.
func EtcdName(controlPlane string) string { return controlPlane + "-etcd" }

// APIServerServiceName is the API server's in-namespace Service, which
// Workloads writes and PKI names in certificates and kubeconfigs.
func APIServerServiceName(controlPlane string) string { return controlPlane + "-kube-apiserver" }
