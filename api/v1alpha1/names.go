// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// The names of the objects in a tenant namespace that one controller writes
// and another reads: each is "<ControlPlane>-<suffix>". A name only its
// writer uses stays in the writer's package.

// Secret is a Secret one controller writes and others read, by its suffix.
// Each group below is written by PKI, with the keys its readers name.
type Secret string

// The CA bundles: their CAs' PEM certificates under CACertKey.
const (
	CABundleSecret     Secret = "ca-bundle"
	ClientCASecret     Secret = "client-ca"
	FrontProxyCASecret Secret = "front-proxy-ca"
	EtcdCASecret       Secret = "etcd-ca"

	CACertKey = "ca.crt"
)

// The certificates, kubernetes.io/tls Secrets: a PEM certificate under
// TLSCertKey and its key under TLSPrivateKeyKey.
const (
	KubeletSignerSecret          Secret = "kubelet-signer"
	APIServerServingSecret       Secret = "apiserver-serving"
	APIServerKubeletClientSecret Secret = "apiserver-kubelet-client"
	APIServerEtcdClientSecret    Secret = "apiserver-etcd-client"
	FrontProxyClientSecret       Secret = "front-proxy-client"
	KonnectivityServerSecret     Secret = "konnectivity-server"

	TLSCertKey       = corev1.TLSCertKey
	TLSPrivateKeyKey = corev1.TLSPrivateKeyKey
)

// The kubeconfigs, under KubeconfigKey. A component's names its certificate,
// key and CA as the files TLSCertKey, TLSPrivateKeyKey and CACertKey beside
// it in the same Secret, which is mounted whole; Lodia's and the admin's
// embed them.
const (
	KonnectivityKubeconfigSecret      Secret = "konnectivity-kubeconfig"
	ControllerManagerKubeconfigSecret Secret = "controller-manager-kubeconfig"
	SchedulerKubeconfigSecret         Secret = "scheduler-kubeconfig"
	LodiaKubeconfigSecret             Secret = "lodia-kubeconfig"
	AdminKubeconfigSecret             Secret = "admin-kubeconfig"

	KubeconfigKey = "kubeconfig"
)

// The ServiceAccount signing key, PEM under SigningKeyKey, which the
// lodia-signer sidecar reads.
const (
	ServiceAccountKeysSecret Secret = "sa-keys"

	SigningKeyKey = "signing.key"
)

// The API server's EncryptionConfiguration, under EncryptionConfigKey.
const (
	EncryptionConfigSecret Secret = "encryption-config"

	EncryptionConfigKey = "encryption-config.yaml"
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
		ControllerManagerKubeconfigSecret, SchedulerKubeconfigSecret, LodiaKubeconfigSecret, AdminKubeconfigSecret,
		ServiceAccountKeysSecret, EncryptionConfigSecret,
	}
}

// Name is the Secret's name for a ControlPlane.
func (s Secret) Name(controlPlane string) string { return controlPlane + "-" + string(s) }

// EtcdCertificateRole is a Secret of one generation of etcd certificates,
// which PKI writes and the Datastore controller names in the Etcd: the CA
// under CACertKey, the others kubernetes.io/tls Secrets.
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

// The values beside the names that one controller renders and another relies
// on.
const (
	// APIServerServicePort is APIServerServiceName's port for the API server,
	// which PKI's kubeconfigs dial.
	APIServerServicePort = 443

	// FrontProxyClientCommonName is the common name of the front-proxy client
	// certificate PKI issues, which the API server allows to set request
	// headers.
	FrontProxyClientCommonName = "front-proxy-client"

	// APIServerReadyzPath is the API server's readiness endpoint, which Health
	// probes and Workloads lets anonymous requests read.
	APIServerReadyzPath = "/readyz"

	// APIServerComponent is the ComponentLabel of the API server's pods,
	// which Workloads writes and the Datastore controller's etcd
	// NetworkPolicy admits.
	APIServerComponent = "kube-apiserver"
)
