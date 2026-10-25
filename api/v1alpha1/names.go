// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import "strconv"

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
var PKISecrets = []Secret{
	CABundleSecret, ClientCASecret, FrontProxyCASecret, EtcdCASecret,
	KubeletSignerSecret, APIServerServingSecret, APIServerKubeletClientSecret, APIServerEtcdClientSecret,
	FrontProxyClientSecret, KonnectivityServerSecret, KonnectivityKubeconfigSecret,
	ControllerManagerKubeconfigSecret, SchedulerKubeconfigSecret, BryggaKubeconfigSecret, AdminKubeconfigSecret,
	ServiceAccountKeysSecret, EncryptionConfigSecret,
}

// Name is the Secret's name for a ControlPlane.
func (s Secret) Name(controlPlane string) string { return controlPlane + "-" + string(s) }

// EtcdCertificateRoles are the Secrets of one generation of etcd
// certificates, which PKI writes and the Datastore controller names in the
// Etcd.
var EtcdCertificateRoles = []string{"ca", "server", "client", "peer", "backup-restore"}

// EtcdCertificateSecretName is "<ControlPlane>-etcd-<role>-<generation>".
func EtcdCertificateSecretName(controlPlane, role string, generation int) string {
	return controlPlane + "-etcd-" + role + "-" + strconv.Itoa(generation)
}

// EtcdName is the Etcd the Datastore controller keeps, whose members PKI's
// etcd certificates name.
func EtcdName(controlPlane string) string { return controlPlane + "-etcd" }

// APIServerName is the API server's Deployment and in-namespace Service,
// which Workloads writes and PKI names in certificates and kubeconfigs.
func APIServerName(controlPlane string) string { return controlPlane + "-kube-apiserver" }

// ControllerManagerName is the controller manager's Deployment.
func ControllerManagerName(controlPlane string) string { return controlPlane + "-kube-controller-manager" }

// SchedulerName is the scheduler's Deployment.
func SchedulerName(controlPlane string) string { return controlPlane + "-kube-scheduler" }
