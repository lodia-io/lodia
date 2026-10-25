// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package datastoretest stands in for the PKI controller in the Datastore
// controller's envtest suite.
package datastoretest

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"maps"
	"math/big"
	"net"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
)

func Generation(controlPlane *bryggav1alpha1.ControlPlane, g int) ([]*corev1.Secret, error) {
	namespace, cp := controlPlane.Namespace, controlPlane.Name
	ca, caKey, caPEM, err := certificateAuthority()
	if err != nil {
		return nil, err
	}
	etcd := bryggav1alpha1.EtcdName(cp)
	service := func(names ...string) []string {
		var dns []string
		for _, name := range names {
			dns = append(dns, name, name+"."+namespace, name+"."+namespace+".svc", name+"."+namespace+".svc.cluster.local")
		}
		return dns
	}
	serverAndClient := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	loopback := []net.IP{net.IPv4(127, 0, 0, 1)}
	leaves := []struct {
		role       bryggav1alpha1.EtcdCertificateRole
		commonName string
		usages     []x509.ExtKeyUsage
		dns        []string
		ips        []net.IP
	}{
		{bryggav1alpha1.EtcdServerRole, etcd, serverAndClient, append(append([]string{etcd + "-local"}, service(etcd+"-client", "*."+etcd+"-peer")...), "localhost"), loopback},
		{bryggav1alpha1.EtcdPeerRole, etcd + "-peer", serverAndClient, service(etcd+"-peer", "*."+etcd+"-peer"), nil},
		{bryggav1alpha1.EtcdBackupRestoreRole, etcd + "-backup-restore", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, append(append([]string{etcd + "-local"}, service(etcd+"-client")...), "localhost"), loopback},
		{bryggav1alpha1.EtcdClientRole, etcd + "-client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, nil},
	}
	labels := map[string]string{bryggav1alpha1.ControlPlaneLabel: cp, bryggav1alpha1.EtcdCertificatesLabel: strconv.Itoa(g)}
	owner := metav1.NewControllerRef(controlPlane, bryggav1alpha1.GroupVersion.WithKind("ControlPlane"))
	secrets := []*corev1.Secret{{
		ObjectMeta: metav1.ObjectMeta{Name: bryggav1alpha1.EtcdCertificateSecretName(cp, bryggav1alpha1.EtcdCARole, g), Namespace: namespace, Labels: maps.Clone(labels), OwnerReferences: []metav1.OwnerReference{*owner}},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{bryggav1alpha1.CACertKey: caPEM},
	}}
	for _, l := range leaves {
		cert, key, err := leaf(ca, caKey, l.commonName, l.usages, l.dns, l.ips)
		if err != nil {
			return nil, fmt.Errorf("issue the %s certificate: %w", l.role, err)
		}
		secrets = append(secrets, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: bryggav1alpha1.EtcdCertificateSecretName(cp, l.role, g), Namespace: namespace, Labels: maps.Clone(labels), OwnerReferences: []metav1.OwnerReference{*owner}},
			Type:       corev1.SecretTypeTLS,
			Data:       map[string][]byte{bryggav1alpha1.TLSCertKey: cert, bryggav1alpha1.TLSPrivateKeyKey: key},
		})
	}
	return secrets, nil
}

func certificateAuthority() (*x509.Certificate, *rsa.PrivateKey, []byte, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, nil, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "etcd-ca"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.AddDate(10, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, nil, err
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, err
	}
	return ca, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

func leaf(ca *x509.Certificate, caKey *rsa.PrivateKey, commonName string, usages []x509.ExtKeyUsage, dns []string, ips []net.IP) (cert, key []byte, err error) {
	private, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.AddDate(1, 0, 0),
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           usages,
		DNSNames:              dns,
		IPAddresses:           ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &private.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}
