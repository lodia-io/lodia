// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strconv"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// etcdRequestTimeout bounds every call to a tenant's etcd.
const etcdRequestTimeout = 10 * time.Second

// DialEtcd connects to the etcd of an Etcd through its client Service, as the
// API server does, with the client certificate and CA the Etcd names.
func DialEtcd(ctx context.Context, reader client.Reader, etcd *druidv1alpha1.Etcd) (*clientv3.Client, error) {
	refs := etcd.Spec.Etcd.ClientUrlTLS
	if refs == nil {
		return nil, fmt.Errorf("no client TLS in Etcd %s", etcd.Name)
	}
	read := func(name string) (*corev1.Secret, error) {
		secret := &corev1.Secret{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: etcd.Namespace, Name: name}, secret); err != nil {
			return nil, fmt.Errorf("read Secret %s: %w", name, err)
		}
		return secret, nil
	}
	leaf, err := read(refs.ClientTLSSecretRef.Name)
	if err != nil {
		return nil, err
	}
	certificate, err := tls.X509KeyPair(leaf.Data[corev1.TLSCertKey], leaf.Data[corev1.TLSPrivateKeyKey])
	if err != nil {
		return nil, fmt.Errorf("read the key pair in Secret %s: %w", leaf.Name, err)
	}
	bundle, err := read(refs.TLSCASecretRef.Name)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle.Data[ptr.Deref(refs.TLSCASecretRef.DataKey, lodiav1alpha1.CACertKey)]) {
		return nil, fmt.Errorf("no CA certificate in Secret %s", bundle.Name)
	}
	host, port := clientEndpoint(etcd)
	return clientv3.New(clientv3.Config{
		Context:     ctx,
		Endpoints:   []string{"https://" + host + ":" + strconv.Itoa(int(port))},
		DialTimeout: 5 * time.Second,
		TLS:         &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: roots, MinVersion: tls.VersionTLS12},
	})
}

// clientEndpoint is etcd-druid's client Service of the Etcd
// (etcd-druid@v0.39.0 api/core/v1alpha1/helper.go#L27-L30).
func clientEndpoint(etcd *druidv1alpha1.Etcd) (string, int32) {
	return druidv1alpha1.GetClientServiceName(etcd.ObjectMeta) + "." + etcd.Namespace + ".svc", druidv1alpha1.GetClientPort(etcd)
}
