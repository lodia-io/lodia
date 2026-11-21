// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// Dial connects to the etcd of s through its client Service, as the API
// server does, with the client certificate and CA the Etcd names, read from
// reader; the caller closes it.
func (e *Engine) Dial(ctx context.Context, reader client.Reader, s *Store) (*clientv3.Client, error) {
	etcd := s.etcd
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
