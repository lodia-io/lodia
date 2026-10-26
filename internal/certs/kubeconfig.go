// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package certs

import (
	"errors"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func Kubeconfig(cluster, server, user string, ca, cert, key []byte, embed bool) ([]byte, error) {
	c := clientcmdapi.NewCluster()
	c.Server = server
	u := clientcmdapi.NewAuthInfo()
	if embed {
		c.CertificateAuthorityData, u.ClientCertificateData, u.ClientKeyData = ca, cert, key
	} else {
		c.CertificateAuthority, u.ClientCertificate, u.ClientKey = lodiav1alpha1.CACertKey, lodiav1alpha1.TLSCertKey, lodiav1alpha1.TLSPrivateKeyKey
	}
	context := user + "@" + cluster
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters[cluster] = c
	cfg.AuthInfos[user] = u
	cfg.Contexts[context] = &clientcmdapi.Context{Cluster: cluster, AuthInfo: user}
	cfg.CurrentContext = context
	return clientcmd.Write(*cfg)
}

func Embedded(kubeconfig []byte) (cert, key []byte, err error) {
	cfg, err := clientcmd.Load(kubeconfig)
	if err != nil {
		return nil, nil, err
	}
	for _, u := range cfg.AuthInfos {
		return u.ClientCertificateData, u.ClientKeyData, nil
	}
	return nil, nil, errors.New("no user")
}
