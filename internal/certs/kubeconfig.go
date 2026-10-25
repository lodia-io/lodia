// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package certs

import (
	"errors"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// Kubeconfig renders a kubeconfig for user at server. With embed the CA, the
// certificate and the key are inline; otherwise they are the files ca.crt,
// tls.crt and tls.key beside the kubeconfig, which client-go reloads.
func Kubeconfig(cluster, server, user string, ca, cert, key []byte, embed bool) ([]byte, error) {
	c := clientcmdapi.NewCluster()
	c.Server = server
	u := clientcmdapi.NewAuthInfo()
	if embed {
		c.CertificateAuthorityData, u.ClientCertificateData, u.ClientKeyData = ca, cert, key
	} else {
		c.CertificateAuthority, u.ClientCertificate, u.ClientKey = "ca.crt", "tls.crt", "tls.key"
	}
	context := user + "@" + cluster
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters[cluster] = c
	cfg.AuthInfos[user] = u
	cfg.Contexts[context] = &clientcmdapi.Context{Cluster: cluster, AuthInfo: user}
	cfg.CurrentContext = context
	return clientcmd.Write(*cfg)
}

// Embedded returns the client certificate and key inline in a kubeconfig.
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
