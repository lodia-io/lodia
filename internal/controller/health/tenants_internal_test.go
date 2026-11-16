// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func kubeconfigWithCA(t *testing.T, server string, certificate *x509.Certificate) []byte {
	t.Helper()
	out, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"tenant": {Server: server, CertificateAuthorityData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": {}},
		Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
		CurrentContext: "tenant",
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func kubeconfigForTLS(t *testing.T, server *httptest.Server) []byte {
	t.Helper()
	return kubeconfigWithCA(t, server.URL, server.Certificate())
}

func localTLS(t *testing.T) (*tls.Config, *x509.Certificate) {
	t.Helper()
	server := httptest.NewUnstartedServer(http.NotFoundHandler())
	server.StartTLS()
	defer server.Close()
	return &tls.Config{Certificates: server.TLS.Certificates, MinVersion: tls.VersionTLS12}, server.Certificate()
}

func serveTenant(t *testing.T, handler http.Handler) []byte {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return kubeconfigFor(t, server.URL)
}

func serveTenantTLS(t *testing.T, handler http.Handler) []byte {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	return kubeconfigForTLS(t, server)
}

func tenantOf(t *testing.T, handler http.Handler) (*NodesReconciler, *tenant) {
	t.Helper()
	return tenantAt(t, serveTenant(t, handler))
}

func tenantOverTLS(t *testing.T, handler http.Handler) (*NodesReconciler, *tenant) {
	t.Helper()
	return tenantAt(t, serveTenantTLS(t, handler))
}

func tenantAt(t *testing.T, kubeconfig []byte) (*NodesReconciler, *tenant) {
	t.Helper()
	r := &NodesReconciler{Timeout: 10 * time.Minute, MaxNodes: 5000}
	tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	return r, tenant
}

func restoreTenantOf(t *testing.T, handler http.Handler) *tenant {
	t.Helper()
	return restoreTenantAt(t, serveTenant(t, handler))
}

func restoreTenantOverTLS(t *testing.T, handler http.Handler) *tenant {
	t.Helper()
	return restoreTenantAt(t, serveTenantTLS(t, handler))
}

func restoreTenantAt(t *testing.T, kubeconfig []byte) *tenant {
	t.Helper()
	r := &Reconciler{Timeout: 10 * time.Minute}
	tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	return tenant
}
