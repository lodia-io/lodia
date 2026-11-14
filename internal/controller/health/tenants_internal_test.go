// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func kubeconfigForTLS(t *testing.T, server *httptest.Server) []byte {
	t.Helper()
	out, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"tenant": {Server: server.URL, CertificateAuthorityData: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": {}},
		Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
		CurrentContext: "tenant",
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func serveTenant(t *testing.T, handler http.Handler) []byte {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return kubeconfigFor(t, server.URL)
}

func tenantOf(t *testing.T, handler http.Handler) (*NodesReconciler, *tenant) {
	t.Helper()
	r := &NodesReconciler{Timeout: 10 * time.Minute, MaxNodes: 5000}
	tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, serveTenant(t, handler))
	if err != nil {
		t.Fatal(err)
	}
	return r, tenant
}

func restoreTenantOf(t *testing.T, handler http.Handler) *tenant {
	t.Helper()
	r := &Reconciler{Timeout: 10 * time.Minute}
	tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, serveTenant(t, handler))
	if err != nil {
		t.Fatal(err)
	}
	return tenant
}
