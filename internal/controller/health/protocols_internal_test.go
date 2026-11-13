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

type protocol struct {
	name  string
	http2 bool
}

var (
	http1     = protocol{"HTTP/1.1", false}
	http2     = protocol{"HTTP/2", true}
	protocols = []protocol{http1, http2}
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

// serve starts a server that answers with handler over the protocol, HTTP/2
// over TLS as a kube-apiserver does, and returns a kubeconfig for it.
func (p protocol) serve(t *testing.T, handler http.Handler) []byte {
	t.Helper()
	wanted := 1
	if p.http2 {
		wanted = 2
	}
	checked := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.ProtoMajor != wanted {
			t.Errorf("the request came over HTTP/%d, want HTTP/%d", req.ProtoMajor, wanted)
		}
		handler.ServeHTTP(w, req)
	})
	if !p.http2 {
		server := httptest.NewServer(checked)
		t.Cleanup(server.Close)
		return kubeconfigFor(t, server.URL)
	}
	server := httptest.NewUnstartedServer(checked)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	return kubeconfigForTLS(t, server)
}

func (p protocol) tenant(t *testing.T, handler http.Handler) (*NodesReconciler, *tenant) {
	t.Helper()
	r := &NodesReconciler{Timeout: 10 * time.Minute, MaxNodes: 5000}
	tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, p.serve(t, handler))
	if err != nil {
		t.Fatal(err)
	}
	return r, tenant
}
