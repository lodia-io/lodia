// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestTheTenantClientNamesItselfAndAsksForJSON(t *testing.T) {
	var agent, accept atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		agent.Store(req.UserAgent())
		accept.Store(req.Header.Get("Accept"))
		http.NotFound(w, req)
	}))
	t.Cleanup(server.Close)
	kubeconfig, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"tenant": {Server: server.URL}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": {}},
		Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
		CurrentContext: "tenant",
	})
	if err != nil {
		t.Fatal(err)
	}
	r := &Reconciler{Timeout: time.Minute}
	tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = tenant.getLease(t.Context(), "kube-scheduler")
	_, _ = tenant.leasePage(t.Context(), metav1.ListOptions{Limit: leasePage})
	_, _ = tenant.nodePage(t.Context(), metav1.ListOptions{Limit: nodePage})
	if got, want := agent.Load(), rest.DefaultKubernetesUserAgent(); got != want {
		t.Errorf("the tenant client sends the User-Agent %q, want %q", got, want)
	}
	if got := accept.Load(); got != "application/json" {
		t.Errorf("the tenant client accepts %q, want application/json only", got)
	}
}
