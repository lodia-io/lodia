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

	"go.lodia.io/lodia/internal/certs"
)

func TestAControlPlaneKeepsItsClientsWhileTheirInputsStay(t *testing.T) {
	r := &Reconciler{}
	key := types.NamespacedName{Namespace: "team-a", Name: "dev"}
	kubeconfig := func(server string) []byte {
		t.Helper()
		out, err := clientcmd.Write(clientcmdapi.Config{
			Clusters:       map[string]*clientcmdapi.Cluster{"tenant": {Server: server}},
			AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": {Token: "token"}},
			Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
			CurrentContext: "tenant",
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	clients := func(server string) *tenant {
		t.Helper()
		c, err := r.tenant(key, kubeconfig(server))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	first := clients("https://dev-kube-apiserver.team-a.svc:443")
	for name, c := range map[string]rest.Interface{"core": first.core.RESTClient(), "admission": first.admission.RESTClient(), "registration": first.registration.RESTClient(), "batch": first.batch.RESTClient()} {
		if c.(*rest.RESTClient).Client != r.clients[key].transport {
			t.Errorf("the %s client does not use the transport forget closes", name)
		}
	}
	if clients("https://dev-kube-apiserver.team-a.svc:443") != first {
		t.Error("the same kubeconfig built second tenant clients")
	}
	second := clients("https://dev-kube-apiserver.team-a.svc:6443")
	if second == first {
		t.Error("a new kubeconfig kept the previous tenant clients")
	}

	ca, err := certs.NewCA("health-clients", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := certs.CertPEM(ca.Cert)
	if err != nil {
		t.Fatal(err)
	}
	prober := r.prober(key, bundle)
	if prober == nil || r.prober(key, bundle) != prober {
		t.Error("the same CA bundle built a second prober")
	}
	if r.prober(key, []byte("no certificate")) != nil {
		t.Error("a bundle without a certificate got a prober")
	}

	r.forget(key)
	if clients("https://dev-kube-apiserver.team-a.svc:6443") == second {
		t.Error("a forgotten ControlPlane kept its tenant clients")
	}
}

func TestTheLeaseClientNamesItself(t *testing.T) {
	var agent atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		agent.Store(req.UserAgent())
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
	r := &Reconciler{}
	tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = tenant.leases.Get(t.Context(), "kube-scheduler", metav1.GetOptions{})
	if got, want := agent.Load(), rest.DefaultKubernetesUserAgent(); got != want {
		t.Errorf("the Lease client sends the User-Agent %q, want %q", got, want)
	}
}
