// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
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
	leases := func(server string) coordinationv1client.LeaseInterface {
		t.Helper()
		l, err := r.leases(key, kubeconfig(server))
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	first := leases("https://dev-kube-apiserver.team-a.svc:443")
	if leases("https://dev-kube-apiserver.team-a.svc:443") != first {
		t.Error("the same kubeconfig built a second Lease client")
	}
	second := leases("https://dev-kube-apiserver.team-a.svc:6443")
	if second == first {
		t.Error("a new kubeconfig kept the previous Lease client")
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
	if leases("https://dev-kube-apiserver.team-a.svc:6443") == second {
		t.Error("a forgotten ControlPlane kept its Lease client")
	}
}
