// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantclient

import (
	"bytes"
	"crypto/x509"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"go.lodia.io/lodia/internal/certs"
)

const keptClientAtMost = 16 << 10

func TestMeasureWhatAKeptClientOfATenantHolds(t *testing.T) {
	if os.Getenv("LODIA_MEASURE_HEAP") == "" {
		t.Skip("LODIA_MEASURE_HEAP is not set")
	}
	ca, err := certs.NewCA("cluster", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue(certs.Profile{CommonName: "lodia:operator", Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, err := certs.CertPEM(ca.Cert)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := certs.CertPEM(leaf.Cert)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := certs.KeyPEM(leaf.Key)
	if err != nil {
		t.Fatal(err)
	}
	kubeconfig, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"tenant": {Server: "https://dev-kube-apiserver.team-a.svc:443", CertificateAuthorityData: caPEM}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": {ClientCertificateData: certPEM, ClientKeyData: keyPEM}},
		Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
		CurrentContext: "tenant",
	})
	if err != nil {
		t.Fatal(err)
	}
	const tenants = 200
	clients := NewClients(Options{Connections: 1, Timeout: 10 * time.Second})
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := range tenants {
		if _, err := clients.Of(types.NamespacedName{Namespace: "team-" + strconv.Itoa(i), Name: "dev"}, bytes.Clone(kubeconfig)); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(clients)
	held := (int64(after.HeapAlloc) - int64(before.HeapAlloc)) / tenants
	t.Logf("a kept client holds %d bytes of live heap, its kubeconfig of %d bytes included, over %d tenants", held, len(kubeconfig), tenants)
	if held > keptClientAtMost {
		t.Errorf("a kept client holds %d bytes, want at most %d", held, keptClientAtMost)
	}
}
