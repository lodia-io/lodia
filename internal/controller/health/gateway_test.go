// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
	"go.lodia.io/lodia/internal/controller/health"
)

type tunnelEndpoint struct {
	*httptest.Server
	address   string
	clientCAs atomic.Pointer[x509.CertPool]
	code      atomic.Int32
	hang      atomic.Bool
}

type gatewayFixture struct {
	api          *endpoint
	tunnel       *tunnelEndpoint
	konnectivity string
	clientCA     *certs.Certificate
	cp           *lodiav1alpha1.ControlPlane
}

func leafPair(t *testing.T, ca *certs.Certificate, usages ...x509.ExtKeyUsage) (tls.Certificate, []byte, []byte) {
	t.Helper()
	now := time.Now()
	leaf, err := ca.Issue(certs.Profile{CommonName: "lodia-tunnel-client", IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, Usages: usages}, now, time.Hour)
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
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return pair, certPEM, keyPEM
}

func serveTunnel(t *testing.T, ca, clientCA *certs.Certificate) *tunnelEndpoint {
	t.Helper()
	return serveTunnelWithInnerCA(t, ca, clientCA, ca)
}

func serveTunnelWithInnerCA(t *testing.T, ca, clientCA, innerCA *certs.Certificate) *tunnelEndpoint {
	t.Helper()
	serving, _, _ := leafPair(t, ca, x509.ExtKeyUsageServerAuth)
	apiServing, _, _ := leafPair(t, innerCA, x509.ExtKeyUsageServerAuth)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e := &tunnelEndpoint{address: listener.Addr().String()}
	e.code.Store(http.StatusOK)
	pool := x509.NewCertPool()
	pool.AddCert(clientCA.Cert)
	e.clientCAs.Store(pool)
	outer := tls.NewListener(listener, &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serving}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: e.clientCAs.Load()}, nil
		},
	})
	e.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if e.hang.Load() {
			<-r.Context().Done()
			return
		}
		if r.URL.Path != "/readyz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(int(e.code.Load()))
	}))
	_ = e.Listener.Close()
	e.Listener = outer
	e.TLS = &tls.Config{Certificates: []tls.Certificate{apiServing}, MinVersion: tls.VersionTLS12}
	e.StartTLS()
	t.Cleanup(e.Close)
	return e
}

func serveTLS(t *testing.T, ca *certs.Certificate) string {
	t.Helper()
	pair, _, _ := leafPair(t, ca, x509.ExtKeyUsageServerAuth)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_ = conn.(*tls.Conn).HandshakeContext(context.Background())
			}()
		}
	}()
	return listener.Addr().String()
}

func stallListener(t *testing.T) (string, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	closed := make(chan struct{}, 8)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				_, _ = io.Copy(io.Discard, conn)
				closed <- struct{}{}
				_ = conn.Close()
			}()
		}
	}()
	return listener.Addr().String(), closed
}

func deadAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func gatewayControlPlane(t *testing.T) *lodiav1alpha1.ControlPlane {
	t.Helper()
	return controlPlaneWith(t, lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
		ParentRef: lodiav1alpha1.ParentReference{Name: "tenants", Namespace: "gateway", SectionName: "tls"},
		Hostnames: lodiav1alpha1.GatewayHostnames{APIServer: "api.dev.lodia.test", Konnectivity: "konnectivity.dev.lodia.test", Tunnel: "tunnel.dev.lodia.test"},
	}})
}

func newGateway(t *testing.T) *gatewayFixture {
	t.Helper()
	cp := gatewayControlPlane(t)
	api := serve(t)
	ca := api.ca
	clientCA, err := certs.NewCA("tunnel-ca", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	_, certPEM, keyPEM := leafPair(t, clientCA, x509.ExtKeyUsageClientAuth)
	f := &gatewayFixture{api: api, tunnel: serveTunnel(t, ca, clientCA), konnectivity: serveTLS(t, ca), clientCA: clientCA, cp: cp}
	writeBundle(t, cp, api)
	if err := k8s.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: lodiav1alpha1.TunnelClientSecret.Name(cp.Name), Namespace: cp.Namespace},
		Data:       map[string][]byte{corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM},
	}); err != nil {
		t.Fatal(err)
	}
	return f
}

func hostPortOf(t *testing.T, address string) map[string]any {
	t.Helper()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{"host": host, "port": int64(p)}
}

func publishGateway(t *testing.T, cp *lodiav1alpha1.ControlPlane, api, konnectivity, tunnel string) {
	t.Helper()
	endpoint := map[string]any{"apiServer": hostPortOf(t, api), "konnectivity": hostPortOf(t, konnectivity), "inCluster": hostPortOf(t, api)}
	if tunnel != "" {
		endpoint["tunnel"] = hostPortOf(t, tunnel)
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     map[string]any{"endpoint": endpoint},
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner("test/endpoint"), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func (f *gatewayFixture) apiAddress() string {
	return f.api.Listener.Addr().String()
}

func (f *gatewayFixture) publishWorking(t *testing.T) {
	t.Helper()
	publishGateway(t, f.cp, f.apiAddress(), f.konnectivity, f.tunnel.address)
}

func (f *gatewayFixture) allAnswered() string {
	return f.api.url + " answered 200 at the public endpoint and through the tunnel " + f.tunnel.address + ", and " + f.konnectivity + " completed a TLS handshake"
}

func (f *gatewayFixture) probe(timeout time.Duration) *health.Reconciler {
	return &health.Reconciler{Client: k8s, Reader: k8s, Period: time.Hour, Timeout: timeout, Clock: clock.RealClock{}}
}

func TestGatewayModeProbesTheThreePaths(t *testing.T) {
	f := newGateway(t)
	f.publishWorking(t)
	eventually(t, func() error {
		return expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered())
	})
}

func TestEachGatewayPathFailingAloneNamesItself(t *testing.T) {
	type broken struct {
		name    string
		apply   func(t *testing.T, f *gatewayFixture) (konnectivity, tunnel string)
		message func(f *gatewayFixture, konnectivity, tunnel string) string
	}
	unchanged := func(f *gatewayFixture) (string, string) { return f.konnectivity, f.tunnel.address }
	cases := []broken{
		{"the public endpoint answers 503", func(t *testing.T, f *gatewayFixture) (string, string) {
			f.api.code.Store(http.StatusServiceUnavailable)
			return unchanged(f)
		}, func(f *gatewayFixture, _, _ string) string { return "api: " + f.api.url + " answered 503" }},
		{"the public endpoint hangs", func(t *testing.T, f *gatewayFixture) (string, string) {
			f.api.hang.Store(true)
			return unchanged(f)
		}, func(f *gatewayFixture, _, _ string) string { return "api: " + f.api.url + " did not answer" }},
		{"the API server behind the tunnel answers 503", func(t *testing.T, f *gatewayFixture) (string, string) {
			f.tunnel.code.Store(http.StatusServiceUnavailable)
			return unchanged(f)
		}, func(f *gatewayFixture, _, tunnel string) string {
			return "tunnel: " + f.api.url + " through " + tunnel + " answered 503"
		}},
		{"nothing listens at the tunnel", func(t *testing.T, f *gatewayFixture) (string, string) {
			return f.konnectivity, deadAddress(t)
		}, func(f *gatewayFixture, _, tunnel string) string {
			return "tunnel: " + f.api.url + " through " + tunnel + " did not answer"
		}},
		{"the tunnel does not take the client certificate", func(t *testing.T, f *gatewayFixture) (string, string) {
			other, err := certs.NewCA("other-ca", time.Now(), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			pool := x509.NewCertPool()
			pool.AddCert(other.Cert)
			f.tunnel.clientCAs.Store(pool)
			return unchanged(f)
		}, func(f *gatewayFixture, _, tunnel string) string {
			return "tunnel: " + f.api.url + " through " + tunnel + " did not answer"
		}},
		{"the tunnel's serving certificate is not the bundle's", func(t *testing.T, f *gatewayFixture) (string, string) {
			other, err := certs.NewCA("other-ca", time.Now(), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			return f.konnectivity, serveTunnelWithInnerCA(t, other, f.clientCA, f.api.ca).address
		}, func(f *gatewayFixture, _, tunnel string) string {
			return "tunnel: " + f.api.url + " through " + tunnel + " did not answer"
		}},
		{"the API server behind the tunnel is not the bundle's", func(t *testing.T, f *gatewayFixture) (string, string) {
			other, err := certs.NewCA("other-ca", time.Now(), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			return f.konnectivity, serveTunnelWithInnerCA(t, f.api.ca, f.clientCA, other).address
		}, func(f *gatewayFixture, _, tunnel string) string {
			return "tunnel: " + f.api.url + " through " + tunnel + " did not answer"
		}},
		{"nothing listens at konnectivity", func(t *testing.T, f *gatewayFixture) (string, string) {
			return deadAddress(t), f.tunnel.address
		}, func(f *gatewayFixture, konnectivity, _ string) string {
			return "konnectivity: " + konnectivity + " did not complete a TLS handshake"
		}},
		{"konnectivity's serving certificate is not the bundle's", func(t *testing.T, f *gatewayFixture) (string, string) {
			other, err := certs.NewCA("other-ca", time.Now(), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			return serveTLS(t, other), f.tunnel.address
		}, func(f *gatewayFixture, konnectivity, _ string) string {
			return "konnectivity: " + konnectivity + " did not complete a TLS handshake"
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newGateway(t)
			f.publishWorking(t)
			eventually(t, func() error {
				return expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered())
			})
			konnectivity, tunnel := c.apply(t, f)
			if konnectivity != f.konnectivity || tunnel != f.tunnel.address {
				publishGateway(t, f.cp, f.apiAddress(), konnectivity, tunnel)
			}
			probe := f.probe(500 * time.Millisecond)
			for failed := 1; failed < health.FailuresBeforeFalse; failed++ {
				reconcile(t, probe, f.cp)
				if err := expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered()); err != nil {
					t.Fatalf("after %d failed probes: %v", failed, err)
				}
			}
			reconcile(t, probe, f.cp)
			if err := expect(t, f.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed, c.message(f, konnectivity, tunnel)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTheMessageNamesEveryFailedPath(t *testing.T) {
	f := newGateway(t)
	f.publishWorking(t)
	eventually(t, func() error {
		return expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered())
	})
	f.api.code.Store(http.StatusInternalServerError)
	f.tunnel.code.Store(http.StatusBadGateway)
	konnectivity := deadAddress(t)
	publishGateway(t, f.cp, f.apiAddress(), konnectivity, f.tunnel.address)
	probe := f.probe(500 * time.Millisecond)
	for range health.FailuresBeforeFalse {
		reconcile(t, probe, f.cp)
	}
	want := fmt.Sprintf("api: %s answered 500; tunnel: %s through %s answered 502; konnectivity: %s did not complete a TLS handshake", f.api.url, f.api.url, f.tunnel.address, konnectivity)
	if err := expect(t, f.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed, want); err != nil {
		t.Fatal(err)
	}
}

func TestAHungGatewayPathCostsOneTimeout(t *testing.T) {
	const timeout = time.Second
	type hang struct {
		name  string
		apply func(*gatewayFixture)
	}
	for _, c := range []hang{
		{"the public endpoint", func(f *gatewayFixture) { f.api.hang.Store(true) }},
		{"behind the tunnel", func(f *gatewayFixture) { f.tunnel.hang.Store(true) }},
		{"the three at once", func(f *gatewayFixture) {
			f.api.hang.Store(true)
			f.tunnel.hang.Store(true)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newGateway(t)
			f.publishWorking(t)
			eventually(t, func() error {
				return expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered())
			})
			c.apply(f)
			probe := f.probe(timeout)
			start := time.Now()
			reconcile(t, probe, f.cp)
			if took := time.Since(start); took < timeout || took >= 2*timeout {
				t.Errorf("a pass over a hung path took %s, want one Timeout of %s", took, timeout)
			}
		})
	}
	for _, c := range []struct {
		name  string
		apply func(f *gatewayFixture, stalled string) (string, string, string)
	}{
		{"a Gateway that accepts and never answers at the public endpoint", func(f *gatewayFixture, stalled string) (string, string, string) {
			return stalled, f.konnectivity, f.tunnel.address
		}},
		{"a Gateway that accepts and never answers at the tunnel", func(f *gatewayFixture, stalled string) (string, string, string) {
			return f.apiAddress(), f.konnectivity, stalled
		}},
		{"a Gateway that accepts and never answers at konnectivity", func(f *gatewayFixture, stalled string) (string, string, string) {
			return f.apiAddress(), stalled, f.tunnel.address
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newGateway(t)
			f.publishWorking(t)
			eventually(t, func() error {
				return expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered())
			})
			stalled, closed := stallListener(t)
			api, konnectivity, tunnel := c.apply(f, stalled)
			publishGateway(t, f.cp, api, konnectivity, tunnel)
			probe := f.probe(timeout)
			start := time.Now()
			reconcile(t, probe, f.cp)
			if took := time.Since(start); took < timeout || took >= 2*timeout {
				t.Errorf("a pass over a stalled path took %s, want one Timeout of %s", took, timeout)
			}
			select {
			case <-closed:
			case <-time.After(3 * timeout):
				t.Errorf("the probe left its stalled connection open longer than %s", 3*timeout)
			}
		})
	}
}

func TestATunnelClientThatIsMissingOrKeptElsewhereWaitsForCertificates(t *testing.T) {
	cp := gatewayControlPlane(t)
	api := serve(t)
	ca := api.ca
	tunnel := serveTunnel(t, ca, ca)
	konnectivity := serveTLS(t, ca)
	writeBundle(t, cp, api)
	publishGateway(t, cp, api.Listener.Addr().String(), konnectivity, tunnel.address)
	name := lodiav1alpha1.TunnelClientSecret.Name(cp.Name)
	eventually(t, func() error {
		return expect(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "waiting for PKI to write the tunnel's client certificate "+name)
	})

	foreign := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: cp.Namespace,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret", Name: "stranger", UID: "abc", Controller: new(true)}},
		},
		Data: map[string][]byte{corev1.TLSCertKey: []byte("x"), corev1.TLSPrivateKeyKey: []byte("y")},
	}
	if err := k8s.Create(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}
	probe := &health.Reconciler{Client: k8s, Reader: k8s, Period: time.Hour, Timeout: 300 * time.Millisecond}
	reconcile(t, probe, cp)
	if err := expect(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "Secret "+name+" is kept by ExternalSecret stranger with UID abc"); err != nil {
		t.Fatal(err)
	}

	foreign.OwnerReferences = nil
	if err := k8s.Update(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}
	reconcile(t, probe, cp)
	if err := expect(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "the tunnel's client certificate "+name+" holds no certificate and key"); err != nil {
		t.Fatal(err)
	}
}

func TestAGatewayEndpointWithoutTheTunnelWaitsForIt(t *testing.T) {
	f := newGateway(t)
	publishGateway(t, f.cp, f.apiAddress(), f.konnectivity, "")
	eventually(t, func() error {
		return expect(t, f.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint.tunnel")
	})
}

func TestARotatedTunnelClientReachesTheNextProbe(t *testing.T) {
	f := newGateway(t)
	f.publishWorking(t)
	setAccess(t, f.cp)
	writeKubeconfig(t, f.cp, tenantKubeconfig)
	probe := f.probe(2 * time.Second)
	reconcile(t, probe, f.cp)
	if err := expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered()); err != nil {
		t.Fatal(err)
	}

	rotated, err := certs.NewCA("tunnel-ca-2", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	_, certPEM, keyPEM := leafPair(t, rotated, x509.ExtKeyUsageClientAuth)
	pool := x509.NewCertPool()
	pool.AddCert(rotated.Cert)
	f.tunnel.clientCAs.Store(pool)
	secret := &corev1.Secret{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: f.cp.Namespace, Name: lodiav1alpha1.TunnelClientSecret.Name(f.cp.Name)}, secret); err != nil {
		t.Fatal(err)
	}
	secret.Data[corev1.TLSCertKey], secret.Data[corev1.TLSPrivateKeyKey] = certPEM, keyPEM
	if err := k8s.Update(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	for range health.FailuresBeforeFalse {
		reconcile(t, probe, f.cp)
	}
	if err := expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered()); err != nil {
		t.Errorf("after the client pair rotated: %v", err)
	}
}

func readsOf(t *testing.T) (client.Reader, *[]string) {
	t.Helper()
	reads := &[]string{}
	return interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Secret); ok {
				*reads = append(*reads, key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}), reads
}

func TestTheOtherModesAreProbedAsBefore(t *testing.T) {
	for _, endpoint := range []lodiav1alpha1.Endpoint{
		{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
		{NodePort: &lodiav1alpha1.NodePortEndpoint{}},
	} {
		cp := controlPlaneWith(t, endpoint)
		e := serve(t)
		writeBundle(t, cp, e)
		publish(t, cp, e)
		eventually(t, func() error {
			return expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200")
		})
		reader, reads := readsOf(t)
		probe := &health.Reconciler{Client: k8s, Reader: reader, Period: time.Hour, Timeout: time.Second, Clock: clock.RealClock{}}
		reconcile(t, probe, cp)
		if got := strings.Join(*reads, ","); got != lodiav1alpha1.CABundleSecret.Name(cp.Name) {
			t.Errorf("%+v: the pass read the Secrets [%s], want only the CA bundle", endpoint, got)
		}
		if err := expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200"); err != nil {
			t.Error(err)
		}
		e.code.Store(http.StatusServiceUnavailable)
		for range health.FailuresBeforeFalse {
			reconcile(t, probe, cp)
		}
		if err := expect(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed, e.url+" answered 503"); err != nil {
			t.Error(err)
		}
	}
}

func TestGatewayModeReadsTheTunnelClientOncePerPass(t *testing.T) {
	f := newGateway(t)
	f.publishWorking(t)
	eventually(t, func() error {
		return expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered())
	})
	reader, reads := readsOf(t)
	r := &health.Reconciler{Client: k8s, Reader: reader, Period: time.Hour, Timeout: time.Second, Clock: clock.RealClock{}}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.cp)}); err != nil {
		t.Fatal(err)
	}
	want := lodiav1alpha1.CABundleSecret.Name(f.cp.Name) + "," + lodiav1alpha1.TunnelClientSecret.Name(f.cp.Name)
	if got := strings.Join(*reads, ","); got != want {
		t.Errorf("the pass read the Secrets [%s], want [%s]", got, want)
	}
}

func TestAFailedReadOfTheTunnelClientLeavesTheStatus(t *testing.T) {
	f := newGateway(t)
	f.publishWorking(t)
	eventually(t, func() error {
		return expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered())
	})
	name := lodiav1alpha1.TunnelClientSecret.Name(f.cp.Name)
	failing := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == name {
				return apierrors.NewServiceUnavailable("the API server is restarting")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	before := get(t, f.cp).ResourceVersion
	probe := &health.Reconciler{Client: k8s, Reader: failing, Period: time.Hour, Timeout: time.Second, Clock: clock.RealClock{}}
	for range health.FailuresBeforeFalse {
		if _, err := probe.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.cp)}); err == nil {
			t.Fatalf("a pass whose read of %s failed returned no error", name)
		}
	}
	if after := get(t, f.cp).ResourceVersion; after != before {
		t.Errorf("a pass whose read failed wrote the status: resourceVersion %s, then %s", before, after)
	}
}
