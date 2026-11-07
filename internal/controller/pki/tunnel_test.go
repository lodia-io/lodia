// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
	"go.lodia.io/lodia/internal/controller/pki"
)

const (
	day              = 24 * time.Hour
	apiHost          = "api.dev.example.test"
	konnectivityHost = "konnectivity.dev.example.test"
	tunnelHost       = "tunnel.dev.example.test"
)

var tunnelSecrets = []string{"dev-cakey-tunnel", "dev-tunnel-ca", "dev-tunnel-serving", "dev-tunnel-client"}

func gatewayEndpoint() lodiav1alpha1.Endpoint {
	return lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
		ParentRef: lodiav1alpha1.ParentReference{Name: "edge", Namespace: "gateways", SectionName: "tls"},
		Hostnames: lodiav1alpha1.GatewayHostnames{APIServer: apiHost, Konnectivity: konnectivityHost, Tunnel: tunnelHost},
	}}
}

func hostPort(host string, port int64) map[string]any {
	return map[string]any{"host": host, "port": port}
}

func publishGatewayEndpoint(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	applyStatus(t, cp, map[string]any{"endpoint": map[string]any{
		"apiServer":    hostPort(apiHost, 443),
		"konnectivity": hostPort(konnectivityHost, 443),
		"inCluster":    hostPort(lodiav1alpha1.DefaultAdvertiseAddress, 6443),
		"tunnel":       hostPort(tunnelHost, 443),
	}})
}

func longLived() *pki.Reconciler {
	r := newReconciler(365 * day)
	r.CALifetime, r.TrustAnchorLifetime, r.CAExpiryNotice = 5*365*day, 10*365*day, 365*day
	return r
}

func hasIP(c *x509.Certificate, want string) bool {
	return slices.ContainsFunc(c.IPAddresses, func(ip net.IP) bool { return ip.String() == want })
}

func pool(t *testing.T, s *corev1.Secret) *x509.CertPool {
	t.Helper()
	bundle, err := certs.ParseCerts(s.Data[lodiav1alpha1.CACertKey])
	if err != nil {
		t.Fatalf("%s: %v", s.Name, err)
	}
	roots := x509.NewCertPool()
	for _, c := range bundle {
		roots.AddCert(c)
	}
	return roots
}

func TestAGatewayControlPlaneGetsTheTunnelsSecrets(t *testing.T) {
	now := clocktesting.NewFakePassiveClock(time.Now())
	r := longLived()
	r.Clock = now
	cp := controlPlaneAt(t, "pki-tunnel-secrets", gatewayEndpoint())
	publishGatewayEndpoint(t, cp)
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)

	secrets := secretsOf(t, cp)
	if want := len(secretTable(cp.Name)) + len(tunnelSecrets); len(secrets) != want {
		t.Errorf("%d Secrets, want %d: %v", len(secrets), want, slices.Sorted(maps.Keys(secrets)))
	}
	pair := []string{lodiav1alpha1.TLSCertKey, lodiav1alpha1.TLSPrivateKeyKey}
	for _, row := range []struct {
		name string
		typ  corev1.SecretType
		keys []string
	}{
		{"dev-cakey-tunnel", corev1.SecretTypeTLS, pair},
		{"dev-tunnel-ca", corev1.SecretTypeOpaque, []string{lodiav1alpha1.CACertKey}},
		{"dev-tunnel-serving", corev1.SecretTypeTLS, pair},
		{"dev-tunnel-client", corev1.SecretTypeTLS, pair},
	} {
		s := secrets[row.name]
		if s == nil {
			t.Fatalf("%s is missing", row.name)
		}
		if s.Type != row.typ {
			t.Errorf("%s has type %s, want %s", row.name, s.Type, row.typ)
		}
		if keys := slices.Sorted(maps.Keys(s.Data)); !slices.Equal(keys, row.keys) {
			t.Errorf("%s has keys %v, want %v", row.name, keys, row.keys)
		}
		if !metav1.IsControlledBy(s, cp) {
			t.Errorf("%s is not controlled by the ControlPlane: %+v", row.name, s.OwnerReferences)
		}
		if got, want := s.Annotations[lodiav1alpha1.DataHashAnnotation], lodiav1alpha1.DataHash(s.Data); got != want {
			t.Errorf("%s carries the hash %q, want %q", row.name, got, want)
		}
		if immutable(s) {
			t.Errorf("%s is immutable", row.name)
		}
	}

	ca := parseTLS(t, secrets["dev-cakey-tunnel"]).Cert
	if !ca.IsCA || ca.CheckSignatureFrom(ca) != nil || !strings.HasPrefix(ca.Subject.CommonName, "tunnel-ca@") || !stamped.MatchString(ca.Subject.CommonName) {
		t.Errorf("the tunnel CA is %q, CA:%t, want a self-signed root tunnel-ca@<unix nanoseconds>", ca.Subject.CommonName, ca.IsCA)
	}
	if want := now.Now().Add(r.CALifetime).Truncate(time.Second); !ca.NotAfter.Equal(want) {
		t.Errorf("the tunnel CA ends at %s, want %s", ca.NotAfter, want)
	}
	bundle, err := certs.Bundle(ca)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(secrets["dev-tunnel-ca"].Data[lodiav1alpha1.CACertKey], bundle) {
		t.Error("dev-tunnel-ca is not the tunnel CA alone")
	}

	cluster := parseTLS(t, secrets["dev-cakey-cluster"]).Cert
	for _, row := range []struct {
		name     string
		ca       *x509.Certificate
		cn       string
		dns      []string
		usages   []x509.ExtKeyUsage
		lifetime time.Duration
	}{
		{"dev-tunnel-serving", cluster, "lodia-tunnel", []string{tunnelHost}, serverAuth, r.LeafLifetime},
		{"dev-tunnel-client", ca, "lodia-tunnel-client", nil, clientAuth, 90 * day},
	} {
		c := parseTLS(t, secrets[row.name]).Cert
		if err := certs.Verify(c, row.ca, now.Now()); err != nil {
			t.Errorf("%s: %v", row.name, err)
		}
		if c.Subject.CommonName != row.cn || len(c.Subject.Organization) != 0 {
			t.Errorf("%s has CN %q and O %v, want CN %q and no O", row.name, c.Subject.CommonName, c.Subject.Organization, row.cn)
		}
		if !slices.Equal(c.DNSNames, row.dns) || len(c.IPAddresses) != 0 {
			t.Errorf("%s has the SANs %v %v, want %v", row.name, c.DNSNames, c.IPAddresses, row.dns)
		}
		if !slices.Equal(c.ExtKeyUsage, row.usages) || c.IsCA || c.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
			t.Errorf("%s has the usages %v, CA:%t", row.name, c.ExtKeyUsage, c.IsCA)
		}
		if want := now.Now().Add(row.lifetime).Truncate(time.Second); !c.NotAfter.Equal(want) {
			t.Errorf("%s ends at %s, want %s", row.name, c.NotAfter, want)
		}
	}

	serving := parseTLS(t, secrets["dev-apiserver-serving"]).Cert
	if !hasIP(serving, lodiav1alpha1.DefaultAdvertiseAddress) || !slices.Contains(serving.DNSNames, apiHost) {
		t.Errorf("the API server's serving certificate names %v %v, want %s and %s", serving.DNSNames, serving.IPAddresses, lodiav1alpha1.DefaultAdvertiseAddress, apiHost)
	}
	if slices.Contains(serving.DNSNames, tunnelHost) {
		t.Errorf("the API server's serving certificate names the tunnel's host: %v", serving.DNSNames)
	}
	konnectivity := parseTLS(t, secrets["dev-konnectivity-server"]).Cert
	if !slices.Equal(slices.Sorted(slices.Values(konnectivity.DNSNames)), []string{konnectivityHost, "localhost"}) || len(konnectivity.IPAddresses) != 1 || !hasIP(konnectivity, "127.0.0.1") {
		t.Errorf("the konnectivity server's certificate names %v %v", konnectivity.DNSNames, konnectivity.IPAddresses)
	}
}

func TestAMovedEndpointReissuesWhatNamesItInGatewayMode(t *testing.T) {
	r := longLived()
	cp := controlPlaneAt(t, "pki-tunnel-moved", gatewayEndpoint())
	publishGatewayEndpoint(t, cp)
	reconcile(t, r, cp)
	endpoint := map[string]any{
		"apiServer":    hostPort(apiHost, 443),
		"konnectivity": hostPort(konnectivityHost, 443),
		"inCluster":    hostPort(lodiav1alpha1.DefaultAdvertiseAddress, 6443),
		"tunnel":       hostPort(tunnelHost, 443),
	}
	for _, c := range []struct {
		name, moved string
		want        []string
	}{
		{"tunnel", "elsewhere.dev.example.test", []string{"dev-tunnel-serving"}},
		{"inCluster", "240.0.0.2", []string{"dev-apiserver-serving"}},
	} {
		before, _ := resourceVersions(t, cp)
		endpoint[c.name] = hostPort(c.moved, 443)
		applyStatus(t, cp, map[string]any{"endpoint": endpoint})
		reconcile(t, r, cp)
		after, _ := resourceVersions(t, cp)
		var changed []string
		for _, name := range slices.Sorted(maps.Keys(after)) {
			if before[name] != after[name] {
				changed = append(changed, name)
			}
		}
		if !slices.Equal(changed, c.want) {
			t.Errorf("moving %s changed %v, want %v", c.name, changed, c.want)
		}
		got := parseTLS(t, secretOf(t, cp, c.want[0])).Cert
		if !slices.Contains(got.DNSNames, c.moved) && !hasIP(got, c.moved) {
			t.Errorf("%s names %v %v, not %s", c.want[0], got.DNSNames, got.IPAddresses, c.moved)
		}
	}
}

func handshake(t *testing.T, serving, client *corev1.Secret, roots, clientCAs *x509.CertPool) error {
	t.Helper()
	pair := func(s *corev1.Secret) tls.Certificate {
		t.Helper()
		c, err := tls.X509KeyPair(s.Data[lodiav1alpha1.TLSCertKey], s.Data[lodiav1alpha1.TLSPrivateKeyKey])
		if err != nil {
			t.Fatalf("%s: %v", s.Name, err)
		}
		return c
	}
	near, far := net.Pipe()
	server := tls.Server(far, &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair(serving)}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs})
	dialer := tls.Client(near, &tls.Config{MinVersion: tls.VersionTLS13, ServerName: tunnelHost, RootCAs: roots, Certificates: []tls.Certificate{pair(client)}})
	served := make(chan error, 1)
	go func() {
		defer func() { _ = far.Close() }()
		served <- server.HandshakeContext(t.Context())
	}()
	err := dialer.HandshakeContext(t.Context())
	_ = near.Close()
	return errors.Join(err, <-served)
}

func TestTheTunnelsCertificatesVerifyAgainstTheirOwnCAsOnly(t *testing.T) {
	cp := controlPlaneAt(t, "pki-tunnel-verify", gatewayEndpoint())
	publishGatewayEndpoint(t, cp)
	reconcile(t, longLived(), cp)
	secrets := secretsOf(t, cp)
	tunnelCA, clusterCA, clientCA := pool(t, secrets["dev-tunnel-ca"]), pool(t, secrets["dev-ca-bundle"]), pool(t, secrets["dev-client-ca"])
	client, serving := parseTLS(t, secrets["dev-tunnel-client"]).Cert, parseTLS(t, secrets["dev-tunnel-serving"]).Cert

	asClient := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	if _, err := client.Verify(x509.VerifyOptions{Roots: tunnelCA, KeyUsages: asClient}); err != nil {
		t.Errorf("dev-tunnel-client does not verify against dev-tunnel-ca: %v", err)
	}
	for name, roots := range map[string]*x509.CertPool{"dev-ca-bundle": clusterCA, "dev-client-ca": clientCA} {
		if _, err := client.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: asClient}); err == nil {
			t.Errorf("dev-tunnel-client verifies against %s", name)
		}
	}
	asServer := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	if _, err := serving.Verify(x509.VerifyOptions{Roots: clusterCA, DNSName: tunnelHost, KeyUsages: asServer}); err != nil {
		t.Errorf("dev-tunnel-serving does not verify against dev-ca-bundle: %v", err)
	}
	if _, err := serving.Verify(x509.VerifyOptions{Roots: tunnelCA, DNSName: tunnelHost, KeyUsages: asServer}); err == nil {
		t.Error("dev-tunnel-serving verifies against dev-tunnel-ca")
	}
	if _, err := client.Verify(x509.VerifyOptions{Roots: tunnelCA, KeyUsages: asServer}); err == nil {
		t.Error("dev-tunnel-client serves")
	}
	if _, err := serving.Verify(x509.VerifyOptions{Roots: clusterCA, KeyUsages: asClient}); err == nil {
		t.Error("dev-tunnel-serving authenticates a client")
	}

	if err := handshake(t, secrets["dev-tunnel-serving"], secrets["dev-tunnel-client"], clusterCA, tunnelCA); err != nil {
		t.Errorf("the handshake with the sidecar's settings failed: %v", err)
	}
	if err := handshake(t, secrets["dev-tunnel-serving"], secrets["dev-apiserver-kubelet-client"], clusterCA, tunnelCA); err == nil {
		t.Error("the sidecar's settings admit a client of the cluster CA")
	}
	if err := handshake(t, secrets["dev-tunnel-serving"], secrets["dev-tunnel-client"], tunnelCA, tunnelCA); err == nil {
		t.Error("a client that trusts the tunnel CA's bundle accepted the serving certificate")
	}
}

func TestAGatewayEndpointWithoutItsTunnelStillWaits(t *testing.T) {
	r := longLived()
	cp := controlPlaneAt(t, "pki-tunnel-partial", gatewayEndpoint())
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)
	ca := parseTLS(t, secrets["dev-cakey-tunnel"]).Cert
	if err := certs.Verify(parseTLS(t, secrets["dev-tunnel-client"]).Cert, ca, time.Now()); err != nil {
		t.Errorf("dev-tunnel-client before the endpoint: %v", err)
	}
	if bundle, err := certs.Bundle(ca); err != nil || !bytes.Equal(secrets["dev-tunnel-ca"].Data[lodiav1alpha1.CACertKey], bundle) {
		t.Errorf("dev-tunnel-ca before the endpoint is not the tunnel CA: %v", err)
	}
	client := secrets["dev-tunnel-client"].ResourceVersion

	applyStatus(t, cp, map[string]any{"endpoint": map[string]any{
		"apiServer":    hostPort(apiHost, 443),
		"konnectivity": hostPort(konnectivityHost, 443),
		"inCluster":    hostPort(lodiav1alpha1.DefaultAdvertiseAddress, 6443),
	}})
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint)
	if secretsOf(t, cp)["dev-tunnel-serving"] != nil {
		t.Error("dev-tunnel-serving issued without status.endpoint.tunnel")
	}

	publishGatewayEndpoint(t, cp)
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	secrets = secretsOf(t, cp)
	if secrets["dev-tunnel-serving"] == nil {
		t.Error("dev-tunnel-serving not issued once status.endpoint.tunnel exists")
	}
	if secrets["dev-tunnel-client"].ResourceVersion != client {
		t.Error("the endpoint's publication wrote dev-tunnel-client, which needs no endpoint")
	}
}

type unreadSecrets struct {
	client.Reader
	t *testing.T
}

func (r unreadSecrets) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if slices.Contains(tunnelSecrets, key.Name) {
		r.t.Errorf("read %s", key.Name)
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func otherModes() map[string]lodiav1alpha1.Endpoint {
	return map[string]lodiav1alpha1.Endpoint{
		"nodeport":     {NodePort: &lodiav1alpha1.NodePortEndpoint{}},
		"loadbalancer": {LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}, CertSANs: []lodiav1alpha1.Hostname{certSAN}},
	}
}

func TestAnotherModeLeavesTheTunnelsNamesAlone(t *testing.T) {
	for mode, endpoint := range otherModes() {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			r := newReconciler(time.Hour)
			cp := controlPlaneAt(t, "pki-other-mode-"+mode, endpoint)
			publishEndpoint(t, cp)
			reconcile(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
			expectCondition(t, cp, lodiav1alpha1.ConditionCAValidityAcceptable, metav1.ConditionTrue, lodiav1alpha1.ReasonValid)
			before := namespaceSecrets(t, cp.Namespace)
			if len(before) != len(secretTable(cp.Name)) {
				t.Errorf("%d Secrets, want %d: %v", len(before), len(secretTable(cp.Name)), slices.Sorted(maps.Keys(before)))
			}
			for _, name := range tunnelSecrets {
				if _, written := before[name]; written {
					t.Errorf("%s written", name)
				}
			}

			owner := metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "someone", UID: "8f0c6f2e-0d6e-4f4e-9e44-0d6a7c1b5a21", Controller: new(true)}
			for i, name := range tunnelSecrets {
				foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace}, Data: map[string][]byte{"theirs": []byte(name)}}
				if i%2 == 0 {
					foreign.OwnerReferences = []metav1.OwnerReference{owner}
				} else {
					foreign.Labels = map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name}
				}
				if err := k8s.Create(ctx, foreign); err != nil {
					t.Fatal(err)
				}
			}
			before = namespaceSecrets(t, cp.Namespace)
			r.Reader = unreadSecrets{Reader: apiReader, t: t}
			reconcile(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
			c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
			for _, name := range tunnelSecrets {
				if strings.Contains(c.Message, name) {
					t.Errorf("CertificatesReady names %s: %s", name, c.Message)
				}
			}
			if after := namespaceSecrets(t, cp.Namespace); !maps.Equal(before, after) {
				t.Errorf("a foreign Secret under a tunnel's name changed: %v, then %v", before, after)
			}

			applyStatus(t, cp, map[string]any{"endpoint": map[string]any{
				"apiServer":    hostPort(endpointHost, endpointPort),
				"konnectivity": hostPort(endpointHost, 8132),
				"inCluster":    hostPort("203.0.113.99", 6443),
				"tunnel":       hostPort(tunnelHost, 443),
			}})
			reconcile(t, r, cp)
			if after := namespaceSecrets(t, cp.Namespace); !maps.Equal(before, after) {
				t.Errorf("an endpoint naming a tunnel and another in-cluster host changed the Secrets: %v, then %v", before, after)
			}
		})
	}
}

func TestARestoreInAnotherModeIgnoresTheTunnelsNames(t *testing.T) {
	ctx := t.Context()
	cp := controlPlaneAt(t, "pki-other-mode-restore", lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}, restoring)
	publishEndpoint(t, cp)
	for _, name := range tunnelSecrets {
		if err := k8s.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace}, Data: map[string][]byte{"theirs": []byte(name)}}); err != nil {
			t.Fatal(err)
		}
	}
	reconcile(t, newReconciler(time.Hour), cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForBackup)
}

func TestAnotherModesSecretsKeepTheirBytes(t *testing.T) {
	for mode, endpoint := range otherModes() {
		t.Run(mode, func(t *testing.T) {
			r := longLived()
			cp := controlPlaneAt(t, "pki-other-mode-bytes-"+mode, endpoint)
			publishEndpoint(t, cp)
			reconcile(t, r, cp)
			secrets := secretsOf(t, cp)
			cluster := parseTLS(t, secrets["dev-cakey-cluster"])
			api := []string{"kubernetes", "kubernetes.default", "kubernetes.default.svc", "kubernetes.default.svc.cluster.local",
				"dev-kube-apiserver", "dev-kube-apiserver." + cp.Namespace, "dev-kube-apiserver." + cp.Namespace + ".svc", "localhost"}
			if endpoint.LoadBalancer != nil {
				api = append(api, certSAN)
			}
			for name, p := range map[string]certs.Profile{
				"dev-apiserver-serving": {CommonName: "kube-apiserver", DNSNames: api,
					IPAddresses: []net.IP{net.ParseIP("10.96.0.1"), net.ParseIP("127.0.0.1"), net.ParseIP(endpointHost)}, Usages: serverAuth},
				"dev-konnectivity-server": {CommonName: "konnectivity-server", DNSNames: []string{"localhost"},
					IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP(endpointHost)}, Usages: serverAuth},
			} {
				issued, err := cluster.Issue(p, time.Now(), r.LeafLifetime)
				if err != nil {
					t.Fatal(err)
				}
				s := secrets[name]
				if s.Data[lodiav1alpha1.TLSCertKey], err = certs.CertPEM(issued.Cert); err != nil {
					t.Fatal(err)
				}
				if s.Data[lodiav1alpha1.TLSPrivateKeyKey], err = certs.KeyPEM(issued.Key); err != nil {
					t.Fatal(err)
				}
				s.Annotations[lodiav1alpha1.DataHashAnnotation] = lodiav1alpha1.DataHash(s.Data)
				if err := k8s.Update(t.Context(), s); err != nil {
					t.Fatal(err)
				}
			}

			before := secretsOf(t, cp)
			reconcile(t, r, cp)
			after := secretsOf(t, cp)
			if len(after) != len(before) {
				t.Errorf("%d Secrets, then %d", len(before), len(after))
			}
			for name, s := range before {
				if after[name] == nil || after[name].ResourceVersion != s.ResourceVersion || !maps.EqualFunc(after[name].Data, s.Data, bytes.Equal) {
					t.Errorf("%s changed in a pass", name)
				}
			}
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
		})
	}
}

func TestAForeignSecretUnderATunnelNameStopsAGatewayControlPlane(t *testing.T) {
	owner := metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "someone", UID: "8f0c6f2e-0d6e-4f4e-9e44-0d6a7c1b5a21", Controller: new(true)}
	for i, name := range tunnelSecrets {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			r := newReconciler(time.Hour)
			cp := controlPlaneAt(t, fmt.Sprintf("pki-tunnel-foreign-%d", i), gatewayEndpoint())
			publishGatewayEndpoint(t, cp)
			foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace, OwnerReferences: []metav1.OwnerReference{owner}}, Data: map[string][]byte{"theirs": []byte(name)}}
			if err := k8s.Create(ctx, foreign); err != nil {
				t.Fatal(err)
			}
			before := namespaceSecrets(t, cp.Namespace)
			result := reconcile(t, r, cp)
			c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
			if c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonSecretsControlledElsewhere || !strings.Contains(c.Message, name+" by ConfigMap someone") {
				t.Fatalf("CertificatesReady is %s %s: %s", c.Status, c.Reason, c.Message)
			}
			if after := namespaceSecrets(t, cp.Namespace); !maps.Equal(before, after) {
				t.Errorf("a Secret was written beside another owner's: %v, then %v", before, after)
			}
			if result.RequeueAfter != time.Minute {
				t.Errorf("requeue after %s, want a minute", result.RequeueAfter)
			}
			if err := k8s.Delete(ctx, foreign); err != nil {
				t.Fatal(err)
			}
			reconcile(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
			if s := secretsOf(t, cp)[name]; s == nil || !metav1.IsControlledBy(s, cp) {
				t.Errorf("%s was not written once the other owner's was gone", name)
			}
		})
	}
}

func TestAPredecessorsTunnelCAIsNeitherReadNorWritten(t *testing.T) {
	ctx := t.Context()
	r := longLived()
	cp := controlPlaneAt(t, "pki-tunnel-predecessor", gatewayEndpoint())
	publishGatewayEndpoint(t, cp)
	reconcile(t, r, cp)
	predecessor := secretOf(t, cp, "dev-cakey-tunnel")
	for name := range secretsOf(t, cp) {
		if name != predecessor.Name {
			if err := k8s.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := k8s.Delete(ctx, cp); err != nil {
		t.Fatal(err)
	}
	successor := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: cp.Name, Namespace: cp.Namespace}, Spec: cp.Spec}
	if err := k8s.Create(ctx, successor); err != nil {
		t.Fatal(err)
	}
	publishGatewayEndpoint(t, successor)
	before := namespaceSecrets(t, cp.Namespace)
	result := reconcile(t, r, successor)
	expectCondition(t, successor, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonSecretsControlledElsewhere)
	if result.RequeueAfter != 10*time.Second {
		t.Errorf("requeue after %s, want 10s while the garbage collector deletes a predecessor's Secret", result.RequeueAfter)
	}
	if after := namespaceSecrets(t, cp.Namespace); !maps.Equal(before, after) {
		t.Errorf("a Secret was written beside the predecessor's: %v, then %v", before, after)
	}
	if err := k8s.Delete(ctx, predecessor); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, successor)
	expectCondition(t, successor, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	if parseTLS(t, secretOf(t, successor, "dev-cakey-tunnel")).Cert.Equal(parseTLS(t, predecessor).Cert) {
		t.Error("the successor keeps the predecessor's tunnel CA")
	}
}

func TestAnUnlabelledTunnelCAIsKeptNotReplaced(t *testing.T) {
	r := longLived()
	cp := controlPlaneAt(t, "pki-tunnel-unlabelled", gatewayEndpoint())
	publishGatewayEndpoint(t, cp)
	reconcile(t, r, cp)
	key := secretOf(t, cp, "dev-cakey-tunnel")
	ca := parseTLS(t, key).Cert
	key.Labels, key.OwnerReferences = nil, nil
	if err := k8s.Update(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	before := namespaceSecrets(t, cp.Namespace)
	down := longLived()
	down.Reader = failingRead{Reader: apiReader, read: "a Secret by name"}
	if _, err := down.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Error("a pass that cannot read the unlabelled tunnel CA succeeded")
	}
	if after := namespaceSecrets(t, cp.Namespace); !maps.Equal(before, after) {
		t.Errorf("a pass that cannot read the unlabelled tunnel CA wrote a Secret: %v, then %v", before, after)
	}
	reconcile(t, r, cp)
	kept := secretOf(t, cp, "dev-cakey-tunnel")
	if !parseTLS(t, kept).Cert.Equal(ca) || !metav1.IsControlledBy(kept, cp) {
		t.Error("an unlabelled tunnel CA was replaced, or not adopted")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func TestARestoreAdoptsNoTunnelSecret(t *testing.T) {
	for i, name := range tunnelSecrets {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			r := newReconciler(time.Hour)
			cp := controlPlaneAt(t, fmt.Sprintf("pki-tunnel-restore-%d", i), gatewayEndpoint(), restoring)
			publishGatewayEndpoint(t, cp)
			found := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace}, Data: map[string][]byte{"restored": []byte(name)}}
			if err := k8s.Create(ctx, found); err != nil {
				t.Fatal(err)
			}
			before := namespaceSecrets(t, cp.Namespace)
			reconcile(t, r, cp)
			c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
			if c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonSecretsExist || !strings.Contains(c.Message, name) {
				t.Fatalf("CertificatesReady is %s %s: %s", c.Status, c.Reason, c.Message)
			}
			if after := namespaceSecrets(t, cp.Namespace); !maps.Equal(before, after) {
				t.Errorf("a restore wrote Secrets beside the one it found: %v, then %v", before, after)
			}
			kept := &corev1.Secret{}
			if err := k8s.Get(ctx, client.ObjectKeyFromObject(found), kept); err != nil {
				t.Fatal(err)
			}
			if len(kept.OwnerReferences) != 0 {
				t.Errorf("%s was adopted: %+v", name, kept.OwnerReferences)
			}
			if err := k8s.Delete(ctx, found); err != nil {
				t.Fatal(err)
			}
			reconcile(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForBackup)
			if s := secretsOf(t, cp)[name]; s == nil || !metav1.IsControlledBy(s, cp) {
				t.Errorf("%s was not written as its own once the found one was gone", name)
			}
		})
	}
}

func TestTheTunnelsClientCertificateRenewsAtDaySixty(t *testing.T) {
	start := time.Now()
	now := clocktesting.NewFakePassiveClock(start)
	r := longLived()
	r.Clock = now
	cp := controlPlaneAt(t, "pki-tunnel-renewal", gatewayEndpoint())
	publishGatewayEndpoint(t, cp)
	reconcile(t, r, cp)
	first := secretsOf(t, cp)
	old := parseTLS(t, first["dev-tunnel-client"])
	ca := parseTLS(t, first["dev-cakey-tunnel"]).Cert
	if got, want := old.Cert.NotAfter, start.Add(90*day).Truncate(time.Second); !got.Equal(want) {
		t.Errorf("it ends at %s, want 90 days after its issuance, %s", got, want)
	}
	due := certs.RenewalTime(old.Cert)
	if got, want := due, start.Add(60*day).Truncate(time.Second); !got.Equal(want) {
		t.Errorf("it renews at %s, want day 60, %s", got, want)
	}

	now.SetTime(due.Add(-time.Minute))
	versions, _ := resourceVersions(t, cp)
	result := reconcile(t, r, cp)
	if result.RequeueAfter != time.Minute {
		t.Errorf("a minute before its renewal the pass requeues after %s", result.RequeueAfter)
	}
	if after, _ := resourceVersions(t, cp); !maps.Equal(versions, after) {
		t.Errorf("a pass before day 60 wrote a Secret: %v, then %v", versions, after)
	}

	now.SetTime(due)
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	after, _ := resourceVersions(t, cp)
	var changed []string
	for _, name := range slices.Sorted(maps.Keys(after)) {
		if versions[name] != after[name] {
			changed = append(changed, name)
		}
	}
	if want := []string{"dev-tunnel-client"}; !slices.Equal(changed, want) {
		t.Errorf("day 60 changed %v, want %v", changed, want)
	}
	renewed := parseTLS(t, secretOf(t, cp, "dev-tunnel-client"))
	if renewed.Cert.SerialNumber.Cmp(old.Cert.SerialNumber) == 0 || renewed.Key.Equal(old.Key) {
		t.Error("the renewal kept the serial or the key")
	}
	if got, want := renewed.Cert.NotAfter, due.Add(90*day); !got.Equal(want) {
		t.Errorf("the renewed certificate ends at %s, want %s", got, want)
	}
	if err := certs.Verify(renewed.Cert, ca, due); err != nil {
		t.Errorf("the renewed certificate does not chain to the same tunnel CA: %v", err)
	}
	if !parseTLS(t, secretOf(t, cp, "dev-cakey-tunnel")).Cert.Equal(ca) || !bytes.Equal(secretOf(t, cp, "dev-tunnel-ca").Data[lodiav1alpha1.CACertKey], first["dev-tunnel-ca"].Data[lodiav1alpha1.CACertKey]) {
		t.Error("the renewal changed the tunnel CA or its bundle")
	}
}

func TestATunnelCertificateOfAnotherProfileIsIssuedAgain(t *testing.T) {
	clientAuth := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	serverAuth := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	both := []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	client := certs.Profile{CommonName: "lodia-tunnel-client", Usages: clientAuth}
	serving := certs.Profile{CommonName: "lodia-tunnel", DNSNames: []string{tunnelHost}, Usages: serverAuth}
	for name, c := range map[string]struct {
		secret, signer, ca string
		want, stored       certs.Profile
	}{
		"a client for servers too": {"dev-tunnel-client", "dev-cakey-tunnel", "dev-cakey-tunnel", client, certs.Profile{CommonName: client.CommonName, Usages: both}},
		"a client in a group":      {"dev-tunnel-client", "dev-cakey-tunnel", "dev-cakey-tunnel", client, certs.Profile{CommonName: client.CommonName, Organizations: []string{"lodia:cluster-admins"}, Usages: clientAuth}},
		"a client of another name": {"dev-tunnel-client", "dev-cakey-tunnel", "dev-cakey-tunnel", client, certs.Profile{CommonName: "someone", Usages: clientAuth}},
		"a client that is a CA":    {"dev-tunnel-client", "dev-cakey-tunnel", "dev-cakey-tunnel", client, certs.Profile{CommonName: client.CommonName, Usages: clientAuth, CA: true}},
		"a client of another CA":   {"dev-tunnel-client", "dev-cakey-cluster", "dev-cakey-tunnel", client, client},
		"a server of another SAN":  {"dev-tunnel-serving", "dev-cakey-cluster", "dev-cakey-cluster", serving, certs.Profile{CommonName: serving.CommonName, DNSNames: []string{tunnelHost, apiHost}, Usages: serverAuth}},
		"a server for clients too": {"dev-tunnel-serving", "dev-cakey-cluster", "dev-cakey-cluster", serving, certs.Profile{CommonName: serving.CommonName, DNSNames: serving.DNSNames, Usages: both}},
		"a server of another CA":   {"dev-tunnel-serving", "dev-cakey-tunnel", "dev-cakey-cluster", serving, serving},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			r := longLived()
			cp := controlPlaneAt(t, "pki-tunnel-profile-"+strings.ToLower(strings.ReplaceAll(name, " ", "-")), gatewayEndpoint())
			publishGatewayEndpoint(t, cp)
			reconcile(t, r, cp)
			first := secretsOf(t, cp)
			stored, err := parseTLS(t, first[c.signer]).Issue(c.stored, time.Now(), 30*day)
			if err != nil {
				t.Fatal(err)
			}
			secret := first[c.secret]
			if secret.Data[lodiav1alpha1.TLSCertKey], err = certs.CertPEM(stored.Cert); err != nil {
				t.Fatal(err)
			}
			if secret.Data[lodiav1alpha1.TLSPrivateKeyKey], err = certs.KeyPEM(stored.Key); err != nil {
				t.Fatal(err)
			}
			if err := k8s.Update(ctx, secret); err != nil {
				t.Fatal(err)
			}

			reconcile(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
			after := parseTLS(t, secretOf(t, cp, c.secret)).Cert
			if after.SerialNumber.Cmp(stored.Cert.SerialNumber) == 0 {
				t.Fatal("the certificate of another profile stayed")
			}
			if !certs.Matches(after, c.want) {
				t.Errorf("issued again as %s %v %v %v", after.Subject, after.DNSNames, after.ExtKeyUsage, after.IsCA)
			}
			if err := after.CheckSignatureFrom(parseTLS(t, secretOf(t, cp, c.ca)).Cert); err != nil {
				t.Errorf("not signed by %s: %v", c.ca, err)
			}
		})
	}
}

func TestATunnelClientPastItsRenewalIsRenewedOnceAnOutageEnds(t *testing.T) {
	start := time.Now()
	now := clocktesting.NewFakePassiveClock(start)
	r := longLived()
	r.Clock = now
	cp := controlPlaneAt(t, "pki-tunnel-outage", gatewayEndpoint())
	publishGatewayEndpoint(t, cp)
	reconcile(t, r, cp)
	old := parseTLS(t, secretOf(t, cp, "dev-tunnel-client"))

	now.SetTime(old.Cert.NotAfter.Add(day))
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	renewed := parseTLS(t, secretOf(t, cp, "dev-tunnel-client"))
	if renewed.Cert.Equal(old.Cert) {
		t.Fatal("an expired client certificate was kept")
	}
	if err := certs.Verify(renewed.Cert, parseTLS(t, secretOf(t, cp, "dev-cakey-tunnel")).Cert, now.Now()); err != nil {
		t.Error(err)
	}
}

func TestALostTunnelCAIsNeverReplaced(t *testing.T) {
	ctx := t.Context()
	r := longLived()
	cp := controlPlaneAt(t, "pki-tunnel-lost", gatewayEndpoint())
	publishGatewayEndpoint(t, cp)
	reconcile(t, r, cp)
	first := secretsOf(t, cp)

	for _, name := range []string{"dev-cakey-tunnel", "dev-front-proxy-client", "dev-tunnel-serving"} {
		if err := k8s.Delete(ctx, first[name]); err != nil {
			t.Fatal(err)
		}
	}
	reconcile(t, r, cp)
	c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
	if c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonCAMissing || !strings.Contains(c.Message, "tunnel") {
		t.Fatalf("CertificatesReady is %s %s: %s", c.Status, c.Reason, c.Message)
	}
	validity := condition(t, cp, lodiav1alpha1.ConditionCAValidityAcceptable)
	if validity.Status != metav1.ConditionUnknown || validity.Reason != lodiav1alpha1.ReasonCAMissing || !strings.Contains(validity.Message, "tunnel") {
		t.Errorf("CAValidityAcceptable is %s %s: %s", validity.Status, validity.Reason, validity.Message)
	}
	after := secretsOf(t, cp)
	if after["dev-cakey-tunnel"] != nil {
		t.Error("the lost tunnel CA was replaced")
	}
	for _, name := range []string{"dev-tunnel-ca", "dev-tunnel-client"} {
		if after[name].ResourceVersion != first[name].ResourceVersion {
			t.Errorf("%s was written while the tunnel CA is lost", name)
		}
	}
	for _, name := range []string{"dev-front-proxy-client", "dev-tunnel-serving"} {
		if after[name] == nil {
			t.Errorf("%s, which another CA signs, was not written while the tunnel CA is lost", name)
		}
	}

	corrupt := first["dev-cakey-tunnel"].DeepCopy()
	corrupt.ResourceVersion, corrupt.UID = "", ""
	corrupt.Data[lodiav1alpha1.TLSPrivateKeyKey] = []byte("not a key")
	if err := k8s.Create(ctx, corrupt); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonCAMissing)
	if !bytes.Equal(secretOf(t, cp, "dev-cakey-tunnel").Data[lodiav1alpha1.TLSPrivateKeyKey], []byte("not a key")) {
		t.Error("the unreadable tunnel CA key was overwritten")
	}

	other := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: cp.Namespace}, Spec: cp.Spec}
	other.Spec.Endpoint = lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}
	if err := k8s.Create(ctx, other); err != nil {
		t.Fatal(err)
	}
	publishEndpoint(t, other)
	reconcile(t, r, other)
	expectCondition(t, other, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func TestALostClusterCALeavesTheTunnelsClientToItsOwnCA(t *testing.T) {
	ctx := t.Context()
	r := longLived()
	cp := controlPlaneAt(t, "pki-tunnel-lost-cluster", gatewayEndpoint())
	publishGatewayEndpoint(t, cp)
	reconcile(t, r, cp)
	for _, name := range []string{"dev-cakey-cluster", "dev-tunnel-client", "dev-tunnel-serving"} {
		if err := k8s.Delete(ctx, secretOf(t, cp, name)); err != nil {
			t.Fatal(err)
		}
	}
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonCAMissing)
	secrets := secretsOf(t, cp)
	if secrets["dev-tunnel-client"] == nil {
		t.Error("dev-tunnel-client, which the tunnel CA signs, was not written while the cluster CA is lost")
	}
	if secrets["dev-tunnel-serving"] != nil {
		t.Error("dev-tunnel-serving, which the cluster CA signs, was written while that CA is lost")
	}
}

func TestAnExpiredTunnelCAIsNeverReplaced(t *testing.T) {
	ctx := t.Context()
	r := longLived()
	cp := controlPlaneAt(t, "pki-tunnel-expired", gatewayEndpoint())
	publishGatewayEndpoint(t, cp)
	reconcile(t, r, cp)

	expired, err := certs.NewCA("tunnel-ca@1", time.Now().Add(-2*time.Hour), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	key := secretOf(t, cp, "dev-cakey-tunnel")
	if key.Data[lodiav1alpha1.TLSCertKey], err = certs.CertPEM(expired.Cert); err != nil {
		t.Fatal(err)
	}
	if key.Data[lodiav1alpha1.TLSPrivateKeyKey], err = certs.KeyPEM(expired.Key); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Update(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(ctx, secretOf(t, cp, "dev-front-proxy-client")); err != nil {
		t.Fatal(err)
	}
	before := secretsOf(t, cp)

	reconcile(t, r, cp)
	c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
	if c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonCAExpired || !strings.Contains(c.Message, "tunnel") {
		t.Fatalf("CertificatesReady is %s %s: %s", c.Status, c.Reason, c.Message)
	}
	validity := condition(t, cp, lodiav1alpha1.ConditionCAValidityAcceptable)
	if want := "tunnel CA at " + expired.Cert.NotAfter.UTC().Format(time.RFC3339); validity.Status != metav1.ConditionFalse || validity.Reason != lodiav1alpha1.ReasonCAExpiring || !strings.Contains(validity.Message, want) {
		t.Errorf("CAValidityAcceptable is %s %s: %s, want it to name %s", validity.Status, validity.Reason, validity.Message, want)
	}
	after := secretsOf(t, cp)
	for _, name := range []string{"dev-cakey-tunnel", "dev-tunnel-ca", "dev-tunnel-client"} {
		if after[name].ResourceVersion != before[name].ResourceVersion {
			t.Errorf("%s was written under an expired tunnel CA", name)
		}
	}
	if after["dev-front-proxy-client"] == nil {
		t.Error("dev-front-proxy-client, which another CA signs, was not written under an expired tunnel CA")
	}
}

func TestCAValidityCountsTheCAsOfTheMode(t *testing.T) {
	for mode, c := range map[string]struct {
		endpoint lodiav1alpha1.Endpoint
		named    []string
		unnamed  []string
	}{
		"gateway":  {gatewayEndpoint(), []string{"client", "front-proxy", "etcd", "tunnel"}, []string{"cluster"}},
		"nodeport": {lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}, []string{"client", "front-proxy", "etcd"}, []string{"cluster", "tunnel"}},
	} {
		t.Run(mode, func(t *testing.T) {
			r := longLived()
			r.CAExpiryNotice = r.CALifetime + day
			cp := controlPlaneAt(t, "pki-ca-validity-"+mode, c.endpoint)
			reconcile(t, r, cp)
			validity := condition(t, cp, lodiav1alpha1.ConditionCAValidityAcceptable)
			if validity.Status != metav1.ConditionFalse || validity.Reason != lodiav1alpha1.ReasonCAExpiring {
				t.Fatalf("CAValidityAcceptable is %s %s: %s", validity.Status, validity.Reason, validity.Message)
			}
			secrets := secretsOf(t, cp)
			for _, name := range c.named {
				at := parseTLS(t, secrets["dev-cakey-"+name]).Cert.NotAfter.UTC().Format(time.RFC3339)
				if !strings.Contains(validity.Message, name+" CA at "+at) {
					t.Errorf("the message %q does not name the %s CA at %s", validity.Message, name, at)
				}
			}
			for _, name := range c.unnamed {
				if strings.Contains(validity.Message, name+" CA") {
					t.Errorf("the message %q names the %s CA", validity.Message, name)
				}
			}
		})
	}

	r := longLived()
	r.Client = failingApply{Client: controller, secret: "dev-cakey-tunnel"}
	cp := controlPlaneAt(t, "pki-ca-validity-unwritten", gatewayEndpoint())
	reconcileFails(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCAValidityAcceptable, metav1.ConditionUnknown, lodiav1alpha1.ReasonReconciling)
}

func TestARefusedTunnelSecretIsIssuingUntilTheNextPass(t *testing.T) {
	for i, name := range tunnelSecrets {
		t.Run("first pass "+name, func(t *testing.T) {
			r := longLived()
			r.Client = failingApply{Client: controller, secret: name}
			cp := controlPlaneAt(t, fmt.Sprintf("pki-tunnel-refused-%d", i), gatewayEndpoint())
			publishGatewayEndpoint(t, cp)
			reconcileFails(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonIssuing)
			written := secretsOf(t, cp)
			if written[name] != nil {
				t.Errorf("%s exists although its apply was refused", name)
			}
			if written["dev-tunnel-ca"] != nil && written["dev-cakey-tunnel"] == nil {
				t.Error("the tunnel CA's bundle was written before its key")
			}
			r.Client = controller
			reconcile(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
			secrets := secretsOf(t, cp)
			ca := parseTLS(t, secrets["dev-cakey-tunnel"]).Cert
			if err := certs.Verify(parseTLS(t, secrets["dev-tunnel-client"]).Cert, ca, time.Now()); err != nil {
				t.Errorf("dev-tunnel-client after the next pass: %v", err)
			}
			if bundle, err := certs.ParseCerts(secrets["dev-tunnel-ca"].Data[lodiav1alpha1.CACertKey]); err != nil || len(bundle) != 1 || !bundle[0].Equal(ca) {
				t.Errorf("dev-tunnel-ca after the next pass holds %v (%v), want the stored CA", subjects(bundle), err)
			}
		})
		t.Run("deleted "+name, func(t *testing.T) {
			ctx := t.Context()
			r := longLived()
			cp := controlPlaneAt(t, fmt.Sprintf("pki-tunnel-deleted-%d", i), gatewayEndpoint())
			publishGatewayEndpoint(t, cp)
			reconcile(t, r, cp)
			if name == "dev-cakey-tunnel" {
				if err := k8s.Delete(ctx, secretOf(t, cp, "dev-tunnel-ca")); err != nil {
					t.Fatal(err)
				}
			}
			if err := k8s.Delete(ctx, secretOf(t, cp, name)); err != nil {
				t.Fatal(err)
			}
			r.Client = failingApply{Client: controller, secret: name}
			reconcileFails(t, r, cp)
			c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
			if c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonIssuing || !strings.Contains(c.Message, name) {
				t.Fatalf("CertificatesReady is %s %s: %s", c.Status, c.Reason, c.Message)
			}
			r.Client = controller
			reconcile(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
		})
	}

	t.Run("a renewal", func(t *testing.T) {
		now := clocktesting.NewFakePassiveClock(time.Now())
		r := longLived()
		r.Clock = now
		cp := controlPlaneAt(t, "pki-tunnel-refused-renewal", gatewayEndpoint())
		publishGatewayEndpoint(t, cp)
		reconcile(t, r, cp)
		old := parseTLS(t, secretOf(t, cp, "dev-tunnel-client")).Cert
		now.SetTime(certs.RenewalTime(old))
		r.Client = failingApply{Client: controller, secret: "dev-tunnel-client"}
		reconcileFails(t, r, cp)
		c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
		if c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonIssuing || !strings.Contains(c.Message, "dev-tunnel-client") {
			t.Fatalf("CertificatesReady is %s %s: %s", c.Status, c.Reason, c.Message)
		}
		if !old.Equal(parseTLS(t, secretOf(t, cp, "dev-tunnel-client")).Cert) {
			t.Fatal("the client certificate changed although its apply failed")
		}
		r.Client = controller
		reconcile(t, r, cp)
		expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
		if old.Equal(parseTLS(t, secretOf(t, cp, "dev-tunnel-client")).Cert) {
			t.Error("the next pass did not finish the renewal")
		}
	})
}

func TestAnEarlierRefusedWriteStopsTheTunnelsClientRenewal(t *testing.T) {
	now := clocktesting.NewFakePassiveClock(time.Now())
	r := longLived()
	r.Clock = now
	cp := controlPlaneAt(t, "pki-tunnel-earlier-refused", gatewayEndpoint())
	publishGatewayEndpoint(t, cp)
	reconcile(t, r, cp)
	old := parseTLS(t, secretOf(t, cp, "dev-tunnel-client")).Cert
	if err := k8s.Delete(t.Context(), secretOf(t, cp, "dev-front-proxy-client")); err != nil {
		t.Fatal(err)
	}
	now.SetTime(certs.RenewalTime(old))
	r.Client = failingApply{Client: controller, secret: "dev-front-proxy-client"}
	reconcileFails(t, r, cp)
	c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
	if c.Reason != lodiav1alpha1.ReasonIssuing || !strings.Contains(c.Message, "dev-tunnel-client") {
		t.Fatalf("CertificatesReady is %s: %s", c.Reason, c.Message)
	}
	if !old.Equal(parseTLS(t, secretOf(t, cp, "dev-tunnel-client")).Cert) {
		t.Error("the client certificate was written after an earlier refused write")
	}
}
