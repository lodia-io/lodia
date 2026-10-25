// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/certs"
	"github.com/segla-io/brygga/internal/controller/pki"
)

const (
	endpointHost = "203.0.113.10"
	endpointPort = 443
	certSAN      = "api.example.test"
)

var (
	cfg    *rest.Config
	k8s    client.Client
	scheme = runtime.NewScheme()

	serverAuth       = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	clientAuth       = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	serverClientAuth = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	stamped          = regexp.MustCompile(`^[a-z-]+@[0-9]+$`)
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("skipping the PKI controller: KUBEBUILDER_ASSETS is not set; run make test")
		os.Exit(0)
	}
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(bryggav1alpha1.AddToScheme(scheme))
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		UseExistingCluster:    new(false),
	}
	var err error
	cfg, err = env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		os.Exit(1)
	}
	k8s, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		_ = env.Stop()
		os.Exit(1)
	}
	code := m.Run()
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop envtest:", err)
	}
	os.Exit(code)
}

func TestALoadBalancerControlPlaneWaitsForItsEndpoint(t *testing.T) {
	cp := controlPlane(t, "pki-manager")
	labelled, err := labels.NewRequirement(bryggav1alpha1.ControlPlaneLabel, selection.Exists, nil)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Cache: cache.Options{
			DefaultNamespaces:           map[string]cache.Config{cp.Namespace: {}},
			DefaultLabelSelector:        labels.NewSelector().Add(*labelled),
			DefaultTransform:            cache.TransformStripManagedFields(),
			ReaderFailOnMissingInformer: true,
			ByObject:                    map[client.Object]cache.ByObject{&bryggav1alpha1.ControlPlane{}: {Label: labels.Everything()}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := newReconciler(time.Hour)
	r.Client, r.Reader = mgr.GetClient(), mgr.GetAPIReader()
	if err := r.SetupWithManager(mgr, 10); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})

	waitFor(t, 30*time.Second, func() bool {
		c := condition(t, cp, bryggav1alpha1.ConditionCertificatesReady)
		return c != nil && c.Status == metav1.ConditionFalse && c.Reason == bryggav1alpha1.ReasonWaitingForEndpoint
	})
	expectCondition(t, cp, bryggav1alpha1.ConditionAccessReady, metav1.ConditionFalse, bryggav1alpha1.ReasonWaitingForAPIServer)
	secrets := secretsOf(t, cp)
	for _, name := range []string{"dev-apiserver-serving", "dev-konnectivity-server", "dev-admin-kubeconfig"} {
		if secrets[name] != nil {
			t.Errorf("%s issued before the endpoint is known", name)
		}
	}
	for _, name := range []string{"dev-cakey-cluster", "dev-kubelet-signer", "dev-brygga-kubeconfig", "dev-sa-keys", "dev-etcd-server-1"} {
		if secrets[name] == nil {
			t.Errorf("%s not issued while waiting for the endpoint", name)
		}
	}

	publishEndpoint(t, cp)
	waitFor(t, 30*time.Second, func() bool {
		c := condition(t, cp, bryggav1alpha1.ConditionCertificatesReady)
		return c != nil && c.Status == metav1.ConditionTrue
	})
	fresh := &bryggav1alpha1.ControlPlane{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		t.Fatal(err)
	}
	ready := meta.FindStatusCondition(fresh.Status.Conditions, bryggav1alpha1.ConditionCertificatesReady)
	if ready.Reason != bryggav1alpha1.ReasonIssued || ready.ObservedGeneration != fresh.Generation {
		t.Errorf("CertificatesReady is %s at generation %d, want %s at %d", ready.Reason, ready.ObservedGeneration, bryggav1alpha1.ReasonIssued, fresh.Generation)
	}
	expectCondition(t, cp, bryggav1alpha1.ConditionCAValidityAcceptable, metav1.ConditionTrue, bryggav1alpha1.ReasonValid)
	creds := fresh.Status.Credentials
	if creds == nil || creds.AdminKubeconfigSecretRef == nil || creds.AdminKubeconfigSecretRef.Name != "dev-admin-kubeconfig" ||
		creds.CABundleSecretRef == nil || creds.CABundleSecretRef.Name != "dev-ca-bundle" {
		t.Errorf("status.credentials is %+v", creds)
	}

	secrets = secretsOf(t, cp)
	rows := secretTable(cp.Name)
	for _, row := range rows {
		s := secrets[row.name]
		if s == nil {
			t.Errorf("%s is missing", row.name)
			continue
		}
		if s.Type != row.typ {
			t.Errorf("%s has type %s, want %s", row.name, s.Type, row.typ)
		}
		if keys := slices.Sorted(maps.Keys(s.Data)); !slices.Equal(keys, row.keys) {
			t.Errorf("%s has keys %v, want %v", row.name, keys, row.keys)
		}
		if !slices.ContainsFunc(s.OwnerReferences, func(o metav1.OwnerReference) bool {
			return o.Kind == "ControlPlane" && o.Name == cp.Name && o.UID == cp.UID && o.Controller != nil && *o.Controller
		}) {
			t.Errorf("%s is not owned by the ControlPlane: %+v", row.name, s.OwnerReferences)
		}
		if s.Labels[bryggav1alpha1.ControlPlaneLabel] != cp.Name {
			t.Errorf("%s has labels %v", row.name, s.Labels)
		}
		for k, v := range row.labels {
			if s.Labels[k] != v {
				t.Errorf("%s has label %s=%q, want %q", row.name, k, s.Labels[k], v)
			}
		}
		if immutable(s) != row.immutable {
			t.Errorf("%s immutable: %t, want %t", row.name, immutable(s), row.immutable)
		}
	}
	if len(secrets) != len(rows) {
		t.Errorf("%d Secrets, want %d: %v", len(secrets), len(rows), slices.Sorted(maps.Keys(secrets)))
	}
}

func TestEveryCertificateVerifiesAgainstItsCA(t *testing.T) {
	start := time.Now()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-table")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)
	ca := func(suffix string) *x509.Certificate {
		t.Helper()
		c := parseTLS(t, secrets["dev-cakey-"+suffix]).Cert
		if !strings.HasPrefix(c.Subject.CommonName, suffix+"-ca@") || !stamped.MatchString(c.Subject.CommonName) {
			t.Errorf("the %s CA is %q, want %s-ca@<unix nanoseconds>", suffix, c.Subject.CommonName, suffix)
		}
		if c.CheckSignatureFrom(c) != nil || !c.IsCA {
			t.Errorf("the %s CA is not a self-signed root", suffix)
		}
		return c
	}
	cluster, clientCA, frontProxy, etcd := ca("cluster"), ca("client"), ca("front-proxy"), ca("etcd")
	ns, n := cp.Namespace, "dev-etcd"
	forms := func(names ...string) []string {
		var out []string
		for _, s := range names {
			out = append(out, s, s+"."+ns, s+"."+ns+".svc", s+"."+ns+".svc.cluster.local")
		}
		return out
	}
	internal := "https://dev-kube-apiserver." + ns + ".svc:443"

	rows := []struct {
		secret         string
		ca             *x509.Certificate
		cn             string
		orgs, dns, ips []string
		usages         []x509.ExtKeyUsage
		isCA           bool
	}{
		{"dev-kubelet-signer", clientCA, "kubelet-signer@", nil, nil, nil, nil, true},
		{"dev-apiserver-serving", cluster, "kube-apiserver", nil,
			[]string{"kubernetes", "kubernetes.default", "kubernetes.default.svc", "kubernetes.default.svc.cluster.local",
				"dev-kube-apiserver", "dev-kube-apiserver." + ns, "dev-kube-apiserver." + ns + ".svc", "localhost", certSAN},
			[]string{"10.96.0.1", "127.0.0.1", endpointHost}, serverAuth, false},
		{"dev-apiserver-kubelet-client", cluster, "kube-apiserver-kubelet-client", []string{"brygga:kubelet-api-admins"}, nil, nil, clientAuth, false},
		{"dev-front-proxy-client", frontProxy, "front-proxy-client", nil, nil, nil, clientAuth, false},
		{"dev-apiserver-etcd-client", etcd, "kube-apiserver-etcd-client", nil, nil, nil, clientAuth, false},
		{"dev-konnectivity-server", cluster, "konnectivity-server", nil, []string{"localhost"}, []string{"127.0.0.1", endpointHost}, serverAuth, false},
		{"dev-konnectivity-kubeconfig", clientCA, "system:konnectivity-server", nil, nil, nil, clientAuth, false},
		{"dev-controller-manager-kubeconfig", clientCA, "system:kube-controller-manager", nil, nil, nil, clientAuth, false},
		{"dev-scheduler-kubeconfig", clientCA, "system:kube-scheduler", nil, nil, nil, clientAuth, false},
		{"dev-admin-kubeconfig", clientCA, "kubernetes-admin", []string{"brygga:cluster-admins"}, nil, nil, clientAuth, false},
		{"dev-brygga-kubeconfig", clientCA, "brygga:operator", nil, nil, nil, clientAuth, false},
		{"dev-etcd-server-1", etcd, n, nil, slices.Concat([]string{n + "-local"}, forms(n+"-client", "*."+n+"-peer"), []string{"localhost"}), []string{"127.0.0.1"}, serverClientAuth, false},
		{"dev-etcd-peer-1", etcd, n + "-peer", nil, forms(n+"-peer", "*."+n+"-peer"), nil, serverClientAuth, false},
		{"dev-etcd-backup-restore-1", etcd, n + "-backup-restore", nil, slices.Concat([]string{n + "-local"}, forms(n+"-client"), []string{"localhost"}), []string{"127.0.0.1"}, serverAuth, false},
		{"dev-etcd-client-1", etcd, n + "-client", nil, nil, nil, clientAuth, false},
	}
	for _, row := range rows {
		t.Run(row.secret, func(t *testing.T) {
			c := leafOf(t, secrets[row.secret])
			if err := certs.Verify(c.Cert, row.ca, time.Now()); err != nil {
				t.Fatal(err)
			}
			if cn := c.Cert.Subject.CommonName; cn != row.cn && (!row.isCA || !strings.HasPrefix(cn, row.cn) || !stamped.MatchString(cn)) {
				t.Errorf("CN %q, want %q", cn, row.cn)
			}
			if !slices.Equal(c.Cert.Subject.Organization, row.orgs) {
				t.Errorf("O %v, want %v", c.Cert.Subject.Organization, row.orgs)
			}
			if got := slices.Sorted(slices.Values(c.Cert.DNSNames)); !slices.Equal(got, slices.Sorted(slices.Values(row.dns))) {
				t.Errorf("DNS SANs %v, want %v", got, row.dns)
			}
			var ips []string
			for _, ip := range c.Cert.IPAddresses {
				ips = append(ips, ip.String())
			}
			if slices.Sort(ips); !slices.Equal(ips, slices.Sorted(slices.Values(row.ips))) {
				t.Errorf("IP SANs %v, want %v", ips, row.ips)
			}
			if !slices.Equal(c.Cert.ExtKeyUsage, row.usages) {
				t.Errorf("extended key usages %v, want %v", c.Cert.ExtKeyUsage, row.usages)
			}
			if c.Cert.IsCA != row.isCA || (row.isCA && (!c.Cert.MaxPathLenZero || c.Cert.MaxPathLen != 0)) {
				t.Errorf("CA:%t with path length %d (zero: %t), want CA:%t with path length 0", c.Cert.IsCA, c.Cert.MaxPathLen, c.Cert.MaxPathLenZero, row.isCA)
			}
			if c.Cert.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
				t.Error("no digitalSignature key usage")
			}
			if c.Cert.NotAfter.After(row.ca.NotAfter) {
				t.Errorf("expires at %s, after its CA at %s", c.Cert.NotAfter, row.ca.NotAfter)
			}
			if issued := c.Cert.NotBefore.Add(certs.Backdate); issued.Before(start.Add(-time.Second)) || issued.After(time.Now()) {
				t.Errorf("valid from %s, want five minutes before its issuance", c.Cert.NotBefore)
			}
		})
	}
	for name, c := range map[string]*x509.Certificate{"cluster": cluster, "client": clientCA, "front-proxy": frontProxy, "etcd": etcd} {
		if issued := c.NotBefore.Add(certs.Backdate); issued.Before(start.Add(-time.Second)) || issued.After(time.Now()) {
			t.Errorf("the %s CA is valid from %s, want five minutes before its issuance", name, c.NotBefore)
		}
	}

	signer := parseTLS(t, secrets["dev-kubelet-signer"]).Cert
	for _, bundle := range []struct {
		secret string
		certs  []*x509.Certificate
	}{
		{"dev-ca-bundle", []*x509.Certificate{cluster}},
		{"dev-client-ca", []*x509.Certificate{signer, clientCA}},
		{"dev-front-proxy-ca", []*x509.Certificate{frontProxy}},
		{"dev-etcd-ca", []*x509.Certificate{etcd}},
		{"dev-etcd-ca-1", []*x509.Certificate{etcd}},
	} {
		want, err := certs.Bundle(bundle.certs...)
		if err != nil {
			t.Fatal(err)
		}
		if got := secrets[bundle.secret].Data["ca.crt"]; !bytes.Equal(got, want) {
			t.Errorf("%s is not the bundle of %v", bundle.secret, subjects(bundle.certs))
		}
	}

	key, err := certs.ParseKey(secrets["dev-sa-keys"].Data["signing.key"])
	if err != nil {
		t.Fatal(err)
	}
	if rsaKey, ok := key.(*rsa.PrivateKey); !ok || rsaKey.N.BitLen() != 2048 {
		t.Errorf("signing.key is %T, want RSA 2048", key)
	}
	if block, _ := pem.Decode(secrets["dev-sa-keys"].Data["signing.key"]); block == nil || block.Type != "PRIVATE KEY" {
		t.Error("signing.key is not PKCS#8")
	}

	var enc apiserverv1.EncryptionConfiguration
	if err := yaml.Unmarshal(secrets["dev-encryption-config"].Data["encryption-config.yaml"], &enc); err != nil {
		t.Fatal(err)
	}
	if len(enc.Resources) != 1 || !slices.Equal(enc.Resources[0].Resources, []string{"secrets"}) || len(enc.Resources[0].Providers) != 1 || enc.Resources[0].Providers[0].Secretbox == nil {
		t.Fatalf("encryption configuration: %+v", enc)
	}
	if keys := enc.Resources[0].Providers[0].Secretbox.Keys; len(keys) != 1 {
		t.Errorf("%d secretbox keys, want 1", len(keys))
	} else if secret, err := base64.StdEncoding.DecodeString(keys[0].Secret); err != nil || len(secret) != 32 {
		t.Errorf("secretbox key of %d bytes (%v), want 32", len(secret), err)
	}

	clusterPEM, err := certs.CertPEM(cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []struct {
		secret, server, user string
		embed                bool
	}{
		{"dev-konnectivity-kubeconfig", "https://127.0.0.1:6443", "system:konnectivity-server", false},
		{"dev-controller-manager-kubeconfig", internal, "system:kube-controller-manager", false},
		{"dev-scheduler-kubeconfig", internal, "system:kube-scheduler", false},
		{"dev-brygga-kubeconfig", internal, "brygga:operator", true},
		{"dev-admin-kubeconfig", "https://" + endpointHost + ":443", "kubernetes-admin", true},
	} {
		s := secrets[k.secret]
		raw, err := clientcmd.Load(s.Data["kubeconfig"])
		if err != nil {
			t.Fatalf("%s: %v", k.secret, err)
		}
		context := raw.Contexts[raw.CurrentContext]
		if context == nil {
			t.Fatalf("%s has no current context", k.secret)
		}
		clusterInfo, user := raw.Clusters[context.Cluster], raw.AuthInfos[context.AuthInfo]
		if clusterInfo.Server != k.server || context.AuthInfo != k.user {
			t.Errorf("%s dials %s as %s, want %s as %s", k.secret, clusterInfo.Server, context.AuthInfo, k.server, k.user)
		}
		if k.embed {
			if !bytes.Equal(clusterInfo.CertificateAuthorityData, clusterPEM) || user.ClientCertificateData == nil || user.ClientKeyData == nil {
				t.Errorf("%s does not embed its certificates", k.secret)
			}
			continue
		}
		if clusterInfo.CertificateAuthority != "ca.crt" || user.ClientCertificate != "tls.crt" || user.ClientKey != "tls.key" {
			t.Errorf("%s references %s, %s, %s", k.secret, clusterInfo.CertificateAuthority, user.ClientCertificate, user.ClientKey)
		}
		if !bytes.Equal(s.Data["ca.crt"], clusterPEM) {
			t.Errorf("%s: ca.crt is not the cluster CA", k.secret)
		}
	}
}

func TestNoSecretButTheCAKeysAndTheKubeletSignerHoldsACAKey(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-cakeys")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)

	var caKeys []crypto.PublicKey
	for _, s := range secrets {
		for _, block := range pemBlocks(t, s) {
			if block.Type != "CERTIFICATE" {
				continue
			}
			c, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			if c.IsCA {
				caKeys = append(caKeys, c.PublicKey)
			}
		}
	}
	if len(caKeys) == 0 {
		t.Fatal("no CA certificate found")
	}
	keys := 0
	for _, name := range slices.Sorted(maps.Keys(secrets)) {
		allowed := strings.Contains(name, "-cakey-") || strings.HasSuffix(name, "-kubelet-signer")
		for _, block := range pemBlocks(t, secrets[name]) {
			if block.Type != "PRIVATE KEY" {
				continue
			}
			keys++
			key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			public := key.(crypto.Signer).Public().(interface{ Equal(crypto.PublicKey) bool })
			isCA := slices.ContainsFunc(caKeys, public.Equal)
			if isCA && !allowed {
				t.Errorf("%s holds the private key of a CA", name)
			}
			if !isCA && allowed {
				t.Errorf("%s holds a key that is no CA's", name)
			}
		}
	}
	if keys < 20 {
		t.Errorf("found %d private keys, expected one per CA, leaf and kubeconfig", keys)
	}
}

func TestEverySecretCarriesTheHashOfItsData(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-data-hash")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	for name, s := range secretsOf(t, cp) {
		if got, want := s.Annotations[bryggav1alpha1.DataHashAnnotation], bryggav1alpha1.DataHash(s.Data); got != want {
			t.Errorf("%s carries %s %q, want %q", name, bryggav1alpha1.DataHashAnnotation, got, want)
		}
	}
}

func TestAnUnchangedControlPlaneWritesNothing(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-unchanged")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	before, cpBefore := resourceVersions(t, cp)
	if len(before) != len(secretTable(cp.Name)) {
		t.Fatalf("%d Secrets after the first pass, want %d", len(before), len(secretTable(cp.Name)))
	}
	reconcile(t, r, cp)
	after, cpAfter := resourceVersions(t, cp)
	for _, name := range slices.Sorted(maps.Keys(before)) {
		if before[name] != after[name] {
			t.Errorf("%s moved from resourceVersion %s to %s", name, before[name], after[name])
		}
	}
	if len(after) != len(before) {
		t.Errorf("%d Secrets after the second pass, want %d", len(after), len(before))
	}
	if cpBefore != cpAfter {
		t.Errorf("the ControlPlane moved from resourceVersion %s to %s", cpBefore, cpAfter)
	}
}

func TestAMovedEndpointReissuesExactlyWhatNamesIt(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-endpoint")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	before, _ := resourceVersions(t, cp)

	const moved = "203.0.113.11"
	publishEndpointAt(t, cp, moved)
	reconcile(t, r, cp)
	after, _ := resourceVersions(t, cp)
	var changed []string
	for _, name := range slices.Sorted(maps.Keys(after)) {
		if before[name] != after[name] {
			changed = append(changed, name)
		}
	}
	if want := []string{"dev-admin-kubeconfig", "dev-apiserver-serving", "dev-konnectivity-server"}; !slices.Equal(changed, want) {
		t.Errorf("a moved endpoint changed %v, want exactly %v", changed, want)
	}
	secrets := secretsOf(t, cp)
	for _, name := range []string{"dev-apiserver-serving", "dev-konnectivity-server"} {
		if c := parseTLS(t, secrets[name]); !slices.ContainsFunc(c.Cert.IPAddresses, func(ip net.IP) bool { return ip.String() == moved }) {
			t.Errorf("%s does not name %s", name, moved)
		}
	}
	raw, err := clientcmd.Load(secrets["dev-admin-kubeconfig"].Data["kubeconfig"])
	if err != nil {
		t.Fatal(err)
	}
	if server := raw.Clusters[cp.Name].Server; server != "https://"+moved+":443" {
		t.Errorf("the admin kubeconfig dials %s", server)
	}
}

func TestLeavesRenewAtTwoThirdsOfTheirLifetime(t *testing.T) {
	t.Parallel()
	const lifetime = 9 * time.Second
	r := newReconciler(lifetime)
	cp := controlPlane(t, "pki-renewal")
	publishEndpoint(t, cp)
	result := reconcile(t, r, cp)
	if result.RequeueAfter <= 0 || result.RequeueAfter > lifetime*2/3 || result.RequeueAfter%time.Second != 0 {
		t.Errorf("requeue after %s, want whole seconds within two thirds of %s", result.RequeueAfter, lifetime)
	}
	before := secretsOf(t, cp)
	serving, signer := parseTLS(t, before["dev-apiserver-serving"]), parseTLS(t, before["dev-kubelet-signer"])
	issued := serving.Cert.NotBefore.Add(certs.Backdate)
	if due := certs.RenewalTime(serving.Cert); due.Sub(issued) < lifetime/2 || due.Sub(issued) > lifetime*3/4 {
		t.Errorf("renewal %s after issuance for a lifetime of %s", due.Sub(issued), lifetime)
	}
	time.Sleep(time.Until(certs.RenewalTime(serving.Cert)) + 500*time.Millisecond)

	reconcile(t, r, cp)
	after := secretsOf(t, cp)
	for name, s := range after {
		prev := before[name]
		generation := s.Labels[bryggav1alpha1.EtcdCertificatesLabel] != ""
		unchanged := strings.Contains(name, "-cakey-") || generation ||
			slices.Contains([]string{"dev-ca-bundle", "dev-front-proxy-ca", "dev-etcd-ca", "dev-sa-keys", "dev-encryption-config"}, name)
		switch {
		case prev == nil:
			if !generation {
				t.Errorf("%s appeared at a leaf renewal", name)
			}
		case name == "dev-client-ca":
		case unchanged:
			if !maps.EqualFunc(prev.Data, s.Data, bytes.Equal) {
				t.Errorf("%s changed at a leaf renewal", name)
			}
		default:
			if maps.EqualFunc(prev.Data, s.Data, bytes.Equal) {
				t.Errorf("%s was not re-issued", name)
			}
		}
	}
	renewed := parseTLS(t, after["dev-apiserver-serving"])
	if renewed.Cert.SerialNumber.Cmp(serving.Cert.SerialNumber) == 0 || !renewed.Cert.NotAfter.After(serving.Cert.NotAfter) {
		t.Error("the serving certificate was not re-issued")
	}
	newSigner := parseTLS(t, after["dev-kubelet-signer"]).Cert
	if newSigner.Equal(signer.Cert) || newSigner.Subject.String() == signer.Cert.Subject.String() {
		t.Fatalf("the kubelet signer was not re-issued under a new subject: %s", newSigner.Subject)
	}
	clientCA := parseTLS(t, after["dev-cakey-client"]).Cert
	want, err := certs.Bundle(clientCA, newSigner, signer.Cert)
	if err != nil {
		t.Fatal(err)
	}
	if got := after["dev-client-ca"].Data["ca.crt"]; !bytes.Equal(got, want) {
		bundle, _ := certs.ParseCerts(got)
		t.Errorf("the client CA bundle holds %v, want the client CA, the new signer and the old one", subjects(bundle))
	}
}

func TestEtcdCertificatesRenewAsGenerations(t *testing.T) {
	t.Parallel()
	const lifetime = 9 * time.Second
	r := newReconciler(lifetime)
	cp := controlPlane(t, "pki-etcd")
	reconcile(t, r, cp)
	expectGenerations(t, cp, 1)
	server := parseTLS(t, secretsOf(t, cp)["dev-etcd-server-1"])
	time.Sleep(time.Until(certs.RenewalTime(server.Cert)) + 500*time.Millisecond)

	reconcile(t, r, cp)
	expectGenerations(t, cp, 1, 2)
	server = parseTLS(t, secretsOf(t, cp)["dev-etcd-server-2"])
	time.Sleep(time.Until(certs.RenewalTime(server.Cert)) + 500*time.Millisecond)

	reconcile(t, r, cp)
	expectGenerations(t, cp, 2, 3)
}

func TestAnIncompleteEtcdGenerationIsReplacedNotRepaired(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-etcd-incomplete")
	reconcile(t, r, cp)
	expectGenerations(t, cp, 1)

	held := secretsOf(t, cp)["dev-etcd-server-1"]
	held.Finalizers = append(held.Finalizers, "druid.gardener.cloud/etcd-druid")
	if err := k8s.Update(ctx, held); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(ctx, held); err != nil {
		t.Fatal(err)
	}
	held = secretsOf(t, cp)["dev-etcd-server-1"]
	if held == nil || held.DeletionTimestamp == nil {
		t.Fatal("the finalizer did not hold the member")
	}
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)
	terminating := secrets["dev-etcd-server-1"]
	if terminating == nil || terminating.ResourceVersion != held.ResourceVersion {
		t.Fatal("the member being deleted was touched instead of leaving generation 1 behind")
	}
	for _, role := range []string{"ca", "server", "client", "peer", "backup-restore"} {
		if secrets["dev-etcd-"+role+"-2"] == nil {
			t.Errorf("generation 2 lacks %s", role)
		}
		if role != "server" && secrets["dev-etcd-"+role+"-1"] != nil {
			t.Errorf("the incomplete generation 1 still holds %s", role)
		}
	}
	terminating.Finalizers = nil
	if err := k8s.Update(ctx, terminating); err != nil {
		t.Fatal(err)
	}
	expectGenerations(t, cp, 2)

	if err := k8s.Delete(ctx, secrets["dev-etcd-client-2"]); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	if secretsOf(t, cp)["dev-etcd-client-2"] != nil {
		t.Error("the missing member of generation 2 was re-issued in place")
	}
	expectGenerations(t, cp, 3)
}

func TestALostCAIsNeverReplaced(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-lost-ca")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	before, _ := resourceVersions(t, cp)

	for _, name := range []string{"dev-cakey-cluster", "dev-cakey-etcd"} {
		if err := k8s.Delete(ctx, secretsOf(t, cp)[name]); err != nil {
			t.Fatal(err)
		}
	}
	reconcile(t, r, cp)
	c := condition(t, cp, bryggav1alpha1.ConditionCertificatesReady)
	if c.Reason != bryggav1alpha1.ReasonCAMissing || !strings.Contains(c.Message, "cluster, etcd") {
		t.Fatalf("CertificatesReady is %s: %s", c.Reason, c.Message)
	}
	after, _ := resourceVersions(t, cp)
	for _, name := range []string{"dev-cakey-cluster", "dev-cakey-etcd"} {
		if _, replaced := after[name]; replaced {
			t.Errorf("the lost CA %s was replaced", name)
		}
	}
	for name, version := range after {
		if before[name] != version {
			t.Errorf("%s was written while the cluster and etcd CAs are lost", name)
		}
	}

	corrupt := secretsOf(t, cp)["dev-cakey-client"]
	corrupt.Data["tls.key"] = []byte("not a key")
	if err := k8s.Update(ctx, corrupt); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	c = condition(t, cp, bryggav1alpha1.ConditionCertificatesReady)
	if c.Reason != bryggav1alpha1.ReasonCAMissing || !strings.Contains(c.Message, "cluster, client, etcd") {
		t.Fatalf("CertificatesReady is %s: %s", c.Reason, c.Message)
	}
	if !bytes.Equal(secretsOf(t, cp)["dev-cakey-client"].Data["tls.key"], []byte("not a key")) {
		t.Fatal("the unreadable client CA key was overwritten")
	}
	_, versionBefore := resourceVersions(t, cp)
	reconcile(t, r, cp)
	if _, versionAfter := resourceVersions(t, cp); versionAfter != versionBefore {
		t.Errorf("the status was rewritten by a pass that changed nothing: %s to %s", versionBefore, versionAfter)
	}
}

func TestAnUnsupportedEncryptionProviderBlocksNothingElse(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-kms")
	cp.Spec.APIServer = &bryggav1alpha1.APIServer{Encryption: &bryggav1alpha1.Encryption{Provider: "kms"}}
	if err := k8s.Update(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	expectCondition(t, cp, bryggav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, bryggav1alpha1.ReasonUnsupported)
	secrets := secretsOf(t, cp)
	if secrets["dev-encryption-config"] != nil {
		t.Error("an encryption configuration was written for kms")
	}
	if want := len(secretTable(cp.Name)) - 1; len(secrets) != want {
		t.Errorf("%d Secrets, want %d: everything but the encryption configuration", len(secrets), want)
	}
	before, versionBefore := resourceVersions(t, cp)
	reconcile(t, r, cp)
	after, versionAfter := resourceVersions(t, cp)
	if !maps.Equal(before, after) || versionBefore != versionAfter {
		t.Error("a second pass wrote something")
	}
}

func TestTheFirstVisitWritesUnknown(t *testing.T) {
	cp := controlPlane(t, "pki-first-visit")
	r := newReconciler(time.Hour)
	r.Reader = failingReader{k8s}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Fatal("a pass with a failing reader succeeded")
	}
	for _, conditionType := range []string{bryggav1alpha1.ConditionCertificatesReady, bryggav1alpha1.ConditionCAValidityAcceptable, bryggav1alpha1.ConditionAccessReady} {
		expectCondition(t, cp, conditionType, metav1.ConditionUnknown, bryggav1alpha1.ReasonReconciling)
	}
	r.Reader = k8s
	reconcile(t, r, cp)
	expectCondition(t, cp, bryggav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, bryggav1alpha1.ReasonWaitingForEndpoint)
}

type failingReader struct {
	client.Reader
}

type failingApply struct {
	client.Client
	secret string
}

func (c failingApply) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	if s, ok := obj.(*corev1ac.SecretApplyConfiguration); ok && *s.GetName() == c.secret {
		return errors.New("the API server refused the apply")
	}
	return c.Client.Apply(ctx, obj, opts...)
}

func replaceSecret(t *testing.T, s *corev1.Secret, content map[string][]byte) {
	t.Helper()
	if err := k8s.Delete(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	fresh := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: s.Name, Namespace: s.Namespace, Labels: s.Labels, OwnerReferences: s.OwnerReferences},
		Type:       s.Type,
		Data:       content,
		Immutable:  new(true),
	}
	if err := k8s.Create(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
}

func reconcileFails(t *testing.T, r *pki.Reconciler, cp *bryggav1alpha1.ControlPlane) {
	t.Helper()
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Fatal("the pass succeeded although an apply failed")
	}
}

func (failingReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("the reader is down")
}

func expectGenerations(t *testing.T, cp *bryggav1alpha1.ControlPlane, want ...int) {
	t.Helper()
	got := map[int][]string{}
	for name, s := range secretsOf(t, cp) {
		if value, ok := s.Labels[bryggav1alpha1.EtcdCertificatesLabel]; ok {
			g, err := strconv.Atoi(value)
			if err != nil {
				t.Fatalf("%s has the label %s=%q", name, bryggav1alpha1.EtcdCertificatesLabel, value)
			}
			got[g] = append(got[g], name)
			if !immutable(s) {
				t.Errorf("%s is not immutable", name)
			}
		}
	}
	if generations := slices.Sorted(maps.Keys(got)); !slices.Equal(generations, want) {
		t.Fatalf("etcd generations %v, want %v", generations, want)
	}
	for _, g := range want {
		var names []string
		for _, role := range []string{"backup-restore", "ca", "client", "peer", "server"} {
			names = append(names, "dev-etcd-"+role+"-"+strconv.Itoa(g))
		}
		if slices.Sort(got[g]); !slices.Equal(got[g], names) {
			t.Errorf("generation %d holds %v, want %v", g, got[g], names)
		}
	}
}

type secretRow struct {
	name      string
	typ       corev1.SecretType
	keys      []string
	labels    map[string]string
	immutable bool
}

func secretTable(cp string) []secretRow {
	tls := []string{"tls.crt", "tls.key"}
	files := []string{"ca.crt", "kubeconfig", "tls.crt", "tls.key"}
	rows := []secretRow{
		{cp + "-cakey-cluster", corev1.SecretTypeTLS, tls, nil, false},
		{cp + "-cakey-client", corev1.SecretTypeTLS, tls, nil, false},
		{cp + "-cakey-front-proxy", corev1.SecretTypeTLS, tls, nil, false},
		{cp + "-cakey-etcd", corev1.SecretTypeTLS, tls, nil, false},
		{cp + "-ca-bundle", corev1.SecretTypeOpaque, []string{"ca.crt"}, nil, false},
		{cp + "-client-ca", corev1.SecretTypeOpaque, []string{"ca.crt"}, nil, false},
		{cp + "-kubelet-signer", corev1.SecretTypeTLS, tls, nil, false},
		{cp + "-apiserver-serving", corev1.SecretTypeTLS, tls, nil, false},
		{cp + "-apiserver-kubelet-client", corev1.SecretTypeTLS, tls, nil, false},
		{cp + "-front-proxy-ca", corev1.SecretTypeOpaque, []string{"ca.crt"}, nil, false},
		{cp + "-front-proxy-client", corev1.SecretTypeTLS, tls, nil, false},
		{cp + "-etcd-ca", corev1.SecretTypeOpaque, []string{"ca.crt"}, nil, false},
		{cp + "-apiserver-etcd-client", corev1.SecretTypeTLS, tls, nil, false},
		{cp + "-konnectivity-server", corev1.SecretTypeTLS, tls, nil, false},
		{cp + "-konnectivity-kubeconfig", corev1.SecretTypeOpaque, files, nil, false},
		{cp + "-controller-manager-kubeconfig", corev1.SecretTypeOpaque, files, nil, false},
		{cp + "-scheduler-kubeconfig", corev1.SecretTypeOpaque, files, nil, false},
		{cp + "-admin-kubeconfig", corev1.SecretTypeOpaque, []string{"kubeconfig"}, nil, false},
		{cp + "-brygga-kubeconfig", corev1.SecretTypeOpaque, []string{"kubeconfig"}, nil, false},
		{cp + "-sa-keys", corev1.SecretTypeOpaque, []string{"signing.key"}, nil, false},
		{cp + "-encryption-config", corev1.SecretTypeOpaque, []string{"encryption-config.yaml"}, nil, false},
	}
	generation := map[string]string{bryggav1alpha1.EtcdCertificatesLabel: "1"}
	rows = append(rows, secretRow{cp + "-etcd-ca-1", corev1.SecretTypeOpaque, []string{"ca.crt"}, generation, true})
	for _, role := range []string{"server", "client", "peer", "backup-restore"} {
		rows = append(rows, secretRow{cp + "-etcd-" + role + "-1", corev1.SecretTypeTLS, tls, generation, true})
	}
	return rows
}

func immutable(s *corev1.Secret) bool {
	return s.Immutable != nil && *s.Immutable
}

func newReconciler(leafLifetime time.Duration) *pki.Reconciler {
	return &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: leafLifetime, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}
}

func controlPlane(t *testing.T, namespace string) *bryggav1alpha1.ControlPlane {
	t.Helper()
	if err := k8s.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil {
		t.Fatal(err)
	}
	cp := &bryggav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: namespace},
		Spec: bryggav1alpha1.ControlPlaneSpec{
			Version:   "v1.37.1",
			Datastore: bryggav1alpha1.DatastoreChoice{Dedicated: &bryggav1alpha1.DedicatedDatastore{}},
			Endpoint:  bryggav1alpha1.Endpoint{LoadBalancer: &bryggav1alpha1.LoadBalancerEndpoint{}, CertSANs: []bryggav1alpha1.Hostname{certSAN}},
			Network:   bryggav1alpha1.Network{ServiceCIDRs: []bryggav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []bryggav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	if err := k8s.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func reconcile(t *testing.T, r *pki.Reconciler, cp *bryggav1alpha1.ControlPlane) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func applyStatus(t *testing.T, cp *bryggav1alpha1.ControlPlane, status map[string]any) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": bryggav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     status,
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner("brygga/workloads"), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func TestTheKonnectivityCertificateNamesTheKonnectivityHost(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-konnectivity")
	const konnectivity = "203.0.113.12"
	applyStatus(t, cp, map[string]any{"endpoint": map[string]any{
		"apiServer":    map[string]any{"host": endpointHost, "port": int64(endpointPort)},
		"konnectivity": map[string]any{"host": konnectivity, "port": int64(8132)},
	}})
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)
	for name, want := range map[string]string{"dev-konnectivity-server": konnectivity, "dev-apiserver-serving": endpointHost} {
		ips := parseTLS(t, secrets[name]).Cert.IPAddresses
		if !slices.ContainsFunc(ips, func(ip net.IP) bool { return ip.String() == want }) {
			t.Errorf("%s names %v, want %s", name, ips, want)
		}
	}
	if ips := parseTLS(t, secrets["dev-konnectivity-server"]).Cert.IPAddresses; slices.ContainsFunc(ips, func(ip net.IP) bool { return ip.String() == endpointHost }) {
		t.Errorf("dev-konnectivity-server names the API server's host %s", endpointHost)
	}
}

func publishEndpoint(t *testing.T, cp *bryggav1alpha1.ControlPlane) {
	t.Helper()
	publishEndpointAt(t, cp, endpointHost)
}

func publishEndpointAt(t *testing.T, cp *bryggav1alpha1.ControlPlane, host string) {
	t.Helper()
	applyStatus(t, cp, map[string]any{"endpoint": map[string]any{
		"apiServer":    map[string]any{"host": host, "port": int64(endpointPort)},
		"konnectivity": map[string]any{"host": host, "port": int64(8132)},
	}})
}

func setAPIServerReady(t *testing.T, cp *bryggav1alpha1.ControlPlane) {
	t.Helper()
	applyStatus(t, cp, map[string]any{"conditions": []any{map[string]any{
		"type":               bryggav1alpha1.ConditionAPIServerReady,
		"status":             string(metav1.ConditionTrue),
		"reason":             bryggav1alpha1.ReasonReady,
		"message":            "",
		"observedGeneration": cp.Generation,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}}})
}

func condition(t *testing.T, cp *bryggav1alpha1.ControlPlane, conditionType string) *metav1.Condition {
	t.Helper()
	fresh := &bryggav1alpha1.ControlPlane{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		t.Fatal(err)
	}
	return meta.FindStatusCondition(fresh.Status.Conditions, conditionType)
}

func expectCondition(t *testing.T, cp *bryggav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	c := condition(t, cp, conditionType)
	if c == nil {
		t.Fatalf("no %s condition", conditionType)
	}
	if c.Status != status || c.Reason != reason {
		t.Fatalf("%s is %s %s (%s), want %s %s", conditionType, c.Status, c.Reason, c.Message, status, reason)
	}
	if c.ObservedGeneration != cp.Generation {
		t.Errorf("%s observed generation %d, want %d", conditionType, c.ObservedGeneration, cp.Generation)
	}
}

func secretsOf(t *testing.T, cp *bryggav1alpha1.ControlPlane) map[string]*corev1.Secret {
	t.Helper()
	var list corev1.SecretList
	if err := k8s.List(t.Context(), &list, client.InNamespace(cp.Namespace), client.MatchingLabels{bryggav1alpha1.ControlPlaneLabel: cp.Name}); err != nil {
		t.Fatal(err)
	}
	out := make(map[string]*corev1.Secret, len(list.Items))
	for i := range list.Items {
		out[list.Items[i].Name] = &list.Items[i]
	}
	return out
}

func resourceVersions(t *testing.T, cp *bryggav1alpha1.ControlPlane) (map[string]string, string) {
	t.Helper()
	versions := map[string]string{}
	for name, s := range secretsOf(t, cp) {
		versions[name] = s.ResourceVersion
	}
	fresh := &bryggav1alpha1.ControlPlane{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		t.Fatal(err)
	}
	return versions, fresh.ResourceVersion
}

func parseTLS(t *testing.T, s *corev1.Secret) *certs.Certificate {
	t.Helper()
	if s == nil {
		t.Fatal("no Secret")
	}
	c, err := certs.Parse(s.Data["tls.crt"], s.Data["tls.key"])
	if err != nil {
		t.Fatalf("%s: %v", s.Name, err)
	}
	return c
}

func leafOf(t *testing.T, s *corev1.Secret) *certs.Certificate {
	t.Helper()
	if s == nil {
		t.Fatal("no Secret")
	}
	if s.Data["tls.crt"] != nil {
		return parseTLS(t, s)
	}
	cert, key, err := certs.Embedded(s.Data["kubeconfig"])
	if err != nil {
		t.Fatalf("%s: %v", s.Name, err)
	}
	c, err := certs.Parse(cert, key)
	if err != nil {
		t.Fatalf("%s: %v", s.Name, err)
	}
	return c
}

func pemBlocks(t *testing.T, s *corev1.Secret) []*pem.Block {
	t.Helper()
	var raw [][]byte
	for key, value := range s.Data {
		if key != "kubeconfig" {
			raw = append(raw, value)
			continue
		}
		kubeconfig, err := clientcmd.Load(value)
		if err != nil {
			t.Fatalf("%s: %v", s.Name, err)
		}
		for _, c := range kubeconfig.Clusters {
			raw = append(raw, c.CertificateAuthorityData)
		}
		for _, u := range kubeconfig.AuthInfos {
			raw = append(raw, u.ClientCertificateData, u.ClientKeyData)
		}
	}
	var blocks []*pem.Block
	for _, data := range raw {
		for block, rest := pem.Decode(data); block != nil; block, rest = pem.Decode(rest) {
			blocks = append(blocks, block)
		}
	}
	return blocks
}

func subjects(list []*x509.Certificate) []string {
	var out []string
	for _, c := range list {
		out = append(out, c.Subject.CommonName)
	}
	return out
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("not true after %s", timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
