// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"bytes"
	"crypto"
	"crypto/x509"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
)

func TestTheRequestCAIsKeptLikeTheKubeletSigner(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "kcr-request-ca")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	first := secretsOf(t, cp)
	clientCA, signer := parseTLS(t, first["dev-cakey-client"]).Cert, parseTLS(t, first["dev-kubelet-signer"]).Cert
	original := parseTLS(t, first["dev-cakey-kubeconfig-requests"]).Cert
	if !original.IsCA || original.CheckSignatureFrom(original) != nil || !strings.HasPrefix(original.Subject.CommonName, "kubeconfig-requests-ca@") || !stamped.MatchString(original.Subject.CommonName) {
		t.Fatalf("the request CA is %q, want a root kubeconfig-requests-ca@<unix nanoseconds>", original.Subject.CommonName)
	}
	if lifetime := original.NotAfter.Sub(original.NotBefore.Add(certs.Backdate)); lifetime != time.Hour {
		t.Errorf("the request CA lives %s, want the leaves' hour", lifetime)
	}
	expectBundle(t, cp, clientCA, signer, original)

	before := secretsOf(t, cp)
	old := before["dev-client-ca"]
	old.Data = map[string][]byte{lodiav1alpha1.CACertKey: mustBundle(t, clientCA, signer)}
	if err := k8s.Update(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(ctx, before["dev-cakey-kubeconfig-requests"]); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	created := parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert
	if created.Equal(original) {
		t.Fatal("no request CA was created for a tenant without one")
	}
	expectBundle(t, cp, clientCA, signer, created)
	if !bytes.Equal(secretsOf(t, cp)["dev-kubelet-signer"].Data[lodiav1alpha1.TLSCertKey], before["dev-kubelet-signer"].Data[lodiav1alpha1.TLSCertKey]) {
		t.Error("the kubelet signer changed with the request CA")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)

	if err := k8s.Delete(ctx, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	replaced := parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert
	if replaced.Equal(created) {
		t.Fatal("a deleted request CA was not replaced")
	}
	expectBundle(t, cp, clientCA, signer, replaced, created)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	expectCondition(t, cp, lodiav1alpha1.ConditionCAValidityAcceptable, metav1.ConditionTrue, lodiav1alpha1.ReasonValid)

	unlabelled := secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]
	unlabelled.Labels, unlabelled.OwnerReferences = nil, nil
	if err := k8s.Update(ctx, unlabelled); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	adopted := secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]
	if adopted == nil || !parseTLS(t, adopted).Cert.Equal(replaced) || !metav1.IsControlledBy(adopted, cp) {
		t.Fatal("a request CA without labels or owner was not adopted as it is")
	}

	secrets := secretsOf(t, cp)
	tls := func(c *certs.Certificate) map[string][]byte {
		return map[string][]byte{lodiav1alpha1.TLSCertKey: mustPEM(t, c.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, c.Key)}
	}
	intermediate, err := parseTLS(t, secrets["dev-cakey-client"]).Issue(certs.Profile{CommonName: "kubeconfig-requests-ca@" + strconv.FormatInt(time.Now().UnixNano(), 10), CA: true}, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := certs.NewCA("someone-ca@1", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name    string
		content map[string][]byte
	}{
		{"an unreadable key", map[string][]byte{lodiav1alpha1.TLSCertKey: secrets["dev-cakey-kubeconfig-requests"].Data[lodiav1alpha1.TLSCertKey], lodiav1alpha1.TLSPrivateKeyKey: []byte("not a key")}},
		{"a leaf", tls(parseTLS(t, secrets["dev-apiserver-kubelet-client"]))},
		{"the kubelet signer", tls(parseTLS(t, secrets["dev-kubelet-signer"]))},
		{"an intermediate under the request CA's name", tls(intermediate)},
		{"a root under another name", tls(foreign)},
	} {
		written, err := certs.ParseCerts(c.content[lodiav1alpha1.TLSCertKey])
		if err != nil {
			t.Fatal(err)
		}
		previous := parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert
		kept, err := certs.ParseCerts(secretsOf(t, cp)["dev-client-ca"].Data[lodiav1alpha1.CACertKey])
		if err != nil {
			t.Fatal(err)
		}
		s := secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]
		s.Data = c.content
		if err := k8s.Update(ctx, s); err != nil {
			t.Fatal(err)
		}
		reconcile(t, r, cp)
		renewed := parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert
		if renewed.Equal(previous) || renewed.Equal(written[0]) || renewed.CheckSignatureFrom(renewed) != nil || !strings.HasPrefix(renewed.Subject.CommonName, "kubeconfig-requests-ca@") || !stamped.MatchString(renewed.Subject.CommonName) {
			t.Fatalf("a request CA Secret holding %s was replaced by %s, want a new request CA", c.name, renewed.Subject.CommonName)
		}
		expectBundle(t, cp, append(kept, renewed)...)
		expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	}
}

func TestEachClientBundleRuleKeepsOnlyItsOwnPredecessors(t *testing.T) {
	for _, c := range []struct {
		name                      string
		signerAge, requestCAAge   time.Duration
		keepsSigner, keepsRequest bool
	}{
		{"a request CA replaced 24 hours 2 minutes ago", 0, 24*time.Hour + 2*time.Minute, true, true},
		{"a request CA replaced 24 hours 6 minutes ago", 0, 24*time.Hour + 6*time.Minute, true, false},
		{"a kubelet signer replaced 31 days ago", 31 * 24 * time.Hour, 0, false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := t.Context()
			r := newReconciler(100 * 24 * time.Hour)
			cp := controlPlane(t, "kcr-bundle-"+strings.ReplaceAll(strings.ToLower(strings.TrimSuffix(c.name, " ago")), " ", "-"))
			reconcile(t, r, cp)
			secrets := secretsOf(t, cp)
			clientCA := parseTLS(t, secrets["dev-cakey-client"])
			now := time.Now()
			oldSigner, err := clientCA.Issue(certs.Profile{CommonName: "kubelet-signer@1", CA: true}, now.Add(-c.signerAge-time.Hour), 100*24*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			signer, err := clientCA.Issue(certs.Profile{CommonName: "kubelet-signer@2", CA: true}, now.Add(-c.signerAge), 100*24*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			oldRequests, err := certs.NewCA("kubeconfig-requests-ca@1", now.Add(-c.requestCAAge-time.Hour), 100*24*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			requests, err := certs.NewCA("kubeconfig-requests-ca@2", now.Add(-c.requestCAAge), 100*24*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			for name, content := range map[string]map[string][]byte{
				"dev-kubelet-signer":            {lodiav1alpha1.TLSCertKey: mustPEM(t, signer.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, signer.Key)},
				"dev-cakey-kubeconfig-requests": {lodiav1alpha1.TLSCertKey: mustPEM(t, requests.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, requests.Key)},
				"dev-client-ca":                 {lodiav1alpha1.CACertKey: mustBundle(t, clientCA.Cert, signer.Cert, oldSigner.Cert, requests.Cert, oldRequests.Cert)},
			} {
				s := secrets[name]
				s.Data = content
				if err := k8s.Update(ctx, s); err != nil {
					t.Fatal(err)
				}
			}
			reconcile(t, r, cp)
			after := secretsOf(t, cp)
			if !bytes.Equal(after["dev-kubelet-signer"].Data[lodiav1alpha1.TLSCertKey], mustPEM(t, signer.Cert)) ||
				!bytes.Equal(after["dev-cakey-kubeconfig-requests"].Data[lodiav1alpha1.TLSCertKey], mustPEM(t, requests.Cert)) {
				t.Fatal("the current kubelet signer or request CA was replaced")
			}
			want := []*x509.Certificate{clientCA.Cert, signer.Cert, requests.Cert}
			if c.keepsSigner {
				want = append(want, oldSigner.Cert)
			}
			if c.keepsRequest {
				want = append(want, oldRequests.Cert)
			}
			expectBundle(t, cp, want...)
		})
	}
}

func TestOnlyAnOlderRequestCAIsKeptAsAPredecessor(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "kcr-request-ca-foreign")
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)
	clientCA, signer := parseTLS(t, secrets["dev-cakey-client"]).Cert, parseTLS(t, secrets["dev-kubelet-signer"]).Cert
	requests := parseTLS(t, secrets["dev-cakey-kubeconfig-requests"]).Cert
	var roots []*x509.Certificate
	for _, name := range []string{"kubeconfig-requests-ca@1", "someone-ca@1", "client-ca@1", "kubeconfig-requests-ca@9223372036854775807"} {
		c, err := certs.NewCA(name, time.Now().Add(-time.Hour), 2*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, c.Cert)
	}
	leaf := leafOf(t, secrets["dev-lodia-kubeconfig"]).Cert
	old := secrets["dev-client-ca"]
	old.Data = map[string][]byte{lodiav1alpha1.CACertKey: mustBundle(t, clientCA, signer, requests, roots[0], roots[1], roots[2], roots[3], leaf)}
	if err := k8s.Update(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	expectBundle(t, cp, clientCA, signer, requests, roots[0])
}

func TestEachRequestCAIsKeptByItsOwnSuccessor(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(100 * 24 * time.Hour)
	cp := controlPlane(t, "kcr-request-ca-successor")
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)
	clientCA, signer := parseTLS(t, secrets["dev-cakey-client"]).Cert, parseTLS(t, secrets["dev-kubelet-signer"]).Cert
	now := time.Now()
	var chain []*certs.Certificate
	for i, age := range []time.Duration{48 * time.Hour, 24*time.Hour + 30*time.Minute, 10 * time.Minute, time.Minute} {
		c, err := certs.NewCA("kubeconfig-requests-ca@"+strconv.Itoa(i+1), now.Add(-age), 100*24*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		chain = append(chain, c)
	}
	current := chain[3]
	for name, content := range map[string]map[string][]byte{
		"dev-cakey-kubeconfig-requests": {lodiav1alpha1.TLSCertKey: mustPEM(t, current.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, current.Key)},
		"dev-client-ca":                 {lodiav1alpha1.CACertKey: mustBundle(t, clientCA, signer, chain[0].Cert, chain[1].Cert, chain[2].Cert, current.Cert)},
	} {
		s := secrets[name]
		s.Data = content
		if err := k8s.Update(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	reconcile(t, r, cp)
	expectBundle(t, cp, clientCA, signer, current.Cert, chain[2].Cert, chain[1].Cert)
}

func TestTheBundleKeepsTheNewestTenRequestCAs(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "kcr-request-ca-cap")
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)
	clientCA, signer := parseTLS(t, secrets["dev-cakey-client"]).Cert, parseTLS(t, secrets["dev-kubelet-signer"]).Cert
	replaced := []*x509.Certificate{parseTLS(t, secrets["dev-cakey-kubeconfig-requests"]).Cert}
	for range 20 {
		if err := k8s.Delete(ctx, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]); err != nil {
			t.Fatal(err)
		}
		reconcile(t, r, cp)
		replaced = append(replaced, parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert)
	}
	want := append([]*x509.Certificate{clientCA, signer}, replaced[len(replaced)-11:]...)
	expectBundle(t, cp, want...)
	reconcile(t, r, cp)
	expectBundle(t, cp, want...)
}

func TestARequestCAIsWrittenLastAndJoinsTheBundleOnTheNextPass(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	r := newReconciler(6 * time.Second)
	cp := controlPlane(t, "kcr-request-ca-order")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	first := secretsOf(t, cp)
	clientCA, signer := parseTLS(t, first["dev-cakey-client"]).Cert, parseTLS(t, first["dev-kubelet-signer"]).Cert
	original := parseTLS(t, first["dev-cakey-kubeconfig-requests"]).Cert
	for _, name := range []string{"dev-cakey-kubeconfig-requests", "dev-encryption-config"} {
		if err := k8s.Delete(ctx, first[name]); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Until(certs.RenewalTime(signer)) + 300*time.Millisecond)

	r.Client = failingApply{Client: controller, secret: "dev-cakey-kubeconfig-requests"}
	for range 5 {
		reconcileFails(t, r, cp)
	}
	refused := secretsOf(t, cp)
	if refused["dev-cakey-kubeconfig-requests"] != nil || refused["dev-encryption-config"] == nil {
		t.Fatal("five passes that cannot write the request CA wrote it, or not the encryption configuration")
	}
	for name, s := range first {
		if strings.Contains(name, "-cakey-") || s.Labels[lodiav1alpha1.EtcdCertificatesLabel] != "" ||
			slices.Contains([]string{"dev-ca-bundle", "dev-client-ca", "dev-front-proxy-ca", "dev-etcd-ca", "dev-sa-keys", "dev-encryption-config"}, name) {
			continue
		}
		if leafOf(t, refused[name]).Cert.Equal(leafOf(t, s).Cert) {
			t.Errorf("%s was not renewed while the request CA could not be written", name)
		}
	}
	expectGenerations(t, cp, 1, 2)
	renewed := parseTLS(t, refused["dev-kubelet-signer"]).Cert
	expectBundle(t, cp, clientCA, renewed, signer, original)
	if c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady); c.Reason != lodiav1alpha1.ReasonIssuing || c.Message != "not written yet: dev-cakey-kubeconfig-requests; the error is in the log" {
		t.Fatalf("CertificatesReady is %s: %s", c.Reason, c.Message)
	}

	r.Client = failingApply{Client: controller, secret: "dev-client-ca"}
	reconcileOnce(t, r, cp)
	stored := parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert
	if stored.Equal(original) {
		t.Fatal("no request CA was stored")
	}
	expectBundle(t, cp, clientCA, renewed, signer, original)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	reconcileFails(t, r, cp)
	if !parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert.Equal(stored) {
		t.Fatal("a pass whose bundle write failed replaced the stored request CA")
	}
	expectBundle(t, cp, clientCA, renewed, signer, original)

	r.Client = controller
	reconcileOnce(t, r, cp)
	expectBundle(t, cp, clientCA, renewed, signer, stored, original)
	before, _ := resourceVersions(t, cp)
	reconcileOnce(t, r, cp)
	if after, _ := resourceVersions(t, cp); !maps.Equal(before, after) {
		t.Error("a pass after the request CA joined the bundle wrote a Secret")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func expectBundle(t *testing.T, cp *lodiav1alpha1.ControlPlane, want ...*x509.Certificate) {
	t.Helper()
	got, err := certs.ParseCerts(secretsOf(t, cp)["dev-client-ca"].Data[lodiav1alpha1.CACertKey])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mustBundle(t, got...), mustBundle(t, want...)) {
		t.Errorf("the client CA bundle holds %v, want %v", subjects(got), subjects(want))
	}
}

func mustBundle(t *testing.T, list ...*x509.Certificate) []byte {
	t.Helper()
	out, err := certs.Bundle(list...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func mustPEM(t *testing.T, c *x509.Certificate) []byte {
	t.Helper()
	out, err := certs.CertPEM(c)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func mustKeyPEM(t *testing.T, key crypto.PrivateKey) []byte {
	t.Helper()
	out, err := certs.KeyPEM(key)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
