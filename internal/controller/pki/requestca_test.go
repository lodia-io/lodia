// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"bytes"
	"crypto"
	"crypto/x509"
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

	leaf := parseTLS(t, secretsOf(t, cp)["dev-apiserver-kubelet-client"])
	for name, content := range map[string]map[string][]byte{
		"an unreadable key": {lodiav1alpha1.TLSCertKey: secretsOf(t, cp)["dev-cakey-kubeconfig-requests"].Data[lodiav1alpha1.TLSCertKey], lodiav1alpha1.TLSPrivateKeyKey: []byte("not a key")},
		"a leaf":            {lodiav1alpha1.TLSCertKey: mustPEM(t, leaf.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, leaf.Key)},
	} {
		previous := parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert
		kept, err := certs.ParseCerts(secretsOf(t, cp)["dev-client-ca"].Data[lodiav1alpha1.CACertKey])
		if err != nil {
			t.Fatal(err)
		}
		s := secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]
		s.Data = content
		if err := k8s.Update(ctx, s); err != nil {
			t.Fatal(err)
		}
		reconcile(t, r, cp)
		renewed := parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert
		if renewed.Equal(previous) || !renewed.IsCA {
			t.Fatalf("a request CA Secret holding %s was not replaced by a CA", name)
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
		{"a request CA replaced 23 hours ago", 0, 23 * time.Hour, true, true},
		{"a request CA replaced 25 hours ago", 0, 25 * time.Hour, true, false},
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
