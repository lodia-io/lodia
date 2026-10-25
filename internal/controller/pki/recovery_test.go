// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"maps"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/certs"
	"github.com/segla-io/brygga/internal/controller/pki"
)

func TestALeafEndingWithItsCAIsIssuedOnceAndAnExpiredCAWritesNothing(t *testing.T) {
	t.Parallel()
	r := newReconciler(6 * time.Second)
	r.CALifetime, r.TrustAnchorLifetime = 9*time.Second, 9*time.Second
	cp := controlPlane(t, "pki-ca-expiry")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	first := secretsOf(t, cp)
	ca := parseTLS(t, first["dev-cakey-cluster"]).Cert
	serving := parseTLS(t, first["dev-apiserver-serving"]).Cert
	if !serving.NotAfter.Before(ca.NotAfter) {
		t.Fatal("the first serving certificate already ends with its CA")
	}

	time.Sleep(time.Until(certs.RenewalTime(serving)) + 300*time.Millisecond)
	reconcile(t, r, cp)
	clamped := secretsOf(t, cp)
	for name, s := range clamped {
		if s.Data["tls.crt"] == nil || strings.Contains(name, "-cakey-") || s.Labels[bryggav1alpha1.EtcdCertificatesLabel] == "1" {
			continue
		}
		c := parseTLS(t, s).Cert
		issuer := parseTLS(t, first["dev-cakey-"+caOf(name)]).Cert
		if !c.NotAfter.Equal(issuer.NotAfter) {
			t.Errorf("%s ends at %s, want its CA's %s", name, c.NotAfter, issuer.NotAfter)
		}
	}
	expectGenerations(t, cp, 1, 2)
	expectCondition(t, cp, bryggav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, pki.ReasonIssued)

	before, _ := resourceVersions(t, cp)
	time.Sleep(time.Until(certs.RenewalTime(parseTLS(t, clamped["dev-apiserver-serving"]).Cert)) + 300*time.Millisecond)
	start := time.Now()
	result := reconcile(t, r, cp)
	after, _ := resourceVersions(t, cp)
	for name, version := range after {
		if name != "dev-client-ca" && before[name] != version {
			t.Errorf("%s, which ends with its CA, was issued again", name)
		}
	}
	signer := parseTLS(t, clamped["dev-kubelet-signer"]).Cert
	want, err := certs.Bundle(parseTLS(t, first["dev-cakey-client"]).Cert, signer)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(secretsOf(t, cp)["dev-client-ca"].Data["ca.crt"], want) {
		t.Error("the client CA bundle still holds the expired previous signer")
	}
	if result.RequeueAfter < time.Until(ca.NotAfter) || result.RequeueAfter > ca.NotAfter.Sub(start)+time.Second {
		t.Errorf("requeue after %s, want the CA's expiry in %s", result.RequeueAfter, time.Until(ca.NotAfter))
	}
	expectGenerations(t, cp, 1, 2)

	time.Sleep(time.Until(ca.NotAfter) + 300*time.Millisecond)
	before, _ = resourceVersions(t, cp)
	result = reconcile(t, r, cp)
	c := condition(t, cp, bryggav1alpha1.ConditionCertificatesReady)
	if c.Status != metav1.ConditionFalse || c.Reason != pki.ReasonCAExpired || !strings.Contains(c.Message, "cluster, client, front-proxy, etcd") {
		t.Fatalf("CertificatesReady is %s %s: %s", c.Status, c.Reason, c.Message)
	}
	after, cpBefore := resourceVersions(t, cp)
	if !maps.Equal(before, after) {
		t.Error("a Secret was written under an expired CA")
	}
	if result.RequeueAfter != 10*time.Minute {
		t.Errorf("requeue after %s: something is scheduled under an expired CA", result.RequeueAfter)
	}
	reconcile(t, r, cp)
	again, cpAfter := resourceVersions(t, cp)
	if !maps.Equal(after, again) || cpBefore != cpAfter {
		t.Error("a pass after the CA expired wrote something")
	}
	expectGenerations(t, cp, 1, 2)
}

func caOf(secret string) string {
	switch {
	case strings.Contains(secret, "-etcd-"):
		return "etcd"
	case strings.Contains(secret, "front-proxy"):
		return "front-proxy"
	case strings.Contains(secret, "kubeconfig"), strings.HasSuffix(secret, "-kubelet-signer"):
		return "client"
	default:
		return "cluster"
	}
}

func TestEtcdGenerationsAreKeptByCompleteness(t *testing.T) {
	t.Parallel()
	r := newReconciler(6 * time.Second)
	cp := controlPlane(t, "pki-etcd-partial")
	reconcile(t, r, cp)
	expectGenerations(t, cp, 1)
	server := parseTLS(t, secretsOf(t, cp)["dev-etcd-server-1"])
	time.Sleep(time.Until(certs.RenewalTime(server.Cert)) + 300*time.Millisecond)

	r.Client = failingApply{Client: k8s, secret: "dev-etcd-peer-2"}
	reconcileFails(t, r, cp)
	partial := secretsOf(t, cp)
	if partial["dev-etcd-server-2"] == nil || partial["dev-etcd-peer-2"] != nil {
		t.Fatal("generation 2 was not left incomplete")
	}

	r.Client = k8s
	reconcile(t, r, cp)
	expectGenerations(t, cp, 1, 3)
}

func TestAnUnlabelledCAKeyIsKeptNotReplaced(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-restored-ca")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	before := secretsOf(t, cp)
	key := before["dev-cakey-cluster"]
	for _, name := range []string{"dev-cakey-cluster", "dev-ca-bundle"} {
		if err := k8s.Delete(ctx, before[name]); err != nil {
			t.Fatal(err)
		}
	}
	if err := k8s.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}, Type: key.Type, Data: key.Data}); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, cp)
	expectCondition(t, cp, bryggav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, pki.ReasonIssued)
	after := secretsOf(t, cp)
	kept := after["dev-cakey-cluster"]
	if kept == nil {
		t.Fatal("the restored key Secret did not get its labels")
	}
	if !maps.EqualFunc(kept.Data, key.Data, bytes.Equal) {
		t.Fatal("the restored CA was replaced")
	}
	if !metav1.IsControlledBy(kept, cp) {
		t.Errorf("the restored key Secret is owned by %+v", kept.OwnerReferences)
	}
	want, err := certs.Bundle(parseTLS(t, key).Cert)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after["dev-ca-bundle"].Data["ca.crt"], want) {
		t.Error("the cluster CA bundle does not hold the restored CA")
	}
	if after["dev-apiserver-serving"].ResourceVersion != before["dev-apiserver-serving"].ResourceVersion {
		t.Error("the serving certificate was re-issued under the restored CA")
	}

	if err := k8s.Delete(ctx, after["dev-cakey-front-proxy"]); err != nil {
		t.Fatal(err)
	}
	bundle := after["dev-front-proxy-ca"]
	bundle.Labels = nil
	if err := k8s.Update(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	c := condition(t, cp, bryggav1alpha1.ConditionCertificatesReady)
	if c.Reason != pki.ReasonCAMissing || !strings.Contains(c.Message, "front-proxy") {
		t.Fatalf("CertificatesReady is %s: %s", c.Reason, c.Message)
	}
	if secretsOf(t, cp)["dev-cakey-front-proxy"] != nil {
		t.Fatal("a new front-proxy CA was created beside its unlabelled bundle")
	}
}

func TestCAValidityIsAnnouncedOutsideReady(t *testing.T) {
	t.Parallel()
	r := newReconciler(time.Hour)
	r.CALifetime, r.TrustAnchorLifetime = time.Hour, 2*time.Hour
	r.CAExpiryNotice = time.Hour - 3*time.Second
	cp := controlPlane(t, "pki-ca-notice")
	publishEndpoint(t, cp)
	start := time.Now()
	result := reconcile(t, r, cp)
	expectCondition(t, cp, bryggav1alpha1.ConditionCAValidityAcceptable, metav1.ConditionTrue, pki.ReasonValid)
	secrets := secretsOf(t, cp)
	crossing := parseTLS(t, secrets["dev-cakey-client"]).Cert.NotAfter.Add(-r.CAExpiryNotice)
	if result.RequeueAfter < time.Until(crossing) || result.RequeueAfter > crossing.Sub(start)+time.Second {
		t.Errorf("requeue after %s, want when the client CA enters its notice in %s", result.RequeueAfter, time.Until(crossing))
	}

	time.Sleep(time.Until(crossing) + 300*time.Millisecond)
	reconcile(t, r, cp)
	c := condition(t, cp, bryggav1alpha1.ConditionCAValidityAcceptable)
	if c.Status != metav1.ConditionFalse || c.Reason != pki.ReasonCAExpiring {
		t.Fatalf("CAValidityAcceptable is %s %s", c.Status, c.Reason)
	}
	for _, name := range []string{"client", "front-proxy", "etcd"} {
		at := parseTLS(t, secrets["dev-cakey-"+name]).Cert.NotAfter.UTC().Format(time.RFC3339)
		if !strings.Contains(c.Message, name+" CA at "+at) {
			t.Errorf("the message %q does not name the %s CA and its expiry %s", c.Message, name, at)
		}
	}
	if strings.Contains(c.Message, "cluster CA") {
		t.Errorf("the message %q names the cluster CA, valid beyond the notice", c.Message)
	}
	expectCondition(t, cp, bryggav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, pki.ReasonIssued)
	_, before := resourceVersions(t, cp)
	reconcile(t, r, cp)
	if _, after := resourceVersions(t, cp); after != before {
		t.Error("an unchanged expiring CA rewrote the status")
	}
}

func TestTheClientBundleTrustsASignerBeforeItIsWritten(t *testing.T) {
	t.Parallel()
	r := newReconciler(6 * time.Second)
	cp := controlPlane(t, "pki-signer-order")
	reconcile(t, r, cp)
	first := secretsOf(t, cp)
	signer := parseTLS(t, first["dev-kubelet-signer"]).Cert
	time.Sleep(time.Until(certs.RenewalTime(signer)) + 300*time.Millisecond)

	r.Client = failingApply{Client: k8s, secret: "dev-client-ca"}
	reconcileFails(t, r, cp)
	if !maps.EqualFunc(secretsOf(t, cp)["dev-kubelet-signer"].Data, first["dev-kubelet-signer"].Data, bytes.Equal) {
		t.Fatal("a new kubelet signer was written before the bundle that trusts it")
	}

	r.Client = k8s
	reconcile(t, r, cp)
	after := secretsOf(t, cp)
	renewed := parseTLS(t, after["dev-kubelet-signer"]).Cert
	if renewed.Equal(signer) {
		t.Fatal("the kubelet signer was not renewed")
	}
	bundle, err := certs.ParseCerts(after["dev-client-ca"].Data["ca.crt"])
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []*x509.Certificate{renewed, signer} {
		if !containsCert(bundle, c) {
			t.Errorf("the client CA bundle lacks %s", c.Subject.CommonName)
		}
	}
}

func containsCert(list []*x509.Certificate, c *x509.Certificate) bool {
	for _, l := range list {
		if l.Equal(c) {
			return true
		}
	}
	return false
}

func TestAnImmutableSecretIsWrittenOnce(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-etcd-foreign")
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)
	server := secrets["dev-etcd-server-1"]
	sec1, err := x509.MarshalECPrivateKey(parseTLS(t, server).Key)
	if err != nil {
		t.Fatal(err)
	}
	replaceSecret(t, server, map[string][]byte{"tls.crt": server.Data["tls.crt"], "tls.key": pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: sec1})})
	before, cpBefore := resourceVersions(t, cp)
	reconcile(t, r, cp)
	after, cpAfter := resourceVersions(t, cp)
	if !maps.Equal(before, after) || cpBefore != cpAfter {
		t.Error("a pass rewrote something beside a member written in another encoding")
	}
	expectGenerations(t, cp, 1)

	replaceSecret(t, secrets["dev-etcd-client-1"], secrets["dev-etcd-peer-1"].Data)
	reconcile(t, r, cp)
	expectGenerations(t, cp, 1, 2)
}
