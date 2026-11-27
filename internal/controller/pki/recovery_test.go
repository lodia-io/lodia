// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"maps"
	"strconv"
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
	backdate(t, cp, time.Second)
	reconcile(t, r, cp)
	clamped := secretsOf(t, cp)
	for name, s := range clamped {
		if s.Data[lodiav1alpha1.TLSCertKey] == nil || strings.Contains(name, "-cakey-") || s.Labels[lodiav1alpha1.EtcdCertificatesLabel] == "1" {
			continue
		}
		c := parseTLS(t, s).Cert
		issuer := parseTLS(t, first["dev-cakey-"+caOf(name)]).Cert
		if !c.NotAfter.Equal(issuer.NotAfter) {
			t.Errorf("%s ends at %s, want its CA's %s", name, c.NotAfter, issuer.NotAfter)
		}
	}
	expectGenerations(t, cp, 1, 2)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)

	before, _ := resourceVersions(t, cp)
	time.Sleep(time.Until(certs.RenewalTime(parseTLS(t, clamped["dev-apiserver-serving"]).Cert)) + 300*time.Millisecond)
	start := time.Now()
	result := reconcile(t, r, cp)
	after, _ := resourceVersions(t, cp)
	for name, version := range after {
		if name != "dev-client-ca" && name != "dev-cakey-kubeconfig-requests" && before[name] != version {
			t.Errorf("%s, which ends with its CA, was issued again", name)
		}
	}
	signer := parseTLS(t, clamped["dev-kubelet-signer"]).Cert
	clientCA := parseTLS(t, first["dev-cakey-client"]).Cert
	requests := parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert
	expectBundle(t, cp, clientCA, signer, parseTLS(t, first["dev-cakey-kubeconfig-requests"]).Cert, parseTLS(t, clamped["dev-cakey-kubeconfig-requests"]).Cert, requests)
	due := ca.NotAfter
	if renewal := certs.RenewalTime(requests); renewal.Before(due) {
		due = renewal
	}
	if result.RequeueAfter < time.Until(due) || result.RequeueAfter > due.Sub(start)+time.Second {
		t.Errorf("requeue after %s, want the CA's expiry or the request CA's renewal in %s", result.RequeueAfter, time.Until(due))
	}
	expectGenerations(t, cp, 1, 2)

	time.Sleep(time.Until(ca.NotAfter) + 300*time.Millisecond)
	before, _ = resourceVersions(t, cp)
	result = reconcile(t, r, cp)
	c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
	if c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonCAExpired || !strings.Contains(c.Message, "cluster, client, front-proxy, etcd") {
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

	r.Client = failingApply{Client: controller, secret: "dev-etcd-peer-2"}
	reconcileFails(t, r, cp)
	partial := secretsOf(t, cp)
	if partial["dev-etcd-server-2"] == nil || partial["dev-etcd-peer-2"] != nil {
		t.Fatal("generation 2 was not left incomplete")
	}

	r.Client = controller
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
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
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
	if !bytes.Equal(after["dev-ca-bundle"].Data[lodiav1alpha1.CACertKey], want) {
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
	c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
	if c.Reason != lodiav1alpha1.ReasonCAMissing || !strings.Contains(c.Message, "front-proxy") {
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
	expectCondition(t, cp, lodiav1alpha1.ConditionCAValidityAcceptable, metav1.ConditionTrue, lodiav1alpha1.ReasonValid)
	secrets := secretsOf(t, cp)
	crossing := parseTLS(t, secrets["dev-cakey-client"]).Cert.NotAfter.Add(-r.CAExpiryNotice)
	if result.RequeueAfter < time.Until(crossing) || result.RequeueAfter > crossing.Sub(start)+time.Second {
		t.Errorf("requeue after %s, want when the client CA enters its notice in %s", result.RequeueAfter, time.Until(crossing))
	}

	time.Sleep(time.Until(crossing) + 300*time.Millisecond)
	reconcile(t, r, cp)
	c := condition(t, cp, lodiav1alpha1.ConditionCAValidityAcceptable)
	if c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonCAExpiring {
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
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	_, before := resourceVersions(t, cp)
	reconcile(t, r, cp)
	if _, after := resourceVersions(t, cp); after != before {
		t.Error("an unchanged expiring CA rewrote the status")
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
	encoded := map[string][]byte{lodiav1alpha1.TLSCertKey: server.Data[lodiav1alpha1.TLSCertKey], lodiav1alpha1.TLSPrivateKeyKey: pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: sec1})}
	replaceSecret(t, server, encoded)
	before, cpBefore := resourceVersions(t, cp)
	reconcile(t, r, cp)
	after, cpAfter := resourceVersions(t, cp)
	for name, version := range after {
		if name != "dev-etcd-server-1" && before[name] != version {
			t.Errorf("%s was rewritten beside a member written in another encoding", name)
		}
	}
	if cpBefore != cpAfter {
		t.Error("the ControlPlane's status changed")
	}
	if got := secretsOf(t, cp)["dev-etcd-server-1"].Data; !maps.EqualFunc(got, encoded, bytes.Equal) {
		t.Error("the member written in another encoding was rewritten")
	}
	before, cpBefore = resourceVersions(t, cp)
	reconcile(t, r, cp)
	if after, cpAfter := resourceVersions(t, cp); !maps.Equal(before, after) || cpBefore != cpAfter {
		t.Error("a second pass wrote something")
	}
	expectGenerations(t, cp, 1)

	replaceSecret(t, secrets["dev-etcd-client-1"], secrets["dev-etcd-peer-1"].Data)
	reconcile(t, r, cp)
	expectGenerations(t, cp, 1, 2)
}

func TestARenewalNotWrittenIsIssuing(t *testing.T) {
	t.Parallel()
	r := newReconciler(6 * time.Second)
	cp := controlPlane(t, "pki-renewal-unwritten")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	serving := parseTLS(t, secretsOf(t, cp)["dev-apiserver-serving"])
	time.Sleep(time.Until(certs.RenewalTime(serving.Cert)) + 300*time.Millisecond)

	r.Client = failingApply{Client: controller, secret: "dev-apiserver-serving"}
	reconcileFails(t, r, cp)
	c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
	if c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonIssuing || !strings.Contains(c.Message, "dev-apiserver-serving") {
		t.Fatalf("CertificatesReady is %s %s: %s", c.Status, c.Reason, c.Message)
	}
	if !serving.Cert.Equal(parseTLS(t, secretsOf(t, cp)["dev-apiserver-serving"]).Cert) {
		t.Fatal("the serving certificate changed although its apply failed")
	}

	r.Client = controller
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func TestAnAdoptionNotWrittenLeavesCertificatesReady(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-adoption-unwritten")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	s := secretsOf(t, cp)["dev-front-proxy-client"]
	s.Labels = nil
	if err := k8s.Update(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	r.Client = failingApply{Client: controller, secret: "dev-front-proxy-client"}
	reconcileFails(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func TestACANotWrittenIsNotCounted(t *testing.T) {
	r := newReconciler(time.Hour)
	r.Client = failingApply{Client: controller, secret: "dev-cakey-client"}
	cp := controlPlane(t, "pki-ca-unwritten")
	reconcileFails(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCAValidityAcceptable, metav1.ConditionUnknown, lodiav1alpha1.ReasonReconciling)
	c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
	if c.Reason != lodiav1alpha1.ReasonIssuing || !strings.Contains(c.Message, "dev-cakey-client") {
		t.Fatalf("CertificatesReady is %s: %s", c.Reason, c.Message)
	}
}

func TestASecretQuotaRefusingAnEtcdGenerationLetsTheRequestCARenew(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-etcd-quota")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	issued := time.Now().Add(-50 * time.Minute)
	requests, err := certs.NewCA("kubeconfig-requests-ca@"+strconv.FormatInt(issued.UnixNano(), 10), issued, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	s := secretOf(t, cp, "dev-cakey-kubeconfig-requests")
	s.Data = map[string][]byte{lodiav1alpha1.TLSCertKey: mustPEM(t, requests.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, requests.Key)}
	if err := k8s.Update(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(ctx, secretOf(t, cp, "dev-etcd-client-1")); err != nil {
		t.Fatal(err)
	}
	quota := fillSecretQuota(t, cp)
	for range 3 {
		reconcileFails(t, r, cp)
	}
	secrets := secretsOf(t, cp)
	renewed := parseTLS(t, secrets["dev-cakey-kubeconfig-requests"]).Cert
	if renewed.Equal(requests.Cert) {
		t.Fatal("a due request CA was not renewed while the quota refused an etcd generation")
	}
	bundle, err := certs.ParseCerts(secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey])
	if err != nil {
		t.Fatal(err)
	}
	if !containsCert(bundle, renewed) {
		t.Errorf("under the quota the bundle holds %v, without the renewed request CA", subjects(bundle))
	}
	if secrets["dev-etcd-ca-2"] != nil || secrets["dev-etcd-server-1"] == nil {
		t.Error("under the quota generation 2 was written or generation 1 deleted")
	}
	want := "not written yet: dev-etcd-backup-restore-2, dev-etcd-ca-2, dev-etcd-client-2, dev-etcd-peer-2, dev-etcd-server-2; the error is in the log"
	if c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady); c.Reason != lodiav1alpha1.ReasonIssuing || c.Message != want {
		t.Errorf("CertificatesReady is %s: %s", c.Reason, c.Message)
	}

	if err := k8s.Delete(ctx, quota); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, func() bool {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		return err == nil
	})
	expectGenerations(t, cp, 2)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func TestAnEtcdGenerationRenewsWithoutAClientCA(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-etcd-no-client-ca")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	corrupt := secretOf(t, cp, "dev-cakey-client")
	corrupt.Data[lodiav1alpha1.TLSPrivateKeyKey] = []byte("not a key")
	if err := k8s.Update(ctx, corrupt); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(ctx, secretOf(t, cp, "dev-etcd-client-1")); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	expectGenerations(t, cp, 2)
}

// A running tenant that loses both etcd CA Secrets beside two etcd generations gets no new etcd CA: CertificatesReady and CAValidityAcceptable say CAMissing naming the newer generation, both generations stay as they were and every other certificate renews, until the key Secret is back.
func TestNoEtcdCAIsCreatedBesideAnEtcdGeneration(t *testing.T) {
	ctx := t.Context()
	start := time.Now()
	clock := clocktesting.NewFakePassiveClock(start)
	r := newReconciler(time.Hour)
	r.Clock = clock
	cp := controlPlane(t, "pki-etcd-ca-lost")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	clock.SetTime(start.Add(41 * time.Minute))
	reconcile(t, r, cp)
	reconcile(t, r, cp)
	expectGenerations(t, cp, 1, 2)
	issued := secretsOf(t, cp)
	before, _ := resourceVersions(t, cp)
	lose := func() {
		t.Helper()
		for _, name := range []string{"dev-cakey-etcd", "dev-etcd-ca"} {
			if err := k8s.Delete(ctx, secretOf(t, cp, name)); err != nil {
				t.Fatal(err)
			}
		}
	}

	lose()
	reconcile(t, r, cp)
	c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
	if c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonCAMissing ||
		!strings.Contains(c.Message, "dev-cakey-etcd and dev-etcd-ca are gone while etcd generation 2 exists") ||
		!strings.Contains(c.Message, "restore dev-cakey-etcd from a backup without its metadata.ownerReferences") {
		t.Fatalf("CertificatesReady is %s %s: %s", c.Status, c.Reason, c.Message)
	}
	if v := condition(t, cp, lodiav1alpha1.ConditionCAValidityAcceptable); v.Status != metav1.ConditionUnknown || v.Reason != lodiav1alpha1.ReasonCAMissing ||
		!strings.Contains(v.Message, "dev-cakey-etcd and dev-etcd-ca are gone while etcd generation 2 exists") {
		t.Fatalf("CAValidityAcceptable is %s %s: %s", v.Status, v.Reason, v.Message)
	}
	after, _ := resourceVersions(t, cp)
	for _, name := range []string{"dev-cakey-etcd", "dev-etcd-ca"} {
		if _, created := after[name]; created {
			t.Errorf("%s was written beside an etcd generation", name)
		}
	}
	for name, version := range after {
		if before[name] != version {
			t.Errorf("%s was written while the etcd CA is lost", name)
		}
	}
	expectGenerations(t, cp, 1, 2)

	key := issued["dev-cakey-etcd"]
	if err := k8s.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}, Type: key.Type, Data: key.Data}); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	restored := secretsOf(t, cp)
	if !metav1.IsControlledBy(restored["dev-cakey-etcd"], cp) || !maps.EqualFunc(restored["dev-cakey-etcd"].Data, key.Data, bytes.Equal) {
		t.Error("the etcd CA put back was not adopted as it is")
	}
	if !bytes.Equal(restored["dev-etcd-ca"].Data[lodiav1alpha1.CACertKey], issued["dev-etcd-ca"].Data[lodiav1alpha1.CACertKey]) {
		t.Error("dev-etcd-ca does not hold the etcd CA put back")
	}
	for name, s := range restored {
		if name != "dev-cakey-etcd" && name != "dev-etcd-ca" && before[name] != s.ResourceVersion {
			t.Errorf("%s was written once the etcd CA was back", name)
		}
	}
	expectGenerations(t, cp, 1, 2)

	lose()
	clock.SetTime(start.Add(90 * time.Minute))
	reconcileOnce(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonCAMissing)
	renewed := secretsOf(t, cp)
	for _, name := range []string{"dev-apiserver-serving", "dev-apiserver-kubelet-client", "dev-front-proxy-client", "dev-konnectivity-server",
		"dev-konnectivity-kubeconfig", "dev-controller-manager-kubeconfig", "dev-scheduler-kubeconfig", "dev-lodia-kubeconfig", "dev-admin-kubeconfig"} {
		if leafOf(t, renewed[name]).Cert.Equal(leafOf(t, issued[name]).Cert) {
			t.Errorf("%s was not renewed while the etcd CA is lost", name)
		}
	}
	for name, s := range renewed {
		if name == "dev-cakey-etcd" || name == "dev-etcd-ca" {
			t.Errorf("%s was written beside an etcd generation", name)
		}
		if (name == "dev-apiserver-etcd-client" || s.Labels[lodiav1alpha1.EtcdCertificatesLabel] != "") && s.ResourceVersion != before[name] {
			t.Errorf("%s, signed by the lost etcd CA, was written", name)
		}
	}
	expectGenerations(t, cp, 1, 2)
}

// A tenant that loses both etcd CA Secrets while a finalizer holds its etcd generation being deleted gets no new etcd CA.
func TestNoEtcdCAIsCreatedBesideAnEtcdGenerationBeingDeleted(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-etcd-ca-lost-held")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	for name, s := range secretsOf(t, cp) {
		if s.Labels[lodiav1alpha1.EtcdCertificatesLabel] != "" {
			s.Finalizers = []string{"example.com/hold"}
			if err := k8s.Update(ctx, s); err != nil {
				t.Fatal(err)
			}
		} else if name != "dev-cakey-etcd" && name != "dev-etcd-ca" {
			continue
		}
		if err := k8s.Delete(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	reconcileOnce(t, r, cp)
	c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
	if c.Reason != lodiav1alpha1.ReasonCAMissing || !strings.Contains(c.Message, "while etcd generation 1 exists") {
		t.Fatalf("CertificatesReady is %s: %s", c.Reason, c.Message)
	}
	if secretsOf(t, cp)["dev-cakey-etcd"] != nil {
		t.Error("an etcd CA was created beside the generation being deleted")
	}
}

// A running tenant with an etcd generation that loses the cluster CA or the client CA, key and bundle, gets a new one: only the etcd CA is held back by a generation.
func TestANewCAIsCreatedBesideAnEtcdGenerationForAnyCAButEtcd(t *testing.T) {
	for _, c := range []struct{ name, key, bundle string }{
		{"cluster", "dev-cakey-cluster", "dev-ca-bundle"},
		{"client", "dev-cakey-client", "dev-client-ca"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newReconciler(time.Hour)
			cp := controlPlane(t, "pki-new-ca-"+c.name)
			publishEndpoint(t, cp)
			reconcile(t, r, cp)
			expectGenerations(t, cp, 1)
			lost := parseTLS(t, secretOf(t, cp, c.key)).Cert
			for _, name := range []string{c.key, c.bundle} {
				if err := k8s.Delete(t.Context(), secretOf(t, cp, name)); err != nil {
					t.Fatal(err)
				}
			}
			reconcile(t, r, cp)
			created := secretsOf(t, cp)[c.key]
			if created == nil || parseTLS(t, created).Cert.Equal(lost) {
				t.Fatalf("%s was not created anew beside etcd generation 1", c.key)
			}
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
			expectGenerations(t, cp, 1)
		})
	}
}

// A previous etcd generation is kept only if its ca.crt holds the etcd CA alone and the etcd CA signed each of its leaves, expired or not.
func TestAPreviousEtcdGenerationIsKeptOnlyIfItChains(t *testing.T) {
	other, err := certs.NewCA("etcd-ca@1", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	etcdAlone := func(etcd *certs.Certificate) []*x509.Certificate { return []*x509.Certificate{etcd.Cert} }
	signedByOther := func(leaf string) func(*certs.Certificate, string) *certs.Certificate {
		return func(etcd *certs.Certificate, role string) *certs.Certificate {
			if role == leaf {
				return other
			}
			return etcd
		}
	}
	for i, c := range []struct {
		name   string
		bundle func(etcd *certs.Certificate) []*x509.Certificate
		signer func(etcd *certs.Certificate, role string) *certs.Certificate
		issued time.Time
		want   []int
	}{
		{"of another etcd CA", func(*certs.Certificate) []*x509.Certificate { return []*x509.Certificate{other.Cert} },
			func(*certs.Certificate, string) *certs.Certificate { return other }, time.Now(), []int{1, 3}},
		{"whose ca.crt holds the etcd CA before another", func(etcd *certs.Certificate) []*x509.Certificate { return []*x509.Certificate{etcd.Cert, other.Cert} },
			func(etcd *certs.Certificate, _ string) *certs.Certificate { return etcd }, time.Now(), []int{1, 3}},
		{"whose ca.crt holds the etcd CA after another", func(etcd *certs.Certificate) []*x509.Certificate { return []*x509.Certificate{other.Cert, etcd.Cert} },
			func(etcd *certs.Certificate, _ string) *certs.Certificate { return etcd }, time.Now(), []int{1, 3}},
		{"with a server leaf another CA signed", etcdAlone, signedByOther("server"), time.Now(), []int{1, 3}},
		{"with a client leaf another CA signed", etcdAlone, signedByOther("client"), time.Now(), []int{1, 3}},
		{"with a peer leaf another CA signed", etcdAlone, signedByOther("peer"), time.Now(), []int{1, 3}},
		{"with a backup-restore leaf another CA signed", etcdAlone, signedByOther("backup-restore"), time.Now(), []int{1, 3}},
		{"whose leaves expired", func(etcd *certs.Certificate) []*x509.Certificate { return []*x509.Certificate{etcd.Cert} },
			func(etcd *certs.Certificate, _ string) *certs.Certificate { return etcd }, time.Now().Add(-2 * time.Hour), []int{2, 3}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newReconciler(time.Hour)
			cp := controlPlane(t, "pki-etcd-chain-"+strconv.Itoa(i))
			reconcile(t, r, cp)
			etcd := parseTLS(t, secretOf(t, cp, "dev-cakey-etcd"))
			writeGeneration(t, cp, 2, c.bundle(etcd), func(role string) *certs.Certificate { return c.signer(etcd, role) }, c.issued)
			reconcile(t, r, cp)
			expectGenerations(t, cp, c.want...)
			for name, s := range secretsOf(t, cp) {
				if s.Labels[lodiav1alpha1.EtcdCertificatesLabel] != "" && !metav1.IsControlledBy(s, cp) {
					t.Errorf("%s, which PKI keeps, was not adopted", name)
				}
			}
		})
	}
}

// A current etcd generation whose ca Secret holds the etcd CA beside another, or is not immutable, is replaced by the next, and the old one is kept as previous only if it still chains.
func TestACurrentEtcdGenerationNeedsItsCASecretImmutableAndHoldingTheEtcdCAAlone(t *testing.T) {
	other, err := certs.NewCA("etcd-ca@1", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range []struct {
		name      string
		bundle    func(etcd []byte) []byte
		immutable bool
		want      []int
	}{
		{"holding another CA beside the etcd CA", func(etcd []byte) []byte { return append(etcd, mustPEM(t, other.Cert)...) }, true, []int{2}},
		{"not immutable", func(etcd []byte) []byte { return etcd }, false, []int{1, 2}},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := t.Context()
			r := newReconciler(time.Hour)
			cp := controlPlane(t, "pki-etcd-current-ca-"+strconv.Itoa(i))
			reconcile(t, r, cp)
			expectGenerations(t, cp, 1)
			s := secretOf(t, cp, "dev-etcd-ca-1")
			if err := k8s.Delete(ctx, s); err != nil {
				t.Fatal(err)
			}
			replaced := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: s.Name, Namespace: s.Namespace, Labels: s.Labels, OwnerReferences: s.OwnerReferences}, Type: s.Type,
				Data: map[string][]byte{lodiav1alpha1.CACertKey: c.bundle(s.Data[lodiav1alpha1.CACertKey])}, Immutable: new(c.immutable)}
			if err := k8s.Create(ctx, replaced); err != nil {
				t.Fatal(err)
			}
			reconcile(t, r, cp)
			expectGenerations(t, cp, c.want...)
		})
	}
}

func writeGeneration(t *testing.T, cp *lodiav1alpha1.ControlPlane, g int, bundle []*x509.Certificate, signer func(role string) *certs.Certificate, issued time.Time) {
	t.Helper()
	labels := map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name, lodiav1alpha1.EtcdCertificatesLabel: strconv.Itoa(g)}
	write := func(role string, typ corev1.SecretType, data map[string][]byte) {
		s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cp.Name + "-etcd-" + role + "-" + strconv.Itoa(g), Namespace: cp.Namespace, Labels: labels}, Type: typ, Data: data, Immutable: new(true)}
		if err := k8s.Create(t.Context(), s); err != nil {
			t.Fatal(err)
		}
	}
	var ca []byte
	for _, c := range bundle {
		ca = append(ca, mustPEM(t, c)...)
	}
	write("ca", corev1.SecretTypeOpaque, map[string][]byte{lodiav1alpha1.CACertKey: ca})
	for _, role := range []string{"server", "client", "peer", "backup-restore"} {
		c, err := signer(role).Issue(certs.Profile{CommonName: cp.Name + "-etcd-" + role, Usages: serverClientAuth}, issued, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		write(role, corev1.SecretTypeTLS, map[string][]byte{lodiav1alpha1.TLSCertKey: mustPEM(t, c.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, c.Key)})
	}
}
