// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
	"go.lodia.io/lodia/internal/controller/pki"
	"go.lodia.io/lodia/internal/operator"
)

func TestAKubeconfigRequestIsAnsweredWithACertificateForItsKey(t *testing.T) {
	enforce(t)
	cp := issuable(t, "kcr-issued")
	key := p256(t)
	kr := kubeconfigRequest(t, cp, key, func(s *lodiav1alpha1.KubeconfigRequestSpec) { s.ExpirationSeconds = 1800 })
	now := clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Add(90 * time.Second))
	logs := &logLines{}
	result, err := requestReconciler(now).Reconcile(logs.into(t.Context()), requestFor(kr))
	if err != nil {
		t.Fatal(err)
	}
	deadline := kr.CreationTimestamp.Add(30 * time.Minute)
	if result.RequeueAfter != deadline.Sub(now.Now()) {
		t.Errorf("requeue after %s, want the deadline in %s", result.RequeueAfter, deadline.Sub(now.Now()))
	}

	kr = freshRequest(t, kr)
	expectReady(t, kr, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	secrets := secretsOf(t, cp)
	requestCA := parseTLS(t, secrets["dev-cakey-kubeconfig-requests"]).Cert
	cert, kubeconfig := issued(t, kr)
	if cert.Subject.CommonName != "lodia:user:"+requesterName(t) || cert.Subject.Organization != nil {
		t.Errorf("the certificate is for %s in %v, want lodia:user:%s in no group", cert.Subject.CommonName, cert.Subject.Organization, requesterName(t))
	}
	if !slices.Equal(cert.ExtKeyUsage, clientAuth) || cert.KeyUsage != x509.KeyUsageDigitalSignature || cert.IsCA || len(cert.UnknownExtKeyUsage) > 0 {
		t.Errorf("the certificate has key usage %v, extended %v, CA %t; want a client certificate", cert.KeyUsage, cert.ExtKeyUsage, cert.IsCA)
	}
	if len(cert.DNSNames)+len(cert.IPAddresses)+len(cert.EmailAddresses)+len(cert.URIs) > 0 {
		t.Errorf("the certificate names %v %v %v %v", cert.DNSNames, cert.IPAddresses, cert.EmailAddresses, cert.URIs)
	}
	if want := now.Now().Add(-certs.Backdate).Truncate(time.Second); !cert.NotBefore.Equal(want) {
		t.Errorf("valid from %s, want %s", cert.NotBefore, want)
	}
	if !cert.NotAfter.Equal(deadline) {
		t.Errorf("valid until %s, want the deadline %s", cert.NotAfter, deadline)
	}
	if kr.Status.ExpirationTimestamp == nil || !kr.Status.ExpirationTimestamp.Time.Equal(cert.NotAfter) {
		t.Errorf("status.expirationTimestamp is %v, want %s", kr.Status.ExpirationTimestamp, cert.NotAfter)
	}
	if !key.Public().(*ecdsa.PublicKey).Equal(cert.PublicKey) {
		t.Error("the certificate is not for the request's key")
	}
	if err := certs.Verify(cert, requestCA, now.Now()); err != nil {
		t.Errorf("the certificate does not chain to the request CA: %v", err)
	}
	if certs.Verify(cert, parseTLS(t, secrets["dev-cakey-client"]).Cert, now.Now()) == nil {
		t.Error("the certificate chains to the client CA")
	}
	bundle, err := certs.ParseCerts(secrets["dev-client-ca"].Data[lodiav1alpha1.CACertKey])
	if err != nil {
		t.Fatal(err)
	}
	if !containsCert(bundle, requestCA) {
		t.Error("the client CA bundle lacks the request CA")
	}

	context := kubeconfig.Contexts[kubeconfig.CurrentContext]
	if context == nil {
		t.Fatal("the kubeconfig has no current context")
	}
	cluster, user := kubeconfig.Clusters[context.Cluster], kubeconfig.AuthInfos[context.AuthInfo]
	if context.Cluster != cp.Name || cluster.Server != "https://"+endpointHost+":443" || context.AuthInfo != "lodia:user:"+requesterName(t) {
		t.Errorf("the kubeconfig dials %s of %s as %s", cluster.Server, context.Cluster, context.AuthInfo)
	}
	if !bytes.Equal(cluster.CertificateAuthorityData, secrets["dev-ca-bundle"].Data[lodiav1alpha1.CACertKey]) {
		t.Error("the kubeconfig's CA is not the cluster CA bundle")
	}
	if user.ClientKeyData != nil || user.ClientKey != "" {
		t.Error("the kubeconfig holds a key")
	}
	fingerprint := sha256.Sum256(cert.Raw)
	logs.expect(t, "issued a certificate", "uid", string(kr.UID), "user", "lodia:user:"+requesterName(t), "access", "",
		"credential-id", "X509SHA256="+hex.EncodeToString(fingerprint[:]), "notAfter", deadline.UTC().Format(time.RFC3339))

	before := kr.ResourceVersion
	now.SetTime(now.Now().Add(time.Minute))
	again := &logLines{}
	if _, err := requestReconciler(now).Reconcile(again.into(t.Context()), requestFor(kr)); err != nil {
		t.Fatal(err)
	}
	if after := freshRequest(t, kr); after.ResourceVersion != before || len(again.lines) > 0 {
		t.Errorf("an issued request was answered again: resourceVersion %s, then %s, logged %v", before, after.ResourceVersion, again.lines)
	}
}

func TestAClusterAdminRequestIsInTheAdminGroup(t *testing.T) {
	enforce(t)
	cp := issuable(t, "kcr-cluster-admin")
	kr := kubeconfigRequest(t, cp, p256(t), func(s *lodiav1alpha1.KubeconfigRequestSpec) { s.Access = "ClusterAdmin" })
	reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Time)), kr)
	cert, _ := issued(t, freshRequest(t, kr))
	if cert.Subject.CommonName != "lodia:user:"+requesterName(t) || !slices.Equal(cert.Subject.Organization, []string{"lodia:cluster-admins"}) {
		t.Errorf("the certificate is for %s in %v, want lodia:user:%s in lodia:cluster-admins", cert.Subject.CommonName, cert.Subject.Organization, requesterName(t))
	}
}

func TestTheRequestGivesItsKeyAndNothingElse(t *testing.T) {
	enforce(t)
	cp := issuable(t, "kcr-subject")
	key := p256(t)
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:        pkix.Name{CommonName: "kubernetes-admin", Organization: []string{"system:masters"}},
		DNSNames:       []string{"api.example.test"},
		EmailAddresses: []string{"admin@example.test"},
		ExtraExtensions: []pkix.Extension{{
			Id:    asn1.ObjectIdentifier{2, 5, 29, 19},
			Value: []byte{0x30, 0x03, 0x01, 0x01, 0xff},
		}},
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	kr := kubeconfigRequest(t, cp, key, func(s *lodiav1alpha1.KubeconfigRequestSpec) {
		s.Request = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csr})
	})
	reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Time)), kr)
	cert, _ := issued(t, freshRequest(t, kr))
	if cert.Subject.CommonName != "lodia:user:"+requesterName(t) || cert.Subject.Organization != nil || cert.IsCA ||
		len(cert.DNSNames)+len(cert.EmailAddresses) > 0 || !slices.Equal(cert.ExtKeyUsage, clientAuth) {
		t.Errorf("the certificate took the request's subject or extensions: %s in %v, CA %t, %v %v %v", cert.Subject.CommonName, cert.Subject.Organization, cert.IsCA, cert.DNSNames, cert.EmailAddresses, cert.ExtKeyUsage)
	}
}

func TestEveryKeyTypeTheAPIServerTakesIsIssuedAndNoWeakerOne(t *testing.T) {
	enforce(t)
	cp := issuable(t, "kcr-keys")
	rsaKey := func(bits int) crypto.Signer {
		key, err := rsa.GenerateKey(rand.Reader, bits)
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	ec := func(curve elliptic.Curve) crypto.Signer {
		key, err := ecdsa.GenerateKey(curve, rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	fixture := func(name string) []byte {
		request, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		return request
	}
	for _, c := range []struct {
		name    string
		request []byte
		message string
	}{
		{name: "RSA 2048", request: certificateRequest(t, rsaKey(2048))},
		{name: "RSA 8192", request: fixture("rsa-8192.csr")},
		{name: "ECDSA P-384", request: certificateRequest(t, ec(elliptic.P384()))},
		{name: "ECDSA P-521", request: certificateRequest(t, ec(elliptic.P521()))},
		{name: "Ed25519", request: certificateRequest(t, edKey)},
		{name: "RSA 1024", request: certificateRequest(t, rsaKey(1024)), message: "spec.request carries an RSA key of 1024 bits, not 2048 to 8192; create another request"},
		{name: "RSA 8193", request: fixture("rsa-8193.csr"), message: "spec.request carries an RSA key of 8193 bits, not 2048 to 8192; create another request"},
		{name: "ECDSA P-224", request: p224Request(t), message: "spec.request carries an ECDSA key on P-224, not P-256, P-384 or P-521; create another request"},
	} {
		t.Run(c.name, func(t *testing.T) {
			kr := kubeconfigRequest(t, cp, nil, func(s *lodiav1alpha1.KubeconfigRequestSpec) { s.Request = c.request })
			reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Time)), kr)
			kr = freshRequest(t, kr)
			if c.message != "" {
				expectReady(t, kr, metav1.ConditionFalse, lodiav1alpha1.ReasonRequestInvalid)
				if got := meta.FindStatusCondition(kr.Status.Conditions, lodiav1alpha1.ConditionReady).Message; got != c.message {
					t.Errorf("Ready says %q, want %q", got, c.message)
				}
				return
			}
			cert, _ := issued(t, kr)
			block, _ := pem.Decode(c.request)
			csr, err := x509.ParseCertificateRequest(block.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			if !csr.PublicKey.(interface{ Equal(crypto.PublicKey) bool }).Equal(cert.PublicKey) {
				t.Error("the certificate is not for the request's key")
			}
		})
	}
}

func TestARequestThatIsNoSignedCertificateRequestIsInvalidForGood(t *testing.T) {
	enforce(t)
	cp := issuable(t, "kcr-invalid")
	key, other := p256(t), p256(t)
	csr := certificateRequest(t, key)
	ours, err := x509.MarshalPKIXPublicKey(key.Public())
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := x509.MarshalPKIXPublicKey(other.Public())
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(csr)
	forged := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: bytes.Replace(block.Bytes, ours, theirs, 1)})
	for _, c := range []struct {
		name, request, message string
	}{
		{"no PEM", "-----BEGIN CERTIFICATE REQUEST-----\nA\n-----END CERTIFICATE REQUEST-----\n", "spec.request is not a PEM certificate request; create another request"},
		{"no certificate request inside", string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: []byte("not DER")})), "spec.request does not parse: "},
		{"another key than the one that signed it", string(forged), "spec.request is not signed by the key it carries: "},
	} {
		t.Run(c.name, func(t *testing.T) {
			kr := kubeconfigRequest(t, cp, key, func(s *lodiav1alpha1.KubeconfigRequestSpec) { s.Request = []byte(c.request) })
			now := clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Time)
			result := reconcileRequest(t, requestReconciler(now), kr)
			kr = freshRequest(t, kr)
			expectReady(t, kr, metav1.ConditionFalse, lodiav1alpha1.ReasonRequestInvalid)
			if got := meta.FindStatusCondition(kr.Status.Conditions, lodiav1alpha1.ConditionReady).Message; !strings.HasPrefix(got, c.message) {
				t.Errorf("Ready says %q, want %q", got, c.message)
			}
			if kr.Status.Kubeconfig != nil || kr.Status.ExpirationTimestamp != nil {
				t.Error("an invalid request holds a kubeconfig")
			}
			if want := kr.CreationTimestamp.Add(time.Hour).Sub(now.Now()); result.RequeueAfter != want {
				t.Errorf("requeue after %s, want the deadline in %s", result.RequeueAfter, want)
			}
			deletePolicy(t, true, true)
			now.SetTime(now.Now().Add(time.Minute))
			reconcileRequest(t, requestReconciler(now), kr)
			if after := freshRequest(t, kr); after.ResourceVersion != kr.ResourceVersion {
				t.Error("an invalid request was written again")
			}
		})
	}
}

func TestARequestWaitsForItsControlPlaneItsEndpointAndItsCertificates(t *testing.T) {
	enforce(t)
	ensureNamespace(t, "kcr-waits")
	kr := kubeconfigRequestFor(t, "kcr-waits", "dev", p256(t))
	now := clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Time)
	r := requestReconciler(now)
	deadline := kr.CreationTimestamp.Add(time.Hour)
	since := metav1.NewTime(now.Now().Add(time.Second))
	pass := func(reason string) {
		t.Helper()
		now.SetTime(now.Now().Add(time.Second))
		result := reconcileRequest(t, r, kr)
		if result.RequeueAfter != deadline.Sub(now.Now()) {
			t.Errorf("requeue after %s, want the deadline in %s", result.RequeueAfter, deadline.Sub(now.Now()))
		}
		fresh := freshRequest(t, kr)
		expectReady(t, fresh, metav1.ConditionFalse, reason)
		if c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionReady); !c.LastTransitionTime.Equal(&since) {
			t.Errorf("Ready, False since %s, moved its lastTransitionTime to %s at %s", since, c.LastTransitionTime, reason)
		}
	}

	pass(lodiav1alpha1.ReasonControlPlaneNotFound)
	cp := controlPlane(t, "kcr-waits")
	pass(lodiav1alpha1.ReasonWaitingForEndpoint)
	publishEndpoint(t, cp)
	pass(lodiav1alpha1.ReasonWaitingForCertificates)
	reconcile(t, newReconciler(time.Hour), cp)
	if err := k8s.Delete(t.Context(), secretsOf(t, cp)["dev-ca-bundle"]); err != nil {
		t.Fatal(err)
	}
	pass(lodiav1alpha1.ReasonWaitingForCertificates)
	reconcile(t, newReconciler(time.Hour), cp)
	secrets := secretsOf(t, cp)
	untrusted := secrets["dev-client-ca"]
	untrusted.Data = map[string][]byte{lodiav1alpha1.CACertKey: mustBundle(t, parseTLS(t, secrets["dev-cakey-client"]).Cert, parseTLS(t, secrets["dev-kubelet-signer"]).Cert)}
	if err := k8s.Update(t.Context(), untrusted); err != nil {
		t.Fatal(err)
	}
	pass(lodiav1alpha1.ReasonWaitingForCertificates)
	reconcile(t, newReconciler(time.Hour), cp)
	now.SetTime(now.Now().Add(time.Minute))
	reconcileRequest(t, r, kr)
	fresh := freshRequest(t, kr)
	issued(t, fresh)
	if c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionReady); !c.LastTransitionTime.Time.Equal(now.Now()) {
		t.Errorf("Ready turned True with the lastTransitionTime %s, want the issuing pass's %s", c.LastTransitionTime, now.Now())
	}
}

func TestAPredecessorsCertificatesAreNotUsed(t *testing.T) {
	enforce(t)
	cp := issuable(t, "kcr-predecessor")
	predecessor := secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]
	if err := k8s.Delete(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		return apierrors.IsNotFound(k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), &lodiav1alpha1.ControlPlane{}))
	})
	cp = controlPlane(t, cp.Namespace)
	publishEndpoint(t, cp)
	if left := secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]; left == nil || left.UID != predecessor.UID {
		t.Fatal("the predecessor's request CA is gone before the test")
	}
	kr := kubeconfigRequest(t, cp, p256(t))
	reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Time)), kr)
	expectReady(t, freshRequest(t, kr), metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates)
}

func TestARequestCAPKIWouldReplaceSignsNothing(t *testing.T) {
	enforce(t)
	ctx := t.Context()
	cp := issuable(t, "kcr-unstamped")
	foreign, err := certs.NewCA("someone-ca@1", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	secrets := secretsOf(t, cp)
	held, err := certs.ParseCerts(secrets["dev-client-ca"].Data[lodiav1alpha1.CACertKey])
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]map[string][]byte{
		"dev-cakey-kubeconfig-requests": {lodiav1alpha1.TLSCertKey: mustPEM(t, foreign.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, foreign.Key)},
		"dev-client-ca":                 {lodiav1alpha1.CACertKey: mustBundle(t, append(held, foreign.Cert)...)},
	} {
		s := secrets[name]
		s.Data = content
		if err := k8s.Update(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	kr := kubeconfigRequest(t, cp, p256(t))
	reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Time)), kr)
	expectReady(t, freshRequest(t, kr), metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates)
}

func TestOnlyABundleThatParsesAndHoldsTheRequestCAItselfTrustsIt(t *testing.T) {
	enforce(t)
	for _, c := range []struct {
		name, namespace, secret string
		content                 func(t *testing.T, s *corev1.Secret) map[string][]byte
	}{
		{"a twin of the request CA in its Secret", "kcr-twin", "dev-cakey-kubeconfig-requests", func(t *testing.T, s *corev1.Secret) map[string][]byte {
			twin, err := certs.NewCA(parseTLS(t, s).Cert.Subject.CommonName, time.Now(), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			return map[string][]byte{lodiav1alpha1.TLSCertKey: mustPEM(t, twin.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, twin.Key)}
		}},
		{"a block that does not parse in the client CA bundle", "kcr-unparsed", "dev-client-ca", func(t *testing.T, s *corev1.Secret) map[string][]byte {
			return map[string][]byte{lodiav1alpha1.CACertKey: append(slices.Clone(s.Data[lodiav1alpha1.CACertKey]), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not DER")})...)}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := issuable(t, c.namespace)
			s := secretsOf(t, cp)[c.secret]
			s.Data = c.content(t, s)
			if err := k8s.Update(t.Context(), s); err != nil {
				t.Fatal(err)
			}
			kr := kubeconfigRequest(t, cp, p256(t))
			reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Time)), kr)
			expectReady(t, freshRequest(t, kr), metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates)
		})
	}
}

func TestAPausedOrDeletingControlPlaneIsAnsweredAndNotWritten(t *testing.T) {
	enforce(t)
	ctx := t.Context()
	cp := issuable(t, "kcr-paused")
	cp = freshControlPlane(t, cp)
	cp.Spec.Paused = true
	cp.Finalizers = append(cp.Finalizers, "test.lodia.io/hold")
	if err := k8s.Update(ctx, cp); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		held := &lodiav1alpha1.ControlPlane{}
		if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(cp), held); err != nil {
			t.Fatal(err)
		}
		held.Finalizers = nil
		if err := k8s.Update(context.Background(), held); err != nil {
			t.Error(err)
		}
	})
	paused := kubeconfigRequest(t, cp, p256(t))
	before := freshControlPlane(t, cp).ResourceVersion
	reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(paused.CreationTimestamp.Time)), paused)
	issued(t, freshRequest(t, paused))
	if after := freshControlPlane(t, cp).ResourceVersion; after != before {
		t.Errorf("issuing for a paused ControlPlane wrote it: resourceVersion %s, then %s", before, after)
	}

	if err := k8s.Delete(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if freshControlPlane(t, cp).DeletionTimestamp == nil {
		t.Fatal("the ControlPlane is not being deleted")
	}
	before = freshControlPlane(t, cp).ResourceVersion
	deleting := kubeconfigRequest(t, cp, p256(t))
	reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(deleting.CreationTimestamp.Time)), deleting)
	issued(t, freshRequest(t, deleting))
	if after := freshControlPlane(t, cp).ResourceVersion; after != before {
		t.Errorf("issuing wrote the ControlPlane: resourceVersion %s, then %s", before, after)
	}
}

func TestARequestTheAdmissionPolicyMayNotHaveCheckedIsNotEnforcedForGood(t *testing.T) {
	for _, c := range []struct {
		name, message string
		grace         func(kr *lodiav1alpha1.KubeconfigRequest) time.Duration
		before        func(t *testing.T)
		after         func(t *testing.T, kr *lodiav1alpha1.KubeconfigRequest)
	}{
		{name: "the binding missing", message: "its binding does not exist", before: func(t *testing.T) { deletePolicy(t, false, true) }},
		{name: "the policy missing", message: "the ValidatingAdmissionPolicy does not exist", before: func(t *testing.T) { deletePolicy(t, true, false) }},
		{name: "the binding in Warn", message: "its binding does not Deny", before: func(t *testing.T) {
			replaceBinding(t, func(b *admissionregistrationv1.ValidatingAdmissionPolicyBinding) {
				b.Spec.ValidationActions = []admissionregistrationv1.ValidationAction{admissionregistrationv1.Warn}
			})
		}},
		{name: "the binding with matchResources", message: "its binding narrows matchResources", before: func(t *testing.T) {
			replaceBinding(t, func(b *admissionregistrationv1.ValidatingAdmissionPolicyBinding) {
				b.Spec.MatchResources = &admissionregistrationv1.MatchResources{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"checked": "true"}}}
			})
		}},
		{name: "the binding with a paramRef", message: "its binding has a paramRef", before: func(t *testing.T) {
			replaceBinding(t, func(b *admissionregistrationv1.ValidatingAdmissionPolicyBinding) {
				b.Spec.ParamRef = &admissionregistrationv1.ParamRef{Name: "unchecked", ParameterNotFoundAction: new(admissionregistrationv1.AllowAction)}
			})
		}},
		{name: "the binding naming another policy", message: `its binding names the policy "kubeconfigrequests.example.test"`, before: func(t *testing.T) {
			replaceBinding(t, func(b *admissionregistrationv1.ValidatingAdmissionPolicyBinding) {
				b.Spec.PolicyName = "kubeconfigrequests.example.test"
			})
		}},
		{name: "the binding created after the request", message: "after its binding", grace: func(kr *lodiav1alpha1.KubeconfigRequest) time.Duration {
			policy, _ := chartPolicy(t)
			if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(policy), policy); err != nil {
				t.Fatal(err)
			}
			return kr.CreationTimestamp.Sub(policy.CreationTimestamp.Time)
		}, after: func(t *testing.T, kr *lodiav1alpha1.KubeconfigRequest) {
			time.Sleep(time.Until(kr.CreationTimestamp.Add(time.Second)))
			replaceBinding(t, func(*admissionregistrationv1.ValidatingAdmissionPolicyBinding) {})
		}},
		{name: "the policy created within the grace", message: "the request was created less than 10s after the policy", grace: func(*lodiav1alpha1.KubeconfigRequest) time.Duration { return 10 * time.Second }, before: func(t *testing.T) {
			deletePolicy(t, true, false)
			enforce(t)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			enforce(t)
			cp := issuable(t, "kcr-policy-"+strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(c.name, "the ")), " ", "-"))
			if c.before != nil {
				c.before(t)
			}
			kr := kubeconfigRequest(t, cp, p256(t))
			r := requestReconciler(clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Add(time.Minute)))
			if c.grace != nil {
				r.PolicyGrace = c.grace(kr)
			}
			if c.after != nil {
				c.after(t, kr)
			}
			reconcileRequest(t, r, kr)
			kr = freshRequest(t, kr)
			expectReady(t, kr, metav1.ConditionFalse, lodiav1alpha1.ReasonPolicyNotEnforced)
			if got := meta.FindStatusCondition(kr.Status.Conditions, lodiav1alpha1.ConditionReady).Message; !strings.Contains(got, c.message+";") {
				t.Errorf("Ready says %q, want %q", got, c.message)
			}

			restorePolicy(t)
			reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Add(time.Minute))), kr)
			if after := freshRequest(t, kr); after.ResourceVersion != kr.ResourceVersion || after.Status.Kubeconfig != nil {
				t.Error("a request the policy did not check was answered once the policy was in place")
			}
		})
	}
}

func TestAPassOnAStaleCacheDoesNotIssueTwice(t *testing.T) {
	enforce(t)
	cp := issuable(t, "kcr-stale")
	kr := kubeconfigRequest(t, cp, p256(t))
	stale := freshRequest(t, kr)
	now := clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Time)
	reconcileRequest(t, requestReconciler(now), kr)
	written := freshRequest(t, kr)
	issued(t, written)

	logs := &logLines{}
	r := requestReconciler(now)
	r.Client = staleRequest{Client: controller, kr: stale}
	if _, err := r.Reconcile(logs.into(t.Context()), requestFor(kr)); !apierrors.IsConflict(err) || strings.Contains(err.Error(), "credential-id") {
		t.Fatalf("a pass on a stale request returned %v, want a conflict that names no credential-id", err)
	}
	if after := freshRequest(t, kr); after.ResourceVersion != written.ResourceVersion || !bytes.Equal(after.Status.Kubeconfig, written.Status.Kubeconfig) {
		t.Error("a pass on a stale request replaced the issued certificate")
	}
	if len(logs.lines) > 0 {
		t.Errorf("a pass whose write conflicted logged %v", logs.lines)
	}
}

func TestARequestIsDeletedAtItsDeadline(t *testing.T) {
	enforce(t)
	ctx := t.Context()
	cp := issuable(t, "kcr-deadline")
	kr := kubeconfigRequest(t, cp, p256(t), func(s *lodiav1alpha1.KubeconfigRequestSpec) { s.ExpirationSeconds = 600 })
	deadline := kr.CreationTimestamp.Add(10 * time.Minute)
	now := clocktesting.NewFakePassiveClock(deadline.Add(-time.Second))
	if result := reconcileRequest(t, requestReconciler(now), kr); result.RequeueAfter != time.Second {
		t.Errorf("requeue after %s, want the deadline in 1s", result.RequeueAfter)
	}
	issued(t, freshRequest(t, kr))
	now.SetTime(deadline)
	reconcileRequest(t, requestReconciler(now), kr)
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(kr), &lodiav1alpha1.KubeconfigRequest{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the request at its deadline: %v, want it deleted", err)
	}

	waiting := kubeconfigRequestFor(t, cp.Namespace, "absent", p256(t), func(s *lodiav1alpha1.KubeconfigRequestSpec) { s.ExpirationSeconds = 600 })
	reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(waiting.CreationTimestamp.Time)), waiting)
	expectReady(t, freshRequest(t, waiting), metav1.ConditionFalse, lodiav1alpha1.ReasonControlPlaneNotFound)
	reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(waiting.CreationTimestamp.Add(10*time.Minute))), waiting)
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(waiting), &lodiav1alpha1.KubeconfigRequest{}); !apierrors.IsNotFound(err) {
		t.Fatalf("a waiting request at its deadline: %v, want it deleted", err)
	}
}

func TestADeadlineDeletesOnlyTheRequestItRead(t *testing.T) {
	enforce(t)
	ctx := t.Context()
	cp := issuable(t, "kcr-deleted-meanwhile")
	kr := kubeconfigRequest(t, cp, p256(t), func(s *lodiav1alpha1.KubeconfigRequestSpec) { s.ExpirationSeconds = 600 })
	stale := freshRequest(t, kr)
	if err := k8s.Delete(ctx, kr); err != nil {
		t.Fatal(err)
	}
	r := requestReconciler(clocktesting.NewFakePassiveClock(stale.CreationTimestamp.Add(10 * time.Minute)))
	r.Client = staleRequest{Client: controller, kr: stale}
	reconcileRequest(t, r, stale)

	again := &lodiav1alpha1.KubeconfigRequest{
		ObjectMeta: metav1.ObjectMeta{Name: stale.Name, Namespace: stale.Namespace},
		Spec:       stale.Spec,
	}
	if err := k8s.Create(ctx, again); err != nil {
		t.Fatal(err)
	}
	reconcileRequest(t, r, stale)
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(again), again); err != nil {
		t.Fatalf("a request created again under the name was deleted at its predecessor's deadline: %v", err)
	}
}

func TestTheWaitsEndOnTheirInputs(t *testing.T) {
	enforce(t)
	cp := controlPlane(t, "kcr-manager")
	reconcile(t, newReconciler(time.Hour), cp)
	ensureNamespace(t, "kcr-manager-late")
	reads := startRequests(t, cp.Namespace, "kcr-manager-late")

	early := kubeconfigRequestFor(t, "kcr-manager-late", "dev", p256(t))
	waitForReady(t, early, metav1.ConditionFalse, lodiav1alpha1.ReasonControlPlaneNotFound)
	reads.after(t, early, "dev")
	controlPlane(t, early.Namespace)
	waitForReady(t, early, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint)

	kr := kubeconfigRequest(t, cp, p256(t))
	waitForReady(t, kr, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint)
	reads.after(t, kr, "dev")
	publishEndpoint(t, cp)
	waitForReady(t, kr, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func TestAWaitForCertificatesEndsOnEachOfItsSecrets(t *testing.T) {
	enforce(t)
	ctx := t.Context()
	cp := issuable(t, "kcr-secret-watch")
	reads := startRequests(t, cp.Namespace)
	remove := func(name string) *corev1.Secret {
		t.Helper()
		s := secretsOf(t, cp)[name]
		if err := k8s.Delete(ctx, s); err != nil {
			t.Fatal(err)
		}
		return s
	}

	for _, name := range []string{"dev-cakey-kubeconfig-requests", "dev-ca-bundle"} {
		saved := remove(name)
		restored := kubeconfigRequest(t, cp, p256(t))
		waitForReady(t, restored, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates)
		reads.after(t, restored, name)
		if err := k8s.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: saved.Name, Namespace: saved.Namespace, Labels: saved.Labels, OwnerReferences: saved.OwnerReferences},
			Type:       saved.Type,
			Data:       saved.Data,
		}); err != nil {
			t.Fatal(err)
		}
		waitForReady(t, restored, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	}

	remove("dev-cakey-kubeconfig-requests")
	replaced := kubeconfigRequest(t, cp, p256(t))
	waitForReady(t, replaced, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates)
	reads.after(t, replaced, "dev-cakey-kubeconfig-requests")
	before := freshControlPlane(t, cp).ResourceVersion
	reconcile(t, newReconciler(time.Hour), cp)
	if after := freshControlPlane(t, cp).ResourceVersion; after != before {
		t.Fatalf("PKI's pass wrote the ControlPlane: resourceVersion %s, then %s", before, after)
	}
	waitForReady(t, replaced, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)

	untrusted, err := certs.NewCA("kubeconfig-requests-ca@"+strconv.FormatInt(time.Now().UnixNano(), 10), time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	s := secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]
	s.Data = map[string][]byte{lodiav1alpha1.TLSCertKey: mustPEM(t, untrusted.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, untrusted.Key)}
	if err := k8s.Update(ctx, s); err != nil {
		t.Fatal(err)
	}
	trusted := kubeconfigRequest(t, cp, p256(t))
	waitForReady(t, trusted, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates)
	reads.after(t, trusted, "dev-client-ca")
	bundle := secretsOf(t, cp)["dev-client-ca"]
	held, err := certs.ParseCerts(bundle.Data[lodiav1alpha1.CACertKey])
	if err != nil {
		t.Fatal(err)
	}
	bundle.Data = map[string][]byte{lodiav1alpha1.CACertKey: mustBundle(t, append(held, untrusted.Cert)...)}
	if err := k8s.Update(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	waitForReady(t, trusted, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	if cert, _ := issued(t, freshRequest(t, trusted)); certs.Verify(cert, untrusted.Cert, time.Now()) != nil {
		t.Error("the request was not signed by the request CA the bundle came to hold")
	}
}

func startRequests(t *testing.T, namespaces ...string) *waitedReads {
	t.Helper()
	options, err := operator.Cache()
	if err != nil {
		t.Fatal(err)
	}
	options.DefaultNamespaces = map[string]cache.Config{}
	for _, namespace := range namespaces {
		options.DefaultNamespaces[namespace] = cache.Config{}
	}
	options.ReaderFailOnMissingInformer = true
	mgr, err := ctrl.NewManager(controllerCfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Cache:                  options,
		Controller:             config.Controller{SkipNameValidation: new(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	reads := &waitedReads{reads: map[string]int{}}
	requests := &pki.KubeconfigRequestReconciler{Client: waitedClient{mgr.GetClient(), reads}, Reader: waitedReader{mgr.GetAPIReader(), reads}, Clock: clocktesting.NewFakePassiveClock(time.Now())}
	if err := requests.SetupWithManager(mgr, 1); err != nil {
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
	return reads
}

func waitForReady(t *testing.T, kr *lodiav1alpha1.KubeconfigRequest, status metav1.ConditionStatus, reason string) {
	t.Helper()
	waitFor(t, 30*time.Second, func() bool {
		c := meta.FindStatusCondition(freshRequest(t, kr).Status.Conditions, lodiav1alpha1.ConditionReady)
		return c != nil && c.Status == status && c.Reason == reason
	})
}

type waitedReads struct {
	mu      sync.Mutex
	request string
	waited  bool
	reads   map[string]int
}

func (w *waitedReads) pass(request client.ObjectKey, kr *lodiav1alpha1.KubeconfigRequest) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.request, w.waited = request.String(), kr != nil && meta.FindStatusCondition(kr.Status.Conditions, lodiav1alpha1.ConditionReady) != nil
}

func (w *waitedReads) read(name string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.waited {
		w.reads[w.request+"/"+name]++
	}
}

func (w *waitedReads) after(t *testing.T, kr *lodiav1alpha1.KubeconfigRequest, name string) {
	t.Helper()
	waitFor(t, 30*time.Second, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.reads[client.ObjectKeyFromObject(kr).String()+"/"+name] > 0
	})
}

type waitedClient struct {
	client.Client
	reads *waitedReads
}

func (c waitedClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := c.Client.Get(ctx, key, obj, opts...)
	switch o := obj.(type) {
	case *lodiav1alpha1.KubeconfigRequest:
		if err != nil {
			o = nil
		}
		c.reads.pass(key, o)
	case *lodiav1alpha1.ControlPlane:
		c.reads.read(key.Name)
	}
	return err
}

type waitedReader struct {
	client.Reader
	reads *waitedReads
}

func (r waitedReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok {
		r.reads.read(key.Name)
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

type staleRequest struct {
	client.Client
	kr *lodiav1alpha1.KubeconfigRequest
}

func (c staleRequest) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if kr, ok := obj.(*lodiav1alpha1.KubeconfigRequest); ok {
		c.kr.DeepCopyInto(kr)
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

type logLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *logLines) into(ctx context.Context) context.Context {
	return logf.IntoContext(ctx, funcr.New(func(prefix, args string) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.lines = append(l.lines, args)
	}, funcr.Options{}))
}

func (l *logLines) expect(t *testing.T, message string, keysAndValues ...string) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.lines) != 1 {
		t.Fatalf("logged %d lines, want one: %v", len(l.lines), l.lines)
	}
	want := []string{`"msg"="` + message + `"`}
	for i := 0; i < len(keysAndValues); i += 2 {
		want = append(want, `"`+keysAndValues[i]+`"="`+keysAndValues[i+1]+`"`)
	}
	for _, field := range want {
		if !strings.Contains(l.lines[0], field) {
			t.Errorf("logged %s, want %s", l.lines[0], field)
		}
	}
}

func requestReconciler(clock *clocktesting.FakePassiveClock) *pki.KubeconfigRequestReconciler {
	return &pki.KubeconfigRequestReconciler{Client: controller, Reader: apiReader, Clock: clock}
}

func requestFor(kr *lodiav1alpha1.KubeconfigRequest) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKeyFromObject(kr)}
}

func reconcileRequest(t *testing.T, r *pki.KubeconfigRequestReconciler, kr *lodiav1alpha1.KubeconfigRequest) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(t.Context(), requestFor(kr))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func issuable(t *testing.T, namespace string) *lodiav1alpha1.ControlPlane {
	t.Helper()
	cp := controlPlane(t, namespace)
	publishEndpoint(t, cp)
	reconcile(t, newReconciler(time.Hour), cp)
	return cp
}

func kubeconfigRequest(t *testing.T, cp *lodiav1alpha1.ControlPlane, key crypto.Signer, change ...func(*lodiav1alpha1.KubeconfigRequestSpec)) *lodiav1alpha1.KubeconfigRequest {
	t.Helper()
	return kubeconfigRequestFor(t, cp.Namespace, cp.Name, key, change...)
}

func kubeconfigRequestFor(t *testing.T, namespace, controlPlane string, key crypto.Signer, change ...func(*lodiav1alpha1.KubeconfigRequestSpec)) *lodiav1alpha1.KubeconfigRequest {
	t.Helper()
	kr := &lodiav1alpha1.KubeconfigRequest{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "request-", Namespace: namespace},
		Spec: lodiav1alpha1.KubeconfigRequestSpec{
			ControlPlaneRef:   lodiav1alpha1.LocalReference{Name: controlPlane},
			User:              requesterName(t),
			ExpirationSeconds: 3600,
		},
	}
	if key != nil {
		kr.Spec.Request = certificateRequest(t, key)
	}
	for _, c := range change {
		c(&kr.Spec)
	}
	if err := k8s.Create(t.Context(), kr); err != nil {
		t.Fatal(err)
	}
	return kr
}

func freshRequest(t *testing.T, kr *lodiav1alpha1.KubeconfigRequest) *lodiav1alpha1.KubeconfigRequest {
	t.Helper()
	fresh := &lodiav1alpha1.KubeconfigRequest{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(kr), fresh); err != nil {
		t.Fatal(err)
	}
	return fresh
}

func expectReady(t *testing.T, kr *lodiav1alpha1.KubeconfigRequest, status metav1.ConditionStatus, reason string) {
	t.Helper()
	c := meta.FindStatusCondition(kr.Status.Conditions, lodiav1alpha1.ConditionReady)
	if c == nil {
		t.Fatal("no Ready condition")
	}
	if c.Status != status || c.Reason != reason {
		t.Fatalf("Ready is %s %s (%s), want %s %s", c.Status, c.Reason, c.Message, status, reason)
	}
	if c.ObservedGeneration != kr.Generation {
		t.Errorf("Ready observed generation %d, want %d", c.ObservedGeneration, kr.Generation)
	}
}

func issued(t *testing.T, kr *lodiav1alpha1.KubeconfigRequest) (*x509.Certificate, *clientcmdapi.Config) {
	t.Helper()
	expectReady(t, kr, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	kubeconfig, err := clientcmd.Load(kr.Status.Kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	user := kubeconfig.AuthInfos[kubeconfig.Contexts[kubeconfig.CurrentContext].AuthInfo]
	parsed, err := certs.ParseCerts(user.ClientCertificateData)
	if err != nil || len(parsed) != 1 {
		t.Fatalf("the kubeconfig holds %d certificates: %v", len(parsed), err)
	}
	return parsed[0], kubeconfig
}

func requesterName(t *testing.T) string {
	t.Helper()
	review := &authenticationv1.SelfSubjectReview{}
	if err := k8s.Create(t.Context(), review); err != nil {
		t.Fatal(err)
	}
	return review.Status.UserInfo.Username
}

func p256(t *testing.T) crypto.Signer {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func certificateRequest(t *testing.T, key crypto.Signer) []byte {
	t.Helper()
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "someone"}}, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})
}

func p224Request(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	subject, err := asn1.Marshal(pkix.Name{CommonName: "p224"}.ToRDNSequence())
	if err != nil {
		t.Fatal(err)
	}
	tbs, err := asn1.Marshal(struct {
		Version    int
		Subject    asn1.RawValue
		PublicKey  asn1.RawValue
		Attributes []asn1.RawValue `asn1:"tag:0"`
	}{0, asn1.RawValue{FullBytes: subject}, asn1.RawValue{FullBytes: public}, []asn1.RawValue{}})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(tbs)
	signature, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	der, err := asn1.Marshal(struct {
		TBS       asn1.RawValue
		Algorithm pkix.AlgorithmIdentifier
		Signature asn1.BitString
	}{asn1.RawValue{FullBytes: tbs}, pkix.AlgorithmIdentifier{Algorithm: asn1.ObjectIdentifier{1, 2, 840, 10045, 4, 3, 2}}, asn1.BitString{Bytes: signature, BitLength: 8 * len(signature)}})
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})
}

func ensureNamespace(t *testing.T, name string) {
	t.Helper()
	if err := k8s.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		t.Fatal(err)
	}
}

func chartPolicy(t *testing.T) (*admissionregistrationv1.ValidatingAdmissionPolicy, *admissionregistrationv1.ValidatingAdmissionPolicyBinding) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "charts", "lodia", "files", "policies", "kubeconfigrequests.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	documents := strings.Split(string(raw), "\n---\n")
	if len(documents) != 2 {
		t.Fatalf("the chart's policy file holds %d documents, want the policy and its binding", len(documents))
	}
	policy, binding := &admissionregistrationv1.ValidatingAdmissionPolicy{}, &admissionregistrationv1.ValidatingAdmissionPolicyBinding{}
	if err := yaml.UnmarshalStrict([]byte(documents[0]), policy); err != nil {
		t.Fatal(err)
	}
	if err := yaml.UnmarshalStrict([]byte(documents[1]), binding); err != nil {
		t.Fatal(err)
	}
	return policy, binding
}

func enforce(t *testing.T) {
	t.Helper()
	policy, binding := chartPolicy(t)
	for _, obj := range []client.Object{policy, binding} {
		if err := k8s.Create(t.Context(), obj); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
	}
}

func deletePolicy(t *testing.T, policy, binding bool) {
	t.Helper()
	p, b := chartPolicy(t)
	for _, obj := range []client.Object{p, b} {
		if (obj == client.Object(p) && !policy) || (obj == client.Object(b) && !binding) {
			continue
		}
		if err := client.IgnoreNotFound(k8s.Delete(t.Context(), obj)); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 10*time.Second, func() bool { return apierrors.IsNotFound(k8s.Get(t.Context(), client.ObjectKeyFromObject(obj), obj)) })
	}
	t.Cleanup(func() { restorePolicy(t) })
}

func replaceBinding(t *testing.T, change func(*admissionregistrationv1.ValidatingAdmissionPolicyBinding)) {
	t.Helper()
	deletePolicy(t, false, true)
	_, binding := chartPolicy(t)
	change(binding)
	if err := k8s.Create(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
}

func restorePolicy(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	policy, binding := chartPolicy(t)
	current := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{}
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(binding), current); err == nil && !equalBinding(current, binding) {
		if err := k8s.Delete(ctx, current); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 10*time.Second, func() bool { return apierrors.IsNotFound(k8s.Get(ctx, client.ObjectKeyFromObject(binding), current)) })
	}
	for _, obj := range []client.Object{policy, binding} {
		if err := k8s.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
	}
}

func equalBinding(a, b *admissionregistrationv1.ValidatingAdmissionPolicyBinding) bool {
	return a.Spec.PolicyName == b.Spec.PolicyName && slices.Equal(a.Spec.ValidationActions, b.Spec.ValidationActions) && a.Spec.MatchResources == nil && a.Spec.ParamRef == nil
}

func TestACertificateEndsWithItsRequestCA(t *testing.T) {
	enforce(t)
	cp := issuable(t, "kcr-clamped")
	requestCA := parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert
	kr := kubeconfigRequest(t, cp, p256(t), func(s *lodiav1alpha1.KubeconfigRequestSpec) { s.ExpirationSeconds = 7200 })
	reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Time)), kr)
	kr = freshRequest(t, kr)
	cert, _ := issued(t, kr)
	if !cert.NotAfter.Equal(requestCA.NotAfter) || !kr.Status.ExpirationTimestamp.Time.Equal(requestCA.NotAfter) {
		t.Errorf("the certificate ends at %s, status says %s, want the request CA's %s", cert.NotAfter, kr.Status.ExpirationTimestamp, requestCA.NotAfter)
	}

	late := kubeconfigRequest(t, cp, p256(t), func(s *lodiav1alpha1.KubeconfigRequestSpec) { s.ExpirationSeconds = 7200 })
	reconcileRequest(t, requestReconciler(clocktesting.NewFakePassiveClock(requestCA.NotAfter)), late)
	expectReady(t, freshRequest(t, late), metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates)
}

func TestATransientErrorAnswersNothing(t *testing.T) {
	enforce(t)
	cp := issuable(t, "kcr-transient")
	for _, read := range []string{"the policy", "the binding", "the ControlPlane", "dev-cakey-kubeconfig-requests", "dev-client-ca", "dev-ca-bundle"} {
		t.Run(read, func(t *testing.T) {
			kr := kubeconfigRequest(t, cp, p256(t))
			r := requestReconciler(clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Time))
			r.Client, r.Reader = failingClient{Client: controller, read: read}, failingGet{Reader: apiReader, read: read}
			if _, err := r.Reconcile(t.Context(), requestFor(kr)); err == nil || err.Error() != "the API server is down" {
				t.Fatalf("a pass whose read failed returned %v, want the read's error", err)
			}
			if after := freshRequest(t, kr); after.ResourceVersion != kr.ResourceVersion {
				t.Error("a pass whose read failed wrote the request")
			}
		})
	}
	kr := kubeconfigRequest(t, cp, p256(t))
	r := requestReconciler(clocktesting.NewFakePassiveClock(kr.CreationTimestamp.Time))
	r.Client = failingStatus{Client: controller}
	logs := &logLines{}
	if _, err := r.Reconcile(logs.into(t.Context()), requestFor(kr)); !apierrors.IsTooManyRequests(err) || strings.Contains(err.Error(), "credential-id") {
		t.Fatalf("a pass whose status write was refused returned %v, want the refusal, naming no credential-id", err)
	}
	if after := freshRequest(t, kr); after.ResourceVersion != kr.ResourceVersion || len(logs.lines) > 0 {
		t.Errorf("a pass whose status write failed wrote the request or logged %v", logs.lines)
	}

	for _, after := range []error{errors.New("the connection broke before the API server answered"), apierrors.NewTimeoutError("the API server did not answer in time", 0)} {
		stored := kubeconfigRequest(t, cp, p256(t))
		r = requestReconciler(clocktesting.NewFakePassiveClock(stored.CreationTimestamp.Time))
		r.Client = failingStatus{Client: controller, stored: after}
		logs = &logLines{}
		_, err := r.Reconcile(logs.into(t.Context()), requestFor(stored))
		cert, _ := issued(t, freshRequest(t, stored))
		fingerprint := sha256.Sum256(cert.Raw)
		if err == nil || !strings.Contains(err.Error(), "credential-id X509SHA256="+hex.EncodeToString(fingerprint[:])) || len(logs.lines) > 0 {
			t.Errorf("a write stored before %q returned %v and logged %v, want the error naming the stored certificate's credential-id", after, err, logs.lines)
		}
	}

	waiting := kubeconfigRequestFor(t, cp.Namespace, "absent", p256(t))
	r = requestReconciler(clocktesting.NewFakePassiveClock(waiting.CreationTimestamp.Time))
	r.Client = failingStatus{Client: controller}
	if result, err := r.Reconcile(t.Context(), requestFor(waiting)); err == nil || result != (ctrl.Result{}) {
		t.Errorf("a wait whose status write failed returned %+v and %v, want no requeue and the error", result, err)
	}
}

type failingGet struct {
	client.Reader
	read string
}

func (f failingGet) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if read(key, obj) == f.read {
		return errors.New("the API server is down")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

type failingClient struct {
	client.Client
	read string
}

func (f failingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if read(key, obj) == f.read {
		return errors.New("the API server is down")
	}
	return f.Client.Get(ctx, key, obj, opts...)
}

func read(key client.ObjectKey, obj client.Object) string {
	switch obj.(type) {
	case *admissionregistrationv1.ValidatingAdmissionPolicy:
		return "the policy"
	case *admissionregistrationv1.ValidatingAdmissionPolicyBinding:
		return "the binding"
	case *lodiav1alpha1.ControlPlane:
		return "the ControlPlane"
	case *corev1.Secret:
		return key.Name
	}
	return ""
}

type failingStatus struct {
	client.Client
	stored error
}

func (f failingStatus) Status() client.SubResourceWriter {
	return failingStatusWriter{f.Client.Status(), f.stored}
}

type failingStatusWriter struct {
	client.SubResourceWriter
	stored error
}

func (f failingStatusWriter) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	if f.stored != nil {
		if err := f.SubResourceWriter.Apply(ctx, obj, opts...); err != nil {
			return err
		}
		return f.stored
	}
	return apierrors.NewTooManyRequests("the API server refused the apply", 1)
}
