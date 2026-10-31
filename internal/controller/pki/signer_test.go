// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/keyutil"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
	"go.lodia.io/lodia/internal/operator"
)

func TestAFreshTenantIssuesItsFirstSignerInOnePass(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "signer-first")
	publishEndpoint(t, cp)
	reconcileOnce(t, r, cp)
	secrets := secretsOf(t, cp)
	if secrets["dev-kubelet-signer-next"] != nil {
		t.Error("the first signer was staged")
	}
	expectBundle(t, cp, parseTLS(t, secrets["dev-cakey-client"]).Cert, parseTLS(t, secrets["dev-kubelet-signer"]).Cert)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func TestAFirstSignerIsStoredBeforeTheBundleTrustsIt(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "signer-first-refused")
	r.Client = failingApply{Client: controller, secret: "dev-kubelet-signer"}
	reconcileFails(t, r, cp)
	if secrets := secretsOf(t, cp); secrets["dev-kubelet-signer"] != nil || secrets["dev-client-ca"] != nil {
		t.Fatal("a pass that could not store the first signer wrote it or the client CA bundle")
	}
	r.Client = failingApply{Client: controller, secret: "dev-client-ca"}
	reconcileFails(t, r, cp)
	secrets := secretsOf(t, cp)
	if secrets["dev-client-ca"] != nil {
		t.Fatal("the client CA bundle was written although its apply failed")
	}
	first := parseTLS(t, secrets["dev-kubelet-signer"]).Cert
	r.Client = controller
	reconcileOnce(t, r, cp)
	secrets = secretsOf(t, cp)
	if !parseTLS(t, secrets["dev-kubelet-signer"]).Cert.Equal(first) || secrets["dev-kubelet-signer-next"] != nil {
		t.Fatal("a stored first signer was issued again or staged")
	}
	expectBundle(t, cp, parseTLS(t, secrets["dev-cakey-client"]).Cert, first)
}

func TestASignerWriteRefusedForFivePassesLeavesTheBundleAsItWas(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "signer-refused")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	before := secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey]
	if err := k8s.Delete(t.Context(), secretOf(t, cp, "dev-kubelet-signer")); err != nil {
		t.Fatal(err)
	}
	r.Client = failingApply{Client: failingApply{Client: controller, secret: "dev-kubelet-signer-next"}, secret: "dev-kubelet-signer"}
	for range 5 {
		reconcileFails(t, r, cp)
	}
	if got := secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey]; !bytes.Equal(got, before) {
		bundle, _ := certs.ParseCerts(got)
		t.Errorf("five passes that could not store a signer left the client CA bundle holding %v", subjects(bundle))
	}
	if c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady); c.Reason != lodiav1alpha1.ReasonIssuing ||
		c.Message != "not written yet: dev-kubelet-signer-next; the error is in the log; dev-kubelet-signer holds no usable signer; its successor is written once dev-client-ca trusts it" {
		t.Errorf("CertificatesReady is %s: %s", c.Reason, c.Message)
	}
}

func TestEveryStateOfASignerReplacementSurvivesEachRefusedWrite(t *testing.T) {
	for _, state := range []struct {
		name    string
		prepare func(*testing.T, *lodiav1alpha1.ControlPlane)
		lost    bool
	}{
		{"due", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) { dueSigner(t, cp) }, false},
		{"lost", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			if err := k8s.Delete(t.Context(), secretOf(t, cp, "dev-kubelet-signer")); err != nil {
				t.Fatal(err)
			}
		}, true},
		{"unreadable", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			s := secretOf(t, cp, "dev-kubelet-signer")
			s.Data = map[string][]byte{lodiav1alpha1.TLSCertKey: []byte("garbage"), lodiav1alpha1.TLSPrivateKeyKey: []byte("garbage")}
			if err := k8s.Update(t.Context(), s); err != nil {
				t.Fatal(err)
			}
		}, true},
		{"foreign", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) { dueSigner(t, cp); foreignCAs(t, cp) }, false},
		{"foreign-pending-signer", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			nextSecret(corev1.SecretTypeTLS, false, true)(t, cp)
			foreignCAs(t, cp)
		}, false},
		{"foreign-lost", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			if err := k8s.Delete(t.Context(), secretOf(t, cp, "dev-kubelet-signer")); err != nil {
				t.Fatal(err)
			}
			foreignCAs(t, cp)
		}, true},
		{"garbage-next", nextSecret(corev1.SecretTypeTLS, false, false), false},
		{"opaque-next", nextSecret(corev1.SecretTypeOpaque, false, false), false},
		{"immutable-next", nextSecret(corev1.SecretTypeTLS, true, false), false},
		{"opaque-pending", nextSecret(corev1.SecretTypeOpaque, false, true), false},
		{"immutable-pending", nextSecret(corev1.SecretTypeTLS, true, true), false},
		{"reencoded-pending", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			dueSigner(t, cp)
			c := clientSigner(t, cp, time.Now())
			key, err := x509.MarshalECPrivateKey(c.Key)
			if err != nil {
				t.Fatal(err)
			}
			createNext(t, cp, append([]byte("subject=CN=kubelet-signer@2\n"), mustPEM(t, c.Cert)...), pem.EncodeToMemory(&pem.Block{Type: keyutil.ECPrivateKeyBlockType, Bytes: key}), true)
		}, false},
		{"foreign-pending", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			dueSigner(t, cp)
			c := foreignSigner(t)
			createNext(t, cp, mustPEM(t, c.Cert), mustKeyPEM(t, c.Key), false)
		}, false},
		{"expired-pending", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			dueSigner(t, cp)
			c := clientSigner(t, cp, time.Now().Add(-2*time.Hour))
			createNext(t, cp, mustPEM(t, c.Cert), mustKeyPEM(t, c.Key), false)
		}, false},
		{"foreign-signer", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) { storeSigner(t, cp, foreignSigner(t)) }, true},
		{"expired-signer", func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
			storeSigner(t, cp, clientSigner(t, cp, time.Now().Add(-2*time.Hour)))
		}, true},
	} {
		t.Run(state.name, func(t *testing.T) {
			t.Parallel()
			writes := replaceSigner(t, "signer-"+state.name, state.prepare, state.lost, 0)
			for nth := 1; nth <= writes; nth++ {
				t.Run(strconv.Itoa(nth), func(t *testing.T) {
					t.Parallel()
					replaceSigner(t, "signer-"+state.name+"-"+strconv.Itoa(nth), state.prepare, state.lost, nth)
				})
			}
		})
	}
}

func foreignCAs(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	secrets := secretsOf(t, cp)
	bundle, err := certs.ParseCerts(secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey])
	if err != nil {
		t.Fatal(err)
	}
	root, err := certs.NewCA("kubelet-signer@3", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	intermediate, err := root.Issue(certs.Profile{CommonName: "kubelet-signer@4", CA: true}, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	expired, err := parseTLS(t, secrets["dev-cakey-client"]).Issue(certs.Profile{CommonName: "kubelet-signer@5", CA: true}, time.Now().Add(-2*time.Hour), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	lapsed, err := parseTLS(t, secrets["dev-cakey-client"]).Issue(certs.Profile{CommonName: "kubelet-signer@6", CA: true}, time.Now().Add(-2*time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	s := secretOf(t, cp, "dev-client-ca")
	s.Data = map[string][]byte{lodiav1alpha1.CACertKey: mustBundle(t, append(bundle, root.Cert, intermediate.Cert, expired.Cert, lapsed.Cert, leafOf(t, secrets["dev-lodia-kubeconfig"]).Cert)...)}
	if err := k8s.Update(t.Context(), s); err != nil {
		t.Fatal(err)
	}
}

func nextSecret(typ corev1.SecretType, immutable, usable bool) func(*testing.T, *lodiav1alpha1.ControlPlane) {
	return func(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
		dueSigner(t, cp)
		content := map[string][]byte{lodiav1alpha1.TLSCertKey: []byte("garbage"), lodiav1alpha1.TLSPrivateKeyKey: []byte("garbage")}
		if usable {
			c := clientSigner(t, cp, time.Now())
			content = map[string][]byte{lodiav1alpha1.TLSCertKey: mustPEM(t, c.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, c.Key)}
		}
		s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "dev-kubelet-signer-next", Namespace: cp.Namespace}, Type: typ, Data: content, Immutable: &immutable}
		if err := k8s.Create(t.Context(), s); err != nil {
			t.Fatal(err)
		}
	}
}

func clientSigner(t *testing.T, cp *lodiav1alpha1.ControlPlane, at time.Time) *certs.Certificate {
	t.Helper()
	c, err := parseTLS(t, secretsOf(t, cp)["dev-cakey-client"]).Issue(certs.Profile{CommonName: "kubelet-signer@2", CA: true}, at, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func foreignSigner(t *testing.T) *certs.Certificate {
	t.Helper()
	root, err := certs.NewCA("client-ca@1", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	c, err := root.Issue(certs.Profile{CommonName: "kubelet-signer@2", CA: true}, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func createNext(t *testing.T, cp *lodiav1alpha1.ControlPlane, cert, key []byte, immutable bool) {
	t.Helper()
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "dev-kubelet-signer-next", Namespace: cp.Namespace}, Type: corev1.SecretTypeTLS, Data: map[string][]byte{lodiav1alpha1.TLSCertKey: cert, lodiav1alpha1.TLSPrivateKeyKey: key}, Immutable: &immutable}
	if err := k8s.Create(t.Context(), s); err != nil {
		t.Fatal(err)
	}
}

func storeSigner(t *testing.T, cp *lodiav1alpha1.ControlPlane, c *certs.Certificate) {
	t.Helper()
	s := secretOf(t, cp, "dev-kubelet-signer")
	s.Data = map[string][]byte{lodiav1alpha1.TLSCertKey: mustPEM(t, c.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, c.Key)}
	if err := k8s.Update(t.Context(), s); err != nil {
		t.Fatal(err)
	}
}

func replaceSigner(t *testing.T, namespace string, prepare func(*testing.T, *lodiav1alpha1.ControlPlane), lost bool, nth int) int {
	t.Helper()
	const propagation = 2 * time.Second
	r := newReconciler(time.Hour)
	r.SignerPropagation = propagation
	cp := controlPlane(t, namespace)
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	prepare(t, cp)
	clientCA := parseTLS(t, secretsOf(t, cp)["dev-cakey-client"]).Cert
	start := readSigner(t, cp)
	isSigner := func(c *x509.Certificate) bool {
		return c.IsCA && c.MaxPathLenZero && certs.Verify(c, clientCA, time.Now()) == nil
	}
	var initial []*x509.Certificate
	for _, c := range start.bundle {
		if isSigner(c) {
			initial = append(initial, c)
		}
	}
	var stored []*x509.Certificate
	writes := 0
	r.Client = refusingNth{Client: controller, writes: &writes, nth: nth}
	for pass := 1; ; pass++ {
		if pass > 20 {
			t.Fatal("the replacement did not complete in 20 passes")
		}
		before := readSigner(t, cp)
		stored = append(stored, before.signer...)
		stored = append(stored, before.next...)
		previous := writes
		result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		end := time.Now()
		if err != nil && (nth <= previous || nth > writes) {
			t.Errorf("pass %d failed though it refused no write: %v", pass, err)
		}
		after := readSigner(t, cp)
		for _, c := range after.bundle {
			if !containsCert(before.bundle, c) && c.IsCA && c.MaxPathLenZero && !containsCert(stored, c) {
				t.Errorf("pass %d added %s, whose key was not stored when it began", pass, c.Subject.CommonName)
			}
		}
		if !bytes.Equal(mustBundle(t, after.bundle...), mustBundle(t, before.bundle...)) {
			for _, c := range after.bundle {
				if !c.Equal(clientCA) && !strings.HasPrefix(c.Subject.CommonName, "kubeconfig-requests-ca@") && !isSigner(c) {
					t.Errorf("after pass %d the bundle holds %s, no valid signer the client CA signed", pass, c.Subject.CommonName)
				}
			}
		}
		var gained []string
		for _, c := range after.bundle {
			if c.IsCA && c.MaxPathLenZero && !containsCert(start.bundle, c) {
				gained = append(gained, c.Subject.CommonName)
			}
		}
		if len(gained) > 1 {
			t.Errorf("after pass %d the bundle gained %v in one replacement", pass, gained)
		}
		for _, c := range initial {
			if !containsCert(after.bundle, c) {
				t.Errorf("pass %d dropped %s before it expired", pass, c.Subject.CommonName)
			}
		}
		for _, c := range before.pending {
			if isSigner(c) && !containsCert(after.next, c) && !containsCert(after.signer, c) {
				t.Errorf("pass %d lost the pending signer %s", pass, c.Subject.CommonName)
			}
		}
		if len(after.signer) == 1 && (len(before.signer) != 1 || !after.signer[0].Equal(before.signer[0])) {
			if !containsCert(before.bundle, after.signer[0]) || end.Before(before.applied.Add(propagation)) {
				t.Errorf("pass %d wrote %s to dev-kubelet-signer before the bundle had held it unchanged for %s", pass, after.signer[0].Subject.CommonName, propagation)
			}
		}
		promoted := len(after.signer) == 1 && (len(start.signer) == 0 || !after.signer[0].Equal(start.signer[0]))
		if err == nil {
			c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
			if lost && !promoted && (c.Reason != lodiav1alpha1.ReasonIssuing || !strings.HasPrefix(c.Message, "dev-kubelet-signer holds no usable signer; its successor is written ")) ||
				(!lost || promoted) && c.Reason != lodiav1alpha1.ReasonIssued {
				t.Errorf("after pass %d CertificatesReady is %s: %s", pass, c.Reason, c.Message)
			}
		}
		if err == nil && promoted && !after.staged && containsCert(after.bundle, after.signer[0]) {
			versions, _ := resourceVersions(t, cp)
			reconcileOnce(t, r, cp)
			if again, _ := resourceVersions(t, cp); !maps.Equal(versions, again) {
				t.Error("a pass after the replacement wrote a Secret")
			}
			want := append([]*x509.Certificate{clientCA, after.signer[0], parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert}, initial...)
			expectBundle(t, cp, want...)
			return writes
		}
		if err == nil && after.unchanged(before) {
			time.Sleep(min(result.RequeueAfter, 2*propagation))
		}
	}
}

type signerState struct {
	signer, next, pending, bundle []*x509.Certificate
	staged                        bool
	applied                       time.Time
	versions                      string
}

func readSigner(t *testing.T, cp *lodiav1alpha1.ControlPlane) signerState {
	t.Helper()
	var state signerState
	for _, name := range []string{"dev-kubelet-signer", "dev-kubelet-signer-next", "dev-client-ca"} {
		s := &corev1.Secret{}
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: name}, s); err != nil {
			if client.IgnoreNotFound(err) != nil {
				t.Fatal(err)
			}
			continue
		}
		state.versions += name + "=" + s.ResourceVersion + " "
		switch name {
		case "dev-kubelet-signer":
			if c, err := certs.Parse(s.Data[lodiav1alpha1.TLSCertKey], s.Data[lodiav1alpha1.TLSPrivateKeyKey]); err == nil {
				state.signer = []*x509.Certificate{c.Cert}
			}
		case "dev-kubelet-signer-next":
			state.staged = true
			if c, err := certs.Parse(s.Data[lodiav1alpha1.TLSCertKey], s.Data[lodiav1alpha1.TLSPrivateKeyKey]); err == nil {
				state.next = []*x509.Certificate{c.Cert}
				if s.Type == corev1.SecretTypeTLS {
					state.pending = state.next
				}
			}
		default:
			bundle, err := certs.ParseCerts(s.Data[lodiav1alpha1.CACertKey])
			if err != nil {
				t.Fatal(err)
			}
			state.bundle = bundle
			state.applied = appliedAt(t, s)
		}
	}
	return state
}

func (s signerState) unchanged(before signerState) bool {
	return s.versions == before.versions
}

func appliedAt(t *testing.T, s *corev1.Secret) time.Time {
	t.Helper()
	for _, f := range s.ManagedFields {
		if f.Manager == "lodia/pki" && f.Operation == metav1.ManagedFieldsOperationApply {
			return f.Time.Time
		}
	}
	t.Fatalf("%s has no entry of lodia/pki", s.Name)
	return time.Time{}
}

func dueSigner(t *testing.T, cp *lodiav1alpha1.ControlPlane) *x509.Certificate {
	t.Helper()
	return signerIssued(t, cp, time.Now().Add(-50*time.Minute))
}

func signerIssued(t *testing.T, cp *lodiav1alpha1.ControlPlane, at time.Time) *x509.Certificate {
	t.Helper()
	secrets := secretsOf(t, cp)
	clientCA := parseTLS(t, secrets["dev-cakey-client"])
	due, err := clientCA.Issue(certs.Profile{CommonName: "kubelet-signer@1", CA: true}, at, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := certs.ParseCerts(secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey])
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]map[string][]byte{
		"dev-kubelet-signer": {lodiav1alpha1.TLSCertKey: mustPEM(t, due.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, due.Key)},
		"dev-client-ca":      {lodiav1alpha1.CACertKey: mustBundle(t, append(bundle, due.Cert)...)},
	} {
		s := secretOf(t, cp, name)
		s.Data = content
		if err := k8s.Update(t.Context(), s); err != nil {
			t.Fatal(err)
		}
	}
	return due.Cert
}

type refusingNth struct {
	client.Client
	writes *int
	nth    int
}

func (c refusingNth) refused() bool {
	*c.writes++
	return *c.writes == c.nth
}

func (c refusingNth) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	if _, ok := obj.(*corev1ac.SecretApplyConfiguration); ok && c.refused() {
		return errors.New("the API server refused the apply")
	}
	return c.Client.Apply(ctx, obj, opts...)
}

func (c refusingNth) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if _, ok := obj.(*corev1.Secret); ok && c.refused() {
		return errors.New("the API server refused the delete")
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func TestASignerIsPromotedOnceTheBundleHasHeldItUnchangedSincePKIsLastWrite(t *testing.T) {
	r := newReconciler(time.Hour)
	r.SignerPropagation = 5 * time.Minute
	cp := controlPlane(t, "signer-propagation")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)
	original := parseTLS(t, secrets["dev-kubelet-signer"]).Cert
	clientCA := parseTLS(t, secrets["dev-cakey-client"]).Cert
	if err := k8s.Delete(t.Context(), secretOf(t, cp, "dev-kubelet-signer")); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r, cp)
	next := parseTLS(t, secretsOf(t, cp)["dev-kubelet-signer-next"]).Cert
	if result := reconcileOnce(t, r, cp); result.RequeueAfter > 5*time.Minute {
		t.Errorf("the pass that trusts the staged signer requeues after %s, want the propagation", result.RequeueAfter)
	}
	requests := parseTLS(t, secretsOf(t, cp)["dev-cakey-kubeconfig-requests"]).Cert
	expectBundle(t, cp, clientCA, next, original, requests)
	waits := func(want time.Duration) {
		t.Helper()
		at := appliedAt(t, secretOf(t, cp, "dev-client-ca")).Add(time.Second + 5*time.Minute)
		until := time.Until(at)
		result := reconcileOnce(t, r, cp)
		if secretsOf(t, cp)["dev-kubelet-signer"] != nil {
			t.Fatal("the staged signer was written early")
		}
		if until < want-2*time.Second || until > want+2*time.Second || result.RequeueAfter < time.Until(at) || result.RequeueAfter >= until+time.Second {
			t.Errorf("requeue after %s, want the promotion in %s, at %s", result.RequeueAfter, want, at)
		}
		if c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady); c.Reason != lodiav1alpha1.ReasonIssuing ||
			c.Message != "dev-kubelet-signer holds no usable signer; its successor is written at "+at.UTC().Format(time.RFC3339) {
			t.Errorf("CertificatesReady is %s: %s, want the promotion at %s", c.Reason, c.Message, at)
		}
	}
	waits(5 * time.Minute)

	backdate(t, cp, 4*time.Minute)
	waits(time.Minute)
	labelled := secretOf(t, cp, "dev-client-ca")
	labelled.Labels["example.com/team"] = "a"
	if err := k8s.Update(t.Context(), labelled); err != nil {
		t.Fatal(err)
	}
	waits(time.Minute)

	if err := k8s.Delete(t.Context(), secretOf(t, cp, "dev-cakey-kubeconfig-requests")); err != nil {
		t.Fatal(err)
	}
	waits(time.Minute)
	waits(time.Minute)
	waits(5 * time.Minute)

	backdate(t, cp, 6*time.Minute)
	reconcileOnce(t, r, cp)
	secrets = secretsOf(t, cp)
	if s := secrets["dev-kubelet-signer"]; s == nil || !parseTLS(t, s).Cert.Equal(next) || secrets["dev-kubelet-signer-next"] != nil {
		t.Fatal("the staged signer was not promoted once the bundle had held it unchanged for the propagation")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func TestAStagedSignerIsPromotedNoEarlierThanThePropagationAfterItsBundleWrite(t *testing.T) {
	const propagation = 2 * time.Second
	r := newReconciler(time.Hour)
	r.SignerPropagation = propagation
	cp := controlPlane(t, "signer-promotion-second")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	dueSigner(t, cp)
	reconcileOnce(t, r, cp)
	next := parseTLS(t, secretsOf(t, cp)["dev-kubelet-signer-next"]).Cert
	var written time.Time
	r.Client = lateInASecond{Client: controller, secret: "dev-client-ca", written: &written}
	reconcileOnce(t, r, cp)
	r.Client = controller
	if written.IsZero() {
		t.Fatal("the pass after the staging did not write the client CA bundle")
	}
	waitFor(t, 10*time.Second, func() bool {
		reconcileOnce(t, r, cp)
		end := time.Now()
		promoted := parseTLS(t, secretsOf(t, cp)["dev-kubelet-signer"]).Cert.Equal(next)
		if promoted && end.Before(written.Add(propagation)) {
			t.Fatalf("the staged signer was promoted %s after its bundle's write began, before the propagation of %s", end.Sub(written), propagation)
		}
		return promoted
	})
}

type lateInASecond struct {
	client.Client
	secret  string
	written *time.Time
}

func (c lateInASecond) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	if s, ok := obj.(*corev1ac.SecretApplyConfiguration); ok && *s.GetName() == c.secret {
		time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(1600 * time.Millisecond)))
		*c.written = time.Now()
	}
	return c.Client.Apply(ctx, obj, opts...)
}

func TestABundleWithoutPKIsEntryPromotesTheStagedSignerAtOnce(t *testing.T) {
	r := newReconciler(time.Hour)
	r.SignerPropagation = time.Hour
	cp := controlPlane(t, "signer-entry-cleared")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	if err := k8s.Delete(t.Context(), secretOf(t, cp, "dev-kubelet-signer")); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r, cp)
	next := parseTLS(t, secretsOf(t, cp)["dev-kubelet-signer-next"]).Cert
	reconcileOnce(t, r, cp)
	bundle := secretOf(t, cp, "dev-client-ca")
	bundle.ManagedFields = []metav1.ManagedFieldsEntry{{}}
	if err := k8s.Update(t.Context(), bundle); err != nil {
		t.Fatal(err)
	}
	if fields := secretOf(t, cp, "dev-client-ca").ManagedFields; len(fields) > 0 {
		t.Fatalf("the bundle's managed fields were not cleared: %v", fields)
	}
	reconcileOnce(t, r, cp)
	if s := secretsOf(t, cp)["dev-kubelet-signer"]; s == nil || !parseTLS(t, s).Cert.Equal(next) {
		t.Error("a bundle without an entry of lodia/pki did not promote the staged signer at once")
	}
}

func TestAStagedSignerOutlivesAPassWithoutTheClientCA(t *testing.T) {
	r := newReconciler(time.Hour)
	r.SignerPropagation = time.Hour
	cp := controlPlane(t, "signer-next-client-ca-lost")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	dueSigner(t, cp)
	reconcileOnce(t, r, cp)
	reconcileOnce(t, r, cp)
	next := parseTLS(t, secretsOf(t, cp)["dev-kubelet-signer-next"]).Cert
	key := secretOf(t, cp, "dev-cakey-client")
	saved := key.Data
	key.Data = map[string][]byte{lodiav1alpha1.TLSCertKey: []byte("garbage"), lodiav1alpha1.TLSPrivateKeyKey: []byte("garbage")}
	if err := k8s.Update(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonCAMissing)
	if s := secretsOf(t, cp)["dev-kubelet-signer-next"]; s == nil || !parseTLS(t, s).Cert.Equal(next) {
		t.Fatal("a pass that could not read the client CA deleted or replaced the staged signer")
	}
	key = secretOf(t, cp, "dev-cakey-client")
	key.Data = saved
	if err := k8s.Update(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r, cp)
	if s := secretsOf(t, cp)["dev-kubelet-signer-next"]; s == nil || !parseTLS(t, s).Cert.Equal(next) {
		t.Fatal("the staged signer was not kept once the client CA came back")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func TestASignerWakesThePassAtItsRenewal(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "signer-wake")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	soon := signerIssued(t, cp, time.Now().Add(-38*time.Minute))
	renewal := certs.RenewalTime(soon)
	before := time.Now()
	result := reconcileOnce(t, r, cp)
	if after := time.Now(); result.RequeueAfter < renewal.Sub(after) || result.RequeueAfter > renewal.Sub(before)+time.Second {
		t.Errorf("requeue after %s, want the signer's renewal at %s", result.RequeueAfter, renewal)
	}
}

func backdate(t *testing.T, cp *lodiav1alpha1.ControlPlane, by time.Duration) {
	t.Helper()
	s := secretOf(t, cp, "dev-client-ca")
	want := appliedAt(t, s).Add(-by)
	for i := range s.ManagedFields {
		if f := &s.ManagedFields[i]; f.Manager == "lodia/pki" && f.Operation == metav1.ManagedFieldsOperationApply {
			f.Time = &metav1.Time{Time: want}
		}
	}
	s.Annotations["example.com/backdated"] = by.String()
	if err := k8s.Update(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if got := appliedAt(t, secretOf(t, cp, "dev-client-ca")); !got.Equal(want) {
		t.Fatalf("the entry of lodia/pki is at %s, want %s", got, want)
	}
}

func TestTheBundleKeepsTheTenNewestReplacedSigners(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "signer-cap")
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)
	clientCA := parseTLS(t, secrets["dev-cakey-client"])
	signer := parseTLS(t, secrets["dev-kubelet-signer"]).Cert
	requests := parseTLS(t, secrets["dev-cakey-kubeconfig-requests"]).Cert
	replaced := replacedSigners(t, clientCA)
	crowd := func() {
		t.Helper()
		s := secretOf(t, cp, "dev-client-ca")
		s.Data = map[string][]byte{lodiav1alpha1.CACertKey: mustBundle(t, append([]*x509.Certificate{clientCA.Cert, signer, requests}, replaced...)...)}
		if err := k8s.Update(t.Context(), s); err != nil {
			t.Fatal(err)
		}
	}
	crowd()
	reconcileOnce(t, r, cp)
	expectBundle(t, cp, append([]*x509.Certificate{clientCA.Cert, signer, requests}, replaced[:10]...)...)

	crowd()
	if err := k8s.Delete(t.Context(), secretOf(t, cp, "dev-kubelet-signer")); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r, cp)
	expectBundle(t, cp, append([]*x509.Certificate{clientCA.Cert, signer, requests}, replaced[:10]...)...)
	next := parseTLS(t, secretsOf(t, cp)["dev-kubelet-signer-next"]).Cert
	reconcileOnce(t, r, cp)
	expectBundle(t, cp, append([]*x509.Certificate{clientCA.Cert, next, signer, requests}, replaced[:10]...)...)
	backdate(t, cp, time.Second)
	reconcileOnce(t, r, cp)
	if !parseTLS(t, secretsOf(t, cp)["dev-kubelet-signer"]).Cert.Equal(next) {
		t.Fatal("the staged signer was not promoted")
	}
	expectBundle(t, cp, append([]*x509.Certificate{clientCA.Cert, next, signer, requests}, replaced[:10]...)...)
	reconcileOnce(t, r, cp)
	expectBundle(t, cp, append([]*x509.Certificate{clientCA.Cert, next, signer, requests}, replaced[:9]...)...)
}

func TestTheBundleKeepsTheTenNewestReplacedSignersWhileADueSignersSuccessorIsStaged(t *testing.T) {
	r := newReconciler(time.Hour)
	r.SignerPropagation = time.Hour
	cp := controlPlane(t, "signer-cap-due")
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)
	clientCA := parseTLS(t, secrets["dev-cakey-client"])
	requests := parseTLS(t, secrets["dev-cakey-kubeconfig-requests"]).Cert
	due := dueSigner(t, cp)
	replaced := replacedSigners(t, clientCA)
	crowd := func() {
		t.Helper()
		s := secretOf(t, cp, "dev-client-ca")
		s.Data = map[string][]byte{lodiav1alpha1.CACertKey: mustBundle(t, append([]*x509.Certificate{clientCA.Cert, due, requests}, replaced...)...)}
		if err := k8s.Update(t.Context(), s); err != nil {
			t.Fatal(err)
		}
	}
	crowd()
	reconcileOnce(t, r, cp)
	expectBundle(t, cp, append([]*x509.Certificate{clientCA.Cert, due, requests}, replaced[:10]...)...)
	next := parseTLS(t, secretsOf(t, cp)["dev-kubelet-signer-next"]).Cert
	crowd()
	reconcileOnce(t, r, cp)
	expectBundle(t, cp, append([]*x509.Certificate{clientCA.Cert, due, next, requests}, replaced[:10]...)...)
}

func replacedSigners(t *testing.T, clientCA *certs.Certificate) []*x509.Certificate {
	t.Helper()
	var replaced []*x509.Certificate
	for i := range 12 {
		age := time.Duration(i+1) * time.Minute
		c, err := clientCA.Issue(certs.Profile{CommonName: "kubelet-signer@" + strconv.Itoa(12-i), CA: true}, time.Now().Add(-age), time.Hour+2*age)
		if err != nil {
			t.Fatal(err)
		}
		replaced = append(replaced, c.Cert)
	}
	return replaced
}

func TestABundleLostWhileASignerIsStagedTrustsTheStagedOne(t *testing.T) {
	r := newReconciler(time.Hour)
	r.SignerPropagation = time.Hour
	cp := controlPlane(t, "signer-bundle-lost")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	if err := k8s.Delete(t.Context(), secretOf(t, cp, "dev-kubelet-signer")); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r, cp)
	secrets := secretsOf(t, cp)
	next := parseTLS(t, secrets["dev-kubelet-signer-next"]).Cert
	if err := k8s.Delete(t.Context(), secretOf(t, cp, "dev-client-ca")); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r, cp)
	reconcileOnce(t, r, cp)
	secrets = secretsOf(t, cp)
	if s := secrets["dev-kubelet-signer-next"]; secrets["dev-kubelet-signer"] != nil || s == nil || !parseTLS(t, s).Cert.Equal(next) {
		t.Fatal("a lost bundle promoted the staged signer at once, or replaced it")
	}
	expectBundle(t, cp, parseTLS(t, secrets["dev-cakey-client"]).Cert, next, parseTLS(t, secrets["dev-cakey-kubeconfig-requests"]).Cert)
	at := appliedAt(t, secretOf(t, cp, "dev-client-ca")).Add(time.Second + time.Hour)
	if c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady); c.Reason != lodiav1alpha1.ReasonIssuing ||
		c.Message != "dev-kubelet-signer holds no usable signer; its successor is written at "+at.UTC().Format(time.RFC3339) {
		t.Errorf("CertificatesReady is %s: %s, want the promotion at %s", c.Reason, c.Message, at)
	}
	backdate(t, cp, time.Hour+time.Second)
	reconcileOnce(t, r, cp)
	secrets = secretsOf(t, cp)
	if s := secrets["dev-kubelet-signer"]; s == nil || !parseTLS(t, s).Cert.Equal(next) || secrets["dev-kubelet-signer-next"] != nil {
		t.Fatal("the staged signer was not promoted once the new bundle had held it for the propagation")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func TestAStagedSignerWithoutLabelsOrOwnerIsAdopted(t *testing.T) {
	r := newReconciler(time.Hour)
	r.SignerPropagation = time.Hour
	cp := controlPlane(t, "signer-next-adopted")
	reconcile(t, r, cp)
	dueSigner(t, cp)
	reconcileOnce(t, r, cp)
	s := secretOf(t, cp, "dev-kubelet-signer-next")
	next := parseTLS(t, s).Cert
	s.Labels, s.OwnerReferences = nil, nil
	if err := k8s.Update(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	adopted := secretsOf(t, cp)["dev-kubelet-signer-next"]
	if adopted == nil || !parseTLS(t, adopted).Cert.Equal(next) || !metav1.IsControlledBy(adopted, cp) {
		t.Fatal("a staged signer without labels or owner was not adopted as it is")
	}
	bundle, err := certs.ParseCerts(secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey])
	if err != nil {
		t.Fatal(err)
	}
	if !containsCert(bundle, next) {
		t.Error("the adopted staged signer is not in the client CA bundle")
	}
}

func TestATrustedPendingSignerMadeImmutableKeepsItsKey(t *testing.T) {
	r := newReconciler(time.Hour)
	r.SignerPropagation = time.Hour
	cp := controlPlane(t, "signer-next-immutable")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	dueSigner(t, cp)
	reconcileOnce(t, r, cp)
	reconcileOnce(t, r, cp)
	s := secretOf(t, cp, "dev-kubelet-signer-next")
	next := parseTLS(t, s).Cert
	s.Immutable = new(true)
	if err := k8s.Update(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	before := secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey]
	for range 3 {
		reconcileOnce(t, r, cp)
	}
	secrets := secretsOf(t, cp)
	if n := secrets["dev-kubelet-signer-next"]; n == nil || n.UID != s.UID {
		t.Fatal("a trusted pending signer made immutable was deleted")
	}
	if got := secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey]; !bytes.Equal(got, before) {
		bundle, _ := certs.ParseCerts(got)
		t.Errorf("the client CA bundle changed to %v while its pending signer was immutable", subjects(bundle))
	}
	backdate(t, cp, time.Hour+time.Second)
	reconcileOnce(t, r, cp)
	secrets = secretsOf(t, cp)
	if s := secrets["dev-kubelet-signer"]; !parseTLS(t, s).Cert.Equal(next) || secrets["dev-kubelet-signer-next"] != nil {
		t.Fatal("the immutable pending signer was not promoted")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
}

func TestADueSignerRestoredWithoutLabelsIsAdoptedWhileItsSuccessorIsStaged(t *testing.T) {
	r := newReconciler(time.Hour)
	r.SignerPropagation = time.Hour
	cp := controlPlane(t, "signer-due-restored")
	reconcile(t, r, cp)
	dueSigner(t, cp)
	reconcileOnce(t, r, cp)
	next := parseTLS(t, secretsOf(t, cp)["dev-kubelet-signer-next"]).Cert
	s := secretOf(t, cp, "dev-kubelet-signer")
	if err := k8s.Delete(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: s.Name, Namespace: s.Namespace}, Type: s.Type, Data: s.Data}); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r, cp)
	secrets := secretsOf(t, cp)
	if adopted := secrets["dev-kubelet-signer"]; adopted == nil || !metav1.IsControlledBy(adopted, cp) || !maps.EqualFunc(adopted.Data, s.Data, bytes.Equal) {
		t.Fatal("a due signer restored without labels or owner while its successor is staged was not adopted with its keys")
	}
	if n := secrets["dev-kubelet-signer-next"]; n == nil || !parseTLS(t, n).Cert.Equal(next) {
		t.Error("the staged successor was lost or replaced")
	}
}

func TestAPendingSignerSecretAnApplyCannotOverwriteOutlivesAnEarlierRefusedWrite(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "signer-next-early-refusal")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	nextSecret(corev1.SecretTypeOpaque, false, false)(t, cp)
	if err := k8s.Delete(t.Context(), secretOf(t, cp, "dev-front-proxy-client")); err != nil {
		t.Fatal(err)
	}
	before := namespaceSecrets(t, cp.Namespace)["dev-kubelet-signer-next"]
	r.Client = failingApply{Client: controller, secret: "dev-front-proxy-client"}
	reconcileFails(t, r, cp)
	if after, kept := namespaceSecrets(t, cp.Namespace)["dev-kubelet-signer-next"]; !kept || after != before {
		t.Fatal("a pass whose earlier write failed deleted or wrote the dev-kubelet-signer-next it cannot overwrite")
	}
	r.Client = controller
	reconcileOnce(t, r, cp)
	if s := secretsOf(t, cp)["dev-kubelet-signer-next"]; s == nil || s.Type != corev1.SecretTypeTLS {
		t.Error("the next pass did not stage a signer in place of the dev-kubelet-signer-next it cannot overwrite")
	}
}

func TestAPendingSignerNoReplacementNeedsIsDeleted(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "signer-next-leftover")
	reconcile(t, r, cp)
	leftover := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "dev-kubelet-signer-next", Namespace: cp.Namespace},
		Type:       corev1.SecretTypeTLS,
		Data:       map[string][]byte{lodiav1alpha1.TLSCertKey: []byte("garbage"), lodiav1alpha1.TLSPrivateKeyKey: []byte("garbage")},
	}
	if err := k8s.Create(t.Context(), leftover); err != nil {
		t.Fatal(err)
	}
	before := secretOf(t, cp, "dev-client-ca")
	reconcileOnce(t, r, cp)
	if _, kept := namespaceSecrets(t, cp.Namespace)["dev-kubelet-signer-next"]; kept {
		t.Error("a pending signer Secret no replacement needs was kept")
	}
	if secretOf(t, cp, "dev-client-ca").ResourceVersion != before.ResourceVersion {
		t.Error("a leftover pending signer changed the client CA bundle")
	}
}

func TestAPendingSignerIsDeletedOnceTheSignerIsUsableAgain(t *testing.T) {
	for _, trusted := range []bool{false, true} {
		t.Run("trusted="+strconv.FormatBool(trusted), func(t *testing.T) {
			r := newReconciler(time.Hour)
			r.SignerPropagation = time.Hour
			cp := controlPlane(t, "signer-restored-"+strconv.FormatBool(trusted))
			publishEndpoint(t, cp)
			reconcile(t, r, cp)
			original := secretOf(t, cp, "dev-kubelet-signer")
			if err := k8s.Delete(t.Context(), original); err != nil {
				t.Fatal(err)
			}
			reconcileOnce(t, r, cp)
			if trusted {
				reconcileOnce(t, r, cp)
			}
			next := parseTLS(t, secretsOf(t, cp)["dev-kubelet-signer-next"]).Cert
			restored := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: original.Name, Namespace: original.Namespace, Labels: original.Labels, OwnerReferences: original.OwnerReferences}, Type: original.Type, Data: original.Data}
			if err := k8s.Create(t.Context(), restored); err != nil {
				t.Fatal(err)
			}
			reconcile(t, r, cp)
			secrets := secretsOf(t, cp)
			if _, kept := namespaceSecrets(t, cp.Namespace)["dev-kubelet-signer-next"]; kept {
				t.Error("a pending signer no replacement needs was kept")
			}
			want := []*x509.Certificate{parseTLS(t, secrets["dev-cakey-client"]).Cert, parseTLS(t, original).Cert, parseTLS(t, secrets["dev-cakey-kubeconfig-requests"]).Cert}
			if trusted {
				want = append(want, next)
			}
			expectBundle(t, cp, want...)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
		})
	}
}

func TestAPendingSignerAnotherOwnerKeepsIsLeftAlone(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "signer-next-held")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	dueSigner(t, cp)
	held := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "dev-kubelet-signer-next", Namespace: cp.Namespace, OwnerReferences: []metav1.OwnerReference{
			{APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "dev", UID: "8f0c6f2e-0d6e-4f4e-9e44-0d6a7c1b5a21", Controller: new(true)},
		}},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"x": []byte("y")},
	}
	if err := k8s.Create(t.Context(), held); err != nil {
		t.Fatal(err)
	}
	before := namespaceSecrets(t, cp.Namespace)
	reconcile(t, r, cp)
	if c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady); c.Reason != lodiav1alpha1.ReasonSecretsControlledElsewhere || !strings.Contains(c.Message, "dev-kubelet-signer-next by ControlPlane dev") {
		t.Errorf("CertificatesReady is %s: %s", c.Reason, c.Message)
	}
	if after := namespaceSecrets(t, cp.Namespace); !maps.Equal(before, after) {
		t.Error("a Secret was written or deleted while another owner keeps the pending signer's")
	}
}

func TestASecretQuotaHoldsTheStagedSignerUntilItGoes(t *testing.T) {
	ctx := t.Context()
	cp := controlPlane(t, "signer-quota")
	publishEndpoint(t, cp)
	settle := newReconciler(time.Hour)
	reconcile(t, settle, cp)
	due := dueSigner(t, cp)
	before := secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey]
	quota := fillSecretQuota(t, cp)

	options, err := operator.Cache()
	if err != nil {
		t.Fatal(err)
	}
	options.DefaultNamespaces = map[string]cache.Config{cp.Namespace: {}}
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
	r := newReconciler(time.Hour)
	r.SignerPropagation = 2 * time.Second
	r.Client, r.Reader = mgr.GetClient(), mgr.GetAPIReader()
	if err := r.SetupWithManager(mgr, 1); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(runCtx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})

	const moved = "203.0.113.11"
	publishEndpointAt(t, cp, moved)
	waitFor(t, 30*time.Second, func() bool {
		c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
		secrets := secretsOf(t, cp)
		ips := parseTLS(t, secrets["dev-apiserver-serving"]).Cert.IPAddresses
		admin, err := clientcmd.Load(secretOf(t, cp, "dev-admin-kubeconfig").Data[lodiav1alpha1.KubeconfigKey])
		if err != nil {
			t.Fatal(err)
		}
		return c != nil && c.Reason == lodiav1alpha1.ReasonIssuing && c.Message == "not written yet: dev-kubelet-signer-next; the error is in the log" &&
			slices.ContainsFunc(ips, func(ip net.IP) bool { return ip.String() == moved }) && admin.Clusters[cp.Name].Server == "https://"+moved+":443"
	})
	time.Sleep(2 * time.Second)
	secrets := secretsOf(t, cp)
	if !bytes.Equal(secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey], before) || !parseTLS(t, secrets["dev-kubelet-signer"]).Cert.Equal(due) {
		bundle, _ := certs.ParseCerts(secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey])
		t.Fatalf("under the quota the client CA bundle changed to %v or the signer was replaced", subjects(bundle))
	}

	if err := k8s.Delete(ctx, quota); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, func() bool {
		secrets := secretsOf(t, cp)
		signer := parseTLS(t, secrets["dev-kubelet-signer"]).Cert
		bundle, _ := certs.ParseCerts(secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey])
		c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
		return !signer.Equal(due) && containsCert(bundle, signer) && secrets["dev-kubelet-signer-next"] == nil && c.Reason == lodiav1alpha1.ReasonIssued
	})
	was, err := certs.ParseCerts(before)
	if err != nil {
		t.Fatal(err)
	}
	expectBundle(t, cp, append(was, parseTLS(t, secretsOf(t, cp)["dev-kubelet-signer"]).Cert)...)
}

func TestASecretQuotaRefusingTheStagedSignerLetsTheRequestCARenew(t *testing.T) {
	r := newReconciler(time.Hour)
	r.SignerPropagation = time.Hour
	cp := controlPlane(t, "signer-quota-requests")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	due := dueSigner(t, cp)
	issued := time.Now().Add(-50 * time.Minute)
	requests, err := certs.NewCA("kubeconfig-requests-ca@"+strconv.FormatInt(issued.UnixNano(), 10), issued, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	s := secretOf(t, cp, "dev-cakey-kubeconfig-requests")
	s.Data = map[string][]byte{lodiav1alpha1.TLSCertKey: mustPEM(t, requests.Cert), lodiav1alpha1.TLSPrivateKeyKey: mustKeyPEM(t, requests.Key)}
	if err := k8s.Update(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	fillSecretQuota(t, cp)
	for range 3 {
		reconcileFails(t, r, cp)
	}
	secrets := secretsOf(t, cp)
	renewed := parseTLS(t, secrets["dev-cakey-kubeconfig-requests"]).Cert
	if renewed.Equal(requests.Cert) {
		t.Fatal("a due request CA was not renewed while the quota refused the staged signer")
	}
	bundle, err := certs.ParseCerts(secretOf(t, cp, "dev-client-ca").Data[lodiav1alpha1.CACertKey])
	if err != nil {
		t.Fatal(err)
	}
	if !containsCert(bundle, renewed) || !parseTLS(t, secrets["dev-kubelet-signer"]).Cert.Equal(due) || secrets["dev-kubelet-signer-next"] != nil {
		t.Errorf("under the quota the bundle holds %v, or the signer changed, or a pending signer was written", subjects(bundle))
	}
	if c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady); c.Reason != lodiav1alpha1.ReasonIssuing || c.Message != "not written yet: dev-kubelet-signer-next; the error is in the log" {
		t.Errorf("CertificatesReady is %s: %s", c.Reason, c.Message)
	}
}

func TestASecretQuotaWithOneSlotLeftGivesItToTheStagedSigner(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	r.SignerPropagation = time.Hour
	cp := controlPlane(t, "signer-quota-slot")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	dueSigner(t, cp)
	if err := k8s.Delete(ctx, secretOf(t, cp, "dev-etcd-client-1")); err != nil {
		t.Fatal(err)
	}
	quota := fillSecretQuota(t, cp)
	used := quota.Status.Used["count/secrets"]
	used.Sub(resource.MustParse("1"))
	quota.Status.Used = corev1.ResourceList{"count/secrets": used}
	if err := k8s.Status().Update(ctx, quota); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, func() bool {
		probe := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: cp.Namespace}}
		return k8s.Create(ctx, probe, client.DryRunAll) == nil
	})
	reconcileFails(t, r, cp)
	secrets := secretsOf(t, cp)
	if secrets["dev-kubelet-signer-next"] == nil || secrets["dev-etcd-ca-2"] != nil {
		t.Error("the last slot went to etcd generation 2 rather than the staged signer")
	}
}

func fillSecretQuota(t *testing.T, cp *lodiav1alpha1.ControlPlane) *corev1.ResourceQuota {
	t.Helper()
	ctx := t.Context()
	full := corev1.ResourceList{"count/secrets": resource.MustParse(strconv.Itoa(len(namespaceSecrets(t, cp.Namespace))))}
	quota := &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "secrets", Namespace: cp.Namespace}, Spec: corev1.ResourceQuotaSpec{Hard: full}}
	if err := k8s.Create(ctx, quota); err != nil {
		t.Fatal(err)
	}
	quota.Status = corev1.ResourceQuotaStatus{Hard: full, Used: full}
	if err := k8s.Status().Update(ctx, quota); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, func() bool {
		probe := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: cp.Namespace}}
		err := k8s.Create(ctx, probe, client.DryRunAll)
		return err != nil && strings.Contains(err.Error(), "exceeded quota")
	})
	return quota
}
