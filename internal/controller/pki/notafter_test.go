// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
)

func TestNotAfterIsTheEarliestExpiryOfTheMode(t *testing.T) {
	for i, c := range []struct {
		name     string
		endpoint lodiav1alpha1.Endpoint
		publish  func(*testing.T, *lodiav1alpha1.ControlPlane)
		earliest string
		lifetime time.Duration
	}{
		{"nodePort", lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}, publishEndpoint, "dev-apiserver-kubelet-client", 365 * day},
		{"gateway", gatewayEndpoint(), publishGatewayEndpoint, "dev-tunnel-client", 90 * day},
	} {
		t.Run(c.name, func(t *testing.T) {
			start := time.Now()
			now := clocktesting.NewFakePassiveClock(start)
			r := longLived()
			r.Clock = now
			cp := controlPlaneAt(t, fmt.Sprintf("pki-not-after-%d", i), c.endpoint)
			reconcile(t, r, cp)
			now.SetTime(start.Add(day))
			c.publish(t, cp)
			reconcile(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)

			secrets := secretsOf(t, cp)
			want := start.Add(c.lifetime).Truncate(time.Second)
			if got := parseTLS(t, secrets[c.earliest]).Cert.NotAfter; !got.Equal(want) {
				t.Fatalf("%s ends at %s, want %s", c.earliest, got, want)
			}
			if serving := parseTLS(t, secrets["dev-apiserver-serving"]).Cert.NotAfter; !serving.After(want) {
				t.Fatalf("dev-apiserver-serving ends at %s, want after %s", serving, want)
			}
			expectNotAfter(t, cp, want)
		})
	}
}

func TestNotAfterMovesWithTheRenewalItNames(t *testing.T) {
	now := clocktesting.NewFakePassiveClock(time.Now())
	r := longLived()
	r.Clock = now
	cp := controlPlaneAt(t, "pki-not-after-renewal", gatewayEndpoint())
	publishGatewayEndpoint(t, cp)
	reconcile(t, r, cp)
	old := parseTLS(t, secretOf(t, cp, "dev-tunnel-client")).Cert
	expectNotAfter(t, cp, old.NotAfter)
	due := certs.RenewalTime(old)

	now.SetTime(due.Add(-time.Minute))
	written := freshControlPlane(t, cp).ResourceVersion
	reconcileOnce(t, r, cp)
	if after := freshControlPlane(t, cp).ResourceVersion; after != written {
		t.Errorf("a pass that issued nothing wrote the ControlPlane: resourceVersion %s, then %s", written, after)
	}

	now.SetTime(due)
	r.Client = failingApply{Client: controller, secret: "dev-tunnel-client"}
	reconcileFails(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonIssuing)
	expectNotAfter(t, cp, old.NotAfter)

	r.Client = controller
	reconcile(t, r, cp)
	renewed := parseTLS(t, secretOf(t, cp, "dev-tunnel-client")).Cert
	if want := due.Add(90 * day); !renewed.NotAfter.Equal(want) {
		t.Fatalf("the renewed dev-tunnel-client ends at %s, want %s", renewed.NotAfter, want)
	}
	expectNotAfter(t, cp, renewed.NotAfter)
}

func TestAPreviousEtcdGenerationIsLeftOutOfNotAfter(t *testing.T) {
	now := clocktesting.NewFakePassiveClock(time.Now())
	r := longLived()
	r.Clock = now
	cp := controlPlane(t, "pki-not-after-generation")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	previous := parseTLS(t, secretOf(t, cp, "dev-etcd-server-1")).Cert
	expectNotAfter(t, cp, previous.NotAfter)

	now.SetTime(certs.RenewalTime(previous))
	reconcile(t, r, cp)
	reconcile(t, r, cp)
	expectGenerations(t, cp, 1, 2)
	current := parseTLS(t, secretOf(t, cp, "dev-etcd-server-2")).Cert
	if signer := parseTLS(t, secretOf(t, cp, "dev-kubelet-signer")).Cert; !signer.NotAfter.Equal(current.NotAfter) {
		t.Fatalf("the kubelet signer ends at %s, want the renewal's %s", signer.NotAfter, current.NotAfter)
	}
	expectNotAfter(t, cp, current.NotAfter)
}

func TestAnEtcdGenerationNotWrittenLeavesThePreviousOneInNotAfter(t *testing.T) {
	now := clocktesting.NewFakePassiveClock(time.Now())
	r := longLived()
	r.Clock = now
	cp := controlPlane(t, "pki-not-after-unwritten-generation")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	previous := parseTLS(t, secretOf(t, cp, "dev-etcd-server-1")).Cert

	now.SetTime(certs.RenewalTime(previous))
	r.Client = refusingNewPeers{controller}
	for range 3 {
		reconcileFails(t, r, cp)
	}
	if signer := parseTLS(t, secretOf(t, cp, "dev-kubelet-signer")).Cert; !signer.NotAfter.After(previous.NotAfter) {
		t.Fatalf("the kubelet signer ends at %s, want it renewed past %s", signer.NotAfter, previous.NotAfter)
	}
	if secretsOf(t, cp)["dev-etcd-server-2"] == nil {
		t.Fatal("no part of a new etcd generation was written")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonIssuing)
	expectNotAfter(t, cp, previous.NotAfter)
}

type refusingNewPeers struct {
	client.Client
}

func (c refusingNewPeers) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	if s, ok := obj.(*corev1ac.SecretApplyConfiguration); ok && strings.HasPrefix(*s.GetName(), "dev-etcd-peer-") && *s.GetName() != "dev-etcd-peer-1" {
		return errors.New("the API server refused the apply")
	}
	return c.Client.Apply(ctx, obj, opts...)
}

func TestAStagedSignerIsLeftOutOfNotAfter(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-not-after-staged-signer")
	reconcile(t, r, cp)
	leaf := parseTLS(t, secretOf(t, cp, "dev-apiserver-kubelet-client")).Cert
	expectNotAfter(t, cp, leaf.NotAfter)
	staged := clientSigner(t, cp, time.Now().Add(-50*time.Minute))
	if !staged.Cert.NotAfter.Before(leaf.NotAfter) {
		t.Fatalf("the staged signer ends at %s, want before %s", staged.Cert.NotAfter, leaf.NotAfter)
	}
	createNext(t, cp, mustPEM(t, staged.Cert), mustKeyPEM(t, staged.Key), false)

	reconcile(t, r, cp)
	if _, kept := namespaceSecrets(t, cp.Namespace)["dev-kubelet-signer-next"]; kept {
		t.Fatal("a staged signer no replacement needs was kept")
	}
	expectNotAfter(t, cp, leaf.NotAfter)
}

func TestARequestCANotWrittenLeavesItsNotAfter(t *testing.T) {
	now := clocktesting.NewFakePassiveClock(time.Now())
	r := longLived()
	r.Clock = now
	cp := controlPlane(t, "pki-not-after-unwritten-request-ca")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	old := parseTLS(t, secretOf(t, cp, "dev-cakey-kubeconfig-requests")).Cert
	expectNotAfter(t, cp, old.NotAfter)

	now.SetTime(certs.RenewalTime(old))
	r.Client = failingApply{Client: controller, secret: "dev-cakey-kubeconfig-requests"}
	for range 3 {
		reconcileFails(t, r, cp)
	}
	for _, name := range []string{"dev-kubelet-signer", "dev-apiserver-kubelet-client"} {
		if c := parseTLS(t, secretOf(t, cp, name)).Cert; !c.NotAfter.After(old.NotAfter) {
			t.Fatalf("%s ends at %s, want it renewed past %s", name, c.NotAfter, old.NotAfter)
		}
	}
	if c := parseTLS(t, secretOf(t, cp, "dev-cakey-kubeconfig-requests")).Cert; !c.NotAfter.Equal(old.NotAfter) {
		t.Fatalf("the request CA was renewed: it ends at %s, want %s", c.NotAfter, old.NotAfter)
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonIssuing)
	expectNotAfter(t, cp, old.NotAfter)
}

func TestAStagedSignerNotWrittenLeavesTheSignersNotAfter(t *testing.T) {
	now := clocktesting.NewFakePassiveClock(time.Now())
	r := longLived()
	r.Clock = now
	cp := controlPlane(t, "pki-not-after-unwritten-signer")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	old := parseTLS(t, secretOf(t, cp, "dev-kubelet-signer")).Cert
	expectNotAfter(t, cp, old.NotAfter)

	now.SetTime(certs.RenewalTime(old))
	r.Client = failingApply{Client: controller, secret: "dev-kubelet-signer-next"}
	for range 3 {
		reconcileFails(t, r, cp)
	}
	for _, name := range []string{"dev-cakey-kubeconfig-requests", "dev-apiserver-kubelet-client", "dev-etcd-server-2"} {
		if c := parseTLS(t, secretOf(t, cp, name)).Cert; !c.NotAfter.After(old.NotAfter) {
			t.Fatalf("%s ends at %s, want it renewed past %s", name, c.NotAfter, old.NotAfter)
		}
	}
	if c := parseTLS(t, secretOf(t, cp, "dev-kubelet-signer")).Cert; !c.NotAfter.Equal(old.NotAfter) {
		t.Fatalf("the kubelet signer was replaced: it ends at %s, want %s", c.NotAfter, old.NotAfter)
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonIssuing)
	expectNotAfter(t, cp, old.NotAfter)
}

func TestARestoreThatFindsSecretsWritesNoNotAfter(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-not-after-restore", restoring)
	publishEndpoint(t, cp)
	ca, err := certs.NewCA("restored", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue(certs.Profile{CommonName: "kube-apiserver-kubelet-client", Organizations: []string{"lodia:kubelet-api-admins"}, Usages: clientAuth}, time.Now(), time.Minute)
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
	planted := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "dev-apiserver-kubelet-client", Namespace: cp.Namespace, Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name}},
		Type:       corev1.SecretTypeTLS,
		Data:       map[string][]byte{lodiav1alpha1.TLSCertKey: certPEM, lodiav1alpha1.TLSPrivateKeyKey: keyPEM},
	}
	if err := k8s.Create(ctx, planted); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonSecretsExist)
	if credentials := freshControlPlane(t, cp).Status.Credentials; credentials != nil && credentials.NotAfter != nil {
		t.Errorf("a restore that adopts nothing wrote status.credentials.notAfter %s", credentials.NotAfter)
	}

	if err := k8s.Delete(ctx, planted); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForBackup)
	expectNotAfter(t, cp, parseTLS(t, secretOf(t, cp, "dev-apiserver-kubelet-client")).Cert.NotAfter)
}

func expectNotAfter(t *testing.T, cp *lodiav1alpha1.ControlPlane, want time.Time) {
	t.Helper()
	var got *metav1.Time
	if credentials := freshControlPlane(t, cp).Status.Credentials; credentials != nil {
		got = credentials.NotAfter
	}
	if got == nil || !got.Time.Equal(want) {
		t.Fatalf("status.credentials.notAfter is %v, want %s", got, want)
	}
}
