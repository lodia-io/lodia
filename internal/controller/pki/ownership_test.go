// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
)

func TestAPredecessorsSecretsAreNeitherReadNorWritten(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-predecessor")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	predecessor := secretsOf(t, cp)
	key := predecessor["dev-cakey-cluster"]
	key.Labels = nil
	if err := k8s.Update(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(ctx, cp); err != nil {
		t.Fatal(err)
	}
	successor := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: cp.Name, Namespace: cp.Namespace}, Spec: cp.Spec}
	if err := k8s.Create(ctx, successor); err != nil {
		t.Fatal(err)
	}
	publishEndpoint(t, successor)

	before := namespaceSecrets(t, cp.Namespace)
	result := reconcile(t, r, successor)
	c := condition(t, successor, lodiav1alpha1.ConditionCertificatesReady)
	if c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonSecretsControlledElsewhere || !strings.Contains(c.Message, "by ControlPlane dev (UID "+string(cp.UID)+")") {
		t.Fatalf("CertificatesReady is %s %s: %s", c.Status, c.Reason, c.Message)
	}
	if after := namespaceSecrets(t, cp.Namespace); !maps.Equal(before, after) {
		t.Fatal("a Secret was written beside the predecessor's")
	}
	if result.RequeueAfter != 10*time.Second {
		t.Errorf("requeue after %s, want 10s while the garbage collector deletes a predecessor's Secrets", result.RequeueAfter)
	}

	for name := range before {
		if err := k8s.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace}}); err != nil {
			t.Fatal(err)
		}
	}
	reconcile(t, r, successor)
	expectCondition(t, successor, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	fresh := secretsOf(t, successor)
	for _, ca := range []string{"cluster", "client", "front-proxy", "etcd"} {
		name := "dev-cakey-" + ca
		if bytes.Equal(fresh[name].Data[lodiav1alpha1.TLSPrivateKeyKey], predecessor[name].Data[lodiav1alpha1.TLSPrivateKeyKey]) {
			t.Errorf("the successor keeps the predecessor's %s CA key", ca)
		}
		if !metav1.IsControlledBy(fresh[name], successor) {
			t.Errorf("%s is not the successor's", name)
		}
	}
}

func TestASecretAnotherOwnerKeepsBlocksEveryWrite(t *testing.T) {
	other := types.UID("8f0c6f2e-0d6e-4f4e-9e44-0d6a7c1b5a21")
	for i, c := range []struct {
		name, secret string
		labelled     bool
		owner        metav1.OwnerReference
		requeue      time.Duration
	}{
		{"a controller of another kind", "dev-front-proxy-client", true,
			metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: "restore", UID: other, Controller: new(true)}, time.Minute},
		{"a reference to another ControlPlane that is not its controller", "dev-apiserver-kubelet-client", true,
			metav1.OwnerReference{APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "dev", UID: other}, 10 * time.Second},
		{"an unlabelled leaf of another ControlPlane", "dev-apiserver-serving", false,
			metav1.OwnerReference{APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "prod", UID: other, Controller: new(true)}, time.Minute},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx := t.Context()
			r := newReconciler(time.Hour)
			cp := controlPlane(t, fmt.Sprintf("pki-held-%d", i))
			publishEndpoint(t, cp)
			reconcile(t, r, cp)
			old := secretsOf(t, cp)[c.secret]
			if err := k8s.Delete(ctx, old); err != nil {
				t.Fatal(err)
			}
			held := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: c.secret, Namespace: cp.Namespace, OwnerReferences: []metav1.OwnerReference{c.owner}}, Type: old.Type, Data: old.Data}
			if c.labelled {
				held.Labels = old.Labels
			}
			if err := k8s.Create(ctx, held); err != nil {
				t.Fatal(err)
			}
			credentials := freshControlPlane(t, cp).Status.Credentials

			down := newReconciler(time.Hour)
			down.Reader = failingRead{Reader: apiReader, read: "the ControlPlane"}
			written := freshControlPlane(t, cp).ResourceVersion
			if _, err := down.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil || err.Error() != "the API server is down" {
				t.Errorf("a pass that cannot read AccessReady from the server returned %v, want the read's error", err)
			}
			if after := freshControlPlane(t, cp).ResourceVersion; after != written {
				t.Errorf("a pass that cannot read AccessReady from the server wrote the ControlPlane: resourceVersion %s, then %s", written, after)
			}

			before := namespaceSecrets(t, cp.Namespace)
			result := reconcile(t, r, cp)
			got := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
			if got.Reason != lodiav1alpha1.ReasonSecretsControlledElsewhere || !strings.Contains(got.Message, c.secret+" by "+c.owner.Kind+" "+c.owner.Name) {
				t.Fatalf("CertificatesReady is %s: %s", got.Reason, got.Message)
			}
			if after := namespaceSecrets(t, cp.Namespace); !maps.Equal(before, after) {
				t.Error("a Secret was written while another owner keeps one under PKI's names")
			}
			if result.RequeueAfter != c.requeue {
				t.Errorf("requeue after %s, want %s", result.RequeueAfter, c.requeue)
			}
			if fresh := freshControlPlane(t, cp).Status.Credentials; fresh == nil || *fresh.AdminKubeconfigSecretRef != *credentials.AdminKubeconfigSecretRef || *fresh.CABundleSecretRef != *credentials.CABundleSecretRef {
				t.Errorf("status.credentials moved from %+v to %+v", credentials, fresh)
			}

			if err := k8s.Delete(ctx, held); err != nil {
				t.Fatal(err)
			}
			reconcile(t, r, cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
			if s := secretsOf(t, cp)[c.secret]; s == nil || !metav1.IsControlledBy(s, cp) {
				t.Errorf("%s was not written again once the other owner's was gone", c.secret)
			}
		})
	}
}

func TestLeftoversUnderPKINames(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-leftovers")
	for _, name := range []string{"dev-etcd-ca-1", "dev-etcd-ca-01"} {
		leftover := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace}, Data: map[string][]byte{lodiav1alpha1.CACertKey: []byte("left over")}, Immutable: new(true)}
		if err := k8s.Create(ctx, leftover); err != nil {
			t.Fatal(err)
		}
	}
	before := namespaceSecrets(t, cp.Namespace)
	reconcile(t, r, cp)
	expectGenerations(t, cp, 2)
	after := namespaceSecrets(t, cp.Namespace)
	if _, kept := after["dev-etcd-ca-1"]; kept {
		t.Error("the leftover of generation 1, older than the current one, was kept")
	}
	if after["dev-etcd-ca-01"] != before["dev-etcd-ca-01"] {
		t.Error("dev-etcd-ca-01, not one of PKI's names, was written or deleted")
	}

	secrets := secretsOf(t, cp)
	for _, name := range []string{"dev-encryption-config", "dev-sa-keys"} {
		restored := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace}, Type: secrets[name].Type, Data: secrets[name].Data}
		if err := k8s.Delete(ctx, secrets[name]); err != nil {
			t.Fatal(err)
		}
		if err := k8s.Create(ctx, restored); err != nil {
			t.Fatal(err)
		}
	}
	reconcile(t, r, cp)
	adopted := secretsOf(t, cp)
	for _, name := range []string{"dev-encryption-config", "dev-sa-keys"} {
		if adopted[name] == nil || !metav1.IsControlledBy(adopted[name], cp) || !maps.EqualFunc(adopted[name].Data, secrets[name].Data, bytes.Equal) {
			t.Errorf("%s restored without labels or owner was not adopted with its keys", name)
		}
	}
}

func TestAnOldGenerationChangedSinceItWasReadIsKept(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-delete-race")
	leftover := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "dev-etcd-ca-1", Namespace: cp.Namespace}, Data: map[string][]byte{lodiav1alpha1.CACertKey: []byte("left over")}, Immutable: new(true)}
	if err := k8s.Create(ctx, leftover); err != nil {
		t.Fatal(err)
	}
	r.Client = changedBeforeDelete{Client: controller}
	reconcileFails(t, r, cp)
	if _, kept := namespaceSecrets(t, cp.Namespace)["dev-etcd-ca-1"]; !kept {
		t.Fatal("a Secret changed after PKI read it was deleted")
	}
	r.Client = controller
	reconcile(t, r, cp)
	if _, kept := namespaceSecrets(t, cp.Namespace)["dev-etcd-ca-1"]; kept {
		t.Error("the old generation was kept once PKI had read it as it is")
	}
}

type changedBeforeDelete struct {
	client.Client
}

func (c changedBeforeDelete) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	changed := &corev1.Secret{}
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(obj), changed); err != nil {
		return err
	}
	changed.Annotations = map[string]string{"changed": "after PKI read it"}
	if err := k8s.Update(ctx, changed); err != nil {
		return err
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func TestKeptGenerationsAreAdopted(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	r := newReconciler(9 * time.Second)
	cp := controlPlane(t, "pki-etcd-adopted")
	reconcile(t, r, cp)
	server := parseTLS(t, secretsOf(t, cp)["dev-etcd-server-1"])
	time.Sleep(time.Until(certs.RenewalTime(server.Cert)) + 500*time.Millisecond)
	reconcile(t, r, cp)
	expectGenerations(t, cp, 1, 2)

	before := secretsOf(t, cp)
	for name, s := range before {
		if s.Labels[lodiav1alpha1.EtcdCertificatesLabel] == "" {
			continue
		}
		orphan := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace, Labels: s.Labels}, Type: s.Type, Data: s.Data, Immutable: new(true)}
		if name == "dev-etcd-peer-2" {
			orphan.Labels = nil
		}
		if err := k8s.Delete(ctx, s); err != nil {
			t.Fatal(err)
		}
		if err := k8s.Create(ctx, orphan); err != nil {
			t.Fatal(err)
		}
	}
	r.Client = failingApply{Client: controller, secret: "dev-etcd-backup-restore-1"}
	reconcileFails(t, r, cp)
	if c := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady); c.Reason != lodiav1alpha1.ReasonIssuing || c.Message != "not written yet: dev-etcd-backup-restore-1; the error is in the log" {
		t.Fatalf("CertificatesReady is %s while generation 1 cannot be adopted: %s", c.Reason, c.Message)
	}
	r.Client = controller
	reconcile(t, r, cp)
	expectGenerations(t, cp, 1, 2)
	for name, s := range secretsOf(t, cp) {
		if s.Labels[lodiav1alpha1.EtcdCertificatesLabel] == "" {
			continue
		}
		if !metav1.IsControlledBy(s, cp) || !maps.EqualFunc(s.Data, before[name].Data, bytes.Equal) {
			t.Errorf("%s was not adopted with its data unchanged", name)
		}
	}
}

func TestKeyMaterialPKICannotReadIsNeverReplaced(t *testing.T) {
	short := base64.StdEncoding.EncodeToString(make([]byte, 16))
	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ecPEM, err := certs.KeyPEM(ec)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range []struct {
		name, secret string
		data         map[string][]byte
	}{
		{"an encryption configuration that does not parse", "dev-encryption-config", map[string][]byte{lodiav1alpha1.EncryptionConfigKey: []byte("resources: [")}},
		{"a key the API server would refuse", "dev-encryption-config", map[string][]byte{lodiav1alpha1.EncryptionConfigKey: []byte(
			"apiVersion: apiserver.config.k8s.io/v1\nkind: EncryptionConfiguration\nresources:\n- resources: [secrets]\n  providers:\n  - secretbox:\n      keys:\n      - name: key1\n        secret: " + short + "\n")}},
		{"no configuration in the Secret", "dev-encryption-config", map[string][]byte{"other": []byte("x")}},
		{"a ServiceAccount key that does not parse", "dev-sa-keys", map[string][]byte{lodiav1alpha1.SigningKeyKey: []byte("not a key")}},
		{"an ECDSA ServiceAccount key", "dev-sa-keys", map[string][]byte{lodiav1alpha1.SigningKeyKey: ecPEM}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newReconciler(time.Hour)
			cp := controlPlane(t, fmt.Sprintf("pki-unreadable-%d", i))
			publishEndpoint(t, cp)
			reconcile(t, r, cp)
			s := secretsOf(t, cp)[c.secret]
			s.Data = c.data
			if err := k8s.Update(t.Context(), s); err != nil {
				t.Fatal(err)
			}
			reconcile(t, r, cp)
			got := condition(t, cp, lodiav1alpha1.ConditionCertificatesReady)
			if got.Reason != lodiav1alpha1.ReasonSecretUnreadable || !strings.Contains(got.Message, c.secret) {
				t.Fatalf("CertificatesReady is %s: %s", got.Reason, got.Message)
			}
			if !maps.EqualFunc(secretsOf(t, cp)[c.secret].Data, c.data, bytes.Equal) {
				t.Errorf("%s, which PKI cannot read, was replaced", c.secret)
			}
		})
	}
}

func TestAFailedReadLeavesTheStatus(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-read-fails")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	secrets := secretsOf(t, cp)
	if err := k8s.Delete(ctx, secrets["dev-cakey-etcd"]); err != nil {
		t.Fatal(err)
	}
	unlabelled := secretsOf(t, cp)["dev-sa-keys"]
	unlabelled.Labels = nil
	if err := k8s.Update(ctx, unlabelled); err != nil {
		t.Fatal(err)
	}
	_, before := resourceVersions(t, cp)
	for _, read := range []string{"the labelled Secrets", "the namespace's metadata", "a Secret by name"} {
		r.Reader = failingRead{Reader: apiReader, read: read}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
			t.Fatalf("a pass that could not read %s succeeded", read)
		}
		if _, after := resourceVersions(t, cp); after != before {
			t.Fatalf("a pass that could not read %s wrote the status", read)
		}
	}
	r.Reader = apiReader
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonCAMissing)
}

type failingRead struct {
	client.Reader
	read string
}

func (f failingRead) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	_, metadata := list.(*metav1.PartialObjectMetadataList)
	if (metadata && f.read == "the namespace's metadata") || (!metadata && f.read == "the labelled Secrets") {
		return errors.New("the API server is down")
	}
	return f.Reader.List(ctx, list, opts...)
}

func (f failingRead) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	_, controlPlane := obj.(*lodiav1alpha1.ControlPlane)
	if (controlPlane && f.read == "the ControlPlane") || (!controlPlane && f.read == "a Secret by name") {
		return errors.New("the API server is down")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

func namespaceSecrets(t *testing.T, namespace string) map[string]string {
	t.Helper()
	var list corev1.SecretList
	if err := k8s.List(t.Context(), &list, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	versions := make(map[string]string, len(list.Items))
	for _, s := range list.Items {
		versions[s.Name] = s.ResourceVersion
	}
	return versions
}

func freshControlPlane(t *testing.T, cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.ControlPlane {
	t.Helper()
	fresh := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		t.Fatal(err)
	}
	return fresh
}
