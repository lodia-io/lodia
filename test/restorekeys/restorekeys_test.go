// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package restorekeys_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	certificatesv1 "k8s.io/api/certificates/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/versions"
)

type object struct {
	obj client.Object
	key string
}

func TestTheCleanupDeletesWhatBindsTheDataToItsSource(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run make restore-keys")
	}
	env := &envtest.Environment{UseExistingCluster: new(false)}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	discovered, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	version, err := discovered.ServerVersion()
	if err != nil {
		t.Fatal(err)
	}
	if minor, _ := versions.Minor(version.GitVersion); !slices.Contains(versions.Minors(), minor) {
		t.Fatalf("the API server runs %s, of no minor Lodia runs: %v", version.GitVersion, versions.Minors())
	}
	t.Logf("kube-apiserver %s", version.GitVersion)
	c, err := client.New(cfg, client.Options{Scheme: clientgoscheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()

	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system-x"}}); err != nil {
		t.Fatal(err)
	}
	lease := func(namespace, name string) *coordinationv1.Lease {
		return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Spec: coordinationv1.LeaseSpec{HolderIdentity: new("source")}}
	}
	secret := func(name string, kind corev1.SecretType) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: metav1.NamespaceSystem}, Type: kind,
			Data: map[string][]byte{"token-id": []byte("abcdef"), "token-secret": []byte("0123456789abcdef")},
		}
	}
	deleted := []object{
		{&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "source-node"}}, "/registry/minions/source-node"},
		{lease(metav1.NamespaceSystem, "kube-controller-manager"), "/registry/leases/kube-system/kube-controller-manager"},
		{lease(corev1.NamespaceNodeLease, "source-node"), "/registry/leases/kube-node-lease/source-node"},
		{certificateSigningRequest(t), "/registry/certificatesigningrequests/csr-source-node"},
		{secret("bootstrap-token-abcdef", corev1.SecretTypeBootstrapToken), "/registry/secrets/kube-system/bootstrap-token-abcdef"},
	}
	served, err := discovered.ServerResourcesForGroupVersion(certificatesv1.SchemeGroupVersion.String())
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(served.APIResources, func(r metav1.APIResource) bool { return r.Name == "podcertificaterequests" }) {
		deleted = append(deleted, object{podCertificateRequest(t), "/registry/podcertificaterequests/default/pcr"})
	} else {
		t.Logf("%s serves no podcertificaterequests", certificatesv1.SchemeGroupVersion)
	}
	kept := []object{
		{lease("kube-system-x", "l"), "/registry/leases/kube-system-x/l"},
		{secret("other", corev1.SecretTypeOpaque), "/registry/secrets/kube-system/other"},
		{&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "extension-apiserver-authentication-x", Namespace: metav1.NamespaceSystem}}, "/registry/configmaps/kube-system/extension-apiserver-authentication-x"},
		{&storagev1.VolumeAttachment{ObjectMeta: metav1.ObjectMeta{Name: "v"}, Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi.example.com", NodeName: "source-node", Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: new("pv")},
		}}, "/registry/volumeattachments/v"},
	}
	for _, o := range slices.Concat(deleted, kept) {
		if err := c.Create(ctx, o.obj); err != nil {
			t.Fatalf("create %T %s: %v", o.obj, o.obj.GetName(), err)
		}
	}
	authentication := &corev1.ConfigMap{}
	eventually(t, func() error {
		return c.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: "extension-apiserver-authentication"}, authentication)
	})
	deleted = append(deleted, object{authentication.DeepCopy(), "/registry/configmaps/kube-system/extension-apiserver-authentication"})

	etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{env.ControlPlane.Etcd.URL.String()}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = etcd.Close() })
	stored := func(key string) bool {
		t.Helper()
		response, err := etcd.Get(ctx, key, clientv3.WithCountOnly())
		if err != nil {
			t.Fatal(err)
		}
		return response.Count == 1
	}
	for _, o := range slices.Concat(deleted, kept) {
		if !stored(o.key) {
			t.Fatalf("the API server stores %T %s elsewhere than %s", o.obj, o.obj.GetName(), o.key)
		}
	}

	keys, revision, err := datastore.Cleanup(ctx, etcd, "5e1f7c3a-0000-4000-8000-0000000000aa", []byte(`{}`))
	if err != nil || revision == 0 || keys < int64(len(deleted)) {
		t.Fatalf("the cleanup deleted %d keys at revision %d, %v; want at least %d", keys, revision, err, len(deleted))
	}
	for _, o := range deleted {
		if stored(o.key) {
			t.Errorf("the cleanup left %s", o.key)
		}
	}
	for _, o := range kept {
		if !stored(o.key) {
			t.Errorf("the cleanup deleted %s", o.key)
		}
	}
	for _, o := range deleted[:len(deleted)-1] {
		eventually(t, func() error {
			err := c.Get(ctx, client.ObjectKeyFromObject(o.obj), o.obj)
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("%T %s is still served: %w", o.obj, o.obj.GetName(), err)
		})
	}
	for _, o := range kept {
		if err := c.Get(ctx, client.ObjectKeyFromObject(o.obj), o.obj); err != nil {
			t.Errorf("%T %s: %v", o.obj, o.obj.GetName(), err)
		}
	}
	eventually(t, func() error {
		again := &corev1.ConfigMap{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(authentication), again); err != nil {
			return err
		}
		if again.UID == authentication.UID {
			return errors.New("the API server still serves the deleted extension-apiserver-authentication")
		}
		if want := authentication.Data["client-ca-file"]; want == "" || again.Data["client-ca-file"] != want {
			return fmt.Errorf("extension-apiserver-authentication came back with client-ca-file %q, want the API server's %q", again.Data["client-ca-file"], want)
		}
		return nil
	})
}

func eventually(t *testing.T, check func() error) {
	t.Helper()
	var last error
	if err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, time.Minute, true, func(context.Context) (bool, error) {
		last = check()
		return last == nil, nil
	}); err != nil {
		t.Fatalf("%v: %v", err, last)
	}
}

func certificateSigningRequest(t *testing.T) *certificatesv1.CertificateSigningRequest {
	t.Helper()
	return &certificatesv1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "csr-source-node"},
		Spec: certificatesv1.CertificateSigningRequestSpec{
			Request:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: request(t)}),
			SignerName: "example.com/restore",
			Usages:     []certificatesv1.KeyUsage{certificatesv1.UsageDigitalSignature},
		},
	}
}

func podCertificateRequest(t *testing.T) *certificatesv1.PodCertificateRequest {
	t.Helper()
	return &certificatesv1.PodCertificateRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "pcr", Namespace: metav1.NamespaceDefault},
		Spec: certificatesv1.PodCertificateRequestSpec{
			SignerName: "example.com/restore", PodName: "pod", PodUID: "5e1f7c3a-0000-4000-8000-0000000000b1",
			ServiceAccountName: "default", ServiceAccountUID: "5e1f7c3a-0000-4000-8000-0000000000b2",
			NodeName: "source-node", NodeUID: "5e1f7c3a-0000-4000-8000-0000000000b3",
			StubPKCS10Request: request(t),
		},
	}
}

func request(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "source-node"}}, key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}
