// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"context"
	"crypto/x509"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/certs"
	"github.com/segla-io/brygga/internal/controller/pki"
)

func TestATenantAPIServerStartedFromTheCertificates(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-apiserver")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	expectCondition(t, cp, bryggav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, pki.ReasonIssued)
	expectCondition(t, cp, bryggav1alpha1.ConditionAccessReady, metav1.ConditionFalse, pki.ReasonWaitingForAPIServer)
	secrets := secretsOf(t, cp)

	dir := t.TempDir()
	file := func(secret, key string) string {
		t.Helper()
		path := filepath.Join(dir, secret+"-"+key)
		if err := os.WriteFile(path, secrets[secret].Data[key], 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	env := &envtest.Environment{}
	api := env.ControlPlane.GetAPIServer()
	api.Authn = &tenantAuthn{
		api:       api,
		clientCA:  parseTLS(t, secrets["dev-cakey-client"]),
		clusterCA: secrets["dev-ca-bundle"].Data["ca.crt"],
		bundle:    file("dev-client-ca-bundle", "ca.crt"),
	}
	saKey := file("dev-sa-keys", "signing.key")
	api.Configure().
		Set("tls-cert-file", file("dev-apiserver-serving", "tls.crt")).
		Set("tls-private-key-file", file("dev-apiserver-serving", "tls.key")).
		Set("requestheader-client-ca-file", file("dev-front-proxy-ca", "ca.crt")).
		Set("requestheader-allowed-names", "front-proxy-client").
		Set("requestheader-username-headers", "X-Remote-User").
		Set("requestheader-group-headers", "X-Remote-Group").
		Set("requestheader-extra-headers-prefix", "X-Remote-Extra-").
		Set("proxy-client-cert-file", file("dev-front-proxy-client", "tls.crt")).
		Set("proxy-client-key-file", file("dev-front-proxy-client", "tls.key")).
		Set("service-account-signing-key-file", saKey).
		Set("service-account-key-file", saKey)
	tenantCfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	admin, err := kubernetes.NewForConfig(tenantCfg)
	if err != nil {
		t.Fatal(err)
	}

	kcmDir := t.TempDir()
	for key, value := range secrets["dev-controller-manager-kubeconfig"].Data {
		if err := os.WriteFile(filepath.Join(kcmDir, key), value, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	kcmCfg, err := clientcmd.BuildConfigFromFlags(tenantCfg.Host, filepath.Join(kcmDir, "kubeconfig"))
	if err != nil {
		t.Fatal(err)
	}
	kcm, err := kubernetes.NewForConfig(kcmCfg)
	if err != nil {
		t.Fatal(err)
	}
	review, err := kcm.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if review.Status.UserInfo.Username != "system:kube-controller-manager" {
		t.Fatalf("the controller manager's kubeconfig authenticates as %q", review.Status.UserInfo.Username)
	}

	setAPIServerReady(t, cp)
	r.Dial = dialer(t, tenantCfg.Host)
	reconcile(t, r, cp)
	expectCondition(t, cp, bryggav1alpha1.ConditionAccessReady, metav1.ConditionTrue, pki.ReasonBound)
	for _, b := range []struct {
		name, kind, role string
	}{
		{"brygga:operator", rbacv1.UserKind, "cluster-admin"},
		{"brygga:cluster-admins", rbacv1.GroupKind, "cluster-admin"},
		{"brygga:kubelet-api-admins", rbacv1.GroupKind, "system:kubelet-api-admin"},
	} {
		binding, err := admin.RbacV1().ClusterRoleBindings().Get(ctx, b.name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(binding.Subjects) != 1 || binding.Subjects[0].Kind != b.kind || binding.Subjects[0].Name != b.name || binding.RoleRef.Kind != "ClusterRole" || binding.RoleRef.Name != b.role {
			t.Errorf("%s binds %+v to %+v", b.name, binding.Subjects, binding.RoleRef)
		}
	}

	if err := admin.RbacV1().ClusterRoleBindings().Delete(ctx, "brygga:operator", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	bryggaCfg, err := clientcmd.RESTConfigFromKubeConfig(secrets["dev-brygga-kubeconfig"].Data["kubeconfig"])
	if err != nil {
		t.Fatal(err)
	}
	bryggaCfg.Dial = r.Dial
	bryggaCfg.Proxy = http.ProxyURL(nil)
	brygga, err := kubernetes.NewForConfig(bryggaCfg)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, err := brygga.RbacV1().ClusterRoleBindings().Get(ctx, "brygga:cluster-admins", metav1.GetOptions{})
		return apierrors.IsForbidden(err)
	})

	reconcile(t, r, cp)
	expectCondition(t, cp, bryggav1alpha1.ConditionAccessReady, metav1.ConditionTrue, pki.ReasonBound)
	if _, err := admin.RbacV1().ClusterRoleBindings().Get(ctx, "brygga:operator", metav1.GetOptions{}); err != nil {
		t.Fatalf("brygga:operator's binding was not restored: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, err := brygga.RbacV1().ClusterRoleBindings().Get(ctx, "brygga:cluster-admins", metav1.GetOptions{})
		return err == nil
	})
}

func dialer(t *testing.T, host string) func(ctx context.Context, network, address string) (net.Conn, error) {
	t.Helper()
	u, err := url.Parse(host)
	if err != nil {
		t.Fatal(err)
	}
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, u.Host)
	}
}

type tenantAuthn struct {
	api       *envtest.APIServer
	clientCA  *certs.Certificate
	clusterCA []byte
	bundle    string
}

func (a *tenantAuthn) Configure(_ string, args *envtest.Arguments) error {
	args.Set("client-ca-file", a.bundle)
	return nil
}

func (a *tenantAuthn) Start() error {
	a.api.CA = a.clusterCA
	return nil
}

func (a *tenantAuthn) Stop() error {
	return nil
}

func (a *tenantAuthn) AddUser(user envtest.User, base *rest.Config) (*rest.Config, error) {
	c, err := a.clientCA.Issue(certs.Profile{CommonName: user.Name, Organizations: user.Groups, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, time.Hour)
	if err != nil {
		return nil, err
	}
	out := rest.CopyConfig(base)
	out.CertData = certs.CertPEM(c.Cert)
	out.KeyData, err = certs.KeyPEM(c.Key)
	return out, err
}
