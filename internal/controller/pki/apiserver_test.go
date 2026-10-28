// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"context"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
)

func TestATenantAPIServerStartedFromTheCertificates(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-apiserver")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAPIServer)
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
	env := &envtest.Environment{UseExistingCluster: new(false)}
	api := env.ControlPlane.GetAPIServer()
	api.Authn = &tenantAuthn{
		api:       api,
		clientCA:  parseTLS(t, secrets["dev-cakey-client"]),
		clusterCA: secrets["dev-ca-bundle"].Data[lodiav1alpha1.CACertKey],
		bundle:    file("dev-client-ca", lodiav1alpha1.CACertKey),
	}
	saKey := file("dev-sa-keys", lodiav1alpha1.SigningKeyKey)
	api.Configure().
		Set("tls-cert-file", file("dev-apiserver-serving", lodiav1alpha1.TLSCertKey)).
		Set("tls-private-key-file", file("dev-apiserver-serving", lodiav1alpha1.TLSPrivateKeyKey)).
		Set("requestheader-client-ca-file", file("dev-front-proxy-ca", lodiav1alpha1.CACertKey)).
		Set("requestheader-allowed-names", "front-proxy-client").
		Set("requestheader-username-headers", "X-Remote-User").
		Set("requestheader-group-headers", "X-Remote-Group").
		Set("requestheader-extra-headers-prefix", "X-Remote-Extra-").
		Set("proxy-client-cert-file", file("dev-front-proxy-client", lodiav1alpha1.TLSCertKey)).
		Set("proxy-client-key-file", file("dev-front-proxy-client", lodiav1alpha1.TLSPrivateKeyKey)).
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
	kcmCfg, err := clientcmd.BuildConfigFromFlags(tenantCfg.Host, filepath.Join(kcmDir, lodiav1alpha1.KubeconfigKey))
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
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionTrue, lodiav1alpha1.ReasonBound)
	r.Dial = unreachable
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Fatal("a pass that could not reach the tenant succeeded")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionTrue, lodiav1alpha1.ReasonBound)
	r.Dial = dialer(t, tenantCfg.Host)
	for _, b := range []struct {
		name, kind, role string
	}{
		{"lodia:operator", rbacv1.UserKind, "cluster-admin"},
		{"lodia:cluster-admins", rbacv1.GroupKind, "cluster-admin"},
		{"lodia:kubelet-api-admins", rbacv1.GroupKind, "system:kubelet-api-admin"},
	} {
		binding, err := admin.RbacV1().ClusterRoleBindings().Get(ctx, b.name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(binding.Subjects) != 1 || binding.Subjects[0].Kind != b.kind || binding.Subjects[0].Name != b.name || binding.RoleRef.Kind != "ClusterRole" || binding.RoleRef.Name != b.role {
			t.Errorf("%s binds %+v to %+v", b.name, binding.Subjects, binding.RoleRef)
		}
	}

	if err := admin.RbacV1().ClusterRoleBindings().Delete(ctx, "lodia:operator", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	lodiaCfg, err := clientcmd.RESTConfigFromKubeConfig(secrets["dev-lodia-kubeconfig"].Data[lodiav1alpha1.KubeconfigKey])
	if err != nil {
		t.Fatal(err)
	}
	lodiaCfg.Dial = r.Dial
	lodiaCfg.Proxy = http.ProxyURL(nil)
	lodia, err := kubernetes.NewForConfig(lodiaCfg)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, err := lodia.RbacV1().ClusterRoleBindings().Get(ctx, "lodia:cluster-admins", metav1.GetOptions{})
		return apierrors.IsForbidden(err)
	})

	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionTrue, lodiav1alpha1.ReasonBound)
	if _, err := admin.RbacV1().ClusterRoleBindings().Get(ctx, "lodia:operator", metav1.GetOptions{}); err != nil {
		t.Fatalf("lodia:operator's binding was not restored: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, err := lodia.RbacV1().ClusterRoleBindings().Get(ctx, "lodia:cluster-admins", metav1.GetOptions{})
		return err == nil
	})

	if err := k8s.Get(ctx, client.ObjectKeyFromObject(cp), cp); err != nil {
		t.Fatal(err)
	}
	stale := cp.Generation
	cp.Spec.Placement = &lodiav1alpha1.Placement{PriorityClassName: "system-cluster-critical"}
	if err := k8s.Update(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if cp.Generation == stale {
		t.Fatal("the generation did not move")
	}
	r.Dial = func(context.Context, string, string) (net.Conn, error) {
		t.Error("the tenant was dialed on an APIServerReady of an older generation")
		return nil, errors.New("not dialed")
	}
	reconcile(t, r, cp)
	expectConditionAt(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionTrue, lodiav1alpha1.ReasonBound, stale)
	setAPIServerReady(t, cp)
	r.Dial = dialer(t, tenantCfg.Host)
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionTrue, lodiav1alpha1.ReasonBound)

	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "no-kubelet-api-admins"},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			MatchConstraints: &admissionregistrationv1.MatchResources{ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
				RuleWithOperations: admissionregistrationv1.RuleWithOperations{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
					Rule:       admissionregistrationv1.Rule{APIGroups: []string{rbacv1.GroupName}, APIVersions: []string{"v1"}, Resources: []string{"clusterrolebindings"}},
				},
			}}},
			Validations: []admissionregistrationv1.Validation{{Expression: "object.metadata.name != 'lodia:kubelet-api-admins'"}},
		},
	}
	if _, err := admin.AdmissionregistrationV1().ValidatingAdmissionPolicies().Create(ctx, policy, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Create(ctx, &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: policy.Name},
		Spec:       admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{PolicyName: policy.Name, ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		binding, err := admin.RbacV1().ClusterRoleBindings().Get(ctx, "lodia:kubelet-api-admins", metav1.GetOptions{})
		if err != nil {
			return false
		}
		_, err = admin.RbacV1().ClusterRoleBindings().Update(ctx, binding, metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}})
		return err != nil
	})
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Fatal("a pass whose binding the tenant refused succeeded")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused)
	if c := condition(t, cp, lodiav1alpha1.ConditionAccessReady); !strings.HasPrefix(c.Message, "ClusterRoleBinding lodia:kubelet-api-admins is refused by the tenant's API server") {
		t.Errorf("AccessReady says %q", c.Message)
	}
}

func TestAccessReadyIsBindingUntilTheTenantAnswers(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-access-unanswered")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	setAPIServerReady(t, cp)
	r.Dial = unreachable
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Fatal("a pass that could not reach the tenant succeeded")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding)
}

func TestAccessReadyKeepsItsGenerationUntilTheAPIServerTakesTheChangeUp(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-access-generations")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	setAPIServerReady(t, cp)
	r.Dial = unreachable
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Fatal("a pass that could not reach the tenant succeeded")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding)
	binding := condition(t, cp, lodiav1alpha1.ConditionAccessReady)

	if err := k8s.Get(ctx, client.ObjectKeyFromObject(cp), cp); err != nil {
		t.Fatal(err)
	}
	cp.Spec.Placement = &lodiav1alpha1.Placement{PriorityClassName: "system-cluster-critical"}
	if err := k8s.Update(ctx, cp); err != nil {
		t.Fatal(err)
	}
	r.Dial = func(context.Context, string, string) (net.Conn, error) {
		t.Error("the tenant was dialed on an APIServerReady of an older generation")
		return nil, errors.New("not dialed")
	}
	reconcile(t, r, cp)
	if kept := condition(t, cp, lodiav1alpha1.ConditionAccessReady); !equality.Semantic.DeepEqual(kept, binding) {
		t.Fatalf("AccessReady is %+v at generation %d, want it kept as %+v", kept, cp.Generation, binding)
	}

	setAPIServerReady(t, cp)
	r.Dial = unreachable
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Fatal("a pass that could not reach the tenant succeeded")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding)

	setAPIServer(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)
	reconcile(t, r, cp)
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAPIServer)
}

func TestAccessReadyOnAStaleReadIsDecidedFromTheServer(t *testing.T) {
	ctx := t.Context()
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-access-stale")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	setAPIServerReady(t, cp)
	r.Dial = unreachable
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Fatal("a pass that could not reach the tenant succeeded")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding)

	fresh := func() *lodiav1alpha1.ControlPlane {
		t.Helper()
		out := &lodiav1alpha1.ControlPlane{}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(cp), out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(cp), cp); err != nil {
		t.Fatal(err)
	}
	cp.Spec.Placement = &lodiav1alpha1.Placement{PriorityClassName: "system-cluster-critical"}
	if err := k8s.Update(ctx, cp); err != nil {
		t.Fatal(err)
	}
	setAPIServerReady(t, cp)
	read := fresh()
	setAPIServer(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)
	stale := fresh()
	pass := func(at *lodiav1alpha1.ControlPlane, reader client.Reader) error {
		withRead := newReconciler(time.Hour)
		withRead.Client, withRead.Reader, withRead.Dial = snapshot{Client: controller, cp: at}, reader, unreachable
		_, err := withRead.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		return err
	}

	if err := pass(read, apiReader); err == nil {
		t.Fatal("a pass that could not reach the tenant succeeded")
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding)

	written := fresh().ResourceVersion
	if err := pass(stale, failingRead{Reader: apiReader, read: "the ControlPlane"}); err == nil || err.Error() != "the API server is down" {
		t.Errorf("a pass that cannot read AccessReady from the server returned %v, want the read's error", err)
	}
	if after := fresh().ResourceVersion; after != written {
		t.Errorf("a pass that cannot read AccessReady from the server wrote the ControlPlane: resourceVersion %s, then %s", written, after)
	}

	if err := pass(stale, apiReader); err != nil {
		t.Fatal(err)
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAPIServer)
}

type snapshot struct {
	client.Client
	cp *lodiav1alpha1.ControlPlane
}

func (c snapshot) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
		c.cp.DeepCopyInto(cp)
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func unreachable(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("connection refused")
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
	c, err := a.clientCA.Issue(certs.Profile{CommonName: user.Name, Organizations: user.Groups, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, time.Now(), time.Hour)
	if err != nil {
		return nil, err
	}
	out := rest.CopyConfig(base)
	if out.CertData, err = certs.CertPEM(c.Cert); err != nil {
		return nil, err
	}
	out.KeyData, err = certs.KeyPEM(c.Key)
	return out, err
}
