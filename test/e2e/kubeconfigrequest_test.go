// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/keyutil"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func requestKubeconfigs(ctx context.Context, t *testing.T, cfg *envconf.Config, k8s client.Client, key client.ObjectKey, node string) {
	t.Helper()
	const name = "requester"
	username := "system:serviceaccount:" + key.Namespace + ":" + name
	subject := []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: key.Namespace, Name: name}}
	objects := []client.Object{
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: key.Namespace}},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: "kubeconfig-requester", Namespace: key.Namespace},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{lodiav1alpha1.GroupVersion.Group}, Resources: []string{"kubeconfigrequests"}, Verbs: []string{"create", "get"}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "kubeconfig-requester", Namespace: key.Namespace},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "kubeconfig-requester"},
			Subjects:   subject,
		},
	}
	admin := []client.Object{
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name + "-admins", Namespace: key.Namespace},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{lodiav1alpha1.GroupVersion.Group}, Resources: []string{"controlplanes/adminkubeconfig"}, ResourceNames: []string{key.Name}, Verbs: []string{"create"}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name + "-admins", Namespace: key.Namespace},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: key.Name + "-admins"},
			Subjects:   subject,
		},
	}
	var requests []client.Object
	defer func() {
		for _, obj := range slices.Concat(requests, objects, admin) {
			if err := client.IgnoreNotFound(k8s.Delete(context.Background(), obj)); err != nil {
				t.Error(err)
			}
		}
	}()
	for _, obj := range objects {
		create(ctx, t, k8s, obj)
	}
	token := &authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: new(int64(3600))}}
	if err := k8s.SubResource("token").Create(ctx, objects[0], token); err != nil {
		t.Fatal(err)
	}
	asRequester := rest.AnonymousClientConfig(cfg.Client().RESTConfig())
	asRequester.BearerToken = token.Status.Token
	requester, err := client.New(asRequester, client.Options{Scheme: k8s.Scheme()})
	if err != nil {
		t.Fatal(err)
	}

	ask := func(user string, access lodiav1alpha1.KubeconfigAccess) (*lodiav1alpha1.KubeconfigRequest, *ecdsa.PrivateKey, error) {
		privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: name}}, privateKey)
		if err != nil {
			t.Fatal(err)
		}
		kr := &lodiav1alpha1.KubeconfigRequest{
			ObjectMeta: metav1.ObjectMeta{GenerateName: name + "-", Namespace: key.Namespace},
			Spec: lodiav1alpha1.KubeconfigRequestSpec{
				ControlPlaneRef: lodiav1alpha1.LocalReference{Name: key.Name},
				User:            user,
				Access:          access,
				Request:         pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csr}),
			},
		}
		err = requester.Create(ctx, kr)
		if err == nil {
			requests = append(requests, kr)
		}
		return kr, privateKey, err
	}
	refused := func(err error, want string) error {
		if err == nil {
			return errors.New("admitted")
		}
		if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), want) {
			return fmt.Errorf("refused with %w, want %q", err, want)
		}
		return nil
	}
	answered := func(kr *lodiav1alpha1.KubeconfigRequest, privateKey *ecdsa.PrivateKey) (client.Client, authenticationv1.UserInfo) {
		t.Helper()
		until(ctx, t, time.Minute, "KubeconfigRequest "+kr.Name+" is Ready", func(ctx context.Context) error {
			if err := requester.Get(ctx, client.ObjectKeyFromObject(kr), kr); err != nil {
				return err
			}
			if c := meta.FindStatusCondition(kr.Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Status != metav1.ConditionTrue {
				return fmt.Errorf("Ready is %+v", c)
			}
			return nil
		})
		kubeconfig, err := clientcmd.Load(kr.Status.Kubeconfig)
		if err != nil {
			t.Fatal(err)
		}
		user := kubeconfig.AuthInfos[kubeconfig.Contexts[kubeconfig.CurrentContext].AuthInfo]
		if user.ClientKeyData != nil {
			t.Fatal("the kubeconfig holds a key")
		}
		if user.ClientKeyData, err = keyutil.MarshalPrivateKeyToPEM(privateKey); err != nil {
			t.Fatal(err)
		}
		tenantCfg, err := clientcmd.NewNonInteractiveClientConfig(*kubeconfig, kubeconfig.CurrentContext, nil, nil).ClientConfig()
		if err != nil {
			t.Fatal(err)
		}
		tenant, err := client.New(tenantCfg, client.Options{})
		if err != nil {
			t.Fatal(err)
		}
		review := &authenticationv1.SelfSubjectReview{}
		if err := tenant.Create(ctx, review); err != nil {
			t.Fatal(err)
		}
		certificates, err := certutil.ParseCertsPEM(user.ClientCertificateData)
		if err != nil {
			t.Fatal(err)
		}
		fingerprint := sha256.Sum256(certificates[0].Raw)
		if id := review.Status.UserInfo.Extra["authentication.kubernetes.io/credential-id"]; !slices.Equal(id, []string{"X509SHA256=" + hex.EncodeToString(fingerprint[:])}) {
			t.Errorf("the tenant names the credential %v, want its certificate's fingerprint", id)
		}
		return tenant, review.Status.UserInfo
	}

	var kr *lodiav1alpha1.KubeconfigRequest
	var privateKey *ecdsa.PrivateKey
	until(ctx, t, time.Minute, username+" asks for a kubeconfig for itself", func(context.Context) error {
		kr, privateKey, err = ask(username, "")
		return err
	})
	_, _, err = ask("system:serviceaccount:"+key.Namespace+":someone-else", "")
	if err := refused(err, "spec.user must be the requester, "+username); err != nil {
		t.Errorf("a request naming another user: %v", err)
	}
	_, _, err = ask(username, "ClusterAdmin")
	if err := refused(err, username+" may not create controlplanes/adminkubeconfig for "+key.Name); err != nil {
		t.Errorf("a ClusterAdmin request without the permission: %v", err)
	}
	tenant, user := answered(kr, privateKey)
	if user.Username != "lodia:user:"+username || slices.Contains(user.Groups, "lodia:cluster-admins") {
		t.Errorf("the kubeconfig authenticates as %s in %v, want lodia:user:%s outside lodia:cluster-admins", user.Username, user.Groups, username)
	}
	if err := tenant.List(ctx, &corev1.NodeList{}); !apierrors.IsForbidden(err) {
		t.Errorf("the kubeconfig without ClusterAdmin lists nodes: %v", err)
	}

	for _, obj := range admin {
		create(ctx, t, k8s, obj)
	}
	until(ctx, t, time.Minute, username+" asks for a ClusterAdmin kubeconfig", func(context.Context) error {
		kr, privateKey, err = ask(username, "ClusterAdmin")
		return err
	})
	tenant, user = answered(kr, privateKey)
	if user.Username != "lodia:user:"+username || !slices.Contains(user.Groups, "lodia:cluster-admins") {
		t.Errorf("the ClusterAdmin kubeconfig authenticates as %s in %v, want lodia:user:%s in lodia:cluster-admins", user.Username, user.Groups, username)
	}
	nodes := &corev1.NodeList{}
	if err := tenant.List(ctx, nodes); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(nodes.Items, func(n corev1.Node) bool { return n.Name == node }) {
		t.Errorf("the ClusterAdmin kubeconfig lists %d nodes, without %s", len(nodes.Items), node)
	}
}
