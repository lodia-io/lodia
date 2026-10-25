// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/segla-io/brygga/internal/certs"
)

// bootstrapLifetime is how long the system:masters certificate that makes the
// bindings lives; it exists in memory only.
const bootstrapLifetime = 10 * time.Minute

// bindings are the tenant's ClusterRoleBindings: each subject is bound under
// its own name.
var bindings = []struct {
	name, kind, role string
}{
	{"brygga:operator", rbacv1.UserKind, "cluster-admin"},
	{"brygga:cluster-admins", rbacv1.GroupKind, "cluster-admin"},
	{"brygga:kubelet-api-admins", rbacv1.GroupKind, "system:kubelet-api-admin"},
}

// bind applies the bindings as brygga:operator and, when that is forbidden
// (the first time, or after someone deleted its binding), with a
// system:masters certificate that is never stored.
func (r *Reconciler) bind(ctx context.Context, t *tenant) error {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(t.existing[t.name("brygga-kubeconfig")].Data[kubeconfigKey])
	if err != nil {
		return err
	}
	// The in-namespace Service is never behind a proxy from the environment.
	cfg.Proxy = http.ProxyURL(nil)
	cfg.Dial = r.Dial
	err = applyBindings(ctx, cfg)
	if !apierrors.IsForbidden(err) {
		return err
	}
	ca := t.cas[clientCA]
	if ca == nil {
		return errors.New("brygga:operator is forbidden and the client CA has no key to issue a bootstrap certificate")
	}
	bootstrap, err := ca.Issue(certs.Profile{CommonName: "brygga:bootstrap", Organizations: []string{"system:masters"}, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, bootstrapLifetime)
	if err != nil {
		return err
	}
	asBootstrap := rest.AnonymousClientConfig(cfg)
	if asBootstrap.CertData, err = certs.CertPEM(bootstrap.Cert); err != nil {
		return err
	}
	if asBootstrap.KeyData, err = certs.KeyPEM(bootstrap.Key); err != nil {
		return err
	}
	return applyBindings(ctx, asBootstrap)
}

func applyBindings(ctx context.Context, cfg *rest.Config) error {
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	for _, b := range bindings {
		binding := rbacv1ac.ClusterRoleBinding(b.name).
			WithLabels(map[string]string{"app.kubernetes.io/managed-by": "brygga"}).
			WithSubjects(rbacv1ac.Subject().WithKind(b.kind).WithAPIGroup(rbacv1.GroupName).WithName(b.name)).
			WithRoleRef(rbacv1ac.RoleRef().WithAPIGroup(rbacv1.GroupName).WithKind("ClusterRole").WithName(b.role))
		if _, err := k8s.RbacV1().ClusterRoleBindings().Apply(ctx, binding, metav1.ApplyOptions{FieldManager: fieldManager, Force: true}); err != nil {
			return fmt.Errorf("apply ClusterRoleBinding %s: %w", b.name, err)
		}
	}
	return nil
}
