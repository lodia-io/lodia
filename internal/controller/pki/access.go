// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/certs"
	"go.lodia.io/lodia/internal/tenantclient"
)

const (
	// bootstrapLifetime is how long the system:masters certificate that makes
	// the bindings lives; it exists in memory only.
	bootstrapLifetime = 10 * time.Minute
	// bindTimeout bounds each apply of the three bindings, and the dial and the
	// handshake of every request.
	bindTimeout = 10 * time.Second
	// clusterAdmins is the group of the admin kubeconfig and of a
	// ClusterAdmin request's certificate, bound to cluster-admin.
	clusterAdmins = "lodia:cluster-admins"
)

var bindings = []struct {
	name, kind, role string
}{
	{"lodia:operator", rbacv1.UserKind, "cluster-admin"},
	{clusterAdmins, rbacv1.GroupKind, "cluster-admin"},
	{"lodia:kubelet-api-admins", rbacv1.GroupKind, "system:kubelet-api-admin"},
}

// bind applies the bindings as lodia:operator and, when that is forbidden
// (the first time, or after someone deleted its binding), with a
// system:masters certificate that is never stored. It returns the binding
// the tenant refused, empty when a failure is not a refusal; once
// lodia:operator is forbidden, every failure is one. Each of the two applies
// ends within bindTimeout, so that a tenant that answers nothing leaves a pass
// its status write inside its minute.
func (r *Reconciler) bind(ctx context.Context, t *tenant) (string, error) {
	kubeconfig := t.existing[t.secret(lodiav1alpha1.LodiaKubeconfigSecret)].Data[lodiav1alpha1.KubeconfigKey]
	operator, err := r.clientOf(client.ObjectKeyFromObject(t.cp), kubeconfig)
	if err != nil {
		return "", err
	}
	refused, err := applyBindings(ctx, operator)
	if !apierrors.IsForbidden(err) {
		if !apistatus.Refused(err) {
			refused = ""
		}
		return refused, err
	}
	ca := t.cas[clientCA]
	if ca == nil {
		return refused, errors.New("lodia:operator is forbidden and the client CA is missing or expired, so no bootstrap certificate can be issued")
	}
	bootstrap, err := ca.Issue(certs.Profile{CommonName: "lodia:bootstrap", Organizations: []string{"system:masters"}, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, t.now, bootstrapLifetime)
	if err != nil {
		return refused, err
	}
	cfg, err := tenantclient.Config(kubeconfig)
	if err != nil {
		return refused, err
	}
	asBootstrap := rest.AnonymousClientConfig(cfg)
	if asBootstrap.CertData, err = certs.CertPEM(bootstrap.Cert); err != nil {
		return refused, err
	}
	if asBootstrap.KeyData, err = certs.KeyPEM(bootstrap.Key); err != nil {
		return refused, err
	}
	once, err := tenantclient.New(asBootstrap, r.options())
	if err != nil {
		return refused, err
	}
	defer once.Close()
	if name, err := applyBindings(ctx, once); err != nil {
		if apistatus.Refused(err) {
			refused = name
		}
		return refused, err
	}
	return "", nil
}

func (r *Reconciler) clientOf(key types.NamespacedName, kubeconfig []byte) (*tenantclient.Client, error) {
	r.mu.Lock()
	if r.clients == nil {
		r.clients = tenantclient.NewClients(r.options())
	}
	clients := r.clients
	r.mu.Unlock()
	return clients.Of(key, kubeconfig)
}

func (r *Reconciler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.clients != nil {
		r.clients.Forget(key)
	}
}

func (r *Reconciler) options() tenantclient.Options {
	return tenantclient.Options{Connections: 1, Timeout: bindTimeout, Dial: r.dial}
}

// dial opens a connection through Dial when a test sets it, read at each dial.
func (r *Reconciler) dial(ctx context.Context, network, address string) (net.Conn, error) {
	if r.Dial != nil {
		return r.Dial(ctx, network, address)
	}
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

// applyBindings also returns the binding it stopped at.
func applyBindings(ctx context.Context, c *tenantclient.Client) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, bindTimeout)
	defer cancel()
	for _, b := range bindings {
		binding := rbacv1ac.ClusterRoleBinding(b.name).
			WithLabels(map[string]string{lodiav1alpha1.ManagedByLabel: lodiav1alpha1.ManagedBy}).
			WithSubjects(rbacv1ac.Subject().WithKind(b.kind).WithAPIGroup(rbacv1.GroupName).WithName(b.name)).
			WithRoleRef(rbacv1ac.RoleRef().WithAPIGroup(rbacv1.GroupName).WithKind("ClusterRole").WithName(b.role))
		body, err := json.Marshal(binding)
		if err != nil {
			return b.name, err
		}
		request := c.Patch(types.ApplyPatchType).Prefix("apis", rbacv1.GroupName, "v1").Resource("clusterrolebindings").Name(b.name).
			VersionedParams(&metav1.PatchOptions{FieldManager: fieldManager, Force: new(true)}, metav1.ParameterCodec).Body(body)
		if _, err := c.Do(ctx, request); err != nil {
			return b.name, fmt.Errorf("apply ClusterRoleBinding %s: %w", b.name, err)
		}
	}
	return "", nil
}
