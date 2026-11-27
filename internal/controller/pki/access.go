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

// refusal is a binding the tenant refused, with its answer.
type refusal struct {
	name string
	err  error
}

// bind applies the bindings as lodia:operator and, when that is forbidden
// (the first time, or after someone deleted its binding), with a
// system:masters certificate that is never stored. It returns the bindings
// the tenant refused, past each of which it applies the rest, with every
// failure joined; once lodia:operator is forbidden, a certificate that cannot
// be made or whose apply gets no answer leaves its refusals. Each of the two
// applies ends within bindTimeout, so that a tenant that answers nothing
// leaves a pass its status write inside its minute.
func (r *Reconciler) bind(ctx context.Context, t *tenant) ([]refusal, error) {
	kubeconfig := t.existing[t.secret(lodiav1alpha1.LodiaKubeconfigSecret)].Data[lodiav1alpha1.KubeconfigKey]
	operator, err := r.clientOf(client.ObjectKeyFromObject(t.cp), kubeconfig)
	if err != nil {
		return nil, err
	}
	refused, err := applyBindings(ctx, operator, true)
	if len(refused) == 0 || !apierrors.IsForbidden(refused[len(refused)-1].err) {
		return refused, err
	}
	ca := t.cas[clientCA]
	if ca == nil {
		return refused, errors.Join(err, errors.New("lodia:operator is forbidden and the client CA is missing or expired, so no bootstrap certificate can be issued"))
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
	again, err := applyBindings(ctx, once, false)
	if err != nil && len(again) == 0 {
		return refused, err
	}
	return again, err
}

// held says whether lodia:operator reads the binding name in the tenant,
// which a refused update leaves and a refused creation does not, and which
// lodia:operator reads only while its own binding grants it: no admission
// policy or webhook judges a read. The second result says whether the tenant
// answered: a missing binding and a refused read are answers.
func (r *Reconciler) held(ctx context.Context, t *tenant, name string) (bool, bool, error) {
	operator, err := r.clientOf(client.ObjectKeyFromObject(t.cp), t.existing[t.secret(lodiav1alpha1.LodiaKubeconfigSecret)].Data[lodiav1alpha1.KubeconfigKey])
	if err != nil {
		return false, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, bindTimeout)
	defer cancel()
	if _, err := operator.Do(ctx, operator.Get().Prefix("apis", rbacv1.GroupName, "v1").Resource("clusterrolebindings").Name(name)); err != nil {
		return false, apierrors.IsNotFound(err) || apistatus.Refused(err), fmt.Errorf("read ClusterRoleBinding %s: %w", name, err)
	}
	return true, true, nil
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

// applyBindings applies every binding, past each one the tenant refuses, and
// returns those it refused with every failure joined; it stops at a failure
// that is no refusal and, while forbiddenStops, at a Forbidden, which
// lodia:operator gets for every binding once its own is gone.
func applyBindings(ctx context.Context, c *tenantclient.Client, forbiddenStops bool) ([]refusal, error) {
	ctx, cancel := context.WithTimeout(ctx, bindTimeout)
	defer cancel()
	var refused []refusal
	var failures error
	for _, b := range bindings {
		binding := rbacv1ac.ClusterRoleBinding(b.name).
			WithLabels(map[string]string{lodiav1alpha1.ManagedByLabel: lodiav1alpha1.ManagedBy}).
			WithSubjects(rbacv1ac.Subject().WithKind(b.kind).WithAPIGroup(rbacv1.GroupName).WithName(b.name)).
			WithRoleRef(rbacv1ac.RoleRef().WithAPIGroup(rbacv1.GroupName).WithKind("ClusterRole").WithName(b.role))
		body, err := json.Marshal(binding)
		if err != nil {
			return refused, errors.Join(failures, err)
		}
		request := c.Patch(types.ApplyPatchType).Prefix("apis", rbacv1.GroupName, "v1").Resource("clusterrolebindings").Name(b.name).
			VersionedParams(&metav1.PatchOptions{FieldManager: fieldManager, Force: new(true)}, metav1.ParameterCodec).Body(body)
		if _, err := c.Do(ctx, request); err != nil {
			err = fmt.Errorf("apply ClusterRoleBinding %s: %w", b.name, err)
			failures = errors.Join(failures, err)
			if !apistatus.Refused(err) {
				return refused, failures
			}
			refused = append(refused, refusal{b.name, err})
			if forbiddenStops && apierrors.IsForbidden(err) {
				return refused, failures
			}
		}
	}
	return refused, failures
}
