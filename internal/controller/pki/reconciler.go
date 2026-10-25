// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package pki is the PKI controller: for every ControlPlane it keeps the CAs,
// leaf certificates, kubeconfigs, ServiceAccount key and encryption
// configuration of the tenant namespace in place, renews leaves at two thirds
// of their lifetime, and binds the tenant's three identities once its API
// server answers.
package pki

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
)

const (
	fieldManager = "brygga/pki"
	kind         = "ControlPlane"
	// bindingsPeriod is how often the tenant's bindings are checked at least:
	// the Tenant controller's apply period.
	bindingsPeriod = 10 * time.Minute
)

// The reasons of CertificatesReady and AccessReady.
const (
	ReasonIssued              = "Issued"
	ReasonIssuing             = "Issuing"
	ReasonWaitingForEndpoint  = "WaitingForEndpoint"
	ReasonBound               = "Bound"
	ReasonBinding             = "Binding"
	ReasonWaitingForAPIServer = "WaitingForAPIServer"
)

// Reconciler is the PKI controller.
type Reconciler struct {
	client.Client
	// Reader reads Secrets whole; the manager's cache holds their metadata only.
	Reader client.Reader
	// LeafLifetime is the lifetime of every leaf certificate and of the
	// kubelet signer, CALifetime of every CA but the cluster CA,
	// TrustAnchorLifetime of the cluster CA.
	LeafLifetime        time.Duration
	CALifetime          time.Duration
	TrustAnchorLifetime time.Duration
	// Dial, when set, opens every connection to a tenant's API server; tests
	// set it, since envtest has no DNS for the tenant's Service.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
}

// SetupWithManager registers the controller as pki.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("pki").
		For(&bryggav1alpha1.ControlPlane{}).
		Owns(&corev1.Secret{}, builder.OnlyMetadata).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10, ReconciliationTimeout: time.Minute}).
		Complete(r)
}

// Reconcile issues what the ControlPlane's namespace lacks, renews what is
// due, binds the tenant's identities once its API server is ready, and
// returns at the earliest renewal time or in ten minutes, whichever is first.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &bryggav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cp.DeletionTimestamp != nil || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	var secrets corev1.SecretList
	if err := r.Reader.List(ctx, &secrets, client.InNamespace(cp.Namespace), client.MatchingLabels{bryggav1alpha1.ControlPlaneLabel: cp.Name}); err != nil {
		return ctrl.Result{}, err
	}
	t := newTenant(cp, secrets.Items)

	issueErr := r.issue(ctx, t)
	certificates := metav1.Condition{Type: bryggav1alpha1.ConditionCertificatesReady, Status: metav1.ConditionTrue, Reason: ReasonIssued,
		Message: "every certificate verifies; the next renewal is at " + t.renewal.UTC().Format(time.RFC3339)}
	switch {
	case issueErr != nil:
		certificates.Status, certificates.Reason, certificates.Message = metav1.ConditionFalse, ReasonIssuing, issueErr.Error()
	case t.endpoint == nil:
		certificates.Status, certificates.Reason, certificates.Message = metav1.ConditionFalse, ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint"
	}

	var bindErr error
	access := metav1.Condition{Type: bryggav1alpha1.ConditionAccessReady, Status: metav1.ConditionFalse, Reason: ReasonWaitingForAPIServer, Message: "APIServerReady is not True"}
	if meta.IsStatusConditionTrue(cp.Status.Conditions, bryggav1alpha1.ConditionAPIServerReady) && t.existing[t.name("brygga-kubeconfig")] != nil {
		if bindErr = r.bind(ctx, t); bindErr != nil {
			access.Reason, access.Message = ReasonBinding, bindErr.Error()
		} else {
			access.Status, access.Reason, access.Message = metav1.ConditionTrue, ReasonBound, "brygga:operator, brygga:cluster-admins and brygga:kubelet-api-admins are bound"
		}
	}

	if err := r.writeStatus(ctx, t, certificates, access); err != nil {
		return ctrl.Result{}, err
	}
	if err := errors.Join(issueErr, bindErr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: t.requeueAfter()}, nil
}

// writeStatus applies PKI's conditions and credential references through the
// status subresource, and only when one of them changed.
func (r *Reconciler) writeStatus(ctx context.Context, t *tenant, conditions ...metav1.Condition) error {
	cp := t.cp
	changed := false
	for i := range conditions {
		c := &conditions[i]
		c.ObservedGeneration = cp.Generation
		c.LastTransitionTime = metav1.NewTime(t.now)
		prev := meta.FindStatusCondition(cp.Status.Conditions, c.Type)
		if prev != nil && prev.Status == c.Status {
			c.LastTransitionTime = prev.LastTransitionTime
		}
		if prev == nil || prev.Status != c.Status || prev.Reason != c.Reason || prev.Message != c.Message || prev.ObservedGeneration != c.ObservedGeneration {
			changed = true
		}
	}
	credentials := map[string]any{}
	prev := cp.Status.Credentials
	if prev == nil {
		prev = &bryggav1alpha1.CredentialsStatus{}
	}
	for _, ref := range []struct {
		field  string
		secret string
		prev   *bryggav1alpha1.LocalReference
	}{
		{"adminKubeconfigSecretRef", t.name("admin-kubeconfig"), prev.AdminKubeconfigSecretRef},
		{"caBundleSecretRef", t.name("ca-bundle"), prev.CABundleSecretRef},
	} {
		if t.existing[ref.secret] == nil {
			continue
		}
		credentials[ref.field] = map[string]any{"name": ref.secret}
		if ref.prev == nil || ref.prev.Name != ref.secret {
			changed = true
		}
	}
	if !changed {
		return nil
	}

	list := make([]any, 0, len(conditions))
	for i := range conditions {
		c, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&conditions[i])
		if err != nil {
			return err
		}
		list = append(list, c)
	}
	status := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": bryggav1alpha1.GroupVersion.String(),
		"kind":       kind,
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     map[string]any{"conditions": list, "credentials": credentials},
	}}
	if err := r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(status), client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply the status of %s: %w", cp.Name, err)
	}
	return nil
}
