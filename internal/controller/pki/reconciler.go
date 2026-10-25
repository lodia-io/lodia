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
	"strings"
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

// The reasons of CertificatesReady and AccessReady. Each has one message,
// which depends on the state alone, so a pass that changes nothing writes
// nothing; the error behind Issuing and Binding is in the log.
const (
	ReasonReconciling                   = "Reconciling"
	ReasonIssued                        = "Issued"
	ReasonIssuing                       = "Issuing"
	ReasonWaitingForEndpoint            = "WaitingForEndpoint"
	ReasonCAMissing                     = "CAMissing"
	ReasonEncryptionProviderUnsupported = "EncryptionProviderUnsupported"
	ReasonBound                         = "Bound"
	ReasonBinding                       = "Binding"
	ReasonWaitingForAPIServer           = "WaitingForAPIServer"
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
	now := time.Now()
	if meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionCertificatesReady) == nil ||
		meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionAccessReady) == nil {
		if err := r.applyStatus(ctx, cp, now, nil, unknown(bryggav1alpha1.ConditionCertificatesReady), unknown(bryggav1alpha1.ConditionAccessReady)); err != nil {
			return ctrl.Result{}, err
		}
	}
	var secrets corev1.SecretList
	if err := r.Reader.List(ctx, &secrets, client.InNamespace(cp.Namespace), client.MatchingLabels{bryggav1alpha1.ControlPlaneLabel: cp.Name}); err != nil {
		return ctrl.Result{}, err
	}
	t := newTenant(cp, secrets.Items, now)

	issueErr := r.issue(ctx, t)
	certificates := t.certificatesReady(issueErr)
	access, bindErr := r.access(ctx, t)
	if err := r.applyStatus(ctx, cp, now, t.credentials(), certificates, access); err != nil {
		return ctrl.Result{}, err
	}
	if err := errors.Join(issueErr, bindErr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: t.requeueAfter()}, nil
}

func unknown(conditionType string) metav1.Condition {
	return metav1.Condition{Type: conditionType, Status: metav1.ConditionUnknown, Reason: ReasonReconciling, Message: "the first pass has not finished"}
}

func (t *tenant) certificatesReady(err error) metav1.Condition {
	c := metav1.Condition{Type: bryggav1alpha1.ConditionCertificatesReady, Status: metav1.ConditionFalse}
	switch {
	case err != nil:
		c.Reason, c.Message = ReasonIssuing, "a Secret could not be written; the error is in the log"
	case len(t.missingCAs) > 0:
		c.Reason, c.Message = ReasonCAMissing, "the "+strings.Join(t.missingCAs, ", ")+" CA has a bundle but no readable key: restore it or rotate the CA; nothing it signs is written"
	case t.endpoint == nil:
		c.Reason, c.Message = ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint"
	case t.unsupportedProvider != "":
		c.Reason, c.Message = ReasonEncryptionProviderUnsupported, "apiServer.encryption.provider "+string(t.unsupportedProvider)+" is not supported yet"
	default:
		c.Status, c.Reason, c.Message = metav1.ConditionTrue, ReasonIssued, "every certificate verifies and is valid beyond its renewal time"
	}
	return c
}

// access binds the tenant's identities once APIServerReady is True for the
// current generation; an older True counts as Unknown.
func (r *Reconciler) access(ctx context.Context, t *tenant) (metav1.Condition, error) {
	c := metav1.Condition{Type: bryggav1alpha1.ConditionAccessReady, Status: metav1.ConditionFalse, Reason: ReasonWaitingForAPIServer, Message: "APIServerReady is not True for this generation"}
	ready := meta.FindStatusCondition(t.cp.Status.Conditions, bryggav1alpha1.ConditionAPIServerReady)
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.ObservedGeneration != t.cp.Generation || t.existing[t.name("brygga-kubeconfig")] == nil {
		return c, nil
	}
	if err := r.bind(ctx, t); err != nil {
		c.Reason, c.Message = ReasonBinding, "the bindings could not be applied; the error is in the log"
		return c, err
	}
	c.Status, c.Reason, c.Message = metav1.ConditionTrue, ReasonBound, "brygga:operator, brygga:cluster-admins and brygga:kubelet-api-admins are bound"
	return c, nil
}

// applyStatus applies PKI's conditions and credential references through the
// status subresource, and only when one of them changed.
func (r *Reconciler) applyStatus(ctx context.Context, cp *bryggav1alpha1.ControlPlane, now time.Time, credentials map[string]any, conditions ...metav1.Condition) error {
	changed := false
	for i := range conditions {
		c := &conditions[i]
		c.ObservedGeneration = cp.Generation
		c.LastTransitionTime = metav1.NewTime(now)
		prev := meta.FindStatusCondition(cp.Status.Conditions, c.Type)
		if prev != nil && prev.Status == c.Status {
			c.LastTransitionTime = prev.LastTransitionTime
		}
		if prev == nil || prev.Status != c.Status || prev.Reason != c.Reason || prev.Message != c.Message || prev.ObservedGeneration != c.ObservedGeneration {
			changed = true
		}
	}
	prev := cp.Status.Credentials
	if prev == nil {
		prev = &bryggav1alpha1.CredentialsStatus{}
	}
	for field, ref := range map[string]*bryggav1alpha1.LocalReference{"adminKubeconfigSecretRef": prev.AdminKubeconfigSecretRef, "caBundleSecretRef": prev.CABundleSecretRef} {
		want, _ := credentials[field].(map[string]any)
		if credentials != nil && ((ref == nil) != (want == nil) || (ref != nil && ref.Name != want["name"])) {
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
	status := map[string]any{"conditions": list}
	if len(credentials) > 0 {
		status["credentials"] = credentials
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": bryggav1alpha1.GroupVersion.String(),
		"kind":       kind,
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     status,
	}}
	if err := r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply the status of %s: %w", cp.Name, err)
	}
	return nil
}
