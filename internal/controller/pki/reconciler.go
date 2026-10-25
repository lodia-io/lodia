// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package pki is the PKI controller.
package pki

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
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
	// bindingsPeriod is the Tenant controller's apply period.
	bindingsPeriod = 10 * time.Minute
)

// Each reason has one message, which depends on the state alone, so a pass
// that changes nothing writes nothing; the errors behind Issuing and Binding
// go to the log.
const (
	ReasonReconciling                   = "Reconciling"
	ReasonIssued                        = "Issued"
	ReasonIssuing                       = "Issuing"
	ReasonWaitingForEndpoint            = "WaitingForEndpoint"
	ReasonCAMissing                     = "CAMissing"
	ReasonCAExpired                     = "CAExpired"
	ReasonEncryptionProviderUnsupported = "EncryptionProviderUnsupported"
	ReasonValid                         = "Valid"
	ReasonCAExpiring                    = "CAExpiring"
	ReasonBound                         = "Bound"
	ReasonBinding                       = "Binding"
	ReasonWaitingForAPIServer           = "WaitingForAPIServer"
)

type Reconciler struct {
	client.Client
	// Reader reads Secrets whole; the manager's cache holds their metadata only.
	Reader client.Reader
	// The kubelet signer lives LeafLifetime, the cluster CA
	// TrustAnchorLifetime, every other CA CALifetime.
	LeafLifetime        time.Duration
	CALifetime          time.Duration
	TrustAnchorLifetime time.Duration
	// CAExpiryNotice is how long before a CA expires CAValidityAcceptable
	// turns False.
	CAExpiryNotice time.Duration
	// Dial, when set, opens every connection to a tenant's API server; tests
	// set it, since envtest has no DNS for the tenant's Service.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("pki").
		For(&bryggav1alpha1.ControlPlane{}).
		Owns(&corev1.Secret{}, builder.OnlyMetadata).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		Complete(r)
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &bryggav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cp.DeletionTimestamp != nil || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	now := time.Now()
	owned := []string{bryggav1alpha1.ConditionCertificatesReady, bryggav1alpha1.ConditionCAValidityAcceptable, bryggav1alpha1.ConditionAccessReady}
	if slices.ContainsFunc(owned, func(c string) bool { return meta.FindStatusCondition(cp.Status.Conditions, c) == nil }) {
		conditions := make([]metav1.Condition, 0, len(owned))
		for _, c := range owned {
			conditions = append(conditions, previous(cp, c))
		}
		if err := r.applyStatus(ctx, cp, now, nil, conditions...); err != nil {
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
	validity, read := t.caValidity(r.CAExpiryNotice)
	if !read {
		validity = previous(cp, bryggav1alpha1.ConditionCAValidityAcceptable)
	}
	access, bindErr := r.access(ctx, t)
	if err := r.applyStatus(ctx, cp, now, t.credentials(), certificates, validity, access); err != nil {
		return ctrl.Result{}, err
	}
	if err := errors.Join(issueErr, bindErr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: t.requeueAfter()}, nil
}

func previous(cp *bryggav1alpha1.ControlPlane, conditionType string) metav1.Condition {
	if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c != nil {
		return *c
	}
	return metav1.Condition{Type: conditionType, Status: metav1.ConditionUnknown, Reason: ReasonReconciling, Message: "the first pass has not finished"}
}

func missingCAMessage(cas []string) string {
	return "missing or unreadable CA key: " + strings.Join(cas, ", ") + "; restore it or rotate the CA, nothing it signs is written"
}

func (t *tenant) certificatesReady(err error) metav1.Condition {
	c := metav1.Condition{Type: bryggav1alpha1.ConditionCertificatesReady, Status: metav1.ConditionFalse}
	switch {
	case err != nil:
		c.Reason, c.Message = ReasonIssuing, "a Secret could not be written; the error is in the log"
	case len(t.missingCAs) > 0:
		c.Reason, c.Message = ReasonCAMissing, missingCAMessage(t.missingCAs)
	case len(t.expiredCAs) > 0:
		c.Reason, c.Message = ReasonCAExpired, "expired CA: "+strings.Join(t.expiredCAs, ", ")+"; rotate it, nothing it signs is written"
	case t.endpoint == nil:
		c.Reason, c.Message = ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint"
	case t.unsupportedProvider != "":
		c.Reason, c.Message = ReasonEncryptionProviderUnsupported, "apiServer.encryption.provider "+string(t.unsupportedProvider)+" is not supported yet"
	default:
		c.Status, c.Reason, c.Message = metav1.ConditionTrue, ReasonIssued, "every certificate verifies and is valid beyond its renewal time"
	}
	return c
}

// caValidity also reports whether every CA was read: a pass that failed
// before keeps the condition it had.
func (t *tenant) caValidity(notice time.Duration) (metav1.Condition, bool) {
	c := metav1.Condition{Type: bryggav1alpha1.ConditionCAValidityAcceptable, Status: metav1.ConditionTrue, Reason: ReasonValid, Message: "no CA expires within " + notice.String()}
	var expiring []string
	for _, ca := range caBundles {
		if cert := t.caCerts[ca.ca]; cert != nil && cert.NotAfter.Sub(t.now) <= notice {
			expiring = append(expiring, ca.ca+" CA at "+cert.NotAfter.UTC().Format(time.RFC3339))
		}
	}
	switch {
	case len(expiring) > 0:
		c.Status, c.Reason, c.Message = metav1.ConditionFalse, ReasonCAExpiring, "expiring within "+notice.String()+": "+strings.Join(expiring, ", ")+"; start its rotation"
	case len(t.missingCAs) > 0:
		c.Status, c.Reason, c.Message = metav1.ConditionUnknown, ReasonCAMissing, missingCAMessage(t.missingCAs)
	}
	return c, len(t.caCerts)+len(t.missingCAs) == len(caBundles)
}

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

// applyStatus writes only on a change. Nil credentials keep the
// ControlPlane's references, and a condition carried from an earlier pass
// keeps its observedGeneration: an apply that leaves out a field PKI owns
// removes it.
func (r *Reconciler) applyStatus(ctx context.Context, cp *bryggav1alpha1.ControlPlane, now time.Time, credentials map[string]any, conditions ...metav1.Condition) error {
	if credentials == nil {
		credentials = map[string]any{}
		if c := cp.Status.Credentials; c != nil {
			for field, ref := range map[string]*bryggav1alpha1.LocalReference{"adminKubeconfigSecretRef": c.AdminKubeconfigSecretRef, "caBundleSecretRef": c.CABundleSecretRef} {
				if ref != nil {
					credentials[field] = map[string]any{"name": ref.Name}
				}
			}
		}
	}
	changed := false
	for i := range conditions {
		c := &conditions[i]
		if c.ObservedGeneration == 0 {
			c.ObservedGeneration = cp.Generation
		}
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
		if (ref == nil) != (want == nil) || (ref != nil && ref.Name != want["name"]) {
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
