// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package pki is the PKI controller.
package pki

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
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

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	fieldManager = "lodia/pki"
	kind         = "ControlPlane"
	// bindingsPeriod is the Tenant controller's apply period.
	bindingsPeriod = 10 * time.Minute
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
		For(&lodiav1alpha1.ControlPlane{}).
		Owns(&corev1.Secret{}, builder.OnlyMetadata).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		Complete(r)
}

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=list;watch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;patch;delete

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cp.DeletionTimestamp != nil || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	now := time.Now()
	owned := []string{lodiav1alpha1.ConditionCertificatesReady, lodiav1alpha1.ConditionCAValidityAcceptable, lodiav1alpha1.ConditionAccessReady}
	if slices.ContainsFunc(owned, func(c string) bool { return meta.FindStatusCondition(cp.Status.Conditions, c) == nil }) {
		conditions := make([]metav1.Condition, 0, len(owned))
		for _, c := range owned {
			conditions = append(conditions, previous(cp, c))
		}
		if err := r.applyStatus(ctx, cp, now, nil, conditions...); err != nil {
			return ctrl.Result{}, err
		}
	}
	t := newTenant(cp, now)
	if err := r.read(ctx, t); err != nil {
		return ctrl.Result{}, err
	}
	if len(t.foreign) > 0 {
		access, bindErr := r.access(ctx, t)
		if err := r.applyStatus(ctx, cp, now, nil, t.controlledElsewhere(), previous(cp, lodiav1alpha1.ConditionCAValidityAcceptable), access); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: t.foreignRequeue()}, bindErr
	}

	if err := r.issue(ctx, t); err != nil {
		return ctrl.Result{}, err
	}
	certificates := t.certificatesReady()
	validity, read := t.caValidity(r.CAExpiryNotice)
	if !read {
		validity = previous(cp, lodiav1alpha1.ConditionCAValidityAcceptable)
	}
	access, bindErr := r.access(ctx, t)
	if err := r.applyStatus(ctx, cp, now, t.credentials(), certificates, validity, access); err != nil {
		return ctrl.Result{}, err
	}
	if err := errors.Join(t.writeErr, bindErr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: t.requeueAfter()}, nil
}

func previous(cp *lodiav1alpha1.ControlPlane, conditionType string) metav1.Condition {
	if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c != nil {
		return *c
	}
	return metav1.Condition{Type: conditionType, Status: metav1.ConditionUnknown, Reason: lodiav1alpha1.ReasonReconciling, Message: "the first pass has not finished"}
}

func missingCAMessage(cas []string) string {
	return "missing or unreadable CA key: " + strings.Join(cas, ", ") + "; restore it or rotate the CA, nothing it signs is written"
}

// certificatesReady reads the state the pass leaves: a write that failed
// counts as what it would have changed, never as the error.
func (t *tenant) certificatesReady() metav1.Condition {
	c := metav1.Condition{Type: lodiav1alpha1.ConditionCertificatesReady, Status: metav1.ConditionFalse}
	switch {
	case len(t.missingCAs) > 0:
		c.Reason, c.Message = lodiav1alpha1.ReasonCAMissing, missingCAMessage(t.missingCAs)
	case len(t.expiredCAs) > 0:
		c.Reason, c.Message = lodiav1alpha1.ReasonCAExpired, "expired CA: "+strings.Join(t.expiredCAs, ", ")+"; rotate it, nothing it signs is written"
	case len(t.unreadable) > 0:
		c.Reason, c.Message = lodiav1alpha1.ReasonSecretUnreadable, "unreadable: "+strings.Join(t.unreadable, ", ")+"; restore it, PKI never replaces a key it cannot read"
	case len(t.pending) > 0:
		c.Reason, c.Message = lodiav1alpha1.ReasonIssuing, "not written yet: "+listed(t.pending)+"; the error is in the log"
	case t.endpoint == nil:
		c.Reason, c.Message = lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint"
	case t.unsupportedProvider != "":
		c.Reason, c.Message = lodiav1alpha1.ReasonUnsupported, "spec.apiServer.encryption.provider "+string(t.unsupportedProvider)+" is not supported yet"
	default:
		c.Status, c.Reason, c.Message = metav1.ConditionTrue, lodiav1alpha1.ReasonIssued, "every certificate verifies and is valid beyond its renewal time"
	}
	return c
}

// caValidity also reports whether every CA was read: a pass that failed
// before keeps the condition it had.
func (t *tenant) caValidity(notice time.Duration) (metav1.Condition, bool) {
	c := metav1.Condition{Type: lodiav1alpha1.ConditionCAValidityAcceptable, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonValid, Message: "no CA expires within " + notice.String()}
	var expiring []string
	for _, ca := range caBundles {
		if cert := t.caCerts[ca.ca]; cert != nil && cert.NotAfter.Sub(t.now) <= notice {
			expiring = append(expiring, ca.ca+" CA at "+cert.NotAfter.UTC().Format(time.RFC3339))
		}
	}
	switch {
	case len(expiring) > 0:
		c.Status, c.Reason, c.Message = metav1.ConditionFalse, lodiav1alpha1.ReasonCAExpiring, "expiring within "+notice.String()+": "+strings.Join(expiring, ", ")+"; start its rotation"
	case len(t.missingCAs) > 0:
		c.Status, c.Reason, c.Message = metav1.ConditionUnknown, lodiav1alpha1.ReasonCAMissing, missingCAMessage(t.missingCAs)
	}
	return c, len(t.caCerts)+len(t.missingCAs) == len(caBundles)
}

func (t *tenant) controlledElsewhere() metav1.Condition {
	held := make([]string, 0, len(t.foreign))
	for _, f := range t.foreign {
		held = append(held, fmt.Sprintf("%s by %s %s (UID %s)", f.name, f.owner.Kind, f.owner.Name, f.owner.UID))
	}
	return metav1.Condition{
		Type:    lodiav1alpha1.ConditionCertificatesReady,
		Status:  metav1.ConditionFalse,
		Reason:  lodiav1alpha1.ReasonSecretsControlledElsewhere,
		Message: "another owner keeps " + listed(held) + "; nothing is written until they are gone: the garbage collector deletes a previous ControlPlane's, any other owner's need a person",
	}
}

func listed(items []string) string {
	items = slices.Sorted(slices.Values(items))
	if len(items) <= 5 {
		return strings.Join(items, ", ")
	}
	return strings.Join(items[:5], ", ") + " and " + strconv.Itoa(len(items)-5) + " more"
}

// access counts a refusal from the tenant as an answer, which turns
// AccessReady False, Refused; no answer keeps a True, since whether the
// tenant can be reached is Health's to report, and leaves anything else
// Binding.
func (r *Reconciler) access(ctx context.Context, t *tenant) (metav1.Condition, error) {
	c := metav1.Condition{Type: lodiav1alpha1.ConditionAccessReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForAPIServer, Message: "APIServerReady is not True for this generation"}
	prev := meta.FindStatusCondition(t.cp.Status.Conditions, lodiav1alpha1.ConditionAccessReady)
	ready := meta.FindStatusCondition(t.cp.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
	if ready == nil || ready.Status != metav1.ConditionTrue || ready.ObservedGeneration != t.cp.Generation || t.existing[t.secret(lodiav1alpha1.LodiaKubeconfigSecret)] == nil {
		if prev != nil && prev.ObservedGeneration < t.cp.Generation {
			return *prev, nil
		}
		return c, nil
	}
	refused, err := r.bind(ctx, t)
	if err == nil {
		c.Status, c.Reason, c.Message = metav1.ConditionTrue, lodiav1alpha1.ReasonBound, "lodia:operator, lodia:cluster-admins and lodia:kubelet-api-admins are bound"
		return c, nil
	}
	if refused != "" {
		c.Reason, c.Message = lodiav1alpha1.ReasonRefused, "ClusterRoleBinding "+refused+" is refused by the tenant's API server; the reason is in Lodia's log"
		return c, err
	}
	if prev != nil && prev.Status == metav1.ConditionTrue {
		return *prev, err
	}
	c.Reason, c.Message = lodiav1alpha1.ReasonBinding, "the tenant has not confirmed the bindings since its API server became ready; the error is in the log"
	return c, err
}

// applyStatus writes only on a change. Nil credentials keep the
// ControlPlane's references, and a condition carried from an earlier pass
// keeps its observedGeneration: an apply that leaves out a field PKI owns
// removes it. One older than cp's generation is applied only over the
// resourceVersion cp was read at, so a stale cache conflicts instead of
// writing it back.
func (r *Reconciler) applyStatus(ctx context.Context, cp *lodiav1alpha1.ControlPlane, now time.Time, credentials map[string]any, conditions ...metav1.Condition) error {
	if credentials == nil {
		credentials = map[string]any{}
		if c := cp.Status.Credentials; c != nil {
			for field, ref := range map[string]*lodiav1alpha1.LocalReference{"adminKubeconfigSecretRef": c.AdminKubeconfigSecretRef, "caBundleSecretRef": c.CABundleSecretRef} {
				if ref != nil {
					credentials[field] = map[string]any{"name": ref.Name}
				}
			}
		}
	}
	changed, kept := false, false
	for i := range conditions {
		c := &conditions[i]
		if c.ObservedGeneration == 0 {
			c.ObservedGeneration = cp.Generation
		}
		kept = kept || c.ObservedGeneration < cp.Generation
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
		prev = &lodiav1alpha1.CredentialsStatus{}
	}
	for field, ref := range map[string]*lodiav1alpha1.LocalReference{"adminKubeconfigSecretRef": prev.AdminKubeconfigSecretRef, "caBundleSecretRef": prev.CABundleSecretRef} {
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
	metadata := map[string]any{"name": cp.Name, "namespace": cp.Namespace}
	if kept {
		metadata["resourceVersion"] = cp.ResourceVersion
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       kind,
		"metadata":   metadata,
		"status":     status,
	}}
	if err := r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(fieldManager), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply the status of %s: %w", cp.Name, err)
	}
	return nil
}
