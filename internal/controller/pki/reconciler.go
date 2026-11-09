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
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/live"
	"go.lodia.io/lodia/internal/restore"
	"go.lodia.io/lodia/internal/statusapply"
)

const (
	fieldManager = "lodia/pki"
	kind         = "ControlPlane"
	// bindingsPeriod is the Tenant controller's apply period.
	bindingsPeriod = 10 * time.Minute
)

type Reconciler struct {
	client.Client
	// Reader reads Secrets whole, since the manager's cache holds their
	// metadata only, and AccessReady when PKI waits or keeps it.
	Reader client.Reader
	// The kubelet signer and the request CA live LeafLifetime, the cluster CA
	// TrustAnchorLifetime, every other CA CALifetime.
	LeafLifetime        time.Duration
	CALifetime          time.Duration
	TrustAnchorLifetime time.Duration
	// CAExpiryNotice is how long before a CA expires CAValidityAcceptable
	// turns False.
	CAExpiryNotice time.Duration
	// SignerPropagation is how long the client CA bundle holds a new kubelet
	// signer, unchanged, before the controller manager gets it: the kubelet's
	// update of the mounted bundle and the API server's reload.
	SignerPropagation time.Duration
	// Clock is the time of a pass, which issues and verifies everything at it.
	Clock clock.PassiveClock
	// Dial, when set, opens every connection to a tenant's API server; tests
	// set it, since envtest has no DNS for the tenant's Service.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("pki").
		For(&lodiav1alpha1.ControlPlane{}).
		Owns(&corev1.Secret{}, builder.OnlyMetadata).
		// A Backup turns Completed only once its <backup>.encryption-config is
		// written, so its own events are the ones a restore waits for.
		Watches(&lodiav1alpha1.Backup{}, handler.EnqueueRequestsFromMapFunc(restore.RestoresOf(r.Client))).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		Complete(r)
}

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/finalizers,verbs=update
// +kubebuilder:rbac:groups=lodia.io,resources=backups,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;patch;delete

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cp.DeletionTimestamp != nil || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	now := r.Clock.Now()
	owned := []string{lodiav1alpha1.ConditionCertificatesReady, lodiav1alpha1.ConditionCAValidityAcceptable, lodiav1alpha1.ConditionAccessReady}
	if slices.ContainsFunc(owned, func(c string) bool { return meta.FindStatusCondition(cp.Status.Conditions, c) == nil }) {
		conditions := make([]metav1.Condition, 0, len(owned))
		for _, c := range owned {
			conditions = append(conditions, previous(cp, c))
		}
		if err := r.applyStatus(ctx, cp, now, credentialRefs(cp.Status.Credentials), conditions...); err != nil {
			return ctrl.Result{}, err
		}
	}
	t := newTenant(cp, now)
	if err := r.read(ctx, t); err != nil {
		return ctrl.Result{}, err
	}
	if len(t.foreign) > 0 || len(t.unadopted) > 0 {
		access, bindErr := r.access(ctx, t)
		if access.Type == "" {
			return ctrl.Result{}, bindErr
		}
		blocked := t.controlledElsewhere()
		if len(t.foreign) == 0 {
			blocked = t.secretsExist()
		}
		if err := r.applyStatus(ctx, cp, now, credentialRefs(cp.Status.Credentials), blocked, previous(cp, lodiav1alpha1.ConditionCAValidityAcceptable), access); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: t.foreignRequeue()}, bindErr
	}

	if err := r.backupKeys(ctx, t); err != nil {
		return ctrl.Result{}, err
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
	if access.Type == "" {
		return ctrl.Result{}, errors.Join(t.writeErr, bindErr)
	}
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
	return statusapply.FirstPass(cp, conditionType)
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
	case len(t.pending) > 0 || t.signerLost:
		var issuing []string
		if len(t.pending) > 0 {
			issuing = append(issuing, "not written yet: "+listed(t.pending)+"; the error is in the log")
		}
		if t.signerLost {
			issuing = append(issuing, t.stagedSigner())
		}
		c.Reason, c.Message = lodiav1alpha1.ReasonIssuing, strings.Join(issuing, "; ")
	case t.endpoint == nil || t.cp.Spec.Endpoint.Gateway != nil && t.tunnelHost == "":
		c.Reason, c.Message = lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint"
	case t.waitingForBackup != "":
		c.Reason, c.Message = lodiav1alpha1.ReasonWaitingForBackup, t.waitingForBackup
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
	bundles := caBundles(t.cp)
	for _, ca := range bundles {
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
	return c, len(t.caCerts)+len(t.missingCAs) == len(bundles)
}

// stagedSigner names the time a signer replacing one that cannot sign is
// written, once its pass has found it in the client CA bundle.
func (t *tenant) stagedSigner() string {
	if t.promotion.IsZero() {
		return t.secret(lodiav1alpha1.KubeletSignerSecret) + " holds no usable signer; its successor is written once " + t.secret(lodiav1alpha1.ClientCASecret) + " trusts it"
	}
	return t.secret(lodiav1alpha1.KubeletSignerSecret) + " holds no usable signer; its successor is written at " + t.promotion.UTC().Format(time.RFC3339)
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

func (t *tenant) secretsExist() metav1.Condition {
	return metav1.Condition{
		Type:    lodiav1alpha1.ConditionCertificatesReady,
		Status:  metav1.ConditionFalse,
		Reason:  lodiav1alpha1.ReasonSecretsExist,
		Message: "a restored ControlPlane adopts no Secret it does not control: " + listed(t.unadopted) + "; nothing is written until they are deleted",
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
// Binding. While it waits it keeps AccessReady at an older generation. What it
// keeps is AccessReady as the API server holds it; a failed read of it returns
// an empty condition, and the pass returns before its status apply.
func (r *Reconciler) access(ctx context.Context, t *tenant) (metav1.Condition, error) {
	c := metav1.Condition{Type: lodiav1alpha1.ConditionAccessReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForAPIServer, Message: "APIServerReady is not True for this generation"}
	if !statusapply.TrueAt(t.cp, lodiav1alpha1.ConditionAPIServerReady) || t.existing[t.secret(lodiav1alpha1.LodiaKubeconfigSecret)] == nil {
		if err := live.Conditions(ctx, r.Reader, t.cp, c.Type); err != nil {
			return metav1.Condition{}, err
		}
		return statusapply.Wait(t.cp, meta.FindStatusCondition(t.cp.Status.Conditions, c.Type), c), nil
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
	if readErr := live.Conditions(ctx, r.Reader, t.cp, c.Type); readErr != nil {
		return metav1.Condition{}, errors.Join(err, readErr)
	}
	if prev := meta.FindStatusCondition(t.cp.Status.Conditions, c.Type); prev != nil && prev.Status == metav1.ConditionTrue {
		return *prev, err
	}
	c.Reason, c.Message = lodiav1alpha1.ReasonBinding, "the tenant has not confirmed the bindings since its API server became ready; the error is in the log"
	return c, err
}

// applyStatus writes only on a change, always with the credentials it is
// given, since an apply that leaves out a field PKI owns removes it. A
// condition carried from an earlier pass keeps its observedGeneration, and
// now is its transition time when its status changes.
func (r *Reconciler) applyStatus(ctx context.Context, cp *lodiav1alpha1.ControlPlane, now time.Time, credentials *lodiav1alpha1.CredentialsStatus, conditions ...metav1.Condition) error {
	for i := range conditions {
		if conditions[i].ObservedGeneration == 0 {
			conditions[i].ObservedGeneration = cp.Generation
		}
		conditions[i].LastTransitionTime = metav1.NewTime(now)
	}
	changed := !equality.Semantic.DeepEqual(credentialRefs(cp.Status.Credentials), credentials)
	if err := statusapply.Apply(ctx, r.Client, r.Reader, fieldManager, cp, lodiav1alpha1.ControlPlaneStatus{Conditions: conditions, Credentials: credentials}, changed); err != nil {
		return fmt.Errorf("apply the status of %s: %w", cp.Name, err)
	}
	return nil
}

// credentialRefs is the part of status.credentials PKI writes, nil when it
// names nothing.
func credentialRefs(c *lodiav1alpha1.CredentialsStatus) *lodiav1alpha1.CredentialsStatus {
	if c == nil || c.AdminKubeconfigSecretRef == nil && c.CABundleSecretRef == nil {
		return nil
	}
	return &lodiav1alpha1.CredentialsStatus{AdminKubeconfigSecretRef: c.AdminKubeconfigSecretRef, CABundleSecretRef: c.CABundleSecretRef}
}
