// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
)

const (
	kubeconfigRequestFieldManager = "lodia/kubeconfigrequest"
	controlPlaneRefField          = "spec.controlPlaneRef.name"

	// userPrefix comes before spec.user in a request's certificate, which
	// keeps every requested name apart from Kubernetes' and Lodia's own.
	userPrefix = "lodia:user:"
)

// KubeconfigRequestReconciler answers KubeconfigRequests with certificates
// from the request CA, and deletes each at its deadline.
type KubeconfigRequestReconciler struct {
	client.Client
	// Reader reads the request CA, the client and cluster CA bundles and the
	// admission policy as the API server holds them.
	Reader client.Reader
	Clock  clock.PassiveClock
	// PolicyGrace is how long after the admission policy and its binding a
	// request must have been created: each API server enforces a binding only
	// once it has loaded it.
	PolicyGrace time.Duration
}

func (r *KubeconfigRequestReconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &lodiav1alpha1.KubeconfigRequest{}, controlPlaneRefField, func(obj client.Object) []string {
		return []string{obj.(*lodiav1alpha1.KubeconfigRequest).Spec.ControlPlaneRef.Name}
	}); err != nil {
		return err
	}
	inputs := predicate.NewPredicateFuncs(func(secret client.Object) bool {
		cp, ok := secret.GetLabels()[lodiav1alpha1.ControlPlaneLabel]
		return ok && slices.Contains([]string{cp + "-cakey-" + requestCA, lodiav1alpha1.ClientCASecret.Name(cp), lodiav1alpha1.CABundleSecret.Name(cp)}, secret.GetName())
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named("kubeconfigrequest").
		For(&lodiav1alpha1.KubeconfigRequest{}).
		Watches(&lodiav1alpha1.ControlPlane{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, cp client.Object) []reconcile.Request {
			return r.requestsOf(ctx, cp.GetNamespace(), cp.GetName())
		})).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, secret client.Object) []reconcile.Request {
			return r.requestsOf(ctx, secret.GetNamespace(), secret.GetLabels()[lodiav1alpha1.ControlPlaneLabel])
		}), builder.WithPredicates(inputs)).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		Complete(r)
}

func (r *KubeconfigRequestReconciler) requestsOf(ctx context.Context, namespace, controlPlane string) []reconcile.Request {
	var list lodiav1alpha1.KubeconfigRequestList
	if err := r.List(ctx, &list, client.InNamespace(namespace), client.MatchingFields{controlPlaneRefField: controlPlane}); err != nil {
		logf.FromContext(ctx).Error(err, "list the KubeconfigRequests of a ControlPlane", "controlPlane", controlPlane)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
	}
	return requests
}

// +kubebuilder:rbac:groups=lodia.io,resources=kubeconfigrequests,verbs=list;watch;delete
// +kubebuilder:rbac:groups=lodia.io,resources=kubeconfigrequests/status,verbs=patch
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=validatingadmissionpolicies;validatingadmissionpolicybindings,verbs=get,resourceNames=kubeconfigrequests.lodia.io

// Reconcile answers a request once, from the state at its pass: Issued and
// the two final reasons are never written again, and the status write
// carries the resourceVersion the pass read, so a pass on a stale cache
// conflicts instead of issuing a second certificate.
func (r *KubeconfigRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	kr := &lodiav1alpha1.KubeconfigRequest{}
	if err := r.Get(ctx, req.NamespacedName, kr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	now := r.Clock.Now()
	deadline := kr.CreationTimestamp.Add(time.Duration(kr.Spec.ExpirationSeconds) * time.Second)
	if !now.Before(deadline) {
		err := r.Delete(ctx, kr, client.Preconditions{UID: &kr.UID})
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	wait := ctrl.Result{RequeueAfter: (deadline.Sub(now) + time.Second - 1).Truncate(time.Second)}
	if ready := meta.FindStatusCondition(kr.Status.Conditions, lodiav1alpha1.ConditionReady); ready != nil &&
		(ready.Status == metav1.ConditionTrue || ready.Reason == lodiav1alpha1.ReasonRequestInvalid || ready.Reason == lodiav1alpha1.ReasonPolicyNotEnforced) {
		return wait, nil
	}

	notReady := func(reason, message string) (ctrl.Result, error) {
		condition := metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: reason, Message: message}
		if err := r.applyStatus(ctx, kr, now, condition, nil, nil); err != nil {
			return ctrl.Result{}, err
		}
		return wait, nil
	}
	if why, err := r.unenforced(ctx, kr); err != nil {
		return ctrl.Result{}, err
	} else if why != "" {
		return notReady(lodiav1alpha1.ReasonPolicyNotEnforced, "the admission policy "+lodiav1alpha1.KubeconfigRequestPolicy+" was not enforced when this request was created: "+why+"; nothing is issued for it, create another once the policy is in place")
	}
	csr, why := parseRequest(kr.Spec.Request)
	if why != "" {
		return notReady(lodiav1alpha1.ReasonRequestInvalid, "spec.request "+why+"; create another request")
	}
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: kr.Namespace, Name: kr.Spec.ControlPlaneRef.Name}, cp); apierrors.IsNotFound(err) {
		return notReady(lodiav1alpha1.ReasonControlPlaneNotFound, "no ControlPlane "+kr.Spec.ControlPlaneRef.Name+" in this namespace")
	} else if err != nil {
		return ctrl.Result{}, err
	}
	if cp.Status.Endpoint == nil {
		return notReady(lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint of ControlPlane "+cp.Name)
	}
	ca, err := r.input(ctx, cp, cp.Name+"-cakey-"+requestCA)
	if err != nil {
		return ctrl.Result{}, err
	}
	signer := readRequestCA(ca)
	trusted, err := r.input(ctx, cp, lodiav1alpha1.ClientCASecret.Name(cp.Name))
	if err != nil {
		return ctrl.Result{}, err
	}
	bundle, err := r.input(ctx, cp, lodiav1alpha1.CABundleSecret.Name(cp.Name))
	if err != nil {
		return ctrl.Result{}, err
	}
	if signer == nil || !now.Before(signer.Cert.NotAfter) || !trusts(trusted, signer.Cert) || len(data(bundle, lodiav1alpha1.CACertKey)) == 0 {
		return notReady(lodiav1alpha1.ReasonWaitingForCertificates, "waiting for PKI to write the request CA "+cp.Name+"-cakey-"+requestCA+", the client CA bundle "+lodiav1alpha1.ClientCASecret.Name(cp.Name)+" that trusts it, and the cluster CA bundle "+lodiav1alpha1.CABundleSecret.Name(cp.Name))
	}

	var groups []string
	if kr.Spec.Access == lodiav1alpha1.KubeconfigAccessClusterAdmin {
		groups = []string{clusterAdmins}
	}
	user := userPrefix + kr.Spec.User
	cert, err := signer.Sign(certs.Profile{CommonName: user, Organizations: groups, Usages: clientAuth}, csr.PublicKey, now, deadline.Sub(now))
	if err != nil {
		return ctrl.Result{}, err
	}
	kubeconfig, err := requestKubeconfig(cp.Name, &endpoint{host: cp.Status.Endpoint.APIServer.Host, port: cp.Status.Endpoint.APIServer.Port}, bundle.Data[lodiav1alpha1.CACertKey], user, cert)
	if err != nil {
		return ctrl.Result{}, err
	}
	condition := metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonIssued, Message: "status.kubeconfig holds a certificate for " + user + " until " + cert.NotAfter.UTC().Format(time.RFC3339)}
	fingerprint := sha256.Sum256(cert.Raw)
	credential := "X509SHA256=" + hex.EncodeToString(fingerprint[:])
	if err := r.applyStatus(ctx, kr, now, condition, kubeconfig, &cert.NotAfter); err != nil {
		// An API server's 4xx answer, a conflict included, stored nothing.
		var status apierrors.APIStatus
		if errors.As(err, &status) && status.Status().Code >= 400 && status.Status().Code < 500 {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, fmt.Errorf("%w; the write may have stored credential-id %s", err, credential)
	}
	logf.FromContext(ctx).Info("issued a certificate", "uid", kr.UID, "user", user, "access", kr.Spec.Access,
		"credential-id", credential, "notAfter", cert.NotAfter.UTC().Format(time.RFC3339))
	return wait, nil
}

// unenforced says why the admission policy may not have checked the request,
// empty when it did.
func (r *KubeconfigRequestReconciler) unenforced(ctx context.Context, kr *lodiav1alpha1.KubeconfigRequest) (string, error) {
	key := client.ObjectKey{Name: lodiav1alpha1.KubeconfigRequestPolicy}
	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{}
	if err := r.Reader.Get(ctx, key, policy); apierrors.IsNotFound(err) {
		return "the ValidatingAdmissionPolicy does not exist", nil
	} else if err != nil {
		return "", err
	}
	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{}
	if err := r.Reader.Get(ctx, key, binding); apierrors.IsNotFound(err) {
		return "its binding does not exist", nil
	} else if err != nil {
		return "", err
	}
	created := kr.CreationTimestamp.Time
	switch {
	case binding.Spec.PolicyName != policy.Name:
		return "its binding names the policy " + strconv.Quote(binding.Spec.PolicyName), nil
	case !slices.Contains(binding.Spec.ValidationActions, admissionregistrationv1.Deny):
		return "its binding does not Deny", nil
	case binding.Spec.MatchResources != nil:
		return "its binding narrows matchResources", nil
	case binding.Spec.ParamRef != nil:
		return "its binding has a paramRef", nil
	case created.Before(policy.CreationTimestamp.Add(r.PolicyGrace)):
		return "the request was created less than " + r.PolicyGrace.String() + " after the policy", nil
	case created.Before(binding.CreationTimestamp.Add(r.PolicyGrace)):
		return "the request was created less than " + r.PolicyGrace.String() + " after its binding", nil
	}
	return "", nil
}

// parseRequest returns the request, or why it is refused.
func parseRequest(request []byte) (*x509.CertificateRequest, string) {
	block, _ := pem.Decode(request)
	if block == nil {
		return nil, "is not a PEM certificate request"
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, "does not parse: " + err.Error()
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, "is not signed by the key it carries: " + err.Error()
	}
	switch key := csr.PublicKey.(type) {
	case *rsa.PublicKey:
		// crypto/tls refuses a client certificate of more than 8192 bits
		// (go1.26.10 src/crypto/tls/handshake_server.go#L955-L960).
		if bits := key.N.BitLen(); bits < 2048 || bits > 8192 {
			return nil, "carries an RSA key of " + strconv.Itoa(bits) + " bits, not 2048 to 8192"
		}
	case *ecdsa.PublicKey:
		if key.Curve != elliptic.P256() && key.Curve != elliptic.P384() && key.Curve != elliptic.P521() {
			return nil, "carries an ECDSA key on " + key.Curve.Params().Name + ", not P-256, P-384 or P-521"
		}
	case ed25519.PublicKey:
	default:
		return nil, fmt.Sprintf("carries a %T, not an RSA, ECDSA or Ed25519 key", key)
	}
	return csr, ""
}

// input reads one of PKI's Secrets live, nil unless this ControlPlane
// controls it: a predecessor's of the same name holds other CAs.
func (r *KubeconfigRequestReconciler) input(ctx context.Context, cp *lodiav1alpha1.ControlPlane, name string) (*corev1.Secret, error) {
	s := &corev1.Secret{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, s); apierrors.IsNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(s, cp) {
		return nil, nil
	}
	return s, nil
}

// trusts reports whether the client CA bundle holds ca, so that no certificate
// is issued that the API server cannot verify.
func trusts(bundle *corev1.Secret, ca *x509.Certificate) bool {
	list, err := certs.ParseCerts(data(bundle, lodiav1alpha1.CACertKey))
	return err == nil && slices.ContainsFunc(list, ca.Equal)
}

// requestKubeconfig names the user as the certificate does, and has no key:
// its holder adds theirs.
func requestKubeconfig(controlPlane string, e *endpoint, caBundle []byte, user string, cert *x509.Certificate) ([]byte, error) {
	certPEM, err := certs.CertPEM(cert)
	if err != nil {
		return nil, err
	}
	return certs.Kubeconfig(controlPlane, e.url(), user, caBundle, certPEM, nil, true)
}

// applyStatus writes only on a change of Ready, and always with the
// resourceVersion the pass read.
func (r *KubeconfigRequestReconciler) applyStatus(ctx context.Context, kr *lodiav1alpha1.KubeconfigRequest, now time.Time, c metav1.Condition, kubeconfig []byte, expiration *time.Time) error {
	c.ObservedGeneration = kr.Generation
	c.LastTransitionTime = metav1.NewTime(now)
	prev := meta.FindStatusCondition(kr.Status.Conditions, c.Type)
	if prev != nil && prev.Status == c.Status {
		if prev.Reason == c.Reason && prev.Message == c.Message && prev.ObservedGeneration == c.ObservedGeneration {
			return nil
		}
		c.LastTransitionTime = prev.LastTransitionTime
	}
	condition, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&c)
	if err != nil {
		return err
	}
	status := map[string]any{"conditions": []any{condition}}
	if kubeconfig != nil {
		status["kubeconfig"] = base64.StdEncoding.EncodeToString(kubeconfig)
		status["expirationTimestamp"] = expiration.UTC().Format(time.RFC3339)
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "KubeconfigRequest",
		"metadata":   map[string]any{"name": kr.Name, "namespace": kr.Namespace, "resourceVersion": kr.ResourceVersion},
		"status":     status,
	}}
	if err := r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(kubeconfigRequestFieldManager), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply the status of KubeconfigRequest %s: %w", kr.Name, err)
	}
	return nil
}
