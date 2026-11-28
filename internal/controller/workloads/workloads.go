// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package workloads is the Workloads controller.
package workloads

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/distribution/reference"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/authnconfig"
	"go.lodia.io/lodia/internal/fence"
	"go.lodia.io/lodia/internal/live"
	"go.lodia.io/lodia/internal/ownership"
	"go.lodia.io/lodia/internal/restore"
	"go.lodia.io/lodia/internal/statusapply"
	"go.lodia.io/lodia/internal/versions"
)

const (
	Name         = "workloads"
	FieldManager = "lodia/" + Name

	// timedOutReason is the Deployment controller's reason for a rollout past
	// its progress deadline
	// (https://github.com/kubernetes/kubernetes/blob/v1.37.1/pkg/controller/deployment/util/deployment_util.go#L85-L86).
	timedOutReason = "ProgressDeadlineExceeded"

	// maxVersion is the MaxLength of status.components.*.version.
	maxVersion = 16
	// maxRevision is the MaxLength of status.components.*.operatorRevision.
	maxRevision = 128

	// componentPods bounds the read of the pods of a component's current
	// template: its ReplicaSet runs at most five replicas, and as many may
	// still be deleted after a scale-down.
	componentPods = 16
	// exitsAgain and exitsAtMost bound how soon a pass follows one while a
	// component has a new replica not available, so that its exit is named
	// once it exits.
	exitsAgain  = 15 * time.Second
	exitsAtMost = 5 * time.Minute
)

var (
	releaseTag = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
	fatalLine  = regexp.MustCompile(`^F[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6} +[0-9]+ [^ ]+:[0-9]+\] `)
)

// Messages are built from the state alone, never from an error, so a pass
// that changes nothing writes nothing.
type Reconciler struct {
	client.Client
	// Reader reads what the label-filtered cache does not hold, and
	// APIServerReady while Workloads waits.
	Reader client.Reader
	// Events records the transitions of the conditions it writes;
	// SetupWithManager sets it.
	Events events.EventRecorder
	// SignerImage is lodia-signer's image, the API server's sidecar.
	SignerImage string
	// TunnelImage is lodia-tunnel's image, the API server's sidecar in
	// gateway mode.
	TunnelImage string
	// GatewayAPI says whether the management cluster served TLSRoute and
	// Gateway when the operator started.
	GatewayAPI bool
	// Revision is the running operator revision, which Workloads renders
	// with once Lifecycle grants it in status.target.operatorRevision.
	Revision string

	writes   sync.Mutex
	gateways map[types.NamespacedName]*fence.Fence

	validating  sync.Mutex
	validations map[types.NamespacedName][]validation
}

// validation is the outcome of validating one authentication configuration.
// A pass validates the spec's and, when that fails, the one stored, so a
// ControlPlane keeps the last two.
type validation struct {
	key string
	err error
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	r.Events = mgr.GetEventRecorder("lodia.io/" + Name)
	readOnce := predicate.NewPredicateFuncs(func(secret client.Object) bool {
		cp, ok := secret.GetLabels()[lodiav1alpha1.ControlPlaneLabel]
		return ok && slices.Contains(restartOnlyNames(cp), secret.GetName())
	})
	b := ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.ControlPlane{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}, builder.OnlyMetadata).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(controlPlaneOf), builder.WithPredicates(readOnce)).
		Watches(&lodiav1alpha1.ControlPlane{}, handler.EnqueueRequestsFromMapFunc(r.replacements))
	if r.GatewayAPI {
		if err := mgr.GetFieldIndexer().IndexField(context.Background(), &gatewayv1.TLSRoute{}, hostnamesIndex, routeHostnames); err != nil {
			return err
		}
		b = b.Owns(&gatewayv1.TLSRoute{}).
			Watches(&gatewayv1.TLSRoute{}, handler.EnqueueRequestsFromMapFunc(r.holders)).
			Watches(&gatewayv1.Gateway{}, handler.EnqueueRequestsFromMapFunc(r.parented), builder.WithPredicates(listenersChanged)).
			Watches(&lodiav1alpha1.ControlPlane{}, handler.EnqueueRequestsFromMapFunc(r.sharers), builder.WithPredicates(createdOrDeleted))
	}
	return b.Complete(r)
}

// replacements are the ControlPlanes whose completed restore replaces source,
// so that its deletion brings the pass that releases them.
func (r *Reconciler) replacements(ctx context.Context, source client.Object) []reconcile.Request {
	list := &lodiav1alpha1.ControlPlaneList{}
	if err := r.List(ctx, list, client.InNamespace(source.GetNamespace())); err != nil {
		logf.FromContext(ctx).Error(err, "list the ControlPlanes that may replace a source", "source", source.GetName())
		return nil
	}
	var out []reconcile.Request
	for i := range list.Items {
		if list.Items[i].Status.Restore.Source.UID == source.GetUID() {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
		}
	}
	return out
}

func controlPlaneOf(_ context.Context, secret client.Object) []reconcile.Request {
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: secret.GetNamespace(), Name: secret.GetLabels()[lodiav1alpha1.ControlPlaneLabel]}}}
}

func restartOnlyNames(cp string) []string {
	var names []string
	for _, secrets := range restartOnly {
		for _, s := range secrets {
			names = append(names, s.Name(cp))
		}
	}
	return names
}

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=list
// +kubebuilder:rbac:groups="",resources=services;configmaps,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=list
// +kubebuilder:rbac:groups="",resources=pods,verbs=list
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=tlsroutes,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		if apierrors.IsNotFound(err) {
			r.validating.Lock()
			delete(r.validations, req.NamespacedName)
			r.validating.Unlock()
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cp.DeletionTimestamp.IsZero() || ptr.Deref(cp.Spec.Paused, false) {
		return ctrl.Result{}, nil
	}
	var read []string
	if cp.Spec.Endpoint.Gateway != (lodiav1alpha1.GatewayEndpoint{}) {
		read = append(read, lodiav1alpha1.ConditionRoutesReady)
	}
	if cp.Spec.Endpoint.LoadBalancer != nil {
		read = append(read, lodiav1alpha1.ConditionLoadBalancerReady)
	}
	if err := live.Components(ctx, r.Reader, cp, read...); err != nil {
		return ctrl.Result{}, err
	}
	if meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady) == nil {
		first := statusapply.FirstPass(cp, lodiav1alpha1.ConditionAPIServerReady)
		var routes *metav1.Condition
		if cp.Spec.Endpoint.Gateway != (lodiav1alpha1.GatewayEndpoint{}) {
			routes = as(lodiav1alpha1.ConditionRoutesReady, first)
		}
		if err := r.applyStatus(ctx, cp, published(cp), cp.Status.Components, first, routes, balancerOf(cp, first)); err != nil {
			return ctrl.Result{}, err
		}
	}
	out, err := r.reconcile(ctx, cp)
	if out == nil {
		return ctrl.Result{}, err
	}
	controlled, readErr := r.controlled(ctx, cp)
	if readErr != nil {
		return ctrl.Result{}, errors.Join(err, readErr)
	}
	if statusErr := r.applyStatus(ctx, cp, out.endpoint, r.components(cp, controlled), out.condition, out.routes, out.balancer); statusErr != nil || err != nil {
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	requeueAfter := out.requeueAfter
	if again := exitsAfter(slices.Collect(maps.Values(controlled)), time.Now()); again > 0 && (requeueAfter == 0 || requeueAfter > again) {
		requeueAfter = again
	}
	if out.routes != nil && out.routes.Status != metav1.ConditionTrue && (requeueAfter == 0 || requeueAfter > time.Minute) {
		requeueAfter = time.Minute
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// outcome is what a pass reports. A nil outcome writes no status, and comes
// with the error that stopped the pass; a refusal has both.
type outcome struct {
	endpoint     *lodiav1alpha1.EndpointStatus
	condition    metav1.Condition
	routes       *metav1.Condition
	balancer     *metav1.Condition
	requeueAfter time.Duration
}

// reconcile publishes the endpoint before it waits on anything or says what it
// does not build, since PKI and Health need it, and a published endpoint stays
// on every path: an apply without it would remove it. In gateway mode it also
// reports RoutesReady, and in loadBalancer mode LoadBalancerReady, at the
// generation it describes when a held pass leaves the Service as stored.
func (r *Reconciler) reconcile(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*outcome, error) {
	svc := service(cp)
	if held, requeueAfter, err := r.controlledElsewhere(ctx, cp, svc); err != nil || held != nil {
		if err != nil {
			return nil, err
		}
		return &outcome{endpoint: published(cp), condition: *held, routes: keptRoutes(cp, *held), balancer: balancerOf(cp, *held), requeueAfter: requeueAfter}, nil
	}
	applied, err := r.apply(ctx, cp, svc)
	if err != nil {
		out, err := refused(published(cp), svc, err)
		if out != nil {
			out.routes = keptRoutes(cp, out.condition)
			out.balancer = balancerOf(cp, out.condition)
		}
		return out, err
	}
	switch {
	case cp.Spec.Endpoint.LoadBalancer != nil:
		endpoint, balancer := loadBalancer(cp, svc)
		if kept := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady); kept != nil && !applied {
			if kept.Reason == lodiav1alpha1.ReasonRefused {
				balancer = *kept.DeepCopy()
			} else {
				balancer.ObservedGeneration = kept.ObservedGeneration
			}
		}
		var waiting *metav1.Condition
		if endpoint == nil {
			waiting = new(newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for an address of the load balancer of Service "+svc.Name+", which LoadBalancerReady says"))
		}
		out, err := r.rollout(ctx, cp, endpoint, waiting, 0)
		if out != nil {
			out.balancer = &balancer
		}
		return out, err
	case cp.Spec.Endpoint.NodePort != nil:
		endpoint, waiting, err := r.nodePortEndpoint(ctx, cp, svc)
		if err != nil {
			return nil, err
		}
		return r.rollout(ctx, cp, endpoint, waiting, time.Minute)
	}
	endpoint, waiting, routes, routesErr := r.gateway(ctx, cp)
	if routes == nil {
		return nil, routesErr
	}
	out, err := r.rollout(ctx, cp, endpoint, waiting, 0)
	if out != nil {
		out.routes = routes
	}
	return out, errors.Join(routesErr, err)
}

// rollout waits on the endpoint and the inputs of the control plane's pods,
// then renders and applies them.
func (r *Reconciler) rollout(ctx context.Context, cp *lodiav1alpha1.ControlPlane, endpoint *lodiav1alpha1.EndpointStatus, waiting *metav1.Condition, requeueAfter time.Duration) (*outcome, error) {
	report := func(condition metav1.Condition) *outcome {
		return &outcome{endpoint: endpoint, condition: condition, requeueAfter: requeueAfter}
	}
	if waiting != nil {
		return report(*waiting), nil
	}
	in, waiting, err := r.inputs(ctx, cp, endpoint)
	if err != nil {
		return nil, err
	}
	if waiting != nil {
		return report(*waiting), nil
	}
	deployments, all := objects(in)
	if stopped, err := r.writeObjects(ctx, cp, endpoint, all); stopped != nil || err != nil {
		return stopped, err
	}
	if r.held(cp) {
		count := func() (metav1.Condition, error) {
			return r.availability(ctx, cp, deployments, in.note), nil
		}
		if in.unknownReport != "" {
			count = func() (metav1.Condition, error) { return r.count(ctx, cp) }
		}
		kept, err := r.keep(ctx, cp, r.holding(cp), in.served, count)
		if err != nil {
			return nil, err
		}
		return report(*kept), nil
	}
	if in.unknownReport != "" {
		return report(newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, in.unknownReport)), nil
	}
	return report(authenticated(r.availability(ctx, cp, deployments, in.note), in.refused+in.served)), nil
}

// authenticated is c, or AuthenticationInvalid with refused in its place unless
// c is False: Unavailable, or ProgressDeadlineExceeded with a component
// unavailable, says more, and carries refused after its own message.
func authenticated(c metav1.Condition, refused string) metav1.Condition {
	switch {
	case refused == "":
		return c
	case c.Status == metav1.ConditionFalse:
		c.Message += "; " + refused
		return c
	}
	return newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, refused)
}

// carried is the refusal c reports: AuthenticationInvalid's message, or what
// authenticated put after the message of a False count.
func carried(c metav1.Condition) string {
	if c.Reason == lodiav1alpha1.ReasonAuthenticationInvalid {
		return c.Message
	}
	if i := strings.Index(c.Message, "; "+notWritten); i >= 0 {
		return c.Message[i+len("; "):]
	}
	return ""
}

// serving is refusal ending with served in place of what it says is served.
func serving(refusal, served string) string {
	if before, found := strings.CutSuffix(refusal, servesLast); found {
		return before + served
	}
	return strings.TrimSuffix(refusal, servesNone) + served
}

type inputs struct {
	rendering rendering
	config    []*corev1.ConfigMap
	// note is what availability says of a controller manager at 0 replicas.
	note string
	// unknownReport says that only the API server's Deployment is written.
	unknownReport string
	// refused says why the spec's authentication configuration is not
	// written, empty when it is written.
	refused string
	// served ends refused with what the API server serves, empty when the
	// pass cannot tell.
	served string
}

// inputs waits, in this order, for the target, the two conditions, the
// Datastore's endpoints and the hashes, and returns what the pods render from
// or the condition that says what it waits for. A held pass whose target the
// table does not hold reports what runs, as the waits do.
func (r *Reconciler) inputs(ctx context.Context, cp *lodiav1alpha1.ControlPlane, endpoint *lodiav1alpha1.EndpointStatus) (*inputs, *metav1.Condition, error) {
	if granted(cp) == "" {
		return nil, new(newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, "waiting for Lifecycle to set status.target.operatorRevision")), nil
	}
	images, message, unknownReport := imagesOf(cp)
	if message != "" {
		if r.held(cp) {
			waiting, err := r.wait(ctx, cp, r.holding(cp))
			return nil, waiting, err
		}
		return nil, new(newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, message)), nil
	}
	if a := cp.Spec.Addons; a != nil && a.Konnectivity != nil {
		var err error
		if images, err = versions.PinKonnectivity(images, a.Konnectivity.Version); err != nil {
			return nil, nil, err
		}
	}
	for _, input := range []struct{ condition, reason string }{
		{lodiav1alpha1.ConditionCertificatesReady, lodiav1alpha1.ReasonWaitingForCertificates},
		{lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ReasonWaitingForDatastore},
	} {
		if !statusapply.TrueAt(cp, input.condition) {
			message := fmt.Sprintf("%s is not True for generation %d", input.condition, cp.Generation)
			waiting, err := r.wait(ctx, cp, newCondition(metav1.ConditionFalse, input.reason, message))
			return nil, waiting, err
		}
	}
	if len(cp.Status.Datastore.Endpoints) == 0 {
		waiting, err := r.wait(ctx, cp, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "waiting for the Datastore controller to publish status.datastore.endpoints"))
		return nil, waiting, err
	}
	config, hashed, err := configMaps(cp)
	if err != nil {
		return nil, nil, err
	}
	hashes, unhashed, err := r.hashes(ctx, cp, hashed)
	if err != nil {
		return nil, nil, err
	}
	if len(unhashed) > 0 {
		message := "waiting for PKI to write the Secrets the control plane reads once, with their data hash: " + strings.Join(unhashed, ", ")
		waiting, err := r.wait(ctx, cp, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, message))
		return nil, waiting, err
	}
	refused, err := r.refusal(cp, endpoint, config)
	if err != nil {
		return nil, nil, err
	}
	config, served, err := r.validated(ctx, cp, endpoint, refused, config, hashed)
	if err != nil {
		return nil, nil, err
	}
	unmet, note, err := r.release(ctx, cp)
	if err != nil {
		return nil, nil, err
	}
	held := false
	if len(unmet) > 0 {
		ran, err := r.ran(ctx, cp)
		if err != nil {
			return nil, nil, err
		}
		held = !ran
	}
	return &inputs{
		rendering: rendering{
			cp:          cp,
			images:      images,
			signerImage: r.SignerImage,
			tunnelImage: r.TunnelImage,
			revision:    r.Revision,
			endpoint:    endpoint,
			etcd:        cp.Status.Datastore.Endpoints,
			hashes:      hashes,
			held:        held,
		},
		config:        config,
		note:          note,
		unknownReport: unknownReport,
		refused:       refused,
		served:        served,
	}, nil, nil
}

// refusal says why kube-apiserver would refuse the spec's authentication
// configuration under the ServiceAccount issuer and the egress selector
// configuration the pass renders, empty when it would not.
func (r *Reconciler) refusal(cp *lodiav1alpha1.ControlPlane, endpoint *lodiav1alpha1.EndpointStatus, config []*corev1.ConfigMap) (string, error) {
	err := r.validate(cp, config[1].Data[authenticationFile], []string{issuer(cp, endpoint.APIServer)}, config[0].Data[egressFile])
	var invalid *authnconfig.Invalid
	if !errors.As(err, &invalid) {
		return "", err
	}
	return notWritten + invalid.Error(), nil
}

const (
	notWritten = "the authentication configuration of spec.apiServer.authentication is not written: "
	servesLast = "; the API server serves its last valid configuration"
	servesNone = "; the API server serves no JWT authenticator"
)

// validated is config with the authentication configuration Workloads writes:
// the spec's unless refused, and otherwise the one stored while this
// ControlPlane controls it and kube-apiserver would load it under the
// ServiceAccount issuer and the egress selector configuration the pass
// renders, or else one without JWT authenticators. The second result ends a
// refusal with what the API server serves: the stored file when it is kept and
// lists JWT authenticators, and otherwise none. A held pass writes over no
// stored file, so it says nothing of one it would replace.
func (r *Reconciler) validated(ctx context.Context, cp *lodiav1alpha1.ControlPlane, endpoint *lodiav1alpha1.EndpointStatus, refused string, config, hashed []*corev1.ConfigMap) ([]*corev1.ConfigMap, string, error) {
	if refused == "" {
		return config, "", nil
	}
	written := []*corev1.ConfigMap{config[0], hashed[1]}
	replaced := servesNone
	if r.held(cp) {
		replaced = ""
	}
	stored := &corev1.ConfigMap{}
	err := r.Reader.Get(ctx, client.ObjectKeyFromObject(hashed[1]), stored)
	switch {
	case apierrors.IsNotFound(err):
		return written, servesNone, nil
	case err != nil:
		return nil, "", fmt.Errorf("read ConfigMap %s: %w", hashed[1].Name, err)
	case !metav1.IsControlledBy(stored, cp):
		return written, replaced, nil
	}
	err = r.validate(cp, stored.Data[authenticationFile], []string{issuer(cp, endpoint.APIServer)}, config[0].Data[egressFile])
	if _, invalid := errors.AsType[*authnconfig.Invalid](err); invalid {
		return written, replaced, nil
	}
	if err != nil {
		return nil, "", err
	}
	if file, err := authnconfig.Decode([]byte(stored.Data[authenticationFile])); err == nil && len(file.JWT) > 0 {
		return config[:1], servesLast, nil
	}
	return config[:1], servesNone, nil
}

// validate is authnconfig.Validate, kept per ControlPlane for the content,
// issuers and egress of its last two validations.
func (r *Reconciler) validate(cp *lodiav1alpha1.ControlPlane, data string, issuers []string, egress string) error {
	sum, egressSum := sha256.Sum256([]byte(data)), sha256.Sum256([]byte(egress))
	key := hex.EncodeToString(sum[:]) + "/" + strings.Join(issuers, ",") + "/" + hex.EncodeToString(egressSum[:])
	name := client.ObjectKeyFromObject(cp)
	r.validating.Lock()
	kept := r.validations[name]
	i := slices.IndexFunc(kept, func(v validation) bool { return v.key == key })
	r.validating.Unlock()
	if i >= 0 {
		return kept[i].err
	}
	err := authnconfig.Validate([]byte(data), issuers, []byte(egress))
	r.validating.Lock()
	defer r.validating.Unlock()
	if r.validations == nil {
		r.validations = map[types.NamespacedName][]validation{}
	}
	r.validations[name] = append([]validation{{key: key, err: err}}, r.validations[name][:min(len(r.validations[name]), 1)]...)
	return err
}

// imagesOf renders the controller manager and the scheduler at the version the
// API server runs, so that they never get ahead of it, and at the target while
// none is reported. The first message stops the pass. The second names a report
// outside the table, which leaves those two where they are and still rolls the
// API server to the target, the one render that can move the report.
func imagesOf(cp *lodiav1alpha1.ControlPlane) (versions.Images, string, string) {
	target := ""
	if cp.Status.Target != nil {
		target = cp.Status.Target.Version
	}
	images, ok := versions.For(target)
	if !ok {
		if target == "" {
			return versions.Images{}, "waiting for Lifecycle to set status.target.version", ""
		}
		return versions.Images{}, "status.target.version " + target + " is not a version Lodia runs", ""
	}
	running := target
	if c := cp.Status.Components; c != nil && c.APIServer.Version != "" {
		running = c.APIServer.Version
	}
	followed, ok := versions.For(running)
	if !ok {
		return images, "", "status.components.apiServer.version " + running + " is not a version Lodia runs"
	}
	images.ControllerManager, images.Scheduler = followed.ControllerManager, followed.Scheduler
	return images, "", ""
}

// objects renders what the pass applies, in that order, and the Deployments
// among them, which the apply fills with the API server's status.
func objects(in *inputs) ([]*appsv1.Deployment, []client.Object) {
	cp := in.rendering.cp
	deployments := in.rendering.deployments()
	if in.unknownReport != "" {
		deployments = []*appsv1.Deployment{in.rendering.apiServer()}
	}
	var all []client.Object
	for _, c := range in.config {
		all = append(all, c)
	}
	for _, p := range networkPolicies(cp) {
		all = append(all, p)
	}
	for _, d := range deployments {
		all = append(all, d)
	}
	for _, b := range podDisruptionBudgets(cp) {
		all = append(all, b)
	}
	return deployments, all
}

// writeObjects applies objects once no other owner keeps one of them, on a
// held pass only those that are missing. A non-nil outcome stops the pass at
// what it reports.
func (r *Reconciler) writeObjects(ctx context.Context, cp *lodiav1alpha1.ControlPlane, endpoint *lodiav1alpha1.EndpointStatus, objects []client.Object) (*outcome, error) {
	if held, requeueAfter, err := r.controlledElsewhere(ctx, cp, objects...); err != nil || held != nil {
		if err != nil {
			return nil, err
		}
		return &outcome{endpoint: endpoint, condition: *held, requeueAfter: requeueAfter}, nil
	}
	for _, obj := range objects {
		if _, err := r.apply(ctx, cp, obj); err != nil {
			return refused(endpoint, obj, err)
		}
	}
	if cp.Spec.HighAvailability == (lodiav1alpha1.HighAvailability{}) {
		if err := r.deletePodDisruptionBudgets(ctx, cp); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// release names, in this order, what a restored ControlPlane's release still
// waits for, and gives the note availability makes of its controller manager
// at 0 replicas; both are empty for a ControlPlane that restores nothing.
func (r *Reconciler) release(ctx context.Context, cp *lodiav1alpha1.ControlPlane) ([]string, string, error) {
	restoring := cp.Spec.RestoreFrom
	if restoring == (lodiav1alpha1.RestoreFrom{}) {
		return nil, "", nil
	}
	var unmet []string
	if restoring.Release != lodiav1alpha1.RestoreReleased {
		unmet = append(unmet, "spec.restoreFrom.release is Pending")
	}
	if cp.Status.Restore == (lodiav1alpha1.RestoreStatus{}) {
		unmet = append(unmet, "the restore is not complete")
	} else if source, err := restore.Source(ctx, r.Client, cp); err != nil {
		return nil, "", err
	} else if source != nil {
		unmet = append(unmet, fmt.Sprintf("ControlPlane %s (UID %s), the Backup's source, exists", source.Name, source.UID))
	}
	if !statusapply.TrueAt(cp, lodiav1alpha1.ConditionAddonsApplied) {
		unmet = append(unmet, fmt.Sprintf("%s is not True for generation %d", lodiav1alpha1.ConditionAddonsApplied, cp.Generation))
	}
	if len(unmet) == 0 {
		return nil, controllerManager + " has 0 of 0 replicas ready", nil
	}
	return unmet, controllerManager + " is held at 0 replicas until the restore is released: " + strings.Join(unmet, "; "), nil
}

// ran says whether the Deployment of cp's controller manager has asked for
// replicas, which records the release for good. The cache can lag Workloads'
// own scale-up, so a Deployment it holds at 0 or not at all is read again
// from the API server.
func (r *Reconciler) ran(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (bool, error) {
	for _, reader := range []client.Reader{r.Client, r.Reader} {
		d := &appsv1.Deployment{}
		err := reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: deploymentName(cp.Name, controllerManager)}, d)
		if client.IgnoreNotFound(err) != nil {
			return false, err
		}
		if err == nil && metav1.IsControlledBy(d, cp) && ptr.Deref(d.Spec.Replicas, 1) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// refused says that the API server refuses obj, so that the status no longer
// tells of the pass before, and returns the error for the backoff either way.
func refused(endpoint *lodiav1alpha1.EndpointStatus, obj client.Object, err error) (*outcome, error) {
	if !apistatus.Refused(err) {
		return nil, err
	}
	message := fmt.Sprintf("the API server refuses %s %s; the reason is in Lodia's log", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName())
	return &outcome{endpoint: endpoint, condition: newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, message)}, err
}

// wait decides from APIServerReady as the API server holds it. While that
// reports availability, AuthenticationInvalid included, it counts the
// Deployments this ControlPlane controls at the generation the condition
// names, and passes the count through authenticated with the refusal that
// condition carries, which the pass that wrote it judged under the inputs it
// rendered; otherwise it keeps a condition of an older generation and returns
// waiting for one of cp's.
func (r *Reconciler) wait(ctx context.Context, cp *lodiav1alpha1.ControlPlane, waiting metav1.Condition) (*metav1.Condition, error) {
	return r.keep(ctx, cp, waiting, "", func() (metav1.Condition, error) { return r.count(ctx, cp) })
}

// keep is wait's decision, with count giving the availability it reports, and
// the refusal it carries ending with served when served is set.
func (r *Reconciler) keep(ctx context.Context, cp *lodiav1alpha1.ControlPlane, waiting metav1.Condition, served string, count func() (metav1.Condition, error)) (*metav1.Condition, error) {
	if err := live.Conditions(ctx, r.Reader, cp, lodiav1alpha1.ConditionAPIServerReady); err != nil {
		return nil, err
	}
	previous := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
	if previous == nil {
		return &waiting, nil
	}
	if slices.Contains([]string{lodiav1alpha1.ReasonReady, lodiav1alpha1.ReasonUnavailable, lodiav1alpha1.ReasonProgressDeadlineExceeded, lodiav1alpha1.ReasonAuthenticationInvalid}, previous.Reason) {
		counted, err := count()
		if err != nil {
			return nil, err
		}
		refusal := carried(*previous)
		if refusal != "" && served != "" {
			refusal = serving(refusal, served)
		}
		counted = authenticated(counted, refusal)
		counted.ObservedGeneration = previous.ObservedGeneration
		return &counted, nil
	}
	return new(statusapply.Wait(cp, previous, waiting)), nil
}

// count is the availability of the Deployments this ControlPlane controls, as
// the cache holds them.
func (r *Reconciler) count(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (metav1.Condition, error) {
	controlled, err := r.controlled(ctx, cp)
	if err != nil {
		return metav1.Condition{}, err
	}
	_, note, err := r.release(ctx, cp)
	if err != nil {
		return metav1.Condition{}, err
	}
	return r.availability(ctx, cp, slices.Collect(maps.Values(controlled)), note), nil
}

// availability is availability's condition over deployments, with the exits
// of their new replicas.
func (r *Reconciler) availability(ctx context.Context, cp *lodiav1alpha1.ControlPlane, deployments []*appsv1.Deployment, note string) metav1.Condition {
	return availability(deployments, note, controllerManagerHeld(cp), r.exits(ctx, cp, deployments))
}

// newReplicaUnavailable says whether d has a replica of its current template
// that is not available: one of the updated replicas, as rolloutCount counts
// them, once the Deployment controller has observed the template.
func newReplicaUnavailable(d *appsv1.Deployment) bool {
	count := rolloutCount(d)
	return count.Updated != nil && *count.Updated < d.Status.UpdatedReplicas
}

// exitsAfter is how soon a pass follows one at now while a component has a
// new replica not available, zero while none has: exitsAgain after its
// rollout last progressed, which the Deployment controller records as its
// Progressing condition's lastUpdateTime
// (https://github.com/kubernetes/kubernetes/blob/v1.37.3/pkg/controller/deployment/progress.go#L63-L84),
// then twice as long each time, up to exitsAtMost, as the kubelet backs off
// that replica's restarts from 10 seconds to five minutes
// (https://github.com/kubernetes/kubernetes/blob/v1.37.3/pkg/kubelet/kubelet.go#L172-L173,
// https://github.com/kubernetes/kubernetes/blob/v1.37.3/pkg/kubelet/apis/config/v1beta1/defaults.go#L45).
// A rollout past its progress deadline, which Lodia leaves at the default ten
// minutes
// (https://github.com/kubernetes/kubernetes/blob/v1.37.3/pkg/apis/apps/v1/defaults.go#L69-L72),
// has not progressed for longer than the doubling takes to reach exitsAtMost.
func exitsAfter(deployments []*appsv1.Deployment, now time.Time) time.Duration {
	var again time.Duration
	for _, d := range deployments {
		if !newReplicaUnavailable(d) {
			continue
		}
		next := exitsAgain
		if i := slices.IndexFunc(d.Status.Conditions, func(c appsv1.DeploymentCondition) bool { return c.Type == appsv1.DeploymentProgressing }); i >= 0 {
			next = exitsAtMost
			if c := d.Status.Conditions[i]; c.Reason != timedOutReason {
				next = min(exitsAtMost, max(exitsAgain, now.Sub(c.LastUpdateTime.Time)+exitsAgain))
			}
		}
		if again == 0 || next < again {
			again = next
		}
	}
	return again
}

// exits says, by component, how the newest of the pods of its current
// template that is not ready and has exited last exited, for a component with
// a new replica not available, with the line of its termination message, the
// tail of the component's logs since it runs with FallbackToLogsOnError, that
// exitLine reads: kube-apiserver's fatal line, a klog line at level F, when
// the message holds one, else its last line.
// A read that fails names no exit, and its error goes to the log: the next
// pass comes as exitsAfter says while the replica stays.
func (r *Reconciler) exits(ctx context.Context, cp *lodiav1alpha1.ControlPlane, deployments []*appsv1.Deployment) map[string]string {
	out := map[string]string{}
	for _, d := range deployments {
		component := d.Labels[lodiav1alpha1.ComponentLabel]
		if !newReplicaUnavailable(d) || !slices.Contains(components, component) {
			continue
		}
		pods, err := r.currentPods(ctx, cp, d, component)
		if err != nil {
			logf.FromContext(ctx).Error(err, "read the pods of a new replica not available", "component", component)
			continue
		}
		if exit := newestExit(pods, component); exit != "" {
			out[component] = exit
		}
	}
	return out
}

// currentPods are the pods of d's current template that are not Failed, an
// evicted one among them, at most componentPods, read from the API server only
// while such a replica exists, since the cache holds no pod of a component:
// those of the oldest ReplicaSet d controls whose template is d's, by creation
// time and then by name, which the Deployment controller keeps of two such
// (https://github.com/kubernetes/kubernetes/blob/v1.37.3/pkg/controller/deployment/util/deployment_util.go#L624-L638,
// https://github.com/kubernetes/kubernetes/blob/v1.37.3/pkg/controller/controller_utils.go#L1228-L1237),
// by the pod-template-hash label it gives them.
func (r *Reconciler) currentPods(ctx context.Context, cp *lodiav1alpha1.ControlPlane, d *appsv1.Deployment, component string) ([]corev1.Pod, error) {
	sets := &appsv1.ReplicaSetList{}
	if err := r.Reader.List(ctx, sets, client.InNamespace(cp.Namespace), client.MatchingLabels(podLabels(cp.Name, component))); err != nil {
		return nil, err
	}
	slices.SortFunc(sets.Items, func(a, b appsv1.ReplicaSet) int {
		return cmp.Or(a.CreationTimestamp.Compare(b.CreationTimestamp.Time), strings.Compare(a.Name, b.Name))
	})
	i := slices.IndexFunc(sets.Items, func(rs appsv1.ReplicaSet) bool {
		return metav1.IsControlledBy(&rs, d) && sameTemplate(rs.Spec.Template, d.Spec.Template)
	})
	if i < 0 {
		return nil, nil
	}
	labels := podLabels(cp.Name, component)
	labels[appsv1.DefaultDeploymentUniqueLabelKey] = sets.Items[i].Labels[appsv1.DefaultDeploymentUniqueLabelKey]
	pods := &corev1.PodList{}
	if err := r.Reader.List(ctx, pods, client.InNamespace(cp.Namespace), client.MatchingLabels(labels),
		client.MatchingFieldsSelector{Selector: fields.OneTermNotEqualSelector("status.phase", string(corev1.PodFailed))}, client.Limit(componentPods)); err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// sameTemplate says whether two templates are equal but for the
// pod-template-hash label.
func sameTemplate(a, b corev1.PodTemplateSpec) bool {
	a.Labels, b.Labels = maps.Clone(a.Labels), maps.Clone(b.Labels)
	delete(a.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
	delete(b.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
	return equality.Semantic.DeepEqual(a, b)
}

// newestExit says how the component's container last exited in the newest of
// pods that has exited and is neither ready nor being deleted, empty while
// none has.
func newestExit(pods []corev1.Pod, component string) string {
	var newest *corev1.Pod
	var ended *corev1.ContainerStateTerminated
	for i := range pods {
		pod := &pods[i]
		t := terminated(containerStatus(pod.Status.ContainerStatuses, component))
		if t == nil || !pod.DeletionTimestamp.IsZero() || podReady(pod) || newest != nil && !newer(pod, newest) {
			continue
		}
		newest, ended = pod, t
	}
	if newest == nil {
		return ""
	}
	exit := fmt.Sprintf("pod %s last exited %d (%s)", newest.Name, ended.ExitCode, ended.Reason)
	if line := exitLine(ended.Message); line != "" {
		exit += ": " + quoted(line)
	}
	return exit
}

func podReady(pod *corev1.Pod) bool {
	return slices.ContainsFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool {
		return c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue
	})
}

func newer(a, b *corev1.Pod) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return b.CreationTimestamp.Before(&a.CreationTimestamp)
	}
	return a.Name > b.Name
}

// exitLine is the line of a termination message a component exited on: its
// fatal line, a klog line at level F
// (https://github.com/kubernetes/klog/blob/v2.140.0/internal/buffer/buffer.go#L132-L153),
// which kube-apiserver prints when a post-start hook fails
// (https://github.com/kubernetes/kubernetes/blob/v1.37.3/staging/src/k8s.io/apiserver/pkg/server/hooks.go#L201-L204)
// and after which another goroutine may still log, when the message holds
// one, else its last line, where the components print the error they exit
// on, after any usage
// (https://github.com/kubernetes/kubernetes/blob/v1.37.3/staging/src/k8s.io/component-base/cli/run.go#L96-L111).
func exitLine(message string) string {
	lines := strings.Split(strings.TrimRight(message, " \t\r\n"), "\n")
	if i := slices.IndexFunc(lines, fatalLine.MatchString); i >= 0 {
		return strings.TrimSpace(lines[i])
	}
	return strings.TrimSpace(lines[len(lines)-1])
}

// held says whether cp's writes wait for Lifecycle to grant the running
// operator revision.
func (r *Reconciler) held(cp *lodiav1alpha1.ControlPlane) bool {
	return granted(cp) != r.Revision
}

func granted(cp *lodiav1alpha1.ControlPlane) string {
	if cp.Status.Target == nil {
		return ""
	}
	return cp.Status.Target.OperatorRevision
}

// holding is what a held pass reports while APIServerReady neither reports
// availability nor describes an earlier generation.
func (r *Reconciler) holding(cp *lodiav1alpha1.ControlPlane) metav1.Condition {
	return newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget,
		fmt.Sprintf("waiting for Lifecycle to grant the running operator revision %s; status.target.operatorRevision is %s", r.Revision, granted(cp)))
}

// controlledElsewhere reports the objects under Workloads' names that another
// owner keeps, read from the cache, which holds the labelled ones, Workloads'
// own and a predecessor's, and from the API server when the cache lacks one.
// The garbage collector deletes a previous
// ControlPlane's, so that wait is short; any other owner's waits for a person.
func (r *Reconciler) controlledElsewhere(ctx context.Context, cp *lodiav1alpha1.ControlPlane, objects ...client.Object) (*metav1.Condition, time.Duration, error) {
	var held []string
	requeueAfter := 10 * time.Second
	for _, obj := range objects {
		current := cached(obj)
		err := r.Get(ctx, client.ObjectKeyFromObject(obj), current)
		if apierrors.IsNotFound(err) {
			err = r.Reader.Get(ctx, client.ObjectKeyFromObject(obj), current)
		}
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, 0, err
		}
		owner := ownership.ControlledElsewhere(current, cp)
		if owner == nil {
			continue
		}
		held = append(held, fmt.Sprintf("%s %s by %s %s (UID %s)", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName(), owner.Kind, owner.Name, owner.UID))
		if !ownership.IsControlPlane(*owner) || owner.Name != cp.Name {
			requeueAfter = time.Minute
		}
	}
	if len(held) == 0 {
		return nil, 0, nil
	}
	slices.Sort(held)
	condition := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere,
		"another owner keeps "+strings.Join(held, ", ")+"; Workloads writes none of them until they are gone: the garbage collector deletes a previous ControlPlane's, any other owner's need a person")
	return &condition, requeueAfter, nil
}

// cached is an empty object of obj's kind as the cache holds it: ConfigMaps
// as metadata alone.
func cached(obj client.Object) client.Object {
	switch obj.(type) {
	case *corev1.Service:
		return &corev1.Service{}
	case *appsv1.Deployment:
		return &appsv1.Deployment{}
	case *networkingv1.NetworkPolicy:
		return &networkingv1.NetworkPolicy{}
	case *policyv1.PodDisruptionBudget:
		return &policyv1.PodDisruptionBudget{}
	case *gatewayv1.TLSRoute:
		return &gatewayv1.TLSRoute{}
	default:
		m := &metav1.PartialObjectMetadata{}
		m.SetGroupVersionKind(obj.GetObjectKind().GroupVersionKind())
		return m
	}
}

// published is the endpoint of a pass that learns nothing of the address: in
// loadBalancer mode the one at endpoint.loadBalancer.host, or else at the host
// published, built again since the rest follows from the spec; in the other
// modes the one published.
func published(cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.EndpointStatus {
	lb := cp.Spec.Endpoint.LoadBalancer
	switch {
	case lb == nil && cp.Status.Endpoint == (lodiav1alpha1.EndpointStatus{}):
		return nil
	case lb == nil:
		return new(cp.Status.Endpoint)
	case lb.Host != "":
		return atHost(cp, string(lb.Host))
	case cp.Status.Endpoint != (lodiav1alpha1.EndpointStatus{}):
		return atHost(cp, cp.Status.Endpoint.APIServer.Host)
	}
	return nil
}

// atHost is a load balancer's endpoint at host: the tenant's pods dial host
// when the spec names it as an IP address, and apiserver-proxy otherwise.
func atHost(cp *lodiav1alpha1.ControlPlane, host string) *lodiav1alpha1.EndpointStatus {
	inCluster := lodiav1alpha1.HostPort{Host: host, Port: lodiav1alpha1.APIServerServicePort}
	if cp.Spec.Endpoint.NodeProxy() {
		inCluster = lodiav1alpha1.HostPort{Host: cp.Spec.AdvertiseAddress(), Port: kubernetesPort}
	}
	return &lodiav1alpha1.EndpointStatus{
		APIServer:    lodiav1alpha1.HostPort{Host: host, Port: lodiav1alpha1.APIServerServicePort},
		Konnectivity: lodiav1alpha1.HostPort{Host: host, Port: konnectivityPort},
		InCluster:    inCluster,
	}
}

// loadBalancer is the endpoint of loadBalancer mode and LoadBalancerReady. With
// host the endpoint is the spec's. Without it the endpoint is discovered and
// keeps its host while an ingress entry still reports it, as a host name or an
// IP, and while none reports an address: the serving certificate, the
// kubeconfigs, the ServiceAccount issuer and every joined kubelet name it. A
// new one is the first entry's host name, else its IP, since apiserver-proxy
// dials either and a name outlives its addresses.
func loadBalancer(cp *lodiav1alpha1.ControlPlane, svc *corev1.Service) (*lodiav1alpha1.EndpointStatus, metav1.Condition) {
	ingress := svc.Status.LoadBalancer.Ingress
	var ips, addresses []string
	for _, in := range ingress {
		if in.Hostname != "" {
			addresses = append(addresses, in.Hostname)
		}
		if in.IP != "" {
			ips, addresses = append(ips, in.IP), append(addresses, in.IP)
		}
	}
	endpoint := published(cp)
	reports := func(in corev1.LoadBalancerIngress) bool {
		return endpoint != nil && (in.Hostname == endpoint.APIServer.Host || sameIP(in.IP, endpoint.APIServer.Host))
	}
	if cp.Spec.Endpoint.LoadBalancer.Host == "" && len(addresses) > 0 && !slices.ContainsFunc(ingress, reports) {
		first := ingress[slices.IndexFunc(ingress, func(in corev1.LoadBalancerIngress) bool { return in.Hostname != "" || in.IP != "" })]
		endpoint = atHost(cp, cmp.Or(first.Hostname, first.IP))
	}
	return endpoint, balancerReady(cp, svc.Name, ips, addresses)
}

// balancerReady is LoadBalancerReady from what the Service's load balancer
// reports.
func balancerReady(cp *lodiav1alpha1.ControlPlane, svc string, ips, addresses []string) metav1.Condition {
	host := string(cp.Spec.Endpoint.LoadBalancer.Host)
	of := "the load balancer of Service " + svc
	switch {
	case len(addresses) == 0:
		return balancerCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForLoadBalancer, of+" reports no address yet")
	case cp.Spec.Endpoint.NodeProxy():
		return balancerCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, of+" reports "+listed(addresses))
	case len(ips) == 0:
		return balancerCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonUnverified,
			fmt.Sprintf("%s reports the host names %s and no IP address, so endpoint.loadBalancer.host %s cannot be compared with them", of, listed(addresses), host))
	case slices.ContainsFunc(ips, func(ip string) bool { return sameIP(ip, host) }):
		return balancerCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, of+" reports endpoint.loadBalancer.host "+host)
	}
	return balancerCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonAddressMismatch,
		fmt.Sprintf("%s reports %s, not endpoint.loadBalancer.host %s: the annotation of endpoint.loadBalancer.annotations that asks the load balancer for it is missing or was ignored", of, listed(ips), host))
}

// sameIP says whether a and b are one IP address, which an ingress entry may
// spell in a form that is not canonical.
func sameIP(a, b string) bool {
	x, errX := netip.ParseAddr(a)
	y, errY := netip.ParseAddr(b)
	return errX == nil && errY == nil && x == y
}

// maxListed bounds the addresses a message names: nothing bounds a Service's
// ingress but the object's size.
const maxListed = 10

func listed(addresses []string) string {
	if len(addresses) > maxListed {
		return fmt.Sprintf("%s and %d more", strings.Join(addresses[:maxListed], ", "), len(addresses)-maxListed)
	}
	return strings.Join(addresses, ", ")
}

// nodePortEndpoint keeps the address it was published with while any node
// still reports it as an InternalIP of the family of the first service range,
// Ready or not: the serving certificate, the kubeconfigs, the ServiceAccount
// issuer and every joined kubelet name it. A new one is the first Ready node's
// by name. No event reports a node, so the caller checks again every minute.
func (r *Reconciler) nodePortEndpoint(ctx context.Context, cp *lodiav1alpha1.ControlPlane, svc *corev1.Service) (*lodiav1alpha1.EndpointStatus, *metav1.Condition, error) {
	published := ""
	if e := cp.Status.Endpoint; e != (lodiav1alpha1.EndpointStatus{}) {
		published = e.APIServer.Host
	}
	range0, err := netip.ParsePrefix(string(cp.Spec.Network.ServiceCIDRs[0]))
	if err != nil {
		return nil, nil, err
	}
	ipv6 := range0.Addr().Is6()
	host, err := r.nodeAddress(ctx, published, ipv6)
	if err != nil {
		return nil, nil, err
	}
	if host == "" {
		family := "IPv4"
		if ipv6 {
			family = "IPv6"
		}
		waiting := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint,
			"no Ready node of the management cluster has an "+family+" InternalIP, the family of network.serviceCIDRs[0]")
		return nil, &waiting, nil
	}
	return &lodiav1alpha1.EndpointStatus{
		APIServer:    lodiav1alpha1.HostPort{Host: host, Port: nodePort(svc, lodiav1alpha1.APIServerServicePort)},
		Konnectivity: lodiav1alpha1.HostPort{Host: host, Port: nodePort(svc, konnectivityPort)},
		InCluster:    lodiav1alpha1.HostPort{Host: host, Port: nodePort(svc, lodiav1alpha1.APIServerServicePort)},
	}, nil, nil
}

func (r *Reconciler) nodeAddress(ctx context.Context, published string, ipv6 bool) (string, error) {
	nodes := &corev1.NodeList{}
	if err := r.Reader.List(ctx, nodes); err != nil {
		return "", err
	}
	slices.SortFunc(nodes.Items, func(a, b corev1.Node) int { return strings.Compare(a.Name, b.Name) })
	first := ""
	for _, node := range nodes.Items {
		var internal []string
		for _, a := range node.Status.Addresses {
			if address, err := netip.ParseAddr(a.Address); a.Type == corev1.NodeInternalIP && err == nil && address.Is6() == ipv6 {
				internal = append(internal, a.Address)
			}
		}
		if published != "" && slices.Contains(internal, published) {
			return published, nil
		}
		ready := slices.ContainsFunc(node.Status.Conditions, func(c corev1.NodeCondition) bool {
			return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
		})
		if first == "" && ready && len(internal) > 0 {
			first = internal[0]
		}
	}
	return first, nil
}

func nodePort(svc *corev1.Service, port int32) int32 {
	if i := slices.IndexFunc(svc.Spec.Ports, func(p corev1.ServicePort) bool { return p.Port == port }); i >= 0 {
		return svc.Spec.Ports[i].NodePort
	}
	return 0
}

func issuer(cp *lodiav1alpha1.ControlPlane, api lodiav1alpha1.HostPort) string {
	if sa := cp.Spec.ServiceAccount; sa != nil && sa.Issuer != "" {
		return sa.Issuer
	}
	return publicURL(api)
}

func publicURL(api lodiav1alpha1.HostPort) string {
	return "https://" + strings.TrimSuffix(net.JoinHostPort(api.Host, strconv.Itoa(int(api.Port))), ":"+strconv.Itoa(lodiav1alpha1.APIServerServicePort))
}

// hashes are each component's restart hashes, from the data hash PKI writes on
// its restart-only Secrets. The second result names the Secrets not written
// with a hash yet.
func (r *Reconciler) hashes(ctx context.Context, cp *lodiav1alpha1.ControlPlane, config []*corev1.ConfigMap) (map[string]string, []string, error) {
	data := map[string]string{}
	var unhashed []string
	for _, secrets := range restartOnly {
		for _, s := range secrets {
			secret := &metav1.PartialObjectMetadata{}
			secret.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
			err := r.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: s.Name(cp.Name)}, secret)
			if client.IgnoreNotFound(err) != nil {
				return nil, nil, err
			}
			data[s.Name(cp.Name)] = secret.Annotations[lodiav1alpha1.DataHashAnnotation]
			if data[s.Name(cp.Name)] == "" {
				unhashed = append(unhashed, s.Name(cp.Name))
			}
		}
	}
	slices.Sort(unhashed)
	return restartHashes(cp.Name, data, config), unhashed, nil
}

// restartHashes are each component's hash of its restart-only Secrets by
// their data hashes and, for the API server, its ConfigMaps as configMaps
// hashes them, so that a change to one rolls the component that has it and
// nothing else.
func restartHashes(cp string, data map[string]string, config []*corev1.ConfigMap) map[string]string {
	out := map[string]string{}
	for component, secrets := range restartOnly {
		h := sha256.New()
		if component == apiServer {
			for _, c := range config {
				for _, key := range slices.Sorted(maps.Keys(c.Data)) {
					sum := sha256.Sum256([]byte(c.Data[key]))
					h.Write([]byte(c.Name + "/" + key + "=" + hex.EncodeToString(sum[:]) + "\n"))
				}
			}
		}
		for _, s := range secrets {
			h.Write([]byte(s.Name(cp) + "=" + data[s.Name(cp)] + "\n"))
		}
		out[component] = hex.EncodeToString(h.Sum(nil))
	}
	return out
}

// controllerManagerHeld says whether Health holds the controller manager's
// leader Lease, which stops it on purpose.
func controllerManagerHeld(cp *lodiav1alpha1.ControlPlane) bool {
	return meta.IsStatusConditionTrue(cp.Status.Conditions, lodiav1alpha1.ConditionControllerManagerHeld)
}

// availability is True while each component has a ready replica; one without
// a Deployment has none. A restored ControlPlane's controller manager at 0
// replicas counts as available, with the note release makes of it, and so does
// one with no ready replica while Health holds its leader Lease. A rollout past
// its deadline is reported whether or not a replica is ready, False when none
// is, since it is what Lifecycle fails a change on.
func availability(deployments []*appsv1.Deployment, held string, leaseHeld bool, exits map[string]string) metav1.Condition {
	var short, stalled []string
	unavailable := false
	for _, component := range components {
		i := slices.IndexFunc(deployments, func(d *appsv1.Deployment) bool { return d.Labels[lodiav1alpha1.ComponentLabel] == component })
		if i < 0 {
			short = append(short, component+" has no Deployment")
			unavailable = true
			continue
		}
		d := deployments[i]
		want := ptr.Deref(d.Spec.Replicas, 1)
		if want == 0 && component == controllerManager && held != "" {
			short = append(short, held)
			continue
		}
		exit, exited := exits[component]
		if leaseHeld && component == controllerManager && d.Status.ReadyReplicas == 0 {
			short = append(short, fmt.Sprintf("%s has 0 of %d replicas ready while Health holds its leader Lease", component, want))
			if exited {
				short = append(short, component+"'s "+exit)
			}
			continue
		}
		if d.Status.ReadyReplicas < want {
			short = append(short, fmt.Sprintf("%s has %d of %d replicas ready", component, d.Status.ReadyReplicas, want))
		}
		if exited {
			short = append(short, component+"'s "+exit)
		}
		unavailable = unavailable || d.Status.ReadyReplicas == 0
		if d.Status.ObservedGeneration == d.Generation && slices.ContainsFunc(d.Status.Conditions, func(c appsv1.DeploymentCondition) bool {
			return c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse && c.Reason == timedOutReason
		}) {
			stalled = append(stalled, "the rollout of Deployment "+d.Name+" passed its progress deadline")
		}
	}
	switch {
	case len(stalled) > 0:
		status := metav1.ConditionTrue
		if unavailable {
			status = metav1.ConditionFalse
		}
		return newCondition(status, lodiav1alpha1.ReasonProgressDeadlineExceeded, strings.Join(append(stalled, short...), "; "))
	case unavailable:
		return newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, strings.Join(short, "; "))
	case len(short) > 0:
		return newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, strings.Join(short, "; "))
	}
	return newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "every replica of kube-apiserver, kube-controller-manager and kube-scheduler is ready")
}

// controlled are the Deployments this ControlPlane controls, by component, as
// the cache holds them.
func (r *Reconciler) controlled(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (map[string]*appsv1.Deployment, error) {
	out := map[string]*appsv1.Deployment{}
	for _, component := range components {
		d := &appsv1.Deployment{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: deploymentName(cp.Name, component)}, d); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		if metav1.IsControlledBy(d, cp) {
			out[component] = d
		}
	}
	return out, nil
}

// components counts the replicas of the Deployments this ControlPlane
// controls, by component, and reports the version each runs, and the
// memberRestore.detectedAt each rolled for. A component starts from its
// previous report, which cp holds as the API server does, and a Deployment
// that is gone keeps it: an apply that leaves a field out removes it.
func (r *Reconciler) components(cp *lodiav1alpha1.ControlPlane, controlled map[string]*appsv1.Deployment) *lodiav1alpha1.ComponentsStatus {
	var previous lodiav1alpha1.ComponentsStatus
	if cp.Status.Components != nil {
		previous = *cp.Status.Components
	}
	out := &lodiav1alpha1.ComponentsStatus{}
	for _, component := range components {
		*slot(out, component) = rolledOut(controlled[component], component, *slot(&previous, component), rollsFor(cp, component))
	}
	if *out == (lodiav1alpha1.ComponentsStatus{}) {
		return nil
	}
	return out
}

func slot(c *lodiav1alpha1.ComponentsStatus, component string) *lodiav1alpha1.ComponentStatus {
	switch component {
	case apiServer:
		return &c.APIServer
	case controllerManager:
		return &c.ControllerManager
	}
	return &c.Scheduler
}

// rolledOut is d's counts, with the version of its image and the revision of
// its stamp once every replica runs the template, 0 of 0 included, and
// previous's report until then, old pods that still terminate aside, as
// kubectl's rollout status counts done. Its memberRestoreDetectedAt moves only
// to rendered, the value the pass renders for the component, once the
// template carries it: a cache behind a finished roll shows an older template
// rolled out, which moves no report back. A component with neither a
// Deployment nor a report is absent. A stamp longer than the field is not
// reported, as a hand-edited image is not.
func rolledOut(d *appsv1.Deployment, component string, previous lodiav1alpha1.ComponentStatus, rendered *metav1.Time) lodiav1alpha1.ComponentStatus {
	if d == nil {
		if previous.Rendered == (lodiav1alpha1.Rendered{}) && previous.MemberRestoreDetectedAt == nil {
			return lodiav1alpha1.ComponentStatus{}
		}
		none := lodiav1alpha1.Count{Total: new(int32(0)), Ready: new(int32(0))}
		return lodiav1alpha1.ComponentStatus{RolloutCount: lodiav1alpha1.RolloutCount{Count: none}, Rendered: previous.Rendered, MemberRestoreDetectedAt: previous.MemberRestoreDetectedAt}
	}
	status := lodiav1alpha1.ComponentStatus{RolloutCount: rolloutCount(d), Rendered: previous.Rendered, MemberRestoreDetectedAt: previous.MemberRestoreDetectedAt}
	if status.Updated != nil && *status.Updated == *status.Total {
		if version := imageVersion(d, component); version != "" {
			status.Version = version
		}
		if stamp := d.Annotations[lodiav1alpha1.OperatorRevisionAnnotation]; stamp != "" && len(stamp) <= maxRevision {
			status.OperatorRevision = stamp
		}
		// @todo: stop this roll for a member restored from snapshots once etcd-backup-restore restores with RevisionBump and MarkCompacted; a member that starts empty keeps it (todo.md).
		if rendered != nil && d.Spec.Template.Annotations[lodiav1alpha1.MemberRestoreDetectedAtAnnotation] == annotated(*rendered) {
			status.MemberRestoreDetectedAt = rendered
		}
	}
	return status
}

// imageVersion is the tag of the component's own container when it is
// vMAJOR.MINOR.PATCH and fits the field: a hand-edited image must not get the
// whole status apply refused.
func imageVersion(d *appsv1.Deployment, component string) string {
	i := slices.IndexFunc(d.Spec.Template.Spec.Containers, func(c corev1.Container) bool { return c.Name == component })
	if i < 0 {
		return ""
	}
	named, err := reference.ParseNormalizedNamed(d.Spec.Template.Spec.Containers[i].Image)
	if err != nil {
		return ""
	}
	if tagged, ok := named.(reference.Tagged); ok && len(tagged.Tag()) <= maxVersion && releaseTag.MatchString(tagged.Tag()) {
		return tagged.Tag()
	}
	return ""
}

// rolloutCount's updated counts, once the Deployment controller has observed
// the template, the available replicas of the template: all available ones
// less those of older templates, which a surge keeps available until the new
// ones are.
func rolloutCount(d *appsv1.Deployment) lodiav1alpha1.RolloutCount {
	s, total := d.Status, ptr.Deref(d.Spec.Replicas, 1)
	count := lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: new(total), Ready: new(min(s.ReadyReplicas, total))}}
	if s.ObservedGeneration == d.Generation {
		count.Updated = new(max(0, min(s.UpdatedReplicas, s.AvailableReplicas-(s.Replicas-s.UpdatedReplicas))))
	}
	return count
}

func (r *Reconciler) deletePodDisruptionBudgets(ctx context.Context, cp *lodiav1alpha1.ControlPlane) error {
	for _, component := range components {
		b := &policyv1.PodDisruptionBudget{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: deploymentName(cp.Name, component)}, b); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if !metav1.IsControlledBy(b, cp) {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, b, client.Preconditions{UID: &b.UID})); err != nil {
			return fmt.Errorf("delete PodDisruptionBudget %s: %w", b.Name, err)
		}
	}
	return nil
}

// apply writes obj, fills it with the object the API server stores, and says
// whether it wrote. A held pass writes only a missing object, and fills obj
// with a stored one as it is, but for the repair after a restored member.
func (r *Reconciler) apply(ctx context.Context, cp *lodiav1alpha1.ControlPlane, obj client.Object) (bool, error) {
	if r.held(cp) {
		repair := restoreAnnotation(obj)
		if stored, err := r.stored(ctx, cp, obj); err != nil || stored {
			if err != nil {
				return false, err
			}
			return r.repair(ctx, obj, repair)
		}
	}
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return false, err
	}
	delete(content, "status")
	dropNulls(content)
	applied := &unstructured.Unstructured{Object: content}
	if err := r.Apply(ctx, client.ApplyConfigurationFromUnstructured(applied), client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		return false, fmt.Errorf("apply %s %s: %w", applied.GetKind(), applied.GetName(), err)
	}
	return true, runtime.DefaultUnstructuredConverter.FromUnstructured(applied.Object, obj)
}

func restoreAnnotation(obj client.Object) string {
	if d, ok := obj.(*appsv1.Deployment); ok {
		return d.Spec.Template.Annotations[lodiav1alpha1.MemberRestoreDetectedAtAnnotation]
	}
	return ""
}

// repair writes value, the annotation of a repair after a restored member
// the render gives, into a stored Deployment whose template carries another,
// with a merge patch of that one annotation: an apply of a partial object
// would drop Workloads' other fields. The repair rolls at once, on a held
// tenant too (docs/decisions/upgrades-and-maintenance-windows.md).
func (r *Reconciler) repair(ctx context.Context, obj client.Object, value string) (bool, error) {
	d, ok := obj.(*appsv1.Deployment)
	if !ok || value == "" || d.Spec.Template.Annotations[lodiav1alpha1.MemberRestoreDetectedAtAnnotation] == value {
		return false, nil
	}
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"annotations": map[string]string{lodiav1alpha1.MemberRestoreDetectedAtAnnotation: value}}}}})
	if err != nil {
		return false, err
	}
	if err := r.Patch(ctx, d, client.RawPatch(types.MergePatchType, patch), client.FieldOwner(FieldManager)); err != nil {
		return false, fmt.Errorf("patch Deployment %s: %w", d.Name, err)
	}
	return true, nil
}

// stored fills obj with the object stored under its name and says whether
// there is one. One the cache lacks is read from the API server: the cache can
// lag Workloads' own create, or miss an object whose label was removed. One
// another owner keeps counts as missing, and nothing is read from it.
func (r *Reconciler) stored(ctx context.Context, cp *lodiav1alpha1.ControlPlane, obj client.Object) (bool, error) {
	key := client.ObjectKeyFromObject(obj)
	current := cached(obj)
	err := r.Get(ctx, key, current)
	if apierrors.IsNotFound(err) {
		err = r.Reader.Get(ctx, key, current)
	}
	gvk := obj.GetObjectKind().GroupVersionKind()
	switch {
	case apierrors.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("read %s %s: %w", gvk.Kind, obj.GetName(), err)
	case ownership.ControlledElsewhere(current, cp) != nil:
		return false, nil
	}
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(current)
	if err != nil {
		return false, err
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(content, obj); err != nil {
		return false, err
	}
	return true, nil
}

// dropNulls removes the null fields a typed object marshals, such as
// creationTimestamp, which an apply would otherwise claim.
func dropNulls(m map[string]any) {
	for k, v := range m {
		switch v := v.(type) {
		case nil:
			delete(m, k)
		case map[string]any:
			dropNulls(v)
		case []any:
			for _, item := range v {
				if item, ok := item.(map[string]any); ok {
					dropNulls(item)
				}
			}
		}
	}
}

// applyStatus applies APIServerReady and, in gateway mode, RoutesReady or, in
// loadBalancer mode, LoadBalancerReady, each at cp's generation unless it names
// one, status.components and, once both addresses exist, status.endpoint
// whole; a nil endpoint or components drops a published one.
func (r *Reconciler) applyStatus(ctx context.Context, cp *lodiav1alpha1.ControlPlane, endpoint *lodiav1alpha1.EndpointStatus, components *lodiav1alpha1.ComponentsStatus, condition metav1.Condition, routes, balancer *metav1.Condition) error {
	conditions := []metav1.Condition{condition}
	for _, c := range []*metav1.Condition{routes, balancer} {
		if c != nil {
			conditions = append(conditions, *c)
		}
	}
	for i := range conditions {
		if conditions[i].ObservedGeneration == 0 {
			conditions[i].ObservedGeneration = cp.Generation
		}
	}
	published := ptr.Deref(endpoint, lodiav1alpha1.EndpointStatus{})
	changed := cp.Status.Endpoint != published || !equality.Semantic.DeepEqual(cp.Status.Components, components)
	return statusapply.Writer{Client: r.Client, Reader: r.Reader, Manager: FieldManager, Events: r.Events}.Apply(ctx, cp, lodiav1alpha1.ControlPlaneStatus{Conditions: conditions, Endpoint: published, Components: components}, changed)
}

func newCondition(status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: status, Reason: reason, Message: message}
}

func as(conditionType string, c metav1.Condition) *metav1.Condition {
	c.Type = conditionType
	return &c
}

func routesCondition(status metav1.ConditionStatus, reason, message string) *metav1.Condition {
	return as(lodiav1alpha1.ConditionRoutesReady, newCondition(status, reason, message))
}

func balancerCondition(status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: lodiav1alpha1.ConditionLoadBalancerReady, Status: status, Reason: reason, Message: message}
}

// balancerOf is LoadBalancerReady in loadBalancer mode on a pass that stops at
// the Service, or on its first pass: the Service's reason and message, since
// the load balancer is the Service's.
func balancerOf(cp *lodiav1alpha1.ControlPlane, service metav1.Condition) *metav1.Condition {
	if cp.Spec.Endpoint.LoadBalancer == nil {
		return nil
	}
	return as(lodiav1alpha1.ConditionLoadBalancerReady, service)
}

// keptRoutes is RoutesReady in gateway mode on a pass that stops at the
// Service, before the listener: as it was, or with none yet, or only the first
// pass's, False with the Service's reason and message, since Reconciling would
// come first in Ready and hide a Service held until a person acts.
func keptRoutes(cp *lodiav1alpha1.ControlPlane, service metav1.Condition) *metav1.Condition {
	if cp.Spec.Endpoint.Gateway == (lodiav1alpha1.GatewayEndpoint{}) {
		return nil
	}
	if kept := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionRoutesReady); kept != nil && kept.Reason != lodiav1alpha1.ReasonReconciling {
		return kept.DeepCopy()
	}
	return as(lodiav1alpha1.ConditionRoutesReady, service)
}
