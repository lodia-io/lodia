// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package workloads is the Workloads controller.
package workloads

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
)

// Messages are built from the state alone, never from an error, so a pass
// that changes nothing writes nothing.
type Reconciler struct {
	client.Client
	// Reader reads what the label-filtered cache does not hold, and
	// APIServerReady while Workloads waits.
	Reader client.Reader
	// SignerImage is lodia-signer's image, the API server's sidecar.
	SignerImage string
	// TunnelImage is lodia-tunnel's image, the API server's sidecar in
	// gateway mode.
	TunnelImage string
	// GatewayAPI says whether the management cluster served TLSRoute and
	// Gateway when the operator started.
	GatewayAPI bool

	writes   sync.Mutex
	gateways map[types.NamespacedName]*semaphore.Weighted
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
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
		if restore := list.Items[i].Status.Restore; restore != nil && restore.Source.UID == source.GetUID() {
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
// +kubebuilder:rbac:groups="",resources=services;configmaps,verbs=list;watch;create;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=list;watch;create;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=list;watch;create;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=list
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=tlsroutes,verbs=list;watch;create;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=list;watch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cp.DeletionTimestamp.IsZero() || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	if cp.Spec.Endpoint.Gateway != nil {
		if err := live.Endpoint(ctx, r.Reader, cp, lodiav1alpha1.ConditionRoutesReady); err != nil {
			return ctrl.Result{}, err
		}
	}
	if meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady) == nil {
		first := statusapply.FirstPass(cp, lodiav1alpha1.ConditionAPIServerReady)
		var routes *metav1.Condition
		if cp.Spec.Endpoint.Gateway != nil {
			routes = asRoutes(first)
		}
		if err := r.applyStatus(ctx, cp, cp.Status.Endpoint, cp.Status.Components, first, routes); err != nil {
			return ctrl.Result{}, err
		}
	}
	out, err := r.reconcile(ctx, cp)
	if out == nil {
		return ctrl.Result{}, err
	}
	components, readErr := r.components(ctx, cp)
	if readErr != nil {
		return ctrl.Result{}, errors.Join(err, readErr)
	}
	if statusErr := r.applyStatus(ctx, cp, out.endpoint, components, out.condition, out.routes); statusErr != nil || err != nil {
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	requeueAfter := out.requeueAfter
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
	requeueAfter time.Duration
}

// reconcile publishes the endpoint before it waits on anything or says what it
// does not build, since PKI and Health need it. In gateway mode it also
// reports RoutesReady, and a published endpoint stays on every path: an apply
// without it would remove it.
func (r *Reconciler) reconcile(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*outcome, error) {
	svc := service(cp)
	if held, requeueAfter, err := r.controlledElsewhere(ctx, cp, svc); err != nil || held != nil {
		if err != nil {
			return nil, err
		}
		var endpoint *lodiav1alpha1.EndpointStatus
		if cp.Spec.Endpoint.Gateway != nil {
			endpoint = cp.Status.Endpoint
		}
		return &outcome{endpoint: endpoint, condition: *held, routes: keptRoutes(cp, *held), requeueAfter: requeueAfter}, nil
	}
	if err := r.apply(ctx, svc); err != nil {
		out, err := refused(cp.Status.Endpoint, svc, err)
		if out != nil {
			out.routes = keptRoutes(cp, out.condition)
		}
		return out, err
	}
	if cp.Spec.Endpoint.Gateway == nil {
		endpoint, waiting, requeueAfter, err := r.endpoint(ctx, cp, svc)
		if err != nil {
			return nil, err
		}
		return r.rollout(ctx, cp, endpoint, waiting, requeueAfter)
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

// rollout says what is not built, or waits on the endpoint and the inputs of
// the control plane's pods, then renders and applies them.
func (r *Reconciler) rollout(ctx context.Context, cp *lodiav1alpha1.ControlPlane, endpoint *lodiav1alpha1.EndpointStatus, waiting *metav1.Condition, requeueAfter time.Duration) (*outcome, error) {
	report := func(condition metav1.Condition) *outcome {
		return &outcome{endpoint: endpoint, condition: condition, requeueAfter: requeueAfter}
	}
	if fields := unsupported(cp); len(fields) > 0 {
		return report(newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported, "not built yet: "+strings.Join(fields, ", "))), nil
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
	return report(availability(deployments, in.note, controllerManagerHeld(cp))), nil
}

type inputs struct {
	rendering rendering
	config    []*corev1.ConfigMap
	// note is what availability says of a controller manager at 0 replicas.
	note string
}

// inputs waits, in this order, for the target, the two conditions, the
// Datastore's endpoints and the hashes, and returns what the pods render from
// or the condition that says what it waits for.
func (r *Reconciler) inputs(ctx context.Context, cp *lodiav1alpha1.ControlPlane, endpoint *lodiav1alpha1.EndpointStatus) (*inputs, *metav1.Condition, error) {
	target := ""
	if cp.Status.Target != nil {
		target = cp.Status.Target.Version
	}
	images, ok := versions.For(target)
	if !ok {
		message := "waiting for Lifecycle to set status.target.version"
		if target != "" {
			message = "status.target.version " + target + " is not a version Lodia runs"
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
	if cp.Status.Datastore == nil || len(cp.Status.Datastore.Endpoints) == 0 {
		waiting, err := r.wait(ctx, cp, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "waiting for the Datastore controller to publish status.datastore.endpoints"))
		return nil, waiting, err
	}
	config, err := configMaps(cp)
	if err != nil {
		return nil, nil, err
	}
	hashes, unhashed, err := r.hashes(ctx, cp, config)
	if err != nil {
		return nil, nil, err
	}
	if len(unhashed) > 0 {
		message := "waiting for PKI to write the Secrets the control plane reads once, with their data hash: " + strings.Join(unhashed, ", ")
		waiting, err := r.wait(ctx, cp, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, message))
		return nil, waiting, err
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
			endpoint:    endpoint,
			etcd:        cp.Status.Datastore.Endpoints,
			hashes:      hashes,
			held:        held,
		},
		config: config,
		note:   note,
	}, nil, nil
}

// objects renders what the pass applies, in that order, and the Deployments
// among them, which the apply fills with the API server's status.
func objects(in *inputs) ([]*appsv1.Deployment, []client.Object) {
	cp := in.rendering.cp
	deployments := in.rendering.deployments()
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

// writeObjects applies objects once no other owner keeps one of them. A non-nil
// outcome stops the pass at what it reports.
func (r *Reconciler) writeObjects(ctx context.Context, cp *lodiav1alpha1.ControlPlane, endpoint *lodiav1alpha1.EndpointStatus, objects []client.Object) (*outcome, error) {
	if held, requeueAfter, err := r.controlledElsewhere(ctx, cp, objects...); err != nil || held != nil {
		if err != nil {
			return nil, err
		}
		return &outcome{endpoint: endpoint, condition: *held, requeueAfter: requeueAfter}, nil
	}
	for _, obj := range objects {
		if err := r.apply(ctx, obj); err != nil {
			return refused(endpoint, obj, err)
		}
	}
	if cp.Spec.HighAvailability == nil {
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
	if restoring == nil {
		return nil, "", nil
	}
	var unmet []string
	if restoring.Release != lodiav1alpha1.RestoreReleased {
		unmet = append(unmet, "spec.restoreFrom.release is Held")
	}
	if cp.Status.Restore == nil {
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
// reports availability, it counts the Deployments this ControlPlane controls
// at the generation the condition names; otherwise it keeps a condition of an
// older generation and returns waiting for one of cp's.
func (r *Reconciler) wait(ctx context.Context, cp *lodiav1alpha1.ControlPlane, waiting metav1.Condition) (*metav1.Condition, error) {
	if err := live.Conditions(ctx, r.Reader, cp, lodiav1alpha1.ConditionAPIServerReady); err != nil {
		return nil, err
	}
	previous := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
	if previous == nil {
		return &waiting, nil
	}
	if slices.Contains([]string{lodiav1alpha1.ReasonReady, lodiav1alpha1.ReasonUnavailable, lodiav1alpha1.ReasonProgressDeadlineExceeded}, previous.Reason) {
		controlled, err := r.controlled(ctx, cp)
		if err != nil {
			return nil, err
		}
		_, note, err := r.release(ctx, cp)
		if err != nil {
			return nil, err
		}
		counted := availability(slices.Collect(maps.Values(controlled)), note, controllerManagerHeld(cp))
		counted.ObservedGeneration = previous.ObservedGeneration
		return &counted, nil
	}
	return new(statusapply.Wait(cp, previous, waiting)), nil
}

// unsupported names the fields this release does not render, so a ControlPlane that
// sets them is not Ready instead of running without them.
func unsupported(cp *lodiav1alpha1.ControlPlane) []string {
	var fields []string
	if cp.Spec.Layout == lodiav1alpha1.LayoutCompact {
		fields = append(fields, "layout Compact")
	}
	if a := cp.Spec.APIServer; a != nil {
		if a.Authentication != nil && len(a.Authentication.JWT) > 0 {
			fields = append(fields, "apiServer.authentication.jwt")
		}
		if a.Audit != nil {
			fields = append(fields, "apiServer.audit")
		}
		if a.Admission != nil {
			fields = append(fields, "apiServer.admission")
		}
	}
	if s := cp.Spec.Scheduler; s != nil && s.ConfigRef != nil {
		fields = append(fields, "scheduler.configRef")
	}
	if sa := cp.Spec.ServiceAccount; sa != nil && sa.PublicDiscovery {
		fields = append(fields, "serviceAccount.publicDiscovery")
	}
	return fields
}

// controlledElsewhere reports the objects under Workloads' names that another
// owner keeps, read from the cache, which holds the labelled ones: Workloads'
// own and a predecessor's. The garbage collector deletes a previous
// ControlPlane's, so that wait is short; any other owner's waits for a person.
func (r *Reconciler) controlledElsewhere(ctx context.Context, cp *lodiav1alpha1.ControlPlane, objects ...client.Object) (*metav1.Condition, time.Duration, error) {
	var held []string
	requeueAfter := 10 * time.Second
	for _, obj := range objects {
		current := cached(obj)
		if err := r.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
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
	default:
		m := &metav1.PartialObjectMetadata{}
		m.SetGroupVersionKind(obj.GetObjectKind().GroupVersionKind())
		return m
	}
}

// A node port endpoint keeps the address it was published with while any node
// still reports it as an InternalIP, Ready or not: the serving certificate,
// the kubeconfigs, the ServiceAccount issuer and every joined kubelet name it.
// A new one is the first Ready node's by name. No event reports a node, so it
// is checked again every minute.
func (r *Reconciler) endpoint(ctx context.Context, cp *lodiav1alpha1.ControlPlane, svc *corev1.Service) (*lodiav1alpha1.EndpointStatus, *metav1.Condition, time.Duration, error) {
	if cp.Spec.Endpoint.NodePort != nil {
		published := ""
		if e := cp.Status.Endpoint; e != nil {
			published = e.APIServer.Host
		}
		host, err := r.nodeAddress(ctx, published)
		if err != nil {
			return nil, nil, 0, err
		}
		if host == "" {
			waiting := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "no Ready node of the management cluster has an InternalIP")
			return nil, &waiting, time.Minute, nil
		}
		return &lodiav1alpha1.EndpointStatus{
			APIServer:    lodiav1alpha1.HostPort{Host: host, Port: nodePort(svc, kubernetesPort)},
			Konnectivity: lodiav1alpha1.HostPort{Host: host, Port: nodePort(svc, konnectivityPort)},
			InCluster:    lodiav1alpha1.HostPort{Host: host, Port: nodePort(svc, kubernetesPort)},
		}, nil, time.Minute, nil
	}
	ingress := svc.Status.LoadBalancer.Ingress
	i := slices.IndexFunc(ingress, func(in corev1.LoadBalancerIngress) bool { return in.IP != "" })
	switch {
	case len(ingress) == 0:
		waiting := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for an address of the load balancer of Service "+svc.Name)
		return nil, &waiting, 0, nil
	case i < 0:
		waiting := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonLoadBalancerHostnameUnsupported,
			fmt.Sprintf("the load balancer of Service %s has the host name %s and no IP, and the API server advertises an IP", svc.Name, ingress[0].Hostname))
		return nil, &waiting, 0, nil
	}
	return &lodiav1alpha1.EndpointStatus{
		APIServer:    lodiav1alpha1.HostPort{Host: ingress[i].IP, Port: lodiav1alpha1.APIServerServicePort},
		Konnectivity: lodiav1alpha1.HostPort{Host: ingress[i].IP, Port: konnectivityPort},
		InCluster:    lodiav1alpha1.HostPort{Host: ingress[i].IP, Port: kubernetesPort},
	}, nil, 0, nil
}

func (r *Reconciler) nodeAddress(ctx context.Context, published string) (string, error) {
	nodes := &corev1.NodeList{}
	if err := r.Reader.List(ctx, nodes); err != nil {
		return "", err
	}
	slices.SortFunc(nodes.Items, func(a, b corev1.Node) int { return strings.Compare(a.Name, b.Name) })
	first := ""
	for _, node := range nodes.Items {
		var internal []string
		for _, a := range node.Status.Addresses {
			if a.Type == corev1.NodeInternalIP {
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

// restartHashes are each component's hash of what it reads once: its
// restart-only Secrets by their data hashes and, for the API server, the
// ConfigMaps Workloads renders, so that a change to one rolls the component
// that reads it once and nothing else.
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
// one with no ready replica while Health holds its leader Lease.
func availability(deployments []*appsv1.Deployment, held string, leaseHeld bool) metav1.Condition {
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
		if leaseHeld && component == controllerManager && d.Status.ReadyReplicas == 0 {
			short = append(short, fmt.Sprintf("%s has 0 of %d replicas ready while Health holds its leader Lease", component, want))
			continue
		}
		if d.Status.ReadyReplicas < want {
			short = append(short, fmt.Sprintf("%s has %d of %d replicas ready", component, d.Status.ReadyReplicas, want))
		}
		unavailable = unavailable || d.Status.ReadyReplicas == 0
		if d.Status.ObservedGeneration == d.Generation && slices.ContainsFunc(d.Status.Conditions, func(c appsv1.DeploymentCondition) bool {
			return c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse && c.Reason == timedOutReason
		}) {
			stalled = append(stalled, "the rollout of Deployment "+d.Name+" passed its progress deadline")
		}
	}
	switch {
	case unavailable:
		return newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, strings.Join(short, "; "))
	case len(stalled) > 0:
		return newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonProgressDeadlineExceeded, strings.Join(append(stalled, short...), "; "))
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
// controls.
func (r *Reconciler) components(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*lodiav1alpha1.ComponentsStatus, error) {
	controlled, err := r.controlled(ctx, cp)
	if err != nil || len(controlled) == 0 {
		return nil, err
	}
	out := &lodiav1alpha1.ComponentsStatus{}
	for component, d := range controlled {
		status := &lodiav1alpha1.ComponentStatus{RolloutCount: rolloutCount(d)}
		switch component {
		case apiServer:
			out.APIServer = status
		case controllerManager:
			out.ControllerManager = status
		case scheduler:
			out.Scheduler = status
		}
	}
	return out, nil
}

// rolloutCount's updated counts, once the Deployment controller has observed
// the template, the available replicas of the template: all available ones
// less those of older templates, which a surge keeps available until the new
// ones are.
func rolloutCount(d *appsv1.Deployment) lodiav1alpha1.RolloutCount {
	s, total := d.Status, ptr.Deref(d.Spec.Replicas, 1)
	count := lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: total, Ready: min(s.ReadyReplicas, total)}}
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

func (r *Reconciler) apply(ctx context.Context, obj client.Object) error {
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return err
	}
	delete(content, "status")
	dropNulls(content)
	applied := &unstructured.Unstructured{Object: content}
	if err := r.Apply(ctx, client.ApplyConfigurationFromUnstructured(applied), client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply %s %s: %w", applied.GetKind(), applied.GetName(), err)
	}
	return runtime.DefaultUnstructuredConverter.FromUnstructured(applied.Object, obj)
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

// applyStatus applies APIServerReady and, in gateway mode, RoutesReady, each
// at cp's generation unless it names one, status.components and, once both
// addresses exist, status.endpoint whole; a nil endpoint or components drops a
// published one.
func (r *Reconciler) applyStatus(ctx context.Context, cp *lodiav1alpha1.ControlPlane, endpoint *lodiav1alpha1.EndpointStatus, components *lodiav1alpha1.ComponentsStatus, condition metav1.Condition, routes *metav1.Condition) error {
	conditions := []metav1.Condition{condition}
	if routes != nil {
		conditions = append(conditions, *routes)
	}
	for i := range conditions {
		if conditions[i].ObservedGeneration == 0 {
			conditions[i].ObservedGeneration = cp.Generation
		}
	}
	changed := !equality.Semantic.DeepEqual(cp.Status.Endpoint, endpoint) || !equality.Semantic.DeepEqual(cp.Status.Components, components)
	return statusapply.Apply(ctx, r.Client, r.Reader, FieldManager, cp, lodiav1alpha1.ControlPlaneStatus{Conditions: conditions, Endpoint: endpoint, Components: components}, changed)
}

func newCondition(status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: status, Reason: reason, Message: message}
}

func asRoutes(c metav1.Condition) *metav1.Condition {
	c.Type = lodiav1alpha1.ConditionRoutesReady
	return &c
}

func routesCondition(status metav1.ConditionStatus, reason, message string) *metav1.Condition {
	return asRoutes(newCondition(status, reason, message))
}

// keptRoutes is RoutesReady in gateway mode on a pass that stops at the
// Service, before the listener: as it was, or with none yet, or only the first
// pass's, False with the Service's reason and message, since Reconciling would
// come first in Ready and hide a Service held until a person acts.
func keptRoutes(cp *lodiav1alpha1.ControlPlane, service metav1.Condition) *metav1.Condition {
	if cp.Spec.Endpoint.Gateway == nil {
		return nil
	}
	if kept := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionRoutesReady); kept != nil && kept.Reason != lodiav1alpha1.ReasonReconciling {
		return kept.DeepCopy()
	}
	return asRoutes(service)
}
