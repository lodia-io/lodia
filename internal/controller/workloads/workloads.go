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
	"time"

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
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/ownership"
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
	// Reader reads what the label-filtered cache does not hold.
	Reader client.Reader
	// SignerImage is lodia-signer's image, the API server's sidecar.
	SignerImage string
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	readOnce := predicate.NewPredicateFuncs(func(secret client.Object) bool {
		cp, ok := secret.GetLabels()[lodiav1alpha1.ControlPlaneLabel]
		return ok && slices.Contains(restartOnlyNames(cp), secret.GetName())
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.ControlPlane{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}, builder.OnlyMetadata).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(controlPlaneOf), builder.WithPredicates(readOnce)).
		Complete(r)
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

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=list;watch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=list;watch;create;patch;delete
// +kubebuilder:rbac:groups="",resources=services;configmaps,verbs=list;watch;create;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=list;watch;create;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=list;watch;create;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=list

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cp.DeletionTimestamp.IsZero() || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	if meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady) == nil {
		first := newCondition(metav1.ConditionUnknown, lodiav1alpha1.ReasonReconciling, "the first pass has not finished")
		if err := r.applyStatus(ctx, cp, cp.Status.Endpoint, cp.Status.Components, first); err != nil {
			return ctrl.Result{}, err
		}
	}
	endpoint, condition, requeueAfter, err := r.reconcile(ctx, cp)
	if condition.Type == "" {
		return ctrl.Result{}, err
	}
	components, readErr := r.components(ctx, cp)
	if readErr != nil {
		return ctrl.Result{}, errors.Join(err, readErr)
	}
	if statusErr := r.applyStatus(ctx, cp, endpoint, components, condition); statusErr != nil || err != nil {
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// reconcile publishes the endpoint before it waits on anything or says what it
// does not build, since PKI and Health need it; a gateway endpoint has no
// Service to publish it from.
func (r *Reconciler) reconcile(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*lodiav1alpha1.EndpointStatus, metav1.Condition, time.Duration, error) {
	var notBuilt *metav1.Condition
	if fields := unsupported(cp); len(fields) > 0 {
		c := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported, "not built yet: "+strings.Join(fields, ", "))
		notBuilt = &c
	}
	if notBuilt != nil && cp.Spec.Endpoint.Gateway != nil {
		return nil, *notBuilt, 0, nil
	}
	svc := service(cp)
	if held, requeueAfter, err := r.controlledElsewhere(ctx, cp, svc); err != nil || held != nil {
		return nil, ptr.Deref(held, metav1.Condition{}), requeueAfter, err
	}
	if err := r.apply(ctx, svc); err != nil {
		return refused(cp.Status.Endpoint, svc, err)
	}
	endpoint, waiting, requeueAfter, err := r.endpoint(ctx, cp, svc)
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	if notBuilt != nil {
		return endpoint, *notBuilt, requeueAfter, nil
	}
	if waiting != nil {
		return endpoint, *waiting, requeueAfter, nil
	}
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
		return endpoint, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, message), requeueAfter, nil
	}
	if images.KonnectivityServer, err = konnectivityImage(cp, images.KonnectivityServer); err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	for _, input := range []struct{ condition, reason string }{
		{lodiav1alpha1.ConditionCertificatesReady, lodiav1alpha1.ReasonWaitingForCertificates},
		{lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ReasonWaitingForDatastore},
	} {
		if c := meta.FindStatusCondition(cp.Status.Conditions, input.condition); c == nil || c.Status != metav1.ConditionTrue || c.ObservedGeneration != cp.Generation {
			message := fmt.Sprintf("%s is not True for generation %d", input.condition, cp.Generation)
			return endpoint, newCondition(metav1.ConditionFalse, input.reason, message), requeueAfter, nil
		}
	}
	if cp.Status.Datastore == nil || len(cp.Status.Datastore.Endpoints) == 0 {
		return endpoint, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "waiting for the Datastore controller to publish status.datastore.endpoints"), requeueAfter, nil
	}
	config, err := configMaps(cp)
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	hashes, unhashed, err := r.hashes(ctx, cp, config)
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	if len(unhashed) > 0 {
		message := "waiting for PKI to write the Secrets the control plane reads once, with their data hash: " + strings.Join(unhashed, ", ")
		return endpoint, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, message), requeueAfter, nil
	}
	deployments := rendering{
		cp:          cp,
		images:      images,
		signerImage: r.SignerImage,
		advertise:   endpoint.APIServer.Host,
		issuer:      issuer(cp, endpoint.APIServer),
		etcd:        cp.Status.Datastore.Endpoints,
		hashes:      hashes,
	}.deployments()
	var objects []client.Object
	for _, c := range config {
		objects = append(objects, c)
	}
	for _, p := range networkPolicies(cp) {
		objects = append(objects, p)
	}
	for _, d := range deployments {
		objects = append(objects, d)
	}
	for _, b := range podDisruptionBudgets(cp) {
		objects = append(objects, b)
	}
	if held, requeueAfter, err := r.controlledElsewhere(ctx, cp, objects...); err != nil || held != nil {
		return endpoint, ptr.Deref(held, metav1.Condition{}), requeueAfter, err
	}
	for _, obj := range objects {
		if err := r.apply(ctx, obj); err != nil {
			return refused(endpoint, obj, err)
		}
	}
	if cp.Spec.HighAvailability == nil {
		if err := r.deletePodDisruptionBudgets(ctx, cp); err != nil {
			return nil, metav1.Condition{}, 0, err
		}
	}
	return endpoint, availability(deployments), requeueAfter, nil
}

// refused says that the API server refuses obj, so that the status no longer
// tells of the pass before, and returns the error for the backoff either way.
func refused(endpoint *lodiav1alpha1.EndpointStatus, obj client.Object, err error) (*lodiav1alpha1.EndpointStatus, metav1.Condition, time.Duration, error) {
	if !apistatus.Refused(err) {
		return nil, metav1.Condition{}, 0, err
	}
	message := fmt.Sprintf("the API server refuses %s %s; the reason is in Lodia's log", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetName())
	return endpoint, newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, message), 0, err
}

// unsupported names the fields phase 1 does not render, so a ControlPlane that
// sets them is not Ready instead of running without them.
func unsupported(cp *lodiav1alpha1.ControlPlane) []string {
	var fields []string
	if cp.Spec.Endpoint.Gateway != nil {
		fields = append(fields, "endpoint.gateway")
	}
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

// availability is True while each component has a ready replica.
func availability(deployments []*appsv1.Deployment) metav1.Condition {
	var short, stalled []string
	unavailable := false
	for _, d := range deployments {
		want := ptr.Deref(d.Spec.Replicas, 1)
		if d.Status.ReadyReplicas < want {
			short = append(short, fmt.Sprintf("%s has %d of %d replicas ready", d.Labels[lodiav1alpha1.ComponentLabel], d.Status.ReadyReplicas, want))
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

// components counts the replicas of the Deployments this ControlPlane
// controls, as the cache holds them.
func (r *Reconciler) components(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*lodiav1alpha1.ComponentsStatus, error) {
	var out *lodiav1alpha1.ComponentsStatus
	for _, component := range components {
		d := &appsv1.Deployment{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: deploymentName(cp.Name, component)}, d); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		if !metav1.IsControlledBy(d, cp) {
			continue
		}
		if out == nil {
			out = &lodiav1alpha1.ComponentsStatus{}
		}
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

// applyStatus applies APIServerReady, status.components and, once both
// addresses exist, status.endpoint whole; a nil endpoint or components drops
// a published one.
func (r *Reconciler) applyStatus(ctx context.Context, cp *lodiav1alpha1.ControlPlane, endpoint *lodiav1alpha1.EndpointStatus, components *lodiav1alpha1.ComponentsStatus, condition metav1.Condition) error {
	condition.ObservedGeneration = cp.Generation
	previous := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
	unchanged := previous != nil && previous.Status == condition.Status && previous.Reason == condition.Reason &&
		previous.Message == condition.Message && previous.ObservedGeneration == condition.ObservedGeneration
	if unchanged && sameEndpoint(cp.Status.Endpoint, endpoint) && equality.Semantic.DeepEqual(cp.Status.Components, components) {
		return nil
	}
	conditions := slices.Clone(cp.Status.Conditions)
	meta.SetStatusCondition(&conditions, condition)
	current, err := runtime.DefaultUnstructuredConverter.ToUnstructured(meta.FindStatusCondition(conditions, lodiav1alpha1.ConditionAPIServerReady))
	if err != nil {
		return err
	}
	status := map[string]any{"conditions": []any{current}}
	if endpoint != nil {
		e, err := runtime.DefaultUnstructuredConverter.ToUnstructured(endpoint)
		if err != nil {
			return err
		}
		status["endpoint"] = e
	}
	if components != nil {
		c, err := runtime.DefaultUnstructuredConverter.ToUnstructured(components)
		if err != nil {
			return err
		}
		status["components"] = c
	}
	controlPlane := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     status,
	}}
	return r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(controlPlane), client.FieldOwner(FieldManager), client.ForceOwnership)
}

func sameEndpoint(a, b *lodiav1alpha1.EndpointStatus) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func newCondition(status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: status, Reason: reason, Message: message}
}
