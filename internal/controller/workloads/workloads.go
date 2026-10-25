// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package workloads is the Workloads controller.
package workloads

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/versions"
)

const (
	Name         = "workloads"
	FieldManager = "brygga/" + Name
)

// Messages are built from the state alone, never from an error, so a pass
// that changes nothing writes nothing.
const (
	ReasonReconciling                     = "Reconciling"
	ReasonGatewayUnsupported              = "GatewayUnsupported"
	ReasonUnsupported                     = "Unsupported"
	ReasonWaitingForEndpoint              = "WaitingForEndpoint"
	ReasonLoadBalancerHostnameUnsupported = "LoadBalancerHostnameUnsupported"
	ReasonWaitingForTarget                = "WaitingForTarget"
	ReasonWaitingForCertificates          = "WaitingForCertificates"
	ReasonWaitingForDatastore             = "WaitingForDatastore"
	ReasonRollingOut                      = "RollingOut"
	ReasonReady                           = "Ready"
)

type Reconciler struct {
	client.Client
	// Reader reads what the label-filtered cache does not hold.
	Reader client.Reader
	// SignerImage is brygga-signer's image, the API server's sidecar.
	SignerImage string
	// DruidNamespace is where etcd-druid runs, which the etcd NetworkPolicy
	// admits.
	DruidNamespace string
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	readOnce := predicate.NewPredicateFuncs(func(secret client.Object) bool {
		cp, ok := secret.GetLabels()[bryggav1alpha1.ControlPlaneLabel]
		return ok && slices.Contains(restartOnlyNames(cp), secret.GetName())
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&bryggav1alpha1.ControlPlane{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}, builder.OnlyMetadata).
		Owns(&networkingv1.NetworkPolicy{}).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(controlPlaneOf), builder.WithPredicates(readOnce)).
		Complete(r)
}

func controlPlaneOf(_ context.Context, secret client.Object) []reconcile.Request {
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: secret.GetNamespace(), Name: secret.GetLabels()[bryggav1alpha1.ControlPlaneLabel]}}}
}

func restartOnlyNames(cp string) []string {
	var names []string
	for _, suffixes := range restartOnly {
		for _, suffix := range suffixes {
			names = append(names, cp+"-"+suffix)
		}
	}
	return names
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &bryggav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cp.DeletionTimestamp.IsZero() || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	if meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionAPIServerReady) == nil {
		first := newCondition(metav1.ConditionUnknown, ReasonReconciling, "the first pass has not finished")
		if err := r.applyStatus(ctx, cp, cp.Status.Endpoint, first); err != nil {
			return ctrl.Result{}, err
		}
	}
	endpoint, condition, requeueAfter, err := r.reconcile(ctx, cp)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.applyStatus(ctx, cp, endpoint, condition); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// reconcile publishes the endpoint before it waits on anything, since PKI
// needs it for the serving certificate.
func (r *Reconciler) reconcile(ctx context.Context, cp *bryggav1alpha1.ControlPlane) (*bryggav1alpha1.EndpointStatus, metav1.Condition, time.Duration, error) {
	if cp.Spec.Endpoint.Gateway != nil {
		return nil, newCondition(metav1.ConditionFalse, ReasonGatewayUnsupported, "endpoint.gateway is not built yet: use loadBalancer or nodePort"), 0, nil
	}
	if fields := unsupported(cp); len(fields) > 0 {
		return nil, newCondition(metav1.ConditionFalse, ReasonUnsupported, "not built yet: "+strings.Join(fields, ", ")), 0, nil
	}
	svc := service(cp)
	if err := r.apply(ctx, svc); err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	endpoint, waiting, requeueAfter, err := r.endpoint(ctx, cp, svc)
	if err != nil || waiting != nil {
		return endpoint, ptr.Deref(waiting, metav1.Condition{}), requeueAfter, err
	}
	target := ""
	if cp.Status.Target != nil {
		target = cp.Status.Target.Version
	}
	images, ok := versions.For(target)
	if !ok {
		message := "waiting for Lifecycle to set status.target.version"
		if target != "" {
			message = "status.target.version " + target + " is not a version Brygga runs"
		}
		return endpoint, newCondition(metav1.ConditionFalse, ReasonWaitingForTarget, message), requeueAfter, nil
	}
	for _, input := range []struct{ condition, reason string }{
		{bryggav1alpha1.ConditionCertificatesReady, ReasonWaitingForCertificates},
		{bryggav1alpha1.ConditionDatastoreReady, ReasonWaitingForDatastore},
	} {
		if c := meta.FindStatusCondition(cp.Status.Conditions, input.condition); c == nil || c.Status != metav1.ConditionTrue || c.ObservedGeneration != cp.Generation {
			message := fmt.Sprintf("%s is not True for generation %d", input.condition, cp.Generation)
			return endpoint, newCondition(metav1.ConditionFalse, input.reason, message), requeueAfter, nil
		}
	}
	egress, err := egressSelector()
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	authn, err := authentication()
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	hashes, missing, err := r.hashes(ctx, cp, egress)
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	if len(missing) > 0 {
		message := "Secrets the control plane mounts do not exist: " + strings.Join(missing, ", ")
		return endpoint, newCondition(metav1.ConditionFalse, ReasonWaitingForCertificates, message), requeueAfter, nil
	}
	objects := []client.Object{
		configMap(cp, "egress-selector", egressFile, egress),
		configMap(cp, "authentication-config", authenticationFile, authn),
		networkPolicy(cp, r.DruidNamespace),
	}
	rendered := rendering{cp: cp, images: images, signerImage: r.SignerImage, endpoint: endpointInputs{
		advertise:  endpoint.APIServer.Host,
		securePort: securePort(cp, svc),
		issuer:     issuer(cp, endpoint.APIServer),
	}, hashes: hashes}.deployments()
	for _, obj := range objects {
		if err := r.apply(ctx, obj); err != nil {
			return nil, metav1.Condition{}, 0, err
		}
	}
	for _, d := range rendered {
		if err := r.apply(ctx, d); err != nil {
			return nil, metav1.Condition{}, 0, err
		}
	}
	return endpoint, readiness(target, rendered), requeueAfter, nil
}

// unsupported names the fields phase 1 does not render, so a ControlPlane that
// sets them is not Ready instead of running without them.
func unsupported(cp *bryggav1alpha1.ControlPlane) []string {
	var fields []string
	if cp.Spec.Layout == bryggav1alpha1.LayoutCompact {
		fields = append(fields, "layout Compact")
	}
	if cp.Spec.HighAvailability != nil {
		fields = append(fields, "highAvailability")
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
	return fields
}

// A node port endpoint is the first Ready management node by name, which no
// event reports, so it is checked again every minute.
func (r *Reconciler) endpoint(ctx context.Context, cp *bryggav1alpha1.ControlPlane, svc *corev1.Service) (*bryggav1alpha1.EndpointStatus, *metav1.Condition, time.Duration, error) {
	endpoint := &bryggav1alpha1.EndpointStatus{Internal: &bryggav1alpha1.HostPort{Host: svc.Name + "." + cp.Namespace + ".svc", Port: apiPort}}
	if cp.Spec.Endpoint.NodePort != nil {
		host, err := r.nodeAddress(ctx)
		if err != nil {
			return nil, nil, 0, err
		}
		if host == "" {
			waiting := newCondition(metav1.ConditionFalse, ReasonWaitingForEndpoint, "no Ready node of the management cluster has an InternalIP")
			return endpoint, &waiting, time.Minute, nil
		}
		endpoint.APIServer = &bryggav1alpha1.HostPort{Host: host, Port: nodePort(svc, kubernetesPort)}
		endpoint.Konnectivity = &bryggav1alpha1.HostPort{Host: host, Port: nodePort(svc, konnectivityPort)}
		return endpoint, nil, time.Minute, nil
	}
	ingress := svc.Status.LoadBalancer.Ingress
	i := slices.IndexFunc(ingress, func(in corev1.LoadBalancerIngress) bool { return in.IP != "" })
	switch {
	case len(ingress) == 0:
		waiting := newCondition(metav1.ConditionFalse, ReasonWaitingForEndpoint, "waiting for an address of the load balancer of Service "+svc.Name)
		return endpoint, &waiting, 0, nil
	case i < 0:
		waiting := newCondition(metav1.ConditionFalse, ReasonLoadBalancerHostnameUnsupported,
			fmt.Sprintf("the load balancer of Service %s has the host name %s and no IP, and the API server advertises an IP", svc.Name, ingress[0].Hostname))
		return endpoint, &waiting, 0, nil
	}
	endpoint.APIServer = &bryggav1alpha1.HostPort{Host: ingress[i].IP, Port: apiPort}
	endpoint.Konnectivity = &bryggav1alpha1.HostPort{Host: ingress[i].IP, Port: konnectivityPort}
	return endpoint, nil, 0, nil
}

func (r *Reconciler) nodeAddress(ctx context.Context) (string, error) {
	nodes := &corev1.NodeList{}
	if err := r.Reader.List(ctx, nodes); err != nil {
		return "", err
	}
	slices.SortFunc(nodes.Items, func(a, b corev1.Node) int { return strings.Compare(a.Name, b.Name) })
	for _, node := range nodes.Items {
		ready := slices.ContainsFunc(node.Status.Conditions, func(c corev1.NodeCondition) bool {
			return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
		})
		if i := slices.IndexFunc(node.Status.Addresses, func(a corev1.NodeAddress) bool { return a.Type == corev1.NodeInternalIP }); ready && i >= 0 {
			return node.Status.Addresses[i].Address, nil
		}
	}
	return "", nil
}

func nodePort(svc *corev1.Service, port int32) int32 {
	if i := slices.IndexFunc(svc.Spec.Ports, func(p corev1.ServicePort) bool { return p.Port == port }); i >= 0 {
		return svc.Spec.Ports[i].NodePort
	}
	return 0
}

// securePort is the node port of 6443 in nodePort mode: the API server
// advertises the node's address and its own port into the kubernetes
// Service's endpoints, which the tenant's nodes reach only through the node
// port.
func securePort(cp *bryggav1alpha1.ControlPlane, svc *corev1.Service) int32 {
	if cp.Spec.Endpoint.NodePort != nil {
		return nodePort(svc, kubernetesPort)
	}
	return kubernetesPort
}

func issuer(cp *bryggav1alpha1.ControlPlane, api *bryggav1alpha1.HostPort) string {
	if sa := cp.Spec.ServiceAccount; sa != nil && sa.Issuer != "" {
		return sa.Issuer
	}
	return "https://" + strings.TrimSuffix(net.JoinHostPort(api.Host, strconv.Itoa(int(api.Port))), ":"+strconv.Itoa(apiPort))
}

// hashes are the restart-only inputs of each component, so that a renewed one
// rolls the component that reads it once and nothing else.
func (r *Reconciler) hashes(ctx context.Context, cp *bryggav1alpha1.ControlPlane, egress []byte) (map[string]string, []string, error) {
	out := map[string]string{}
	var missing []string
	for _, component := range slices.Sorted(maps.Keys(restartOnly)) {
		h := sha256.New()
		if component == apiServer {
			h.Write(egress)
		}
		for _, suffix := range restartOnly[component] {
			secret := &corev1.Secret{}
			name := cp.Name + "-" + suffix
			if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, secret); err != nil {
				if apierrors.IsNotFound(err) {
					missing = append(missing, name)
					continue
				}
				return nil, nil, err
			}
			for _, key := range slices.Sorted(maps.Keys(secret.Data)) {
				for _, field := range [][]byte{[]byte(name), []byte(key), secret.Data[key]} {
					h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(field))))
					h.Write(field)
				}
			}
		}
		out[component] = hex.EncodeToString(h.Sum(nil))
	}
	slices.Sort(missing)
	return out, missing, nil
}

func readiness(version string, deployments []*appsv1.Deployment) metav1.Condition {
	for _, d := range deployments {
		want := ptr.Deref(d.Spec.Replicas, 1)
		switch {
		case d.Status.ObservedGeneration < d.Generation:
			return newCondition(metav1.ConditionFalse, ReasonRollingOut,
				fmt.Sprintf("the Deployment controller has not observed generation %d of Deployment %s", d.Generation, d.Name))
		case d.Status.UpdatedReplicas != want || d.Status.ReadyReplicas != want || d.Status.Replicas != want:
			return newCondition(metav1.ConditionFalse, ReasonRollingOut,
				fmt.Sprintf("Deployment %s: %d of %d replicas updated, %d ready, %d in all", d.Name, d.Status.UpdatedReplicas, want, d.Status.ReadyReplicas, d.Status.Replicas))
		}
	}
	return newCondition(metav1.ConditionTrue, ReasonReady,
		fmt.Sprintf("kube-apiserver, kube-controller-manager and kube-scheduler run %s, every replica updated and ready", version))
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

func (r *Reconciler) applyStatus(ctx context.Context, cp *bryggav1alpha1.ControlPlane, endpoint *bryggav1alpha1.EndpointStatus, condition metav1.Condition) error {
	condition.ObservedGeneration = cp.Generation
	previous := meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionAPIServerReady)
	unchanged := previous != nil && previous.Status == condition.Status && previous.Reason == condition.Reason &&
		previous.Message == condition.Message && previous.ObservedGeneration == condition.ObservedGeneration
	if unchanged && sameEndpoint(cp.Status.Endpoint, endpoint) {
		return nil
	}
	conditions := slices.Clone(cp.Status.Conditions)
	meta.SetStatusCondition(&conditions, condition)
	current, err := runtime.DefaultUnstructuredConverter.ToUnstructured(meta.FindStatusCondition(conditions, bryggav1alpha1.ConditionAPIServerReady))
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
	controlPlane := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": bryggav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     status,
	}}
	return r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(controlPlane), client.FieldOwner(FieldManager), client.ForceOwnership)
}

func sameEndpoint(a, b *bryggav1alpha1.EndpointStatus) bool {
	if a == nil || b == nil {
		return a == b
	}
	return ptr.Equal(a.Internal, b.Internal) && ptr.Equal(a.APIServer, b.APIServer) && ptr.Equal(a.Konnectivity, b.Konnectivity)
}

func newCondition(status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: bryggav1alpha1.ConditionAPIServerReady, Status: status, Reason: reason, Message: message}
}
