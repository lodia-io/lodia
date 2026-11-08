// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"golang.org/x/sync/semaphore"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/ownership"
)

// gateway checks the listener the parentRef names, publishes the endpoint at
// its port, and writes the routes once this ControlPlane owns its hostnames.
// It returns the endpoint, the wait of APIServerReady while there is none,
// and RoutesReady, empty on an error that stops the pass. A listener that
// cannot take the routes leaves those that exist and a published endpoint as
// they are: the endpoint never moves, since the ServiceAccount issuer follows
// its port and every kubelet dials it.
func (r *Reconciler) gateway(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*lodiav1alpha1.EndpointStatus, *metav1.Condition, metav1.Condition, error) {
	published := cp.Status.Endpoint
	gw, listener, reason, problem, err := r.listener(ctx, cp, published)
	if err != nil {
		return nil, nil, metav1.Condition{}, err
	}
	if problem != "" {
		routes := *asRoutes(newCondition(metav1.ConditionFalse, reason, problem))
		if published != nil {
			return published, nil, routes, nil
		}
		ref := cp.Spec.Endpoint.Gateway.ParentRef
		waiting := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint,
			fmt.Sprintf("no endpoint until listener %s of Gateway %s/%s can take the routes, which RoutesReady says", ref.SectionName, ref.Namespace, ref.Name))
		return nil, &waiting, routes, nil
	}
	endpoint := published
	if endpoint == nil {
		endpoint = gatewayEndpoint(cp, listener.Port)
	}
	routes, err := r.attach(ctx, cp, gw, listener.Port)
	return endpoint, nil, routes, err
}

func gatewayEndpoint(cp *lodiav1alpha1.ControlPlane, port int32) *lodiav1alpha1.EndpointStatus {
	hostnames := cp.Spec.Endpoint.Gateway.Hostnames
	advertise := lodiav1alpha1.DefaultAdvertiseAddress
	if a := cp.Spec.APIServer; a != nil && a.AdvertiseAddress != "" {
		advertise = a.AdvertiseAddress
	}
	return &lodiav1alpha1.EndpointStatus{
		APIServer:    lodiav1alpha1.HostPort{Host: string(hostnames.APIServer), Port: port},
		Konnectivity: lodiav1alpha1.HostPort{Host: string(hostnames.Konnectivity), Port: port},
		InCluster:    lodiav1alpha1.HostPort{Host: advertise, Port: kubernetesPort},
		Tunnel:       &lodiav1alpha1.HostPort{Host: string(hostnames.Tunnel), Port: port},
	}
}

// listener is the listener sectionName names on the cached Gateway, or the
// reason and the message that say why the routes cannot attach to it. Whether
// it admits the namespace is the Gateway's to answer, in the routes' status.
func (r *Reconciler) listener(ctx context.Context, cp *lodiav1alpha1.ControlPlane, published *lodiav1alpha1.EndpointStatus) (*gatewayv1.Gateway, gatewayv1.Listener, string, string, error) {
	ref := cp.Spec.Endpoint.Gateway.ParentRef
	if !r.GatewayAPI {
		return nil, gatewayv1.Listener{}, lodiav1alpha1.ReasonGatewayUnsupported,
			"the management cluster did not serve TLSRoute and Gateway at gateway.networking.k8s.io/v1 when the operator started, the only time it looks: restart the operator once they are served", nil
	}
	gw := &gatewayv1.Gateway{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, gw); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, gatewayv1.Listener{}, lodiav1alpha1.ReasonGatewayNotFound, fmt.Sprintf("Gateway %s/%s does not exist", ref.Namespace, ref.Name), nil
		}
		return nil, gatewayv1.Listener{}, "", "", err
	}
	unsupported := func(format string, args ...any) (*gatewayv1.Gateway, gatewayv1.Listener, string, string, error) {
		name := fmt.Sprintf("listener %s of Gateway %s/%s ", ref.SectionName, ref.Namespace, ref.Name)
		return nil, gatewayv1.Listener{}, lodiav1alpha1.ReasonGatewayUnsupported, name + fmt.Sprintf(format, args...), nil
	}
	i := slices.IndexFunc(gw.Spec.Listeners, func(l gatewayv1.Listener) bool { return string(l.Name) == ref.SectionName })
	if i < 0 {
		return unsupported("does not exist")
	}
	listener := gw.Spec.Listeners[i]
	if listener.Protocol != gatewayv1.TLSProtocolType {
		return unsupported("has protocol %s, not TLS", listener.Protocol)
	}
	if listener.TLS == nil || ptr.Deref(listener.TLS.Mode, "") != gatewayv1.TLSModePassthrough {
		return unsupported("is not in Passthrough mode")
	}
	for _, hostname := range hostnames(cp) {
		if l := ptr.Deref(listener.Hostname, ""); l != "" && string(l) != hostname && !matches(string(l), hostname) {
			return unsupported("has the hostname %s, which does not cover %s", l, hostname)
		}
	}
	if status := slices.IndexFunc(gw.Status.Listeners, func(s gatewayv1.ListenerStatus) bool { return string(s.Name) == ref.SectionName }); status >= 0 {
		s := gw.Status.Listeners[status]
		if !slices.ContainsFunc(s.SupportedKinds, func(k gatewayv1.RouteGroupKind) bool {
			return ptr.Deref(k.Group, gatewayv1.GroupName) == gatewayv1.GroupName && k.Kind == "TLSRoute"
		}) {
			return unsupported("does not take TLSRoutes, by its status")
		}
		for _, t := range []gatewayv1.ListenerConditionType{gatewayv1.ListenerConditionAccepted, gatewayv1.ListenerConditionProgrammed} {
			if c := meta.FindStatusCondition(s.Conditions, string(t)); c != nil && c.Status == metav1.ConditionFalse {
				return unsupported("is not %s: %s", t, c.Reason)
			}
		}
	}
	if published != nil && listener.Port != published.APIServer.Port {
		return unsupported("is at port %d, and the endpoint, which never moves, was published at port %d", listener.Port, published.APIServer.Port)
	}
	return gw, listener, "", "", nil
}

func hostnames(cp *lodiav1alpha1.ControlPlane) []string {
	h := cp.Spec.Endpoint.Gateway.Hostnames
	return []string{string(h.APIServer), string(h.Konnectivity), string(h.Tunnel)}
}

func gatewayOf(cp *lodiav1alpha1.ControlPlane) types.NamespacedName {
	ref := cp.Spec.Endpoint.Gateway.ParentRef
	return types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}
}

type routeProblem struct{ reason, message string }

// The order of RoutesReady's reasons when several routes have problems.
var routeReasons = []string{
	lodiav1alpha1.ReasonHostnameConflict, lodiav1alpha1.ReasonObjectsControlledElsewhere,
	lodiav1alpha1.ReasonRefused, lodiav1alpha1.ReasonRouteNotAccepted,
}

// attach writes this ControlPlane's routes while it owns all three hostnames,
// and deletes those it controls otherwise. Every write goes through write's
// re-check; a route the cache holds as rendered gets none, so a pass that
// changes nothing writes nothing on the Gateway. The error of a refused route
// comes with its condition, for the backoff.
func (r *Reconciler) attach(ctx context.Context, cp *lodiav1alpha1.ControlPlane, gw *gatewayv1.Gateway, port int32) (metav1.Condition, error) {
	owners, err := r.owners(ctx, cp)
	if err != nil {
		return metav1.Condition{}, err
	}
	if len(owners) > 0 {
		if err := r.deleteRoutes(ctx, cp); err != nil {
			return metav1.Condition{}, err
		}
		var named []string
		for _, hostname := range hostnames(cp) {
			if owner, ok := owners[hostname]; ok {
				named = append(named, fmt.Sprintf("%s by ControlPlane %s/%s (UID %s)", hostname, owner.Namespace, owner.Name, owner.UID))
			}
		}
		return *asRoutes(newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHostnameConflict,
			fmt.Sprintf("hostnames named first on Gateway %s/%s by another ControlPlane: %s; no route of this ControlPlane is applied until they are free", gw.Namespace, gw.Name, strings.Join(named, ", ")))), nil
	}
	var problems []routeProblem
	var refusals error
	for _, desired := range routes(cp) {
		current := &gatewayv1.TLSRoute{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(desired), current); err != nil {
			if !apierrors.IsNotFound(err) {
				return metav1.Condition{}, err
			}
			current = nil
		}
		if current != nil {
			if owner := ownership.ControlledElsewhere(current, cp); owner != nil {
				problems = append(problems, heldElsewhere(desired, owner))
				continue
			}
		}
		if current == nil || !unchanged(current, desired) {
			problem, err := r.write(ctx, cp, gw, port, desired)
			switch {
			case apistatus.Refused(err):
				problems = append(problems, routeProblem{lodiav1alpha1.ReasonRefused, fmt.Sprintf("the API server refuses TLSRoute %s; the reason is in Lodia's log", desired.Name)})
				refusals = errors.Join(refusals, err)
				continue
			case err != nil:
				return metav1.Condition{}, err
			case problem != nil:
				problems = append(problems, *problem)
				continue
			}
			current = desired
		}
		if message := notAccepted(current, cp.Spec.Endpoint.Gateway.ParentRef); message != "" {
			problems = append(problems, routeProblem{lodiav1alpha1.ReasonRouteNotAccepted, message})
		}
	}
	if len(problems) == 0 {
		return *asRoutes(newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted,
			fmt.Sprintf("every route is applied, Accepted and ResolvedRefs by Gateway %s/%s", gw.Namespace, gw.Name))), nil
	}
	reason := routeReasons[slices.IndexFunc(routeReasons, func(reason string) bool {
		return slices.ContainsFunc(problems, func(p routeProblem) bool { return p.reason == reason })
	})]
	var messages []string
	for _, p := range problems {
		messages = append(messages, p.message)
	}
	return *asRoutes(newCondition(metav1.ConditionFalse, reason, strings.Join(messages, "; "))), refusals
}

func heldElsewhere(route *gatewayv1.TLSRoute, owner *metav1.OwnerReference) routeProblem {
	return routeProblem{lodiav1alpha1.ReasonObjectsControlledElsewhere,
		fmt.Sprintf("another owner keeps TLSRoute %s by %s %s (UID %s); Workloads writes no route over it", route.Name, owner.Kind, owner.Name, owner.UID)}
}

// owners are, by hostname, the ControlPlanes that own one of cp's hostnames:
// the first, by creationTimestamp, namespace and name, among those not being
// deleted that name it on the same Gateway, as the cache holds them.
func (r *Reconciler) owners(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (map[string]*lodiav1alpha1.ControlPlane, error) {
	list := &lodiav1alpha1.ControlPlaneList{}
	if err := r.List(ctx, list); err != nil {
		return nil, err
	}
	mine := hostnames(cp)
	out := map[string]*lodiav1alpha1.ControlPlane{}
	for i := range list.Items {
		other := &list.Items[i]
		if other.UID == cp.UID || !other.DeletionTimestamp.IsZero() || other.Spec.Endpoint.Gateway == nil || gatewayOf(other) != gatewayOf(cp) || !first(other, cp) {
			continue
		}
		for _, hostname := range hostnames(other) {
			if slices.Contains(mine, hostname) && (out[hostname] == nil || first(other, out[hostname])) {
				out[hostname] = other
			}
		}
	}
	return out, nil
}

func first(a, b *lodiav1alpha1.ControlPlane) bool {
	switch {
	case !a.CreationTimestamp.Equal(&b.CreationTimestamp):
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	case a.Namespace != b.Namespace:
		return a.Namespace < b.Namespace
	}
	return a.Name < b.Name
}

func (r *Reconciler) deleteRoutes(ctx context.Context, cp *lodiav1alpha1.ControlPlane) error {
	for _, route := range routes(cp) {
		current := &gatewayv1.TLSRoute{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(route), current); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}
		if !metav1.IsControlledBy(current, cp) {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, current, client.Preconditions{UID: &current.UID})); err != nil {
			return fmt.Errorf("delete TLSRoute %s: %w", current.Name, err)
		}
	}
	return nil
}

// unchanged says whether the cached route is the render: its spec whole, since
// an extra hostname is a second SNI and a weight of 0 stops the traffic,
// Lodia's labels among its own, and Lodia's owner reference.
func unchanged(current, desired *gatewayv1.TLSRoute) bool {
	if !equality.Semantic.DeepEqual(current.Spec, desired.Spec) {
		return false
	}
	for key, value := range desired.Labels {
		if v, ok := current.Labels[key]; !ok || v != value {
			return false
		}
	}
	own := desired.OwnerReferences[0]
	i := slices.IndexFunc(current.OwnerReferences, func(ref metav1.OwnerReference) bool { return ref.UID == own.UID })
	return i >= 0 && equality.Semantic.DeepEqual(current.OwnerReferences[i], own)
}

// write applies route unless another route on its Gateway holds its hostname,
// or another owner keeps a route under its name, as the API server holds them:
// it reads the routes without the cache under a semaphore of the Gateway, so
// that two ControlPlanes never both create one hostname, whatever the cache
// holds. The order among ControlPlanes comes from the cache, so a lagging one
// at worst lets a younger one create first, which then deletes its routes.
func (r *Reconciler) write(ctx context.Context, cp *lodiav1alpha1.ControlPlane, gw *gatewayv1.Gateway, port int32, route *gatewayv1.TLSRoute) (*routeProblem, error) {
	writes := r.semaphore(client.ObjectKeyFromObject(gw))
	if err := writes.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer writes.Release(1)
	list := &gatewayv1.TLSRouteList{}
	if err := r.Reader.List(ctx, list); err != nil {
		return nil, err
	}
	hostname := string(route.Spec.Hostnames[0])
	for i := range list.Items {
		other := &list.Items[i]
		if other.Namespace == route.Namespace && other.Name == route.Name {
			if owner := ownership.ControlledElsewhere(other, cp); owner != nil {
				problem := heldElsewhere(route, owner)
				return &problem, nil
			}
			continue
		}
		if slices.Contains(held(other, gw, port), hostname) {
			return &routeProblem{lodiav1alpha1.ReasonHostnameConflict,
				fmt.Sprintf("hostname %s is held by TLSRoute %s/%s on Gateway %s/%s; Workloads writes no route for it until that one is gone", hostname, other.Namespace, other.Name, gw.Namespace, gw.Name)}, nil
		}
	}
	return nil, r.apply(ctx, route)
}

func (r *Reconciler) semaphore(gateway types.NamespacedName) *semaphore.Weighted {
	r.writes.Lock()
	defer r.writes.Unlock()
	if r.gateways == nil {
		r.gateways = map[types.NamespacedName]*semaphore.Weighted{}
	}
	if _, ok := r.gateways[gateway]; !ok {
		r.gateways[gateway] = semaphore.NewWeighted(1)
	}
	return r.gateways[gateway]
}

// notAccepted says why route is not Accepted and ResolvedRefs at its
// generation on the parent entry of ref, or nothing when it is.
func notAccepted(route *gatewayv1.TLSRoute, ref lodiav1alpha1.ParentReference) string {
	gateway := ref.Namespace + "/" + ref.Name
	i := slices.IndexFunc(route.Status.Parents, func(p gatewayv1.RouteParentStatus) bool {
		return ptr.Deref(p.ParentRef.Group, gatewayv1.GroupName) == gatewayv1.GroupName && ptr.Deref(p.ParentRef.Kind, "Gateway") == "Gateway" &&
			string(ptr.Deref(p.ParentRef.Namespace, gatewayv1.Namespace(route.Namespace))) == ref.Namespace && string(p.ParentRef.Name) == ref.Name &&
			string(ptr.Deref(p.ParentRef.SectionName, "")) == ref.SectionName
	})
	if i < 0 {
		return fmt.Sprintf("TLSRoute %s has no status from Gateway %s yet", route.Name, gateway)
	}
	for _, t := range []gatewayv1.RouteConditionType{gatewayv1.RouteConditionAccepted, gatewayv1.RouteConditionResolvedRefs} {
		switch c := meta.FindStatusCondition(route.Status.Parents[i].Conditions, string(t)); {
		case c == nil:
			return fmt.Sprintf("TLSRoute %s has no %s from Gateway %s yet", route.Name, t, gateway)
		case c.Status != metav1.ConditionTrue:
			return fmt.Sprintf("TLSRoute %s is not %s by Gateway %s: %s", route.Name, t, gateway, c.Reason)
		case c.ObservedGeneration != route.Generation:
			return fmt.Sprintf("TLSRoute %s's %s from Gateway %s describes generation %d, not %d", route.Name, t, gateway, c.ObservedGeneration, route.Generation)
		}
	}
	return ""
}

// held are the names route holds on gw at port: for each parentRef naming gw,
// its hostnames intersected with the hostname of each listener at that port
// the parentRef selects, by sectionName and port, or every one if neither is
// set, since the Gateway merges the listeners of a port into one. That is
// every SNI Envoy Gateway renders for the route and, so that no holder is
// missed, a name another listener at the port names exactly, which it drops
// (https://github.com/envoyproxy/gateway/blob/v1.9.3/internal/gatewayapi/helpers.go#L361-L381).
// A wildcard holds no name, and a route the Gateway has not accepted holds
// its names too.
func held(route *gatewayv1.TLSRoute, gw *gatewayv1.Gateway, port int32) []string {
	var out []string
	for _, p := range route.Spec.ParentRefs {
		if ptr.Deref(p.Group, gatewayv1.GroupName) != gatewayv1.GroupName || ptr.Deref(p.Kind, "Gateway") != "Gateway" ||
			string(ptr.Deref(p.Namespace, gatewayv1.Namespace(route.Namespace))) != gw.Namespace || string(p.Name) != gw.Name {
			continue
		}
		for _, l := range gw.Spec.Listeners {
			if l.Port != port || p.SectionName != nil && *p.SectionName != l.Name || p.Port != nil && *p.Port != l.Port {
				continue
			}
			out = append(out, names(route.Spec.Hostnames, string(ptr.Deref(l.Hostname, "")))...)
		}
	}
	return out
}

// names are the exact names in the intersection of a route's hostnames with a
// listener's, as Envoy Gateway intersects them
// (https://github.com/envoyproxy/gateway/blob/v1.9.3/internal/gatewayapi/helpers.go#L296-L359).
func names(route []gatewayv1.Hostname, listener string) []string {
	exact := listener != "" && !strings.HasPrefix(listener, "*")
	if len(route) == 0 && exact {
		return []string{listener}
	}
	var out []string
	for _, h := range route {
		hostname := string(h)
		switch {
		case !strings.HasPrefix(hostname, "*") && (listener == "" || listener == hostname || matches(listener, hostname)):
			out = append(out, hostname)
		case strings.HasPrefix(hostname, "*") && exact && matches(hostname, listener):
			out = append(out, listener)
		}
	}
	return out
}

// matches says whether the wildcard *.suffix covers the name, by one label or
// more, as the Gateway API's hostnames do.
func matches(wildcard, name string) bool {
	suffix, ok := strings.CutPrefix(wildcard, "*")
	return ok && strings.HasSuffix(name, suffix) && len(name) > len(suffix) && !strings.HasPrefix(name, "*")
}

// holders are the ControlPlanes whose hostname route holds, by the rule of
// held, so that its every change, its deletion included, brings their pass.
func (r *Reconciler) holders(ctx context.Context, obj client.Object) []reconcile.Request {
	route, ok := obj.(*gatewayv1.TLSRoute)
	if !ok {
		return nil
	}
	list := &lodiav1alpha1.ControlPlaneList{}
	if err := r.List(ctx, list); err != nil {
		logf.FromContext(ctx).Error(err, "list the ControlPlanes whose hostname a TLSRoute may hold", "route", client.ObjectKeyFromObject(route))
		return nil
	}
	gateways := map[types.NamespacedName]*gatewayv1.Gateway{}
	var out []reconcile.Request
	for i := range list.Items {
		cp := &list.Items[i]
		if cp.Spec.Endpoint.Gateway == nil {
			continue
		}
		key := gatewayOf(cp)
		gw, seen := gateways[key]
		if !seen {
			gw = &gatewayv1.Gateway{}
			if err := r.Get(ctx, key, gw); err != nil {
				if !apierrors.IsNotFound(err) {
					logf.FromContext(ctx).Error(err, "get the Gateway of a ControlPlane", "gateway", key)
				}
				gw = nil
			}
			gateways[key] = gw
		}
		if gw == nil {
			continue
		}
		section := cp.Spec.Endpoint.Gateway.ParentRef.SectionName
		l := slices.IndexFunc(gw.Spec.Listeners, func(l gatewayv1.Listener) bool { return string(l.Name) == section })
		if l < 0 {
			continue
		}
		if names := held(route, gw, gw.Spec.Listeners[l].Port); slices.ContainsFunc(hostnames(cp), func(h string) bool { return slices.Contains(names, h) }) {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		}
	}
	return out
}

func (r *Reconciler) parented(ctx context.Context, gateway client.Object) []reconcile.Request {
	list := &lodiav1alpha1.ControlPlaneList{}
	if err := r.List(ctx, list); err != nil {
		logf.FromContext(ctx).Error(err, "list the ControlPlanes on a Gateway", "gateway", client.ObjectKeyFromObject(gateway))
		return nil
	}
	var out []reconcile.Request
	for i := range list.Items {
		if cp := &list.Items[i]; cp.Spec.Endpoint.Gateway != nil && gatewayOf(cp) == client.ObjectKeyFromObject(gateway) {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		}
	}
	return out
}

// sharers are the ControlPlanes that name one of obj's hostnames on its
// Gateway, whose owner its creation or deletion may change.
func (r *Reconciler) sharers(ctx context.Context, obj client.Object) []reconcile.Request {
	cp, ok := obj.(*lodiav1alpha1.ControlPlane)
	if !ok || cp.Spec.Endpoint.Gateway == nil {
		return nil
	}
	list := &lodiav1alpha1.ControlPlaneList{}
	if err := r.List(ctx, list); err != nil {
		logf.FromContext(ctx).Error(err, "list the ControlPlanes that may share a hostname", "controlPlane", client.ObjectKeyFromObject(cp))
		return nil
	}
	mine := hostnames(cp)
	var out []reconcile.Request
	for i := range list.Items {
		other := &list.Items[i]
		if other.UID == cp.UID || other.Spec.Endpoint.Gateway == nil || gatewayOf(other) != gatewayOf(cp) {
			continue
		}
		if slices.ContainsFunc(hostnames(other), func(h string) bool { return slices.Contains(mine, h) }) {
			out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(other)})
		}
	}
	return out
}

var createdOrDeleted = predicate.Funcs{
	UpdateFunc:  func(event.UpdateEvent) bool { return false },
	GenericFunc: func(event.GenericEvent) bool { return false },
}

// listenersChanged passes a Gateway's new generation and a change of a
// listener's conditions or supportedKinds, what the listener check reads, but
// not attachedRoutes, which changes with every route of every tenant.
var listenersChanged = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		before, okBefore := e.ObjectOld.(*gatewayv1.Gateway)
		after, okAfter := e.ObjectNew.(*gatewayv1.Gateway)
		return !okBefore || !okAfter || before.Generation != after.Generation || !equality.Semantic.DeepEqual(listenerStatus(before), listenerStatus(after))
	},
}

func listenerStatus(gw *gatewayv1.Gateway) []gatewayv1.ListenerStatus {
	out := make([]gatewayv1.ListenerStatus, 0, len(gw.Status.Listeners))
	for _, s := range gw.Status.Listeners {
		s.AttachedRoutes = 0
		out = append(out, s)
	}
	return out
}
