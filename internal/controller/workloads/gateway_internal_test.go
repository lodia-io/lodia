// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

var at = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := gatewayv1.Install(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func ranked(namespace, name string, created time.Duration, gateway, api, konnectivity, tunnel string) *lodiav1alpha1.ControlPlane {
	return &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID(namespace + "/" + name), CreationTimestamp: metav1.NewTime(at.Add(created))},
		Spec: lodiav1alpha1.ControlPlaneSpec{Endpoint: lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
			ParentRef: lodiav1alpha1.ParentReference{Namespace: "gateways", Name: gateway, SectionName: "tls"},
			Hostnames: lodiav1alpha1.GatewayHostnames{APIServer: lodiav1alpha1.Hostname(api), Konnectivity: lodiav1alpha1.Hostname(konnectivity), Tunnel: lodiav1alpha1.Hostname(tunnel)},
		}}},
	}
}

func TestAHostnameBelongsToTheFirstByAgeNamespaceAndName(t *testing.T) {
	cp := ranked("b", "dev", time.Second, "shared", "api.x", "konnectivity.x", "tunnel.x")
	deleting := ranked("a", "deleting", -time.Hour, "shared", "api.x", "konnectivity.x", "tunnel.x")
	deleting.DeletionTimestamp, deleting.Finalizers = new(metav1.NewTime(at)), []string{"lodia.io/datastore"}
	others := []*lodiav1alpha1.ControlPlane{
		ranked("z", "older", 0, "shared", "api.x", "other.a", "other.b"),
		ranked("z", "oldest", -time.Second, "shared", "other.c", "api.x", "other.d"),
		ranked("a", "same-second", time.Second, "shared", "other.e", "konnectivity.x", "other.f"),
		ranked("b", "cp", time.Second, "shared", "tunnel.x", "other.g", "other.h"),
		ranked("b", "zz", time.Second, "shared", "other.i", "other.j", "tunnel.x"),
		ranked("a", "younger", 2*time.Second, "shared", "konnectivity.x", "other.k", "other.l"),
		ranked("a", "elsewhere", -time.Hour, "other", "api.x", "konnectivity.x", "tunnel.x"),
		deleting,
	}
	builder := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(cp)
	for _, o := range others {
		builder = builder.WithObjects(o)
	}
	r := &Reconciler{Client: builder.Build()}
	owners, err := r.owners(t.Context(), cp)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for hostname, owner := range owners {
		got[hostname] = owner.Namespace + "/" + owner.Name
	}
	want := map[string]string{"api.x": "z/oldest", "konnectivity.x": "a/same-second", "tunnel.x": "b/cp"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("the owners are %v, want %v", got, want)
	}

	first := ranked("b", "cp", time.Second, "shared", "tunnel.x", "other.g", "other.h")
	r = &Reconciler{Client: fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(first, cp).Build()}
	if owners, err := r.owners(t.Context(), first); err != nil || len(owners) != 0 {
		t.Errorf("the first to name tunnel.x has the owners %v, %v", owners, err)
	}
}

func TestARouteHoldsTheNamesItsListenersIntersectAtThePort(t *testing.T) {
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: "gateways", Name: "shared"},
		Spec: gatewayv1.GatewaySpec{Listeners: []gatewayv1.Listener{
			{Name: "wild", Port: 443, Hostname: new(gatewayv1.Hostname("*.zone"))},
			{Name: "api", Port: 443, Hostname: new(gatewayv1.Hostname("api.t.zone"))},
			{Name: "other", Port: 8443, Hostname: new(gatewayv1.Hostname("*.zone"))},
			{Name: "any", Port: 9443},
		}},
	}
	parent := func(change func(*gatewayv1.ParentReference)) gatewayv1.ParentReference {
		p := gatewayv1.ParentReference{Namespace: new(gatewayv1.Namespace("gateways")), Name: "shared"}
		change(&p)
		return p
	}
	section := func(name string) func(*gatewayv1.ParentReference) {
		return func(p *gatewayv1.ParentReference) { p.SectionName = new(gatewayv1.SectionName(name)) }
	}
	every := func(*gatewayv1.ParentReference) {}
	for _, c := range []struct {
		name      string
		namespace string
		parent    gatewayv1.ParentReference
		hostnames []gatewayv1.Hostname
		port      int32
		want      []string
	}{
		{"a name on the wildcard listener", "t", parent(section("wild")), []gatewayv1.Hostname{"api.t.zone"}, 443, []string{"api.t.zone"}},
		{"a name two labels under the wildcard", "t", parent(section("wild")), []gatewayv1.Hostname{"deep.api.t.zone"}, 443, []string{"deep.api.t.zone"}},
		{"a wildcard on the wildcard listener", "t", parent(section("wild")), []gatewayv1.Hostname{"*.zone"}, 443, nil},
		{"a wildcard on the exact listener", "t", parent(section("api")), []gatewayv1.Hostname{"*.zone"}, 443, []string{"api.t.zone"}},
		{"a wildcard on every listener of the port", "t", parent(every), []gatewayv1.Hostname{"*.zone"}, 443, []string{"api.t.zone"}},
		{"a wildcard by port", "t", parent(func(p *gatewayv1.ParentReference) { p.Port = new(gatewayv1.PortNumber(443)) }), []gatewayv1.Hostname{"*.zone"}, 443, []string{"api.t.zone"}},
		{"another port", "t", parent(func(p *gatewayv1.ParentReference) { p.Port = new(gatewayv1.PortNumber(8443)) }), []gatewayv1.Hostname{"api.t.zone"}, 443, nil},
		{"a listener of another port", "t", parent(section("other")), []gatewayv1.Hostname{"api.t.zone"}, 443, nil},
		{"no intersection", "t", parent(section("wild")), []gatewayv1.Hostname{"api.t.example.org"}, 443, nil},
		{"no hostnames on the exact listener", "t", parent(section("api")), nil, 443, []string{"api.t.zone"}},
		{"a listener without a hostname", "t", parent(section("any")), []gatewayv1.Hostname{"api.t.example.org"}, 9443, []string{"api.t.example.org"}},
		{"the route's own namespace", "gateways", gatewayv1.ParentReference{Name: "shared", SectionName: new(gatewayv1.SectionName("wild"))}, []gatewayv1.Hostname{"api.t.zone"}, 443, []string{"api.t.zone"}},
		{"another namespace by default", "t", gatewayv1.ParentReference{Name: "shared", SectionName: new(gatewayv1.SectionName("wild"))}, []gatewayv1.Hostname{"api.t.zone"}, 443, nil},
		{"another Gateway", "t", parent(func(p *gatewayv1.ParentReference) { p.Name = "private" }), []gatewayv1.Hostname{"api.t.zone"}, 443, nil},
		{"another kind", "t", parent(func(p *gatewayv1.ParentReference) { p.Kind = new(gatewayv1.Kind("Service")) }), []gatewayv1.Hostname{"api.t.zone"}, 443, nil},
		{"another group", "t", parent(func(p *gatewayv1.ParentReference) { p.Group = new(gatewayv1.Group("example.com")) }), []gatewayv1.Hostname{"api.t.zone"}, 443, nil},
	} {
		route := &gatewayv1.TLSRoute{
			ObjectMeta: metav1.ObjectMeta{Namespace: c.namespace, Name: "r"},
			Spec:       gatewayv1.TLSRouteSpec{CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{c.parent}}, Hostnames: c.hostnames},
		}
		if got := held(route, gw, c.port); !slices.Equal(got, c.want) {
			t.Errorf("%s: the route holds %v, want %v", c.name, got, c.want)
		}
	}
}

func TestAGatewayEventPassesWhatTheListenerCheckReads(t *testing.T) {
	base := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Generation: 1},
		Status: gatewayv1.GatewayStatus{Listeners: []gatewayv1.ListenerStatus{{
			Name:           "tls",
			SupportedKinds: []gatewayv1.RouteGroupKind{{Kind: "TLSRoute"}},
			AttachedRoutes: 3,
			Conditions:     []metav1.Condition{{Type: "Programmed", Status: metav1.ConditionTrue, Reason: "Programmed"}},
		}}},
	}
	for name, c := range map[string]struct {
		change func(*gatewayv1.Gateway)
		want   bool
	}{
		"a new generation":         {func(gw *gatewayv1.Gateway) { gw.Generation = 2 }, true},
		"a listener's condition":   {func(gw *gatewayv1.Gateway) { gw.Status.Listeners[0].Conditions[0].Status = metav1.ConditionFalse }, true},
		"a listener's kinds":       {func(gw *gatewayv1.Gateway) { gw.Status.Listeners[0].SupportedKinds = nil }, true},
		"another attachedRoutes":   {func(gw *gatewayv1.Gateway) { gw.Status.Listeners[0].AttachedRoutes = 4 }, false},
		"the Gateway's conditions": {func(gw *gatewayv1.Gateway) { gw.Status.Conditions = []metav1.Condition{{Type: "Programmed"}} }, false},
		"a label":                  {func(gw *gatewayv1.Gateway) { gw.Labels = map[string]string{"example.com/team": "a"} }, false},
	} {
		after := base.DeepCopy()
		c.change(after)
		if got := listenersChanged.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: after}); got != c.want {
			t.Errorf("%s: a pass %t, want %t", name, got, c.want)
		}
	}
}

func TestAWriteWaitsForTheGatewaysSemaphoreUntilThePasssDeadline(t *testing.T) {
	cp := ranked("t", "dev", 0, "shared", "api.x", "konnectivity.x", "tunnel.x")
	gw := &gatewayv1.Gateway{ObjectMeta: metav1.ObjectMeta{Namespace: "gateways", Name: "shared"}}
	var lists atomic.Int32
	listed := errors.New("listed")
	reader := interceptor.NewClient(fake.NewClientBuilder().WithScheme(scheme(t)).Build(), interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			lists.Add(1)
			return listed
		},
	})
	r := &Reconciler{Reader: reader}
	held := r.semaphore(client.ObjectKeyFromObject(gw))
	if err := held.Acquire(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := r.write(ctx, cp, gw, 443, routes(cp)[0]); !errors.Is(err, context.DeadlineExceeded) || lists.Load() != 0 {
		t.Errorf("a write while another holds the Gateway's semaphore returned %v after %d reads, want the deadline before any", err, lists.Load())
	}
	held.Release(1)
	if _, err := r.write(t.Context(), cp, gw, 443, routes(cp)[0]); !errors.Is(err, listed) || lists.Load() != 1 {
		t.Errorf("a write once the semaphore is free returned %v after %d reads, want the read's", err, lists.Load())
	}
	if other := r.semaphore(types.NamespacedName{Namespace: "gateways", Name: "private"}); other == held {
		t.Error("two Gateways share a semaphore")
	}
}
