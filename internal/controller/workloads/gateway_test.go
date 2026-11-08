// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/watch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/controller/workloads"
	"go.lodia.io/lodia/internal/operator"
)

const zone = "gw.example.com"

var tlsRouteKind = gatewayv1.RouteGroupKind{Group: new(gatewayv1.Group(gatewayv1.GroupName)), Kind: "TLSRoute"}

func TestAGatewayControlPlaneRunsBehindItsListener(t *testing.T) {
	cp := gatewayRunning(t)
	ns := cp.Namespace
	expectEndpoint(t, cp, lodiav1alpha1.EndpointStatus{
		APIServer:    lodiav1alpha1.HostPort{Host: "api.dev." + zone, Port: 443},
		Konnectivity: lodiav1alpha1.HostPort{Host: "konnectivity.dev." + zone, Port: 443},
		InCluster:    lodiav1alpha1.HostPort{Host: "240.0.0.1", Port: 6443},
		Tunnel:       &lodiav1alpha1.HostPort{Host: "tunnel.dev." + zone, Port: 443},
	})
	if c := routesReady(t, cp); c.Message != "every route is applied and Accepted by Gateway "+ns+"/shared" {
		t.Errorf("RoutesReady says %q", c.Message)
	}

	svc := getService(t, cp)
	var ports []string
	for _, p := range svc.Spec.Ports {
		ports = append(ports, fmt.Sprintf("%s %d %s", p.Name, p.Port, p.TargetPort.String()))
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP || strings.Join(ports, ", ") != "https 443 https, kubernetes 6443 https, konnectivity 8132 konnectivity, tunnel 6444 tunnel" {
		t.Errorf("the Service is %s with the ports %v", svc.Spec.Type, ports)
	}

	for suffix, port := range map[string]int32{"kube-apiserver": 443, "konnectivity": 8132, "tunnel": 6444} {
		route := tlsRoute(t, ns, "dev-"+suffix)
		hostname := map[string]string{"kube-apiserver": "api", "konnectivity": "konnectivity", "tunnel": "tunnel"}[suffix] + ".dev." + zone
		want := gatewayv1.TLSRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{
				Group: new(gatewayv1.Group("gateway.networking.k8s.io")), Kind: new(gatewayv1.Kind("Gateway")),
				Namespace: new(gatewayv1.Namespace(ns)), Name: "shared", SectionName: new(gatewayv1.SectionName("tls")),
			}}},
			Hostnames: []gatewayv1.Hostname{gatewayv1.Hostname(hostname)},
			Rules: []gatewayv1.TLSRouteRule{{BackendRefs: []gatewayv1.BackendRef{{
				BackendObjectReference: gatewayv1.BackendObjectReference{Group: new(gatewayv1.Group("")), Kind: new(gatewayv1.Kind("Service")), Name: "dev-kube-apiserver", Port: new(port)},
				Weight:                 new(int32(1)),
			}}}},
		}
		if !equality.Semantic.DeepEqual(route.Spec, want) {
			t.Errorf("TLSRoute %s is %+v, want %+v", route.Name, route.Spec, want)
		}
		if route.Labels[lodiav1alpha1.ControlPlaneLabel] != "dev" || !metav1.IsControlledBy(route, cp) || !*route.OwnerReferences[0].BlockOwnerDeletion {
			t.Errorf("TLSRoute %s has the labels %v and the owners %+v", route.Name, route.Labels, route.OwnerReferences)
		}
	}

	policy := &networkingv1.NetworkPolicy{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "dev-kube-apiserver"}, policy); err != nil {
		t.Fatal(err)
	}
	var admitted []string
	for _, p := range policy.Spec.Ingress[0].Ports {
		admitted = append(admitted, p.Port.String())
	}
	if !slices.Equal(admitted, []string{"https", "8132", "6444"}) {
		t.Errorf("the API server's NetworkPolicy admits %v", admitted)
	}

	pod := deployment(t, cp, "kube-apiserver").Spec.Template.Spec
	expectFlags(t, pod.Containers[0].Command, "--advertise-address=240.0.0.1", "--service-account-issuer=https://api.dev."+zone,
		"--service-account-jwks-uri=https://api.dev."+zone+"/openid/v1/jwks")
	var sidecars []string
	for _, c := range pod.InitContainers {
		sidecars = append(sidecars, c.Name)
	}
	if !slices.Equal(sidecars, []string{"lodia-signer", "konnectivity-server", "lodia-tunnel"}) {
		t.Fatalf("the API server's sidecars are %v", sidecars)
	}
	tunnel := pod.InitContainers[2]
	health := corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(8135), Scheme: corev1.URISchemeHTTP}}
	want := corev1.Container{
		Name:  "lodia-tunnel",
		Image: tunnelImage,
		Args: []string{"serve", "--listen=:6444", "--hostname=tunnel.dev." + zone, "--cert-dir=/etc/lodia/tunnel-serving", "--client-ca=/etc/lodia/tunnel-ca",
			"--upstream=127.0.0.1:6443", "--health-probe-bind-address=:8135"},
		Ports:         []corev1.ContainerPort{{Name: "tunnel", ContainerPort: 6444, Protocol: corev1.ProtocolTCP}},
		RestartPolicy: new(corev1.ContainerRestartPolicyAlways),
		StartupProbe:  &corev1.Probe{ProbeHandler: health, TimeoutSeconds: 1, PeriodSeconds: 1, SuccessThreshold: 1, FailureThreshold: 30},
		LivenessProbe: &corev1.Probe{ProbeHandler: health, TimeoutSeconds: 1, PeriodSeconds: 10, SuccessThreshold: 1, FailureThreshold: 3},
		Resources:     tunnel.Resources,
		SecurityContext: &corev1.SecurityContext{
			RunAsUser: new(int64(65532)), RunAsGroup: new(int64(65532)), RunAsNonRoot: new(true),
			AllowPrivilegeEscalation: new(false), ReadOnlyRootFilesystem: new(true),
			Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "tunnel-serving", MountPath: "/etc/lodia/tunnel-serving", ReadOnly: true},
			{Name: "tunnel-ca", MountPath: "/etc/lodia/tunnel-ca", ReadOnly: true},
		},
		TerminationMessagePath:   corev1.TerminationMessagePathDefault,
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		ImagePullPolicy:          corev1.PullIfNotPresent,
	}
	if !equality.Semantic.DeepEqual(tunnel, want) {
		t.Errorf("the tunnel sidecar is\n%+v\nwant\n%+v", tunnel, want)
	}
	secrets := map[string]string{}
	for _, v := range pod.Volumes {
		if v.Secret != nil {
			secrets[v.Name] = v.Secret.SecretName
		}
	}
	if secrets["tunnel-serving"] != "dev-tunnel-serving" || secrets["tunnel-ca"] != "dev-tunnel-ca" {
		t.Errorf("the API server's pod mounts the Secrets %v", secrets)
	}

	before := hashes(t, cp)
	for _, s := range []lodiav1alpha1.Secret{lodiav1alpha1.TunnelServingSecret, lodiav1alpha1.TunnelCASecret} {
		setSecret(t, cp, s, "renewed")
	}
	reconcileNow(t, cp)
	if after := hashes(t, cp); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Errorf("a renewal of the tunnel's Secrets, which the sidecar reloads, changed the restart hashes %v to %v", before, after)
	}

	gw := &gatewayv1.Gateway{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "shared"}, gw); err != nil {
		t.Fatal(err)
	}
	setListenerStatus(t, gw, listenerStatus([]gatewayv1.RouteGroupKind{{Kind: "TLSRoute"}}, metav1.ConditionTrue, metav1.ConditionTrue, "Accepted"))
	reconcileNow(t, cp)
	if err := expectRoutes(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted); err != nil {
		t.Errorf("with a listener that takes TLSRoutes, Accepted and Programmed: %v", err)
	}
}

func TestTheAdvertiseAddressAndTheListenersPortArePublished(t *testing.T) {
	ns := namespace(t)
	createGateway(t, ns, passthrough("tls", "*."+zone, 8443))
	cp := createNamed(t, ns, "dev", onGateway(ns, "dev"), func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.APIServer = &lodiav1alpha1.APIServer{AdvertiseAddress: "198.51.100.7"}
	})
	inputsReady(t, cp)
	eventually(t, func() error {
		return expectEndpointIs(cp, lodiav1alpha1.EndpointStatus{
			APIServer:    lodiav1alpha1.HostPort{Host: "api.dev." + zone, Port: 8443},
			Konnectivity: lodiav1alpha1.HostPort{Host: "konnectivity.dev." + zone, Port: 8443},
			InCluster:    lodiav1alpha1.HostPort{Host: "198.51.100.7", Port: 6443},
			Tunnel:       &lodiav1alpha1.HostPort{Host: "tunnel.dev." + zone, Port: 8443},
		})
	})
	expectFlags(t, eventuallyDeployment(t, cp, "kube-apiserver").Spec.Template.Spec.Containers[0].Command, "--advertise-address=198.51.100.7",
		"--service-account-issuer=https://api.dev."+zone+":8443", "--service-account-jwks-uri=https://api.dev."+zone+":8443/openid/v1/jwks")
}

func TestAFieldNotBuiltYetHidesNoneOfTheGatewayPath(t *testing.T) {
	ns := namespace(t)
	createGateway(t, ns)
	cp := createNamed(t, ns, "dev", onGateway(ns, "dev"), func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.ServiceAccount = &lodiav1alpha1.ServiceAccount{PublicDiscovery: true}
	})
	accept(t, cp)
	eventually(t, func() error {
		if err := expectRoutes(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted); err != nil {
			return err
		}
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported)
	})
	if c := condition(t, cp); c.Message != "not built yet: serviceAccount.publicDiscovery" {
		t.Errorf("APIServerReady says %q", c.Message)
	}
	expectEndpoint(t, cp, lodiav1alpha1.EndpointStatus{
		APIServer:    lodiav1alpha1.HostPort{Host: "api.dev." + zone, Port: 443},
		Konnectivity: lodiav1alpha1.HostPort{Host: "konnectivity.dev." + zone, Port: 443},
		InCluster:    lodiav1alpha1.HostPort{Host: "240.0.0.1", Port: 6443},
		Tunnel:       &lodiav1alpha1.HostPort{Host: "tunnel.dev." + zone, Port: 443},
	})
	if svc := getService(t, cp); svc.Spec.Type != corev1.ServiceTypeClusterIP || !metav1.IsControlledBy(svc, cp) {
		t.Errorf("the Service is %s, controlled by %v", svc.Spec.Type, metav1.GetControllerOf(svc))
	}
	if n := deployments(t, cp); n != 0 {
		t.Errorf("%d Deployments for a ControlPlane with a field not built yet", n)
	}
}

func TestEveryListenerCheckIsGatewayUnsupported(t *testing.T) {
	terminate := passthrough("tls", "*."+zone, 443)
	terminate.TLS = &gatewayv1.ListenerTLSConfig{Mode: new(gatewayv1.TLSModeTerminate), CertificateRefs: []gatewayv1.SecretObjectReference{{Name: "serving"}}}
	http := gatewayv1.Listener{Name: "tls", Port: 443, Protocol: gatewayv1.HTTPProtocolType, Hostname: new(gatewayv1.Hostname("*." + zone))}
	tls := []gatewayv1.RouteGroupKind{tlsRouteKind}
	for name, c := range map[string]struct {
		listener gatewayv1.Listener
		status   *gatewayv1.ListenerStatus
		message  string
	}{
		"no listener of that name": {passthrough("other", "*."+zone, 443), nil, "does not exist"},
		"another protocol":         {http, nil, "has protocol HTTP, not TLS"},
		"Terminate mode":           {terminate, nil, "is not in Passthrough mode"},
		"a wildcard that does not cover the hostnames": {passthrough("tls", "*.elsewhere.example.com", 443), nil,
			"has the hostname *.elsewhere.example.com, which does not cover api.dev." + zone},
		"one name, not all three": {passthrough("tls", "api.dev."+zone, 443), nil, "has the hostname api.dev." + zone + ", which does not cover konnectivity.dev." + zone},
		"supportedKinds without TLSRoute": {passthrough("tls", "*."+zone, 443),
			new(listenerStatus([]gatewayv1.RouteGroupKind{{Kind: "HTTPRoute"}}, metav1.ConditionTrue, metav1.ConditionTrue, "Accepted")), "does not take TLSRoutes, by its status"},
		"TLSRoute of another group": {passthrough("tls", "*."+zone, 443),
			new(listenerStatus([]gatewayv1.RouteGroupKind{{Group: new(gatewayv1.Group("example.com")), Kind: "TLSRoute"}}, metav1.ConditionTrue, metav1.ConditionTrue, "Accepted")), "does not take TLSRoutes, by its status"},
		"no supportedKinds": {passthrough("tls", "*."+zone, 443),
			new(listenerStatus(nil, metav1.ConditionTrue, metav1.ConditionTrue, "Accepted")), "does not take TLSRoutes, by its status"},
		"not Accepted": {passthrough("tls", "*."+zone, 443),
			new(listenerStatus(tls, metav1.ConditionFalse, metav1.ConditionTrue, "PortUnavailable")), "is not Accepted: PortUnavailable"},
		"not Programmed": {passthrough("tls", "*."+zone, 443),
			new(listenerStatus(tls, metav1.ConditionTrue, metav1.ConditionFalse, "Invalid")), "is not Programmed: Invalid"},
	} {
		t.Run(name, func(t *testing.T) {
			ns := namespace(t)
			gw := createGateway(t, ns, c.listener)
			if c.status != nil {
				setListenerStatus(t, gw, *c.status)
			}
			cp := createNamed(t, ns, "dev", onGateway(ns, "dev"))
			eventually(t, func() error {
				if err := expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonGatewayUnsupported); err != nil {
					return err
				}
				return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint)
			})
			if got, want := routesReady(t, cp).Message, "listener tls of Gateway "+ns+"/shared "+c.message; got != want {
				t.Errorf("RoutesReady says %q, want %q", got, want)
			}
			if got, want := condition(t, cp).Message, "no endpoint until listener tls of Gateway "+ns+"/shared can take the routes, which RoutesReady says"; got != want {
				t.Errorf("APIServerReady says %q, want %q", got, want)
			}
			if e := get(t, cp).Status.Endpoint; e != nil {
				t.Errorf("status.endpoint is %+v", e)
			}
			expectNoRoutes(t, cp)
			if result := reconcileNow(t, cp); result.RequeueAfter != time.Minute {
				t.Errorf("requeue after %s, want a minute", result.RequeueAfter)
			}
		})
	}
}

func TestAListenersStatusAloneBringsAPass(t *testing.T) {
	cp := gatewayRunning(t)
	gw := gatewayOf(t, cp)
	published := get(t, cp).Status.Endpoint
	quiet(t, cp)
	setListenerStatus(t, gw, listenerStatus([]gatewayv1.RouteGroupKind{tlsRouteKind}, metav1.ConditionTrue, metav1.ConditionFalse, "Pending"))
	eventually(t, func() error { return expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonGatewayUnsupported) })
	if c := routesReady(t, cp); !strings.HasSuffix(c.Message, "is not Programmed: Pending") {
		t.Errorf("RoutesReady says %q", c.Message)
	}
	expectEndpoint(t, cp, *published)
	setListenerStatus(t, gw, listenerStatus([]gatewayv1.RouteGroupKind{tlsRouteKind}, metav1.ConditionTrue, metav1.ConditionTrue, "Programmed"))
	eventually(t, func() error { return expectRoutes(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted) })
}

func TestAGatewayCreatedLaterBringsThePass(t *testing.T) {
	ns := namespace(t)
	cp := createNamed(t, ns, "dev", onGateway(ns, "dev"))
	eventually(t, func() error {
		if err := expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonGatewayNotFound); err != nil {
			return err
		}
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint)
	})
	if c := routesReady(t, cp); c.Message != "Gateway "+ns+"/shared does not exist" {
		t.Errorf("RoutesReady says %q", c.Message)
	}
	if svc := getService(t, cp); svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("the Service is %s", svc.Spec.Type)
	}
	quiet(t, cp)
	createGateway(t, ns)
	eventually(t, func() error {
		if err := expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRouteNotAccepted); err != nil {
			return err
		}
		if e := get(t, cp).Status.Endpoint; e == nil || e.Tunnel == nil {
			return fmt.Errorf("status.endpoint is %+v", e)
		}
		return nil
	})
	want := "TLSRoute dev-kube-apiserver has no status from Gateway " + ns + "/shared yet; TLSRoute dev-konnectivity has no status from Gateway " + ns + "/shared yet; TLSRoute dev-tunnel has no status from Gateway " + ns + "/shared yet"
	if c := routesReady(t, cp); c.Message != want {
		t.Errorf("RoutesReady says %q, want %q", c.Message, want)
	}
}

func TestARouteNotAcceptedSaysTheGatewaysReason(t *testing.T) {
	cp := gatewayRunning(t)
	route := tlsRoute(t, cp.Namespace, "dev-konnectivity")
	setRouteStatus(t, route, metav1.ConditionTrue, metav1.ConditionFalse, "BackendNotFound", route.Generation)
	eventually(t, func() error { return expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRouteNotAccepted) })
	if c := routesReady(t, cp); c.Message != "TLSRoute dev-konnectivity is not ResolvedRefs by Gateway "+cp.Namespace+"/shared: BackendNotFound" {
		t.Errorf("RoutesReady says %q", c.Message)
	}
	setRouteStatus(t, route, metav1.ConditionFalse, metav1.ConditionTrue, "NotAllowedByListeners", route.Generation)
	eventually(t, func() error {
		if c := routesReady(t, cp); c.Message != "TLSRoute dev-konnectivity is not Accepted by Gateway "+cp.Namespace+"/shared: NotAllowedByListeners" {
			return fmt.Errorf("RoutesReady says %q", c.Message)
		}
		return nil
	})
	eventually(t, func() error {
		fresh := tlsRoute(t, route.Namespace, route.Name)
		if err := writeRouteStatus(fresh, metav1.ConditionTrue, metav1.ConditionTrue, "Accepted", fresh.Generation); err != nil {
			return err
		}
		fresh = tlsRoute(t, route.Namespace, route.Name)
		fresh.Status.Parents[0].ParentRef.SectionName = new(gatewayv1.SectionName("other"))
		return k8s.Status().Update(t.Context(), fresh)
	})
	eventually(t, func() error {
		if c := routesReady(t, cp); c.Message != "TLSRoute dev-konnectivity has no status from Gateway "+cp.Namespace+"/shared yet" {
			return fmt.Errorf("with the status of another listener alone, RoutesReady says %q", c.Message)
		}
		return nil
	})
	setRouteStatus(t, route, metav1.ConditionTrue, metav1.ConditionTrue, "Accepted", route.Generation-1)
	eventually(t, func() error {
		want := fmt.Sprintf("TLSRoute dev-konnectivity's Accepted from Gateway %s/shared describes generation %d, not %d", cp.Namespace, route.Generation-1, route.Generation)
		if c := routesReady(t, cp); c.Message != want {
			return fmt.Errorf("RoutesReady says %q, want %q", c.Message, want)
		}
		return nil
	})
	if err := expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady); err != nil {
		t.Error(err)
	}
}

func TestRoutesWithoutReadyEndpointsAreResolvedWhileTheAPIServerIsUnavailable(t *testing.T) {
	ns := namespace(t)
	createGateway(t, ns)
	cp := createNamed(t, ns, "dev", onGateway(ns, "dev"))
	inputsReady(t, cp)
	for _, name := range routeNames(cp) {
		eventually(t, func() error {
			route := tlsRouteOrNil(t, ns, name)
			if route == nil {
				return fmt.Errorf("no TLSRoute %s yet", name)
			}
			return writeRouteStatus(route, metav1.ConditionTrue, metav1.ConditionFalse, "EndpointsNotFound", route.Generation)
		})
	}
	eventually(t, func() error {
		if err := expectRoutes(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted); err != nil {
			return err
		}
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable)
	})

	route := tlsRoute(t, ns, "dev-konnectivity")
	expectMessage := func(want string) {
		t.Helper()
		eventually(t, func() error {
			if err := expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRouteNotAccepted); err != nil {
				return err
			}
			if c := routesReady(t, cp); c.Message != want {
				return fmt.Errorf("RoutesReady says %q, want %q", c.Message, want)
			}
			return nil
		})
	}
	eventually(t, func() error {
		fresh := tlsRoute(t, ns, route.Name)
		if err := writeRouteStatus(fresh, metav1.ConditionTrue, metav1.ConditionFalse, "EndpointsNotFound", fresh.Generation); err != nil {
			return err
		}
		fresh = tlsRoute(t, ns, route.Name)
		fresh.Status.Parents[0].Conditions[1].ObservedGeneration = fresh.Generation - 1
		return k8s.Status().Update(t.Context(), fresh)
	})
	expectMessage(fmt.Sprintf("TLSRoute dev-konnectivity's ResolvedRefs from Gateway %s/shared describes generation %d, not %d", ns, route.Generation-1, route.Generation))
	setRouteStatus(t, route, metav1.ConditionTrue, metav1.ConditionUnknown, "EndpointsNotFound", route.Generation)
	expectMessage("TLSRoute dev-konnectivity is not ResolvedRefs by Gateway " + ns + "/shared: EndpointsNotFound")
	setRouteStatus(t, route, metav1.ConditionFalse, metav1.ConditionTrue, "EndpointsNotFound", route.Generation)
	expectMessage("TLSRoute dev-konnectivity is not Accepted by Gateway " + ns + "/shared: EndpointsNotFound")
}

func TestWithoutTheGatewayAPIServedAGatewayControlPlaneIsUnsupported(t *testing.T) {
	pass := func(t *testing.T, cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.ControlPlane {
		t.Helper()
		var applied *lodiav1alpha1.ControlPlane
		notServed := errors.New("the Gateway API is not served")
		blind := interceptor.NewClient(k8s, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				switch obj.(type) {
				case *gatewayv1.Gateway, *gatewayv1.TLSRoute:
					return notServed
				}
				return c.Get(ctx, key, obj, opts...)
			},
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				switch list.(type) {
				case *gatewayv1.GatewayList, *gatewayv1.TLSRouteList:
					return notServed
				}
				return c.List(ctx, list, opts...)
			},
			SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
				applied = decoded(t, obj)
				return nil
			},
		})
		r := &workloads.Reconciler{Client: blind, Reader: blind, SignerImage: signerImage, TunnelImage: tunnelImage}
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
			t.Fatal(err)
		}
		if applied == nil {
			t.Fatal("the pass applied no status")
		}
		c := meta.FindStatusCondition(applied.Status.Conditions, lodiav1alpha1.ConditionRoutesReady)
		want := "the management cluster did not serve TLSRoute and Gateway at gateway.networking.k8s.io/v1 when the operator started, the only time it looks: restart the operator once they are served"
		if c == nil || c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonGatewayUnsupported || c.Message != want {
			t.Errorf("RoutesReady is %+v", c)
		}
		return applied
	}
	t.Run("a new ControlPlane", func(t *testing.T) {
		ns := namespace(t)
		cp := createNamed(t, ns, "dev", onGateway(ns, "dev"))
		eventually(t, func() error { return expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonGatewayNotFound) })
		applied := pass(t, cp)
		if c := meta.FindStatusCondition(applied.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady); c == nil || c.Reason != lodiav1alpha1.ReasonWaitingForEndpoint {
			t.Errorf("APIServerReady is %+v", c)
		}
		if applied.Status.Endpoint != nil {
			t.Errorf("status.endpoint is %+v", applied.Status.Endpoint)
		}
	})
	t.Run("a published endpoint after a restart", func(t *testing.T) {
		cp := gatewayRunning(t)
		published := get(t, cp).Status.Endpoint
		applied := pass(t, cp)
		if !equality.Semantic.DeepEqual(applied.Status.Endpoint, published) {
			t.Errorf("status.endpoint is %+v, want the published %+v", applied.Status.Endpoint, published)
		}
		if c := meta.FindStatusCondition(applied.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady); c == nil || c.Reason != lodiav1alpha1.ReasonReady {
			t.Errorf("APIServerReady is %+v", c)
		}
	})
}

func TestAStaleCacheNeverDropsThePublishedEndpoint(t *testing.T) {
	cp := gatewayRunning(t)
	published := get(t, cp).Status.Endpoint
	routes := routesReady(t, cp)
	for name, held := range map[string]bool{"a pass past the Service": false, "a pass that stops at a held Service": true} {
		t.Run(name, func(t *testing.T) {
			var applied []*lodiav1alpha1.ControlPlane
			stale := interceptor.NewClient(k8s, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := c.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					switch cached := obj.(type) {
					case *lodiav1alpha1.ControlPlane:
						cached.Status = lodiav1alpha1.ControlPlaneStatus{}
					case *corev1.Service:
						if held {
							cached.OwnerReferences = []metav1.OwnerReference{foreignController()}
						}
					}
					return nil
				},
				SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
					applied = append(applied, decoded(t, obj))
					return nil
				},
			})
			r := &workloads.Reconciler{Client: stale, Reader: k8s, SignerImage: signerImage, TunnelImage: tunnelImage}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
				t.Fatal(err)
			}
			if len(applied) != 2 {
				t.Fatalf("the pass applied %d statuses, want the first pass's and its own", len(applied))
			}
			for i, a := range applied {
				if !equality.Semantic.DeepEqual(a.Status.Endpoint, published) {
					t.Errorf("status apply %d carries the endpoint %+v, want the published %+v", i, a.Status.Endpoint, published)
				}
			}
			if c := meta.FindStatusCondition(applied[1].Status.Conditions, lodiav1alpha1.ConditionRoutesReady); held && !equality.Semantic.DeepEqual(c, routes) {
				t.Errorf("the pass applied RoutesReady %+v, want the one the API server holds, %+v", c, routes)
			}
		})
	}
}

func TestAPublishedEndpointStays(t *testing.T) {
	cp := gatewayRunning(t)
	ns := cp.Namespace
	published := *get(t, cp).Status.Endpoint
	written := resourceVersions(t, ns, &gatewayv1.TLSRouteList{})
	kept := func(t *testing.T, routes, apiServerReady string, message string) {
		t.Helper()
		eventually(t, func() error {
			if err := expectRoutesAt(cp, routes); err != nil {
				return err
			}
			if c := condition(t, cp); c.Reason != apiServerReady {
				return fmt.Errorf("APIServerReady is %+v, want %s", c, apiServerReady)
			}
			return nil
		})
		if message != "" {
			if c := routesReady(t, cp); c.Message != message {
				t.Errorf("RoutesReady says %q, want %q", c.Message, message)
			}
		}
		expectEndpoint(t, cp, published)
		if after := resourceVersions(t, ns, &gatewayv1.TLSRouteList{}); fmt.Sprint(after) != fmt.Sprint(written) {
			t.Errorf("the routes were written: %v, then %v", written, after)
		}
	}
	listener := "listener tls of Gateway " + ns + "/shared "
	t.Run("the listener moves port", func(t *testing.T) {
		changeGateway(t, cp, func(gw *gatewayv1.Gateway) { gw.Spec.Listeners[0].Port = 8443 })
		kept(t, lodiav1alpha1.ReasonGatewayUnsupported, lodiav1alpha1.ReasonReady, listener+"is at port 8443, and the endpoint, which never moves, was published at port 443")
		changeGateway(t, cp, func(gw *gatewayv1.Gateway) { gw.Spec.Listeners[0].Port = 443 })
		kept(t, lodiav1alpha1.ReasonAccepted, lodiav1alpha1.ReasonReady, "")
	})
	t.Run("the listener goes", func(t *testing.T) {
		changeGateway(t, cp, func(gw *gatewayv1.Gateway) { gw.Spec.Listeners[0].Name = "renamed" })
		kept(t, lodiav1alpha1.ReasonGatewayUnsupported, lodiav1alpha1.ReasonReady, listener+"does not exist")
		changeGateway(t, cp, func(gw *gatewayv1.Gateway) { gw.Spec.Listeners[0].Name = "tls" })
		kept(t, lodiav1alpha1.ReasonAccepted, lodiav1alpha1.ReasonReady, "")
	})
	t.Run("the Gateway goes", func(t *testing.T) {
		if err := k8s.Delete(t.Context(), gatewayOf(t, cp)); err != nil {
			t.Fatal(err)
		}
		kept(t, lodiav1alpha1.ReasonGatewayNotFound, lodiav1alpha1.ReasonReady, "Gateway "+ns+"/shared does not exist")
		createGateway(t, ns)
		kept(t, lodiav1alpha1.ReasonAccepted, lodiav1alpha1.ReasonReady, "")
	})
	t.Run("the Service is held", func(t *testing.T) {
		before := routesReady(t, cp)
		changeService(t, cp, func(svc *corev1.Service) { svc.OwnerReferences = []metav1.OwnerReference{foreignController()} })
		kept(t, lodiav1alpha1.ReasonAccepted, lodiav1alpha1.ReasonObjectsControlledElsewhere, "")
		if after := routesReady(t, cp); !equality.Semantic.DeepEqual(after, before) {
			t.Errorf("RoutesReady moved from %+v to %+v", before, after)
		}
		changeService(t, cp, func(svc *corev1.Service) { svc.OwnerReferences = nil })
		eventually(t, func() error {
			reconcileNow(t, cp)
			return expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
		})
		kept(t, lodiav1alpha1.ReasonAccepted, lodiav1alpha1.ReasonReady, "")
	})
	t.Run("the Service is refused", func(t *testing.T) {
		binding := refuse(t, ns, "", "services", "dev-kube-apiserver", getService(t, cp))
		r := &workloads.Reconciler{Client: k8s, Reader: k8s, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true}
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !apistatus.Refused(err) {
			t.Errorf("a pass whose Service is refused returned %v, want the refusal", err)
		}
		kept(t, lodiav1alpha1.ReasonAccepted, lodiav1alpha1.ReasonRefused, "")
		if err := k8s.Delete(t.Context(), binding); err != nil {
			t.Fatal(err)
		}
		eventually(t, func() error {
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
			return err
		})
		kept(t, lodiav1alpha1.ReasonAccepted, lodiav1alpha1.ReasonReady, "")
	})
}

func TestRoutesReadyOnTheServicesExitsWithoutOneOfItsOwn(t *testing.T) {
	t.Run("a held Service", func(t *testing.T) {
		ns := namespace(t)
		held := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "dev-kube-apiserver", Namespace: ns, Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev"},
				OwnerReferences: []metav1.OwnerReference{foreignController()}},
			Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "https", Port: 443}}},
		}
		if err := k8s.Create(t.Context(), held); err != nil {
			t.Fatal(err)
		}
		cp := createNamed(t, ns, "dev", onGateway(ns, "dev"))
		sameAsTheService := func() error {
			if err := expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere); err != nil {
				return err
			}
			if r, a := routesReady(t, cp), condition(t, cp); r.Message != a.Message || a.Reason != r.Reason {
				return fmt.Errorf("RoutesReady is %+v, APIServerReady %+v", r, a)
			}
			return nil
		}
		eventually(t, sameAsTheService)
		reconciling := map[string]any{
			"type": lodiav1alpha1.ConditionRoutesReady, "status": string(metav1.ConditionUnknown), "reason": lodiav1alpha1.ReasonReconciling,
			"message": "the first pass has not finished", "observedGeneration": int64(1), "lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		}
		apiServerReady, err := runtime.DefaultUnstructuredConverter.ToUnstructured(condition(t, cp))
		if err != nil {
			t.Fatal(err)
		}
		applyStatus(t, cp, workloads.FieldManager, map[string]any{"conditions": []any{apiServerReady, reconciling}})
		reconcileNow(t, cp)
		if err := sameAsTheService(); err != nil {
			t.Errorf("after an interrupted first pass: %v", err)
		}
	})
	t.Run("a refused Service", func(t *testing.T) {
		ns := namespace(t)
		refuse(t, ns, "", "services", "dev-kube-apiserver", &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "dev-kube-apiserver", Namespace: ns},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Port: 443}}},
		})
		cp := createNamed(t, ns, "dev", onGateway(ns, "dev"))
		eventually(t, func() error {
			if err := expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused); err != nil {
				return err
			}
			if r, a := routesReady(t, cp), condition(t, cp); r.Message != a.Message || a.Reason != r.Reason {
				return fmt.Errorf("RoutesReady is %+v, APIServerReady %+v", r, a)
			}
			return nil
		})
	})
}

func TestAYoungerControlPlaneDeletesOnlyTheRoutesItControls(t *testing.T) {
	ns := namespace(t)
	createGateway(t, ns)
	a := createNamed(t, ns, "a", onGateway(ns, "same"))
	eventually(t, func() error { return expectRoutes(a, metav1.ConditionFalse, lodiav1alpha1.ReasonRouteNotAccepted) })
	b := createNamed(t, ns, "b", onGateway(ns, "same"))
	controlled := handmade(t, ns, "b-kube-apiserver", "api.same."+zone, ns, "tls", controllerOf(b))
	orphan := handmade(t, ns, "b-konnectivity", "konnectivity.same."+zone, ns, "tls")
	foreign := handmade(t, ns, "b-tunnel", "tunnel.same."+zone, ns, "tls", foreignController())
	eventually(t, func() error {
		if err := expectRoutes(b, metav1.ConditionFalse, lodiav1alpha1.ReasonHostnameConflict); err != nil {
			return err
		}
		if tlsRouteOrNil(t, ns, controlled.Name) != nil {
			return errors.New("the route b controls is still there")
		}
		return nil
	})
	want := fmt.Sprintf("hostnames named first on Gateway %s/shared by another ControlPlane: api.same.%s by ControlPlane %s/a (UID %s), konnectivity.same.%s by ControlPlane %s/a (UID %s), tunnel.same.%s by ControlPlane %s/a (UID %s); no route of this ControlPlane is applied until they are free",
		ns, zone, ns, a.UID, zone, ns, a.UID, zone, ns, a.UID)
	if c := routesReady(t, b); c.Message != want {
		t.Errorf("RoutesReady says %q, want %q", c.Message, want)
	}
	for _, kept := range []*gatewayv1.TLSRoute{orphan, foreign} {
		if tlsRouteOrNil(t, ns, kept.Name) == nil {
			t.Errorf("TLSRoute %s, which b does not control, was deleted", kept.Name)
		}
	}
	for _, name := range []string{"a-kube-apiserver", "a-konnectivity", "a-tunnel"} {
		if tlsRouteOrNil(t, ns, name) == nil {
			t.Errorf("the owner's TLSRoute %s is missing", name)
		}
	}
	quiet(t, b)
	if err := k8s.Delete(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		if c := routesReady(t, b); !strings.HasPrefix(c.Message, "hostname api.same."+zone+" is held by TLSRoute "+ns+"/a-kube-apiserver") {
			return fmt.Errorf("once the owner is gone, RoutesReady says %q", c.Message)
		}
		return nil
	})
}

func TestAHolderOnAnotherListenerOfThePort(t *testing.T) {
	ns := namespace(t)
	createGateway(t, ns, passthrough("tls", "*."+zone, 443), passthrough("exact", "api.h."+zone, 443))
	other := namespace(t)
	holder := handmade(t, other, "holder", "*."+zone, ns, "exact")
	handmade(t, other, "wildcard", "*."+zone, ns, "tls")
	cp := createNamed(t, ns, "h", onGateway(ns, "h"))
	eventually(t, func() error { return expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonHostnameConflict) })
	want := "hostname api.h." + zone + " is held by TLSRoute " + other + "/holder on Gateway " + ns + "/shared; Workloads writes no route for it until that one is gone; " +
		"TLSRoute h-konnectivity has no status from Gateway " + ns + "/shared yet; TLSRoute h-tunnel has no status from Gateway " + ns + "/shared yet"
	if c := routesReady(t, cp); c.Message != want {
		t.Errorf("RoutesReady says %q, want %q", c.Message, want)
	}
	if tlsRouteOrNil(t, ns, "h-kube-apiserver") != nil {
		t.Error("a route was written for a hostname another route holds")
	}
	quiet(t, cp)
	if err := k8s.Delete(t.Context(), holder); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		if tlsRouteOrNil(t, ns, "h-kube-apiserver") == nil {
			return errors.New("no h-kube-apiserver yet")
		}
		return nil
	})
}

func TestARouteEditedAwayFromAHostnameReleasesIt(t *testing.T) {
	ns := namespace(t)
	createGateway(t, ns)
	other := namespace(t)
	holder := handmade(t, other, "holder", "api.e."+zone, ns, "tls")
	cp := createNamed(t, ns, "e", onGateway(ns, "e"))
	eventually(t, func() error { return expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonHostnameConflict) })
	quiet(t, cp)
	eventually(t, func() error {
		fresh := tlsRoute(t, other, holder.Name)
		fresh.Spec.Hostnames = []gatewayv1.Hostname{"elsewhere." + zone}
		return k8s.Update(t.Context(), fresh)
	})
	eventually(t, func() error {
		if tlsRouteOrNil(t, ns, "e-kube-apiserver") == nil {
			return errors.New("no e-kube-apiserver yet")
		}
		return nil
	})
}

func TestTwoConcurrentCreationsOfOneHostnameYieldOneRoute(t *testing.T) {
	ns := namespace(t)
	a := createNamed(t, ns, "a", onGateway(ns, "race"))
	b := createNamed(t, ns, "b", onGateway(ns, "race"))
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: ns},
		Spec:       gatewayv1.GatewaySpec{GatewayClassName: "example", Listeners: []gatewayv1.Listener{passthrough("tls", "*."+zone, 443)}},
	}
	var gets, lists, applies atomic.Int32
	stale := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			switch o := obj.(type) {
			case *gatewayv1.Gateway:
				gw.DeepCopyInto(o)
				return nil
			case *gatewayv1.TLSRoute:
				gets.Add(1)
				return apierrors.NewNotFound(schema.GroupResource{Group: gatewayv1.GroupName, Resource: "tlsroutes"}, key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *lodiav1alpha1.ControlPlaneList:
				return nil
			case *gatewayv1.TLSRouteList:
				if lists.Add(1) == 1 {
					if err := c.List(ctx, list, opts...); err != nil {
						return err
					}
					waitFor(time.Minute, func() bool { return gets.Load() >= 2 })
					if waitFor(2*time.Second, func() bool { return lists.Load() >= 2 }) {
						waitFor(time.Minute, func() bool { return applies.Load() >= 1 })
					}
					return nil
				}
			}
			return c.List(ctx, list, opts...)
		},
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			err := c.Apply(ctx, obj, opts...)
			if kindOf(t, obj) == "TLSRoute" {
				applies.Add(1)
			}
			return err
		},
	})
	r := &workloads.Reconciler{Client: stale, Reader: stale, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true}
	errs := make([]error, 2)
	var passes sync.WaitGroup
	for i, cp := range []*lodiav1alpha1.ControlPlane{a, b} {
		passes.Go(func() {
			_, errs[i] = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		})
	}
	passes.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	list := &gatewayv1.TLSRouteList{}
	if err := k8s.List(t.Context(), list, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	routes := map[string][]string{}
	for _, route := range list.Items {
		for _, h := range route.Spec.Hostnames {
			routes[string(h)] = append(routes[string(h)], route.Name)
		}
	}
	for _, prefix := range []string{"api", "konnectivity", "tunnel"} {
		if got := routes[prefix+".race."+zone]; len(got) != 1 {
			t.Errorf("%s.race.%s has the routes %v, want one", prefix, zone, got)
		}
	}
}

func TestAnUnchangedRouteIsNotWritten(t *testing.T) {
	cp := gatewayRunning(t)
	expectNoRouteWrites(t, cp, "unchanged routes")
	for _, name := range routeNames(cp) {
		eventually(t, func() error {
			route := tlsRoute(t, cp.Namespace, name)
			route.Labels["example.com/team"] = "a"
			route.OwnerReferences = append([]metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "anchor", UID: "2b1c6a5e-9d4f-4f0e-8a3b-7c2d1e0f9a8b"}}, route.OwnerReferences...)
			return k8s.Update(t.Context(), route, client.FieldOwner("intruder"))
		})
	}
	expectNoRouteWrites(t, cp, "routes with a foreign label and an extra owner before Lodia's")
	for _, name := range routeNames(cp) {
		if route := tlsRoute(t, cp.Namespace, name); route.Labels["example.com/team"] != "a" || len(route.OwnerReferences) != 2 {
			t.Errorf("TLSRoute %s has the labels %v and the owners %+v", name, route.Labels, route.OwnerReferences)
		}
	}
}

func TestAnEditedRouteIsRepaired(t *testing.T) {
	cp := gatewayRunning(t)
	want := map[string]*gatewayv1.TLSRoute{}
	for _, name := range routeNames(cp) {
		want[name] = tlsRoute(t, cp.Namespace, name)
	}
	backend := func(r *gatewayv1.TLSRoute) *gatewayv1.BackendRef { return &r.Spec.Rules[0].BackendRefs[0] }
	for name, edit := range map[string]func(*gatewayv1.TLSRoute){
		"an extra hostname":   func(r *gatewayv1.TLSRoute) { r.Spec.Hostnames = append(r.Spec.Hostnames, "extra."+zone) },
		"a replaced hostname": func(r *gatewayv1.TLSRoute) { r.Spec.Hostnames = []gatewayv1.Hostname{"replaced." + zone} },
		"an extra parentRef": func(r *gatewayv1.TLSRoute) {
			r.Spec.ParentRefs = append(r.Spec.ParentRefs, gatewayv1.ParentReference{Name: "elsewhere"})
		},
		"a parentRef port": func(r *gatewayv1.TLSRoute) { r.Spec.ParentRefs[0].Port = new(gatewayv1.PortNumber(443)) },
		"sectionName":      func(r *gatewayv1.TLSRoute) { r.Spec.ParentRefs[0].SectionName = new(gatewayv1.SectionName("other")) },
		"an extra backend": func(r *gatewayv1.TLSRoute) {
			r.Spec.Rules[0].BackendRefs = append(r.Spec.Rules[0].BackendRefs, gatewayv1.BackendRef{BackendObjectReference: gatewayv1.BackendObjectReference{Name: "elsewhere", Port: new(gatewayv1.PortNumber(443))}})
		},
		"weight 0":              func(r *gatewayv1.TLSRoute) { backend(r).Weight = new(int32(0)) },
		"the backend's group":   func(r *gatewayv1.TLSRoute) { backend(r).Group = new(gatewayv1.Group("example.com")) },
		"the backend's kind":    func(r *gatewayv1.TLSRoute) { backend(r).Kind = new(gatewayv1.Kind("ServiceImport")) },
		"the backend's port":    func(r *gatewayv1.TLSRoute) { backend(r).Port = new(gatewayv1.PortNumber(8443)) },
		"a removed label":       func(r *gatewayv1.TLSRoute) { delete(r.Labels, lodiav1alpha1.ControlPlaneLabel) },
		"no blockOwnerDeletion": func(r *gatewayv1.TLSRoute) { r.OwnerReferences[0].BlockOwnerDeletion = new(false) },
	} {
		t.Run(name, func(t *testing.T) {
			for _, route := range routeNames(cp) {
				eventually(t, func() error {
					fresh := tlsRoute(t, cp.Namespace, route)
					edit(fresh)
					return k8s.Update(t.Context(), fresh, client.FieldOwner("intruder"))
				})
			}
			for _, route := range routeNames(cp) {
				eventually(t, func() error {
					fresh := tlsRoute(t, cp.Namespace, route)
					if !equality.Semantic.DeepEqual(fresh.Spec, want[route].Spec) || fresh.Labels[lodiav1alpha1.ControlPlaneLabel] != cp.Name ||
						!equality.Semantic.DeepEqual(fresh.OwnerReferences, want[route].OwnerReferences) {
						return fmt.Errorf("TLSRoute %s is %+v with the labels %v and the owners %+v", route, fresh.Spec, fresh.Labels, fresh.OwnerReferences)
					}
					return nil
				})
			}
		})
	}
}

func TestUseDefaultGatewaysOnTheExperimentalCRDIsLeftAlone(t *testing.T) {
	module, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "sigs.k8s.io/gateway-api").Output()
	if err != nil {
		t.Fatal(err)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "api", "crds"), filepath.Join(strings.TrimSpace(string(module)), "config", "crd", "experimental")},
		ErrorIfCRDPathMissing: true,
		UseExistingCluster:    new(false),
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	experimental, err := client.NewWithWatch(cfg, client.Options{Scheme: operator.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "experimental"}}
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: ns.Name},
		Spec:       gatewayv1.GatewaySpec{GatewayClassName: "example", Listeners: []gatewayv1.Listener{passthrough("tls", "*."+zone, 443)}},
	}
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns.Name},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   version,
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint:  onGateway(ns.Name, "dev"),
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	for _, obj := range []client.Object{ns, gw, cp} {
		if err := experimental.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	var lists atomic.Int32
	reader := interceptor.NewClient(experimental, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*gatewayv1.TLSRouteList); ok {
				lists.Add(1)
			}
			return c.List(ctx, list, opts...)
		},
	})
	r := &workloads.Reconciler{Client: experimental, Reader: reader, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	route := &gatewayv1.TLSRoute{}
	if err := experimental.Get(t.Context(), client.ObjectKey{Namespace: ns.Name, Name: "dev-kube-apiserver"}, route); err != nil {
		t.Fatal(err)
	}
	route.Spec.UseDefaultGateways = gatewayv1.GatewayDefaultScopeAll
	if err := experimental.Update(t.Context(), route, client.FieldOwner("intruder")); err != nil {
		t.Fatal(err)
	}
	before := route.ResourceVersion
	lists.Store(0)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if err := experimental.Get(t.Context(), client.ObjectKeyFromObject(route), route); err != nil {
		t.Fatal(err)
	}
	if route.Spec.UseDefaultGateways != gatewayv1.GatewayDefaultScopeAll || route.ResourceVersion != before {
		t.Errorf("useDefaultGateways is %q at resourceVersion %s, want All left at %s", route.Spec.UseDefaultGateways, route.ResourceVersion, before)
	}
	if n := lists.Load(); n != 1 {
		t.Errorf("the pass read the routes without the cache %d times, want once, for the route that compares unequal", n)
	}
}

func TestARoutingProblemLeavesTheAPIServerReady(t *testing.T) {
	ns := namespace(t)
	createGateway(t, ns)
	binding := refuse(t, ns, gatewayv1.GroupName, "tlsroutes", "dev-tunnel", newRoute(ns, "dev-tunnel", "tunnel.dev."+zone, ns, "tls"))
	cp := createNamed(t, ns, "dev", onGateway(ns, "dev"))
	inputsReady(t, cp)
	setTrue(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	eventually(t, func() error {
		if err := expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused); err != nil {
			return err
		}
		return expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
	})
	if c := routesReady(t, cp); !strings.HasPrefix(c.Message, "TLSRoute dev-kube-apiserver has no status from Gateway "+ns+"/shared yet; TLSRoute dev-konnectivity has no status from Gateway "+ns+"/shared yet; the API server refuses TLSRoute dev-tunnel; the reason is in Lodia's log") {
		t.Errorf("RoutesReady says %q", c.Message)
	}
	if tlsRouteOrNil(t, ns, "dev-kube-apiserver") == nil || tlsRouteOrNil(t, ns, "dev-konnectivity") == nil || tlsRouteOrNil(t, ns, "dev-tunnel") != nil {
		t.Error("want the two routes before the refused one, and not the refused one")
	}
	r := &workloads.Reconciler{Client: k8s, Reader: k8s, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !apistatus.Refused(err) {
		t.Errorf("a pass whose route is refused returned %v, want the refusal for the backoff", err)
	}

	if err := k8s.Delete(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		if tlsRouteOrNil(t, ns, "dev-tunnel") == nil {
			return errors.New("no dev-tunnel yet")
		}
		return nil
	})
	accept(t, cp)
	eventually(t, func() error { return expectRoutes(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted) })

	held := tlsRoute(t, ns, "dev-konnectivity")
	held.OwnerReferences = []metav1.OwnerReference{foreignController()}
	if err := k8s.Update(t.Context(), held); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		if err := expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere); err != nil {
			return err
		}
		return expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady)
	})
	if c := routesReady(t, cp); c.Message != "another owner keeps TLSRoute dev-konnectivity by Example held (UID 7a3c5e1f-2b4d-4c6e-8f0a-1b2c3d4e5f60); Workloads writes no route over it" {
		t.Errorf("RoutesReady says %q", c.Message)
	}
	if fresh := tlsRoute(t, ns, "dev-konnectivity"); fresh.ResourceVersion != held.ResourceVersion {
		t.Error("the route another owner keeps was written")
	}
	if c := meta.FindStatusCondition(get(t, cp).Status.Conditions, lodiav1alpha1.ConditionAccessReady); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("AccessReady is %+v", c)
	}
}

func TestASameHostnameRestoreWaitsForItsSource(t *testing.T) {
	ns := namespace(t)
	createGateway(t, ns)
	prod := sourceOnGateway(t, ns)
	leftover := handmade(t, ns, "prod-kube-apiserver", "api.prod."+zone, ns, "tls", controllerOf(prod))
	cp := createNamed(t, ns, "restored", onGateway(ns, "prod"), func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.RestoreFrom = &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: "prod-backup"}, Release: lodiav1alpha1.RestoreHeld}
	})
	inputsReady(t, cp)
	setRestore(t, cp, lodiav1alpha1.UIDReference{Name: prod.Name, UID: prod.UID})
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	eventually(t, func() error {
		if err := expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonHostnameConflict); err != nil {
			return err
		}
		return expectAt(cp, 1, metav1.ConditionTrue, lodiav1alpha1.ReasonReady,
			heldUntil+"spec.restoreFrom.release is Held; ControlPlane prod (UID "+string(prod.UID)+"), the Backup's source, exists; AddonsApplied is not True for generation 1")
	})
	if c := routesReady(t, cp); !strings.Contains(c.Message, "api.prod."+zone+" by ControlPlane "+ns+"/prod (UID "+string(prod.UID)+")") {
		t.Errorf("RoutesReady says %q", c.Message)
	}
	if e := get(t, cp).Status.Endpoint; e == nil || e.APIServer.Host != "api.prod."+zone {
		t.Errorf("status.endpoint is %+v", e)
	}
	expectNoRoutes(t, cp)

	quiet(t, cp)
	if err := k8s.Delete(t.Context(), prod); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		if c := routesReady(t, cp); c.Reason != lodiav1alpha1.ReasonHostnameConflict || !strings.Contains(c.Message, "is held by TLSRoute "+ns+"/prod-kube-apiserver") {
			return fmt.Errorf("RoutesReady is %+v", c)
		}
		if tlsRouteOrNil(t, ns, "restored-konnectivity") == nil || tlsRouteOrNil(t, ns, "restored-tunnel") == nil {
			return errors.New("no route for the hostnames the source's routes left free")
		}
		return nil
	})
	quiet(t, cp)
	if err := k8s.Delete(t.Context(), leftover); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		if tlsRouteOrNil(t, ns, "restored-kube-apiserver") == nil {
			return errors.New("no restored-kube-apiserver yet")
		}
		return nil
	})
}

func TestAGatewayPassThatChangesNothingWritesNothing(t *testing.T) {
	cp := gatewayRunning(t)
	if e := get(t, cp).Status.Endpoint; e == nil || e.Tunnel == nil {
		t.Fatalf("status.endpoint is %+v", e)
	}
	before := resourceVersions(t, cp.Namespace, &corev1.ServiceList{}, &gatewayv1.TLSRouteList{})
	expectNoRouteWrites(t, cp, "an unchanged ControlPlane")
	after := resourceVersions(t, cp.Namespace, &corev1.ServiceList{}, &gatewayv1.TLSRouteList{})
	if fmt.Sprint(after) != fmt.Sprint(before) {
		t.Errorf("a pass over an unchanged gateway ControlPlane wrote:\n%v\nthen\n%v", before, after)
	}
}

func TestAGatewayControlPlanesFirstPassSaysRoutesReadyIsReconciling(t *testing.T) {
	ns := namespace(t)
	events, err := k8s.Watch(t.Context(), &lodiav1alpha1.ControlPlaneList{}, client.InNamespace(ns))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Stop()
	createNamed(t, ns, "dev", onGateway(ns, "dev"))
	for {
		select {
		case event := <-events.ResultChan():
			fresh, ok := event.Object.(*lodiav1alpha1.ControlPlane)
			if !ok || event.Type != watch.Modified || meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady) == nil {
				continue
			}
			for _, conditionType := range []string{lodiav1alpha1.ConditionAPIServerReady, lodiav1alpha1.ConditionRoutesReady} {
				c := meta.FindStatusCondition(fresh.Status.Conditions, conditionType)
				if c == nil || c.Status != metav1.ConditionUnknown || c.Reason != lodiav1alpha1.ReasonReconciling {
					t.Errorf("the first status says %s %+v, want Unknown/%s", conditionType, c, lodiav1alpha1.ReasonReconciling)
				}
			}
			return
		case <-time.After(30 * time.Second):
			t.Fatal("no APIServerReady condition was written")
		}
	}
}

func TestAGatewayControlPlaneWithoutRoutesReadyGetsIt(t *testing.T) {
	ns := namespace(t)
	cp := createNamed(t, ns, "dev", onGateway(ns, "dev"))
	eventually(t, func() error { return expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonGatewayNotFound) })
	fresh := get(t, cp)
	kept, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&lodiav1alpha1.ControlPlaneStatus{
		Conditions: []metav1.Condition{*meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)},
	})
	if err != nil {
		t.Fatal(err)
	}
	applyStatus(t, cp, workloads.FieldManager, kept)
	reconcileNow(t, cp)
	eventually(t, func() error { return expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonGatewayNotFound) })
}

func TestARestartBetweenWritesGoesThroughTheUncachedRead(t *testing.T) {
	cp := gatewayRunning(t)
	before := resourceVersions(t, cp.Namespace, &gatewayv1.TLSRouteList{})
	var lists atomic.Int32
	restarted := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*gatewayv1.TLSRoute); ok {
				return apierrors.NewNotFound(schema.GroupResource{Group: gatewayv1.GroupName, Resource: "tlsroutes"}, key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*gatewayv1.TLSRouteList); ok {
				lists.Add(1)
			}
			return c.List(ctx, list, opts...)
		},
	})
	r := &workloads.Reconciler{Client: restarted, Reader: restarted, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if n := lists.Load(); n != 3 {
		t.Errorf("the pass read the routes without the cache %d times, want once per route", n)
	}
	if after := resourceVersions(t, cp.Namespace, &gatewayv1.TLSRouteList{}); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Errorf("the routes moved: %v, then %v", before, after)
	}
	if err := expectRoutes(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted); err != nil {
		t.Error(err)
	}
}

func TestATransientErrorReturnsBeforeAnyStatus(t *testing.T) {
	cp := gatewayRunning(t)
	unavailable := apierrors.NewServiceUnavailable("injected")
	notCached := func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*gatewayv1.TLSRoute); ok {
			return apierrors.NewNotFound(schema.GroupResource{Group: gatewayv1.GroupName, Resource: "tlsroutes"}, key.Name)
		}
		return c.Get(ctx, key, obj, opts...)
	}
	for name, c := range map[string]struct{ client, reader interceptor.Funcs }{
		"the live read": {reader: interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				return unavailable
			}
			return c.Get(ctx, key, obj, opts...)
		}}},
		"the Gateway's read": {client: interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*gatewayv1.Gateway); ok {
				return unavailable
			}
			return c.Get(ctx, key, obj, opts...)
		}}},
		"the uncached read of the routes": {client: interceptor.Funcs{Get: notCached}, reader: interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*gatewayv1.TLSRouteList); ok {
					return unavailable
				}
				return c.List(ctx, list, opts...)
			}}},
		"a route's apply": {client: interceptor.Funcs{Get: notCached, Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			if kindOf(t, obj) == "TLSRoute" {
				return unavailable
			}
			return c.Apply(ctx, obj, opts...)
		}}},
	} {
		t.Run(name, func(t *testing.T) {
			r := &workloads.Reconciler{Client: noControlPlaneWrite(t, interceptor.NewClient(k8s, c.client)), Reader: interceptor.NewClient(k8s, c.reader), SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true}
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !apierrors.IsServiceUnavailable(err) {
				t.Errorf("the pass returned %v, want the injected error", err)
			}
		})
	}
}

func TestAPredecessorsRoutesWaitForTheGarbageCollector(t *testing.T) {
	ns := namespace(t)
	createGateway(t, ns)
	predecessor := metav1.OwnerReference{APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "dev", UID: "5d1e8a52-5f0e-4bd3-a1a8-0f3c0e6a8b11", Controller: new(true)}
	sameName := handmade(t, ns, "dev-kube-apiserver", "api.dev."+zone, ns, "tls", predecessor)
	otherName := handmade(t, ns, "old-konnectivity", "konnectivity.dev."+zone, ns, "tls", predecessor)
	cp := createNamed(t, ns, "dev", onGateway(ns, "dev"))
	eventually(t, func() error { return expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonHostnameConflict) })
	want := "another owner keeps TLSRoute dev-kube-apiserver by ControlPlane dev (UID 5d1e8a52-5f0e-4bd3-a1a8-0f3c0e6a8b11); Workloads writes no route over it; " +
		"hostname konnectivity.dev." + zone + " is held by TLSRoute " + ns + "/old-konnectivity on Gateway " + ns + "/shared; Workloads writes no route for it until that one is gone; " +
		"TLSRoute dev-tunnel has no status from Gateway " + ns + "/shared yet"
	if c := routesReady(t, cp); c.Message != want {
		t.Errorf("RoutesReady says %q, want %q", c.Message, want)
	}
	if fresh := tlsRoute(t, ns, sameName.Name); fresh.ResourceVersion != sameName.ResourceVersion {
		t.Error("the predecessor's route was written")
	}
	if tlsRouteOrNil(t, ns, "dev-konnectivity") != nil {
		t.Error("a route was written for a hostname the predecessor's route holds")
	}
	uncached := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*gatewayv1.TLSRoute); ok {
				return apierrors.NewNotFound(schema.GroupResource{Group: gatewayv1.GroupName, Resource: "tlsroutes"}, key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	r := &workloads.Reconciler{Client: uncached, Reader: k8s, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if fresh := tlsRoute(t, ns, sameName.Name); fresh.ResourceVersion != sameName.ResourceVersion {
		t.Error("a pass whose cache lacks the predecessor's route wrote over it")
	}
	quiet(t, cp)
	for _, route := range []*gatewayv1.TLSRoute{sameName, otherName} {
		if err := k8s.Delete(t.Context(), route); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, func() error {
		for _, name := range routeNames(cp) {
			if route := tlsRouteOrNil(t, ns, name); route == nil || !metav1.IsControlledBy(route, cp) {
				return fmt.Errorf("TLSRoute %s is %v", name, route)
			}
		}
		return nil
	})
}

func onGateway(gatewayNamespace, prefix string) lodiav1alpha1.Endpoint {
	return lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
		ParentRef: lodiav1alpha1.ParentReference{Name: "shared", Namespace: gatewayNamespace, SectionName: "tls"},
		Hostnames: lodiav1alpha1.GatewayHostnames{
			APIServer:    lodiav1alpha1.Hostname("api." + prefix + "." + zone),
			Konnectivity: lodiav1alpha1.Hostname("konnectivity." + prefix + "." + zone),
			Tunnel:       lodiav1alpha1.Hostname("tunnel." + prefix + "." + zone),
		},
	}}
}

func passthrough(name, hostname string, port int32) gatewayv1.Listener {
	l := gatewayv1.Listener{Name: gatewayv1.SectionName(name), Port: port, Protocol: gatewayv1.TLSProtocolType, TLS: &gatewayv1.ListenerTLSConfig{Mode: new(gatewayv1.TLSModePassthrough)}}
	if hostname != "" {
		l.Hostname = new(gatewayv1.Hostname(hostname))
	}
	return l
}

func createGateway(t *testing.T, ns string, listeners ...gatewayv1.Listener) *gatewayv1.Gateway {
	t.Helper()
	if len(listeners) == 0 {
		listeners = []gatewayv1.Listener{passthrough("tls", "*."+zone, 443)}
	}
	gw := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: ns},
		Spec:       gatewayv1.GatewaySpec{GatewayClassName: "example", Listeners: listeners},
	}
	if err := k8s.Create(t.Context(), gw); err != nil {
		t.Fatal(err)
	}
	return gw
}

func gatewayOf(t *testing.T, cp *lodiav1alpha1.ControlPlane) *gatewayv1.Gateway {
	t.Helper()
	ref := cp.Spec.Endpoint.Gateway.ParentRef
	gw := &gatewayv1.Gateway{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, gw); err != nil {
		t.Fatal(err)
	}
	return gw
}

func changeGateway(t *testing.T, cp *lodiav1alpha1.ControlPlane, change func(*gatewayv1.Gateway)) {
	t.Helper()
	eventually(t, func() error {
		gw := gatewayOf(t, cp)
		change(gw)
		return k8s.Update(t.Context(), gw)
	})
}

func listenerStatus(kinds []gatewayv1.RouteGroupKind, accepted, programmed metav1.ConditionStatus, reason string) gatewayv1.ListenerStatus {
	now := metav1.Now()
	return gatewayv1.ListenerStatus{
		Name:           "tls",
		SupportedKinds: kinds,
		Conditions: []metav1.Condition{
			{Type: string(gatewayv1.ListenerConditionAccepted), Status: accepted, Reason: reason, Message: "set by the test", LastTransitionTime: now},
			{Type: string(gatewayv1.ListenerConditionProgrammed), Status: programmed, Reason: reason, Message: "set by the test", LastTransitionTime: now},
		},
	}
}

func setListenerStatus(t *testing.T, gw *gatewayv1.Gateway, statuses ...gatewayv1.ListenerStatus) {
	t.Helper()
	eventually(t, func() error {
		fresh := &gatewayv1.Gateway{}
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(gw), fresh); err != nil {
			return err
		}
		fresh.Status.Listeners = statuses
		return k8s.Status().Update(t.Context(), fresh)
	})
}

func createNamed(t *testing.T, ns, name string, endpoint lodiav1alpha1.Endpoint, change ...func(*lodiav1alpha1.ControlPlaneSpec)) *lodiav1alpha1.ControlPlane {
	t.Helper()
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   version,
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint:  endpoint,
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	for _, c := range change {
		c(&cp.Spec)
	}
	if err := k8s.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func sourceOnGateway(t *testing.T, ns string) *lodiav1alpha1.ControlPlane {
	t.Helper()
	return createNamed(t, ns, "prod", onGateway(ns, "prod"), func(spec *lodiav1alpha1.ControlPlaneSpec) { spec.Paused = true })
}

func gatewayRunning(t *testing.T) *lodiav1alpha1.ControlPlane {
	t.Helper()
	ns := namespace(t)
	createGateway(t, ns)
	cp := createNamed(t, ns, "dev", onGateway(ns, "dev"))
	inputsReady(t, cp)
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	accept(t, cp)
	eventually(t, func() error {
		if err := expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady); err != nil {
			return err
		}
		return expectRoutes(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted)
	})
	return cp
}

func routeNames(cp *lodiav1alpha1.ControlPlane) []string {
	return []string{cp.Name + "-kube-apiserver", cp.Name + "-konnectivity", cp.Name + "-tunnel"}
}

func tlsRouteOrNil(t *testing.T, ns, name string) *gatewayv1.TLSRoute {
	t.Helper()
	route := &gatewayv1.TLSRoute{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, route); err != nil {
		if client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
		return nil
	}
	return route
}

func tlsRoute(t *testing.T, ns, name string) *gatewayv1.TLSRoute {
	t.Helper()
	route := tlsRouteOrNil(t, ns, name)
	if route == nil {
		t.Fatalf("no TLSRoute %s/%s", ns, name)
	}
	return route
}

func expectNoRoutes(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	for _, name := range routeNames(cp) {
		if tlsRouteOrNil(t, cp.Namespace, name) != nil {
			t.Errorf("TLSRoute %s exists", name)
		}
	}
}

func handmade(t *testing.T, ns, name, hostname, gatewayNamespace, section string, owners ...metav1.OwnerReference) *gatewayv1.TLSRoute {
	t.Helper()
	route := newRoute(ns, name, hostname, gatewayNamespace, section, owners...)
	if err := k8s.Create(t.Context(), route); err != nil {
		t.Fatal(err)
	}
	return route
}

func newRoute(ns, name, hostname, gatewayNamespace, section string, owners ...metav1.OwnerReference) *gatewayv1.TLSRoute {
	route := &gatewayv1.TLSRoute{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"example.com/made": "by-hand"}},
		Spec: gatewayv1.TLSRouteSpec{
			CommonRouteSpec: gatewayv1.CommonRouteSpec{ParentRefs: []gatewayv1.ParentReference{{
				Namespace: new(gatewayv1.Namespace(gatewayNamespace)), Name: "shared", SectionName: new(gatewayv1.SectionName(section)),
			}}},
			Hostnames: []gatewayv1.Hostname{gatewayv1.Hostname(hostname)},
			Rules: []gatewayv1.TLSRouteRule{{BackendRefs: []gatewayv1.BackendRef{{
				BackendObjectReference: gatewayv1.BackendObjectReference{Name: "elsewhere", Port: new(gatewayv1.PortNumber(443))},
			}}}},
		},
	}
	for _, owner := range owners {
		route.OwnerReferences = append(route.OwnerReferences, owner)
		if owner.Kind == "ControlPlane" {
			route.Labels[lodiav1alpha1.ControlPlaneLabel] = owner.Name
		}
	}
	return route
}

func controllerOf(cp *lodiav1alpha1.ControlPlane) metav1.OwnerReference {
	return *metav1.NewControllerRef(cp, lodiav1alpha1.GroupVersion.WithKind("ControlPlane"))
}

func foreignController() metav1.OwnerReference {
	return metav1.OwnerReference{APIVersion: "example.com/v1", Kind: "Example", Name: "held", UID: "7a3c5e1f-2b4d-4c6e-8f0a-1b2c3d4e5f60", Controller: new(true)}
}

func changeService(t *testing.T, cp *lodiav1alpha1.ControlPlane, change func(*corev1.Service)) {
	t.Helper()
	eventually(t, func() error {
		svc := getService(t, cp)
		change(svc)
		return k8s.Update(t.Context(), svc)
	})
}

func accept(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	for _, name := range routeNames(cp) {
		eventually(t, func() error {
			route := tlsRouteOrNil(t, cp.Namespace, name)
			if route == nil {
				return fmt.Errorf("no TLSRoute %s yet", name)
			}
			return writeRouteStatus(route, metav1.ConditionTrue, metav1.ConditionTrue, "Accepted", route.Generation)
		})
	}
}

func setRouteStatus(t *testing.T, route *gatewayv1.TLSRoute, accepted, resolved metav1.ConditionStatus, reason string, generation int64) {
	t.Helper()
	eventually(t, func() error {
		fresh := tlsRoute(t, route.Namespace, route.Name)
		return writeRouteStatus(fresh, accepted, resolved, reason, generation)
	})
}

func writeRouteStatus(route *gatewayv1.TLSRoute, accepted, resolved metav1.ConditionStatus, reason string, generation int64) error {
	now := metav1.Now()
	route.Status.Parents = []gatewayv1.RouteParentStatus{{
		ParentRef:      route.Spec.ParentRefs[0],
		ControllerName: "example.com/gateway",
		Conditions: []metav1.Condition{
			{Type: string(gatewayv1.RouteConditionAccepted), Status: accepted, Reason: reason, Message: "set by the test", ObservedGeneration: generation, LastTransitionTime: now},
			{Type: string(gatewayv1.RouteConditionResolvedRefs), Status: resolved, Reason: reason, Message: "set by the test", ObservedGeneration: generation, LastTransitionTime: now},
		},
	}}
	return k8s.Status().Update(context.Background(), route)
}

func routesReady(t *testing.T, cp *lodiav1alpha1.ControlPlane) *metav1.Condition {
	t.Helper()
	return meta.FindStatusCondition(get(t, cp).Status.Conditions, lodiav1alpha1.ConditionRoutesReady)
}

func expectRoutes(cp *lodiav1alpha1.ControlPlane, status metav1.ConditionStatus, reason string) error {
	fresh := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		return err
	}
	c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionRoutesReady)
	if c == nil || c.Status != status || c.Reason != reason || c.ObservedGeneration != fresh.Generation {
		return fmt.Errorf("RoutesReady is %+v, want %s %s at generation %d", c, status, reason, fresh.Generation)
	}
	return nil
}

func expectRoutesAt(cp *lodiav1alpha1.ControlPlane, reason string) error {
	status := metav1.ConditionFalse
	if reason == lodiav1alpha1.ReasonAccepted {
		status = metav1.ConditionTrue
	}
	return expectRoutes(cp, status, reason)
}

func expectEndpointIs(cp *lodiav1alpha1.ControlPlane, want lodiav1alpha1.EndpointStatus) error {
	fresh := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		return err
	}
	if e := fresh.Status.Endpoint; e == nil || !equality.Semantic.DeepEqual(*e, want) {
		return fmt.Errorf("status.endpoint is %+v, want %+v", e, want)
	}
	return nil
}

func expectEndpoint(t *testing.T, cp *lodiav1alpha1.ControlPlane, want lodiav1alpha1.EndpointStatus) {
	t.Helper()
	if err := expectEndpointIs(cp, want); err != nil {
		t.Error(err)
	}
}

func expectNoRouteWrites(t *testing.T, cp *lodiav1alpha1.ControlPlane, what string) {
	t.Helper()
	var writes, lists, statuses atomic.Int32
	route := func(obj client.Object) {
		if _, ok := obj.(*gatewayv1.TLSRoute); ok {
			writes.Add(1)
		}
	}
	counting := interceptor.NewClient(k8s, interceptor.Funcs{
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			if kindOf(t, obj) == "TLSRoute" {
				writes.Add(1)
			}
			return c.Apply(ctx, obj, opts...)
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			route(obj)
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			route(obj)
			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			route(obj)
			return c.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			route(obj)
			return c.Delete(ctx, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*gatewayv1.TLSRouteList); ok {
				lists.Add(1)
			}
			return c.List(ctx, list, opts...)
		},
		SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
			statuses.Add(1)
			return c.SubResource(subResource).Apply(ctx, obj, opts...)
		},
	})
	r := &workloads.Reconciler{Client: noControlPlaneWrite(t, counting), Reader: counting, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if writes.Load() > 0 || lists.Load() > 0 || statuses.Load() > 0 {
		t.Errorf("a pass over %s wrote %d routes, read them without the cache %d times and applied %d statuses", what, writes.Load(), lists.Load(), statuses.Load())
	}
}

func refuse(t *testing.T, ns, group, resource, name string, probe client.Object) *admissionregistrationv1.ValidatingAdmissionPolicyBinding {
	t.Helper()
	version := "v1"
	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "refuse-" + resource + "-in-" + ns},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			FailurePolicy: new(admissionregistrationv1.Fail),
			MatchConstraints: &admissionregistrationv1.MatchResources{ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
				RuleWithOperations: admissionregistrationv1.RuleWithOperations{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
					Rule:       admissionregistrationv1.Rule{APIGroups: []string{group}, APIVersions: []string{version}, Resources: []string{resource}},
				},
			}}},
			Validations: []admissionregistrationv1.Validation{{
				Expression: "object.metadata.namespace != '" + ns + "' || object.metadata.name != '" + name + "'",
				Message:    "no " + name + " in " + ns,
			}},
		},
	}
	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: policy.Name},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        policy.Name,
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
		},
	}
	for _, obj := range []client.Object{policy, binding} {
		if err := k8s.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := client.IgnoreNotFound(k8s.Delete(context.Background(), obj)); err != nil {
				t.Error(err)
			}
		})
	}
	eventually(t, func() error {
		var err error
		if probe.GetResourceVersion() == "" {
			err = k8s.Create(t.Context(), probe.DeepCopyObject().(client.Object), client.DryRunAll)
		} else {
			err = k8s.Update(t.Context(), probe.DeepCopyObject().(client.Object), client.DryRunAll)
		}
		if err == nil || !strings.Contains(err.Error(), "no "+name+" in "+ns) {
			return fmt.Errorf("the policy does not refuse %s yet: %w", name, err)
		}
		return nil
	})
	return binding
}

func decoded(t *testing.T, obj runtime.ApplyConfiguration) *lodiav1alpha1.ControlPlane {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{}
	if err := json.Unmarshal(raw, cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func kindOf(t *testing.T, obj runtime.ApplyConfiguration) string {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	var typed metav1.TypeMeta
	if err := json.Unmarshal(raw, &typed); err != nil {
		t.Fatal(err)
	}
	return typed.Kind
}

func waitFor(timeout time.Duration, done func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !done() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return true
}

func quiet(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	state := func() string {
		return get(t, cp).ResourceVersion + fmt.Sprint(resourceVersions(t, cp.Namespace, &gatewayv1.TLSRouteList{}))
	}
	last, since := state(), time.Now()
	eventually(t, func() error {
		if now := state(); now != last {
			last, since = now, time.Now()
		}
		if time.Since(since) < 2*time.Second {
			return errors.New("the ControlPlane or its routes changed in the last two seconds")
		}
		return nil
	})
}
