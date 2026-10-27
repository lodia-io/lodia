// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestKubeProxyServesItsHealthOnTheAddressGiven(t *testing.T) {
	for address, want := range map[string]string{"": "127.0.0.1:10256", "0.0.0.0": "0.0.0.0:10256", "::": "[::]:10256"} {
		cp := &lodiav1alpha1.ControlPlane{
			ObjectMeta: metav1.ObjectMeta{Name: "dev"},
			Spec: lodiav1alpha1.ControlPlaneSpec{
				Network: lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
				Addons:  &lodiav1alpha1.Addons{KubeProxy: &lodiav1alpha1.KubeProxy{HealthzBindAddress: address}},
			},
			Status: lodiav1alpha1.ControlPlaneStatus{
				Target:   &lodiav1alpha1.Target{Version: "v1.35.8"},
				Endpoint: &lodiav1alpha1.EndpointStatus{APIServer: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 443}, Konnectivity: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 8132}, InCluster: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 6443}},
			},
		}
		v, _, waiting, err := newValues(cp, nil)
		if err != nil || waiting != "" {
			t.Fatalf("%q: %v %s", address, err, waiting)
		}
		if v.ProxyHealthz != want {
			t.Errorf("healthzBindAddress %q renders %s, want %s", address, v.ProxyHealthz, want)
		}
	}
}

func TestTheKubernetesEndpointsAreRenderedFromInCluster(t *testing.T) {
	for host, addressType := range map[string]discoveryv1.AddressType{"203.0.113.10": discoveryv1.AddressTypeIPv4, "2001:db8::10": discoveryv1.AddressTypeIPv6} {
		cp := &lodiav1alpha1.ControlPlane{
			ObjectMeta: metav1.ObjectMeta{Name: "dev"},
			Spec:       lodiav1alpha1.ControlPlaneSpec{Network: lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}}},
			Status: lodiav1alpha1.ControlPlaneStatus{
				Target:   &lodiav1alpha1.Target{Version: "v1.35.8"},
				Endpoint: &lodiav1alpha1.EndpointStatus{APIServer: lodiav1alpha1.HostPort{Host: "api.dev.example.test", Port: 443}, Konnectivity: lodiav1alpha1.HostPort{Host: "api.dev.example.test", Port: 8132}, InCluster: lodiav1alpha1.HostPort{Host: host, Port: 31443}},
			},
		}
		v, _, waiting, err := newValues(cp, nil)
		if err != nil || waiting != "" {
			t.Fatalf("%s: %v %s", host, err, waiting)
		}
		objects, err := render(v)
		if err != nil {
			t.Fatal(err)
		}
		var endpoints *unstructured.Unstructured
		slice := &discoveryv1.EndpointSlice{}
		for _, obj := range objects {
			switch entryOf(obj) {
			case entry{Version: "v1", Kind: "Endpoints", Namespace: metav1.NamespaceDefault, Name: "kubernetes"}:
				endpoints = obj
			case entry{Group: "discovery.k8s.io", Version: "v1", Kind: "EndpointSlice", Namespace: metav1.NamespaceDefault, Name: "kubernetes"}:
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, slice); err != nil {
					t.Fatal(err)
				}
			}
		}
		if endpoints == nil {
			t.Fatalf("%s renders no Endpoints default/kubernetes", host)
		}
		subsets, err := json.Marshal(endpoints.Object["subsets"])
		if want := fmt.Sprintf(`[{"addresses":[{"ip":%q}],"ports":[{"name":"https","port":31443,"protocol":"TCP"}]}]`, host); err != nil || string(subsets) != want ||
			endpoints.GetLabels()[discoveryv1.LabelSkipMirror] != "true" {
			t.Errorf("%s renders the Endpoints' subsets %s labelled %v, want %s", host, subsets, endpoints.GetLabels(), want)
		}
		if slice.AddressType != addressType || len(slice.Endpoints) != 1 || !slices.Equal(slice.Endpoints[0].Addresses, []string{host}) ||
			slice.Endpoints[0].Conditions.Ready == nil || !*slice.Endpoints[0].Conditions.Ready ||
			len(slice.Ports) != 1 || *slice.Ports[0].Name != "https" || *slice.Ports[0].Port != 31443 || *slice.Ports[0].Protocol != corev1.ProtocolTCP ||
			slice.Labels[discoveryv1.LabelServiceName] != "kubernetes" || slice.Labels[discoveryv1.LabelManagedBy] != "lodia.io" {
			t.Errorf("%s renders the EndpointSlice %s %+v %+v labelled %v", host, slice.AddressType, slice.Endpoints, slice.Ports, slice.Labels)
		}
	}
}
