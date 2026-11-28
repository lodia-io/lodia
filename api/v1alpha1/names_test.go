// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1_test

import (
	"testing"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestNodeProxyAndAdvertiseAddress(t *testing.T) {
	gateway := lodiav1alpha1.Endpoint{Gateway: lodiav1alpha1.GatewayEndpoint{ParentRef: lodiav1alpha1.ParentReference{Name: "tenants", Namespace: "gateway", SectionName: "tls"}}}
	loadBalancer := func(host lodiav1alpha1.EndpointHost) lodiav1alpha1.Endpoint {
		return lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{Host: host}}
	}
	nodePort := lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}
	named := &lodiav1alpha1.APIServer{AdvertiseAddress: "fd12:3456:789a::1"}
	for _, c := range []struct {
		name      string
		endpoint  lodiav1alpha1.Endpoint
		apiServer *lodiav1alpha1.APIServer
		proxy     bool
		advertise string
	}{
		{"gateway", gateway, nil, true, "240.0.0.1"},
		{"gateway with an API server that names no address", gateway, &lodiav1alpha1.APIServer{}, true, "240.0.0.1"},
		{"gateway with an address", gateway, named, true, "fd12:3456:789a::1"},
		{"a load balancer without a host", loadBalancer(""), nil, true, "240.0.0.1"},
		{"a load balancer without a host, with an address", loadBalancer(""), named, true, "fd12:3456:789a::1"},
		{"a load balancer at a name", loadBalancer("api.dev.example.com"), nil, true, "240.0.0.1"},
		{"a load balancer at a name of one label", loadBalancer("api"), nil, true, "240.0.0.1"},
		{"a load balancer at a name, with an address", loadBalancer("api.dev.example.com"), named, true, "fd12:3456:789a::1"},
		{"a load balancer at an IPv4 address", loadBalancer("203.0.113.10"), nil, false, ""},
		{"a load balancer at an IPv6 address", loadBalancer("2001:db8::10"), nil, false, ""},
		{"a load balancer at an IP address, with an address stored under an older schema", loadBalancer("203.0.113.10"), named, false, ""},
		{"a node port", nodePort, nil, false, ""},
		{"a node port, with an address stored under an older schema", nodePort, named, false, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := c.endpoint.NodeProxy(); got != c.proxy {
				t.Errorf("NodeProxy() = %t, want %t", got, c.proxy)
			}
			spec := lodiav1alpha1.ControlPlaneSpec{Endpoint: c.endpoint, APIServer: c.apiServer}
			if got := spec.AdvertiseAddress(); got != c.advertise {
				t.Errorf("AdvertiseAddress() = %q, want %q", got, c.advertise)
			}
		})
	}
}
