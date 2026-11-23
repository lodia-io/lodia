// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"strings"
	"testing"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestReadinessTakesTheConditionOfTheWayOutWhereItStandsInTheChainOfCauses(t *testing.T) {
	const build, reach = "CertificatesReady DatastoreReady APIServerReady AccessReady AddonsApplied", "Reachable ControllersLeading"
	for _, c := range []struct {
		name     string
		endpoint lodiav1alpha1.Endpoint
		want     string
	}{
		{"gateway", lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{}}, "RoutesReady " + build + " " + reach},
		{"a load balancer without a host", lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}}, "LoadBalancerReady " + build + " " + reach},
		{"a load balancer at an IP address", lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{Host: "203.0.113.10"}}, build + " LoadBalancerReady " + reach},
		{"a load balancer at a name", lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{Host: "api.dev.example.com"}}, build + " LoadBalancerReady " + reach},
		{"a node port", lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}, build + " " + reach},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := strings.Join(readiness(&lodiav1alpha1.ControlPlane{Spec: lodiav1alpha1.ControlPlaneSpec{Endpoint: c.endpoint}}), " "); got != c.want {
				t.Errorf("Ready is made of %s, want %s", got, c.want)
			}
		})
	}
}
