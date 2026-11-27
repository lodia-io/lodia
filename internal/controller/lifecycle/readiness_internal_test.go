// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

// Ready True takes the first refusal's reason among its inputs and the oldest generation, and a False input still comes first.
func TestReadyTrueTakesTheFirstRefusalAmongItsInputs(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Generation: 3}}
	for _, conditionType := range readiness(cp) {
		cp.Status.Conditions = append(cp.Status.Conditions, metav1.Condition{Type: conditionType, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, ObservedGeneration: 3})
	}
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionAccessReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonRefused, Message: "ClusterRoleBinding lodia:operator is refused", ObservedGeneration: 3})
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonAuthenticationInvalid, Message: "every replica is ready; not written", ObservedGeneration: 2})
	got := ready(cp)
	if got.Status != metav1.ConditionTrue || got.Reason != lodiav1alpha1.ReasonAuthenticationInvalid || got.Message != "APIServerReady: every replica is ready; not written" || got.ObservedGeneration != 2 {
		t.Errorf("Ready is %+v, want True AuthenticationInvalid naming APIServerReady at generation 2", got)
	}
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionReachable, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, Message: "api: refused", ObservedGeneration: 3})
	if got := ready(cp); got.Status != metav1.ConditionFalse || got.Reason != lodiav1alpha1.ReasonExternalProbeFailed {
		t.Errorf("Ready is %+v, want False ExternalProbeFailed", got)
	}
}
