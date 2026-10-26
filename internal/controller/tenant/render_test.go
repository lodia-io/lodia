// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
				Endpoint: &lodiav1alpha1.EndpointStatus{APIServer: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 443}, Konnectivity: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 8132}},
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
