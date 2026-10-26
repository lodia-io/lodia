// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
)

func TestKubeProxyServesItsHealthOnTheAddressGiven(t *testing.T) {
	for address, want := range map[string]string{"": "127.0.0.1:10256", "0.0.0.0": "0.0.0.0:10256", "::": "[::]:10256"} {
		cp := &bryggav1alpha1.ControlPlane{
			ObjectMeta: metav1.ObjectMeta{Name: "dev"},
			Spec: bryggav1alpha1.ControlPlaneSpec{
				Network: bryggav1alpha1.Network{ServiceCIDRs: []bryggav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []bryggav1alpha1.CIDR{"10.244.0.0/16"}},
				Addons:  &bryggav1alpha1.Addons{KubeProxy: &bryggav1alpha1.KubeProxy{HealthzBindAddress: address}},
			},
			Status: bryggav1alpha1.ControlPlaneStatus{
				Target:   &bryggav1alpha1.Target{Version: "v1.35.8"},
				Endpoint: &bryggav1alpha1.EndpointStatus{APIServer: bryggav1alpha1.HostPort{Host: "203.0.113.10", Port: 443}, Konnectivity: bryggav1alpha1.HostPort{Host: "203.0.113.10", Port: 8132}},
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
