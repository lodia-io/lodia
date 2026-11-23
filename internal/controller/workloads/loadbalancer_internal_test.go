// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func balancerPlane(host lodiav1alpha1.EndpointHost, published string, serviceCIDR lodiav1alpha1.CIDR) *lodiav1alpha1.ControlPlane {
	cp := &lodiav1alpha1.ControlPlane{Spec: lodiav1alpha1.ControlPlaneSpec{
		Endpoint: lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{Host: host}},
		Network:  lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{serviceCIDR}},
	}}
	if published != "" {
		cp.Status.Endpoint = atHost(cp, published)
	}
	return cp
}

func ingresses(entries ...string) *corev1.Service {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "dev-kube-apiserver"}}
	for _, e := range entries {
		var in corev1.LoadBalancerIngress
		for part := range strings.SplitSeq(e, "+") {
			switch {
			case part == "":
			case strings.ContainsAny(part, ":") || strings.Trim(part, "0123456789.") == "":
				in.IP = part
			default:
				in.Hostname = part
			}
		}
		svc.Status.LoadBalancer.Ingress = append(svc.Status.LoadBalancer.Ingress, in)
	}
	return svc
}

func TestADiscoveredHostIsKeptWhileTheLoadBalancerReportsIt(t *testing.T) {
	for _, c := range []struct {
		name, published string
		ingress         []string
		want            string
	}{
		{"none published, none reported", "", nil, ""},
		{"none published, an empty entry", "", []string{""}, ""},
		{"an IP", "", []string{"203.0.113.10"}, "203.0.113.10"},
		{"a host name before its IP", "", []string{"lb.example.com+203.0.113.10"}, "lb.example.com"},
		{"the first entry that reports one", "", []string{"", "203.0.113.11", "lb.example.com"}, "203.0.113.11"},
		{"an added entry before it", "203.0.113.10", []string{"203.0.113.11", "203.0.113.10"}, "203.0.113.10"},
		{"reordered", "203.0.113.10", []string{"203.0.113.10", "203.0.113.11"}, "203.0.113.10"},
		{"behind a host name", "203.0.113.10", []string{"lb.example.com", "203.0.113.10"}, "203.0.113.10"},
		{"its IP replaced beside the name", "lb.example.com", []string{"lb.example.com+198.51.100.7"}, "lb.example.com"},
		{"a host name behind an IP", "lb.example.com", []string{"198.51.100.7", "lb.example.com"}, "lb.example.com"},
		{"an IP spelled another way", "2001:db8::10", []string{"2001:DB8:0::10"}, "2001:db8::10"},
		{"an ingress that empties", "203.0.113.10", nil, "203.0.113.10"},
		{"entries that report nothing", "203.0.113.10", []string{""}, "203.0.113.10"},
		{"replaced by another address", "203.0.113.10", []string{"198.51.100.7"}, "198.51.100.7"},
		{"a name replaced by an IP", "lb.example.com", []string{"198.51.100.7"}, "198.51.100.7"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := balancerPlane("", c.published, "10.96.0.0/16")
			endpoint, _ := loadBalancer(cp, ingresses(c.ingress...))
			got := ""
			if endpoint != nil {
				got = endpoint.APIServer.Host
				if want := *atHost(cp, got); *endpoint != want {
					t.Errorf("the endpoint is %+v, want %+v", *endpoint, want)
				}
			}
			if got != c.want {
				t.Errorf("the endpoint's host is %q, want %q", got, c.want)
			}
		})
	}
}

func TestTheEndpointAtAHostIsTheSpecsWhateverTheLoadBalancerReports(t *testing.T) {
	for host, inCluster := range map[lodiav1alpha1.EndpointHost]lodiav1alpha1.HostPort{
		"203.0.113.10":        {Host: "203.0.113.10", Port: 443},
		"api.dev.example.com": {Host: "240.0.0.1", Port: 6443},
	} {
		for _, ingress := range [][]string{nil, {"198.51.100.7"}, {"lb.example.com"}} {
			cp := balancerPlane(host, "198.51.100.7", "10.96.0.0/16")
			endpoint, _ := loadBalancer(cp, ingresses(ingress...))
			want := lodiav1alpha1.EndpointStatus{
				APIServer:    lodiav1alpha1.HostPort{Host: string(host), Port: 443},
				Konnectivity: lodiav1alpha1.HostPort{Host: string(host), Port: 8132},
				InCluster:    inCluster,
			}
			if endpoint == nil || *endpoint != want {
				t.Errorf("host %s, ingress %v: the endpoint is %+v, want %+v", host, ingress, endpoint, want)
			}
		}
	}
	cp := balancerPlane("api.dev.example.com", "", "fd00:10:96::/108")
	cp.Spec.APIServer = &lodiav1alpha1.APIServer{AdvertiseAddress: "fd12:3456:789a::1"}
	if endpoint, _ := loadBalancer(cp, ingresses()); endpoint == nil || endpoint.InCluster != (lodiav1alpha1.HostPort{Host: "fd12:3456:789a::1", Port: 6443}) {
		t.Errorf("a name with an advertise address gives the endpoint %+v", endpoint)
	}
}

func TestLoadBalancerReadyFromWhatTheLoadBalancerReports(t *testing.T) {
	const of = "the load balancer of Service dev-kube-apiserver"
	const repair = "the annotation of endpoint.loadBalancer.annotations that asks the load balancer for it is missing or was ignored"
	many := make([]string, 12)
	for i := range many {
		many[i] = fmt.Sprintf("198.51.100.%d", i+1)
	}
	for _, c := range []struct {
		name    string
		host    lodiav1alpha1.EndpointHost
		cidr    lodiav1alpha1.CIDR
		ingress []string
		want    string
	}{
		{"no host, no ingress", "", "10.96.0.0/16", nil, "False WaitingForLoadBalancer " + of + " reports no address yet"},
		{"no host, an entry with nothing", "", "10.96.0.0/16", []string{""}, "False WaitingForLoadBalancer " + of + " reports no address yet"},
		{"no host, an IP", "", "10.96.0.0/16", []string{"198.51.100.7"}, "True Ready " + of + " reports 198.51.100.7"},
		{"no host, a host name and its IP", "", "10.96.0.0/16", []string{"lb.example.com+198.51.100.7"}, "True Ready " + of + " reports lb.example.com, 198.51.100.7"},
		{"a name, no ingress", "api.dev.example.com", "10.96.0.0/16", nil, "False WaitingForLoadBalancer " + of + " reports no address yet"},
		{"a name, an IP of no relation", "api.dev.example.com", "10.96.0.0/16", []string{"198.51.100.7"}, "True Ready " + of + " reports 198.51.100.7"},
		{"an IP, no ingress", "203.0.113.10", "10.96.0.0/16", nil, "False WaitingForLoadBalancer " + of + " reports no address yet"},
		{"an IP, the IP", "203.0.113.10", "10.96.0.0/16", []string{"203.0.113.10"}, "True Ready " + of + " reports endpoint.loadBalancer.host 203.0.113.10"},
		{"an IP, among others", "203.0.113.10", "10.96.0.0/16", []string{"198.51.100.7", "lb.example.com+203.0.113.10"}, "True Ready " + of + " reports endpoint.loadBalancer.host 203.0.113.10"},
		{"an IPv6 address spelled another way", "2001:db8::10", "fd00:10:96::/108", []string{"2001:DB8:0::10"}, "True Ready " + of + " reports endpoint.loadBalancer.host 2001:db8::10"},
		{"an IP, host names alone", "203.0.113.10", "10.96.0.0/16", []string{"a.elb.example.com", "b.elb.example.com"},
			"True Unverified " + of + " reports the host names a.elb.example.com, b.elb.example.com and no IP address, so endpoint.loadBalancer.host 203.0.113.10 cannot be compared with them"},
		{"an IP, another", "203.0.113.10", "10.96.0.0/16", []string{"198.51.100.7"},
			"False AddressMismatch " + of + " reports 198.51.100.7, not endpoint.loadBalancer.host 203.0.113.10: " + repair},
		{"an IP, others beside host names", "203.0.113.10", "10.96.0.0/16", []string{"lb.example.com", "198.51.100.7+b.example.com", "198.51.100.8"},
			"False AddressMismatch " + of + " reports 198.51.100.7, 198.51.100.8, not endpoint.loadBalancer.host 203.0.113.10: " + repair},
		{"an IP, more others than a message names", "203.0.113.10", "10.96.0.0/16", many,
			"False AddressMismatch " + of + " reports " + strings.Join(many[:10], ", ") + " and 2 more, not endpoint.loadBalancer.host 203.0.113.10: " + repair},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, ready := loadBalancer(balancerPlane(c.host, "", c.cidr), ingresses(c.ingress...))
			if ready.Type != lodiav1alpha1.ConditionLoadBalancerReady {
				t.Errorf("the condition is of type %s", ready.Type)
			}
			if got := fmt.Sprintf("%s %s %s", ready.Status, ready.Reason, ready.Message); got != c.want {
				t.Errorf("LoadBalancerReady is %q, want %q", got, c.want)
			}
		})
	}
}

func TestANodePortHostIsOfTheFamilyOfTheFirstServiceRange(t *testing.T) {
	node := func(name string, ready bool, addresses ...corev1.NodeAddress) *corev1.Node {
		n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: corev1.NodeStatus{Addresses: addresses}}
		if ready {
			n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
		}
		return n
	}
	internal := func(address string) corev1.NodeAddress {
		return corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: address}
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	nodes := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		node("a", true, internal("2001:db8::a"), internal("192.0.2.10"), corev1.NodeAddress{Type: corev1.NodeExternalIP, Address: "198.51.100.10"}),
		node("b", false, internal("192.0.2.11"), internal("2001:db8::b")),
		node("c", true, internal("2001:db8::c")),
	).Build()
	r := &Reconciler{Reader: nodes}
	for _, c := range []struct {
		name, published string
		ipv6            bool
		want            string
	}{
		{"IPv4, none published", "", false, "192.0.2.10"},
		{"IPv6, none published", "", true, "2001:db8::a"},
		{"IPv4, a NotReady node's kept", "192.0.2.11", false, "192.0.2.11"},
		{"IPv6, a NotReady node's kept", "2001:db8::b", true, "2001:db8::b"},
		{"IPv6, an IPv4 address published", "192.0.2.11", true, "2001:db8::a"},
		{"IPv4, an address no node reports", "192.0.2.99", false, "192.0.2.10"},
		{"IPv4, an external address", "198.51.100.10", false, "192.0.2.10"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := r.nodeAddress(context.Background(), c.published, c.ipv6)
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Errorf("the node address is %q, want %q", got, c.want)
			}
		})
	}
	only6 := &Reconciler{Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(node("c", true, internal("2001:db8::c"))).Build()}
	if got, err := only6.nodeAddress(context.Background(), "", false); err != nil || got != "" {
		t.Errorf("an IPv4 tenant beside IPv6 nodes alone gets %q, %v", got, err)
	}
}
