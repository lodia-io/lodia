// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"maps"
	"net"
	"slices"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestEachWayPodsGoIsNamedInTheCertificatesAndTheKubeconfigs(t *testing.T) {
	const elb = "a1b2c3.elb.example.test"
	loadBalancer := func(host lodiav1alpha1.EndpointHost) lodiav1alpha1.Endpoint {
		return lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{Host: host}}
	}
	for _, c := range []struct {
		namespace        string
		endpoint         lodiav1alpha1.Endpoint
		advertiseAddress string
		public           string
		port             int64
		inCluster        string
		inClusterPort    int64
		ips, dns         []string
		konnIPs, konnDNS []string
	}{
		{"pki-in-cluster-ip-host", loadBalancer(endpointHost), "", endpointHost, 443, endpointHost, 443,
			[]string{endpointHost}, nil, []string{endpointHost}, nil},
		{"pki-in-cluster-name-host", loadBalancer(apiHost), "", apiHost, 443, lodiav1alpha1.DefaultAdvertiseAddress, 6443,
			[]string{lodiav1alpha1.DefaultAdvertiseAddress}, []string{apiHost}, nil, []string{apiHost}},
		{"pki-in-cluster-name-host-advertised", loadBalancer(apiHost), "240.0.0.7", apiHost, 443, "240.0.0.7", 6443,
			[]string{"240.0.0.7"}, []string{apiHost}, nil, []string{apiHost}},
		{"pki-in-cluster-discovered-ip", loadBalancer(""), "", endpointHost, 443, lodiav1alpha1.DefaultAdvertiseAddress, 6443,
			[]string{endpointHost, lodiav1alpha1.DefaultAdvertiseAddress}, nil, []string{endpointHost}, nil},
		{"pki-in-cluster-discovered-name", loadBalancer(""), "", elb, 443, lodiav1alpha1.DefaultAdvertiseAddress, 6443,
			[]string{lodiav1alpha1.DefaultAdvertiseAddress}, []string{elb}, nil, []string{elb}},
		{"pki-in-cluster-node-port", lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}, "", endpointHost, 31443, endpointHost, 31443,
			[]string{endpointHost}, nil, []string{endpointHost}, nil},
	} {
		t.Run(c.namespace, func(t *testing.T) {
			cp := controlPlaneAt(t, c.namespace, c.endpoint, func(spec *lodiav1alpha1.ControlPlaneSpec) {
				if c.advertiseAddress != "" {
					spec.APIServer = &lodiav1alpha1.APIServer{AdvertiseAddress: c.advertiseAddress}
				}
			})
			applyStatus(t, cp, map[string]any{"endpoint": map[string]any{
				"apiServer":    hostPort(c.public, c.port),
				"konnectivity": hostPort(c.public, 8132),
				"inCluster":    hostPort(c.inCluster, c.inClusterPort),
			}})
			reconcile(t, newReconciler(time.Hour), cp)
			expectCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued)

			secrets := secretsOf(t, cp)
			if want := len(secretTable(cp.Name)); len(secrets) != want {
				t.Errorf("%d Secrets, want %d and none of the tunnel's: %v", len(secrets), want, slices.Sorted(maps.Keys(secrets)))
			}
			for _, name := range tunnelSecrets {
				if secrets[name] != nil {
					t.Errorf("%s written outside gateway mode", name)
				}
			}
			ns := cp.Namespace
			for name, want := range map[string][2][]string{
				"dev-apiserver-serving": {
					append([]string{"10.96.0.1", "127.0.0.1"}, c.ips...),
					append([]string{"kubernetes", "kubernetes.default", "kubernetes.default.svc", "kubernetes.default.svc.cluster.local",
						"dev-kube-apiserver", "dev-kube-apiserver." + ns, "dev-kube-apiserver." + ns + ".svc", "localhost"}, c.dns...),
				},
				"dev-konnectivity-server": {append([]string{"127.0.0.1"}, c.konnIPs...), append([]string{"localhost"}, c.konnDNS...)},
			} {
				cert := parseTLS(t, secrets[name]).Cert
				var ips []string
				for _, ip := range cert.IPAddresses {
					ips = append(ips, ip.String())
				}
				if !slices.Equal(slices.Sorted(slices.Values(ips)), slices.Sorted(slices.Values(want[0]))) ||
					!slices.Equal(slices.Sorted(slices.Values(cert.DNSNames)), slices.Sorted(slices.Values(want[1]))) {
					t.Errorf("%s names %v %v, want %v %v", name, ips, cert.DNSNames, want[0], want[1])
				}
			}
			internal := lodiav1alpha1.APIServerServiceURL(ns, cp.Name)
			for name, want := range map[string]string{
				"dev-admin-kubeconfig":              "https://" + net.JoinHostPort(c.public, strconv.Itoa(int(c.port))),
				"dev-konnectivity-kubeconfig":       internal,
				"dev-controller-manager-kubeconfig": internal,
				"dev-scheduler-kubeconfig":          internal,
				"dev-lodia-kubeconfig":              internal,
			} {
				kubeconfig, err := clientcmd.Load(secrets[name].Data[lodiav1alpha1.KubeconfigKey])
				if err != nil {
					t.Fatal(err)
				}
				if server := kubeconfig.Clusters[cp.Name].Server; server != want {
					t.Errorf("%s dials %s, want %s", name, server, want)
				}
			}
		})
	}
}
