// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestTheKonnectivityVersionReplacesTheTagOfAnyReference(t *testing.T) {
	pinned := &lodiav1alpha1.ControlPlane{Spec: lodiav1alpha1.ControlPlaneSpec{
		Addons: &lodiav1alpha1.Addons{Konnectivity: &lodiav1alpha1.Konnectivity{Version: "v0.37.2"}},
	}}
	digest := "@sha256:" + strings.Repeat("ab", 32)
	for image, want := range map[string]string{
		"registry.k8s.io/kas-network-proxy/proxy-server:v0.37.0":                "registry.k8s.io/kas-network-proxy/proxy-server:v0.37.2",
		"registry.example:5000/kas-network-proxy/proxy-server:v0.37.0":          "registry.example:5000/kas-network-proxy/proxy-server:v0.37.2",
		"registry.example:5000/kas-network-proxy/proxy-server:v0.37.0" + digest: "registry.example:5000/kas-network-proxy/proxy-server:v0.37.2",
		"registry.example:5000/kas-network-proxy/proxy-server" + digest:         "registry.example:5000/kas-network-proxy/proxy-server:v0.37.2",
	} {
		if got, err := konnectivityImage(pinned, image); err != nil || got != want {
			t.Errorf("%s pinned to v0.37.2 is %q, %v; want %s", image, got, err, want)
		}
		if got, err := konnectivityImage(&lodiav1alpha1.ControlPlane{}, image); err != nil || got != image {
			t.Errorf("%s without a pin is %q, %v", image, got, err)
		}
	}
}

func TestEveryConfigMapRollsTheAPIServerAlone(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team"}}
	config, err := configMaps(cp)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(config, func(c *corev1.ConfigMap) bool { return c.Name == "dev-authentication-config" }) {
		t.Fatalf("the ConfigMaps are %v", config)
	}
	before := restartHashes(cp.Name, nil, config)
	for i, c := range config {
		changed := slices.Clone(config)
		changed[i] = c.DeepCopy()
		for key := range changed[i].Data {
			changed[i].Data[key] += "\n"
		}
		after := restartHashes(cp.Name, nil, changed)
		if after[apiServer] == before[apiServer] || after[controllerManager] != before[controllerManager] {
			t.Errorf("a change to %s gives the restart hashes %v, then %v: want the API server's alone to change", c.Name, before, after)
		}
	}
}

func TestAnExtraArgumentNeverReplacesAFlagLodiaSets(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team"},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Network: lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
			APIServer: &lodiav1alpha1.APIServer{ExtraArgs: map[string]string{
				"endpoint-reconciler-type": "lease",
				"shutdown-delay-duration":  "70s",
				"enable-admission-plugins": "PodSecurity",
				"event-ttl":                "2h",
			}},
			ControllerManager: &lodiav1alpha1.Component{ExtraArgs: map[string]string{"leader-elect": "false", "node-monitor-grace-period": "40s", "controllers": "-ttl-controller"}},
		},
	}
	r := rendering{cp: cp, etcd: []lodiav1alpha1.HostPort{{Host: "dev-etcd-client.team.svc", Port: 2379}}}
	for component, want := range map[string][]string{
		"kube-apiserver":          {"--endpoint-reconciler-type=none", "--shutdown-delay-duration=15s", "--enable-admission-plugins=NodeRestriction,PodSecurity", "--event-ttl=2h"},
		"kube-controller-manager": {"--leader-elect=true", "--node-monitor-grace-period=1m0s", "--controllers=*,bootstrap-signer-controller,token-cleaner-controller,-ttl-controller"},
	} {
		d := map[string]func() *appsv1.Deployment{"kube-apiserver": r.apiServer, "kube-controller-manager": r.controllerManager}[component]()
		command := d.Spec.Template.Spec.Containers[0].Command
		for _, flag := range want {
			if !slices.Contains(command, flag) {
				t.Errorf("%s runs %v, without %s", component, command, flag)
			}
		}
	}
}
