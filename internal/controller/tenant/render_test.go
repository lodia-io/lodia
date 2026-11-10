// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/versions"
)

var updateGolden = flag.Bool("update-golden", false, "write testdata/render from the render instead of comparing with it")

func TestTheRenderIsPinned(t *testing.T) {
	network := lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}}
	outsideGateway := map[string]lodiav1alpha1.Endpoint{
		"loadBalancer": {LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
		"nodePort":     {NodePort: &lodiav1alpha1.NodePortEndpoint{}},
	}
	for golden, c := range map[string]struct {
		addons    *lodiav1alpha1.Addons
		status    lodiav1alpha1.EndpointStatus
		endpoints map[string]lodiav1alpha1.Endpoint
	}{
		"defaults.yaml": {
			status:    lodiav1alpha1.EndpointStatus{APIServer: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 443}, Konnectivity: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 8132}, InCluster: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 6443}},
			endpoints: outsideGateway,
		},
		"agents-as-a-deployment.yaml": {
			addons: &lodiav1alpha1.Addons{
				KubeProxy:    &lodiav1alpha1.KubeProxy{Mode: "ipvs", HealthzBindAddress: "::", Conntrack: &lodiav1alpha1.KubeProxyConntrack{MaxPerCore: new(int32(0))}},
				CoreDNS:      &lodiav1alpha1.CoreDNS{Replicas: new(int32(3))},
				Konnectivity: &lodiav1alpha1.Konnectivity{Version: "v0.35.1", Agent: &lodiav1alpha1.KonnectivityAgent{Mode: "Deployment", Replicas: new(int32(2))}},
			},
			status:    lodiav1alpha1.EndpointStatus{APIServer: lodiav1alpha1.HostPort{Host: "api.dev.example.test", Port: 31443}, Konnectivity: lodiav1alpha1.HostPort{Host: "api.dev.example.test", Port: 31132}, InCluster: lodiav1alpha1.HostPort{Host: "2001:db8::10", Port: 31443}},
			endpoints: outsideGateway,
		},
		"gateway.yaml": {
			status: lodiav1alpha1.EndpointStatus{
				APIServer:    lodiav1alpha1.HostPort{Host: "api.dev.example.test", Port: 443},
				Konnectivity: lodiav1alpha1.HostPort{Host: "konnectivity.dev.example.test", Port: 443},
				InCluster:    lodiav1alpha1.HostPort{Host: "240.0.0.1", Port: 6443},
				Tunnel:       &lodiav1alpha1.HostPort{Host: "tunnel.dev.example.test", Port: 443},
			},
			endpoints: map[string]lodiav1alpha1.Endpoint{"gateway": {Gateway: &lodiav1alpha1.GatewayEndpoint{
				ParentRef: lodiav1alpha1.ParentReference{Name: "edge", Namespace: "gateways", SectionName: "tls"},
				Hostnames: lodiav1alpha1.GatewayHostnames{APIServer: "api.dev.example.test", Konnectivity: "konnectivity.dev.example.test", Tunnel: "tunnel.dev.example.test"},
			}}},
		},
	} {
		for mode, endpoint := range c.endpoints {
			cp := &lodiav1alpha1.ControlPlane{
				ObjectMeta: metav1.ObjectMeta{Name: "dev"},
				Spec:       lodiav1alpha1.ControlPlaneSpec{Endpoint: endpoint, Network: network, Addons: c.addons},
				Status:     lodiav1alpha1.ControlPlaneStatus{Target: &lodiav1alpha1.Target{Version: "v1.35.8"}, Endpoint: &c.status},
			}
			v, waiting, err := newValues(cp, []byte("the cluster CA"))
			if err != nil || waiting != "" {
				t.Fatalf("%s, %s: %v %s", golden, mode, err, waiting)
			}
			v.TunnelImage, v.tunnelCert, v.tunnelKey = tunnelImage, []byte(tunnelCert), []byte(tunnelKey)
			objects, err := render(v)
			if err != nil {
				t.Fatalf("%s, %s: %v", golden, mode, err)
			}
			var rendered bytes.Buffer
			for _, obj := range objects {
				doc, err := yaml.Marshal(obj.Object)
				if err != nil {
					t.Fatalf("%s, %s: %v", golden, mode, err)
				}
				rendered.WriteString("---\n")
				rendered.Write(doc)
			}
			path := filepath.Join("testdata", "render", golden)
			if *updateGolden {
				if err := os.WriteFile(path, rendered.Bytes(), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(rendered.Bytes(), want) {
				t.Errorf("%s renders other bytes than %s, which would roll every tenant in that mode:\n%s", mode, path, rendered.Bytes())
			}
		}
	}
}

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
		v, waiting, err := newValues(cp, nil)
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
		v, waiting, err := newValues(cp, nil)
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

const (
	tunnelImage = "registry.example/lodia-tunnel:test"
	tunnelCert  = "the tunnel client's certificate"
	tunnelKey   = "the tunnel client's key"
)

func gatewayControlPlane(inCluster, tunnel string) *lodiav1alpha1.ControlPlane {
	return &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev"},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Endpoint: lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
				ParentRef: lodiav1alpha1.ParentReference{Name: "edge", Namespace: "gateways", SectionName: "tls"},
				Hostnames: lodiav1alpha1.GatewayHostnames{APIServer: "api.dev.example.test", Konnectivity: "konnectivity.dev.example.test", Tunnel: "tunnel.dev.example.test"},
			}},
			Network: lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
		Status: lodiav1alpha1.ControlPlaneStatus{
			Target: &lodiav1alpha1.Target{Version: "v1.35.8", OperatorRevision: "v0.0.0-test+running"},
			Endpoint: &lodiav1alpha1.EndpointStatus{
				APIServer:    lodiav1alpha1.HostPort{Host: "api.dev.example.test", Port: 443},
				Konnectivity: lodiav1alpha1.HostPort{Host: "konnectivity.dev.example.test", Port: 443},
				InCluster:    lodiav1alpha1.HostPort{Host: inCluster, Port: 6443},
				Tunnel:       &lodiav1alpha1.HostPort{Host: tunnel, Port: 443},
			},
		},
	}
}

func rendered(t *testing.T, cp *lodiav1alpha1.ControlPlane) []*unstructured.Unstructured {
	t.Helper()
	v, waiting, err := newValues(cp, []byte("the cluster CA"))
	if err != nil || waiting != "" {
		t.Fatalf("%v %s", err, waiting)
	}
	v.TunnelImage, v.tunnelCert, v.tunnelKey = tunnelImage, []byte(tunnelCert), []byte(tunnelKey)
	objects, err := render(v)
	if err != nil {
		t.Fatal(err)
	}
	return objects
}

func renderedObject(t *testing.T, objects []*unstructured.Unstructured, e entry, into any) {
	t.Helper()
	for _, obj := range objects {
		if entryOf(obj) == e {
			if err := runtime.DefaultUnstructuredConverter.FromUnstructuredWithValidation(obj.Object, into, true); err != nil {
				t.Fatalf("%s: %v", e, err)
			}
			return
		}
	}
	t.Fatalf("the render holds no %s", e)
}

func apiserverProxyOf(t *testing.T, objects []*unstructured.Unstructured) *appsv1.DaemonSet {
	t.Helper()
	ds := &appsv1.DaemonSet{}
	renderedObject(t, objects, entry{Group: "apps", Version: "v1", Kind: "DaemonSet", Namespace: "kube-system", Name: "apiserver-proxy"}, ds)
	return ds
}

func TestApiserverProxyRendersAsSpecified(t *testing.T) {
	for _, c := range []struct{ inCluster, tunnel, listen, upstream string }{
		{"240.0.0.1", "tunnel.dev.example.test", "240.0.0.1:6443", "tunnel.dev.example.test:443"},
		{"fd12:3456:789a::1", "2001:db8::20", "[fd12:3456:789a::1]:6443", "[2001:db8::20]:443"},
	} {
		ds := apiserverProxyOf(t, rendered(t, gatewayControlPlane(c.inCluster, c.tunnel)))
		labels := map[string]string{"k8s-app": "apiserver-proxy"}
		if ds.Labels["k8s-app"] != "apiserver-proxy" || ds.Labels[lodiav1alpha1.ManagedByLabel] != lodiav1alpha1.ManagedBy {
			t.Errorf("%s: the DaemonSet is labelled %v", c.inCluster, ds.Labels)
		}
		probe := func(args ...string) *corev1.Probe {
			return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: append([]string{"/ko-app/lodia-tunnel"}, args...)}}}
		}
		startup := probe("addr", "--address="+c.inCluster, "--check")
		startup.PeriodSeconds, startup.FailureThreshold = 1, 30
		always := corev1.ContainerRestartPolicyAlways
		surge, unavailable := intstr.FromInt32(1), intstr.FromInt32(0)
		want := appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
				Type:          appsv1.RollingUpdateDaemonSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDaemonSet{MaxSurge: &surge, MaxUnavailable: &unavailable},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					HostNetwork:                   true,
					DNSPolicy:                     corev1.DNSDefault,
					PriorityClassName:             "system-node-critical",
					AutomountServiceAccountToken:  new(false),
					TerminationGracePeriodSeconds: new(int64(45)),
					Tolerations:                   []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
					NodeSelector:                  map[string]string{"kubernetes.io/os": "linux"},
					Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "type", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"virtual-kubelet"}}}}},
					}}},
					SecurityContext: &corev1.PodSecurityContext{FSGroup: new(int64(65532)), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
					InitContainers: []corev1.Container{{
						Name:          "addr",
						Image:         tunnelImage,
						RestartPolicy: &always,
						Args:          []string{"addr", "--address=" + c.inCluster},
						Resources:     corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("5m"), corev1.ResourceMemory: resource.MustParse("15Mi")}},
						SecurityContext: &corev1.SecurityContext{
							RunAsUser:                new(int64(0)),
							RunAsNonRoot:             new(false),
							AllowPrivilegeEscalation: new(false),
							ReadOnlyRootFilesystem:   new(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"NET_ADMIN"}},
						},
						StartupProbe: startup,
					}},
					Containers: []corev1.Container{{
						Name:      "proxy",
						Image:     tunnelImage,
						Args:      []string{"proxy", "--listen=" + c.listen, "--upstream=" + c.upstream, "--cert-dir=/etc/lodia/client", "--ca=/etc/lodia/ca", "--socket=/run/lodia/proxy.sock"},
						Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("5m"), corev1.ResourceMemory: resource.MustParse("30Mi")}},
						SecurityContext: &corev1.SecurityContext{
							RunAsUser:                new(int64(65532)),
							RunAsGroup:               new(int64(65532)),
							RunAsNonRoot:             new(true),
							AllowPrivilegeEscalation: new(false),
							ReadOnlyRootFilesystem:   new(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						ReadinessProbe: probe("probe", "--socket=/run/lodia/proxy.sock"),
						LivenessProbe:  probe("probe", "--socket=/run/lodia/proxy.sock"),
						VolumeMounts: []corev1.VolumeMount{
							{Name: "client", MountPath: "/etc/lodia/client", ReadOnly: true},
							{Name: "ca", MountPath: "/etc/lodia/ca", ReadOnly: true},
							{Name: "run", MountPath: "/run/lodia"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "client", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "apiserver-proxy-client", DefaultMode: new(int32(0o440))}}},
						{Name: "ca", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}}}},
						{Name: "run", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		}
		if !equality.Semantic.DeepEqual(ds.Spec, want) {
			got, _ := yaml.Marshal(ds.Spec)
			expected, _ := yaml.Marshal(want)
			t.Errorf("%s: the DaemonSet's spec is\n%s\nwant\n%s", c.inCluster, got, expected)
		}
	}
}

func TestOnlyTheProxysUIDReadsTheClientKey(t *testing.T) {
	spec := apiserverProxyOf(t, rendered(t, gatewayControlPlane("240.0.0.1", "tunnel.dev.example.test"))).Spec.Template.Spec
	if group := spec.SecurityContext.FSGroup; group == nil || *group != 65532 {
		t.Errorf("fsGroup is %v, want the proxy's group 65532, or UID 65532 cannot read the key", group)
	}
	if mode := spec.Volumes[0].Secret.DefaultMode; spec.Volumes[0].Name != "client" || mode == nil || *mode != 0o440 {
		t.Errorf("the client volume %s has the mode %v, want 0440, group-readable and nobody else's", spec.Volumes[0].Name, mode)
	}
	proxy := spec.Containers[0].SecurityContext
	if *proxy.RunAsUser != 65532 || *proxy.RunAsGroup != 65532 {
		t.Errorf("proxy runs as %d:%d, want 65532:65532", *proxy.RunAsUser, *proxy.RunAsGroup)
	}
	if mounts := spec.InitContainers[0].VolumeMounts; len(mounts) != 0 {
		t.Errorf("addr, which holds NET_ADMIN, mounts %v", mounts)
	}
}

func TestApiserverProxysSecretIsTheBuildersOfThePair(t *testing.T) {
	objects := rendered(t, gatewayControlPlane("240.0.0.1", "tunnel.dev.example.test"))
	last := objects[len(objects)-1]
	if !equality.Semantic.DeepEqual(last.Object, apiserverProxyClient([]byte(tunnelCert), []byte(tunnelKey)).Object) {
		t.Errorf("the render appends %v", last.Object)
	}
	secret := &corev1.Secret{}
	renderedObject(t, objects, entry{Version: "v1", Kind: "Secret", Namespace: "kube-system", Name: "apiserver-proxy-client"}, secret)
	if secret.Type != corev1.SecretTypeTLS || len(secret.Data) != 2 || string(secret.Data["tls.crt"]) != tunnelCert || string(secret.Data["tls.key"]) != tunnelKey {
		t.Errorf("the Secret is of type %s with %q", secret.Type, secret.Data)
	}
	if secret.Labels[lodiav1alpha1.ManagedByLabel] != lodiav1alpha1.ManagedBy {
		t.Errorf("the Secret is labelled %v", secret.Labels)
	}
}

func TestGatewayModeMovesTheAgentsToThePublicPath(t *testing.T) {
	for _, agent := range []struct {
		kind  string
		addon *lodiav1alpha1.Addons
	}{
		{"DaemonSet", nil},
		{"Deployment", &lodiav1alpha1.Addons{Konnectivity: &lodiav1alpha1.Konnectivity{Agent: &lodiav1alpha1.KonnectivityAgent{Mode: "Deployment"}}}},
	} {
		key := entry{Group: "apps", Version: "v1", Kind: agent.kind, Namespace: "kube-system", Name: "konnectivity-agent"}
		gateway := gatewayControlPlane("240.0.0.1", "tunnel.dev.example.test")
		gateway.Spec.Addons = agent.addon
		objects := rendered(t, gateway)
		other := gateway.DeepCopy()
		other.Spec.Endpoint = lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}}
		other.Status.Endpoint.Tunnel = nil

		var got, want corev1.PodTemplateSpec
		if agent.kind == "DaemonSet" {
			g, w := &appsv1.DaemonSet{}, &appsv1.DaemonSet{}
			renderedObject(t, objects, key, g)
			renderedObject(t, rendered(t, other), key, w)
			got, want = g.Spec.Template, w.Spec.Template
		} else {
			g, w := &appsv1.Deployment{}, &appsv1.Deployment{}
			renderedObject(t, objects, key, g)
			renderedObject(t, rendered(t, other), key, w)
			got, want = g.Spec.Template, w.Spec.Template
		}
		want.Spec.DNSPolicy = corev1.DNSDefault
		container := &want.Spec.Containers[0]
		at := slices.Index(container.Command, "--keepalive-time=30s") + 1
		container.Command = slices.Insert(container.Command, at, "--kubeconfig=/etc/konnectivity-agent/kubeconfig")
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "kubeconfig", MountPath: "/etc/konnectivity-agent"})
		want.Spec.Volumes = append(want.Spec.Volumes, corev1.Volume{Name: "kubeconfig", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "konnectivity-agent-kubeconfig"}}}})
		if !equality.Semantic.DeepEqual(got, want) {
			g, _ := yaml.Marshal(got)
			w, _ := yaml.Marshal(want)
			t.Errorf("%s: the agents' pod template is\n%s\nwant the other modes' with the public path\n%s", agent.kind, g, w)
		}

		cm := &corev1.ConfigMap{}
		renderedObject(t, objects, entry{Version: "v1", Kind: "ConfigMap", Namespace: "kube-system", Name: "konnectivity-agent-kubeconfig"}, cm)
		kubeconfig, err := clientcmd.Load([]byte(cm.Data["kubeconfig"]))
		if err != nil {
			t.Fatal(err)
		}
		cluster, user := kubeconfig.Clusters["default"], kubeconfig.AuthInfos["default"]
		if kubeconfig.CurrentContext != "default" || cluster == nil || user == nil ||
			cluster.Server != "https://api.dev.example.test:443" ||
			cluster.CertificateAuthority != "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt" ||
			user.TokenFile != "/var/run/secrets/kubernetes.io/serviceaccount/token" {
			t.Errorf("the agents' kubeconfig is %q", cm.Data["kubeconfig"])
		}
		if cm.Labels[lodiav1alpha1.ManagedByLabel] != lodiav1alpha1.ManagedBy {
			t.Errorf("the agents' kubeconfig is labelled %v", cm.Labels)
		}
	}
}

func TestAGatewayEndpointWithoutATunnelWaits(t *testing.T) {
	gateway := gatewayControlPlane("240.0.0.1", "tunnel.dev.example.test")
	gateway.Status.Endpoint.Tunnel = nil
	if reason, waiting := waitsFor(gateway); reason != lodiav1alpha1.ReasonWaitingForEndpoint || waiting == "" {
		t.Errorf("a gateway endpoint without a tunnel gives %q %q", reason, waiting)
	}
	other := gateway.DeepCopy()
	other.Spec.Endpoint = lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}
	if reason, waiting := waitsFor(other); waiting != "" {
		t.Errorf("a nodePort endpoint without a tunnel gives %q %q", reason, waiting)
	}
}

func TestTheTunnelIsAnInput(t *testing.T) {
	cp := gatewayControlPlane("240.0.0.1", "tunnel.dev.example.test")
	before := inputs(cp)
	for name, change := range map[string]func(*lodiav1alpha1.HostPort){
		"host": func(h *lodiav1alpha1.HostPort) { h.Host = "other.dev.example.test" },
		"port": func(h *lodiav1alpha1.HostPort) { h.Port = 8443 },
	} {
		changed := cp.DeepCopy()
		change(changed.Status.Endpoint.Tunnel)
		if inputs(changed) == before {
			t.Errorf("a pass does not run for a change of the tunnel's %s", name)
		}
	}
}

func TestTheOperatorRevisionIsAnInput(t *testing.T) {
	cp := gatewayControlPlane("240.0.0.1", "tunnel.dev.example.test")
	before := inputs(cp)
	for name, revision := range map[string]string{"granted": "v0.0.1-test+next", "gone": ""} {
		changed := cp.DeepCopy()
		changed.Status.Target.OperatorRevision = revision
		if inputs(changed) == before {
			t.Errorf("a pass does not run when the operator revision is %s", name)
		}
	}
}

func reporting(target, apiServer, controllerManager, scheduler string) *lodiav1alpha1.ControlPlane {
	component := func(version string) *lodiav1alpha1.ComponentStatus {
		return &lodiav1alpha1.ComponentStatus{Rendered: lodiav1alpha1.Rendered{Version: version}}
	}
	return &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev"},
		Spec:       lodiav1alpha1.ControlPlaneSpec{Network: lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}}},
		Status: lodiav1alpha1.ControlPlaneStatus{
			Target:     &lodiav1alpha1.Target{Version: target},
			Endpoint:   &lodiav1alpha1.EndpointStatus{APIServer: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 443}, Konnectivity: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 8132}, InCluster: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 6443}},
			Components: &lodiav1alpha1.ComponentsStatus{APIServer: component(apiServer), ControllerManager: component(controllerManager), Scheduler: component(scheduler)},
		},
	}
}

func TestTheAddonsRenderAtTheOldestOfTheComponents(t *testing.T) {
	for name, c := range map[string]struct {
		cp                             *lodiav1alpha1.ControlPlane
		version, coreDNS, konnectivity string
	}{
		"a patch apart":                         {reporting("v1.35.9", "v1.35.9", "v1.35.8", "v1.35.9"), "v1.35.8", "v1.13.1", "v0.35.0"},
		"a minor apart, the scheduler":          {reporting("v1.36.2", "v1.36.2", "v1.36.2", "v1.35.9"), "v1.35.9", "v1.13.1", "v0.35.0"},
		"a minor apart, the controller manager": {reporting("v1.36.2", "v1.36.2", "v1.35.8", "v1.36.1"), "v1.35.8", "v1.13.1", "v0.35.0"},
		"every component at the new minor":      {reporting("v1.36.2", "v1.36.2", "v1.36.2", "v1.36.2"), "v1.36.2", "v1.14.2", "v0.36.0"},
		"no report":                             {reporting("v1.36.2", "", "", ""), "v1.36.2", "v1.14.2", "v0.36.0"},
		"no components": {func() *lodiav1alpha1.ControlPlane {
			cp := reporting("v1.37.1", "", "", "")
			cp.Status.Components = nil
			return cp
		}(), "v1.37.1", "v1.14.6", "v0.37.0"},
		"an absent report counts as the target": {reporting("v1.36.2", "v1.36.2", "", "v1.35.8"), "v1.35.8", "v1.13.1", "v0.35.0"},
		"the target the oldest":                 {reporting("v1.35.8", "v1.35.9", "", "v1.35.9"), "v1.35.8", "v1.13.1", "v0.35.0"},
		"a newer version outside the table":     {reporting("v1.37.1", "v1.37.1", "v1.37.1", "v1.38.0"), "v1.37.1", "v1.14.6", "v0.37.0"},
	} {
		objects := rendered(t, c.cp)
		var kubeProxy, agent appsv1.DaemonSet
		var coreDNS appsv1.Deployment
		renderedObject(t, objects, entry{Group: "apps", Version: "v1", Kind: "DaemonSet", Namespace: "kube-system", Name: "kube-proxy"}, &kubeProxy)
		renderedObject(t, objects, entry{Group: "apps", Version: "v1", Kind: "DaemonSet", Namespace: "kube-system", Name: "konnectivity-agent"}, &agent)
		renderedObject(t, objects, entry{Group: "apps", Version: "v1", Kind: "Deployment", Namespace: "kube-system", Name: "coredns"}, &coreDNS)
		var kubeadm corev1.ConfigMap
		renderedObject(t, objects, entry{Version: "v1", Kind: "ConfigMap", Namespace: "kube-system", Name: "kubeadm-config"}, &kubeadm)
		var cluster struct {
			KubernetesVersion string `json:"kubernetesVersion"`
		}
		if err := yaml.Unmarshal([]byte(kubeadm.Data["ClusterConfiguration"]), &cluster); err != nil {
			t.Fatal(err)
		}
		for what, got := range map[string][2]string{
			"kube-proxy":         {kubeProxy.Spec.Template.Spec.Containers[0].Image, "registry.k8s.io/kube-proxy:" + c.version},
			"CoreDNS":            {coreDNS.Spec.Template.Spec.Containers[0].Image, "registry.k8s.io/coredns/coredns:" + c.coreDNS},
			"konnectivity-agent": {agent.Spec.Template.Spec.Containers[0].Image, "registry.k8s.io/kas-network-proxy/proxy-agent:" + c.konnectivity},
			"kubeadm-config":     {cluster.KubernetesVersion, c.version},
		} {
			if got[0] != got[1] {
				t.Errorf("%s: %s is %s, want %s", name, what, got[0], got[1])
			}
		}
		if v, _, err := newValues(c.cp, nil); err != nil || v.Version != c.version {
			t.Errorf("%s: the values carry the version %v (%v), want %s, which status.addons.version reports", name, v.Version, err, c.version)
		}
	}
}

func TestAVersionOutsideTheTableWaitsForTheTarget(t *testing.T) {
	for name, c := range map[string]struct {
		cp   *lodiav1alpha1.ControlPlane
		said string
	}{
		"the oldest component below the table": {reporting("v1.37.1", "v1.37.1", "v1.34.2", "v1.37.1"), `"v1.34.2"`},
		"every component above the table":      {reporting("v1.38.0", "v1.38.0", "v1.38.0", "v1.38.0"), `"v1.38.0"`},
		"the target the oldest, outside":       {reporting("v1.34.2", "v1.37.1", "", "v1.37.1"), `"v1.34.2"`},
		"a version that is none":               {reporting("v1.37.1", "v1.37.1", "latest", "v1.37.1"), `""`},
	} {
		_, message, err := newValues(c.cp, nil)
		if err != nil || !strings.Contains(message, c.said) || !strings.HasSuffix(message, "is not a version Lodia runs") {
			t.Errorf("%s: newValues gives %q %v, want a message naming %s", name, message, err, c.said)
		}
	}
}

func TestTheAddonsAreNeverNewerThanTheOldestComponent(t *testing.T) {
	table := []string{"v1.35.8", "v1.35.9", "v1.36.0", "v1.36.2", "v1.37.1"}
	reports := append([]string{""}, table...)
	checked := 0
	for _, target := range table {
		for _, apiServer := range reports {
			for _, controllerManager := range reports {
				for _, scheduler := range reports {
					cp := reporting(target, apiServer, controllerManager, scheduler)
					v, message, err := newValues(cp, nil)
					if err != nil || message != "" {
						t.Fatalf("target %s, reports %q %q %q: %q %v", target, apiServer, controllerManager, scheduler, message, err)
					}
					oldest := ""
					for _, report := range []string{apiServer, controllerManager, scheduler} {
						running := target
						if report != "" {
							running = report
						}
						if version.MustParseSemantic(v.Version).GreaterThan(version.MustParseSemantic(running)) {
							t.Errorf("target %s, reports %q %q %q: the addons are at %s, newer than a component at %s", target, apiServer, controllerManager, scheduler, v.Version, running)
						}
						if oldest == "" || version.MustParseSemantic(running).LessThan(version.MustParseSemantic(oldest)) {
							oldest = running
						}
					}
					if v.Version != oldest {
						t.Errorf("target %s, reports %q %q %q: the addons are at %s, want the oldest component's %s", target, apiServer, controllerManager, scheduler, v.Version, oldest)
					}
					images, _ := versions.For(v.Version)
					if v.KubeProxyImage != images.KubeProxy || v.CoreDNSImage != images.CoreDNS || v.AgentImage != images.KonnectivityAgent {
						t.Errorf("target %s, reports %q %q %q: the images %s %s %s are not those of %s", target, apiServer, controllerManager, scheduler, v.KubeProxyImage, v.CoreDNSImage, v.AgentImage, v.Version)
					}
					checked++
				}
			}
		}
	}
	if want := len(table) * len(reports) * len(reports) * len(reports); checked != want {
		t.Errorf("checked %d combinations, want %d", checked, want)
	}
}

func TestTheComponentVersionsAreInputsAndTheReplicaCountsAreNot(t *testing.T) {
	cp := reporting("v1.36.2", "v1.36.2", "v1.36.1", "v1.36.0")
	cp.Status.Components.APIServer.RolloutCount = lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: 2, Ready: 2}, Updated: new(int32(2))}
	runs := func(changed *lodiav1alpha1.ControlPlane) bool {
		return inputsChanged().Update(event.UpdateEvent{ObjectOld: cp, ObjectNew: changed})
	}
	for name, change := range map[string]func(*lodiav1alpha1.ComponentsStatus){
		"the API server's version":         func(c *lodiav1alpha1.ComponentsStatus) { c.APIServer.Version = "v1.36.1" },
		"the controller manager's version": func(c *lodiav1alpha1.ComponentsStatus) { c.ControllerManager.Version = "v1.36.2" },
		"the scheduler's version":          func(c *lodiav1alpha1.ComponentsStatus) { c.Scheduler.Version = "v1.36.1" },
		"a report that goes":               func(c *lodiav1alpha1.ComponentsStatus) { c.ControllerManager = nil },
		"a report from another component": func(c *lodiav1alpha1.ComponentsStatus) {
			c.ControllerManager.Version, c.Scheduler.Version = "v1.36.0", "v1.36.1"
		},
		"the components that are all gone": func(c *lodiav1alpha1.ComponentsStatus) { *c = lodiav1alpha1.ComponentsStatus{} },
	} {
		changed := cp.DeepCopy()
		change(changed.Status.Components)
		if !runs(changed) {
			t.Errorf("a pass does not run for a change of %s", name)
		}
	}
	for name, change := range map[string]func(*lodiav1alpha1.ComponentStatus){
		"total":         func(c *lodiav1alpha1.ComponentStatus) { c.Total = 3 },
		"ready":         func(c *lodiav1alpha1.ComponentStatus) { c.Ready = 1 },
		"updated":       func(c *lodiav1alpha1.ComponentStatus) { c.Updated = new(int32(1)) },
		"updated, gone": func(c *lodiav1alpha1.ComponentStatus) { c.Updated = nil },
	} {
		changed := cp.DeepCopy()
		change(changed.Status.Components.APIServer)
		if runs(changed) {
			t.Errorf("a pass runs for a change of the replica count %s", name)
		}
	}
}
