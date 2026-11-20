// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"bytes"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/versions"
)

func copyRendering(t *testing.T, cp *lodiav1alpha1.ControlPlane, endpoint lodiav1alpha1.EndpointStatus) rendering {
	t.Helper()
	images, ok := versions.For("v1.37.1")
	if !ok {
		t.Fatal("no images for v1.37.1")
	}
	return rendering{
		cp:          cp,
		images:      images,
		signerImage: "registry.example/lodia-signer:test",
		tunnelImage: "registry.example/lodia-tunnel:test",
		revision:    "v0.0.0-test+golden",
		endpoint:    &endpoint,
		etcd:        []lodiav1alpha1.HostPort{{Host: "dev-etcd-client.team-a.svc", Port: 2379}},
		hashes:      map[string]string{apiServer: "api-hash", controllerManager: "cm-hash"},
	}
}

func TestTheCopyIsPinned(t *testing.T) {
	nodePort := lodiav1alpha1.EndpointStatus{APIServer: lodiav1alpha1.HostPort{Host: "192.0.2.11", Port: 31443}, Konnectivity: lodiav1alpha1.HostPort{Host: "192.0.2.11", Port: 31132}, InCluster: lodiav1alpha1.HostPort{Host: "192.0.2.11", Port: 31443}}
	for mode, c := range map[string]struct {
		spec     lodiav1alpha1.ControlPlaneSpec
		endpoint lodiav1alpha1.EndpointStatus
	}{
		"nodePort": {spec: lodiav1alpha1.ControlPlaneSpec{Endpoint: lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}}, endpoint: nodePort},
		"highAvailability": {spec: lodiav1alpha1.ControlPlaneSpec{
			Endpoint:         lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}},
			HighAvailability: &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"},
		}, endpoint: nodePort},
		"gateway": {spec: lodiav1alpha1.ControlPlaneSpec{Endpoint: lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
			ParentRef: lodiav1alpha1.ParentReference{Name: "edge", Namespace: "gateways", SectionName: "tls"},
			Hostnames: lodiav1alpha1.GatewayHostnames{APIServer: "api.dev.example.test", Konnectivity: "konnectivity.dev.example.test", Tunnel: "tunnel.dev.example.test"},
		}}}, endpoint: lodiav1alpha1.EndpointStatus{
			APIServer:    lodiav1alpha1.HostPort{Host: "api.dev.example.test", Port: 443},
			Konnectivity: lodiav1alpha1.HostPort{Host: "konnectivity.dev.example.test", Port: 443},
			InCluster:    lodiav1alpha1.HostPort{Host: "240.0.0.1", Port: 6443},
			Tunnel:       &lodiav1alpha1.HostPort{Host: "tunnel.dev.example.test", Port: 443},
		}},
	} {
		spec := c.spec
		spec.Version = "v1.37.1"
		spec.Network = lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}}
		cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a", UID: "5e1f7c3a-0000-4000-8000-000000000001"}, Spec: spec}
		var rendered bytes.Buffer
		for _, obj := range []any{copyRendering(t, cp, c.endpoint).rehearsalCopy("v1.37.1", 3, resource.MustParse("4Gi"), nil), copyPolicy(cp, "lodia-system")} {
			doc, err := yaml.Marshal(obj)
			if err != nil {
				t.Fatalf("%s: %v", mode, err)
			}
			rendered.WriteString("---\n")
			rendered.Write(doc)
		}
		path := filepath.Join("testdata", "render", "rehearsal-"+mode+".yaml")
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
			t.Errorf("the copy of a %s tenant renders other bytes than %s:\n%s", mode, path, rendered.Bytes())
		}
	}
}

func flagsOf(command []string) map[string]string {
	flags := map[string]string{}
	for _, arg := range command[1:] {
		name, value, _ := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		flags[name] = value
	}
	return flags
}

func TestTheCopyChangesOnlyWhatTheDecisionLists(t *testing.T) {
	resources := &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}}
	etcd := &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")}}
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a", UID: "5e1f7c3a-0000-4000-8000-000000000001"},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:  "v1.37.1",
			Endpoint: lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}},
			Network:  lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
			APIServer: &lodiav1alpha1.APIServer{
				ExtraArgs: map[string]string{
					"enable-admission-plugins":  "AlwaysPullImages,ValidatingAdmissionWebhook",
					"disable-admission-plugins": "DefaultStorageClass,MutatingAdmissionWebhook",
					"etcd-compaction-interval":  "10m",
					"audit-log-maxage":          "3",
				},
				FeatureGates: map[string]bool{"APIServerTracing": true},
				Resources:    resources,
			},
			Placement: &lodiav1alpha1.Placement{
				NodeSelector:      map[string]string{"pool": "control-planes"},
				Tolerations:       []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}},
				PriorityClassName: "tenant-control-planes",
			},
		},
	}
	r := copyRendering(t, cp, lodiav1alpha1.EndpointStatus{APIServer: lodiav1alpha1.HostPort{Host: "192.0.2.11", Port: 31443}, InCluster: lodiav1alpha1.HostPort{Host: "192.0.2.11", Port: 31443}})
	render := r.apiServer().Spec.Template.Spec
	pod := r.rehearsalCopy("v1.37.1", 7, resource.MustParse("8Gi"), etcd)

	want := flagsOf(render.Containers[0].Command)
	for _, name := range []string{"etcd-cafile", "etcd-certfile", "etcd-keyfile"} {
		delete(want, name)
	}
	maps.Copy(want, map[string]string{
		"etcd-servers":              "http://127.0.0.1:2379",
		"etcd-compaction-interval":  "0",
		"enable-admission-plugins":  "NodeRestriction,AlwaysPullImages",
		"disable-admission-plugins": "DefaultStorageClass,MutatingAdmissionWebhook,ValidatingAdmissionWebhook",
	})
	api := pod.Spec.Containers[0]
	if got := flagsOf(api.Command); !maps.Equal(got, want) {
		t.Errorf("the copy's flags are %v, want %v", got, want)
	}
	if api.Image != "registry.k8s.io/kube-apiserver:v1.37.1" || !equality.Semantic.DeepEqual(api.Resources, *resources) {
		t.Errorf("the copy's API server runs %s with %+v", api.Image, api.Resources)
	}
	if api.LivenessProbe != nil || api.StartupProbe != nil || api.Lifecycle != nil || api.ReadinessProbe.PeriodSeconds != 1 || api.ReadinessProbe.HTTPGet.Path != "/readyz" {
		t.Errorf("the copy's API server has the probes %+v, %+v, %+v and the lifecycle %+v", api.ReadinessProbe, api.LivenessProbe, api.StartupProbe, api.Lifecycle)
	}
	if api.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
		t.Errorf("the copy's API server's termination message policy is %s", api.TerminationMessagePolicy)
	}
	var mounted []string
	for _, m := range api.VolumeMounts {
		mounted = append(mounted, m.Name)
	}
	if slices.Contains(mounted, "etcd-ca") || slices.Contains(mounted, "etcd-client") || !slices.Contains(mounted, "encryption") {
		t.Errorf("the copy's API server mounts %v", mounted)
	}
	var initContainers []string
	for _, c := range pod.Spec.InitContainers {
		initContainers = append(initContainers, c.Name)
	}
	if !slices.Equal(initContainers, []string{"snapshot", "restore", "etcd", "lodia-signer"}) {
		t.Errorf("the copy's init containers are %v", initContainers)
	}
	if e := pod.Spec.InitContainers[2]; !equality.Semantic.DeepEqual(e.Resources, *etcd) || !slices.Contains(e.Command, "--quota-backend-bytes=8589934592") {
		t.Errorf("the copy's etcd runs %v with %+v", e.Command, e.Resources)
	}
	var volumes []string
	for _, v := range pod.Spec.Volumes {
		volumes = append(volumes, v.Name)
		if v.Name == "rehearsal" && v.EmptyDir.SizeLimit.Cmp(resource.MustParse("16Gi")) != 0 {
			t.Errorf("the copy's emptyDir is limited to %s, want twice the quota", v.EmptyDir.SizeLimit)
		}
	}
	if slices.Contains(volumes, "konnectivity-server") || slices.Contains(volumes, "konnectivity-kubeconfig") || !slices.Contains(volumes, "etcd-client") || !slices.Contains(volumes, "rehearsal") {
		t.Errorf("the copy's volumes are %v", volumes)
	}
	s := pod.Spec
	if !maps.Equal(s.NodeSelector, render.NodeSelector) || !equality.Semantic.DeepEqual(s.Tolerations, render.Tolerations) || s.PriorityClassName != render.PriorityClassName {
		t.Errorf("the copy's placement is %v, %v, %s, want the render's", s.NodeSelector, s.Tolerations, s.PriorityClassName)
	}
	if s.RestartPolicy != corev1.RestartPolicyOnFailure || *s.ActiveDeadlineSeconds != 900 || *s.TerminationGracePeriodSeconds != 5 || *s.AutomountServiceAccountToken {
		t.Errorf("the copy restarts %s, ends after %d s, has a grace of %d s and mounts a token: %t", s.RestartPolicy, *s.ActiveDeadlineSeconds, *s.TerminationGracePeriodSeconds, *s.AutomountServiceAccountToken)
	}
	if !maps.Equal(pod.Labels, map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev", lodiav1alpha1.ComponentLabel: lodiav1alpha1.RehearsalComponent}) || !metav1.IsControlledBy(pod, cp) {
		t.Errorf("the copy is labelled %v and owned by %+v", pod.Labels, pod.OwnerReferences)
	}
}

func TestTheCopyReadsTheAdmissionPluginsAsKubeAPIServerDoes(t *testing.T) {
	for _, extra := range []map[string]string{
		{"enable-admission-plugins": `AlwaysPullImages,"ValidatingAdmissionWebhook"`},
		{"enable-admission-plugins": `AlwaysPullImages,"MutatingAdmissionWebhook"`, "disable-admission-plugins": `DefaultStorageClass`},
	} {
		cp := &lodiav1alpha1.ControlPlane{
			ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a", UID: "5e1f7c3a-0000-4000-8000-000000000001"},
			Spec: lodiav1alpha1.ControlPlaneSpec{
				Version:   "v1.37.1",
				Endpoint:  lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}},
				Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
				APIServer: &lodiav1alpha1.APIServer{ExtraArgs: extra},
			},
		}
		r := copyRendering(t, cp, lodiav1alpha1.EndpointStatus{InCluster: lodiav1alpha1.HostPort{Host: "192.0.2.11", Port: 31443}})
		var enabled, disabled []string
		flags := pflag.NewFlagSet("kube-apiserver", pflag.ContinueOnError)
		flags.StringSliceVar(&enabled, "enable-admission-plugins", nil, "")
		flags.StringSliceVar(&disabled, "disable-admission-plugins", nil, "")
		flags.ParseErrorsAllowlist.UnknownFlags = true
		if err := flags.Parse(r.rehearsalCopy("v1.37.1", 1, resource.MustParse("4Gi"), nil).Spec.Containers[0].Command[1:]); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(enabled, []string{"NodeRestriction", "AlwaysPullImages"}) || !slices.Contains(disabled, "MutatingAdmissionWebhook") || !slices.Contains(disabled, "ValidatingAdmissionWebhook") ||
			slices.ContainsFunc(enabled, func(p string) bool { return slices.Contains(disabled, p) }) || slices.ContainsFunc(disabled, func(p string) bool { return strings.Contains(p, `"`) }) {
			t.Errorf("with %v the copy enables %q and disables %q", extra, enabled, disabled)
		}
	}
}

func TestTheCopysPolicyAdmitsTheOperatorAlone(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a"}}
	p := copyPolicy(cp, "lodia-system")
	if !maps.Equal(p.Spec.PodSelector.MatchLabels, map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev", lodiav1alpha1.ComponentLabel: lodiav1alpha1.RehearsalComponent}) {
		t.Errorf("the copy's NetworkPolicy selects %v", p.Spec.PodSelector.MatchLabels)
	}
	if len(p.Spec.Ingress) != 1 || len(p.Spec.Ingress[0].From) != 1 || len(p.Spec.Ingress[0].Ports) != 1 {
		t.Fatalf("the copy's NetworkPolicy admits %+v", p.Spec.Ingress)
	}
	rule := p.Spec.Ingress[0]
	if from := rule.From[0]; from.PodSelector != nil || from.IPBlock != nil || !maps.Equal(from.NamespaceSelector.MatchLabels, map[string]string{corev1.LabelMetadataName: "lodia-system"}) {
		t.Errorf("the copy's NetworkPolicy admits %+v", from)
	}
	if port := rule.Ports[0]; port.Port.IntValue() != 6443 || *port.Protocol != corev1.ProtocolTCP || port.EndPort != nil {
		t.Errorf("the copy's NetworkPolicy admits the port %+v", port)
	}
	if !slices.Equal(p.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}) {
		t.Errorf("the copy's NetworkPolicy has the types %v", p.Spec.PolicyTypes)
	}
}
