// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/versions"
)

var updateGolden = flag.Bool("update-golden", false, "write testdata/render from the render instead of comparing with it")

func TestTheRenderOutsideGatewayModeIsPinned(t *testing.T) {
	images, ok := versions.For("v1.37.1")
	if !ok {
		t.Fatal("no images for v1.37.1")
	}
	for mode, c := range map[string]struct {
		endpoint lodiav1alpha1.Endpoint
		status   lodiav1alpha1.EndpointStatus
	}{
		"loadBalancer": {
			endpoint: lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
			status:   lodiav1alpha1.EndpointStatus{APIServer: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 443}, Konnectivity: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 8132}, InCluster: lodiav1alpha1.HostPort{Host: "203.0.113.10", Port: 6443}},
		},
		"nodePort": {
			endpoint: lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}},
			status:   lodiav1alpha1.EndpointStatus{APIServer: lodiav1alpha1.HostPort{Host: "192.0.2.11", Port: 31443}, Konnectivity: lodiav1alpha1.HostPort{Host: "192.0.2.11", Port: 31132}, InCluster: lodiav1alpha1.HostPort{Host: "192.0.2.11", Port: 31443}},
		},
	} {
		cp := &lodiav1alpha1.ControlPlane{
			ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a", UID: "5e1f7c3a-0000-4000-8000-000000000001"},
			Spec: lodiav1alpha1.ControlPlaneSpec{
				Version:  "v1.37.1",
				Endpoint: c.endpoint,
				Network:  lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
			},
		}
		var objects []any
		objects = append(objects, service(cp))
		for _, p := range networkPolicies(cp) {
			objects = append(objects, p)
		}
		for _, d := range (rendering{
			cp:          cp,
			images:      images,
			signerImage: "registry.example/lodia-signer:test",
			tunnelImage: "registry.example/lodia-tunnel:test",
			endpoint:    &c.status,
			etcd:        []lodiav1alpha1.HostPort{{Host: "dev-etcd-client.team-a.svc", Port: 2379}},
			hashes:      map[string]string{apiServer: "api-hash", controllerManager: "cm-hash"},
		}).deployments() {
			objects = append(objects, d)
		}
		var rendered bytes.Buffer
		for _, obj := range objects {
			doc, err := yaml.Marshal(obj)
			if err != nil {
				t.Fatalf("%s: %v", mode, err)
			}
			rendered.WriteString("---\n")
			rendered.Write(doc)
		}
		path := filepath.Join("testdata", "render", mode+".yaml")
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
	r := rendering{cp: cp, endpoint: &lodiav1alpha1.EndpointStatus{}, etcd: []lodiav1alpha1.HostPort{{Host: "dev-etcd-client.team.svc", Port: 2379}}}
	for component, want := range map[string][]string{
		"kube-apiserver":          {"--endpoint-reconciler-type=none", "--shutdown-delay-duration=0s", "--enable-admission-plugins=NodeRestriction,PodSecurity", "--event-ttl=2h"},
		"kube-controller-manager": {"--leader-elect=true", "--node-monitor-grace-period=60s", "--controllers=*,bootstrap-signer-controller,token-cleaner-controller,-ttl-controller"},
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

func TestAvailabilityIsAReadyReplicaOfEachComponent(t *testing.T) {
	deployment := func(component string, replicas, ready int32, timedOut bool) *appsv1.Deployment {
		d := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "dev-" + component, Labels: map[string]string{lodiav1alpha1.ComponentLabel: component}, Generation: 2},
			Spec:       appsv1.DeploymentSpec{Replicas: new(replicas)},
			Status:     appsv1.DeploymentStatus{ObservedGeneration: 2, ReadyReplicas: ready},
		}
		if timedOut {
			d.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded"}}
		}
		return d
	}
	stale := deployment(apiServer, 2, 2, true)
	stale.Status.ObservedGeneration = 1
	const held = "kube-controller-manager is held at 0 replicas until the restore is released: spec.restoreFrom.release is Held"
	for _, row := range []struct {
		name        string
		deployments []*appsv1.Deployment
		held, want  string
	}{
		{"every replica ready", []*appsv1.Deployment{deployment(apiServer, 2, 2, false), deployment(controllerManager, 2, 2, false), deployment(scheduler, 2, 2, false)}, "",
			"True Ready every replica of kube-apiserver, kube-controller-manager and kube-scheduler is ready"},
		{"a held controller manager", []*appsv1.Deployment{deployment(apiServer, 2, 1, false), deployment(controllerManager, 0, 0, false), deployment(scheduler, 2, 2, false)}, held,
			"True Ready kube-apiserver has 1 of 2 replicas ready; " + held},
		{"a held controller manager beside an unavailable scheduler", []*appsv1.Deployment{deployment(apiServer, 2, 2, false), deployment(controllerManager, 0, 0, false), deployment(scheduler, 2, 0, false)}, held,
			"False Unavailable " + held + "; kube-scheduler has 0 of 2 replicas ready"},
		{"a scheduler at 0 beside a held controller manager", []*appsv1.Deployment{deployment(apiServer, 2, 2, false), deployment(controllerManager, 0, 0, false), deployment(scheduler, 0, 0, false)}, held,
			"False Unavailable " + held},
		{"a released controller manager", []*appsv1.Deployment{deployment(apiServer, 2, 2, false), deployment(controllerManager, 2, 2, false), deployment(scheduler, 2, 2, false)}, held,
			"True Ready every replica of kube-apiserver, kube-controller-manager and kube-scheduler is ready"},
		{"one API server of two ready", []*appsv1.Deployment{deployment(apiServer, 2, 1, false), deployment(controllerManager, 2, 2, false), deployment(scheduler, 2, 2, false)}, "",
			"True Ready kube-apiserver has 1 of 2 replicas ready"},
		{"a surge", []*appsv1.Deployment{deployment(apiServer, 2, 4, false), deployment(controllerManager, 1, 1, false), deployment(scheduler, 1, 2, false)}, "",
			"True Ready every replica of kube-apiserver, kube-controller-manager and kube-scheduler is ready"},
		{"a scheduler without a ready replica", []*appsv1.Deployment{deployment(apiServer, 2, 1, false), deployment(controllerManager, 2, 2, false), deployment(scheduler, 2, 0, false)}, "",
			"False Unavailable kube-apiserver has 1 of 2 replicas ready; kube-scheduler has 0 of 2 replicas ready"},
		{"a rollout past its deadline", []*appsv1.Deployment{deployment(apiServer, 2, 2, true), deployment(controllerManager, 2, 1, false), deployment(scheduler, 2, 2, false)}, "",
			"True ProgressDeadlineExceeded the rollout of Deployment dev-kube-apiserver passed its progress deadline; kube-controller-manager has 1 of 2 replicas ready"},
		{"a deadline of a generation not observed", []*appsv1.Deployment{stale, deployment(controllerManager, 2, 2, false), deployment(scheduler, 2, 2, false)}, "",
			"True Ready every replica of kube-apiserver, kube-controller-manager and kube-scheduler is ready"},
		{"a deadline and no ready replica", []*appsv1.Deployment{deployment(apiServer, 2, 0, true), deployment(controllerManager, 2, 2, false), deployment(scheduler, 2, 2, false)}, "",
			"False Unavailable kube-apiserver has 0 of 2 replicas ready"},
		{"a scheduler without a Deployment", []*appsv1.Deployment{deployment(apiServer, 2, 1, false), deployment(controllerManager, 2, 2, false)}, "",
			"False Unavailable kube-apiserver has 1 of 2 replicas ready; kube-scheduler has no Deployment"},
	} {
		c := availability(row.deployments, row.held, false)
		if got := fmt.Sprintf("%s %s %s", c.Status, c.Reason, c.Message); got != row.want {
			t.Errorf("%s: APIServerReady is %q, want %q", row.name, got, row.want)
		}
	}
	for _, row := range []struct {
		name        string
		deployments []*appsv1.Deployment
		held        bool
		want        string
	}{
		{name: "a controller manager with no ready replica under a hold", deployments: []*appsv1.Deployment{deployment(apiServer, 2, 2, false), deployment(controllerManager, 1, 0, false), deployment(scheduler, 2, 2, false)}, held: true,
			want: "True Ready kube-controller-manager has 0 of 1 replicas ready while Health holds its leader Lease"},
		{name: "the same without the hold", deployments: []*appsv1.Deployment{deployment(apiServer, 2, 2, false), deployment(controllerManager, 1, 0, false), deployment(scheduler, 2, 2, false)},
			want: "False Unavailable kube-controller-manager has 0 of 1 replicas ready"},
		{name: "a controller manager with a ready replica under a hold is counted as usual", deployments: []*appsv1.Deployment{deployment(apiServer, 2, 2, false), deployment(controllerManager, 2, 1, false), deployment(scheduler, 2, 2, false)}, held: true,
			want: "True Ready kube-controller-manager has 1 of 2 replicas ready"},
		{name: "an API server with no ready replica under a hold", deployments: []*appsv1.Deployment{deployment(apiServer, 2, 0, false), deployment(controllerManager, 1, 0, false), deployment(scheduler, 2, 2, false)}, held: true,
			want: "False Unavailable kube-apiserver has 0 of 2 replicas ready; kube-controller-manager has 0 of 1 replicas ready while Health holds its leader Lease"},
		{name: "a scheduler with no ready replica under a hold", deployments: []*appsv1.Deployment{deployment(apiServer, 2, 2, false), deployment(controllerManager, 1, 0, false), deployment(scheduler, 2, 0, false)}, held: true,
			want: "False Unavailable kube-controller-manager has 0 of 1 replicas ready while Health holds its leader Lease; kube-scheduler has 0 of 2 replicas ready"},
	} {
		c := availability(row.deployments, "", row.held)
		if got := fmt.Sprintf("%s %s %s", c.Status, c.Reason, c.Message); got != row.want {
			t.Errorf("%s: APIServerReady is %q, want %q", row.name, got, row.want)
		}
	}
}

func TestUpdatedCountsAvailableReplicasOfTheCurrentTemplate(t *testing.T) {
	for _, row := range []struct {
		name                                string
		observed                            bool
		replicas, updated, ready, available int32
		want                                string
	}{
		{"a template not observed", false, 2, 2, 2, 2, "2/2/absent"},
		{"a surge starts", true, 4, 2, 2, 2, "2/2/0"},
		{"one new replica available", true, 3, 2, 3, 2, "2/2/1"},
		{"both new available, the old ones going", true, 4, 2, 4, 4, "2/2/2"},
		{"rolled out", true, 2, 2, 2, 2, "2/2/2"},
		{"new replicas that never start", true, 4, 2, 2, 2, "2/2/0"},
		{"a replica lost", true, 2, 2, 1, 1, "2/1/1"},
	} {
		d := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Generation: 3},
			Spec:       appsv1.DeploymentSpec{Replicas: new(int32(2))},
			Status:     appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: row.replicas, UpdatedReplicas: row.updated, ReadyReplicas: row.ready, AvailableReplicas: row.available},
		}
		if row.observed {
			d.Status.ObservedGeneration = 3
		}
		count := rolloutCount(d)
		updated := "absent"
		if count.Updated != nil {
			updated = fmt.Sprint(*count.Updated)
		}
		if got := fmt.Sprintf("%d/%d/%s", count.Total, count.Ready, updated); got != row.want {
			t.Errorf("%s: total/ready/updated %s, want %s", row.name, got, row.want)
		}
	}
}
