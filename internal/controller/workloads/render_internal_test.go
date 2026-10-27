// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"fmt"
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
	for _, row := range []struct {
		name        string
		deployments []*appsv1.Deployment
		want        string
	}{
		{"every replica ready", []*appsv1.Deployment{deployment(apiServer, 2, 2, false), deployment(controllerManager, 2, 2, false), deployment(scheduler, 2, 2, false)},
			"True Ready every replica of kube-apiserver, kube-controller-manager and kube-scheduler is ready"},
		{"one API server of two ready", []*appsv1.Deployment{deployment(apiServer, 2, 1, false), deployment(controllerManager, 2, 2, false), deployment(scheduler, 2, 2, false)},
			"True Ready kube-apiserver has 1 of 2 replicas ready"},
		{"a surge", []*appsv1.Deployment{deployment(apiServer, 2, 4, false), deployment(controllerManager, 1, 1, false), deployment(scheduler, 1, 2, false)},
			"True Ready every replica of kube-apiserver, kube-controller-manager and kube-scheduler is ready"},
		{"a scheduler without a ready replica", []*appsv1.Deployment{deployment(apiServer, 2, 1, false), deployment(controllerManager, 2, 2, false), deployment(scheduler, 2, 0, false)},
			"False Unavailable kube-apiserver has 1 of 2 replicas ready; kube-scheduler has 0 of 2 replicas ready"},
		{"a rollout past its deadline", []*appsv1.Deployment{deployment(apiServer, 2, 2, true), deployment(controllerManager, 2, 1, false), deployment(scheduler, 2, 2, false)},
			"True ProgressDeadlineExceeded the rollout of Deployment dev-kube-apiserver passed its progress deadline; kube-controller-manager has 1 of 2 replicas ready"},
		{"a deadline of a generation not observed", []*appsv1.Deployment{stale, deployment(controllerManager, 2, 2, false), deployment(scheduler, 2, 2, false)},
			"True Ready every replica of kube-apiserver, kube-controller-manager and kube-scheduler is ready"},
		{"a deadline and no ready replica", []*appsv1.Deployment{deployment(apiServer, 2, 0, true), deployment(controllerManager, 2, 2, false), deployment(scheduler, 2, 2, false)},
			"False Unavailable kube-apiserver has 0 of 2 replicas ready"},
	} {
		c := availability(row.deployments)
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
