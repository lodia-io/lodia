// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const digest = "@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func deploymentOf(component, image string, replicas int32, status appsv1.DeploymentStatus) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 3},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{{Name: "konnectivity-server", Image: "registry.k8s.io/kas-network-proxy/proxy-server:v0.37.0"}},
				Containers:     []corev1.Container{{Name: "sidecar", Image: "registry.example/sidecar:v9.9.9"}, {Name: component, Image: image}},
			}},
		},
		Status: status,
	}
}

func TestTheVersionIsTheTagOfTheComponentsOwnImage(t *testing.T) {
	const image = "registry.k8s.io/kube-apiserver"
	for _, c := range []struct{ image, want string }{
		{image + ":v1.37.1", "v1.37.1"},
		{image + ":v1.37.1" + digest, "v1.37.1"},
		{"kube-apiserver:v1.37.1", "v1.37.1"},
		{"registry.example:5000/kube-apiserver:v1.37.1", "v1.37.1"},
		{"registry.example:5000/kube-apiserver:v10.100.1000", "v10.100.1000"},
		{image + ":v1.37.1234567890", "v1.37.1234567890"},
		{image + ":v1.37.12345678901", ""},
		{image, ""},
		{image + digest, ""},
		{image + ":latest", ""},
		{image + ":1.37.1", ""},
		{image + ":v1.37", ""},
		{image + ":v1.37.1-rc.1", ""},
		{image + ":v1.37.1_build", ""},
		{image + ":v1.37.x", ""},
		{"registry.k8s.io/Kube-APIServer:v1.37.1", ""},
		{"", ""},
	} {
		d := deploymentOf(apiServer, c.image, 1, appsv1.DeploymentStatus{})
		if got := imageVersion(d, apiServer); got != c.want {
			t.Errorf("the version of %q is %q, want %q", c.image, got, c.want)
		}
	}
}

func TestTheVersionIsOfTheContainerNamedForTheComponent(t *testing.T) {
	d := deploymentOf(apiServer, "registry.k8s.io/kube-apiserver:v1.37.1", 1, appsv1.DeploymentStatus{})
	if got := imageVersion(d, apiServer); got != "v1.37.1" {
		t.Errorf("the version is %q beside a sidecar at v9.9.9 and an init container at v0.37.0, want v1.37.1", got)
	}
	if got := imageVersion(d, scheduler); got != "" {
		t.Errorf("the version of a container the Deployment does not have is %q", got)
	}
}

func TestAComponentReportsTheVersionOnlyOnceEveryReplicaRunsIt(t *testing.T) {
	const template = "registry.k8s.io/kube-controller-manager:v1.37.2"
	previous := &lodiav1alpha1.ComponentStatus{
		RolloutCount: lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: 9, Ready: 9}},
		Rendered:     lodiav1alpha1.Rendered{Version: "v1.37.1", OperatorRevision: "kept"},
	}
	for _, c := range []struct {
		name     string
		image    string
		replicas int32
		status   appsv1.DeploymentStatus
		previous *lodiav1alpha1.ComponentStatus
		want     lodiav1alpha1.ComponentStatus
	}{
		{
			"every replica updated and available", template, 2,
			appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2}, previous,
			lodiav1alpha1.ComponentStatus{RolloutCount: counts(2, 2, new(int32(2))), Rendered: lodiav1alpha1.Rendered{Version: "v1.37.2", OperatorRevision: "kept"}},
		},
		{
			"one of two replicas updated", template, 2,
			appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 2, UpdatedReplicas: 1, ReadyReplicas: 2, AvailableReplicas: 2}, previous,
			lodiav1alpha1.ComponentStatus{RolloutCount: counts(2, 2, new(int32(1))), Rendered: previous.Rendered},
		},
		{
			"every replica updated and one not available", template, 2,
			appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 1, AvailableReplicas: 1}, previous,
			lodiav1alpha1.ComponentStatus{RolloutCount: counts(2, 1, new(int32(1))), Rendered: previous.Rendered},
		},
		{
			"a surge that keeps the old replicas available", template, 2,
			appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 4, UpdatedReplicas: 2, ReadyReplicas: 4, AvailableReplicas: 2}, previous,
			lodiav1alpha1.ComponentStatus{RolloutCount: counts(2, 2, new(int32(0))), Rendered: previous.Rendered},
		},
		{
			"a generation the Deployment controller has not observed", template, 2,
			appsv1.DeploymentStatus{ObservedGeneration: 2, Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2}, previous,
			lodiav1alpha1.ComponentStatus{RolloutCount: counts(2, 2, nil), Rendered: previous.Rendered},
		},
		{
			"0 of 0 replicas", template, 0,
			appsv1.DeploymentStatus{ObservedGeneration: 3}, previous,
			lodiav1alpha1.ComponentStatus{RolloutCount: counts(0, 0, new(int32(0))), Rendered: lodiav1alpha1.Rendered{Version: "v1.37.2", OperatorRevision: "kept"}},
		},
		{
			"0 of 0 replicas, the generation not observed", template, 0,
			appsv1.DeploymentStatus{ObservedGeneration: 2}, previous,
			lodiav1alpha1.ComponentStatus{RolloutCount: counts(0, 0, nil), Rendered: previous.Rendered},
		},
		{
			"a tag that is not a version", "registry.k8s.io/kube-controller-manager:latest", 1,
			appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}, previous,
			lodiav1alpha1.ComponentStatus{RolloutCount: counts(1, 1, new(int32(1))), Rendered: previous.Rendered},
		},
		{
			"a tag that is not a version, and no previous report", "registry.k8s.io/kube-controller-manager:latest", 1,
			appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}, nil,
			lodiav1alpha1.ComponentStatus{RolloutCount: counts(1, 1, new(int32(1)))},
		},
		{
			"no previous report", template, 1,
			appsv1.DeploymentStatus{ObservedGeneration: 3, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}, nil,
			lodiav1alpha1.ComponentStatus{RolloutCount: counts(1, 1, new(int32(1))), Rendered: lodiav1alpha1.Rendered{Version: "v1.37.2"}},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := rolledOut(deploymentOf(controllerManager, c.image, c.replicas, c.status), controllerManager, c.previous)
			if got == nil || !equality.Semantic.DeepEqual(*got, c.want) {
				t.Errorf("the report is %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestAComponentWhoseDeploymentIsGoneKeepsItsReportWithNoReplicas(t *testing.T) {
	for _, c := range []struct {
		name     string
		previous *lodiav1alpha1.ComponentStatus
		want     *lodiav1alpha1.ComponentStatus
	}{
		{"a version", &lodiav1alpha1.ComponentStatus{RolloutCount: counts(2, 2, new(int32(2))), Rendered: lodiav1alpha1.Rendered{Version: "v1.37.1"}},
			&lodiav1alpha1.ComponentStatus{Rendered: lodiav1alpha1.Rendered{Version: "v1.37.1"}}},
		{"a revision", &lodiav1alpha1.ComponentStatus{Rendered: lodiav1alpha1.Rendered{OperatorRevision: "v0.1.0+abc"}},
			&lodiav1alpha1.ComponentStatus{Rendered: lodiav1alpha1.Rendered{OperatorRevision: "v0.1.0+abc"}}},
		{"counts alone", &lodiav1alpha1.ComponentStatus{RolloutCount: counts(2, 2, new(int32(2)))}, nil},
		{"none", nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := rolledOut(nil, apiServer, c.previous)
			if !equality.Semantic.DeepEqual(got, c.want) {
				t.Errorf("the report is %+v, want %+v", got, c.want)
			}
		})
	}
}

func counts(total, ready int32, updated *int32) lodiav1alpha1.RolloutCount {
	return lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: total, Ready: ready}, Updated: updated}
}
