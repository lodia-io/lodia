// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/version"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func controlPlaneAt(target, apiServer string) *lodiav1alpha1.ControlPlane {
	cp := &lodiav1alpha1.ControlPlane{}
	if target != "" {
		cp.Status.Target = &lodiav1alpha1.Target{Version: target}
	}
	if apiServer != "" {
		cp.Status.Components = &lodiav1alpha1.ComponentsStatus{APIServer: &lodiav1alpha1.ComponentStatus{Rendered: lodiav1alpha1.Rendered{Version: apiServer}}}
	}
	return cp
}

func tagOf(image string) string {
	return image[strings.LastIndex(image, ":")+1:]
}

func TestTheImagesFollowTheTargetAndTheAPIServersReport(t *testing.T) {
	for name, c := range map[string]struct {
		target, apiServer string
		kubeAPIServer     string
		followers         string
		message, unknown  string
	}{
		"no report":                    {target: "v1.37.2", kubeAPIServer: "v1.37.2", followers: "v1.37.2"},
		"a report a patch behind":      {target: "v1.37.2", apiServer: "v1.37.1", kubeAPIServer: "v1.37.2", followers: "v1.37.1"},
		"a report a minor behind":      {target: "v1.37.1", apiServer: "v1.36.4", kubeAPIServer: "v1.37.1", followers: "v1.36.4"},
		"a report at the target":       {target: "v1.37.2", apiServer: "v1.37.2", kubeAPIServer: "v1.37.2", followers: "v1.37.2"},
		"a report ahead of the target": {target: "v1.37.1", apiServer: "v1.37.2", kubeAPIServer: "v1.37.1", followers: "v1.37.2"},
		"a report outside the table":   {target: "v1.37.1", apiServer: "v1.33.2", kubeAPIServer: "v1.37.1", unknown: "status.components.apiServer.version v1.33.2 is not a version Lodia runs"},
		"a report that is no version":  {target: "v1.37.1", apiServer: "latest", kubeAPIServer: "v1.37.1", unknown: "status.components.apiServer.version latest is not a version Lodia runs"},
		"no target":                    {apiServer: "v1.37.1", message: "waiting for Lifecycle to set status.target.version"},
		"a target outside the table":   {target: "v1.34.1", apiServer: "v1.37.1", message: "status.target.version v1.34.1 is not a version Lodia runs"},
	} {
		images, message, unknown := imagesOf(controlPlaneAt(c.target, c.apiServer))
		if message != c.message || unknown != c.unknown {
			t.Errorf("%s: the messages are %q and %q, want %q and %q", name, message, unknown, c.message, c.unknown)
		}
		if c.message != "" {
			continue
		}
		if c.unknown != "" {
			if got := tagOf(images.APIServer); got != c.kubeAPIServer {
				t.Errorf("%s: kube-apiserver is at %s, want %s", name, got, c.kubeAPIServer)
			}
			continue
		}
		for component, got := range map[string]string{
			"kube-apiserver":          tagOf(images.APIServer),
			"kube-controller-manager": tagOf(images.ControllerManager),
			"kube-scheduler":          tagOf(images.Scheduler),
		} {
			want := c.followers
			if component == "kube-apiserver" {
				want = c.kubeAPIServer
			}
			if got != want {
				t.Errorf("%s: %s is at %s, want %s", name, component, got, want)
			}
		}
	}
}

func TestTheControllerManagerAndSchedulerAreNeverNewerThanTheAPIServersReport(t *testing.T) {
	table := []string{"v1.35.8", "v1.35.9", "v1.36.0", "v1.36.2", "v1.37.1"}
	reports := append([]string{""}, table...)
	checked := 0
	for _, target := range table {
		for _, apiServer := range reports {
			for _, controllerManager := range reports {
				for _, scheduler := range reports {
					cp := &lodiav1alpha1.ControlPlane{Status: lodiav1alpha1.ControlPlaneStatus{
						Target: &lodiav1alpha1.Target{Version: target},
						Components: &lodiav1alpha1.ComponentsStatus{
							APIServer:         &lodiav1alpha1.ComponentStatus{Rendered: lodiav1alpha1.Rendered{Version: apiServer}},
							ControllerManager: &lodiav1alpha1.ComponentStatus{Rendered: lodiav1alpha1.Rendered{Version: controllerManager}},
							Scheduler:         &lodiav1alpha1.ComponentStatus{Rendered: lodiav1alpha1.Rendered{Version: scheduler}},
						},
					}}
					images, message, unknown := imagesOf(cp)
					if message != "" || unknown != "" {
						t.Fatalf("target %s, reports %q %q %q: %s%s", target, apiServer, controllerManager, scheduler, message, unknown)
					}
					running := version.MustParseSemantic(target)
					if apiServer != "" {
						running = version.MustParseSemantic(apiServer)
					}
					for component, image := range map[string]string{"controller manager": images.ControllerManager, "scheduler": images.Scheduler} {
						if version.MustParseSemantic(tagOf(image)).GreaterThan(running) {
							t.Errorf("target %s, reports %q %q %q: the %s is at %s, newer than the API server's %s", target, apiServer, controllerManager, scheduler, component, tagOf(image), running)
						}
					}
					if tagOf(images.APIServer) != target {
						t.Errorf("target %s, reports %q %q %q: the API server is at %s", target, apiServer, controllerManager, scheduler, tagOf(images.APIServer))
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
