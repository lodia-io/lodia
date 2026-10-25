// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package versions_test

import (
	"slices"
	"testing"

	"github.com/segla-io/brygga/internal/versions"
)

func TestImagesOfASupportedVersion(t *testing.T) {
	images, ok := versions.For("v1.35.8")
	if !ok {
		t.Fatal("v1.35.8 is not supported")
	}
	want := versions.Images{
		APIServer:          "registry.k8s.io/kube-apiserver:v1.35.8",
		ControllerManager:  "registry.k8s.io/kube-controller-manager:v1.35.8",
		Scheduler:          "registry.k8s.io/kube-scheduler:v1.35.8",
		KubeProxy:          "registry.k8s.io/kube-proxy:v1.35.8",
		CoreDNS:            "registry.k8s.io/coredns/coredns:v1.13.1",
		KonnectivityServer: "registry.k8s.io/kas-network-proxy/proxy-server:v0.35.0",
		KonnectivityAgent:  "registry.k8s.io/kas-network-proxy/proxy-agent:v0.35.0",
	}
	if images != want {
		t.Errorf("images of v1.35.8:\n got %+v\nwant %+v", images, want)
	}
}

func TestUnsupportedVersions(t *testing.T) {
	for _, version := range []string{"v1.34.2", "v1.38.0", "v1.35", ""} {
		if _, ok := versions.For(version); ok {
			t.Errorf("%q is supported", version)
		}
	}
}

func TestMinors(t *testing.T) {
	if got, want := versions.Minors(), []string{"1.35", "1.36", "1.37"}; !slices.Equal(got, want) {
		t.Errorf("minors are %v, want %v", got, want)
	}
}
