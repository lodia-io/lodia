// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package versions_test

import (
	"slices"
	"strings"
	"testing"

	"go.lodia.io/lodia/internal/versions"
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

func TestTheKonnectivityPinReplacesTheTagOfAnyReference(t *testing.T) {
	digest := "@sha256:" + strings.Repeat("ab", 32)
	for image, repinned := range map[string]string{
		"registry.k8s.io/kas-network-proxy/%s:v0.37.0":                "registry.k8s.io/kas-network-proxy/%s:v0.37.2",
		"registry.example:5000/kas-network-proxy/%s:v0.37.0":          "registry.example:5000/kas-network-proxy/%s:v0.37.2",
		"registry.example:5000/kas-network-proxy/%s:v0.37.0" + digest: "registry.example:5000/kas-network-proxy/%s:v0.37.2",
		"registry.example:5000/kas-network-proxy/%s" + digest:         "registry.example:5000/kas-network-proxy/%s:v0.37.2",
	} {
		images := versions.Images{
			APIServer:          "registry.k8s.io/kube-apiserver:v1.37.0",
			KonnectivityServer: strings.Replace(image, "%s", "proxy-server", 1),
			KonnectivityAgent:  strings.Replace(image, "%s", "proxy-agent", 1),
		}
		pinned, err := versions.PinKonnectivity(images, "v0.37.2")
		if err != nil {
			t.Fatal(err)
		}
		want := versions.Images{
			APIServer:          images.APIServer,
			KonnectivityServer: strings.Replace(repinned, "%s", "proxy-server", 1),
			KonnectivityAgent:  strings.Replace(repinned, "%s", "proxy-agent", 1),
		}
		if pinned != want {
			t.Errorf("%s pinned to v0.37.2:\n got %+v\nwant %+v", image, pinned, want)
		}
		if unpinned, err := versions.PinKonnectivity(images, ""); err != nil || unpinned != images {
			t.Errorf("%s without a pin is %+v, %v", image, unpinned, err)
		}
	}
}
