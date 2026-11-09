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

func TestLatestIsAPatchOfItsMinor(t *testing.T) {
	for _, minor := range versions.Minors() {
		latest, ok := versions.Latest(minor)
		if !ok {
			t.Fatalf("minor %s has no latest", minor)
		}
		if got, ok := versions.Minor(latest); !ok || got != minor {
			t.Errorf("latest of %s is %s", minor, latest)
		}
		if _, ok := versions.For(latest); !ok {
			t.Errorf("%s, the latest of %s, has no images", latest, minor)
		}
	}
	if latest, ok := versions.Latest("1.34"); ok {
		t.Errorf("1.34 has a latest, %s, and Lodia does not run it", latest)
	}
}

func TestNext(t *testing.T) {
	for _, c := range []struct {
		minor, want string
		ok          bool
	}{
		{"1.35", "1.36", true},
		{"1.36", "1.37", true},
		{"1.37", "1.38", false},
		{"1.34", "1.35", true},
		{"1.33", "1.34", false},
		{"nonsense", "", false},
	} {
		if got, ok := versions.Next(c.minor); got != c.want || ok != c.ok {
			t.Errorf("Next(%q) is %q, %t, want %q, %t", c.minor, got, ok, c.want, c.ok)
		}
	}
}

func TestMinorsAhead(t *testing.T) {
	for _, c := range []struct {
		from, to string
		want     int
		ok       bool
	}{
		{"v1.36.6", "v1.36.1", 0, true},
		{"v1.36.6", "v1.37.0", 1, true},
		{"v1.9.0", "v1.10.0", 1, true},
		{"v1.37.2", "v1.34.0", -3, true},
		{"v1.33.9", "1.37", 4, true},
		{"v1.34.2", "1.37", 3, true},
		{"v1.36.6", "v2.0.0", 0, false},
		{"v1.36.6", "latest", 0, false},
	} {
		if got, ok := versions.MinorsAhead(c.from, c.to); got != c.want || ok != c.ok {
			t.Errorf("MinorsAhead(%q, %q) is %d, %t, want %d, %t", c.from, c.to, got, ok, c.want, c.ok)
		}
	}
}

func TestOldest(t *testing.T) {
	for _, c := range []struct {
		name     string
		reported []string
		want     string
		ok       bool
	}{
		{"one", []string{"v1.36.2"}, "v1.36.2", true},
		{"equal", []string{"v1.36.2", "v1.36.2", "v1.36.2"}, "v1.36.2", true},
		{"a patch apart", []string{"v1.36.3", "v1.36.2", "v1.36.10", "v1.36.4"}, "v1.36.2", true},
		{"a minor apart", []string{"v1.37.0", "v1.36.9", "v1.37.1", "v1.36.10"}, "v1.36.9", true},
		{"a minor above a patch", []string{"v1.37.0", "v1.36.9", "v1.36.12"}, "v1.36.9", true},
		{"numbers, not text", []string{"v1.9.0", "v1.10.0"}, "v1.9.0", true},
		{"none", nil, "", false},
		{"one of them empty", []string{"v1.36.2", "", "v1.36.3"}, "", false},
		{"one without a patch", []string{"v1.36.2", "v1.35", "v1.36.3"}, "", false},
		{"one that is not a version", []string{"v1.36.2", "latest"}, "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got, ok := versions.Oldest(c.reported...); got != c.want || ok != c.ok {
				t.Errorf("the oldest of %q is %q, %t; want %q, %t", c.reported, got, ok, c.want, c.ok)
			}
		})
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
