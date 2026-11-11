// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package versions is the table of the Kubernetes minors Lodia runs and the
// images of each version.
package versions

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/distribution/reference"
	"k8s.io/apimachinery/pkg/util/version"
)

const registry = "registry.k8s.io"

type Images struct {
	APIServer          string
	ControllerManager  string
	Scheduler          string
	KubeProxy          string
	CoreDNS            string
	KonnectivityServer string
	KonnectivityAgent  string
}

// CoreDNS is kubeadm's pin for each minor (kubernetes/kubernetes
// cmd/kubeadm/app/constants/constants.go at v1.35.0, v1.36.0 and v1.37.1);
// konnectivity 0.NN serves Kubernetes 1.NN (apiserver-network-proxy README);
// latest is the minor's newest patch, https://dl.k8s.io/release/stable-<minor>.txt.
var minors = map[string]struct{ coreDNS, konnectivity, latest string }{
	"1.35": {"v1.13.1", "v0.35.0", "v1.35.10"},
	"1.36": {"v1.14.2", "v0.36.0", "v1.36.6"},
	"1.37": {"v1.14.6", "v0.37.0", "v1.37.2"},
}

// For returns the images of a Kubernetes version vMAJOR.MINOR.PATCH, and
// false when Lodia does not run its minor.
func For(version string) (Images, bool) {
	minor, ok := Minor(version)
	m, supported := minors[minor]
	if !ok || !supported {
		return Images{}, false
	}
	return Images{
		APIServer:          registry + "/kube-apiserver:" + version,
		ControllerManager:  registry + "/kube-controller-manager:" + version,
		Scheduler:          registry + "/kube-scheduler:" + version,
		KubeProxy:          registry + "/kube-proxy:" + version,
		CoreDNS:            registry + "/coredns/coredns:" + m.coreDNS,
		KonnectivityServer: registry + "/kas-network-proxy/proxy-server:" + m.konnectivity,
		KonnectivityAgent:  registry + "/kas-network-proxy/proxy-agent:" + m.konnectivity,
	}, true
}

// PinKonnectivity returns images with both konnectivity images at the tag
// pin, in place of their tag and digest: the agents and the server follow one
// pin. An empty pin leaves them as they are.
func PinKonnectivity(images Images, pin string) (Images, error) {
	if pin == "" {
		return images, nil
	}
	server, err := withTag(images.KonnectivityServer, pin)
	if err != nil {
		return Images{}, err
	}
	agent, err := withTag(images.KonnectivityAgent, pin)
	if err != nil {
		return Images{}, err
	}
	images.KonnectivityServer, images.KonnectivityAgent = server, agent
	return images, nil
}

func withTag(image, tag string) (string, error) {
	named, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return "", err
	}
	tagged, err := reference.WithTag(reference.TrimNamed(named), tag)
	if err != nil {
		return "", err
	}
	return tagged.String(), nil
}

// Minor returns "1.35" for "v1.35.8".
func Minor(version string) (string, bool) {
	major, rest, ok := strings.Cut(strings.TrimPrefix(version, "v"), ".")
	minor, _, hasPatch := strings.Cut(rest, ".")
	return major + "." + minor, ok && hasPatch
}

// Oldest returns the oldest of the given Kubernetes versions, and false when
// none is given or one is not a semantic version: a version that cannot be
// compared says nothing of what runs.
func Oldest(reported ...string) (string, bool) {
	oldest, found := "", false
	var floor *version.Version
	for _, r := range reported {
		v, err := version.ParseSemantic(r)
		if err != nil {
			return "", false
		}
		if !found || v.LessThan(floor) {
			oldest, floor, found = r, v, true
		}
	}
	return oldest, found
}

// Newer says whether the Kubernetes version a is newer than b, and false when
// either is not a semantic version.
func Newer(a, b string) bool {
	va, errA := version.ParseSemantic(a)
	vb, errB := version.ParseSemantic(b)
	return errA == nil && errB == nil && vb.LessThan(va)
}

// Latest returns the newest patch of minor in the table, and false when Lodia
// does not run it.
func Latest(minor string) (string, bool) {
	m, ok := minors[minor]
	return m.latest, ok
}

// Next returns the minor after minor ("1.37" for "1.36"), and false when Lodia
// does not run it; minor may have left the table.
func Next(minor string) (string, bool) {
	v, err := version.ParseGeneric(minor)
	if err != nil {
		return "", false
	}
	next := fmt.Sprintf("%d.%d", v.Major(), v.Minor()+1)
	_, ok := minors[next]
	return next, ok
}

// MinorsAhead returns how many minors to is ahead of from, each a version or a
// minor, negative when it is behind, and false when one does not parse or
// their majors differ.
func MinorsAhead(from, to string) (int, bool) {
	a, errFrom := version.ParseGeneric(from)
	b, errTo := version.ParseGeneric(to)
	if errFrom != nil || errTo != nil || a.Major() != b.Major() {
		return 0, false
	}
	return int(b.Minor()) - int(a.Minor()), true
}

// Minors are the minors Lodia runs, oldest first.
func Minors() []string {
	return slices.Sorted(maps.Keys(minors))
}
