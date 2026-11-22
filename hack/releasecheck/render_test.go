// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path"
	"path/filepath"
	"strings"
	"testing"
)

func container(name, image string, args ...string) string {
	out := "        - name: " + name + "\n          image: " + image + "\n"
	if len(args) > 0 {
		out += "          args:\n"
	}
	for _, arg := range args {
		out += "            - " + arg + "\n"
	}
	return out
}

func deployment(name, initContainers string, containers ...string) string {
	out := "---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: " + name + "\nspec:\n  template:\n    spec:\n"
	if initContainers != "" {
		out += "      initContainers:\n" + initContainers
	}
	return out + "      containers:\n" + strings.Join(containers, "")
}

const druid = "---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: etcd-druid\nspec:\n  template:\n    spec:\n      containers:\n" +
	"        - name: etcd-druid\n          image: europe-docker.pkg.dev/gardener-project/releases/gardener/etcd-druid:v0.39.2\n"

func operatorRender(operator, signer, tunnel string) string {
	return deployment("lodia", "", container("lodia", operator, "--signer-image="+signer, "--tunnel-image="+tunnel, "--etcd-druid-version=v0.39.2")) + druid
}

func TestReleaseRenderCheckRequiresEachReferenceInItsRole(t *testing.T) {
	operator, signer, tunnel := references[0], references[1], references[2]
	fourth := "ghcr.io/lodia-io/lodia-other@sha256:" + strings.Repeat("e", 64)
	const pause = "registry.k8s.io/pause:3.10"
	signerArg, tunnelArg := "--signer-image="+signer, "--tunnel-image="+tunnel
	tests := []struct {
		name   string
		render string
		ok     bool
	}{
		{name: "each reference in its role", render: operatorRender(operator, signer, tunnel), ok: true},
		{name: "the arguments in the other order", render: deployment("lodia", "", container("lodia", operator, tunnelArg, signerArg)) + druid, ok: true},
		{name: "a container and a Deployment that hold no reference", render: operatorRender(operator, signer, tunnel) + deployment("other", "", container("lodia", pause)), ok: true},
		{name: "the operator's and the signer's swapped", render: operatorRender(signer, operator, tunnel)},
		{name: "the signer's and the tunnel's swapped", render: operatorRender(operator, tunnel, signer)},
		{name: "the operator's and the tunnel's swapped", render: operatorRender(tunnel, signer, operator)},
		{name: "a digest of another repository in a role", render: operatorRender(operator, "ghcr.io/lodia-io/lodia-signer@sha256:"+digestA, tunnel)},
		{name: "an image by tag", render: operatorRender(operator, signer, "ghcr.io/lodia-io/lodia-tunnel:0.2.0-rc.1")},
		{name: "the operator's image missing", render: deployment("lodia", "", "        - name: lodia\n          args:\n            - "+signerArg+"\n            - "+tunnelArg+"\n") + druid},
		{name: "the operator's image in an init container and another in the container lodia", render: deployment("lodia", container("prepare", operator), container("lodia", pause, signerArg, tunnelArg)) + druid},
		{name: "the signer's argument in a container beside it", render: deployment("lodia", "", container("lodia", operator, tunnelArg), container("sidecar", pause, signerArg)) + druid},
		{name: "the tunnel's argument in an init container", render: deployment("lodia", container("prepare", pause, tunnelArg), container("lodia", operator, signerArg)) + druid},
		{name: "the three roles in a container not named lodia", render: deployment("lodia", "", container("manager", operator, signerArg, tunnelArg)) + druid},
		{name: "the three roles in a Deployment not named lodia", render: deployment("other", "", container("lodia", operator, signerArg, tunnelArg)) + druid},
		{name: "the three roles in an object that is not a Deployment", render: strings.Replace(operatorRender(operator, signer, tunnel), "kind: Deployment", "kind: StatefulSet", 1)},
		{name: "a fourth reference", render: operatorRender(operator, signer, tunnel) + deployment("other", "", container("other", fourth))},
		{name: "the operator's image again in another container", render: deployment("lodia", "", container("lodia", operator, signerArg, tunnelArg), container("sidecar", operator)) + druid},
		{name: "an argument twice", render: deployment("lodia", "", container("lodia", operator, signerArg, signerArg, tunnelArg)) + druid},
		{name: "a reference in a comment", render: operatorRender(operator, signer, tunnel) + "# " + fourth + "\n"},
		{name: "a reference after the argument of a role on its line", render: strings.Replace(operatorRender(operator, signer, tunnel), signerArg+"\n", signerArg+" # "+fourth+"\n", 1)},
		{name: "nothing", render: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			box := newToolbox(t, map[string]string{"helm": recorder})
			render := box.write("render.yaml", tt.render)
			packaged := filepath.Join(box.dir, "lodia-0.2.0-rc.1.tgz")

			calls, output, err := box.run(0, []string{"RENDER=" + render}, "release-render-check", "CHART="+packaged)

			if want := "helm template lodia " + packaged + " --namespace lodia-system\n"; calls != want {
				t.Errorf("rendered with %q, want %q", calls, want)
			}
			if tt.ok != (err == nil) {
				t.Errorf("failed with %v and %q, want a failure %v", err, output, !tt.ok)
			}
			if !tt.ok && !strings.Contains(output, "the default render of "+packaged+" has the lines") {
				t.Errorf("the failure says %q, want it to name the render", output)
			}
		})
	}
}

func TestReleasePullChecksTheRenderOfTheChartItPulledWithEveryCredentialStoreEmpty(t *testing.T) {
	operator, signer, tunnel := references[0], references[1], references[2]
	good := operatorRender(operator, signer, tunnel)
	swapped := operatorRender(signer, operator, tunnel)
	for _, tt := range []struct {
		name   string
		render string
		ok     bool
	}{
		{name: "a chart that renders the digests pushed", render: good, ok: true},
		{name: "a chart that renders two of them swapped", render: swapped},
	} {
		t.Run(tt.name, func(t *testing.T) {
			box := newToolbox(t, map[string]string{"crane": recorder, "helm": recorder})
			render := box.write("render.yaml", tt.render)
			chart := filepath.Join(box.release, "pull", "chart")
			pulled := filepath.Join(chart, "lodia-"+releaseVersion+".tgz")

			calls, _, err := box.run(0, []string{"RENDER=" + render}, "release-pull", "TAG="+releaseTag, "CRANE="+box.tool("crane"))

			var want []string
			for _, image := range references[:3] {
				repository, _, _ := strings.Cut(image, "@")
				want = append(want, "crane pull --format oci "+image+" "+filepath.Join(box.release, "pull", "images", path.Base(repository)))
			}
			chartImage := references[3]
			want = append(want, "helm pull oci://"+chartImage+" --destination "+chart, "helm template lodia "+pulled+" --namespace lodia-system")
			if calls != lines(want...) || tt.ok != (err == nil) {
				t.Errorf("ran %q and failed with %v, want %q and a failure %v", calls, err, lines(want...), !tt.ok)
			}
			if got := box.read(box.calls + ".docker"); !strings.HasPrefix(got, strings.Repeat("empty\n", 4)) {
				t.Errorf("DOCKER_CONFIG at each call is %q, want an empty directory at the four pulls", got)
			}
			if got := box.read(box.calls + ".helm"); !strings.HasPrefix(got, strings.Repeat("absent\n", 4)) {
				t.Errorf("HELM_REGISTRY_CONFIG at each call is %q, want a file that does not exist at the four pulls", got)
			}
		})
	}
}
