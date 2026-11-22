// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"path"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseRenderCheckRequiresEachReferenceInItsRole(t *testing.T) {
	role := func(operator, signer, tunnel string) string {
		return "kind: Deployment\n" +
			"          image: " + operator + "\n" +
			"          args:\n" +
			"            - --signer-image=" + signer + "\n" +
			"            - --tunnel-image=" + tunnel + "\n" +
			"            - --etcd-druid-version=v0.39.2\n"
	}
	operator, signer, tunnel := references[0], references[1], references[2]
	fourth := "ghcr.io/lodia-io/lodia-other@sha256:" + strings.Repeat("e", 64)
	tests := []struct {
		name   string
		render string
		ok     bool
	}{
		{name: "each reference in its role", render: role(operator, signer, tunnel), ok: true},
		{name: "the operator's and the signer's swapped", render: role(signer, operator, tunnel)},
		{name: "the signer's and the tunnel's swapped", render: role(operator, tunnel, signer)},
		{name: "the operator's and the tunnel's swapped", render: role(tunnel, signer, operator)},
		{name: "a digest of another repository in a role", render: role(operator, "ghcr.io/lodia-io/lodia-signer@sha256:"+digestA, tunnel)},
		{name: "an image by tag", render: role(operator, signer, "ghcr.io/lodia-io/lodia-tunnel:0.2.0-rc.1")},
		{name: "the operator's image missing", render: strings.Replace(role(operator, signer, tunnel), "          image: "+operator+"\n", "", 1)},
		{name: "a fourth reference", render: role(operator, signer, tunnel) + "        - image: " + fourth + "\n"},
		{name: "the reference of a role twice", render: role(operator, signer, tunnel) + "          image: " + operator + "\n"},
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
	good := "          image: " + operator + "\n            - --signer-image=" + signer + "\n            - --tunnel-image=" + tunnel + "\n"
	swapped := "          image: " + signer + "\n            - --signer-image=" + operator + "\n            - --tunnel-image=" + tunnel + "\n"
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
