// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

const koTool = `#!/bin/sh
{ printf 'VERSION=%s KO_DOCKER_REPO=%s ko' "$VERSION" "$KO_DOCKER_REPO"; printf ' %s' "$@"; echo; } >> "$CALLS"
cat "$KO_OUT"
`

func TestReleaseImagesTakesEachDigestFromTheRepositoryItNames(t *testing.T) {
	const (
		operator = "sha256:" + "1111111111111111111111111111111111111111111111111111111111111111"
		signer   = "sha256:" + "2222222222222222222222222222222222222222222222222222222222222222"
		tunnel   = "sha256:" + "3333333333333333333333333333333333333333333333333333333333333333"
		stranger = "sha256:" + "4444444444444444444444444444444444444444444444444444444444444444"

		stale = "stale\n"
		built = "LODIA_IMAGE=ghcr.io/lodia-io/lodia@" + operator + "\n" +
			"SIGNER_IMAGE=ghcr.io/lodia-io/lodia-signer@" + signer + "\n" +
			"TUNNEL_IMAGE=ghcr.io/lodia-io/lodia-tunnel@" + tunnel + "\n"
	)
	pushed := func(name, digest string) string { return imageRegistry + "/" + name + "@" + digest }
	tagged := func(name, digest string) string {
		return imageRegistry + "/" + name + ":" + releaseVersion + "@" + digest
	}
	tests := []struct {
		name    string
		printed []string
		want    string
		wantErr string
	}{
		{name: "in the order of the arguments", printed: []string{pushed("lodia", operator), pushed("lodia-signer", signer), pushed("lodia-tunnel", tunnel)}, want: built},
		{name: "in another order", printed: []string{pushed("lodia-tunnel", tunnel), pushed("lodia", operator), pushed("lodia-signer", signer)}, want: built},
		{name: "in the order of a map, with the tag ko puts on an image it does not push", printed: []string{tagged("lodia-signer", signer), tagged("lodia-tunnel", tunnel), tagged("lodia", operator)}, want: built},
		{name: "with a line of another repository", printed: []string{pushed("lodia", operator), pushed("lodia-signer", signer), pushed("lodia-tunnel", tunnel), pushed("lodia-other", stranger)}, want: built},
		{name: "without the signer", printed: []string{pushed("lodia", operator), pushed("lodia-tunnel", tunnel)}, wantErr: "ko printed [] for ghcr.io/lodia-io/lodia-signer"},
		{name: "without the operator, beside two repositories that start with its name", printed: []string{pushed("lodia-signer", signer), pushed("lodia-tunnel", tunnel)}, wantErr: "ko printed [] for ghcr.io/lodia-io/lodia,"},
		{name: "with the tunnel twice at two digests", printed: []string{pushed("lodia", operator), pushed("lodia-signer", signer), pushed("lodia-tunnel", tunnel), pushed("lodia-tunnel", stranger)}, wantErr: "for ghcr.io/lodia-io/lodia-tunnel, want one digest"},
		{name: "with the tunnel twice at one digest", printed: []string{pushed("lodia", operator), pushed("lodia-signer", signer), pushed("lodia-tunnel", tunnel), pushed("lodia-tunnel", tunnel)}, wantErr: "for ghcr.io/lodia-io/lodia-tunnel, want one digest"},
		{name: "with a digest that is not a sha256", printed: []string{pushed("lodia", operator), pushed("lodia-signer", "sha256:abc"), pushed("lodia-tunnel", tunnel)}, wantErr: "ko printed [sha256:abc] for ghcr.io/lodia-io/lodia-signer"},
		{name: "with nothing", wantErr: "ko printed [] for ghcr.io/lodia-io/lodia,"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			box := newToolbox(t, map[string]string{"ko": koTool})
			box.write("work/release/images.env", stale)
			box.write("work/release/chart.env", stale)
			out := box.write("ko-out", lines(tt.printed...))

			_, output, err := box.run(0, []string{"KO_OUT=" + out}, "release-images", "TAG="+releaseTag, "KO="+box.tool("ko"))

			if tt.wantErr != "" {
				if err == nil || !strings.Contains(output, tt.wantErr) {
					t.Errorf("want a failure with %q, got %v and %q", tt.wantErr, err, output)
				}
				if box.exists("images.env") {
					t.Errorf("a failed release-images left images.env: %q", box.read(filepath.Join(box.release, "images.env")))
				}
			} else {
				if err != nil {
					t.Fatalf("%v\n%s", err, output)
				}
				if got := box.read(filepath.Join(box.release, "images.env")); got != tt.want {
					t.Errorf("images.env is %q, want %q", got, tt.want)
				}
			}
			if box.exists("chart.env") {
				t.Error("release-images left the chart.env of an earlier run")
			}
		})
	}
}

func TestReleaseImagesBuildsTwoPlatformsTaggedWithTheVersionWithoutItsV(t *testing.T) {
	printed := lines(references[0], references[1], references[2])
	box := newToolbox(t, map[string]string{"ko": koTool})
	out := box.write("ko-out", printed)
	const build = "ko build --push=%s --base-import-paths --platform=linux/amd64,linux/arm64 --tags=" + releaseVersion + " ./cmd/lodia ./cmd/lodia-signer ./cmd/lodia-tunnel"
	for _, tt := range []struct {
		name string
		args []string
		push string
	}{
		{name: "pushing by default", push: "true"},
		{name: "without a push", args: []string{"KO_PUSH=false"}, push: "false"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, output, err := box.run(0, []string{"KO_OUT=" + out}, append([]string{"release-images", "TAG=" + releaseTag, "KO=" + box.tool("ko")}, tt.args...)...)

			want := lines("VERSION=" + releaseTag + " KO_DOCKER_REPO=" + imageRegistry + " " + fmt.Sprintf(build, tt.push))
			if got != want || err != nil {
				t.Errorf("ko was called as %q and the target failed with %v and %q, want %q", got, err, output, want)
			}
		})
	}
}
