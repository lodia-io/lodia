// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const druidArchive = "etcd-druid-v0.39.2.tgz"

const (
	yqTool = `#!/bin/sh
key="${1#.}"
key="${key%%.*}"
awk -v key="$key:" '/^[^ #]/ { inside = ($1 == key) } inside && $1 == "digest:" { print NR; exit }' "$2"
`

	helmTool = `#!/bin/sh
case "$1" in
dependency)
	mkdir -p "$3/charts" && cp "$DEPENDENCY" "$3/charts/etcd-druid-v0.39.2.tgz"
	;;
push)
	{ printf 'helm'; printf ' %s' "$@"; echo; } >> "$CALLS"
	cat "$PUSH_OUT"
	;;
*)
	exec "$REAL_HELM" "$@"
	;;
esac
`
)

func chartToolbox(t *testing.T) (*toolbox, []string) {
	t.Helper()
	realHelm, err := exec.LookPath("helm")
	if err != nil {
		t.Fatal(err)
	}
	archive := druidTarball(t)
	sum := sha256.Sum256(archive)
	dependency := filepath.Join(t.TempDir(), druidArchive)
	if err := os.WriteFile(dependency, archive, 0o644); err != nil {
		t.Fatal(err)
	}
	box := newToolbox(t, map[string]string{"helm": helmTool, "yq": yqTool}, "REAL_HELM="+realHelm, "DEPENDENCY="+dependency)
	vendored := box.write("etcd-druid.sha256", hex.EncodeToString(sum[:])+"  "+druidArchive+"\n")
	return box, []string{"release-chart", "TAG=" + releaseTag, "YQ=" + box.tool("yq"), "DRUID_SUM=" + vendored}
}

func druidTarball(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	zipper := gzip.NewWriter(&buf)
	archive := tar.NewWriter(zipper)
	for _, file := range [][2]string{
		{"etcd-druid/Chart.yaml", "apiVersion: v2\nname: etcd-druid\nversion: v0.39.2\n"},
		{"etcd-druid/values.yaml", "{}\n"},
	} {
		if err := archive.WriteHeader(&tar.Header{Name: file[0], Mode: 0o644, Size: int64(len(file[1])), ModTime: time.Unix(1, 0)}); err != nil {
			t.Fatal(err)
		}
		if _, err := archive.Write([]byte(file[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zipper.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func chartWithValues(t *testing.T, edit func(string) string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "lodia")
	if out, err := exec.CommandContext(t.Context(), "cp", "-R", filepath.Join(root, "charts", "lodia"), dir).CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	values := filepath.Join(dir, "values.yaml")
	raw, err := os.ReadFile(values)
	if err != nil {
		t.Fatal(err)
	}
	edited := edit(string(raw))
	if edited == string(raw) {
		t.Fatal("the edit changed nothing")
	}
	if err := os.WriteFile(values, []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestReleaseChartRefusesWhatItWasNotToldToPackage(t *testing.T) {
	const emptyDigest = "image:\n  repository: ghcr.io/lodia-io/lodia\n  tag: \"\"\n  digest: \"\""
	tests := []struct {
		name    string
		prepare func(*toolbox) []string
		wantErr string
	}{
		{name: "a vendored etcd-druid chart that is not the one the sum names", prepare: func(b *toolbox) []string {
			return []string{"DRUID_SUM=" + b.write("other.sha256", strings.Repeat("0", 64)+"  "+druidArchive+"\n")}
		}, wantErr: "is not the one charts/etcd-druid.sha256 names"},
		{name: "a values.yaml whose digest line is not empty", prepare: func(b *toolbox) []string {
			return []string{"CHART=" + chartWithValues(b.t, func(values string) string {
				return strings.Replace(values, emptyDigest, strings.TrimSuffix(emptyDigest, `""`)+`"sha256:`+strings.Repeat("9", 64)+`"`, 1)
			})}
		}, wantErr: "the packaged values.yaml differs from"},
		{name: "no images.env", prepare: func(b *toolbox) []string {
			if err := os.Remove(filepath.Join(b.release, "images.env")); err != nil {
				b.t.Fatal(err)
			}
			return nil
		}, wantErr: "images.env is missing: run make release-images first"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			box, args := chartToolbox(t)

			_, output, err := box.run(0, nil, append(args, tt.prepare(box)...)...)

			if err == nil || !strings.Contains(output, tt.wantErr) {
				t.Errorf("want a failure with %q, got %v and %q", tt.wantErr, err, output)
			}
		})
	}
}

func TestReleaseChartPushesOnlyOnRequestAndNamesTheChartByTheDigestPushPrints(t *testing.T) {
	const (
		pushedLine = "Pushed: " + chartRegistry + "/lodia:" + releaseVersion + "\n"
		digest     = "sha256:" + "5555555555555555555555555555555555555555555555555555555555555555"
		digestLine = "Digest: " + digest + "\n"
		namesIt    = "CHART_IMAGE=" + chartRegistry + "/lodia@" + digest + "\n"
	)
	tests := []struct {
		name    string
		out     string
		want    string
		wantErr string
	}{
		{name: "the output of the push", out: pushedLine + digestLine, want: namesIt},
		{name: "the digest line before the pushed one", out: digestLine + pushedLine, want: namesIt},
		{name: "no pushed line", out: digestLine, wantErr: "pushed [] and digest [" + digest + "]"},
		{name: "a pushed line naming another repository", out: "Pushed: " + imageRegistry + "/lodia:" + releaseVersion + "\n" + digestLine, wantErr: "pushed [" + imageRegistry + "/lodia:" + releaseVersion + "]"},
		{name: "a pushed line naming another tag", out: "Pushed: " + chartRegistry + "/lodia:0.2.0\n" + digestLine, wantErr: "pushed [" + chartRegistry + "/lodia:0.2.0]"},
		{name: "no digest line", out: pushedLine, wantErr: "and digest []"},
		{name: "two digest lines", out: pushedLine + digestLine + "Digest: sha256:" + strings.Repeat("6", 64) + "\n", wantErr: "want " + chartRegistry + "/lodia:" + releaseVersion + " and one digest"},
		{name: "a digest that is not a sha256", out: pushedLine + "Digest: sha256:abc\n", wantErr: "and digest [sha256:abc]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			box, args := chartToolbox(t)
			out := box.write("push-out", tt.out)

			calls, output, err := box.run(0, []string{"PUSH_OUT=" + out}, append(args, "HELM_PUSH=true")...)

			wantCall := "helm push " + filepath.Join(box.release, "lodia-"+releaseVersion+".tgz") + " oci://" + chartRegistry + "\n"
			if calls != wantCall {
				t.Errorf("the chart was pushed as %q, want %q", calls, wantCall)
			}
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(output, tt.wantErr) {
					t.Errorf("want a failure with %q, got %v and %q", tt.wantErr, err, output)
				}
				if box.exists("chart.env") {
					t.Errorf("a refused push left chart.env: %q", box.read(filepath.Join(box.release, "chart.env")))
				}
				return
			}
			if err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
			if got := box.read(filepath.Join(box.release, "chart.env")); got != tt.want {
				t.Errorf("chart.env is %q, want %q", got, tt.want)
			}
		})
	}

	t.Run("without HELM_PUSH", func(t *testing.T) {
		box, args := chartToolbox(t)

		calls, output, err := box.run(0, nil, args...)

		if err != nil || calls != "" || box.exists("chart.env") {
			t.Errorf("pushed %q and wrote chart.env %v, failing with %v and %q, want neither", calls, box.exists("chart.env"), err, output)
		}
		if !box.exists("lodia-" + releaseVersion + ".tgz") {
			t.Error("the chart was not packaged")
		}
	})
}
