// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
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
	echo "${TZ-unset}" >> "$CALLS.tz"
	cat "$PUSH_OUT"
	;;
template)
	{ printf 'helm'; printf ' %s' "$@"; echo; } >> "$CALLS"
	exec "$REAL_HELM" "$@"
	;;
*)
	exec "$REAL_HELM" "$@"
	;;
esac
`
)

func chartToolbox(t *testing.T) (*toolbox, []string) {
	t.Helper()
	box, vendored := chartTools(t)
	return box, []string{"release-chart", "TAG=" + releaseTag, "YQ=" + box.tool("yq"), vendored}
}

func chartTools(t *testing.T) (*toolbox, string) {
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
	box := newToolbox(t, map[string]string{"helm": helmTool, "yq": yqTool, "ko": koTool}, "REAL_HELM="+realHelm, "DEPENDENCY="+dependency)
	vendored := box.write("etcd-druid.sha256", hex.EncodeToString(sum[:])+"  "+druidArchive+"\n")
	return box, "DRUID_SUM=" + vendored
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

func chartWithFile(t *testing.T, name string, edit func(string) string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "lodia")
	if out, err := exec.CommandContext(t.Context(), "cp", "-R", filepath.Join(root, "charts", "lodia"), dir).CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	file := filepath.Join(dir, name)
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	edited := edit(string(raw))
	if edited == string(raw) {
		t.Fatal("the edit changed nothing")
	}
	if err := os.WriteFile(file, []byte(edited), 0o644); err != nil {
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
			return []string{"CHART=" + chartWithFile(b.t, "values.yaml", func(values string) string {
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

func commitAt(t *testing.T, when time.Time) string {
	t.Helper()
	stamp := "@" + strconv.FormatInt(when.Unix(), 10) + " " + when.Format("-0700")
	t.Setenv("GIT_AUTHOR_DATE", stamp)
	t.Setenv("GIT_COMMITTER_DATE", stamp)
	r := newRepository(t)
	r.commit(map[string]string{"file": "content\n"})
	return filepath.Join(r.dir, ".git")
}

func TestReleaseChartPackagesOneCommitToOneArchive(t *testing.T) {
	const tick = 1100 * time.Millisecond
	for _, tt := range []struct {
		zone   string
		commit time.Time
	}{
		{zone: "Europe/Kyiv", commit: time.Date(2026, 10, 25, 1, 30, 0, 0, time.UTC)},
		{zone: "America/New_York", commit: time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC)},
	} {
		t.Run(tt.zone, func(t *testing.T) {
			zone, err := time.LoadLocation(tt.zone)
			if err != nil {
				t.Fatal(err)
			}
			wall := func(at time.Time) string { return at.In(zone).Format(time.TimeOnly) }
			if wall(tt.commit) != wall(tt.commit.Add(-time.Hour)) && wall(tt.commit) != wall(tt.commit.Add(time.Hour)) {
				t.Fatalf("%s is %s in %s, a time that does not repeat", tt.commit, wall(tt.commit), tt.zone)
			}
			box, args := chartToolbox(t)
			env := append(slices.Clone(box.env), "TZ="+tt.zone, "GIT_DIR="+commitAt(t, tt.commit.In(zone)))

			var sums []string
			for i, name := range []string{"first", "second"} {
				if i > 0 {
					time.Sleep(tick)
				}
				release := filepath.Join(box.dir, name)
				if err := os.MkdirAll(release, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(release, "images.env"), []byte(imagesEnv), 0o644); err != nil {
					t.Fatal(err)
				}
				stdout, stderr, err := runMakeEnv(t, env, append(args, "RELEASE_DIR="+release)...)
				if err != nil {
					t.Fatalf("%v\n%s%s", err, stdout, stderr)
				}
				archive := filepath.Join(release, "lodia-"+releaseVersion+".tgz")
				raw, err := os.ReadFile(archive)
				if err != nil {
					t.Fatal(err)
				}
				sum := sha256.Sum256(raw)
				sums = append(sums, hex.EncodeToString(sum[:]))
				info, err := os.Stat(archive)
				if err != nil {
					t.Fatal(err)
				}
				if !info.ModTime().Equal(tt.commit) {
					t.Errorf("%s is dated %s, want the commit's %s", archive, info.ModTime().UTC(), tt.commit)
				}
				var undated []string
				for entry, modTime := range tarModTimes(t, raw) {
					if !modTime.Equal(tt.commit) {
						undated = append(undated, entry)
					}
				}
				if len(undated) > 0 {
					slices.Sort(undated)
					t.Errorf("%s holds %d files not dated at the commit's %s, the first %s", archive, len(undated), tt.commit, undated[0])
				}
			}

			if sums[0] != sums[1] {
				version, _ := exec.CommandContext(t.Context(), "helm", "version", "--short").Output()
				t.Errorf("two packages of one commit have the sha256 %s and %s with %s, want one; the release pins v4.3.1", sums[0], sums[1], strings.TrimSpace(string(version)))
			}
		})
	}
}

func TestReleaseChartPushesUnderUTCWhateverTheZoneOfTheHost(t *testing.T) {
	box, args := chartToolbox(t)
	out := box.write("push-out", "Pushed: "+chartRegistry+"/lodia:"+releaseVersion+"\nDigest: sha256:"+strings.Repeat("5", 64)+"\n")

	_, output, err := box.run(0, []string{"PUSH_OUT=" + out, "TZ=Europe/Kyiv"}, append(args, "HELM_PUSH=true")...)

	if err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
	if got := box.read(box.calls + ".tz"); got != "UTC0\n" {
		t.Errorf("helm push ran with TZ %q, want UTC0", got)
	}
}

func TestVerifyReleaseChecksTheRenderOfTheChartItPackaged(t *testing.T) {
	const operatorContainer = "      containers:\n        - name: lodia\n          image: {{ include \"lodia.image\" (dict \"image\" .Values.image \"appVersion\" .Chart.AppVersion) }}\n"
	for _, tt := range []struct {
		name string
		edit func(string) string
		ok   bool
	}{
		{name: "the chart of the repository", ok: true},
		{name: "a chart with the operator's image in an init container and another in the container lodia", edit: func(deployment string) string {
			return strings.Replace(deployment, operatorContainer, "      initContainers:\n        - name: prepare\n          image: {{ include \"lodia.image\" (dict \"image\" .Values.image \"appVersion\" .Chart.AppVersion) }}\n      containers:\n        - name: lodia\n          image: registry.k8s.io/pause:3.10\n", 1)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			box, vendored := chartTools(t)
			printed := box.write("ko-out", lines(references[0], references[1], references[2]))
			args := []string{"verify-release", "KO=" + box.tool("ko"), vendored}
			if tt.edit != nil {
				args = append(args, "CHART="+chartWithFile(t, "templates/deployment.yaml", tt.edit))
			}

			calls, output, err := box.run(0, []string{"KO_OUT=" + printed}, args...)

			archive := filepath.Join(box.release, "lodia-0.0.0-verify.tgz")
			want := lines(
				"VERSION=v0.0.0-verify KO_DOCKER_REPO="+imageRegistry+" ko build --push=false --base-import-paths --platform=linux/amd64,linux/arm64 --tags=0.0.0-verify ./cmd/lodia ./cmd/lodia-signer ./cmd/lodia-tunnel",
				"helm template lodia "+archive+" --namespace lodia-system",
			)
			if calls != want {
				t.Errorf("ran %q, want %q", calls, want)
			}
			if tt.ok != (err == nil) {
				t.Errorf("failed with %v and %q, want a failure %v", err, output, !tt.ok)
			}
			if !tt.ok && !strings.Contains(output, "the default render of "+archive+" has the lines") {
				t.Errorf("the failure says %q, want it to name the render of the packaged chart", output)
			}
		})
	}
}

func tarModTimes(t *testing.T, raw []byte) map[string]time.Time {
	t.Helper()
	zipper, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	times := map[string]time.Time{}
	archive := tar.NewReader(zipper)
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		times[header.Name] = header.ModTime
	}
	if len(times) == 0 {
		t.Fatal("the archive holds no file")
	}
	return times
}
