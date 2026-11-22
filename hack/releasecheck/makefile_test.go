// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	releaseTag     = "v0.2.0-rc.1"
	releaseVersion = "0.2.0-rc.1"
	imageRegistry  = "ghcr.io/lodia-io"
	chartRegistry  = "ghcr.io/lodia-io/charts"

	recorder = `#!/bin/sh
{ printf '%s' "${0##*/}"; printf ' %s' "$@"; echo; } >> "$CALLS"
if [ -d "${DOCKER_CONFIG:-}" ] && [ -z "$(ls -A "$DOCKER_CONFIG")" ]; then echo empty; else echo set; fi >> "$CALLS.docker"
if [ -n "${HELM_REGISTRY_CONFIG:-}" ] && [ ! -e "$HELM_REGISTRY_CONFIG" ]; then echo absent; else echo present; fi >> "$CALLS.helm"
[ "$(wc -l < "$CALLS" | tr -d ' ')" = "$FAIL_AT" ] && exit 1
case "${0##*/} $1" in
"crane manifest") name="${2%@*}"; cat "$INDEXES/${name##*/}" ;;
"helm pull") mkdir -p "$4" && : > "$4/lodia-$PULLED.tgz" ;;
"helm template") cat "$RENDER" ;;
esac
exit 0
`
)

func runMakeEnv(t *testing.T, env []string, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "make", append([]string{"--no-print-directory"}, args...)...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

type toolbox struct {
	t       *testing.T
	dir     string
	release string
	bin     string
	calls   string
	env     []string
}

func newToolbox(t *testing.T, tools map[string]string, env ...string) *toolbox {
	t.Helper()
	dir := releaseDir(t)
	b := &toolbox{
		t:       t,
		dir:     dir,
		release: filepath.Join(dir, "work", "release"),
		bin:     t.TempDir(),
		calls:   filepath.Join(dir, "calls"),
	}
	for name, script := range tools {
		if err := os.WriteFile(filepath.Join(b.bin, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	decoy := filepath.Join(dir, "decoy")
	if err := os.MkdirAll(decoy, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(decoy, "config.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	b.env = append([]string{
		"PATH=" + b.bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"CALLS=" + b.calls,
		"DOCKER_CONFIG=" + decoy,
		"HELM_REGISTRY_CONFIG=" + filepath.Join(decoy, "config.json"),
		"PULLED=" + releaseVersion,
	}, env...)
	return b
}

func (b *toolbox) tool(name string) string {
	return filepath.Join(b.bin, name)
}

func (b *toolbox) write(name, content string) string {
	b.t.Helper()
	file := filepath.Join(b.dir, name)
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		b.t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(content), 0o644); err != nil {
		b.t.Fatal(err)
	}
	return file
}

func (b *toolbox) run(failAt int, env []string, args ...string) (string, string, error) {
	b.t.Helper()
	for _, file := range []string{b.calls, b.calls + ".docker", b.calls + ".helm"} {
		if err := os.Remove(file); err != nil && !errors.Is(err, fs.ErrNotExist) {
			b.t.Fatal(err)
		}
	}
	refuse := ""
	if failAt > 0 {
		refuse = strconv.Itoa(failAt)
	}
	all := append(append([]string{"FAIL_AT=" + refuse}, b.env...), env...)
	stdout, stderr, err := runMakeEnv(b.t, all, append(args, "RELEASE_DIR="+b.release)...)
	return b.read(b.calls), stdout + stderr, err
}

func (b *toolbox) read(file string) string {
	b.t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		b.t.Fatal(err)
	}
	return string(raw)
}

func (b *toolbox) exists(name string) bool {
	b.t.Helper()
	_, err := os.Stat(filepath.Join(b.release, name))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		b.t.Fatal(err)
	}
	return err == nil
}

func lines(calls ...string) string {
	if len(calls) == 0 {
		return ""
	}
	return strings.Join(calls, "\n") + "\n"
}

func TestTheMakefileSignsAndVerifiesTheImagesAndTheChartOfTheRelease(t *testing.T) {
	const (
		signer   = "--certificate-identity https://github.com/lodia-io/lodia/.github/workflows/release.yml@refs/tags/" + releaseTag + " --certificate-oidc-issuer https://token.actions.githubusercontent.com"
		attested = "-R lodia-io/lodia --signer-workflow lodia-io/lodia/.github/workflows/release.yml --source-ref refs/tags/" + releaseTag
	)
	images, chart := references[:3], references[3]
	platforms := map[string][2]string{
		"lodia":        {"sha256:" + strings.Repeat("1", 64), "sha256:" + strings.Repeat("2", 64)},
		"lodia-signer": {"sha256:" + strings.Repeat("3", 64), "sha256:" + strings.Repeat("4", 64)},
		"lodia-tunnel": {"sha256:" + strings.Repeat("5", 64), "sha256:" + strings.Repeat("6", 64)},
	}
	box := newToolbox(t, map[string]string{"cosign": recorder, "gh": recorder, "crane": recorder})
	indexes := filepath.Join(box.dir, "indexes")
	writeIndexes := func(override map[string]string) {
		t.Helper()
		for name, platform := range platforms {
			index := `{"schemaVersion":2,"manifests":[{"digest":"` + platform[0] + `"},{"digest":"` + platform[1] + `"}]}`
			if listed, ok := override[name]; ok {
				index = listed
			}
			box.write(filepath.Join("indexes", name), index)
		}
	}
	run := func(target string, refuseCall int) (string, string, error) {
		return box.run(refuseCall, []string{"INDEXES=" + indexes}, target, "TAG="+releaseTag, "CRANE="+box.tool("crane"))
	}

	var sign, verify, attest []string
	for _, image := range images {
		sign = append(sign, "cosign sign --yes --recursive "+image)
		repository, _, _ := strings.Cut(image, "@")
		verify = append(verify, "cosign verify "+signer+" "+image, "crane manifest "+image)
		for _, platform := range platforms[path.Base(repository)] {
			verify = append(verify, "cosign verify "+signer+" "+repository+"@"+platform)
		}
	}
	sign = append(sign, "cosign sign --yes "+chart)
	verify = append(verify, "cosign verify "+signer+" "+chart)
	for _, reference := range references {
		attest = append(attest, "gh attestation verify oci://"+reference+" "+attested)
	}

	writeIndexes(nil)
	for _, tt := range []struct {
		target string
		calls  []string
	}{
		{"release-sign", sign},
		{"release-verify-signatures", verify},
		{"release-verify-attestations", attest},
	} {
		t.Run(tt.target, func(t *testing.T) {
			got, stderr, err := run(tt.target, 0)
			if got != lines(tt.calls...) || err != nil {
				t.Errorf("ran %q and failed with %v and %q, want %q", got, err, stderr, lines(tt.calls...))
			}
			for refused := 1; refused <= len(tt.calls); refused++ {
				got, _, err := run(tt.target, refused)
				if got != lines(tt.calls[:refused]...) || err == nil {
					t.Errorf("with %q refused, ran %q and failed with %v, want the first %d calls and a failure", tt.calls[refused-1], got, err, refused)
				}
			}
		})
	}

	t.Run("release-verify-signatures with every credential store empty", func(t *testing.T) {
		if _, stderr, err := run("release-verify-signatures", 0); err != nil {
			t.Fatalf("%v\n%s", err, stderr)
		}
		if got, want := box.read(box.calls+".docker"), strings.Repeat("empty\n", len(verify)); got != want {
			t.Errorf("DOCKER_CONFIG at each call is %q, want an empty directory at all %d", got, len(verify))
		}
	})

	for position := range images {
		for _, listed := range []string{`{"manifests":[]}`, `{}`, ``} {
			t.Run("release-verify-signatures with the index of image "+strconv.Itoa(position)+" "+listed, func(t *testing.T) {
				writeIndexes(map[string]string{path.Base(strings.Split(images[position], "@")[0]): listed})
				t.Cleanup(func() { writeIndexes(nil) })

				got, _, err := run("release-verify-signatures", 0)

				if want := lines(verify[:4*position+2]...); got != want || err == nil {
					t.Errorf("ran %q and failed with %v, want %q and a failure", got, err, want)
				}
			})
		}
	}
}
