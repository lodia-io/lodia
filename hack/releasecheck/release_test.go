// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/yaml"
)

const root = "../.."

const (
	checkout   = "actions/checkout"
	setupGo    = "actions/setup-go"
	cosign     = "sigstore/cosign-installer"
	setupHelm  = "azure/setup-helm"
	attest     = "actions/attest"
	refsStepID = "refs"

	tagCheck       = "Check the tag"
	login          = "Log in to the registry"
	pushImages     = "Push the images"
	pushChart      = "Package and push the chart"
	pull           = "Pull all four without credentials"
	sign           = "Sign the images and the chart"
	verifySigned   = "Verify the signatures as a user does"
	nameArtifacts  = "Name the artifacts by their digests"
	verifyAttested = "Verify the attestations as a user does"
	publish        = "Publish the release"
)

const (
	imagesEnv = "LODIA_IMAGE=ghcr.io/lodia-io/lodia@sha256:" + digestA + "\n" +
		"SIGNER_IMAGE=ghcr.io/lodia-io/lodia-signer@sha256:" + digestB + "\n" +
		"TUNNEL_IMAGE=ghcr.io/lodia-io/lodia-tunnel@sha256:" + digestC + "\n"
	chartEnv = "CHART_IMAGE=ghcr.io/lodia-io/charts/lodia@sha256:" + digestD + "\n"

	digestA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	digestB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	digestC = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	digestD = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
)

var references = []string{
	"ghcr.io/lodia-io/lodia@sha256:" + digestA,
	"ghcr.io/lodia-io/lodia-signer@sha256:" + digestB,
	"ghcr.io/lodia-io/lodia-tunnel@sha256:" + digestC,
	"ghcr.io/lodia-io/charts/lodia@sha256:" + digestD,
}

type step struct {
	ID              string            `json:"id"`
	Name            string            `json:"name"`
	Uses            string            `json:"uses"`
	With            map[string]any    `json:"with"`
	Shell           string            `json:"shell"`
	Env             map[string]string `json:"env"`
	Run             string            `json:"run"`
	If              any               `json:"if"`
	ContinueOnError any               `json:"continue-on-error"`
}

type workflow struct {
	On          any               `json:"true"`
	Env         map[string]string `json:"env"`
	Permissions map[string]string `json:"permissions"`
	Jobs        map[string]struct {
		Needs       any               `json:"needs"`
		Env         map[string]string `json:"env"`
		If          string            `json:"if"`
		Permissions map[string]string `json:"permissions"`
		Steps       []step            `json:"steps"`
	} `json:"jobs"`
}

func releaseFile() string {
	return filepath.Join(root, ".github", "workflows", "release.yml")
}

func readRelease(t *testing.T) workflow {
	t.Helper()
	raw, err := os.ReadFile(releaseFile())
	if err != nil {
		t.Fatal(err)
	}
	var w workflow
	if err := yaml.Unmarshal(raw, &w); err != nil {
		t.Fatal(err)
	}
	return w
}

func releaseSteps(t *testing.T) []step {
	t.Helper()
	return readRelease(t).Jobs["release"].Steps
}

func stepNamed(t *testing.T, name string) step {
	t.Helper()
	for _, s := range releaseSteps(t) {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("the release job has no step %q", name)
	return step{}
}

func runMake(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "make", append([]string{"--no-print-directory"}, args...)...)
	cmd.Dir = root
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func releaseDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "work", "release"), 0o755); err != nil {
		t.Fatal(err)
	}
	for file, content := range map[string]string{"images.env": imagesEnv, "chart.env": chartEnv} {
		if err := os.WriteFile(filepath.Join(dir, "work", "release", file), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func runScript(t *testing.T, dir, script string, env ...string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "bash", "-e", "-c", script)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the script failed: %v\n%s\n%s", err, script, out)
	}
}

func TestTheReleaseWorkflowStartsOnEveryVTagAndNothingElse(t *testing.T) {
	w := readRelease(t)

	if diff := cmp.Diff(map[string]any{"push": map[string]any{"tags": []any{"v*"}}}, w.On); diff != "" {
		t.Errorf("the trigger (-want +got):\n%s", diff)
	}
}

func TestTheReleaseWorkflowGrantsOnlyWhatItUsesAndHasNoCleanupJob(t *testing.T) {
	w := readRelease(t)

	got := map[string]map[string]string{"workflow": w.Permissions}
	for name, job := range w.Jobs {
		got[name] = job.Permissions
	}
	want := map[string]map[string]string{
		"workflow": {"contents": "read"},
		"release":  {"contents": "write", "packages": "write", "actions": "read", "id-token": "write", "attestations": "write"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("permissions, one entry per job (-want +got):\n%s", diff)
	}
}

func TestTheReleaseWorkflowSetsNoEnvironmentAboveItsSteps(t *testing.T) {
	w := readRelease(t)

	if w.Env != nil {
		t.Errorf("the workflow sets the environment %v, which every make call of the release reads", w.Env)
	}
	for name, job := range w.Jobs {
		if job.Env != nil {
			t.Errorf("the job %s sets the environment %v, which every make call of it reads", name, job.Env)
		}
	}
}

func TestTheReleaseWorkflowSetsUpTheRunnerForItsSteps(t *testing.T) {
	var got []map[string]any
	for _, s := range releaseSteps(t) {
		switch action, _, _ := strings.Cut(s.Uses, "@"); action {
		case checkout, setupGo, cosign, setupHelm:
			got = append(got, s.With)
		}
	}

	want := []map[string]any{
		{"fetch-depth": float64(0), "persist-credentials": false},
		{"go-version-file": "go.mod"},
		{"cosign-release": "v3.1.4"},
		{"version": "v4.3.1"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("the inputs of checkout, setup-go, cosign-installer and setup-helm, in order (-want +got):\n%s", diff)
	}
}

func TestTheReleaseWorkflowRunsItsStepsInOrder(t *testing.T) {
	var order []string
	runs := map[string]string{}
	envs := map[string]map[string]string{}
	for _, s := range releaseSteps(t) {
		if s.If != nil || s.ContinueOnError != nil {
			t.Errorf("the step %q has an if or a continue-on-error, so a failed step may not stop the job", s.Name)
		}
		switch {
		case s.Name != "":
			order = append(order, s.Name)
			runs[s.Name] = s.Run
			envs[s.Name] = s.Env
		case s.Uses != "":
			action, _, _ := strings.Cut(s.Uses, "@")
			order = append(order, action)
		default:
			t.Errorf("an unnamed step that runs %q", s.Run)
		}
	}

	want := []string{
		checkout, setupGo, cosign, setupHelm,
		tagCheck, login, pushImages, pushChart, pull, sign, verifySigned,
		nameArtifacts, attest, attest, attest, attest,
		verifyAttested, publish,
	}
	if diff := cmp.Diff(want, order); diff != "" {
		t.Errorf("the steps of the release job (-want +got):\n%s", diff)
	}
	for name, want := range map[string]string{
		tagCheck:   `go run ./hack/releasecheck tag --commit="$GITHUB_SHA" "$GITHUB_REF_NAME"`,
		login:      `make release-login`,
		pushImages: `make release-images TAG="$GITHUB_REF_NAME"`,
		pushChart:  `make release-chart TAG="$GITHUB_REF_NAME" HELM_PUSH=true`,
		sign:       `make release-sign`,
	} {
		if runs[name] != want {
			t.Errorf("the step %q runs %q, want %q", name, runs[name], want)
		}
	}
	if diff := cmp.Diff(map[string]map[string]string{
		tagCheck:       {"GITHUB_TOKEN": "${{ github.token }}"},
		login:          {"GITHUB_TOKEN": "${{ github.token }}"},
		pushImages:     nil,
		pushChart:      nil,
		pull:           nil,
		sign:           nil,
		verifySigned:   nil,
		nameArtifacts:  nil,
		verifyAttested: {"GH_TOKEN": "${{ github.token }}"},
		publish:        {"GH_TOKEN": "${{ github.token }}"},
	}, envs); diff != "" {
		t.Errorf("the environments of the named steps (-want +got):\n%s", diff)
	}
}

func TestTheReleaseWorkflowReadsWhatItPushedWithEveryCredentialStoreEmpty(t *testing.T) {
	for name, want := range map[string]string{
		pull:           `DOCKER_CONFIG="$(mktemp -d)" HELM_REGISTRY_CONFIG="$(mktemp -d)/config.json" make release-pull`,
		verifySigned:   `DOCKER_CONFIG="$(mktemp -d)" make release-verify-signatures TAG="$GITHUB_REF_NAME"`,
		verifyAttested: `DOCKER_CONFIG="$(mktemp -d)" make release-verify-attestations TAG="$GITHUB_REF_NAME"`,
	} {
		if got := stepNamed(t, name).Run; got != want {
			t.Errorf("the step %q runs %q, want %q", name, got, want)
		}
	}
}

func TestTheReleaseWorkflowAttestsEachArtifactWithTheSettingsThatWorked(t *testing.T) {
	attested := 0
	for _, s := range releaseSteps(t) {
		if action, _, _ := strings.Cut(s.Uses, "@"); action != attest {
			continue
		}
		attested++
		with := maps.Clone(s.With)
		for _, subject := range []string{"subject-name", "subject-digest"} {
			if _, ok := with[subject]; !ok {
				t.Errorf("attestation %d has no %s", attested, subject)
			}
			delete(with, subject)
		}
		if diff := cmp.Diff(map[string]any{"push-to-registry": true, "create-storage-record": false}, with); diff != "" {
			t.Errorf("attestation %d, the inputs beside its subject (-want +got):\n%s", attested, diff)
		}
		if diff := cmp.Diff(map[string]string{"NODE_OPTIONS": "--max-http-header-size=32768"}, s.Env); diff != "" {
			t.Errorf("attestation %d, its environment (-want +got):\n%s", attested, diff)
		}
	}
	if attested != 4 {
		t.Errorf("the release attests %d artifacts, want the three images and the chart", attested)
	}
}

func TestTheReleaseWorkflowAttestsExactlyTheReferencesTheEnvFilesName(t *testing.T) {
	var refs step
	for _, s := range releaseSteps(t) {
		if s.ID == refsStepID {
			refs = s
		}
	}
	if refs.Run == "" {
		t.Fatalf("no step has the id %s, which the attestations name", refsStepID)
	}
	dir := releaseDir(t)
	outputs := filepath.Join(dir, "outputs")
	runScript(t, dir, refs.Run, "GITHUB_OUTPUT="+outputs)
	raw, err := os.ReadFile(outputs)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		key, value, _ := strings.Cut(line, "=")
		have[key] = value
	}
	output := regexp.MustCompile(`^\$\{\{ steps\.` + refsStepID + `\.outputs\.([a-z-]+) \}\}$`)
	resolve := func(value any) string {
		m := output.FindStringSubmatch(value.(string))
		if m == nil {
			t.Errorf("%q is not an output of the step %s", value, refsStepID)
			return ""
		}
		return have[m[1]]
	}

	var got []string
	for _, s := range releaseSteps(t) {
		if action, _, _ := strings.Cut(s.Uses, "@"); action == attest {
			got = append(got, resolve(s.With["subject-name"])+"@"+resolve(s.With["subject-digest"]))
		}
	}

	slices.Sort(got)
	want := slices.Sorted(slices.Values(references))
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("what the attestations name (-want +got):\n%s", diff)
	}
}

func TestTheReleaseWorkflowPublishesAPreReleaseForATagWithADashAndNamesWhatItPushed(t *testing.T) {
	script := stepNamed(t, publish).Run
	bin := t.TempDir()
	fakeGH := "#!/bin/sh\nfor arg in \"$@\"; do printf '%s\\0' \"$arg\"; done > \"$CALLS\"\n"
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(fakeGH), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		tag        string
		prerelease bool
	}{
		{"v0.1.0-alpha.1", true},
		{"v0.1.0-rc.1", true},
		{"v0.1.0", false},
		{"v1.2.3", false},
	} {
		t.Run(tt.tag, func(t *testing.T) {
			dir := releaseDir(t)
			calls := filepath.Join(dir, "calls")
			runScript(t, dir, script, "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "CALLS="+calls, "GITHUB_REF_NAME="+tt.tag)
			raw, err := os.ReadFile(calls)
			if err != nil {
				t.Fatal(err)
			}
			args := strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00")

			if len(args) < 7 {
				t.Fatalf("gh was called with %q", args)
			}
			notes := args[6]
			want := []string{"release", "create", tt.tag, "--verify-tag", "--generate-notes", "--notes", notes}
			if tt.prerelease {
				want = append(want, "--prerelease")
			}
			if diff := cmp.Diff(want, args); diff != "" {
				t.Errorf("the arguments of gh (-want +got):\n%s", diff)
			}
			for _, want := range append([]string{"oci://ghcr.io/lodia-io/charts/lodia", "`" + strings.TrimPrefix(tt.tag, "v") + "`"}, references...) {
				if !strings.Contains(notes, want) {
					t.Errorf("the notes do not contain %q:\n%s", want, notes)
				}
			}
		})
	}
}

func TestTheReleaseWorkflowPinsEveryActionToACommitWithItsVersion(t *testing.T) {
	raw, err := os.ReadFile(releaseFile())
	if err != nil {
		t.Fatal(err)
	}
	pinned := regexp.MustCompile(`^\s*- uses: [A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[0-9a-f]{40} # v[0-9]+\.[0-9]+\.[0-9]+$`)

	actions := 0
	for number, line := range strings.Split(string(raw), "\n") {
		if !strings.Contains(line, "uses:") {
			continue
		}
		actions++
		if !pinned.MatchString(line) {
			t.Errorf("line %d, %q, does not pin an action to a full commit SHA with its version in a comment", number+1, strings.TrimSpace(line))
		}
	}
	if actions == 0 {
		t.Error("the release workflow uses no action, so the check found nothing to pin")
	}
}

func TestReleaseCheckFetchesThenChecksTheCommit(t *testing.T) {
	out, stderr, err := runMake(t, "-n", "release-check", "TAG=v0.1.0-alpha.1", "COMMIT=abc123")
	if err != nil {
		t.Fatalf("%v\n%s", err, stderr)
	}

	fetch := strings.Index(out, "git fetch --quiet --tags origin main\n")
	check := strings.Index(out, `GITHUB_TOKEN="${GITHUB_TOKEN:-$(gh auth token)}" go run ./hack/releasecheck tag --commit="abc123" "v0.1.0-alpha.1"`)
	if fetch < 0 || check < fetch {
		t.Errorf("want the fetch of origin/main and the tags, then the check of COMMIT with the token of the environment or of gh; the fetch is at %d and the check at %d of:\n%s", fetch, check, out)
	}

	out, stderr, err = runMake(t, "-n", "release-check", "TAG=v0.1.0")
	if err != nil || !strings.Contains(out, `--commit="HEAD" "v0.1.0"`) {
		t.Errorf("want the commit to default to HEAD, got %v and %q\n%s", err, out, stderr)
	}
}

func TestReleaseCheckWithoutATagSaysHowToUseItBeforeItFetchesAnything(t *testing.T) {
	out, stderr, err := runMake(t, "release-check")

	if err == nil || !strings.Contains(out, "usage: make release-check TAG=vX.Y.Z") || strings.Contains(out, "git fetch") {
		t.Errorf("want a failure with the usage and no fetch, got %v, %q and %q", err, out, stderr)
	}
}
