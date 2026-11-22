// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/yaml"
)

const (
	firstTag     = "v0.1.0-alpha.1"
	nightlyFleet = "nightly-fleet.yml"
	nightlyHA    = "nightly-ha.yml"
	fleetTitle   = "nightly-fleet, 300 tenants"
)

var clock = time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)

func installCommand(version string) string {
	return "helm install lodia oci://ghcr.io/lodia-io/charts/lodia --version " + version + " \\\n  -n lodia-system --create-namespace\n"
}

func upgradeCommand(version string) string {
	return "helm upgrade lodia oci://ghcr.io/lodia-io/charts/lodia --version " + version + " \\\n  -n lodia-system\n"
}

func goodFiles(version string) map[string]string {
	return map[string]string{
		".github/workflows/ci.yml":          "name: ci\n",
		".github/workflows/nightly.yml":     "name: nightly\n",
		".github/workflows/" + nightlyHA:    "name: nightly-ha\n",
		".github/workflows/" + nightlyFleet: "name: nightly-fleet\nrun-name: nightly-fleet, ${{ inputs.tenants || '300' }} tenants\non:\n  workflow_dispatch:\n    inputs:\n      tenants:\n        default: \"300\"\n",
		".github/workflows/release.yml":     "name: release\n",
		chartFile:                           "apiVersion: v2\nname: lodia\nversion: 0.0.0-dev\nappVersion: 0.0.0-dev\n",
		"README.md":                         installCommand(version),
		"docs/README.md":                    "# Lodia\n\n" + installCommand(version),
		"docs/operations.md":                "## Upgrading Lodia\n\n" + upgradeCommand(version),
	}
}

func withInput(name, input, def, runName string) string {
	file := "name: " + name + "\non:\n  workflow_dispatch:\n    inputs:\n      " + input + ":\n        default: \"" + def + "\"\n"
	if runName != "" {
		file += "run-name: " + runName + "\n"
	}
	return file
}

type repository struct {
	t   *testing.T
	dir string
}

func TestMain(m *testing.M) {
	for k, v := range map[string]string{
		"GIT_CONFIG_GLOBAL":   os.DevNull,
		"GIT_CONFIG_NOSYSTEM": "1",
		"GIT_AUTHOR_NAME":     "Test",
		"GIT_AUTHOR_EMAIL":    "test@example.test",
		"GIT_COMMITTER_NAME":  "Test",
		"GIT_COMMITTER_EMAIL": "test@example.test",
	} {
		if err := os.Setenv(k, v); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

func newRepository(t *testing.T) *repository {
	t.Helper()
	r := &repository{t: t, dir: t.TempDir()}
	r.git("init", "--quiet", "--initial-branch=main")
	return r
}

func (r *repository) git(args ...string) string {
	r.t.Helper()
	out, err := exec.CommandContext(r.t.Context(), "git", append([]string{"-C", r.dir}, args...)...).CombinedOutput()
	if err != nil {
		r.t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func (r *repository) write(path, content string) {
	r.t.Helper()
	full := filepath.Join(r.dir, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		r.t.Fatal(err)
	}
}

func (r *repository) commit(files map[string]string) string {
	r.t.Helper()
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		r.t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != ".git" {
			if err := os.RemoveAll(filepath.Join(r.dir, entry.Name())); err != nil {
				r.t.Fatal(err)
			}
		}
	}
	for path, content := range files {
		r.write(path, content)
	}
	r.git("add", "--all")
	r.git("commit", "--quiet", "--allow-empty", "--message", "commit")
	return r.git("rev-parse", "HEAD")
}

func (r *repository) tag(name, sha string) {
	r.t.Helper()
	r.git("tag", "--annotate", "--message", name, name, sha)
}

func (r *repository) originMain(sha string) {
	r.t.Helper()
	r.git("update-ref", originMain, sha)
}

type attemptKey struct {
	id      int64
	attempt int
}

type fakeGitHub struct {
	runs          map[string][]Run
	attempts      map[attemptKey]Run
	releases      []string
	runsErr       map[string]error
	attemptErr    error
	releasesErr   error
	askedRuns     []string
	askedAttempts []attemptKey
}

func (f *fakeGitHub) Runs(_ context.Context, workflow, sha string) ([]Run, error) {
	f.askedRuns = append(f.askedRuns, workflow+"@"+sha)
	if err := f.runsErr[workflow]; err != nil {
		return nil, err
	}
	runs := slices.Clone(f.runs[workflow])
	for i := range runs {
		if runs[i].Title == "" {
			runs[i].Title = strings.TrimSuffix(workflow, ".yml")
		}
	}
	return runs, nil
}

func (f *fakeGitHub) Attempt(_ context.Context, id int64, attempt int) (Run, error) {
	key := attemptKey{id, attempt}
	f.askedAttempts = append(f.askedAttempts, key)
	return f.attempts[key], f.attemptErr
}

func (f *fakeGitHub) Releases(context.Context) ([]string, error) {
	return f.releases, f.releasesErr
}

func runOf(id int64, event, status, conclusion string, minutes int) Run {
	return Run{
		ID:         id,
		URL:        fmt.Sprintf("https://example.test/runs/%d", id),
		Event:      event,
		Attempt:    1,
		Status:     status,
		Conclusion: conclusion,
		Started:    clock.Add(time.Duration(minutes) * time.Minute),
	}
}

func done(id int64, event, conclusion string, minutes int) Run {
	return runOf(id, event, completed, conclusion, minutes)
}

func going(id int64, event string, attempt, minutes int) Run {
	r := runOf(id, event, "in_progress", "", minutes)
	r.Attempt = attempt
	return r
}

type world struct {
	t          *testing.T
	repo       *repository
	gh         *fakeGitHub
	tag        string
	files      map[string]string
	offMain    bool
	releaseTag string
	apiTag     string
	after      func(*repository)
}

const (
	here  = ""
	none  = "none"
	other = "other"
)

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{t: t, repo: newRepository(t), gh: &fakeGitHub{
		runs:     map[string][]Run{},
		attempts: map[attemptKey]Run{},
		runsErr:  map[string]error{},
	}}
	w.useTag(firstTag)
	w.gh.runs[ciWorkflow] = []Run{done(1, pushEvent, success, 0)}
	for _, file := range []string{"nightly.yml", nightlyHA, nightlyFleet} {
		w.gh.runs[file] = []Run{done(2, scheduleEvent, success, 0)}
	}
	return w
}

func (w *world) useTag(tag string) {
	w.tag = tag
	w.files = goodFiles(strings.TrimPrefix(tag, "v"))
}

func (w *world) check() (string, string, error) {
	w.t.Helper()
	earlier := w.repo.commit(w.files)
	sha := w.repo.commit(w.files)
	if w.offMain {
		w.repo.originMain(earlier)
	} else {
		w.repo.originMain(sha)
	}
	for name, mode := range map[string]string{w.tag: w.releaseTag, "api/" + w.tag: w.apiTag} {
		switch mode {
		case here:
			w.repo.tag(name, sha)
		case other:
			w.repo.tag(name, earlier)
		}
	}
	if w.after != nil {
		w.after(w.repo)
	}
	got, err := checkTag(w.t.Context(), w.repo.dir, w.gh, w.tag, sha)
	return sha, got, err
}

func (w *world) nightly(file string, runs ...Run) {
	w.gh.runs[file] = runs
}

func TestTheTagCheckAccepts(t *testing.T) {
	tests := []struct {
		name   string
		change func(*world)
	}{
		{name: "a pre-release with everything green", change: func(*world) {}},
		{name: "a stable release", change: func(w *world) { w.useTag("v0.1.0") }},
		{name: "a release above the published ones of its line", change: func(w *world) {
			w.gh.releases = []string{"v0.1.0-alpha.0"}
		}},
		{name: "a release of a new minor beside the published ones of an old", change: func(w *world) {
			w.useTag("v0.2.0-alpha.1")
			w.gh.releases = []string{"v0.1.0", "v0.1.9"}
		}},
		{name: "a release of a new major with the minor of a published one", change: func(w *world) {
			w.useTag("v1.1.0-alpha.1")
			w.gh.releases = []string{"v0.1.0"}
		}},
		{name: "a numeric prerelease above the one before it", change: func(w *world) {
			w.useTag("v0.1.0-alpha.10")
			w.gh.releases = []string{"v0.1.0-alpha.9"}
		}},
		{name: "a published release of a newer minor is not in the line", change: func(w *world) {
			w.gh.releases = []string{"v0.2.0"}
		}},
		{name: "a published release of a newer major with the same minor is not in the line", change: func(w *world) {
			w.gh.releases = []string{"v1.1.0"}
		}},
		{name: "published names that are not versions", change: func(w *world) {
			w.gh.releases = []string{"latest", "api/v0.3.0", "v0.1"}
		}},
		{name: "the install commands name the version with an equals sign", change: func(w *world) {
			w.files["README.md"] = "helm install lodia chart --version=0.1.0-alpha.1\n"
		}},
		{name: "the install commands end the file with the version", change: func(w *world) {
			w.files["docs/README.md"] = "helm install lodia chart --version 0.1.0-alpha.1"
		}},
		{name: "the install command of a longer version beside the right one", change: func(w *world) {
			w.files["README.md"] = installCommand("0.1.0-alpha.10") + installCommand("0.1.0-alpha.1")
		}},
		{name: "a run of a nightly in progress on its first attempt beside a green one", change: func(w *world) {
			w.nightly(nightlyHA, done(10, scheduleEvent, success, 0), going(11, dispatchEvent, 1, 10))
		}},
		{name: "a failed run, then a dispatched green one", change: func(w *world) {
			w.nightly(nightlyHA, done(10, scheduleEvent, "failure", 0), done(11, dispatchEvent, success, 10))
		}},
		{name: "a green run re-run and in progress", change: func(w *world) {
			re := going(10, scheduleEvent, 2, 10)
			w.nightly(nightlyHA, re)
			w.gh.attempts[attemptKey{10, 1}] = done(10, scheduleEvent, success, 0)
		}},
		{name: "a failed run re-run to green", change: func(w *world) {
			w.nightly(nightlyHA, done(10, scheduleEvent, success, 10))
		}},
		{name: "two runs started together, the later one green", change: func(w *world) {
			w.nightly(nightlyHA, done(10, scheduleEvent, "failure", 0), done(11, dispatchEvent, success, 0))
		}},
		{name: "a run of the fleet at its default size dispatched", change: func(w *world) {
			dispatched := done(10, dispatchEvent, success, 10)
			dispatched.Title = fleetTitle
			w.nightly(nightlyFleet, done(9, scheduleEvent, "failure", 0), dispatched)
		}},
		{name: "a run of another nightly dispatched at its default input", change: func(w *world) {
			w.files[".github/workflows/"+nightlyHA] = withInput("nightly-ha", "shards", "4", "nightly-ha, ${{ inputs.shards || '4' }} shards")
			dispatched := done(10, dispatchEvent, success, 10)
			dispatched.Title = "nightly-ha, 4 shards"
			w.nightly(nightlyHA, dispatched)
		}},
		{name: "a run of a pull request at the commit is no run of a nightly", change: func(w *world) {
			w.nightly(nightlyHA, done(10, scheduleEvent, success, 0), done(11, "pull_request", "failure", 10))
		}},
		{name: "a pull request's run of ci.yml at the commit is no run of main", change: func(w *world) {
			w.gh.runs[ciWorkflow] = append(w.gh.runs[ciWorkflow], done(12, "pull_request", "failure", 0))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			tt.change(w)

			sha, got, err := w.check()

			if err != nil {
				t.Errorf("want the tag accepted, got %v", err)
			}
			if got != sha {
				t.Errorf("checked %s, want %s", got, sha)
			}
		})
	}
}

func TestTheTagCheckRefuses(t *testing.T) {
	untagged := func(tag string) func(*world) {
		return func(w *world) { w.tag, w.releaseTag, w.apiTag = tag, none, none }
	}
	forbidden := errors.New("listing the runs of ci.yml answered 403: the token needs actions: read")
	failed := func(minutes int) Run { return done(10, scheduleEvent, "failure", minutes) }
	tests := []struct {
		name   string
		change func(*world)
		want   string
	}{
		{name: "a tag that is not a version", change: untagged("latest"), want: `"latest" is not a semantic version`},
		{name: "a tag with two components", change: untagged("v0.1"), want: "not a semantic version"},
		{name: "a tag without the v", change: untagged("0.1.0"), want: "not written vMAJOR.MINOR.PATCH"},
		{name: "a tag with a space", change: untagged(" v0.1.0"), want: "not written vMAJOR.MINOR.PATCH"},
		{name: "a tag with build metadata", change: untagged("v0.1.0+build.1"), want: "build metadata"},
		{name: "a commit off main", change: func(w *world) { w.offMain = true }, want: "is not on origin/main"},
		{name: "no tag of the release", change: func(w *world) { w.releaseTag = none }, want: "the tag v0.1.0-alpha.1 does not exist here"},
		{name: "a tag of the release at another commit", change: func(w *world) { w.releaseTag = other }, want: "the tag v0.1.0-alpha.1 is at"},
		{name: "no tag of the API module", change: func(w *world) { w.apiTag = none }, want: "the tag api/v0.1.0-alpha.1 does not exist here"},
		{name: "a tag of the API module at another commit", change: func(w *world) { w.apiTag = other }, want: "the tag api/v0.1.0-alpha.1 is at"},
		{name: "a tag of another version is no tag of the API module", change: func(w *world) {
			w.apiTag = none
			w.after = func(r *repository) { r.tag("api/v0.1.0-alpha.2", r.git("rev-parse", "HEAD")) }
		}, want: "the tag api/v0.1.0-alpha.1 does not exist here"},
		{name: "the tag of a published release", change: func(w *world) {
			w.gh.releases = []string{"v0.1.0-alpha.1"}
		}, want: "v0.1.0-alpha.1 is published in the line 0.1, and v0.1.0-alpha.1 is not above it"},
		{name: "a tag below a published release of its line", change: func(w *world) {
			w.gh.releases = []string{"v0.1.0-alpha.2"}
		}, want: "v0.1.0-alpha.2 is published in the line 0.1, and v0.1.0-alpha.1 is not above it"},
		{name: "a numeric prerelease below the one before it", change: func(w *world) {
			w.useTag("v0.1.0-alpha.2")
			w.gh.releases = []string{"v0.1.0-alpha.10"}
		}, want: "v0.1.0-alpha.10 is published in the line 0.1"},
		{name: "a pre-release of a line whose stable release is published", change: func(w *world) {
			w.gh.releases = []string{"v0.1.0"}
		}, want: "v0.1.0 is published in the line 0.1"},
		{name: "a tag below a published patch", change: func(w *world) {
			w.useTag("v0.1.0")
			w.gh.releases = []string{"v0.1.1"}
		}, want: "v0.1.1 is published in the line 0.1"},
		{name: "the releases that cannot be listed", change: func(w *world) {
			w.gh.releasesErr = errors.New("listing the releases: boom")
		}, want: "listing the releases"},
		{name: "a run of ci.yml that failed", change: func(w *world) {
			w.gh.runs[ciWorkflow] = []Run{done(1, pushEvent, "failure", 0)}
		}, want: "runs/1 ended as failure"},
		{name: "a run of ci.yml that was cancelled", change: func(w *world) {
			w.gh.runs[ciWorkflow] = []Run{done(1, pushEvent, "cancelled", 0)}
		}, want: "ended as cancelled"},
		{name: "a run of ci.yml that was skipped", change: func(w *world) {
			w.gh.runs[ciWorkflow] = []Run{done(1, pushEvent, "skipped", 0)}
		}, want: "ended as skipped"},
		{name: "a run of ci.yml still going", change: func(w *world) {
			w.gh.runs[ciWorkflow] = []Run{going(1, pushEvent, 1, 0)}
		}, want: "runs/1 is in_progress"},
		{name: "a run of ci.yml still queued", change: func(w *world) {
			w.gh.runs[ciWorkflow] = []Run{runOf(1, pushEvent, "queued", "", 0)}
		}, want: "is queued"},
		{name: "no run of ci.yml at all", change: func(w *world) { w.gh.runs[ciWorkflow] = nil }, want: "ci.yml has no run for the push"},
		{name: "only a pull request's run of ci.yml", change: func(w *world) {
			w.gh.runs[ciWorkflow] = []Run{done(1, "pull_request", success, 0)}
		}, want: "ci.yml has no run for the push"},
		{name: "one run of ci.yml green and one not", change: func(w *world) {
			w.gh.runs[ciWorkflow] = append(w.gh.runs[ciWorkflow], done(2, pushEvent, "failure", 0))
		}, want: "runs/2 ended as failure"},
		{name: "a 403 from GitHub", change: func(w *world) { w.gh.runsErr[ciWorkflow] = forbidden }, want: "answered 403"},
		{name: "no nightly workflow at the commit", change: func(w *world) {
			for _, file := range []string{"nightly.yml", nightlyHA, nightlyFleet} {
				delete(w.files, ".github/workflows/"+file)
			}
		}, want: "no .github/workflows/nightly*.yml at"},
		{name: "a nightly with no run", change: func(w *world) { w.nightly(nightlyHA) }, want: "nightly-ha.yml has no completed run on main at"},
		{name: "a nightly with no run names the dispatch", change: func(w *world) { w.nightly(nightlyHA) }, want: "gh workflow run nightly-ha.yml --ref main"},
		{name: "a nightly whose run failed", change: func(w *world) { w.nightly(nightlyHA, failed(0)) }, want: "runs/10, the newest run of nightly-ha.yml at"},
		{name: "a nightly whose run failed names the re-run", change: func(w *world) { w.nightly(nightlyHA, failed(0)) }, want: "gh run rerun 10"},
		{name: "a nightly whose run was cancelled", change: func(w *world) {
			w.nightly(nightlyHA, done(10, scheduleEvent, "cancelled", 0))
		}, want: "ended as cancelled"},
		{name: "a nightly whose run was skipped", change: func(w *world) {
			w.nightly(nightlyHA, done(10, scheduleEvent, "skipped", 0))
		}, want: "ended as skipped"},
		{name: "a nightly whose run timed out", change: func(w *world) {
			w.nightly(nightlyHA, done(10, dispatchEvent, "timed_out", 0))
		}, want: "ended as timed_out"},
		{name: "a nightly with a run in progress on its first attempt alone", change: func(w *world) {
			w.nightly(nightlyHA, going(10, dispatchEvent, 1, 0))
		}, want: "nightly-ha.yml has no completed run"},
		{name: "a nightly with a queued run alone", change: func(w *world) {
			w.nightly(nightlyHA, runOf(10, scheduleEvent, "queued", "", 0))
		}, want: "nightly-ha.yml has no completed run"},
		{name: "a nightly with a pull request's run alone", change: func(w *world) {
			w.nightly(nightlyHA, done(10, "pull_request", success, 0))
		}, want: "nightly-ha.yml has no completed run"},
		{name: "a nightly with a green run, then a failed one", change: func(w *world) {
			w.nightly(nightlyHA, done(10, scheduleEvent, success, 0), done(11, dispatchEvent, "failure", 10))
		}, want: "runs/11, the newest run"},
		{name: "a nightly with a failed run in progress on its first attempt beside a failed one", change: func(w *world) {
			w.nightly(nightlyHA, failed(0), going(11, dispatchEvent, 1, 10))
		}, want: "runs/10, the newest run"},
		{name: "a nightly with a failed run re-run and in progress", change: func(w *world) {
			w.nightly(nightlyHA, going(10, scheduleEvent, 2, 10))
			w.gh.attempts[attemptKey{10, 1}] = failed(0)
		}, want: "runs/10, the newest run of nightly-ha.yml at"},
		{name: "a nightly with a failed run on its third attempt in progress", change: func(w *world) {
			w.nightly(nightlyHA, going(10, scheduleEvent, 3, 10))
			w.gh.attempts[attemptKey{10, 2}] = failed(5)
			w.gh.attempts[attemptKey{10, 1}] = done(10, scheduleEvent, success, 0)
		}, want: "ended as failure"},
		{name: "a nightly with a green dispatch beside a failed run that is re-run", change: func(w *world) {
			w.nightly(nightlyHA, going(10, scheduleEvent, 2, 30), done(11, dispatchEvent, success, 20))
			w.gh.attempts[attemptKey{10, 1}] = failed(0)
		}, want: "runs/10, the newest run"},
		{name: "two nightly runs started together, the later one failed", change: func(w *world) {
			w.nightly(nightlyHA, done(10, scheduleEvent, success, 0), done(11, dispatchEvent, "failure", 0))
		}, want: "runs/11, the newest run"},
		{name: "a nightly whose re-run cannot be read", change: func(w *world) {
			w.nightly(nightlyHA, going(10, scheduleEvent, 2, 10))
			w.gh.attemptErr = errors.New("reading attempt 1 of the run 10 answered 403")
		}, want: "reading attempt 1 of the run 10 answered 403"},
		{name: "a nightly whose runs cannot be listed", change: func(w *world) {
			w.gh.runsErr[nightlyHA] = errors.New("listing the runs of nightly-ha.yml answered 403")
		}, want: "listing the runs of nightly-ha.yml answered 403"},
		{name: "a run of the fleet dispatched at another size", change: func(w *world) {
			dispatched := done(10, dispatchEvent, success, 10)
			dispatched.Title = "nightly-fleet, 1000 tenants"
			w.nightly(nightlyFleet, dispatched)
		}, want: `nightly-fleet.yml has no completed run on main at`},
		{name: "a run of the fleet dispatched at another size names its title", change: func(w *world) {
			dispatched := done(10, dispatchEvent, success, 10)
			dispatched.Title = "nightly-fleet, 1000 tenants"
			w.nightly(nightlyFleet, dispatched)
		}, want: `1 dispatched with a title other than "nightly-fleet, 300 tenants"`},
		{name: "a run of the fleet at another size does not hide a failed one", change: func(w *world) {
			dispatched := done(11, dispatchEvent, success, 10)
			dispatched.Title = "nightly-fleet, 1000 tenants"
			w.nightly(nightlyFleet, done(10, scheduleEvent, "failure", 0), dispatched)
		}, want: "runs/10, the newest run of nightly-fleet.yml"},
		{name: "a dispatched run of a nightly titled by another name", change: func(w *world) {
			dispatched := done(10, dispatchEvent, success, 10)
			dispatched.Title = "something else"
			w.nightly(nightlyHA, dispatched)
		}, want: "nightly-ha.yml has no completed run"},
		{name: "a nightly whose run-name holds another expression", change: func(w *world) {
			w.files[".github/workflows/"+nightlyHA] = "name: nightly-ha\nrun-name: nightly-ha ${{ github.actor }}\n"
		}, want: "an expression other than"},
		{name: "a nightly that gains an input and no run-name", change: func(w *world) {
			w.files[".github/workflows/"+nightlyHA] = withInput("nightly-ha", "shards", "4", "")
		}, want: "the input shards of workflow_dispatch is not stated in the run-name"},
		{name: "a nightly that gains an input its run-name does not state", change: func(w *world) {
			w.files[".github/workflows/"+nightlyHA] = withInput("nightly-ha", "shards", "4", "nightly-ha, ${{ inputs.other || '4' }} shards")
		}, want: "the input shards of workflow_dispatch is not stated in the run-name"},
		{name: "a nightly whose run-name states an input with another default", change: func(w *world) {
			w.files[".github/workflows/"+nightlyHA] = withInput("nightly-ha", "shards", "4", "nightly-ha, ${{ inputs.shards || '8' }} shards")
		}, want: "as `${{ inputs.shards || '4' }}`"},
		{name: "a run of another nightly dispatched at another input", change: func(w *world) {
			w.files[".github/workflows/"+nightlyHA] = withInput("nightly-ha", "shards", "4", "nightly-ha, ${{ inputs.shards || '4' }} shards")
			dispatched := done(10, dispatchEvent, success, 10)
			dispatched.Title = "nightly-ha, 8 shards"
			w.nightly(nightlyHA, dispatched)
		}, want: "nightly-ha.yml has no completed run"},
		{name: "a chart that is not at 0.0.0-dev", change: func(w *world) {
			w.files[chartFile] = "apiVersion: v2\nname: lodia\nversion: 0.1.0-alpha.1\nappVersion: 0.0.0-dev\n"
		}, want: `has version "0.1.0-alpha.1" and appVersion "0.0.0-dev"`},
		{name: "a chart whose appVersion is not 0.0.0-dev", change: func(w *world) {
			w.files[chartFile] = "apiVersion: v2\nname: lodia\nversion: 0.0.0-dev\nappVersion: 0.1.0-alpha.1\n"
		}, want: `and appVersion "0.1.0-alpha.1"`},
		{name: "a README that does not name the version", change: func(w *world) {
			w.files["README.md"] = "helm install lodia charts/lodia\n"
		}, want: "README.md at"},
		{name: "a README that does not name the version names the fix", change: func(w *world) {
			w.files["README.md"] = "helm install lodia charts/lodia\n"
		}, want: "does not name `--version 0.1.0-alpha.1`"},
		{name: "a docs README that does not name the version", change: func(w *world) {
			w.files["docs/README.md"] = "helm install lodia charts/lodia\n"
		}, want: "docs/README.md at"},
		{name: "a README that names the version before the previous one", change: func(w *world) {
			w.files["README.md"] = installCommand("0.1.0-alpha.0")
		}, want: "README.md at"},
		{name: "a README that names a longer version", change: func(w *world) {
			w.files["README.md"] = installCommand("0.1.0-alpha.10")
		}, want: "README.md at"},
		{name: "a README that names the version without the flag", change: func(w *world) {
			w.files["README.md"] = "version 0.1.0-alpha.1\n"
		}, want: "README.md at"},
		{name: "a README that names the tag with its v", change: func(w *world) {
			w.files["README.md"] = "helm install lodia chart --version v0.1.0-alpha.1\n"
		}, want: "README.md at"},
		{name: "a docs README that is gone", change: func(w *world) { delete(w.files, "docs/README.md") }, want: "docs/README.md"},
		{name: "an operations doc that does not name the version", change: func(w *world) {
			w.files["docs/operations.md"] = "## Upgrading Lodia\n\nhelm upgrade lodia oci://ghcr.io/lodia-io/charts/lodia -n lodia-system\n"
		}, want: "docs/operations.md at"},
		{name: "an operations doc that does not name the version names the fix", change: func(w *world) {
			w.files["docs/operations.md"] = "helm upgrade\n"
		}, want: "does not name `--version 0.1.0-alpha.1`: bump its install and upgrade commands"},
		{name: "an operations doc that names the previous version", change: func(w *world) {
			w.files["docs/operations.md"] = upgradeCommand("0.1.0-alpha.0")
		}, want: "docs/operations.md at"},
		{name: "an operations doc that names a longer version", change: func(w *world) {
			w.files["docs/operations.md"] = upgradeCommand("0.1.0-alpha.10")
		}, want: "docs/operations.md at"},
		{name: "an operations doc that is gone", change: func(w *world) { delete(w.files, "docs/operations.md") }, want: "docs/operations.md"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			tt.change(w)

			_, got, err := w.check()

			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("want a refusal containing %q, got %v", tt.want, err)
			}
			if got != "" && !strings.Contains(err.Error(), got) {
				t.Errorf("want the refusal to name the commit %s, got %v", got, err)
			}
		})
	}
}

func TestTheTagCheckNamesEveryRuleTheTagBreaks(t *testing.T) {
	w := newWorld(t)
	w.releaseTag, w.apiTag = none, none
	w.gh.runs[ciWorkflow] = []Run{done(1, pushEvent, "failure", 0)}
	w.nightly(nightlyHA)
	w.nightly(nightlyFleet, done(3, scheduleEvent, "failure", 0))
	w.files[chartFile] = "apiVersion: v2\nname: lodia\nversion: 0.1.0-alpha.1\nappVersion: 0.1.0-alpha.1\n"
	w.files["README.md"] = "no install command\n"
	w.files["docs/operations.md"] = "no upgrade command\n"
	w.gh.releases = []string{"v0.1.0-alpha.1"}
	w.offMain = true

	_, _, err := w.check()

	if err == nil {
		t.Fatal("want a refusal")
	}
	for _, want := range []string{
		"is not on origin/main",
		"the tag v0.1.0-alpha.1 does not exist here",
		"the tag api/v0.1.0-alpha.1 does not exist here",
		"is published in the line 0.1",
		"ci.yml is not green",
		"nightly-ha.yml has no completed run",
		"runs/3, the newest run of nightly-fleet.yml",
		"has version",
		"README.md at",
		"docs/operations.md at",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("want the refusal to contain %q, got:\n%v", want, err)
		}
	}
	for _, unwanted := range []string{"nightly.yml has no", "docs/README.md at"} {
		if strings.Contains(err.Error(), unwanted) {
			t.Errorf("want the refusal not to contain %q, got:\n%v", unwanted, err)
		}
	}
}

func TestTheNightliesAskedForAreTheFilesOfTheCommit(t *testing.T) {
	w := newWorld(t)
	w.files[".github/workflows/weekly.yml"] = "name: weekly\n"
	w.files[".github/workflows/nightly.txt"] = "name: nightly-txt\n"
	w.files[".github/workflows/mynightly.yml"] = "name: mynightly\n"
	w.after = func(r *repository) {
		files := goodFiles("0.1.0-alpha.1")
		files[".github/workflows/nightly-later.yml"] = "name: nightly-later\n"
		delete(files, ".github/workflows/"+nightlyHA)
		r.commit(files)
	}

	sha, _, err := w.check()

	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, file := range []string{ciWorkflow, nightlyFleet, nightlyHA, "nightly.yml"} {
		want = append(want, file+"@"+sha)
	}
	got := slices.Clone(w.gh.askedRuns)
	slices.Sort(got)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("the runs asked for (-want +got):\n%s", diff)
	}
}

func TestARunInProgressOnALaterAttemptIsCountedAsTheAttemptBefore(t *testing.T) {
	w := newWorld(t)
	w.nightly(nightlyHA, going(10, scheduleEvent, 4, 10))
	w.gh.attempts[attemptKey{10, 3}] = done(10, scheduleEvent, success, 5)

	_, _, err := w.check()

	if err != nil {
		t.Error(err)
	}
	if diff := cmp.Diff([]attemptKey{{10, 3}}, w.gh.askedAttempts, cmp.AllowUnexported(attemptKey{})); diff != "" {
		t.Errorf("the attempts asked for (-want +got):\n%s", diff)
	}
}

func TestACommitThatDoesNotExistIsRefused(t *testing.T) {
	w := newWorld(t)
	w.repo.commit(w.files)

	_, err := checkTag(t.Context(), w.repo.dir, w.gh, firstTag, "nonesuch")

	if err == nil || !strings.Contains(err.Error(), `commit "nonesuch"`) {
		t.Errorf("want the commit named, got %v", err)
	}
}

func TestACheckWithAnExpiredDeadlineRefusesInsteadOfAccepting(t *testing.T) {
	w := newWorld(t)
	sha := w.repo.commit(w.files)
	w.repo.originMain(sha)
	w.repo.tag(w.tag, sha)
	w.repo.tag("api/"+w.tag, sha)
	ctx, cancel := context.WithTimeout(t.Context(), -time.Second)
	defer cancel()

	_, err := checkTag(ctx, w.repo.dir, w.gh, w.tag, sha)

	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Errorf("want the deadline named, got %v", err)
	}
}

func TestACheckWithoutOriginMainSaysToFetchIt(t *testing.T) {
	w := newWorld(t)
	sha := w.repo.commit(w.files)

	_, err := checkTag(t.Context(), w.repo.dir, w.gh, firstTag, sha)

	if err == nil || !strings.Contains(err.Error(), "origin/main does not exist here: fetch it first") {
		t.Errorf("want the fetch named, got %v", err)
	}
}

func TestANightlyIsReadWithItsDefaultTitle(t *testing.T) {
	tests := []struct {
		name, raw, want, wantErr string
	}{
		{name: "a name alone", raw: "name: nightly-ha\n", want: "nightly-ha"},
		{name: "a run-name without an expression", raw: "name: nightly-ha\nrun-name: nightly, ha\n", want: "nightly, ha"},
		{name: "a run-name with an input and its default", raw: "name: f\nrun-name: f, ${{ inputs.tenants || '300' }} tenants\n", want: "f, 300 tenants"},
		{name: "a run-name with two inputs", raw: "name: f\nrun-name: ${{inputs.a||'1'}}-${{ inputs.b-c || 'x y' }}\n", want: "1-x y"},
		{name: "a run-name with another expression", raw: "name: f\nrun-name: f ${{ github.actor }}\n", wantErr: "expression other than"},
		{name: "an input stated with its default", raw: withInput("f", "tenants", "300", "f, ${{ inputs.tenants || '300' }} tenants"), want: "f, 300 tenants"},
		{name: "an input stated with a default written as a number", raw: "name: f\non:\n  workflow_dispatch:\n    inputs:\n      tenants:\n        default: 300\nrun-name: f, ${{ inputs.tenants || '300' }}\n", want: "f, 300"},
		{name: "an input with no default stated with an empty one", raw: "name: f\non:\n  workflow_dispatch:\n    inputs:\n      tenants: {}\nrun-name: f${{ inputs.tenants || '' }}\n", want: "f"},
		{name: "a boolean input stated with its default", raw: "name: f\non:\n  workflow_dispatch:\n    inputs:\n      quick:\n        type: boolean\n        default: true\nrun-name: f, quick ${{ inputs.quick || 'true' }}\n", wantErr: "the input quick of workflow_dispatch is of type boolean"},
		{name: "a number input stated with its default", raw: "name: f\non:\n  workflow_dispatch:\n    inputs:\n      tenants:\n        type: number\n        default: 300\nrun-name: f, ${{ inputs.tenants || '300' }} tenants\n", wantErr: "the input tenants of workflow_dispatch is of type number"},
		{name: "a boolean input the run-name does not state", raw: "name: f\non:\n  workflow_dispatch:\n    inputs:\n      quick:\n        type: boolean\n        default: true\n", wantErr: "the input quick of workflow_dispatch is of type boolean"},
		{name: "a string input stated with its default", raw: "name: f\non:\n  workflow_dispatch:\n    inputs:\n      tenants:\n        type: string\n        default: '300'\nrun-name: f, ${{ inputs.tenants || '300' }} tenants\n", want: "f, 300 tenants"},
		{name: "a choice input stated with its default", raw: "name: f\non:\n  workflow_dispatch:\n    inputs:\n      size:\n        type: choice\n        options: [small, large]\n        default: small\nrun-name: f, ${{ inputs.size || 'small' }}\n", want: "f, small"},
		{name: "a boolean input beside a string input stated with its default", raw: "name: f\non:\n  workflow_dispatch:\n    inputs:\n      quick:\n        type: boolean\n      tenants:\n        default: '300'\nrun-name: f, ${{ inputs.tenants || '300' }}\n", wantErr: "the input quick of workflow_dispatch is of type boolean"},
		{name: "two inputs, both stated", raw: "name: f\non:\n  workflow_dispatch:\n    inputs:\n      a:\n        default: '1'\n      b:\n        default: 'x'\nrun-name: ${{ inputs.a || '1' }}-${{ inputs.b || 'x' }}\n", want: "1-x"},
		{name: "an input in a workflow with no run-name", raw: withInput("f", "tenants", "300", ""), wantErr: "the input tenants of workflow_dispatch is not stated in the run-name as `${{ inputs.tenants || '300' }}`"},
		{name: "an input the run-name does not state", raw: withInput("f", "tenants", "300", "f"), wantErr: "the input tenants of workflow_dispatch is not stated"},
		{name: "an input stated with another default", raw: withInput("f", "tenants", "300", "f ${{ inputs.tenants || '100' }}"), wantErr: "as `${{ inputs.tenants || '300' }}`"},
		{name: "two inputs, one stated", raw: "name: f\non:\n  workflow_dispatch:\n    inputs:\n      a:\n        default: '1'\n      b:\n        default: 'x'\nrun-name: ${{ inputs.a || '1' }}\n", wantErr: "the input b of workflow_dispatch is not stated"},
		{name: "an input in a workflow whose on key is quoted", raw: "name: f\n\"on\":\n  workflow_dispatch:\n    inputs:\n      tenants:\n        default: '300'\n", wantErr: "the input tenants of workflow_dispatch is not stated"},
		{name: "a run-name that states an input the workflow does not declare", raw: "name: f\non:\n  workflow_dispatch:\nrun-name: f${{ inputs.tenants || '300' }}\n", want: "f300"},
		{name: "a dispatch with no inputs and the events as a list", raw: "name: f\non: [workflow_dispatch]\n", want: "f"},
		{name: "a dispatch with no inputs and the events as a word", raw: "name: f\non: workflow_dispatch\n", want: "f"},
		{name: "a run-name with an input and no default", raw: "name: f\nrun-name: f ${{ inputs.tenants }}\n", wantErr: "expression other than"},
		{name: "no name", raw: "on: push\n", wantErr: "no name"},
		{name: "not YAML", raw: "name: [\n", wantErr: "yaml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseNightly("f.yml", []byte(tt.raw))

			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("want an error containing %q, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil || got.title != tt.want || got.file != "f.yml" {
				t.Errorf("got %+v and %v, want the title %q", got, err, tt.want)
			}
		})
	}
}

func TestEveryNightlyOfTheRepositoryIsReadAndTheFleetsTitleStatesItsDefaultSize(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", workflowsDir, nightlyGlob))
	if err != nil || len(files) == 0 {
		t.Fatalf("found %v and %v", files, err)
	}
	titles := map[string]string{}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		n, err := parseNightly(filepath.Base(file), raw)
		if err != nil {
			t.Errorf("%s: %v", file, err)
		}
		titles[n.file] = n.title
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", workflowsDir, nightlyFleet))
	if err != nil {
		t.Fatal(err)
	}
	var fleet struct {
		On struct {
			Dispatch struct {
				Inputs struct {
					Tenants struct {
						Default string `json:"default"`
					} `json:"tenants"`
				} `json:"inputs"`
			} `json:"workflow_dispatch"`
		} `json:"true"`
	}
	if err := yaml.Unmarshal(raw, &fleet); err != nil {
		t.Fatal(err)
	}
	size := fleet.On.Dispatch.Inputs.Tenants.Default
	if size == "" {
		t.Fatal("the fleet's input tenants has no default")
	}
	if want := "nightly-fleet, " + size + " tenants"; titles[nightlyFleet] != want {
		t.Errorf("the title of the fleet at its defaults is %q, want %q", titles[nightlyFleet], want)
	}
	if want := "${TENANTS:-" + size + "}"; !strings.Contains(string(raw), want) {
		t.Errorf("the fleet's step does not fall back to %s", want)
	}
	for file, title := range titles {
		if file != nightlyFleet && title != strings.TrimSuffix(file, ".yml") {
			t.Errorf("%s has the title %q, want its name", file, title)
		}
	}
}
