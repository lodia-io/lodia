// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/version"
	"sigs.k8s.io/yaml"
)

const (
	originMain = "refs/remotes/origin/main"
	chartFile  = "charts/lodia/Chart.yaml"
	devVersion = "0.0.0-dev"

	completed = "completed"
	success   = "success"
)

var installDocs = []string{"README.md", "docs/README.md"}

type Run struct {
	ID         int64
	URL        string
	Event      string
	Title      string
	Attempt    int
	Status     string
	Conclusion string
	Started    time.Time
}

type GitHub interface {
	Runs(ctx context.Context, workflow, sha string) ([]Run, error)
	Attempt(ctx context.Context, id int64, attempt int) (Run, error)
	Releases(ctx context.Context) ([]string, error)
}

// checkTag names every rule that tag breaks at commit, not the first one.
func checkTag(ctx context.Context, dir string, gh GitHub, tag, commit string) (string, error) {
	v, err := parseTag(tag)
	if err != nil {
		return "", err
	}
	out, err := git(ctx, dir, "rev-parse", "--verify", "--end-of-options", commit+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("commit %q: %w", commit, err)
	}
	sha := strings.TrimSpace(string(out))

	if err := errors.Join(
		requireOnMain(ctx, dir, sha),
		requireTags(ctx, dir, tag, sha),
		requireAbovePublished(ctx, gh, v),
		requireGreenCI(ctx, gh, sha),
		requireGreenNightlies(ctx, dir, gh, sha),
		requireDevChart(ctx, dir, sha),
		requireInstallCommands(ctx, dir, sha, strings.TrimPrefix(tag, "v")),
	); err != nil {
		return sha, fmt.Errorf("%s may not be tagged on %s:\n%w", tag, sha, err)
	}
	return sha, nil
}

// A tag is released as an image tag and as the chart's version too, which have
// no place for build metadata, and the operator's revision appends its own
// "+".
func parseTag(tag string) (*version.Version, error) {
	v, err := version.ParseSemantic(tag)
	if err != nil {
		return nil, fmt.Errorf("%q is not a semantic version: %w", tag, err)
	}
	if tag != "v"+v.String() {
		return nil, fmt.Errorf("%q is not written vMAJOR.MINOR.PATCH[-PRERELEASE]", tag)
	}
	if v.BuildMetadata() != "" {
		return nil, fmt.Errorf("%q has build metadata, which an image tag cannot carry", tag)
	}
	return v, nil
}

func requireOnMain(ctx context.Context, dir, sha string) error {
	if _, err := git(ctx, dir, "rev-parse", "--verify", "--quiet", originMain); err != nil {
		return errors.New("origin/main does not exist here: fetch it first")
	}
	_, err := git(ctx, dir, "merge-base", "--is-ancestor", sha, originMain)
	if exit, ok := errors.AsType[*exec.ExitError](err); ok && exit.ExitCode() == 1 {
		return fmt.Errorf("%s is not on origin/main: tag a commit that is", sha)
	}
	return err
}

// A release has two tags at its commit: the release's, and the API module's
// under api/, as Go reads the tag of a module in a subdirectory:
// https://go.dev/ref/mod#vcs-version
func requireTags(ctx context.Context, dir, tag, sha string) error {
	var problems []error
	for _, of := range []struct{ name, what string }{{tag, "the release"}, {"api/" + tag, "the API module"}} {
		out, err := git(ctx, dir, "rev-parse", "--verify", "--quiet", "--end-of-options", "refs/tags/"+of.name+"^{commit}")
		if exit, ok := errors.AsType[*exec.ExitError](err); ok && exit.ExitCode() == 1 {
			problems = append(problems, fmt.Errorf("the tag %s does not exist here: tag %s at %s first", of.name, of.what, sha))
			continue
		}
		if err != nil {
			return err
		}
		if got := strings.TrimSpace(string(out)); got != sha {
			problems = append(problems, fmt.Errorf("the tag %s is at %s, not at %s: both tags of a release name one commit", of.name, got, sha))
		}
	}
	return errors.Join(problems...)
}

// A tag at or below a published release of its line is the tag an older commit
// gets, and the run of a tag is its own commit's release.yml.
func requireAbovePublished(ctx context.Context, gh GitHub, tag *version.Version) error {
	names, err := gh.Releases(ctx)
	if err != nil {
		return err
	}
	var newest *version.Version
	for _, name := range names {
		v, err := version.ParseSemantic(name)
		if err != nil {
			continue
		}
		if v.Major() == tag.Major() && v.Minor() == tag.Minor() && !v.LessThan(tag) && (newest == nil || v.GreaterThan(newest)) {
			newest = v
		}
	}
	if newest != nil {
		return fmt.Errorf("v%s is published in the line %d.%d, and v%s is not above it: tag the next version of the line", newest, tag.Major(), tag.Minor(), tag)
	}
	return nil
}

// The chart on main stays at 0.0.0-dev: the release writes its version into
// the packaged copy only.
func requireDevChart(ctx context.Context, dir, sha string) error {
	raw, err := git(ctx, dir, "show", sha+":"+chartFile)
	if err != nil {
		return err
	}
	var chart struct {
		Version    string `json:"version"`
		AppVersion string `json:"appVersion"`
	}
	if err := yaml.Unmarshal(raw, &chart); err != nil {
		return fmt.Errorf("%s at %s: %w", chartFile, sha, err)
	}
	if chart.Version != devVersion || chart.AppVersion != devVersion {
		return fmt.Errorf("%s at %s has version %q and appVersion %q, want %s: the release writes its version into the packaged copy only", chartFile, sha, chart.Version, chart.AppVersion, devVersion)
	}
	return nil
}

func requireInstallCommands(ctx context.Context, dir, sha, chartVersion string) error {
	install := regexp.MustCompile(`--version[=\s]+` + regexp.QuoteMeta(chartVersion) + `(?:[^0-9A-Za-z.+-]|$)`)
	var problems []error
	for _, doc := range installDocs {
		raw, err := git(ctx, dir, "show", sha+":"+doc)
		if err != nil {
			return err
		}
		if !install.Match(raw) {
			problems = append(problems, fmt.Errorf("%s at %s does not name `--version %s`: bump its install commands in the last pull request before the tag", doc, sha, chartVersion))
		}
	}
	return errors.Join(problems...)
}

func git(ctx context.Context, dir string, args ...string) ([]byte, error) {
	//nolint:gosec // G204: the arguments are git's subcommands, and the tag, commit and paths this program was given
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}
