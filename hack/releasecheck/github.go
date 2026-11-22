// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/google/go-github/v92/github"
)

const branch = "main"

var origin = regexp.MustCompile(`github\.com[:/]([^/]+)/([^/]+?)(?:\.git)?/?$`)

type githubClient struct {
	client      *github.Client
	owner, repo string
}

func newGitHub(owner, repo, token string, opts ...github.ClientOptionsFunc) (*githubClient, error) {
	client, err := github.NewClient(append([]github.ClientOptionsFunc{github.WithAuthToken(token)}, opts...)...)
	if err != nil {
		return nil, err
	}
	return &githubClient{client: client, owner: owner, repo: repo}, nil
}

func originRepository(ctx context.Context, dir string) (owner, repo string, err error) {
	out, err := git(ctx, dir, "remote", "get-url", "origin")
	if err != nil {
		return "", "", err
	}
	url := strings.TrimSpace(string(out))
	m := origin.FindStringSubmatch(url)
	if m == nil {
		return "", "", fmt.Errorf("origin %q is not a github.com repository", url)
	}
	return m[1], m[2], nil
}

func (g *githubClient) Runs(ctx context.Context, workflow, sha string) ([]Run, error) {
	list, _, err := g.client.Actions.ListWorkflowRunsByFileName(ctx, g.owner, g.repo, workflow, &github.ListWorkflowRunsOptions{
		Branch:      branch,
		HeadSHA:     sha,
		ListOptions: github.ListOptions{PerPage: 100},
	})
	if err != nil {
		return nil, g.runsError("listing the runs of "+workflow, err)
	}
	runs := make([]Run, 0, len(list.WorkflowRuns))
	for _, run := range list.WorkflowRuns {
		runs = append(runs, toRun(run))
	}
	return runs, nil
}

func (g *githubClient) Attempt(ctx context.Context, id int64, attempt int) (Run, error) {
	run, _, err := g.client.Actions.GetWorkflowRunAttempt(ctx, g.owner, g.repo, id, attempt, nil)
	if err != nil {
		return Run{}, g.runsError(fmt.Sprintf("reading attempt %d of the run %d", attempt, id), err)
	}
	return toRun(run), nil
}

func (g *githubClient) Releases(ctx context.Context) ([]string, error) {
	var tags []string
	opts := &github.ListOptions{PerPage: 100}
	for {
		page, resp, err := g.client.Repositories.ListReleases(ctx, g.owner, g.repo, opts)
		if err != nil {
			return nil, fmt.Errorf("listing the releases: %w", err)
		}
		for _, release := range page {
			if !release.GetDraft() {
				tags = append(tags, release.GetTagName())
			}
		}
		if resp.NextPage == 0 {
			return tags, nil
		}
		opts.Page = resp.NextPage
	}
}

func toRun(run *github.WorkflowRun) Run {
	return Run{
		ID:         run.GetID(),
		URL:        run.GetHTMLURL(),
		Event:      run.GetEvent(),
		Title:      run.GetDisplayTitle(),
		Attempt:    run.GetRunAttempt(),
		Status:     run.GetStatus(),
		Conclusion: run.GetConclusion(),
		Started:    run.GetRunStartedAt().Time,
	}
}

func (g *githubClient) runsError(what string, err error) error {
	if refused, ok := errors.AsType[*github.ErrorResponse](err); ok && refused.Response != nil &&
		(refused.Response.StatusCode == http.StatusForbidden || refused.Response.StatusCode == http.StatusNotFound) {
		return fmt.Errorf("%s answered %d: the token needs actions: read on %s/%s: %w", what, refused.Response.StatusCode, g.owner, g.repo, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}
