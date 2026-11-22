// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-github/v92/github"
)

func githubAt(t *testing.T, handler http.HandlerFunc) *githubClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	base := server.URL + "/"
	gh, err := newGitHub("lodia-io", "lodia", "the-token", github.WithURLs(&base, &base))
	if err != nil {
		t.Fatal(err)
	}
	return gh
}

func TestTheRunsAreThoseOfTheWorkflowOnMainAtTheCommit(t *testing.T) {
	gh := githubAt(t, func(w http.ResponseWriter, r *http.Request) {
		if want := "/repos/lodia-io/lodia/actions/workflows/nightly-ha.yml/runs"; r.URL.Path != want {
			t.Errorf("asked for %s, want %s", r.URL.Path, want)
		}
		query := r.URL.Query()
		for key, want := range map[string]string{"branch": "main", "head_sha": "abc123", "per_page": "100"} {
			if got := query.Get(key); got != want {
				t.Errorf("asked with %s=%q, want %q", key, got, want)
			}
		}
		if got := r.Header.Get("Authorization"); got != "Bearer the-token" {
			t.Errorf("asked with Authorization %q", got)
		}
		_, _ = w.Write([]byte(`{"total_count": 2, "workflow_runs": [
			{"id": 1, "html_url": "https://example.test/runs/1", "event": "schedule", "display_title": "nightly-ha", "run_attempt": 1,
			 "status": "completed", "conclusion": "success", "run_started_at": "2026-11-22T05:30:00Z"},
			{"id": 2, "html_url": "https://example.test/runs/2", "event": "workflow_dispatch", "display_title": "nightly-ha", "run_attempt": 2,
			 "status": "in_progress", "conclusion": null, "run_started_at": "2026-11-22T09:00:00Z"}
		]}`))
	})

	got, err := gh.Runs(t.Context(), "nightly-ha.yml", "abc123")

	if err != nil {
		t.Fatal(err)
	}
	want := []Run{
		{ID: 1, URL: "https://example.test/runs/1", Event: "schedule", Title: "nightly-ha", Attempt: 1, Status: "completed", Conclusion: "success", Started: time.Date(2026, 11, 22, 5, 30, 0, 0, time.UTC)},
		{ID: 2, URL: "https://example.test/runs/2", Event: "workflow_dispatch", Title: "nightly-ha", Attempt: 2, Status: "in_progress", Started: time.Date(2026, 11, 22, 9, 0, 0, 0, time.UTC)},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("runs (-want +got):\n%s", diff)
	}
}

func TestAnAttemptIsTheRunsOfThatNumber(t *testing.T) {
	gh := githubAt(t, func(w http.ResponseWriter, r *http.Request) {
		if want := "/repos/lodia-io/lodia/actions/runs/42/attempts/2"; r.URL.Path != want {
			t.Errorf("asked for %s, want %s", r.URL.Path, want)
		}
		_, _ = w.Write([]byte(`{"id": 42, "html_url": "https://example.test/runs/42", "run_attempt": 2, "status": "completed", "conclusion": "failure"}`))
	})

	got, err := gh.Attempt(t.Context(), 42, 2)

	if err != nil {
		t.Fatal(err)
	}
	want := Run{ID: 42, URL: "https://example.test/runs/42", Attempt: 2, Status: "completed", Conclusion: "failure"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("attempt (-want +got):\n%s", diff)
	}
}

func TestTheRunsAndAttemptsNameThePermissionOnA403And404(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound} {
		gh := githubAt(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"message": "Resource not accessible by integration"}`))
		})

		_, runsErr := gh.Runs(t.Context(), "ci.yml", "abc123")
		_, attemptErr := gh.Attempt(t.Context(), 42, 1)

		for _, err := range []error{runsErr, attemptErr} {
			for _, want := range []string{fmt.Sprintf("answered %d", status), "actions: read on lodia-io/lodia"} {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("%d: want an error containing %q, got %v", status, want, err)
				}
			}
		}
	}
}

func TestTheRunsAndAttemptsReportAnyOtherFailure(t *testing.T) {
	gh := githubAt(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	_, runsErr := gh.Runs(t.Context(), "ci.yml", "abc123")
	_, attemptErr := gh.Attempt(t.Context(), 42, 1)

	if runsErr == nil || !strings.Contains(runsErr.Error(), "listing the runs of ci.yml") || strings.Contains(runsErr.Error(), "actions: read") {
		t.Errorf("want the plain error of the listing, got %v", runsErr)
	}
	if attemptErr == nil || !strings.Contains(attemptErr.Error(), "reading attempt 1 of the run 42") || strings.Contains(attemptErr.Error(), "actions: read") {
		t.Errorf("want the plain error of the attempt, got %v", attemptErr)
	}
}

func TestAnExpiredDeadlineIsAnErrorOfEveryCall(t *testing.T) {
	gh := githubAt(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"total_count": 0, "workflow_runs": []}`))
	})
	ctx, cancel := context.WithTimeout(t.Context(), -time.Second)
	defer cancel()

	_, runsErr := gh.Runs(ctx, "ci.yml", "abc123")
	_, attemptErr := gh.Attempt(ctx, 42, 1)
	_, releasesErr := gh.Releases(ctx)

	for _, err := range []error{runsErr, attemptErr, releasesErr} {
		if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
			t.Errorf("want the deadline named, got %v", err)
		}
	}
}

func TestTheRepositoryIsOrigins(t *testing.T) {
	for _, url := range []string{
		"git@github.com:lodia-io/lodia.git",
		"https://github.com/lodia-io/lodia",
		"https://github.com/lodia-io/lodia.git",
		"ssh://git@github.com/lodia-io/lodia.git",
		"https://x-access-token:token@github.com/lodia-io/lodia/",
	} {
		r := newRepository(t)
		r.git("remote", "add", "origin", url)

		owner, repo, err := originRepository(t.Context(), r.dir)

		if err != nil || owner != "lodia-io" || repo != "lodia" {
			t.Errorf("%s: got %q %q %v", url, owner, repo, err)
		}
	}

	r := newRepository(t)
	r.git("remote", "add", "origin", "https://gitlab.com/lodia-io/lodia.git")
	if _, _, err := originRepository(t.Context(), r.dir); err == nil {
		t.Error("an origin outside github.com: want a refusal")
	}
}

func TestTheReleasesAreThePublishedOnesOfEveryPage(t *testing.T) {
	var asked []string
	gh := githubAt(t, func(w http.ResponseWriter, r *http.Request) {
		if want := "/repos/lodia-io/lodia/releases"; r.URL.Path != want {
			t.Errorf("asked for %s, want %s", r.URL.Path, want)
		}
		asked = append(asked, r.URL.Query().Get("page"))
		switch r.URL.Query().Get("page") {
		case "", "1":
			w.Header().Set("Link", `<`+"http://"+r.Host+`/repos/lodia-io/lodia/releases?page=2>; rel="next"`)
			_, _ = w.Write([]byte(`[{"tag_name": "v0.1.0-alpha.3", "draft": true}, {"tag_name": "v0.1.0-alpha.2", "prerelease": true}]`))
		default:
			_, _ = w.Write([]byte(`[{"tag_name": "v0.1.0-alpha.1", "prerelease": true, "draft": false}]`))
		}
	})

	got, err := gh.Releases(t.Context())

	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]string{"v0.1.0-alpha.2", "v0.1.0-alpha.1"}, got); diff != "" {
		t.Errorf("releases (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"", "2"}, asked); diff != "" {
		t.Errorf("pages asked (-want +got):\n%s", diff)
	}
}

func TestTheReleasesReportAFailure(t *testing.T) {
	gh := githubAt(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	_, err := gh.Releases(t.Context())

	if err == nil || !strings.Contains(err.Error(), "listing the releases") {
		t.Errorf("want the error of the listing, got %v", err)
	}
}
