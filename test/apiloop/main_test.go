// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const internalError = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"etcdserver: leader changed","code":500}`

func answering(t *testing.T, retryAfter string, status int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(internalError))
	}))
	t.Cleanup(server.Close)
	return server
}

func TestAFailedAnswerNamesItsBodyAndRetryAfter(t *testing.T) {
	server := answering(t, "", http.StatusInternalServerError)
	_, followed, err := get(t.Context(), server.Client(), server.URL, time.Second)
	if err == nil {
		t.Fatal("a 500 returned no error")
	}
	if followed != "" {
		t.Errorf("an answer without a Retry-After was followed: %q", followed)
	}
	for _, want := range []string{"status 500 Internal Server Error", `Retry-After ""`, `etcdserver: leader changed`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error %q does not name %q", err, want)
		}
	}
}

func TestAFollowedRetryAfterKeepsTheAnswerThatAskedForIt(t *testing.T) {
	server := answering(t, "0", http.StatusServiceUnavailable)
	_, followed, err := get(t.Context(), server.Client(), server.URL, time.Second)
	if err == nil {
		t.Fatal("two 503s returned no error")
	}
	for _, want := range []string{"status 503 Service Unavailable", `Retry-After "0"`, `etcdserver: leader changed`} {
		if !strings.Contains(followed, want) {
			t.Errorf("the followed answer %q does not name %q", followed, want)
		}
	}
}

func TestABodyLongerThanTheBoundIsCut(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(strings.Repeat("x", 2*maxFailedBody)))
	}))
	t.Cleanup(server.Close)
	_, _, err := get(t.Context(), server.Client(), server.URL, time.Second)
	if err == nil {
		t.Fatal("a 500 returned no error")
	}
	if got := strings.Count(err.Error(), "x"); got != maxFailedBody {
		t.Errorf("the error holds %d bytes of the body, want %d", got, maxFailedBody)
	}
}
