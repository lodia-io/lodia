// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

type closedNetwork struct{}

func (closedNetwork) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("the test closed the network")
}

func TestTheCommandRefusesATagThatIsNotAVersion(t *testing.T) {
	saved := http.DefaultTransport
	http.DefaultTransport = closedNetwork{}
	t.Cleanup(func() { http.DefaultTransport = saved })
	t.Setenv("GITHUB_TOKEN", "not-a-token")
	r := newRepository(t)
	r.git("remote", "add", "origin", "https://github.com/lodia-io/lodia.git")
	t.Chdir(r.dir)

	err := run(t.Context(), []string{"tag", "0.1.0"})

	if err == nil || !strings.Contains(err.Error(), "not written vMAJOR.MINOR.PATCH") {
		t.Errorf("want the tag refused, got %v", err)
	}
}

func TestTheCommandRefusesTheTagWithTheReasonsOfTheClone(t *testing.T) {
	saved := http.DefaultTransport
	http.DefaultTransport = closedNetwork{}
	t.Cleanup(func() { http.DefaultTransport = saved })
	t.Setenv("GITHUB_TOKEN", "not-a-token")
	r := newRepository(t)
	r.git("remote", "add", "origin", "https://github.com/lodia-io/lodia.git")
	r.originMain(r.commit(goodFiles("0.1.0-alpha.1")))
	t.Chdir(r.dir)

	err := run(t.Context(), []string{"tag", "--commit=HEAD", firstTag})

	for _, want := range []string{"the tag v0.1.0-alpha.1 does not exist here", "the tag api/v0.1.0-alpha.1 does not exist here", "the test closed the network"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("want a refusal containing %q, got %v", want, err)
		}
	}
}

func TestTheCommandTakesTheTagSubcommandAndOneTag(t *testing.T) {
	for _, args := range [][]string{nil, {"metadata"}, {"tag"}, {"tag", "v0.1.0", "v0.2.0"}} {
		if err := run(t.Context(), args); err == nil || !strings.Contains(err.Error(), "usage: releasecheck tag") {
			t.Errorf("%v: want the usage, got %v", args, err)
		}
	}
}
