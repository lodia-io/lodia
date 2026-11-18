// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	utilnet "k8s.io/apimachinery/pkg/util/net"
)

type idleCloser struct {
	http.RoundTripper
	closed atomic.Bool
}

func (i *idleCloser) CloseIdleConnections() { i.closed.Store(true) }

func TestTheCapStillLetsAForgottenControlPlaneCloseItsIdleConnections(t *testing.T) {
	below := &idleCloser{}
	utilnet.CloseIdleConnectionsFor(capResponses(below, 1, time.Minute))
	if !below.closed.Load() {
		t.Error("CloseIdleConnectionsFor did not reach the transport below the cap")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestTheCapFailsABodyPastTheBoundAndSaysSoToTheRequestThatAsked(t *testing.T) {
	for _, c := range []struct {
		name    string
		size    int
		flagged bool
		wantCut bool
	}{
		{"a byte over, asked", MaxResponseBytes + 1, true, true},
		{"a byte over, not asked", MaxResponseBytes + 1, false, false},
		{"exactly the bound, asked", MaxResponseBytes, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			body := strings.NewReader(strings.Repeat("x", c.size))
			transport := capResponses(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(body)}, nil
			}), 1, time.Minute)
			var cut bool
			ctx := t.Context()
			if c.flagged {
				ctx = context.WithValue(ctx, cutKey{}, &cut)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://tenant", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := transport.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			read, err := io.ReadAll(resp.Body)
			if wantErr := c.size > MaxResponseBytes; wantErr != errors.Is(err, ErrResponseTooLong) || len(read) != min(c.size, MaxResponseBytes) {
				t.Errorf("read %d bytes and returned %v, want %d bytes and the error of the bound: %t", len(read), err, min(c.size, MaxResponseBytes), wantErr)
			}
			if cut != c.wantCut {
				t.Errorf("the request was told of the cut: %t, want %t", cut, c.wantCut)
			}
		})
	}
}
