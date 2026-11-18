// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantclient

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	// MaxResponseBytes is the longest response body Lodia reads from a tenant,
	// whose administrator writes the objects it reads.
	MaxResponseBytes = 8 << 20

	// MaxResponseHeaderBytes is the most bytes of status line and headers
	// Lodia reads of a response; net/http reads up to 10 MiB of them
	// otherwise. A kube-apiserver's are under 512 bytes (envtest, 1.35.0).
	MaxResponseHeaderBytes = 64 << 10

	// IdleConnections is how many connections a tenant client keeps idle
	// between passes (docs/operations.md, Capacity).
	IdleConnections = 1
)

var ErrResponseTooLong = fmt.Errorf("the response is longer than maxResponseBytes, %d bytes", MaxResponseBytes)

// capResponses cuts every body at MaxResponseBytes, so that the client's read
// of it fails before anything is decoded, and the headers of every response at
// MaxResponseHeaderBytes. A request that wants to know the cut carries a *bool
// under cutKey: client-go replaces the error of the read with the status error
// for a response outside 2xx. next is the transport client-go builds for this
// tenant alone, since its config sets a proxy, which it never caches. It has at
// most connections connections open, dialing, in use and idle: a read past that
// waits for one to close or be given back, at most its own deadline. It keeps
// one of them idle, which every pass of a tenant at rest reuses, and closes the
// others a held restore's pass dialed as it gives them back. A TLS handshake
// ends within timeout.
func capResponses(next http.RoundTripper, connections int, timeout time.Duration) http.RoundTripper {
	if transport, ok := next.(*http.Transport); ok {
		transport.MaxResponseHeaderBytes = MaxResponseHeaderBytes
		transport.MaxConnsPerHost = connections
		transport.MaxIdleConnsPerHost = IdleConnections
		transport.TLSHandshakeTimeout = timeout
	}
	return cappedTransport{next}
}

type (
	cappedTransport struct{ next http.RoundTripper }
	cutKey          struct{}
)

func (c cappedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := c.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	cut, _ := req.Context().Value(cutKey{}).(*bool)
	resp.Body = cappedBody{http.MaxBytesReader(nil, resp.Body, MaxResponseBytes), cut}
	return resp, nil
}

// WrappedRoundTripper lets utilnet.CloseIdleConnectionsFor reach the transport
// below.
func (c cappedTransport) WrappedRoundTripper() http.RoundTripper { return c.next }

type cappedBody struct {
	io.ReadCloser
	cut *bool
}

func (b cappedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if tooLong := new(http.MaxBytesError); errors.As(err, &tooLong) {
		err = ErrResponseTooLong
		if b.cut != nil {
			*b.cut = true
		}
	}
	return n, err
}
