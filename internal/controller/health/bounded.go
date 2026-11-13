// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

const (
	// maxResponseBytes is the longest response body Health reads from a tenant,
	// whose administrator writes the objects it lists.
	maxResponseBytes = 8 << 20

	// maxResponseHeaderBytes is the most bytes of status line and headers
	// Health reads of a response; net/http reads up to 10 MiB of them
	// otherwise. A kube-apiserver's are under 512 bytes (envtest, 1.35.0).
	maxResponseHeaderBytes = 64 << 10
)

var errResponseTooLong = fmt.Errorf("the response is longer than maxResponseBytes, %d bytes", maxResponseBytes)

// capResponses cuts every body at maxResponseBytes, so that the client's read
// of it fails before anything is decoded, and the headers of every response at
// maxResponseHeaderBytes. A request that wants to know the cut carries a *bool
// under cutKey: client-go replaces the error of the read with the status error
// for a response outside 2xx. next is the transport client-go builds for this
// tenant alone, since its config sets a proxy, which it never caches.
func capResponses(next http.RoundTripper) http.RoundTripper {
	if transport, ok := next.(*http.Transport); ok {
		transport.MaxResponseHeaderBytes = maxResponseHeaderBytes
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
	resp.Body = cappedBody{http.MaxBytesReader(nil, resp.Body, maxResponseBytes), cut}
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
		err = errResponseTooLong
		if b.cut != nil {
			*b.cut = true
		}
	}
	return n, err
}
