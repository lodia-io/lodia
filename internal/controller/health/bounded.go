// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

// maxResponseBytes is the longest response body Health reads from a tenant,
// whose administrator writes the objects it lists.
const maxResponseBytes = 8 << 20

var errResponseTooLong = fmt.Errorf("the response is longer than maxResponseBytes, %d bytes", maxResponseBytes)

// capResponses cuts every body at maxResponseBytes, so that the client's read
// of it fails before anything is decoded.
func capResponses(next http.RoundTripper) http.RoundTripper {
	return cappedTransport{next}
}

type cappedTransport struct{ next http.RoundTripper }

func (c cappedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := c.next.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = cappedBody{http.MaxBytesReader(nil, resp.Body, maxResponseBytes)}
	return resp, nil
}

// WrappedRoundTripper lets utilnet.CloseIdleConnectionsFor reach the transport
// below.
func (c cappedTransport) WrappedRoundTripper() http.RoundTripper { return c.next }

type cappedBody struct{ io.ReadCloser }

func (b cappedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if tooLong := new(http.MaxBytesError); errors.As(err, &tooLong) {
		err = errResponseTooLong
	}
	return n, err
}
