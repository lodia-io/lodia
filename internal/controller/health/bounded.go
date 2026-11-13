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
// of it fails before anything is decoded. A request that wants to know the cut
// carries a *bool under cutKey: client-go replaces the error of the read with
// the status error for a response outside 2xx.
func capResponses(next http.RoundTripper) http.RoundTripper {
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
