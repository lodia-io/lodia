// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
)

const bufferSize = 16 * 1024

var buffers = sync.Pool{New: func() any {
	buf := make([]byte, bufferSize)
	return &buf
}}

var errSplicePanicked = errors.New("splice panicked")

// conn is either side of a splice: a TCP connection or a TLS session.
type conn interface {
	net.Conn
	CloseWrite() error
}

// splice copies both ways until both directions end, one fails or ctx is done,
// passing a half-close on, and closes both sides. It returns the first
// failure.
func splice(ctx context.Context, log *slog.Logger, a, b conn) error {
	var once sync.Once
	var failure error
	finish := func(err error) {
		once.Do(func() {
			failure = err
			_ = a.Close()
			_ = b.Close()
		})
	}
	stop := context.AfterFunc(ctx, func() { finish(nil) })
	defer stop()

	oneWay := func(dst, src conn) {
		err := errSplicePanicked
		recovered(ctx, log, func() { err = copyThenCloseWrite(dst, src) })
		if err != nil {
			finish(err)
		}
	}
	var copies sync.WaitGroup
	copies.Go(func() { oneWay(b, a) })
	oneWay(a, b)
	copies.Wait()
	finish(nil)
	return failure
}

// copyThenCloseWrite hides ReadFrom and WriteTo from io.CopyBuffer: through
// them a TCP connection would copy with a 32 KiB buffer of its own.
func copyThenCloseWrite(dst, src conn) error {
	buf := buffers.Get().(*[]byte)
	defer buffers.Put(buf)
	if _, err := io.CopyBuffer(struct{ io.Writer }{dst}, struct{ io.Reader }{src}, *buf); err != nil {
		return err
	}
	return dst.CloseWrite()
}
