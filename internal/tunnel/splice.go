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
	"sync/atomic"
	"syscall"
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

// splice copies both ways between the connection a hop accepted, downstream
// (the pod's for proxy, the tunnel session for serve), and the one it dialed,
// upstream (the tunnel or the API server for proxy, the API server for serve),
// until both directions end, one fails or ctx is done, passing a half-close on,
// and closes both sides. It returns the first failure. A reset, a broken pipe
// or a half-close on a gone socket is none on the downstream leg, where the
// peer chose to end its connection and a hop further down logs its own cause,
// and none on the upstream leg once a direction has ended cleanly, which is how
// a TLS session ends against a peer that closed first.
func splice(ctx context.Context, log *slog.Logger, downstream, upstream conn) error {
	var once sync.Once
	var failure error
	var ended atomic.Bool
	finish := func(err error) {
		once.Do(func() {
			failure = err
			_ = downstream.Close()
			_ = upstream.Close()
		})
	}
	stop := context.AfterFunc(ctx, func() { finish(nil) })
	defer stop()

	oneWay := func(dst, src conn, srcIsDownstream bool) {
		srcFailed, err := false, errSplicePanicked
		recovered(ctx, log, func() { srcFailed, err = copyThenCloseWrite(dst, src, &ended) })
		downstreamFailed := srcFailed == srcIsDownstream
		switch {
		case err == nil:
		case peerGone(err) && (downstreamFailed || ended.Load()):
			finish(nil)
		default:
			finish(err)
		}
	}
	var copies sync.WaitGroup
	copies.Go(func() { oneWay(upstream, downstream, true) })
	oneWay(downstream, upstream, false)
	copies.Wait()
	finish(nil)
	return failure
}

// copyThenCloseWrite hides ReadFrom and WriteTo from io.CopyBuffer: through
// them a TCP connection would copy with a 32 KiB buffer of its own. It reports
// whether a failure came from src.
func copyThenCloseWrite(dst, src conn, ended *atomic.Bool) (srcFailed bool, err error) {
	buf := buffers.Get().(*[]byte)
	defer buffers.Put(buf)
	var readErr error
	read := func(p []byte) (int, error) {
		n, err := src.Read(p)
		if !errors.Is(err, io.EOF) {
			readErr = err
		}
		return n, err
	}
	if _, err := io.CopyBuffer(struct{ io.Writer }{dst}, readerFunc(read), *buf); err != nil {
		return readErr != nil && errors.Is(err, readErr), err
	}
	ended.Store(true)
	return false, dst.CloseWrite()
}

type readerFunc func(p []byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

func peerGone(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ENOTCONN)
}
