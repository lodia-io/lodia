// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package tunnel implements lodia-tunnel, which carries a tenant's in-cluster
// API traffic through its Gateway: Server beside each API server, Address and
// Proxy on each node.
package tunnel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"syscall"
	"time"
)

const (
	acceptBackoffMin = 5 * time.Millisecond
	acceptBackoffMax = time.Second
)

var keepAlive = net.KeepAliveConfig{Enable: true, Idle: 30 * time.Second, Interval: 10 * time.Second, Count: 3}

func listenTCP(ctx context.Context, address string, control func(network, address string, c syscall.RawConn) error) (net.Listener, error) {
	lc := net.ListenConfig{KeepAliveConfig: keepAlive, Control: control}
	return lc.Listen(ctx, "tcp", address)
}

func tcpDialer() *net.Dialer {
	return &net.Dialer{KeepAliveConfig: keepAlive}
}

// accept calls admit, on this goroutine, for each connection of ln, until ctx
// is done or the listener fails; it leaves ln closed.
func accept(ctx context.Context, ln net.Listener, log *slog.Logger, admit func(net.Conn)) error {
	stop := context.AfterFunc(ctx, func() { _ = ln.Close() })
	defer stop()
	defer func() { _ = ln.Close() }()
	var backoff time.Duration
	for {
		c, err := ln.Accept()
		switch {
		case err == nil:
			backoff = 0
			admit(c)
		case ctx.Err() != nil:
			return nil
		case !transientAcceptError(err):
			return fmt.Errorf("accept on %s: %w", ln.Addr(), err)
		default:
			backoff = min(max(2*backoff, acceptBackoffMin), acceptBackoffMax)
			log.WarnContext(ctx, "accept failed, retrying", "listen", ln.Addr().String(), "backoff", backoff, "error", err)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func transientAcceptError(err error) bool {
	return errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) ||
		errors.Is(err, syscall.ENOBUFS) || errors.Is(err, syscall.ENOMEM)
}

// recovered keeps a panic in one connection's handling from ending the
// process.
func recovered(ctx context.Context, log *slog.Logger, handle func()) {
	defer func() {
		if r := recover(); r != nil {
			log.ErrorContext(ctx, "connection panicked", "panic", r, "stack", string(debug.Stack()))
		}
	}()
	handle()
}
