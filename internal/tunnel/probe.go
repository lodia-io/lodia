// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"time"
)

const probeTimeout = time.Second

// listenSocket replaces whatever file is at path: an emptyDir outlives a
// container, and a leftover socket would otherwise crash-loop the next one.
func listenSocket(ctx context.Context, path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return (&net.ListenConfig{}).Listen(ctx, "unix", path)
}

func answerProbe(c net.Conn) {
	_, _ = c.Write([]byte{'k'})
	_ = c.Close()
}

// Probe reports whether the socket of a Proxy answers, which it does while the
// Proxy's accept loop runs.
func Probe(ctx context.Context, socket string) error {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	c, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	deadline, _ := ctx.Deadline()
	_ = c.SetDeadline(deadline)
	_, err = io.ReadFull(c, make([]byte, 1))
	return err
}
