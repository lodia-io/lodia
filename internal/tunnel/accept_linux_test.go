// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package tunnel

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func shutdownListener(t *testing.T, addr string) {
	t.Helper()
	_, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		listening, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_ACCEPTCONN)
		if err != nil || listening == 0 {
			continue
		}
		bound, err := unix.Getsockname(fd)
		if err != nil {
			continue
		}
		if inet4, ok := bound.(*unix.SockaddrInet4); ok && inet4.Port == port {
			if err := unix.Shutdown(fd, unix.SHUT_RDWR); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatalf("nothing listens on %s", addr)
}

func TestAFatalAcceptErrorStopsHealthz(t *testing.T) {
	f := startServer(t, nil)
	shutdownListener(t, f.Listen)
	eventually(t, "/healthz to stop answering", func() bool { return !f.healthy() })
	if err := f.stop(); err == nil || !strings.Contains(err.Error(), "accept on") {
		t.Fatalf("Run returned %v", err)
	}
}

func TestAFatalAcceptErrorStopsTheProbe(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	f := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), nil)
	shutdownListener(t, f.Listen)
	eventually(t, "probe to stop answering", func() bool { return Probe(context.Background(), f.Socket) != nil })
	if err := f.stop(); err == nil || !strings.Contains(err.Error(), "accept on") {
		t.Fatalf("Run returned %v", err)
	}
}
