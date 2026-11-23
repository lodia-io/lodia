// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestTheFilesAreTheOnesPKIWrites(t *testing.T) {
	if tlsCertFile != lodiav1alpha1.TLSCertKey || tlsKeyFile != lodiav1alpha1.TLSPrivateKeyKey || caCertFile != lodiav1alpha1.CACertKey {
		t.Errorf("reads %s, %s and %s; PKI writes %s, %s and %s", tlsCertFile, tlsKeyFile, caCertFile,
			lodiav1alpha1.TLSCertKey, lodiav1alpha1.TLSPrivateKeyKey, lodiav1alpha1.CACertKey)
	}
}

func TestEveryTCPLegKeepsAlive(t *testing.T) {
	ln, err := listenTCP(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	dialTCP(t, ln.Addr().String())
	accepted, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = accepted.Close() }()

	f := startTunnel(t, echo)
	serveLeg, err := f.server.dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = serveLeg.Close() }()
	proxyLeg, err := f.proxy.dialUpstream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = proxyLeg.Close() }()

	for name, c := range map[string]net.Conn{"accepted": accepted, "serve's upstream": serveLeg, "proxy's upstream": proxyLeg.(*tls.Conn).NetConn()} {
		raw, err := c.(*net.TCPConn).SyscallConn()
		if err != nil {
			t.Fatal(err)
		}
		var enabled, interval, count int
		var optErrs [3]error
		if err := raw.Control(func(fd uintptr) {
			enabled, optErrs[0] = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_KEEPALIVE)
			interval, optErrs[1] = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_KEEPINTVL)
			count, optErrs[2] = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_KEEPCNT)
		}); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(optErrs[:]...); err != nil {
			t.Fatal(err)
		}
		if enabled == 0 || interval != 10 || count != 3 {
			t.Errorf("%s: keepalive %d, interval %d, count %d", name, enabled, interval, count)
		}
	}
	if keepAlive.Idle != 30*time.Second {
		t.Errorf("idle %s", keepAlive.Idle)
	}
}

type scriptedListener struct {
	net.Listener
	mu     sync.Mutex
	script []error
	at     []time.Time
}

func (l *scriptedListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	l.at = append(l.at, time.Now())
	var err error
	if len(l.script) > 0 {
		err, l.script = l.script[0], l.script[1:]
	}
	l.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return l.Listener.Accept()
}

func TestEMFILEOnAcceptBacksOffAndAcceptingResumes(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EMFILE, syscall.ENFILE, syscall.ENOBUFS, syscall.ENOMEM} {
		t.Run(errno.Error(), func(t *testing.T) {
			tcp, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ln := &scriptedListener{Listener: tcp, script: []error{
				&net.OpError{Op: "accept", Err: errno},
				&net.OpError{Op: "accept", Err: errno},
				&net.OpError{Op: "accept", Err: errno},
			}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			accepted := make(chan net.Conn, 1)
			done := make(chan error, 1)
			go func() { done <- accept(ctx, ln, discardLog(), func(c net.Conn) { accepted <- c }) }()

			c := dialTCP(t, tcp.Addr().String())
			defer func() { _ = c.Close() }()
			select {
			case got := <-accepted:
				_ = got.Close()
			case <-time.After(waitFor):
				t.Fatal("accepting did not resume")
			}

			ln.mu.Lock()
			at := ln.at
			ln.mu.Unlock()
			for i, want := range []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond} {
				if gap := at[i+1].Sub(at[i]); gap < want {
					t.Errorf("attempt %d after %s, want at least %s", i+1, gap, want)
				}
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func loggedBackoffs(logs *logBuffer) []string {
	var got []string
	for _, match := range regexp.MustCompile(`backoff=(\S+)`).FindAllStringSubmatch(logs.String(), -1) {
		got = append(got, match[1])
	}
	return got
}

func TestAcceptBackoffStopsAtItsMaximumAndWithItsContext(t *testing.T) {
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	script := make([]error, 12)
	for i := range script {
		script[i] = syscall.EMFILE
	}
	ln := &scriptedListener{Listener: tcp, script: script}
	log, logs := newLog(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- accept(ctx, ln, log, func(net.Conn) {}) }()
	want := []string{"5ms", "10ms", "20ms", "40ms", "80ms", "160ms", "320ms", "640ms", "1s", "1s"}
	eventually(t, "the backoff to reach its maximum", func() bool { return logs.count("accept failed") >= len(want) })
	if got := loggedBackoffs(logs); !slices.Equal(got[:len(want)], want) {
		t.Fatalf("backed off %v, want %v", got, want)
	}
	start := time.Now()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("took %s to stop in a backoff", took)
	}
}

func TestAcceptBackoffStartsOverAfterAConnection(t *testing.T) {
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := &scriptedListener{Listener: tcp, script: []error{syscall.EMFILE, syscall.EMFILE, syscall.EMFILE, nil, syscall.EMFILE}}
	log, logs := newLog(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- accept(ctx, ln, log, func(c net.Conn) { _ = c.Close() }) }()
	dialTCP(t, tcp.Addr().String())
	want := []string{"5ms", "10ms", "20ms", "5ms"}
	eventually(t, "the failure after the connection", func() bool { return logs.count("accept failed") == len(want) })
	if got := loggedBackoffs(logs); !slices.Equal(got, want) {
		t.Fatalf("backed off %v, want %v", got, want)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAnAcceptErrorThatWillNotPassIsReturned(t *testing.T) {
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	broken := errors.New("broken")
	if err := accept(context.Background(), &scriptedListener{Listener: tcp, script: []error{broken}}, discardLog(), func(net.Conn) {}); !errors.Is(err, broken) {
		t.Fatalf("accept returned %v", err)
	}
	if _, err := tcp.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("the listener was left open: %v", err)
	}
}

type fakeConn struct {
	net.Conn
	read          func(p []byte) (int, error)
	writeErr      error
	closeWriteErr error
	written       atomic.Int64
	closes        atomic.Int64
	closeWrite    atomic.Int64
}

func (c *fakeConn) Read(p []byte) (int, error) { return c.read(p) }

func (c *fakeConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	c.written.Add(int64(len(p)))
	return len(p), nil
}

func (c *fakeConn) Close() error {
	c.closes.Add(1)
	return nil
}

func (c *fakeConn) CloseWrite() error {
	c.closeWrite.Add(1)
	return c.closeWriteErr
}

func (c *fakeConn) ReadFrom(io.Reader) (int64, error) {
	return 0, errors.New("copied around the pooled buffer through ReadFrom")
}

func (c *fakeConn) WriteTo(io.Writer) (int64, error) {
	return 0, errors.New("copied around the pooled buffer through WriteTo")
}

func TestASpliceCopiesThroughPooledBuffers(t *testing.T) {
	sizes := make(chan int, 16)
	source := func() func(p []byte) (int, error) {
		sent := false
		return func(p []byte) (int, error) {
			sizes <- len(p)
			if sent {
				return 0, io.EOF
			}
			sent = true
			return copy(p, "payload"), nil
		}
	}
	a, b := &fakeConn{read: source()}, &fakeConn{read: source()}
	if err := splice(context.Background(), discardLog(), a, b); err != nil {
		t.Fatal(err)
	}
	close(sizes)
	for size := range sizes {
		if size != 16*1024 {
			t.Fatalf("read into %d bytes, want 16 KiB", size)
		}
	}
	for name, c := range map[string]*fakeConn{"a": a, "b": b} {
		if c.written.Load() != int64(len("payload")) || c.closeWrite.Load() != 1 || c.closes.Load() != 1 {
			t.Errorf("%s: wrote %d, half-closed %d, closed %d times", name, c.written.Load(), c.closeWrite.Load(), c.closes.Load())
		}
	}
}

func TestASpliceClosesBothSidesWhenOneFails(t *testing.T) {
	broken := errors.New("broken")
	unblock := make(chan struct{})
	a := &fakeConn{read: func([]byte) (int, error) { return 0, broken }}
	b := &fakeConn{read: func([]byte) (int, error) {
		<-unblock
		return 0, net.ErrClosed
	}}
	done := make(chan error, 1)
	go func() { done <- splice(context.Background(), discardLog(), a, b) }()
	eventually(t, "both sides closed", func() bool { return a.closes.Load() == 1 && b.closes.Load() == 1 })
	close(unblock)
	if err := <-done; !errors.Is(err, broken) {
		t.Fatalf("splice returned %v, want the first failure", err)
	}
	if a.closeWrite.Load()+b.closeWrite.Load() != 0 {
		t.Fatal("a failure was passed on as a half-close")
	}
}

func TestASpliceSurvivesAPanicInEitherDirection(t *testing.T) {
	for _, panicking := range []string{"a", "b"} {
		t.Run(panicking, func(t *testing.T) {
			unblock := make(chan struct{})
			blocked := func([]byte) (int, error) {
				<-unblock
				return 0, net.ErrClosed
			}
			boom := func([]byte) (int, error) { panic("boom") }
			a, b := &fakeConn{read: blocked}, &fakeConn{read: boom}
			if panicking == "a" {
				a, b = &fakeConn{read: boom}, &fakeConn{read: blocked}
			}
			log, logs := newLog(t)
			done := make(chan error, 1)
			go func() { done <- splice(context.Background(), log, a, b) }()
			eventually(t, "both sides closed", func() bool { return a.closes.Load() == 1 && b.closes.Load() == 1 })
			close(unblock)
			if err := <-done; !errors.Is(err, errSplicePanicked) {
				t.Fatalf("splice returned %v", err)
			}
			if logs.count("connection panicked") != 1 {
				t.Fatal("the panic is not in the log")
			}
		})
	}
}

func TestASpliceEndsWithItsContext(t *testing.T) {
	unblock := make(chan struct{})
	blocked := func([]byte) (int, error) {
		<-unblock
		return 0, net.ErrClosed
	}
	a, b := &fakeConn{read: blocked}, &fakeConn{read: blocked}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- splice(ctx, discardLog(), a, b) }()
	cancel()
	eventually(t, "both sides closed", func() bool { return a.closes.Load() == 1 && b.closes.Load() == 1 })
	close(unblock)
	if err := <-done; err != nil {
		t.Fatalf("splice returned %v, want no failure for a closing", err)
	}
}

func syscallError(op string, errno syscall.Errno) error {
	return &net.OpError{Op: op, Net: "tcp", Err: os.NewSyscallError(op, errno)}
}

func cleanEOF([]byte) (int, error) { return 0, io.EOF }

func failWith(err error) func([]byte) (int, error) {
	return func([]byte) (int, error) { return 0, err }
}

func sendOnce(data string) func([]byte) (int, error) {
	return func(p []byte) (int, error) { return copy(p, data), nil }
}

func blockedUntil(release <-chan struct{}) func([]byte) (int, error) {
	return func([]byte) (int, error) {
		<-release
		return 0, net.ErrClosed
	}
}

func afterHalfClose(c *fakeConn, read func(p []byte) (int, error)) func(p []byte) (int, error) {
	return func(p []byte) (int, error) {
		for c.closeWrite.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
		return read(p)
	}
}

type spliceScenario struct {
	downstream bool
	build      func(err error, release <-chan struct{}) (downstream, upstream *fakeConn)
}

var afterACleanEnd = map[string]spliceScenario{
	"the downstream ended, a read of the upstream": {false, func(err error, _ <-chan struct{}) (*fakeConn, *fakeConn) {
		up := &fakeConn{}
		up.read = afterHalfClose(up, failWith(err))
		return &fakeConn{read: cleanEOF}, up
	}},
	"the downstream ended, a write to the downstream": {true, func(err error, _ <-chan struct{}) (*fakeConn, *fakeConn) {
		up := &fakeConn{}
		up.read = afterHalfClose(up, sendOnce("late"))
		return &fakeConn{read: cleanEOF, writeErr: err}, up
	}},
	"the downstream ended, the half-close of the upstream": {false, func(err error, release <-chan struct{}) (*fakeConn, *fakeConn) {
		return &fakeConn{read: cleanEOF}, &fakeConn{read: blockedUntil(release), closeWriteErr: err}
	}},
	"the upstream ended, a read of the downstream": {true, func(err error, _ <-chan struct{}) (*fakeConn, *fakeConn) {
		down := &fakeConn{}
		down.read = afterHalfClose(down, failWith(err))
		return down, &fakeConn{read: cleanEOF}
	}},
	"the upstream ended, a write to the upstream": {false, func(err error, _ <-chan struct{}) (*fakeConn, *fakeConn) {
		down := &fakeConn{}
		down.read = afterHalfClose(down, sendOnce("late"))
		return down, &fakeConn{read: cleanEOF, writeErr: err}
	}},
	"the upstream ended, the half-close of the downstream": {true, func(err error, release <-chan struct{}) (*fakeConn, *fakeConn) {
		return &fakeConn{read: blockedUntil(release), closeWriteErr: err}, &fakeConn{read: cleanEOF}
	}},
}

var beforeAnyEnd = map[string]spliceScenario{
	"a read of the downstream": {true, func(err error, release <-chan struct{}) (*fakeConn, *fakeConn) {
		return &fakeConn{read: failWith(err)}, &fakeConn{read: blockedUntil(release)}
	}},
	"a write to the downstream": {true, func(err error, release <-chan struct{}) (*fakeConn, *fakeConn) {
		return &fakeConn{read: blockedUntil(release), writeErr: err}, &fakeConn{read: sendOnce("early")}
	}},
	"a read of the upstream": {false, func(err error, release <-chan struct{}) (*fakeConn, *fakeConn) {
		return &fakeConn{read: blockedUntil(release)}, &fakeConn{read: failWith(err)}
	}},
	"a write to the upstream": {false, func(err error, release <-chan struct{}) (*fakeConn, *fakeConn) {
		return &fakeConn{read: sendOnce("early")}, &fakeConn{read: blockedUntil(release), writeErr: err}
	}},
}

func spliceOf(t *testing.T, scenario spliceScenario, err error) error {
	t.Helper()
	release := make(chan struct{})
	downstream, upstream := scenario.build(err, release)
	done := make(chan error, 1)
	go func() { done <- splice(context.Background(), discardLog(), downstream, upstream) }()
	eventually(t, "both sides closed", func() bool { return downstream.closes.Load() == 1 && upstream.closes.Load() == 1 })
	close(release)
	return <-done
}

func eachSplice(t *testing.T, scenarios map[string]spliceScenario, downstream *bool, errs map[string]error, run func(t *testing.T, scenario spliceScenario, err error)) {
	t.Helper()
	for name, scenario := range scenarios {
		if downstream != nil && scenario.downstream != *downstream {
			continue
		}
		for kind, err := range errs {
			t.Run(name+"/"+kind, func(t *testing.T) { run(t, scenario, err) })
		}
	}
}

func teardownErrors() map[string]error {
	return map[string]error{
		"a reset":       syscallError("read", syscall.ECONNRESET),
		"a broken pipe": syscallError("write", syscall.EPIPE),
		"not connected": syscallError("shutdown", syscall.ENOTCONN),
	}
}

func unrelatedErrors() map[string]error {
	return map[string]error{
		"a timeout":           syscallError("read", syscall.ETIMEDOUT),
		"a truncated session": io.ErrUnexpectedEOF,
		"another error":       errors.New("unrelated"),
	}
}

func requireNoFailure(t *testing.T, scenario spliceScenario, err error) {
	t.Helper()
	if got := spliceOf(t, scenario, err); got != nil {
		t.Fatalf("splice returned %v, want no failure", got)
	}
}

func requireFailure(t *testing.T, scenario spliceScenario, err error) {
	t.Helper()
	if got := spliceOf(t, scenario, err); !errors.Is(got, err) {
		t.Fatalf("splice returned %v, want %v", got, err)
	}
}

func TestASpliceDoesNotReportTheTeardownOfTheDownstreamLegBeforeOrAfterAnEnd(t *testing.T) {
	downstream := true
	eachSplice(t, beforeAnyEnd, &downstream, teardownErrors(), requireNoFailure)
	eachSplice(t, afterACleanEnd, &downstream, teardownErrors(), requireNoFailure)
}

func TestASpliceDoesNotReportTheTeardownOfTheUpstreamLegAfterACleanEnd(t *testing.T) {
	upstream := false
	eachSplice(t, afterACleanEnd, &upstream, teardownErrors(), requireNoFailure)
}

func TestASpliceReportsTheTeardownOfTheUpstreamLegBeforeAnyCleanEnd(t *testing.T) {
	upstream := false
	eachSplice(t, beforeAnyEnd, &upstream, teardownErrors(), requireFailure)
}

func TestASpliceReportsAnUnrelatedErrorOfEitherLegBeforeOrAfterACleanEnd(t *testing.T) {
	eachSplice(t, beforeAnyEnd, nil, unrelatedErrors(), requireFailure)
	eachSplice(t, afterACleanEnd, nil, unrelatedErrors(), requireFailure)
}
