// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"golang.org/x/time/rate"
)

const (
	lookupTimeout    = 10 * time.Second
	firstLookupRetry = time.Second
)

// Resolver answers the upstream host's addresses; net.Resolver is one.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// Proxy is lodia-tunnel proxy: for each connection a pod makes to the node's
// advertise address it opens the tunnel's outer TLS session, or, with no
// CertDir and CADir, splices the pod's bytes to the upstream as they are.
type Proxy struct {
	Listen  string
	CertDir string
	CADir   string
	Socket  string
	Log     *slog.Logger

	Resolver       Resolver
	RefreshPeriod  time.Duration
	DialTimeout    time.Duration
	AttemptTimeout time.Duration
	ShutdownWait   time.Duration
	MaxConns       int
	MaxPerIP       int
	ReloadRetry    time.Duration

	host     string
	port     uint16
	addrs    atomic.Pointer[[]netip.Addr]
	answered atomic.Pointer[netip.Addr]
	failed   atomic.Pointer[netip.Addr]
	sessions tls.ClientSessionCache
	pair     *source[tls.Certificate]
	ca       *source[x509.CertPool]
	limits   connLimits
	conns    sync.WaitGroup
	connect  func(ctx context.Context, addr netip.AddrPort) (conn, error)

	capLog, lookupLog, dialLog, spliceLog rate.Sometimes
}

// NewProxy returns a Proxy with the design's bounds for an upstream of the
// form host:port.
func NewProxy(listen, upstream, certDir, caDir, socket string, log *slog.Logger) (*Proxy, error) {
	host, port, err := net.SplitHostPort(upstream)
	if err != nil {
		return nil, err
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return nil, fmt.Errorf("port of %s: %w", upstream, err)
	}
	p := &Proxy{
		Listen: listen, CertDir: certDir, CADir: caDir, Socket: socket, Log: log,
		Resolver:       net.DefaultResolver,
		RefreshPeriod:  30 * time.Second,
		DialTimeout:    6 * time.Second,
		AttemptTimeout: 2 * time.Second,
		ShutdownWait:   30 * time.Second,
		MaxConns:       1000,
		MaxPerIP:       128,
		ReloadRetry:    reloadRetry,

		host:     host,
		port:     uint16(portNumber),
		sessions: tls.NewLRUClientSessionCache(0),
		limits:   connLimits{perIP: map[netip.Addr]int{}},

		capLog:    everyMinute(),
		lookupLog: everyMinute(),
		dialLog:   everyMinute(),
		spliceLog: everyMinute(),
	}
	p.connect = connectTCP
	return p, nil
}

func (p *Proxy) tunneled() bool {
	return p.CertDir != ""
}

// Run loads the sources, then serves until ctx is done, when it closes the
// listener and waits up to ShutdownWait for its connections.
func (p *Proxy) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	var loaded []<-chan struct{}
	if p.tunneled() {
		p.pair = pairSource(p.CertDir, p.ReloadRetry, p.Log)
		p.ca = caSource(p.CADir, p.ReloadRetry, p.Log)
		g.Go(func() error { p.pair.watch(ctx); return nil })
		g.Go(func() error { p.ca.watch(ctx); return nil })
		loaded = append(loaded, p.pair.loaded, p.ca.loaded)
	}
	g.Go(func() error { p.refresh(ctx); return nil })
	g.Go(func() error {
		if !allLoaded(ctx, loaded...) {
			return nil
		}
		return p.serve(ctx)
	})
	return g.Wait()
}

// refresh keeps the upstream's addresses, the last good ones when a lookup
// fails. A dial reads them; none is made before the first answer, so until then
// it also retries on a shorter timer.
func (p *Proxy) refresh(ctx context.Context) {
	if addr, err := netip.ParseAddr(p.host); err == nil {
		p.addrs.Store(&[]netip.Addr{addr})
		return
	}
	tick := time.NewTicker(p.RefreshPeriod)
	defer tick.Stop()
	for {
		lookup, cancel := context.WithTimeout(ctx, lookupTimeout)
		addrs, err := p.Resolver.LookupNetIP(lookup, "ip", p.host)
		cancel()
		if err != nil {
			p.lookupLog.Do(func() { p.Log.WarnContext(ctx, "lookup failed, keeping the last good", "host", p.host, "error", err) })
		} else {
			p.addrs.Store(&addrs)
		}
		var retry <-chan time.Time
		if p.addrs.Load() == nil {
			retry = time.After(firstLookupRetry)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-retry:
		}
	}
}

func (p *Proxy) serve(ctx context.Context) error {
	ln, err := listenTCP(ctx, p.Listen, reusePort)
	if err != nil {
		return err
	}
	socket, err := listenSocket(ctx, p.Socket)
	if err != nil {
		_ = ln.Close()
		return fmt.Errorf("listen on %s: %w", p.Socket, err)
	}
	p.Log.InfoContext(ctx, "proxying", "listen", ln.Addr().String(), "upstream", p.host, "tunnel", p.tunneled(), "socket", p.Socket)

	conns, closeConns := context.WithCancel(context.WithoutCancel(ctx))
	defer closeConns()
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return accept(gctx, ln, p.Log, p.admit(conns)) })
	g.Go(func() error { return accept(gctx, socket, p.Log, answerProbe) })
	err = g.Wait()

	drained := make(chan struct{})
	go func() {
		p.conns.Wait()
		close(drained)
	}()
	wait := time.NewTimer(p.ShutdownWait)
	defer wait.Stop()
	select {
	case <-drained:
	case <-wait.C:
		closeConns()
		<-drained
	}
	return err
}

func reusePort(_, _ string, c syscall.RawConn) error {
	var opErr error
	err := c.Control(func(fd uintptr) {
		opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	})
	return errors.Join(err, opErr)
}

func (p *Proxy) admit(ctx context.Context) func(net.Conn) {
	return func(raw net.Conn) {
		client := raw.(*net.TCPConn)
		ip := client.RemoteAddr().(*net.TCPAddr).AddrPort().Addr().Unmap()
		if !p.limits.acquire(ip, p.MaxConns, p.MaxPerIP) {
			p.capLog.Do(func() {
				p.Log.WarnContext(ctx, "connections at a cap, closing a new one", "client", ip.String(), "cap", p.MaxConns, "capPerIP", p.MaxPerIP)
			})
			_ = client.Close()
			return
		}
		p.conns.Go(func() {
			defer p.limits.release(ip)
			defer func() { _ = client.Close() }()
			recovered(ctx, p.Log, func() { p.forward(ctx, client) })
		})
	}
}

func (p *Proxy) forward(ctx context.Context, client *net.TCPConn) {
	upstream, err := p.dialUpstream(ctx)
	if err != nil {
		p.dialLog.Do(func() { p.Log.WarnContext(ctx, "upstream dial failed", "upstream", p.host, "error", err) })
		return
	}
	var downstream conn = client
	if plain, ok := upstream.(*answering); ok {
		downstream = plain.pod(client)
	}
	if err := splice(ctx, p.Log, downstream, upstream); err != nil {
		p.spliceLog.Do(func() {
			p.Log.WarnContext(ctx, "connection failed", "client", client.RemoteAddr().String(), "error", err)
		})
	}
}

// dialUpstream tries the upstream's addresses in turn, as order ranks them,
// within DialTimeout, and moves on when an attempt fails. DialTimeout stays
// under the 10 s of a client's TLS handshake, which runs while this dials.
func (p *Proxy) dialUpstream(ctx context.Context) (conn, error) {
	addrs := p.addrs.Load()
	if addrs == nil || len(*addrs) == 0 {
		return nil, fmt.Errorf("no addresses of %s yet", p.host)
	}
	ctx, cancel := context.WithTimeout(ctx, p.DialTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	last := p.answered.Load()
	var errs []error
	for _, addr := range order(*addrs, last, p.failed.Load()) {
		upstream, err := p.dialAddr(ctx, addr, last)
		if err == nil {
			return upstream, nil
		}
		errs = append(errs, err)
		if !time.Now().Before(deadline) {
			break
		}
	}
	return nil, errors.Join(errs...)
}

// order is the address that last answered, while the lookup lists it, then the
// lookup's addresses from the one after the address that last failed: the
// connection that met it may have no way or time left to move on, so the next
// starts past it.
func order(addrs []netip.Addr, answered, failed *netip.Addr) []netip.Addr {
	start := 0
	if failed != nil {
		start = slices.Index(addrs, *failed) + 1
	}
	ordered := make([]netip.Addr, 0, len(addrs))
	if answered != nil && slices.Contains(addrs, *answered) {
		ordered = append(ordered, *answered)
	}
	for i := range addrs {
		if addr := addrs[(start+i)%len(addrs)]; answered == nil || addr != *answered {
			ordered = append(ordered, addr)
		}
	}
	return ordered
}

// dialAddr remembers addr once it has answered: the outer handshake is done, or
// without the tunnel its first byte has arrived. It fails addr on a failed
// connect, a handshake's error, or without the tunnel when the upstream ends
// before that byte. The handshake runs within what is left of the total, not
// within the attempt, so that a slow tunnel that is up is not dialed again.
func (p *Proxy) dialAddr(ctx context.Context, addr netip.Addr, last *netip.Addr) (conn, error) {
	attempt, cancel := context.WithTimeout(ctx, p.AttemptTimeout)
	upstream, err := p.connect(attempt, netip.AddrPortFrom(addr, p.port))
	cancel()
	if err != nil {
		p.fail(addr, last)
		return nil, err
	}
	if !p.tunneled() {
		return &answering{
			conn:     upstream,
			answered: func() { p.answered.Store(&addr) },
			failed:   func() { p.fail(addr, last) },
		}, nil
	}
	session := tls.Client(upstream, p.tlsConfig())
	if err := session.HandshakeContext(ctx); err != nil {
		_ = upstream.Close()
		p.fail(addr, last)
		return nil, err
	}
	p.answered.Store(&addr)
	return session, nil
}

// fail starts later dials after addr, and forgets addr if it is last, the
// address remembered when the dial began, and still remembered: a newer answer
// is never undone.
func (p *Proxy) fail(addr netip.Addr, last *netip.Addr) {
	if last != nil && *last == addr {
		p.answered.CompareAndSwap(last, nil)
	}
	p.failed.Store(&addr)
}

func connectTCP(ctx context.Context, addr netip.AddrPort) (conn, error) {
	upstream, err := tcpDialer().DialContext(ctx, "tcp", addr.String())
	if err != nil {
		return nil, err
	}
	return upstream.(*net.TCPConn), nil
}

// answering is an upstream without the tunnel. It has answered with its first
// byte, and has failed if it ends before one while its pod's side is open: a pod
// that ended its side may be what ended it.
type answering struct {
	conn
	once     sync.Once
	answered func()
	failed   func()
	podEnded atomic.Bool
}

func (a *answering) Read(b []byte) (int, error) {
	n, err := a.conn.Read(b)
	switch {
	case n > 0:
		a.once.Do(a.answered)
	case err != nil && !a.podEnded.Load():
		a.once.Do(a.failed)
	}
	return n, err
}

func (a *answering) pod(c conn) conn {
	return &podSide{conn: c, ended: &a.podEnded}
}

type podSide struct {
	conn
	ended *atomic.Bool
}

func (s *podSide) Read(b []byte) (int, error) {
	n, err := s.conn.Read(b)
	if err != nil {
		s.ended.Store(true)
	}
	return n, err
}

func (p *Proxy) tlsConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
		ServerName: p.host,
		RootCAs:    p.ca.current.Load(),
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return p.pair.current.Load(), nil
		},
		ClientSessionCache: p.sessions,
	}
}

type connLimits struct {
	mu    sync.Mutex
	total int
	perIP map[netip.Addr]int
}

func (l *connLimits) acquire(ip netip.Addr, maxTotal, maxPerIP int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total >= maxTotal || l.perIP[ip] >= maxPerIP {
		return false
	}
	l.total++
	l.perIP[ip]++
	return true
}

func (l *connLimits) release(ip netip.Addr) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total--
	l.perIP[ip]--
	if l.perIP[ip] == 0 {
		delete(l.perIP, ip)
	}
}
