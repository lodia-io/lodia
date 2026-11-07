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
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"golang.org/x/time/rate"
)

const lookupTimeout = 10 * time.Second

// Resolver answers the upstream host's addresses; net.Resolver is one.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// Proxy is lodia-tunnel proxy: it opens the tunnel's outer TLS session for
// each connection a pod makes to the node's advertise address.
type Proxy struct {
	Listen  string
	CertDir string
	CADir   string
	Socket  string
	Log     *slog.Logger

	Resolver      Resolver
	RefreshPeriod time.Duration
	DialTimeout   time.Duration
	ShutdownWait  time.Duration
	MaxConns      int
	MaxPerIP      int
	ReloadRetry   time.Duration

	host     string
	port     uint16
	addrs    atomic.Pointer[[]netip.Addr]
	sessions tls.ClientSessionCache
	pair     *source[tls.Certificate]
	ca       *source[x509.CertPool]
	limits   connLimits
	conns    sync.WaitGroup
	dial     func(ctx context.Context) (conn, error)

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
		Resolver:      net.DefaultResolver,
		RefreshPeriod: 30 * time.Second,
		DialTimeout:   10 * time.Second,
		ShutdownWait:  30 * time.Second,
		MaxConns:      1000,
		MaxPerIP:      128,
		ReloadRetry:   reloadRetry,

		host:     host,
		port:     uint16(portNumber),
		sessions: tls.NewLRUClientSessionCache(0),
		limits:   connLimits{perIP: map[netip.Addr]int{}},

		capLog:    everyMinute(),
		lookupLog: everyMinute(),
		dialLog:   everyMinute(),
		spliceLog: everyMinute(),
	}
	p.dial = p.dialUpstream
	return p, nil
}

// Run loads the sources, then serves until ctx is done, when it closes the
// listener and waits up to ShutdownWait for its connections.
func (p *Proxy) Run(ctx context.Context) error {
	p.pair = pairSource(p.CertDir, p.ReloadRetry, p.Log)
	p.ca = caSource(p.CADir, p.ReloadRetry, p.Log)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { p.pair.watch(ctx); return nil })
	g.Go(func() error { p.ca.watch(ctx); return nil })
	g.Go(func() error { p.refresh(ctx); return nil })
	g.Go(func() error {
		if !allLoaded(ctx, p.pair.loaded, p.ca.loaded) {
			return nil
		}
		return p.serve(ctx)
	})
	return g.Wait()
}

// refresh keeps the upstream's addresses, the last good ones when a lookup
// fails. A dial reads them; none is made before the first answer.
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
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
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
	p.Log.InfoContext(ctx, "proxying", "listen", ln.Addr().String(), "upstream", p.host, "socket", p.Socket)

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
	upstream, err := p.dial(ctx)
	if err != nil {
		p.dialLog.Do(func() { p.Log.WarnContext(ctx, "upstream dial failed", "upstream", p.host, "error", err) })
		return
	}
	if err := splice(ctx, p.Log, client, upstream); err != nil {
		p.spliceLog.Do(func() {
			p.Log.WarnContext(ctx, "connection failed", "client", client.RemoteAddr().String(), "error", err)
		})
	}
}

// dialUpstream tries the addresses in turn within DialTimeout, each with its
// share of what is left, as net.Dialer does: a silent address must not use up
// the time of the next.
func (p *Proxy) dialUpstream(ctx context.Context) (conn, error) {
	addrs := p.addrs.Load()
	if addrs == nil {
		return nil, fmt.Errorf("no addresses of %s yet", p.host)
	}
	ctx, cancel := context.WithTimeout(ctx, p.DialTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	dialer := &tls.Dialer{NetDialer: tcpDialer(), Config: p.tlsConfig()}
	var errs []error
	for i, addr := range *addrs {
		attempt, cancel := context.WithTimeout(ctx, time.Until(deadline)/time.Duration(len(*addrs)-i))
		upstream, err := dialer.DialContext(attempt, "tcp", netip.AddrPortFrom(addr, p.port).String())
		cancel()
		if err == nil {
			return upstream.(*tls.Conn), nil
		}
		errs = append(errs, err)
	}
	return nil, errors.Join(errs...)
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
