// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"container/list"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// Server is lodia-tunnel serve: it ends the tunnel's outer TLS session beside
// an API server and splices each authenticated session to it.
type Server struct {
	Listen      string
	Hostname    string
	CertDir     string
	ClientCADir string
	Upstream    string
	HealthAddr  string
	Log         *slog.Logger

	HandshakeTimeout time.Duration
	MaxPending       int
	MaxSessions      int64
	DialTimeout      time.Duration
	ReloadRetry      time.Duration

	pair     *source[tls.Certificate]
	clientCA *source[x509.CertPool]
	pending  pendingHandshakes
	sessions atomic.Int64
	conns    sync.WaitGroup
	dial     func(ctx context.Context) (conn, error)

	evictLog, refuseLog, capLog, dialLog, spliceLog rate.Sometimes
}

// NewServer returns a Server with the design's bounds.
func NewServer(listen, hostname, certDir, clientCADir, upstream, healthAddr string, log *slog.Logger) *Server {
	s := &Server{
		Listen: listen, Hostname: hostname, CertDir: certDir, ClientCADir: clientCADir, Upstream: upstream, HealthAddr: healthAddr, Log: log,
		HandshakeTimeout: 10 * time.Second,
		MaxPending:       256,
		MaxSessions:      10_000,
		DialTimeout:      2 * time.Second,
		ReloadRetry:      reloadRetry,

		evictLog:  everyMinute(),
		refuseLog: everyMinute(),
		capLog:    everyMinute(),
		dialLog:   everyMinute(),
		spliceLog: everyMinute(),
	}
	s.dial = s.dialUpstream
	return s
}

// everyMinute limits a log line that a flood could repeat.
func everyMinute() rate.Sometimes {
	return rate.Sometimes{Interval: time.Minute}
}

// Run loads the sources, then serves until ctx is done, when it closes at
// once: a native sidecar gets SIGTERM after the API server has exited.
func (s *Server) Run(ctx context.Context) error {
	s.pair = pairSource(s.CertDir, s.ReloadRetry, s.Log)
	s.clientCA = caSource(s.ClientCADir, s.ReloadRetry, s.Log)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { s.pair.watch(ctx); return nil })
	g.Go(func() error { s.clientCA.watch(ctx); return nil })
	g.Go(func() error {
		if !allLoaded(ctx, s.pair.loaded, s.clientCA.loaded) {
			return nil
		}
		return s.serve(ctx)
	})
	return g.Wait()
}

func (s *Server) serve(ctx context.Context) error {
	ln, err := listenTCP(ctx, s.Listen, nil)
	if err != nil {
		return err
	}
	s.Log.InfoContext(ctx, "serving", "listen", ln.Addr().String(), "hostname", s.Hostname, "upstream", s.Upstream)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	health := &http.Server{Addr: s.HealthAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return accept(ctx, ln, s.Log, s.admit(ctx)) })
	g.Go(func() error {
		if err := health.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		return health.Close()
	})
	err = g.Wait()
	s.conns.Wait()
	return err
}

func (s *Server) admit(ctx context.Context) func(net.Conn) {
	cfg := s.tlsConfig()
	return func(raw net.Conn) {
		entry, evicted := s.pending.add(raw, s.MaxPending)
		if evicted {
			s.evictLog.Do(func() {
				s.Log.WarnContext(ctx, "pending handshakes at their bound, evicting the oldest", "bound", s.MaxPending)
			})
		}
		s.conns.Go(func() {
			recovered(ctx, s.Log, func() { s.handle(ctx, tls.Server(raw, cfg), entry) })
		})
	}
}

// tlsConfig checks the name in GetConfigForClient because a resumed session
// skips choosing a certificate, so no check there would see it.
func (s *Server) tlsConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			if hello.ServerName != s.Hostname {
				return nil, fmt.Errorf("server name %q is not %s", hello.ServerName, s.Hostname)
			}
			return &tls.Config{
				MinVersion:   tls.VersionTLS13,
				MaxVersion:   tls.VersionTLS13,
				Certificates: []tls.Certificate{*s.pair.current.Load()},
				ClientAuth:   tls.RequireAndVerifyClientCert,
				ClientCAs:    s.clientCA.current.Load(),
			}, nil
		},
	}
}

func (s *Server) handle(ctx context.Context, session *tls.Conn, entry *list.Element) {
	defer func() { _ = session.Close() }()
	defer s.pending.remove(entry)

	handshake, cancel := context.WithTimeout(ctx, s.HandshakeTimeout)
	defer cancel()
	err := session.HandshakeContext(handshake)
	s.pending.remove(entry)
	if err != nil {
		s.refuseLog.Do(func() {
			s.Log.WarnContext(ctx, "handshake failed", "client", session.RemoteAddr().String(), "error", err)
		})
		return
	}

	defer s.sessions.Add(-1)
	if s.sessions.Add(1) > s.MaxSessions {
		s.capLog.Do(func() { s.Log.WarnContext(ctx, "sessions at their cap, closing a new one", "cap", s.MaxSessions) })
		return
	}

	upstream, err := s.dial(ctx)
	if err != nil {
		s.dialLog.Do(func() { s.Log.WarnContext(ctx, "upstream dial failed", "upstream", s.Upstream, "error", err) })
		return
	}
	if err := splice(ctx, s.Log, session, upstream); err != nil {
		s.spliceLog.Do(func() {
			s.Log.WarnContext(ctx, "session failed", "client", session.RemoteAddr().String(), "error", err)
		})
	}
}

func (s *Server) dialUpstream(ctx context.Context) (conn, error) {
	ctx, cancel := context.WithTimeout(ctx, s.DialTimeout)
	defer cancel()
	upstream, err := tcpDialer().DialContext(ctx, "tcp", s.Upstream)
	if err != nil {
		return nil, err
	}
	return upstream.(*net.TCPConn), nil
}

// pendingHandshakes are the connections that have not finished their
// handshake, oldest first.
type pendingHandshakes struct {
	mu    sync.Mutex
	conns list.List
}

func (p *pendingHandshakes) add(c net.Conn, bound int) (entry *list.Element, evicted bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conns.Len() >= bound {
		oldest := p.conns.Front()
		p.conns.Remove(oldest)
		_ = oldest.Value.(net.Conn).Close()
		evicted = true
	}
	return p.conns.PushBack(c), evicted
}

func (p *pendingHandshakes) remove(entry *list.Element) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns.Remove(entry)
}
