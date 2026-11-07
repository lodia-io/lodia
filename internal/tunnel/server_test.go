// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.lodia.io/lodia/internal/certs"
)

func TestServerBoundsAreTheDesigns(t *testing.T) {
	s := NewServer(":6444", tunnelHost, "c", "a", "127.0.0.1:6443", ":8135", nil)
	if s.MaxPending != 256 || s.HandshakeTimeout != 10*time.Second || s.MaxSessions != 10_000 || s.DialTimeout != 2*time.Second || s.HealthTimeout != 5*time.Second {
		t.Fatalf("pending %d, handshake %s, sessions %d, dial %s, health %s", s.MaxPending, s.HandshakeTimeout, s.MaxSessions, s.DialTimeout, s.HealthTimeout)
	}
}

func TestServeSplicesAnAuthenticatedSession(t *testing.T) {
	f := startServer(t, nil)
	c, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(c); err != nil {
		t.Fatal(err)
	}
	if state := c.ConnectionState(); state.Version != tls.VersionTLS13 {
		t.Fatalf("negotiated %x, want TLS 1.3", state.Version)
	}
}

func TestServeRefuses(t *testing.T) {
	f := startServer(t, nil)
	otherCA := newCA(t, "other")

	for _, tc := range []struct {
		name string
		tune func(*tls.Config)
		want string
	}{
		{"no client certificate", func(c *tls.Config) { c.Certificates = nil }, "certificate required"},
		{"a client certificate from another CA", presenting(tlsPair(t, client(t, otherCA))), "unknown certificate authority"},
		{"an expired client certificate", presenting(tlsPair(t, expired(t, f.tunnelCA, clientProfile))), "expired certificate"},
		{"a client certificate without clientAuth", presenting(tlsPair(t, serving(t, f.tunnelCA))), "bad certificate"},
		{"an empty server name", func(c *tls.Config) { c.ServerName, c.InsecureSkipVerify = "", true }, "internal error"},
		{"another server name", func(c *tls.Config) { c.ServerName, c.InsecureSkipVerify = "other.lodia.test", true }, "internal error"},
		{"TLS 1.2", func(c *tls.Config) { c.MinVersion, c.MaxVersion = tls.VersionTLS12, tls.VersionTLS12 }, "protocol version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := f.config()
			tc.tune(cfg)
			c, err := dialTLS(t, f.Listen, cfg)
			if err == nil {
				err = roundTrip(c)
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want a refusal with %q", err, tc.want)
			}
			if n := f.upstream.accepted.Load(); n != 0 {
				t.Fatalf("the upstream was dialled %d times", n)
			}
		})
	}

	c, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(c); err != nil {
		t.Fatalf("the certificate those were refused against: %v", err)
	}
}

func presenting(pair tls.Certificate) func(*tls.Config) {
	return func(c *tls.Config) {
		c.Certificates = nil
		c.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &pair, nil }
	}
}

type anyKeySessions struct {
	mu    sync.Mutex
	state *tls.ClientSessionState
}

func (s *anyKeySessions) Get(string) (*tls.ClientSessionState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, s.state != nil
}

func (s *anyKeySessions) Put(_ string, state *tls.ClientSessionState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state != nil {
		s.state = state
	}
}

func TestAResumedSessionCannotChangeItsServerName(t *testing.T) {
	f := startServer(t, nil)
	sessions := &anyKeySessions{}

	cfg := f.config()
	cfg.ClientSessionCache = sessions
	c, err := dialTLS(t, f.Listen, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(c); err != nil {
		t.Fatal(err)
	}

	again, err := dialTLS(t, f.Listen, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !again.ConnectionState().DidResume {
		t.Fatal("the same name did not resume, so this test proves nothing")
	}
	if err := roundTrip(again); err != nil {
		t.Fatal(err)
	}
	dialled := f.upstream.accepted.Load()

	for _, name := range []string{"", "other.lodia.test"} {
		other := f.config()
		other.ClientSessionCache = sessions
		other.ServerName, other.InsecureSkipVerify = name, true
		c, err := dialTLS(t, f.Listen, other)
		if err == nil {
			err = roundTrip(c)
		}
		if err == nil {
			t.Fatalf("a session resumed under the name %q", name)
		}
	}
	if n := f.upstream.accepted.Load(); n != dialled {
		t.Fatalf("the upstream was dialled %d more times", n-dialled)
	}
}

func TestAnExpiredClientCertificateGetsNothingFromItsTicket(t *testing.T) {
	f := startServer(t, nil)
	leaf := shortLived(t, f.tunnelCA, clientProfile)
	sessions := tls.NewLRUClientSessionCache(0)
	cfg := f.config()
	cfg.Certificates = []tls.Certificate{tlsPair(t, leaf)}
	cfg.ClientSessionCache = sessions

	first, err := dialTLS(t, f.Listen, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(first); err != nil {
		t.Fatal(err)
	}
	second, err := dialTLS(t, f.Listen, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !second.ConnectionState().DidResume {
		t.Fatal("no session to resume, so this test proves nothing")
	}
	if err := roundTrip(second); err != nil {
		t.Fatal(err)
	}
	waitPastExpiry(leaf)

	dialled := f.upstream.accepted.Load()
	expiredClient, err := dialTLS(t, f.Listen, cfg)
	if err == nil {
		err = roundTrip(expiredClient)
	}
	if err == nil || !strings.Contains(err.Error(), "expired certificate") {
		t.Fatalf("got %v, want a refusal with the expired certificate", err)
	}
	if n := f.upstream.accepted.Load(); n != dialled {
		t.Fatalf("the upstream was dialled %d more times", n-dialled)
	}

	renewed := f.config()
	renewed.ClientSessionCache = sessions
	c, err := dialTLS(t, f.Listen, renewed)
	if err != nil {
		t.Fatal(err)
	}
	if c.ConnectionState().DidResume {
		t.Fatal("a session resumed past its certificate's expiry")
	}
	if err := roundTrip(c); err != nil {
		t.Fatal(err)
	}
}

func TestATamperedTicketIsIgnored(t *testing.T) {
	f := startServer(t, nil)
	sessions := &anyKeySessions{}
	cfg := f.config()
	cfg.ClientSessionCache = sessions
	c, err := dialTLS(t, f.Listen, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(c); err != nil {
		t.Fatal(err)
	}
	state, _ := sessions.Get("")
	ticket, session, err := state.ResumptionState()
	if err != nil {
		t.Fatal(err)
	}
	tampered := bytes.Clone(ticket)
	tampered[len(tampered)/2] ^= 0xff
	forged, err := tls.NewResumptionState(tampered, session)
	if err != nil {
		t.Fatal(err)
	}
	sessions.Put("", forged)

	bare := f.config()
	bare.Certificates = nil
	bare.ClientSessionCache = sessions
	dialled := f.upstream.accepted.Load()
	got, err := dialTLS(t, f.Listen, bare)
	if err == nil {
		err = roundTrip(got)
	}
	if err == nil || !strings.Contains(err.Error(), "certificate required") {
		t.Fatalf("got %v, want a tampered ticket to leave a certificate required", err)
	}
	if n := f.upstream.accepted.Load(); n != dialled {
		t.Fatalf("the upstream was dialled %d more times", n-dialled)
	}

	sessions.Put("", forged)
	full, err := dialTLS(t, f.Listen, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if full.ConnectionState().DidResume {
		t.Fatal("a session resumed on a tampered ticket")
	}
	if err := roundTrip(full); err != nil {
		t.Fatal(err)
	}
}

func TestATicketOfAnotherReplicaIsIgnored(t *testing.T) {
	cluster, tunnelCA := newCA(t, "cluster"), newCA(t, "tunnel")
	replica := func() *serverFixture {
		f := newServerFixture(t, nil, cluster, tunnelCA)
		writeVolume(t, f.certDir, pairFiles(t, serving(t, cluster)))
		writeVolume(t, f.caDir, caFiles(t, tunnelCA))
		f.start(t)
		f.waitHealthy(t)
		return f
	}
	a, b := replica(), replica()

	sessions := &anyKeySessions{}
	cfg := a.config()
	cfg.ClientSessionCache = sessions
	c, err := dialTLS(t, a.Listen, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(c); err != nil {
		t.Fatal(err)
	}

	bare := b.config()
	bare.Certificates = nil
	bare.ClientSessionCache = sessions
	got, err := dialTLS(t, b.Listen, bare)
	if err == nil {
		err = roundTrip(got)
	}
	if err == nil || !strings.Contains(err.Error(), "certificate required") {
		t.Fatalf("got %v, want another replica's ticket to leave a certificate required", err)
	}
	if n := b.upstream.accepted.Load(); n != 0 {
		t.Fatalf("the upstream was dialled %d times", n)
	}

	withCertificate := b.config()
	withCertificate.ClientSessionCache = sessions
	full, err := dialTLS(t, b.Listen, withCertificate)
	if err != nil {
		t.Fatal(err)
	}
	if full.ConnectionState().DidResume {
		t.Fatal("a session resumed on another replica's ticket")
	}
	if err := roundTrip(full); err != nil {
		t.Fatal(err)
	}
}

func TestAStalledHandshakeClosesAtItsDeadlineAndNeverDialsTheUpstream(t *testing.T) {
	const deadline = 200 * time.Millisecond
	f := startServer(t, func(s *Server) { s.HandshakeTimeout = deadline })

	for name, stall := range map[string]func(c net.Conn, stop <-chan struct{}){
		"silent": func(net.Conn, <-chan struct{}) {},
		"trickling a ClientHello": func(c net.Conn, stop <-chan struct{}) {
			for _, b := range []byte{0x16, 0x03, 0x01, 0x02, 0x00, 0x01, 0x00} {
				select {
				case <-stop:
					return
				case <-time.After(deadline / 5):
					_, _ = c.Write([]byte{b})
				}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			c := dialTCP(t, f.Listen)
			stop := make(chan struct{})
			defer close(stop)
			go stall(c, stop)
			requireClosed(t, c)
			if took := time.Since(start); took < deadline {
				t.Fatalf("closed after %s, before its deadline of %s", took, deadline)
			}
			if n := f.upstream.accepted.Load(); n != 0 {
				t.Fatalf("the upstream was dialled %d times", n)
			}
		})
	}
}

func TestAFloodEvictsTheOldestWhileARealHandshakeCompletes(t *testing.T) {
	f := startServer(t, nil)
	oldest := dialTCP(t, f.Listen)

	stop := make(chan struct{})
	full := make(chan struct{})
	var flood sync.WaitGroup
	flood.Go(func() {
		for opened := 1; ; opened++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
			c, err := net.DialTimeout("tcp", f.Listen, waitFor)
			if err != nil {
				t.Error(err)
				return
			}
			t.Cleanup(func() { _ = c.Close() })
			if opened == f.MaxPending+1 {
				close(full)
			}
		}
	})
	defer flood.Wait()
	defer close(stop)

	select {
	case <-full:
	case <-time.After(waitFor):
		t.Fatal("the flood did not reach the bound")
	}
	requireClosed(t, oldest)

	c, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatalf("a real handshake during the flood: %v", err)
	}
	if err := roundTrip(c); err != nil {
		t.Fatalf("a real session during the flood: %v", err)
	}
	if n := f.upstream.accepted.Load(); n != 1 {
		t.Fatalf("the upstream was dialled %d times, want only for the real session", n)
	}
}

func TestEvictionTakesTheOldestPendingAndNeverASession(t *testing.T) {
	f := startServer(t, func(s *Server) { s.MaxPending = 2 })
	session, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(session); err != nil {
		t.Fatal(err)
	}

	stalled := make([]*net.TCPConn, 5)
	for i := range stalled {
		stalled[i] = dialTCP(t, f.Listen)
	}
	for _, c := range stalled[:3] {
		requireClosed(t, c)
	}
	for _, c := range stalled[3:] {
		requireOpen(t, c)
	}
	if err := roundTrip(session); err != nil {
		t.Fatalf("the session after the evictions: %v", err)
	}
}

func TestTheSessionCapCountsOnlyFinishedHandshakes(t *testing.T) {
	f := startServer(t, func(s *Server) { s.MaxSessions = 1 })
	for range 3 {
		dialTCP(t, f.Listen)
	}

	first, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(first); err != nil {
		t.Fatalf("the first session, beside stalled handshakes: %v", err)
	}

	over, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatal(err)
	}
	requireClosed(t, over)
	if n := f.upstream.accepted.Load(); n != 1 {
		t.Fatalf("the upstream was dialled %d times, want only for the first", n)
	}
	if err := roundTrip(first); err != nil {
		t.Fatalf("the first session after the refusal: %v", err)
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the slot to free", func() bool { return f.sessions.Load() == 0 })
	next, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(next); err != nil {
		t.Fatalf("a session after the slot freed: %v", err)
	}
}

func TestAFailedUpstreamDialClosesTheSession(t *testing.T) {
	refused := freeAddr(t)
	f := startServer(t, func(s *Server) { s.Upstream = refused })
	c, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatal(err)
	}
	requireClosed(t, c)
	eventually(t, "the slot to free", func() bool { return f.sessions.Load() == 0 })
	if f.logs.count("upstream dial failed") != 1 {
		t.Fatal("the failed dial is not in the log")
	}
}

func TestAResetClientEndsItsSessionAndIsLogged(t *testing.T) {
	f := startServer(t, nil)
	raw := dialTCP(t, f.Listen)
	c := tls.Client(raw, f.config())
	if err := roundTrip(c); err != nil {
		t.Fatal(err)
	}
	reset(t, raw)
	eventually(t, "the failure in the log", func() bool { return f.logs.count("session failed") == 1 })
	eventually(t, "the slot to free", func() bool { return f.sessions.Load() == 0 })
}

func TestServeServesANewPairToNewSessionsWhileAnOldOneKeepsWorking(t *testing.T) {
	f := startServer(t, nil)
	old, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(old); err != nil {
		t.Fatal(err)
	}

	renewed := serving(t, f.cluster)
	writeVolume(t, f.certDir, pairFiles(t, renewed))
	eventually(t, "the new pair", func() bool { return serialServed(t, f) == renewed.Cert.SerialNumber.String() })
	if err := roundTrip(old); err != nil {
		t.Fatalf("the old session after the reload: %v", err)
	}
}

func serialServed(t *testing.T, f *serverFixture) string {
	t.Helper()
	c, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	return c.ConnectionState().PeerCertificates[0].SerialNumber.String()
}

func TestServeUsesANewClientCAForTheNextHandshake(t *testing.T) {
	f := startServer(t, nil)
	sessions := tls.NewLRUClientSessionCache(0)
	cfg := f.config()
	cfg.ClientSessionCache = sessions
	old, err := dialTLS(t, f.Listen, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(old); err != nil {
		t.Fatal(err)
	}
	resumed, err := dialTLS(t, f.Listen, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !resumed.ConnectionState().DidResume {
		t.Fatal("no session to resume, so this test proves nothing")
	}

	rotated := newCA(t, "tunnel rotated")
	writeVolume(t, f.caDir, caFiles(t, rotated))
	eventually(t, "the old client's refusal, through a cached session too", func() bool {
		c, err := dialTLS(t, f.Listen, cfg)
		if err == nil {
			err = roundTrip(c)
		}
		return err != nil
	})

	next := f.config()
	next.Certificates = []tls.Certificate{tlsPair(t, client(t, rotated))}
	c, err := dialTLS(t, f.Listen, next)
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(c); err != nil {
		t.Fatalf("a client of the new CA: %v", err)
	}
	if err := roundTrip(old); err != nil {
		t.Fatalf("the old session after the reload: %v", err)
	}
}

func TestServeKeepsTheLastGoodPair(t *testing.T) {
	for name, breakIt := range badPairVolumes(t, newCA(t, "for the bad volumes")) {
		t.Run(name, func(t *testing.T) {
			f := startServer(t, nil)
			before := serialServed(t, f)
			failures := f.logs.count("not loaded")
			breakIt(f.certDir)
			eventually(t, "the failed reload", func() bool { return f.logs.count("not loaded") > failures })
			if got := serialServed(t, f); got != before {
				t.Fatalf("serving %s, want the last good %s", got, before)
			}

			repaired := serving(t, f.cluster)
			repairVolume(t, f.certDir, pairFiles(t, repaired))
			eventually(t, "the repaired pair", func() bool { return serialServed(t, f) == repaired.Cert.SerialNumber.String() })
		})
	}
}

func TestServeKeepsTheLastGoodClientCA(t *testing.T) {
	for name, breakIt := range badCAVolumes(t, newCA(t, "for the bad volumes")) {
		t.Run(name, func(t *testing.T) {
			f := startServer(t, nil)
			failures := f.logs.count("not loaded")
			breakIt(f.caDir)
			eventually(t, "the failed reload", func() bool { return f.logs.count("not loaded") > failures })
			c, err := dialTLS(t, f.Listen, f.config())
			if err != nil {
				t.Fatal(err)
			}
			if err := roundTrip(c); err != nil {
				t.Fatalf("a client of the last good CA: %v", err)
			}

			rotated := newCA(t, "tunnel rotated")
			repairVolume(t, f.caDir, caFiles(t, rotated))
			next := f.config()
			next.Certificates = []tls.Certificate{tlsPair(t, client(t, rotated))}
			eventually(t, "the repaired CA", func() bool {
				c, err := dialTLS(t, f.Listen, next)
				return err == nil && roundTrip(c) == nil
			})
		})
	}
}

func TestServeDoesNotListenUntilItHasLoaded(t *testing.T) {
	cluster, tunnelCA := newCA(t, "cluster"), newCA(t, "tunnel")
	goodPair, goodCA := pairFiles(t, serving(t, cluster)), caFiles(t, tunnelCA)

	for name, setUp := range map[string]func(t *testing.T, f *serverFixture){
		"an empty directory": func(*testing.T, *serverFixture) {},
		"a bad pair": func(t *testing.T, f *serverFixture) {
			t.Helper()
			writeVolume(t, f.certDir, map[string][]byte{tlsCertFile: []byte("garbage"), tlsKeyFile: []byte("garbage")})
			writeVolume(t, f.caDir, goodCA)
		},
		"a bad CA": func(t *testing.T, f *serverFixture) {
			t.Helper()
			writeVolume(t, f.certDir, goodPair)
			writeVolume(t, f.caDir, map[string][]byte{caCertFile: []byte("garbage")})
		},
		"directories that do not exist yet": func(t *testing.T, f *serverFixture) {
			t.Helper()
			if err := os.RemoveAll(f.certDir); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(f.caDir); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newServerFixture(t, func(s *Server) { s.ReloadRetry = 20 * time.Millisecond }, cluster, tunnelCA)
			setUp(t, f)
			f.start(t)

			eventually(t, "a report in the log", func() bool {
				return f.logs.count("not loaded") >= 2
			})
			if c, err := net.DialTimeout("tcp", f.Listen, time.Second); err == nil {
				_ = c.Close()
				t.Fatal("listening with nothing good loaded")
			}
			if f.healthy() {
				t.Fatal("/healthz answers with no accept loop")
			}

			for dir, files := range map[string]map[string][]byte{f.certDir: goodPair, f.caDir: goodCA} {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				writeVolume(t, dir, files)
			}
			f.waitHealthy(t)
			c, err := dialTLS(t, f.Listen, f.config())
			if err != nil {
				t.Fatal(err)
			}
			if err := roundTrip(c); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServeReadsAPlainDirectoryToo(t *testing.T) {
	cluster, tunnelCA := newCA(t, "cluster"), newCA(t, "tunnel")
	f := newServerFixture(t, nil, cluster, tunnelCA)
	for name, data := range pairFiles(t, serving(t, cluster)) {
		if err := os.WriteFile(filepath.Join(f.certDir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(f.caDir, caCertFile), caFiles(t, tunnelCA)[caCertFile], 0o600); err != nil {
		t.Fatal(err)
	}
	f.start(t)
	f.waitHealthy(t)
}

func TestServeClosesAtOnceOnItsSignal(t *testing.T) {
	f := startServer(t, nil)
	session, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(session); err != nil {
		t.Fatal(err)
	}
	stalled := dialTCP(t, f.Listen)
	if !f.healthy() {
		t.Fatal("/healthz does not answer while the accept loop runs")
	}

	start := time.Now()
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("took %s to stop", took)
	}
	requireClosed(t, session)
	requireClosed(t, stalled)
	if f.healthy() {
		t.Fatal("/healthz answers after the accept loop stopped")
	}
	if c, err := net.DialTimeout("tcp", f.Listen, time.Second); err == nil {
		_ = c.Close()
		t.Fatal("still listening")
	}
}

func TestHealthClosesAConnectionAtItsTimeout(t *testing.T) {
	f := startServer(t, func(s *Server) { s.HealthTimeout = 200 * time.Millisecond })
	for name, sent := range map[string]string{
		"silent":                "",
		"idle after a request":  "GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n",
		"its body never coming": "POST /healthz HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			c := dialTCP(t, f.HealthAddr)
			if _, err := c.Write([]byte(sent)); err != nil {
				t.Fatal(err)
			}
			requireClosedAfterData(t, c)
		})
	}
}

func TestHealthClosesAClientThatDoesNotRead(t *testing.T) {
	f := startServer(t, func(s *Server) { s.HealthTimeout = 200 * time.Millisecond })
	c := dialTCP(t, f.HealthAddr)
	requests := bytes.Repeat([]byte("GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n"), 1024)
	for {
		if err := c.SetWriteDeadline(time.Now().Add(waitFor)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Write(requests); err != nil {
			if isTimeout(err) {
				t.Fatal("/healthz still holds a client that reads nothing")
			}
			return
		}
	}
}

func TestServeStopsBeforeItHasLoaded(t *testing.T) {
	f := newServerFixture(t, nil, newCA(t, "cluster"), newCA(t, "tunnel"))
	f.start(t)
	eventually(t, "the first failed load", func() bool { return f.logs.count("not loaded") >= 2 })
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
}

func TestServeFailsWhenItCannotBind(t *testing.T) {
	for name, tune := range map[string]func(s *Server, taken string){
		"the listener": func(s *Server, taken string) { s.Listen = taken },
		"/healthz":     func(s *Server, taken string) { s.HealthAddr = taken },
	} {
		t.Run(name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			cluster, tunnelCA := newCA(t, "cluster"), newCA(t, "tunnel")
			f := newServerFixture(t, func(s *Server) { tune(s, ln.Addr().String()) }, cluster, tunnelCA)
			writeVolume(t, f.certDir, pairFiles(t, serving(t, cluster)))
			writeVolume(t, f.caDir, caFiles(t, tunnelCA))
			ctx, cancel := context.WithTimeout(context.Background(), waitFor)
			defer cancel()
			var opErr *net.OpError
			if err := f.Run(ctx); !errors.As(err, &opErr) || ctx.Err() != nil {
				t.Fatalf("Run returned %v, context %v, want the bind's error at once", err, ctx.Err())
			}
		})
	}
}

type plainUpstream struct {
	addr     string
	accepted atomic.Int64
}

func startPlainUpstream(t *testing.T, handle func(*net.TCPConn)) *plainUpstream {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	u := &plainUpstream{addr: ln.Addr().String()}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			u.accepted.Add(1)
			go func() {
				defer func() { _ = c.Close() }()
				handle(c.(*net.TCPConn))
			}()
		}
	}()
	return u
}

func echo(c *net.TCPConn) {
	_, _ = io.Copy(c, c)
}

type serverFixture struct {
	*Server
	certDir, caDir string
	cluster        *certs.Certificate
	tunnelCA       *certs.Certificate
	clientPair     tls.Certificate
	upstream       *plainUpstream
	logs           *logBuffer
	stop           func() error
}

func startServer(t *testing.T, tune func(*Server)) *serverFixture {
	t.Helper()
	cluster, tunnelCA := newCA(t, "cluster"), newCA(t, "tunnel")
	f := newServerFixture(t, tune, cluster, tunnelCA)
	writeVolume(t, f.certDir, pairFiles(t, serving(t, cluster)))
	writeVolume(t, f.caDir, caFiles(t, tunnelCA))
	f.start(t)
	f.waitHealthy(t)
	return f
}

func newServerFixture(t *testing.T, tune func(*Server), cluster, tunnelCA *certs.Certificate) *serverFixture {
	t.Helper()
	log, logs := newLog(t)
	upstream := startPlainUpstream(t, echo)
	s := NewServer(freeAddr(t), tunnelHost, t.TempDir(), t.TempDir(), upstream.addr, freeAddr(t), log)
	if tune != nil {
		tune(s)
	}
	return &serverFixture{
		Server: s, certDir: s.CertDir, caDir: s.ClientCADir, cluster: cluster, tunnelCA: tunnelCA,
		clientPair: tlsPair(t, client(t, tunnelCA)), upstream: upstream, logs: logs,
	}
}

func (f *serverFixture) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx) }()
	var once sync.Once
	var result error
	f.stop = func() error {
		once.Do(func() {
			cancel()
			result = <-done
		})
		return result
	}
	t.Cleanup(func() { _ = f.stop() })
}

func (f *serverFixture) healthy() bool {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+f.HealthAddr+"/healthz", http.NoBody)
	if err != nil {
		return false
	}
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK
}

func (f *serverFixture) waitHealthy(t *testing.T) {
	t.Helper()
	eventually(t, "serve's /healthz", f.healthy)
}

func (f *serverFixture) config() *tls.Config {
	pair := f.clientPair
	return &tls.Config{ServerName: tunnelHost, RootCAs: roots(f.cluster), Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS13}
}

func TestAPanicInAServeSessionEndsOnlyThatSession(t *testing.T) {
	panicking := atomic.Bool{}
	panicking.Store(true)
	f := startServer(t, func(s *Server) {
		s.MaxSessions = 1
		dial := s.dial
		s.dial = func(ctx context.Context) (conn, error) {
			if panicking.Load() {
				panic("boom")
			}
			return dial(ctx)
		}
	})
	c, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatal(err)
	}
	requireClosed(t, c)
	eventually(t, "the panic in the log", func() bool { return f.logs.count("connection panicked") == 1 })
	panicking.Store(false)
	eventually(t, "the session slot to free", func() bool { return f.sessions.Load() == 0 })
	next, err := dialTLS(t, f.Listen, f.config())
	if err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(next); err != nil {
		t.Fatalf("a session after the panic: %v", err)
	}
}
