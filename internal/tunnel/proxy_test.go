// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.lodia.io/lodia/internal/certs"
)

func TestProxyBoundsAreTheDesigns(t *testing.T) {
	p, err := NewProxy("240.0.0.1:6443", "tunnel.lodia.test:443", "c", "a", "s", nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.MaxConns != 1000 || p.MaxPerIP != 128 || p.DialTimeout != 10*time.Second || p.RefreshPeriod != 30*time.Second || p.ShutdownWait != 30*time.Second {
		t.Fatalf("conns %d, per IP %d, dial %s, refresh %s, wait %s", p.MaxConns, p.MaxPerIP, p.DialTimeout, p.RefreshPeriod, p.ShutdownWait)
	}
}

func TestProxyRefusesAnUpstreamWithoutAPort(t *testing.T) {
	for _, upstream := range []string{"tunnel.lodia.test", "tunnel.lodia.test:https", "tunnel.lodia.test:70000"} {
		if _, err := NewProxy("240.0.0.1:6443", upstream, "c", "a", "s", nil); err == nil {
			t.Errorf("accepted %q", upstream)
		}
	}
}

type tunnelFixture struct {
	server *serverFixture
	proxy  *proxyFixture
}

func startTunnel(t *testing.T, handle func(*net.TCPConn)) *tunnelFixture {
	t.Helper()
	server := startServer(t, func(s *Server) {
		s.Upstream = startPlainUpstream(t, handle).addr
	})
	_, port, err := net.SplitHostPort(server.Listen)
	if err != nil {
		t.Fatal(err)
	}
	proxy := startProxy(t, net.JoinHostPort(tunnelHost, port),
		pairFiles(t, client(t, server.tunnelCA)), caFiles(t, server.cluster),
		func(p *Proxy) { p.Resolver = always("127.0.0.1") })
	eventually(t, "the first lookup", func() bool { return proxy.addrs.Load() != nil })
	return &tunnelFixture{server: server, proxy: proxy}
}

func TestProxyAndServeCarryARequestAndItsEcho(t *testing.T) {
	f := startTunnel(t, echo)
	c := dialTCP(t, f.proxy.Listen)
	for range 3 {
		if err := roundTrip(c); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHalfCloseCrossesTheTunnelFromTheClient(t *testing.T) {
	f := startTunnel(t, func(c *net.TCPConn) {
		got, err := io.ReadAll(c)
		if err != nil {
			return
		}
		_, _ = c.Write(append([]byte("got "), got...))
	})
	c := dialTCP(t, f.proxy.Listen)
	if err := c.SetDeadline(time.Now().Add(waitFor)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "got request" {
		t.Fatalf("read %q", got)
	}
}

func TestHalfCloseCrossesTheTunnelFromTheUpstream(t *testing.T) {
	received := make(chan string, 1)
	f := startTunnel(t, func(c *net.TCPConn) {
		if _, err := c.Write([]byte("banner")); err != nil {
			return
		}
		if err := c.CloseWrite(); err != nil {
			return
		}
		got, err := io.ReadAll(c)
		if err != nil {
			return
		}
		received <- string(got)
	})
	c := dialTCP(t, f.proxy.Listen)
	if err := c.SetDeadline(time.Now().Add(waitFor)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 16)
	n, err := io.ReadAtLeast(c, got, len("banner"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got[:n]) != "banner" {
		t.Fatalf("read %q", got[:n])
	}
	if _, err := c.Read(got); !errors.Is(err, io.EOF) {
		t.Fatalf("read after the upstream's half-close returned %v, want EOF", err)
	}
	if _, err := c.Write([]byte("late")); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case g := <-received:
		if g != "late" {
			t.Fatalf("the upstream read %q", g)
		}
	case <-time.After(waitFor):
		t.Fatal("the upstream read nothing after its own half-close")
	}
}

func TestAClosedSideEndsTheOther(t *testing.T) {
	f := startTunnel(t, func(c *net.TCPConn) {
		_, _ = c.Write([]byte("x"))
	})
	requireClosedAfterData(t, dialTCP(t, f.proxy.Listen))
}

func requireClosedAfterData(t *testing.T, c net.Conn) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(waitFor)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(c); err != nil {
		t.Fatal(err)
	}
}

func TestAResetClientEndsItsConnectionAndIsLogged(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	f := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), nil)
	c := dialTCP(t, f.Listen)
	if err := roundTrip(c); err != nil {
		t.Fatal(err)
	}
	reset(t, c)
	eventually(t, "the failure in the log", func() bool { return f.logs.count("connection failed") == 1 })
}

func TestProxyRefusesAServer(t *testing.T) {
	ca := newCA(t, "cluster")
	otherCA := newCA(t, "other")
	profile := certs.Profile{
		CommonName: tunnelHost, DNSNames: []string{tunnelHost}, IPAddresses: []net.IP{loopbackIP},
		Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	for _, tc := range []struct {
		name string
		leaf *certs.Certificate
		tune func(*tls.Config)
	}{
		{name: "from another CA", leaf: serving(t, otherCA)},
		{name: "with a certificate for another name", leaf: issue(t, ca, certs.Profile{
			CommonName: "other.lodia.test", DNSNames: []string{"other.lodia.test"}, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}, time.Now())},
		{name: "expired", leaf: expired(t, ca, profile)},
		{name: "with a certificate without serverAuth", leaf: client(t, ca)},
		{name: "on TLS 1.2", leaf: serving(t, ca), tune: func(c *tls.Config) { c.MinVersion, c.MaxVersion = tls.VersionTLS12, tls.VersionTLS12 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := startTLSUpstream(t, "127.0.0.1:0", tc.leaf, tc.tune)
			f := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), nil)
			c := dialTCP(t, f.Listen)
			if _, err := c.Write([]byte("secret")); err != nil {
				t.Fatal(err)
			}
			requireClosed(t, c)
			if n := upstream.received.Load(); n != 0 {
				t.Fatalf("the server got %d bytes", n)
			}
		})
	}

	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	f := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), nil)
	if err := tripThrough(t, f.Listen); err != nil {
		t.Fatalf("the server those were refused against: %v", err)
	}
}

func TestProxyResumesSessions(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	f := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), nil)
	if err := tripThrough(t, f.Listen); err != nil {
		t.Fatal(err)
	}
	if upstream.lastResumed() {
		t.Fatal("the first connection resumed")
	}
	if err := tripThrough(t, f.Listen); err != nil {
		t.Fatal(err)
	}
	if !upstream.lastResumed() {
		t.Fatal("the second connection did not resume")
	}
}

func TestProxyClosesOverEitherCapAtOnce(t *testing.T) {
	for name, tune := range map[string]func(*Proxy){
		"connections":             func(p *Proxy) { p.MaxConns = 2 },
		"connections per address": func(p *Proxy) { p.MaxPerIP = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			ca := newCA(t, "cluster")
			upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
			f := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), tune)

			first, second := dialTCP(t, f.Listen), dialTCP(t, f.Listen)
			for _, c := range []net.Conn{first, second} {
				if err := roundTrip(c); err != nil {
					t.Fatal(err)
				}
			}
			requireClosed(t, dialTCP(t, f.Listen))

			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			eventually(t, "the freed slot", func() bool { return tripThrough(t, f.Listen) == nil })
			if err := roundTrip(second); err != nil {
				t.Fatalf("a connection under the cap after the refusal: %v", err)
			}
		})
	}
}

func TestTwoProxiesShareThePort(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	first := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), nil)
	listen := first.Listen
	second := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), func(p *Proxy) { p.Listen = listen })
	if err := Probe(context.Background(), second.Socket); err != nil {
		t.Fatal(err)
	}
	if err := tripThrough(t, listen); err != nil {
		t.Fatal(err)
	}
}

func TestProxyDialsNothingBeforeTheFirstAnswer(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	_, port, err := net.SplitHostPort(upstream.addr)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	resolver := &fakeResolver{answer: func(int64) ([]netip.Addr, error) {
		<-release
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	}}
	f := startProxy(t, net.JoinHostPort(tunnelHost, port), pairFiles(t, client(t, ca)), caFiles(t, ca), func(p *Proxy) { p.Resolver = resolver })

	requireClosed(t, dialTCP(t, f.Listen))
	if f.logs.count("no addresses") != 1 {
		t.Fatal("the failed dial is not in the log")
	}
	upstream.mu.Lock()
	dialled := len(upstream.resumed) + len(upstream.failures)
	upstream.mu.Unlock()
	if dialled != 0 {
		t.Fatalf("the upstream was dialled %d times", dialled)
	}

	close(release)
	eventually(t, "the first answer", func() bool { return f.addrs.Load() != nil })
	if err := tripThrough(t, f.Listen); err != nil {
		t.Fatal(err)
	}
}

func TestAFailedRefreshKeepsTheLastGoodAndAnAnswerReplacesIt(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	_, port, err := net.SplitHostPort(upstream.addr)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	answer := []netip.Addr{netip.MustParseAddr("::1"), netip.MustParseAddr("127.0.0.1")}
	var failure error
	setAnswer := func(addrs []netip.Addr, err error) {
		mu.Lock()
		defer mu.Unlock()
		answer, failure = addrs, err
	}
	resolver := &fakeResolver{answer: func(int64) ([]netip.Addr, error) {
		mu.Lock()
		defer mu.Unlock()
		return answer, failure
	}}
	f := startProxy(t, net.JoinHostPort(tunnelHost, port), pairFiles(t, client(t, ca)), caFiles(t, ca), func(p *Proxy) {
		p.Resolver = resolver
		p.RefreshPeriod = 5 * time.Millisecond
	})
	eventually(t, "the first lookup", func() bool { return f.addrs.Load() != nil })
	if err := tripThrough(t, f.Listen); err != nil {
		t.Fatal(err)
	}

	setAnswer(nil, errors.New("resolver down"))
	seen := resolver.calls.Load()
	eventually(t, "a failed refresh", func() bool { return resolver.calls.Load() > seen+1 })
	if f.logs.count("lookup failed") != 1 {
		t.Fatal("the failed lookup is not in the log once")
	}
	if got := *f.addrs.Load(); len(got) != 2 {
		t.Fatalf("addresses %v, want the last good", got)
	}
	if err := tripThrough(t, f.Listen); err != nil {
		t.Fatalf("a dial after the failed refresh: %v", err)
	}

	setAnswer([]netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil)
	eventually(t, "the new answer", func() bool { return len(*f.addrs.Load()) == 1 })
}

func TestAnUpstreamThatIsAnIPSkipsTheResolver(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	resolver := always("192.0.2.1")
	f := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), func(p *Proxy) { p.Resolver = resolver })
	if err := tripThrough(t, f.Listen); err != nil {
		t.Fatal(err)
	}
	if n := resolver.calls.Load(); n != 0 {
		t.Fatalf("the resolver was asked %d times", n)
	}
}

func TestADialTriesTheAddressesInTurn(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	_, port, err := net.SplitHostPort(upstream.addr)
	if err != nil {
		t.Fatal(err)
	}
	f := startProxy(t, net.JoinHostPort(tunnelHost, port), pairFiles(t, client(t, ca)), caFiles(t, ca), func(p *Proxy) {
		p.Resolver = always("::1", "127.0.0.1")
	})
	eventually(t, "the first lookup", func() bool { return f.addrs.Load() != nil })
	if err := tripThrough(t, f.Listen); err != nil {
		t.Fatal(err)
	}
}

func TestASilentAddressLeavesTheNextItsShareOfTheDial(t *testing.T) {
	silent, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	defer func() { _ = silent.Close() }()
	_, port, err := net.SplitHostPort(silent.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ca := newCA(t, "cluster")
	startTLSUpstream(t, net.JoinHostPort("127.0.0.1", port), serving(t, ca), nil)
	f := startProxy(t, net.JoinHostPort(tunnelHost, port), pairFiles(t, client(t, ca)), caFiles(t, ca), func(p *Proxy) {
		p.Resolver = always("::1", "127.0.0.1")
		p.DialTimeout = 2 * time.Second
	})
	eventually(t, "the first lookup", func() bool { return f.addrs.Load() != nil })
	start := time.Now()
	if err := tripThrough(t, f.Listen); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 900*time.Millisecond || took > 2*time.Second {
		t.Fatalf("took %s, want about half of the 2s dial on the silent address", took)
	}
}

func TestADialStopsAtItsTimeout(t *testing.T) {
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = silent.Close() }()
	ca := newCA(t, "cluster")
	f := startProxy(t, silent.Addr().String(), pairFiles(t, client(t, ca)), caFiles(t, ca), func(p *Proxy) { p.DialTimeout = 200 * time.Millisecond })
	start := time.Now()
	requireClosed(t, dialTCP(t, f.Listen))
	if took := time.Since(start); took < 200*time.Millisecond {
		t.Fatalf("gave up after %s", took)
	}
	if f.logs.count("upstream dial failed") != 1 {
		t.Fatal("the failed dial is not in the log")
	}
}

func TestProxyPresentsANewPairToNewSessionsWhileAnOldOneKeepsWorking(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), noTickets)
	first := client(t, ca)
	f := startProxy(t, upstream.addr, pairFiles(t, first), caFiles(t, ca), nil)
	old := dialTCP(t, f.Listen)
	if err := roundTrip(old); err != nil {
		t.Fatal(err)
	}
	if got := upstream.lastSerial(); got != first.Cert.SerialNumber.String() {
		t.Fatalf("presented %s", got)
	}

	renewed := client(t, ca)
	writeVolume(t, f.certDir, pairFiles(t, renewed))
	eventually(t, "the new pair", func() bool {
		return tripThrough(t, f.Listen) == nil && upstream.lastSerial() == renewed.Cert.SerialNumber.String()
	})
	if err := roundTrip(old); err != nil {
		t.Fatalf("the old session after the reload: %v", err)
	}
}

func TestProxyUsesANewCAPoolForTheNextDial(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	f := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), nil)
	old := dialTCP(t, f.Listen)
	if err := roundTrip(old); err != nil {
		t.Fatal(err)
	}
	if err := tripThrough(t, f.Listen); err != nil {
		t.Fatal(err)
	}
	if !upstream.lastResumed() {
		t.Fatal("no session to resume, so this test proves nothing")
	}

	rotated := newCA(t, "cluster rotated")
	writeVolume(t, f.caDir, caFiles(t, rotated))
	eventually(t, "the old server's refusal, through a cached session too", func() bool {
		return tripThrough(t, f.Listen) != nil
	})
	if err := roundTrip(old); err != nil {
		t.Fatalf("the old session after the reload: %v", err)
	}

	pair := tlsPair(t, serving(t, rotated))
	upstream.cert.Store(&pair)
	if err := tripThrough(t, f.Listen); err != nil {
		t.Fatalf("a server of the new CA: %v", err)
	}
}

func TestProxyKeepsTheLastGoodPair(t *testing.T) {
	ca := newCA(t, "cluster")
	for name, breakIt := range badPairVolumes(t, ca) {
		t.Run(name, func(t *testing.T) {
			upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), noTickets)
			good := client(t, ca)
			f := startProxy(t, upstream.addr, pairFiles(t, good), caFiles(t, ca), nil)
			failures := f.logs.count("not loaded")
			breakIt(f.certDir)
			eventually(t, "the failed reload", func() bool { return f.logs.count("not loaded") > failures })
			if err := tripThrough(t, f.Listen); err != nil {
				t.Fatal(err)
			}
			if got := upstream.lastSerial(); got != good.Cert.SerialNumber.String() {
				t.Fatalf("presented %s, want the last good", got)
			}

			repaired := client(t, ca)
			repairVolume(t, f.certDir, pairFiles(t, repaired))
			eventually(t, "the repaired pair", func() bool {
				return tripThrough(t, f.Listen) == nil && upstream.lastSerial() == repaired.Cert.SerialNumber.String()
			})
		})
	}
}

func TestProxyKeepsTheLastGoodCAPool(t *testing.T) {
	ca := newCA(t, "cluster")
	for name, breakIt := range badCAVolumes(t, ca) {
		t.Run(name, func(t *testing.T) {
			upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), noTickets)
			f := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), nil)
			failures := f.logs.count("not loaded")
			breakIt(f.caDir)
			eventually(t, "the failed reload", func() bool { return f.logs.count("not loaded") > failures })
			if err := tripThrough(t, f.Listen); err != nil {
				t.Fatalf("a server of the last good CA: %v", err)
			}

			repairVolume(t, f.caDir, caFiles(t, newCA(t, "cluster rotated")))
			eventually(t, "the repaired pool", func() bool { return tripThrough(t, f.Listen) != nil })
		})
	}
}

func TestProxyDoesNotListenUntilItHasLoaded(t *testing.T) {
	ca := newCA(t, "cluster")
	goodPair, goodCA := pairFiles(t, client(t, ca)), caFiles(t, ca)
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)

	for name, setUp := range map[string]func(t *testing.T, f *proxyFixture){
		"an empty directory": func(*testing.T, *proxyFixture) {},
		"a bad pair": func(t *testing.T, f *proxyFixture) {
			t.Helper()
			writeVolume(t, f.certDir, map[string][]byte{tlsCertFile: []byte("garbage"), tlsKeyFile: []byte("garbage")})
			writeVolume(t, f.caDir, goodCA)
		},
		"a bad CA": func(t *testing.T, f *proxyFixture) {
			t.Helper()
			writeVolume(t, f.certDir, goodPair)
			writeVolume(t, f.caDir, map[string][]byte{caCertFile: []byte("garbage")})
		},
		"directories that do not exist yet": func(t *testing.T, f *proxyFixture) {
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
			f := newProxyFixture(t, upstream.addr, func(p *Proxy) { p.ReloadRetry = 20 * time.Millisecond })
			setUp(t, f)
			f.start(t)

			eventually(t, "a report in the log", func() bool { return f.logs.count("not loaded") >= 2 })
			if c, err := net.DialTimeout("tcp", f.Listen, time.Second); err == nil {
				_ = c.Close()
				t.Fatal("listening with nothing good loaded")
			}
			if err := Probe(context.Background(), f.Socket); err == nil {
				t.Fatal("probe answered with no accept loop")
			}

			for dir, files := range map[string]map[string][]byte{f.certDir: goodPair, f.caDir: goodCA} {
				if err := os.MkdirAll(dir, 0o700); err != nil {
					t.Fatal(err)
				}
				writeVolume(t, dir, files)
			}
			f.waitReady(t)
			if err := tripThrough(t, f.Listen); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProbeAnswersWhileTheAcceptLoopRuns(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	f := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), nil)
	if err := Probe(context.Background(), f.Socket); err != nil {
		t.Fatal(err)
	}
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
	if err := Probe(context.Background(), f.Socket); err == nil {
		t.Fatal("probe answered after the accept loop stopped")
	}
}

func TestProbeFailsOnASocketThatDoesNotAnswer(t *testing.T) {
	missing := socketPath(t)
	if err := Probe(context.Background(), missing); err == nil {
		t.Fatal("probe answered with no socket")
	}

	path := socketPath(t)
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	if err := Probe(context.Background(), path); err == nil {
		t.Fatal("probe answered with no one accepting")
	}
}

func TestProxyStartsOverALeftover(t *testing.T) {
	for name, leave := range map[string]func(t *testing.T, path string){
		"a socket": func(t *testing.T, path string) {
			t.Helper()
			ln, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			ln.(*net.UnixListener).SetUnlinkOnClose(false)
			if err := ln.Close(); err != nil {
				t.Fatal(err)
			}
			if err := Probe(context.Background(), path); err == nil {
				t.Fatal("probe answered on the leftover")
			}
		},
		"a file": func(t *testing.T, path string) {
			t.Helper()
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			ca := newCA(t, "cluster")
			upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
			f := newProxyFixture(t, upstream.addr, nil)
			leave(t, f.Socket)
			writeVolume(t, f.certDir, pairFiles(t, client(t, ca)))
			writeVolume(t, f.caDir, caFiles(t, ca))
			f.start(t)
			f.waitReady(t)
			if err := tripThrough(t, f.Listen); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProxyStopsBeforeItHasLoaded(t *testing.T) {
	f := newProxyFixture(t, "127.0.0.1:1", nil)
	f.start(t)
	eventually(t, "the first failed load", func() bool { return f.logs.count("not loaded") >= 2 })
	if err := f.stop(); err != nil {
		t.Fatal(err)
	}
}

func TestProxyFailsWhenItCannotBind(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = taken.Close() }()

	for name, tune := range map[string]func(*Proxy){
		"the listener":           func(p *Proxy) { p.Listen = taken.Addr().String() },
		"the socket's directory": func(p *Proxy) { p.Socket = t.TempDir() + "/missing/health.sock" },
		"a directory at the socket's path": func(p *Proxy) {
			p.Socket = t.TempDir()
			if err := os.WriteFile(p.Socket+"/file", nil, 0o600); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newProxyFixture(t, upstream.addr, tune)
			writeVolume(t, f.certDir, pairFiles(t, client(t, ca)))
			writeVolume(t, f.caDir, caFiles(t, ca))
			ctx, cancel := context.WithTimeout(context.Background(), waitFor)
			defer cancel()
			if err := f.Run(ctx); err == nil || ctx.Err() != nil {
				t.Fatalf("Run returned %v, context %v, want the bind's error at once", err, ctx.Err())
			}
		})
	}
}

func TestProxyOnItsSignalClosesTheListenerAndWaitsForItsConnections(t *testing.T) {
	const wait = time.Second
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	f := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), func(p *Proxy) { p.ShutdownWait = wait })

	finishes, staysOpen := dialTCP(t, f.Listen), dialTCP(t, f.Listen)
	for _, c := range []net.Conn{finishes, staysOpen} {
		if err := roundTrip(c); err != nil {
			t.Fatal(err)
		}
	}

	start := time.Now()
	stopped := make(chan error, 1)
	go func() { stopped <- f.stop() }()

	eventually(t, "the listener to close", func() bool {
		c, err := net.DialTimeout("tcp", f.Listen, time.Second)
		if err != nil {
			return true
		}
		_ = c.Close()
		return false
	})
	eventually(t, "probe to stop answering", func() bool { return Probe(context.Background(), f.Socket) != nil })
	if err := roundTrip(finishes); err != nil {
		t.Fatalf("an open connection during the wait: %v", err)
	}
	if err := finishes.Close(); err != nil {
		t.Fatal(err)
	}
	if err := roundTrip(staysOpen); err != nil {
		t.Fatalf("a connection still open during the wait: %v", err)
	}

	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(waitFor):
		t.Fatal("did not stop")
	}
	if took := time.Since(start); took < wait {
		t.Fatalf("stopped after %s, before its wait of %s", took, wait)
	}
	requireClosed(t, staysOpen)
}

func TestProxyStopsBeforeItsWaitWhenItsConnectionsHaveFinished(t *testing.T) {
	ca := newCA(t, "cluster")
	upstream := startTLSUpstream(t, "127.0.0.1:0", serving(t, ca), nil)
	f := startProxy(t, upstream.addr, pairFiles(t, client(t, ca)), caFiles(t, ca), nil)
	c := dialTCP(t, f.Listen)
	if err := roundTrip(c); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- f.stop() }()
	eventually(t, "the listener to close", func() bool {
		probe, err := net.DialTimeout("tcp", f.Listen, time.Second)
		if err != nil {
			return true
		}
		_ = probe.Close()
		return false
	})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(waitFor):
		t.Fatalf("still waiting %s after its last connection finished", waitFor)
	}
}

func TestAnExpiredClientPairIsRefusedUntilItsRenewalLoads(t *testing.T) {
	f := startTunnel(t, echo)
	writeVolume(t, f.proxy.certDir, pairFiles(t, expired(t, f.server.tunnelCA, certs.Profile{
		CommonName: "lodia-tunnel-client", Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})))
	eventually(t, "the expired pair's refusal", func() bool { return tripThrough(t, f.proxy.Listen) != nil })

	writeVolume(t, f.proxy.certDir, pairFiles(t, client(t, f.server.tunnelCA)))
	eventually(t, "the renewed pair", func() bool { return tripThrough(t, f.proxy.Listen) == nil })
}

func TestAnExpiredServingPairIsRefusedUntilItsRenewalLoads(t *testing.T) {
	f := startTunnel(t, echo)
	writeVolume(t, f.server.certDir, pairFiles(t, expired(t, f.server.cluster, certs.Profile{
		CommonName: tunnelHost, DNSNames: []string{tunnelHost}, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})))
	eventually(t, "the loaded pair", func() bool { return f.server.logs.count("loaded") >= 3 })
	cfg := f.server.config()
	if _, err := dialTLS(t, f.server.Listen, cfg); err == nil {
		t.Fatal("a client accepted the expired pair")
	}
	if !f.server.healthy() {
		t.Fatal("an expired pair stopped the accept loop")
	}

	writeVolume(t, f.server.certDir, pairFiles(t, serving(t, f.server.cluster)))
	eventually(t, "the renewed pair", func() bool {
		c, err := dialTLS(t, f.server.Listen, cfg)
		return err == nil && roundTrip(c) == nil
	})
}

func TestATunnelCARotationGetsPastCachedSessions(t *testing.T) {
	f := startTunnel(t, echo)
	if err := tripThrough(t, f.proxy.Listen); err != nil {
		t.Fatal(err)
	}
	if err := tripThrough(t, f.proxy.Listen); err != nil {
		t.Fatal(err)
	}

	rotated := newCA(t, "tunnel rotated")
	writeVolume(t, f.server.caDir, caFiles(t, f.server.tunnelCA, rotated))
	writeVolume(t, f.proxy.certDir, pairFiles(t, client(t, rotated)))
	eventually(t, "the bundle of both", func() bool { return f.server.logs.count("loaded") >= 3 && f.proxy.logs.count("loaded") >= 3 })
	writeVolume(t, f.server.caDir, caFiles(t, rotated))
	eventually(t, "the new CA alone", func() bool { return f.server.logs.count("loaded") >= 4 })
	eventually(t, "a session of the new client", func() bool { return tripThrough(t, f.proxy.Listen) == nil })
}

type tlsUpstream struct {
	addr     string
	cert     atomic.Pointer[tls.Certificate]
	received atomic.Int64

	mu       sync.Mutex
	serials  []string
	resumed  []bool
	failures []error
}

func startTLSUpstream(t *testing.T, addr string, leaf *certs.Certificate, tune func(*tls.Config)) *tlsUpstream {
	t.Helper()
	u := &tlsUpstream{}
	pair := tlsPair(t, leaf)
	u.cert.Store(&pair)
	cfg := &tls.Config{
		MinVersion:     tls.VersionTLS13,
		ClientAuth:     tls.RequireAnyClientCert,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return u.cert.Load(), nil },
	}
	if tune != nil {
		tune(cfg)
	}
	ln, err := tls.Listen("tcp", addr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	u.addr = ln.Addr().String()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go u.handle(c.(*tls.Conn))
		}
	}()
	return u
}

func (u *tlsUpstream) handle(c *tls.Conn) {
	defer func() { _ = c.Close() }()
	if err := c.HandshakeContext(context.Background()); err != nil {
		u.mu.Lock()
		u.failures = append(u.failures, err)
		u.mu.Unlock()
		return
	}
	state := c.ConnectionState()
	u.mu.Lock()
	if len(state.PeerCertificates) > 0 {
		u.serials = append(u.serials, state.PeerCertificates[0].SerialNumber.String())
	}
	u.resumed = append(u.resumed, state.DidResume)
	u.mu.Unlock()
	buf := make([]byte, 1024)
	for {
		n, err := c.Read(buf)
		u.received.Add(int64(n))
		if n > 0 {
			if _, err := c.Write(buf[:n]); err != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (u *tlsUpstream) lastSerial() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.serials) == 0 {
		return ""
	}
	return u.serials[len(u.serials)-1]
}

func (u *tlsUpstream) lastResumed() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.resumed) > 0 && u.resumed[len(u.resumed)-1]
}

func noTickets(cfg *tls.Config) {
	cfg.SessionTicketsDisabled = true
}

type fakeResolver struct {
	calls  atomic.Int64
	answer func(call int64) ([]netip.Addr, error)
}

func (r *fakeResolver) LookupNetIP(_ context.Context, _, _ string) ([]netip.Addr, error) {
	return r.answer(r.calls.Add(1))
}

func always(addrs ...string) *fakeResolver {
	parsed := make([]netip.Addr, len(addrs))
	for i, a := range addrs {
		parsed[i] = netip.MustParseAddr(a)
	}
	return &fakeResolver{answer: func(int64) ([]netip.Addr, error) { return parsed, nil }}
}

type proxyFixture struct {
	*Proxy
	certDir, caDir string
	logs           *logBuffer
	stop           func() error
}

func newProxyFixture(t *testing.T, upstream string, tune func(*Proxy)) *proxyFixture {
	t.Helper()
	log, logs := newLog(t)
	p, err := NewProxy(freeAddr(t), upstream, t.TempDir(), t.TempDir(), socketPath(t), log)
	if err != nil {
		t.Fatal(err)
	}
	if tune != nil {
		tune(p)
	}
	return &proxyFixture{Proxy: p, certDir: p.CertDir, caDir: p.CADir, logs: logs}
}

func (f *proxyFixture) start(t *testing.T) {
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

func (f *proxyFixture) waitReady(t *testing.T) {
	t.Helper()
	eventually(t, "proxy's socket", func() bool { return Probe(context.Background(), f.Socket) == nil })
}

func startProxy(t *testing.T, upstream string, pair, ca map[string][]byte, tune func(*Proxy)) *proxyFixture {
	t.Helper()
	f := newProxyFixture(t, upstream, tune)
	writeVolume(t, f.certDir, pair)
	writeVolume(t, f.caDir, ca)
	f.start(t)
	f.waitReady(t)
	return f
}

func TestAPanicInAProxyConnectionEndsOnlyThatConnection(t *testing.T) {
	log, logs := newLog(t)
	p, err := NewProxy("", "127.0.0.1:1", "", "", "", log)
	if err != nil {
		t.Fatal(err)
	}
	p.MaxConns = 1
	panicking := atomic.Bool{}
	panicking.Store(true)
	upstream := startPlainUpstream(t, echo)
	p.dial = func(ctx context.Context) (conn, error) {
		if panicking.Load() {
			panic("boom")
		}
		c, err := tcpDialer().DialContext(ctx, "tcp", upstream.addr)
		if err != nil {
			return nil, err
		}
		return c.(*net.TCPConn), nil
	}
	ln, err := listenTCP(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	go func() { _ = accept(ctx, ln, log, p.admit(ctx)) }()

	requireClosed(t, dialTCP(t, ln.Addr().String()))
	eventually(t, "the panic in the log", func() bool { return logs.count("connection panicked") == 1 })
	panicking.Store(false)
	eventually(t, "a connection after the panic, in the slot it released", func() bool {
		return tripThrough(t, ln.Addr().String()) == nil
	})
}

func TestConnLimitsCountPerAddressAndInAll(t *testing.T) {
	first, second, third := netip.MustParseAddr("10.0.0.1"), netip.MustParseAddr("10.0.0.2"), netip.MustParseAddr("10.0.0.3")
	l := connLimits{perIP: map[netip.Addr]int{}}
	for i, step := range []struct {
		ip   netip.Addr
		want bool
	}{{first, true}, {first, true}, {first, false}, {second, true}, {second, false}, {third, false}} {
		if got := l.acquire(step.ip, 3, 2); got != step.want {
			t.Fatalf("step %d, %s: acquired %t, want %t", i, step.ip, got, step.want)
		}
	}
	l.release(first)
	if !l.acquire(third, 3, 2) {
		t.Fatal("a freed slot was not given to another address")
	}
	l.release(first)
	if _, kept := l.perIP[first]; kept {
		t.Fatal("an address with no connections stayed in the map")
	}
	if l.total != 2 {
		t.Fatalf("total %d", l.total)
	}
}
