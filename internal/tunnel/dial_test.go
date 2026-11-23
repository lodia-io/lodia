// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var (
	dead1 = netip.MustParseAddr("192.0.2.1")
	dead2 = netip.MustParseAddr("192.0.2.2")
	dead3 = netip.MustParseAddr("192.0.2.3")
	dead4 = netip.MustParseAddr("192.0.2.4")
	live1 = netip.MustParseAddr("192.0.2.11")
	live2 = netip.MustParseAddr("192.0.2.12")
)

type connectCall struct {
	addr   netip.Addr
	at     time.Duration
	budget time.Duration
}

type pipeConn struct{ net.Conn }

func (pipeConn) CloseWrite() error { return nil }

type fakeNet struct {
	start time.Time

	mu        sync.Mutex
	behaviors map[netip.Addr]func(ctx context.Context) (conn, error)
	calls     []connectCall
	opened    []net.Conn
}

func (n *fakeNet) connect(ctx context.Context, addr netip.AddrPort) (conn, error) {
	deadline, _ := ctx.Deadline()
	n.mu.Lock()
	n.calls = append(n.calls, connectCall{addr: addr.Addr(), at: time.Since(n.start), budget: time.Until(deadline)})
	behave := n.behaviors[addr.Addr()]
	n.mu.Unlock()
	return behave(ctx)
}

func (n *fakeNet) set(addr netip.Addr, behave func(ctx context.Context) (conn, error)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.behaviors[addr] = behave
}

func (n *fakeNet) attempts() []netip.Addr {
	n.mu.Lock()
	defer n.mu.Unlock()
	var addrs []netip.Addr
	for _, c := range n.calls {
		addrs = append(addrs, c.addr)
	}
	return addrs
}

func (n *fakeNet) forget() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls = nil
	n.start = time.Now()
}

func (n *fakeNet) closeAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, c := range n.opened {
		_ = c.Close()
	}
}

func (n *fakeNet) serving(handle func(net.Conn)) func(ctx context.Context) (conn, error) {
	return func(context.Context) (conn, error) {
		client, server := net.Pipe()
		n.mu.Lock()
		n.opened = append(n.opened, client)
		n.mu.Unlock()
		go handle(server)
		return pipeConn{client}, nil
	}
}

func silent(ctx context.Context) (conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func refused(context.Context) (conn, error) {
	return nil, errors.New("connection refused")
}

type dialFixture struct {
	*Proxy
	net *fakeNet
	tls *tls.Config
}

func newDialFixture(t *testing.T, tunnel bool, addrs ...netip.Addr) *dialFixture {
	t.Helper()
	certDir, caDir := "", ""
	if tunnel {
		certDir, caDir = "c", "a"
	}
	p, err := NewProxy("240.0.0.1:6443", tunnelHost+":443", certDir, caDir, "s", discardLog())
	if err != nil {
		t.Fatal(err)
	}
	f := &dialFixture{Proxy: p, net: &fakeNet{start: time.Now(), behaviors: map[netip.Addr]func(context.Context) (conn, error){}}}
	p.connect = f.net.connect
	p.addrs.Store(&addrs)
	if tunnel {
		ca := newCA(t, "cluster")
		pair := tlsPair(t, client(t, ca))
		p.pair = pairSource(certDir, 0, p.Log)
		p.pair.current.Store(&pair)
		p.ca = caSource(caDir, 0, p.Log)
		p.ca.current.Store(roots(ca))
		f.tls = &tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{tlsPair(t, serving(t, ca))},
			ClientAuth:   tls.RequireAnyClientCert,
		}
	}
	return f
}

func (f *dialFixture) answeringAfter(wait time.Duration) func(ctx context.Context) (conn, error) {
	return f.net.serving(func(c net.Conn) {
		defer func() { _ = c.Close() }()
		time.Sleep(wait)
		if !f.tunneled() {
			_, _ = c.Write([]byte("k"))
			return
		}
		session := tls.Server(c, f.tls)
		if session.Handshake() != nil {
			return
		}
		_, _ = io.Copy(io.Discard, session)
	})
}

func (f *dialFixture) answering() func(ctx context.Context) (conn, error) {
	return f.answeringAfter(0)
}

func (f *dialFixture) resetting() func(ctx context.Context) (conn, error) {
	return f.net.serving(func(c net.Conn) { _ = c.Close() })
}

func (f *dialFixture) stalling() func(ctx context.Context) (conn, error) {
	return f.net.serving(func(c net.Conn) {
		defer func() { _ = c.Close() }()
		_, _ = io.Copy(io.Discard, c)
	})
}

func (f *dialFixture) dial(t *testing.T) (time.Duration, error) {
	t.Helper()
	start := time.Now()
	upstream, err := f.dialUpstream(context.Background())
	took := time.Since(start)
	if err != nil {
		return took, err
	}
	if !f.tunneled() {
		_, _ = upstream.Read(make([]byte, 1))
	}
	return took, nil
}

func (f *dialFixture) mustDial(t *testing.T) time.Duration {
	t.Helper()
	took, err := f.dial(t)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return took
}

func (f *dialFixture) lastAnswered() netip.Addr {
	if a := f.answered.Load(); a != nil {
		return *a
	}
	return netip.Addr{}
}

func requireAttempts(t *testing.T, f *dialFixture, want ...netip.Addr) {
	t.Helper()
	got := f.net.attempts()
	if len(got) != len(want) {
		t.Fatalf("attempts %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("attempts %v, want %v", got, want)
		}
	}
}

func requireTook(t *testing.T, got, want time.Duration) {
	t.Helper()
	if got != want {
		t.Fatalf("took %s, want %s", got, want)
	}
}

func eachMode(t *testing.T, run func(t *testing.T, tunnel bool)) {
	t.Helper()
	for name, tunnel := range map[string]bool{"plain": false, "tunnel": true} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) { run(t, tunnel) })
		})
	}
}

func TestADialAttemptsEachSilentAddressAheadOfTheAnsweringOneOnceAndCapped(t *testing.T) {
	eachMode(t, func(t *testing.T, tunnel bool) {
		f := newDialFixture(t, tunnel, dead1, dead2, live1)
		defer f.net.closeAll()
		f.net.set(dead1, silent)
		f.net.set(dead2, silent)
		f.net.set(live1, f.answering())

		requireTook(t, f.mustDial(t), 2*f.AttemptTimeout)
		requireAttempts(t, f, dead1, dead2, live1)
		calls := f.net.calls
		if calls[0].at != 0 || calls[1].at != 2*time.Second || calls[2].at != 4*time.Second {
			t.Fatalf("attempts began at %v, %v and %v", calls[0].at, calls[1].at, calls[2].at)
		}
		if calls[0].budget != 2*time.Second || calls[1].budget != 2*time.Second || calls[2].budget != 2*time.Second {
			t.Fatalf("attempts had %v, %v and %v", calls[0].budget, calls[1].budget, calls[2].budget)
		}
		if got := f.lastAnswered(); got != live1 {
			t.Fatalf("remembered %v, want %v", got, live1)
		}
	})
}

func TestADialAfterTheFirstSuccessMakesOneAttemptWhileTheAddressIsListed(t *testing.T) {
	eachMode(t, func(t *testing.T, tunnel bool) {
		f := newDialFixture(t, tunnel, dead1, dead2, live1)
		defer f.net.closeAll()
		f.net.set(dead1, silent)
		f.net.set(dead2, silent)
		f.net.set(live1, f.answering())
		f.mustDial(t)

		for _, order := range [][]netip.Addr{{dead1, dead2, live1}, {live1, dead1, dead2}, {dead2, live1, dead1}} {
			f.net.forget()
			f.addrs.Store(&order)
			requireTook(t, f.mustDial(t), 0)
			requireAttempts(t, f, live1)
		}
	})
}

func TestARefreshThatDropsTheAnsweringAddressCostsOneMoreRound(t *testing.T) {
	eachMode(t, func(t *testing.T, tunnel bool) {
		f := newDialFixture(t, tunnel, dead1, live1)
		defer f.net.closeAll()
		f.net.set(dead1, silent)
		f.net.set(live1, f.answering())
		f.net.set(live2, f.answering())
		f.mustDial(t)

		f.net.forget()
		f.addrs.Store(&[]netip.Addr{dead1, live2})
		requireTook(t, f.mustDial(t), f.AttemptTimeout)
		requireAttempts(t, f, dead1, live2)
		if got := f.lastAnswered(); got != live2 {
			t.Fatalf("remembered %v, want %v", got, live2)
		}

		f.net.forget()
		requireTook(t, f.mustDial(t), 0)
		requireAttempts(t, f, live2)
	})
}

func TestARememberedAddressThatDiesCostsEachConnectionBeforeTheNextSuccessOneAttempt(t *testing.T) {
	eachMode(t, func(t *testing.T, tunnel bool) {
		f := newDialFixture(t, tunnel, live1, live2)
		defer f.net.closeAll()
		f.net.set(live1, f.answering())
		f.net.set(live2, f.answering())
		f.mustDial(t)
		if got := f.lastAnswered(); got != live1 {
			t.Fatalf("remembered %v, want %v", got, live1)
		}

		f.net.set(live1, silent)
		f.net.forget()
		const begun = 5
		var wg sync.WaitGroup
		took := make([]time.Duration, begun)
		for i := range begun {
			wg.Go(func() { took[i] = f.mustDial(t) })
		}
		wg.Wait()
		for i, d := range took {
			if d != f.AttemptTimeout {
				t.Fatalf("connection %d took %s, want one attempt of %s", i, d, f.AttemptTimeout)
			}
		}
		if got := len(f.net.attempts()); got != 2*begun {
			t.Fatalf("%d attempts, want one at each address per connection", got)
		}

		f.net.forget()
		requireTook(t, f.mustDial(t), 0)
		requireAttempts(t, f, live2)
	})
}

func TestADialNeverTakesLongerThanItsTotal(t *testing.T) {
	for name, tc := range map[string]struct {
		addrs []netip.Addr
		took  time.Duration
		tried int
	}{
		"two silent addresses":                     {[]netip.Addr{dead1, dead2}, 4 * time.Second, 2},
		"three silent addresses":                   {[]netip.Addr{dead1, dead2, dead3}, 6 * time.Second, 3},
		"four silent addresses":                    {[]netip.Addr{dead1, dead2, dead3, dead4}, 6 * time.Second, 3},
		"an answering address behind three silent": {[]netip.Addr{dead1, dead2, dead3, live1}, 6 * time.Second, 3},
	} {
		t.Run(name, func(t *testing.T) {
			eachMode(t, func(t *testing.T, tunnel bool) {
				f := newDialFixture(t, tunnel, tc.addrs...)
				defer f.net.closeAll()
				for _, a := range tc.addrs {
					f.net.set(a, silent)
				}
				took, err := f.dial(t)
				if err == nil {
					t.Fatal("dialed")
				}
				requireTook(t, took, tc.took)
				if got := len(f.net.attempts()); got != tc.tried {
					t.Fatalf("%d attempts, want %d", got, tc.tried)
				}
				if f.answered.Load() != nil {
					t.Fatal("remembered an address that never answered")
				}
			})
		})
	}
}

func TestADialWithNoAddressesMakesNoAttempt(t *testing.T) {
	eachMode(t, func(t *testing.T, tunnel bool) {
		f := newDialFixture(t, tunnel)
		if _, err := f.dial(t); err == nil {
			t.Fatal("dialed")
		}
		requireAttempts(t, f)
	})
}

func TestADialThatIsRefusedMovesOnAtOnce(t *testing.T) {
	eachMode(t, func(t *testing.T, tunnel bool) {
		f := newDialFixture(t, tunnel, dead1, live1)
		defer f.net.closeAll()
		f.net.set(dead1, refused)
		f.net.set(live1, f.answering())
		requireTook(t, f.mustDial(t), 0)
		requireAttempts(t, f, dead1, live1)
	})
}

func TestAnAttemptAfterALongHandshakeHasOnlyWhatIsLeftOfTheTotal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newDialFixture(t, true, live1, dead1)
		defer f.net.closeAll()
		f.net.set(live1, f.answeringAfter(5*time.Second))
		f.net.set(dead1, silent)
		f.mustDial(t)

		f.net.set(live1, f.net.serving(func(c net.Conn) {
			defer func() { _ = c.Close() }()
			time.Sleep(5 * time.Second)
		}))
		f.net.forget()
		took, err := f.dial(t)
		if err == nil {
			t.Fatal("dialed")
		}
		requireTook(t, took, 6*time.Second)
		requireAttempts(t, f, live1, dead1)
		if got := f.net.calls[1]; got.at != 5*time.Second || got.budget != time.Second {
			t.Fatalf("the second attempt began at %s with %s, want 5s with the 1s that was left", got.at, got.budget)
		}
	})
}

func TestASlowHandshakeIsNotCutAndDialedAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newDialFixture(t, true, live1, live2)
		defer f.net.closeAll()
		f.net.set(live1, f.answeringAfter(5*time.Second))
		f.net.set(live2, f.answering())

		requireTook(t, f.mustDial(t), 5*time.Second)
		requireAttempts(t, f, live1)
		if got := f.lastAnswered(); got != live1 {
			t.Fatalf("remembered %v, want %v", got, live1)
		}
	})
}

func TestAHandshakeThatOutlastsTheTotalEndsTheDialWithNoSecondAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newDialFixture(t, true, live1, live2)
		defer f.net.closeAll()
		f.net.set(live1, f.stalling())
		f.net.set(live2, f.answering())

		took, err := f.dial(t)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("dial returned %v, want the deadline", err)
		}
		requireTook(t, took, f.DialTimeout)
		requireAttempts(t, f, live1)
		if f.answered.Load() != nil {
			t.Fatal("remembered an address whose handshake never finished")
		}
	})
}

func TestAHandshakeThatFailsMovesOnAndTheFailedAddressIsNotRemembered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newDialFixture(t, true, dead1, live1)
		defer f.net.closeAll()
		f.net.set(dead1, f.resetting())
		f.net.set(live1, f.answering())

		requireTook(t, f.mustDial(t), 0)
		requireAttempts(t, f, dead1, live1)
		if got := f.lastAnswered(); got != live1 {
			t.Fatalf("remembered %v, want %v", got, live1)
		}
	})
}

func TestAnAddressThatAcceptsAndResetsIsNotRemembered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newDialFixture(t, false, dead1, live1)
		defer f.net.closeAll()
		f.net.set(dead1, f.resetting())
		f.net.set(live1, f.answering())

		f.mustDial(t)
		requireAttempts(t, f, dead1)
		if f.answered.Load() != nil {
			t.Fatal("remembered an address that sent nothing")
		}

		f.addrs.Store(&[]netip.Addr{live1, dead1})
		f.net.forget()
		f.mustDial(t)
		if got := f.lastAnswered(); got != live1 {
			t.Fatalf("remembered %v, want %v", got, live1)
		}

		f.addrs.Store(&[]netip.Addr{dead1, live1})
		f.net.forget()
		f.mustDial(t)
		requireAttempts(t, f, live1)
		if got := f.lastAnswered(); got != live1 {
			t.Fatalf("remembered %v after another address reset, want %v", got, live1)
		}
	})
}

func TestAnAddressIsRememberedWhenItsFirstByteArrivesNotWhenItConnects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newDialFixture(t, false, live1)
		defer f.net.closeAll()
		f.net.set(live1, f.answeringAfter(time.Second))

		upstream, err := f.dialUpstream(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if f.answered.Load() != nil {
			t.Fatal("remembered at connect")
		}
		if _, err := upstream.Read(make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		if got := f.lastAnswered(); got != live1 {
			t.Fatalf("remembered %v, want %v", got, live1)
		}
	})
}

func TestConnectionsThatAnswerAndFailAtOnceRememberOnlyAnAddressThatAnswered(t *testing.T) {
	eachMode(t, func(t *testing.T, tunnel bool) {
		f := newDialFixture(t, tunnel, dead1, live1, live2, dead2)
		defer f.net.closeAll()
		f.net.set(dead1, silent)
		f.net.set(dead2, refused)
		f.net.set(live1, f.answering())
		f.net.set(live2, f.answeringAfter(time.Second))

		stop := make(chan struct{})
		var watcher sync.WaitGroup
		watcher.Go(func() {
			for {
				if a := f.answered.Load(); a != nil && *a != live1 && *a != live2 {
					t.Errorf("remembered %v, which never answered", *a)
					return
				}
				select {
				case <-stop:
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
		})
		orders := [][]netip.Addr{{dead1, live1, live2, dead2}, {dead2, dead1, live2, live1}, {live2, dead2, dead1, live1}, {live1, live2, dead1, dead2}}
		var dials sync.WaitGroup
		for i := range 40 {
			dials.Go(func() {
				time.Sleep(time.Duration(i%7) * 300 * time.Millisecond)
				f.addrs.Store(&orders[i%len(orders)])
				if _, err := f.dial(t); err != nil {
					t.Errorf("dial %d: %v", i, err)
				}
			})
		}
		dials.Wait()
		close(stop)
		watcher.Wait()
		if got := f.lastAnswered(); got != live1 && got != live2 {
			t.Fatalf("remembered %v, want an address that answered", got)
		}
	})
}
