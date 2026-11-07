// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"context"
	"errors"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type fakeKernel struct {
	mu        sync.Mutex
	addresses []HostAddress
	routes    []HostRoute
	added     []HostAddress
	deleted   []HostAddress

	listErr      error
	routesErr    error
	addErr       []error
	deleteErr    error
	subscribeErr []error
	subscribed   []chan struct{}
}

func (k *fakeKernel) Addresses() ([]HostAddress, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.addresses), k.listErr
}

func (k *fakeKernel) Routes() ([]HostRoute, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Clone(k.routes), k.routesErr
}

func (k *fakeKernel) AddAddress(a HostAddress) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.added = append(k.added, a)
	if len(k.addErr) > 0 {
		var err error
		err, k.addErr = k.addErr[0], k.addErr[1:]
		if err != nil {
			return err
		}
	}
	for _, have := range k.addresses {
		if have.Interface == a.Interface && have.Prefix == a.Prefix {
			return syscall.EEXIST
		}
	}
	k.addresses = append(k.addresses, a)
	return nil
}

func (k *fakeKernel) DeleteAddress(a HostAddress) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.deleted = append(k.deleted, a)
	if k.deleteErr != nil {
		return k.deleteErr
	}
	k.remove(a)
	return nil
}

func (k *fakeKernel) remove(a HostAddress) {
	k.addresses = slices.DeleteFunc(k.addresses, func(have HostAddress) bool {
		return have.Interface == a.Interface && have.Prefix == a.Prefix
	})
}

func (k *fakeKernel) Subscribe(context.Context) (<-chan struct{}, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.subscribeErr) > 0 {
		var err error
		err, k.subscribeErr = k.subscribeErr[0], k.subscribeErr[1:]
		if err != nil {
			return nil, err
		}
	}
	ch := make(chan struct{}, 1)
	k.subscribed = append(k.subscribed, ch)
	return ch, nil
}

func (k *fakeKernel) holds(want HostAddress) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return slices.Contains(k.addresses, want)
}

func (k *fakeKernel) addCalls() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.added)
}

func (k *fakeKernel) subscriptions() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.subscribed)
}

func (k *fakeKernel) lastSubscription() chan struct{} {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.subscribed[len(k.subscribed)-1]
}

func prefix(s string) netip.Prefix {
	return netip.MustParsePrefix(s)
}

const advertised = "240.0.0.1"

var heldV4 = HostAddress{Interface: "lo", Prefix: prefix(advertised + "/32"), Label: "lo:lodia"}

func runAddress(t *testing.T, k *fakeKernel, ip string, tune func(*Address)) (*logBuffer, func() error) {
	t.Helper()
	log, logs := newLog(t)
	a := NewAddress(netip.MustParseAddr(ip), k, log)
	if tune != nil {
		tune(a)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	var once sync.Once
	var result error
	stop := func() error {
		once.Do(func() {
			cancel()
			result = <-done
		})
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return logs, stop
}

func TestAddressRefusesAnOnLinkAddressNamingTheInterface(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ip        string
		addresses []HostAddress
		routes    []HostRoute
		iface     string
	}{
		{
			name: "by the prefix of an address", ip: advertised, iface: "eth0",
			addresses: []HostAddress{{Interface: "eth0", Prefix: prefix("240.0.0.5/24")}},
		},
		{
			name: "by the same address on another interface", ip: advertised, iface: "dummy0",
			addresses: []HostAddress{{Interface: "dummy0", Prefix: prefix(advertised + "/32")}},
		},
		{
			name: "by a route without a gateway", ip: advertised, iface: "eth1",
			routes: []HostRoute{{Interface: "eth1", Prefix: prefix("240.0.0.0/8")}},
		},
		{
			name: "IPv6, by the prefix of an address", ip: "fd00:10::1", iface: "eth0",
			addresses: []HostAddress{{Interface: "eth0", Prefix: prefix("fd00:10::5/64")}},
		},
		{
			name: "IPv6, by a route without a gateway", ip: "fd00:10::1", iface: "eth1",
			routes: []HostRoute{{Interface: "eth1", Prefix: prefix("fd00:10::/48")}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := &fakeKernel{addresses: tc.addresses, routes: tc.routes}
			a := NewAddress(netip.MustParseAddr(tc.ip), k, discardLog())
			err := a.Run(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.iface) {
				t.Fatalf("Run returned %v, want it to name %s", err, tc.iface)
			}
			if len(k.added) != 0 || len(k.deleted) != 0 {
				t.Fatalf("wrote on a refusal: added %v, deleted %v", k.added, k.deleted)
			}
		})
	}
}

func TestAddressIsNotRefusedForWhatIsNotOnLink(t *testing.T) {
	k := &fakeKernel{
		addresses: []HostAddress{
			{Interface: "lo", Prefix: prefix("127.0.0.1/8"), Label: "lo"},
			{Interface: "eth0", Prefix: prefix("10.0.0.5/24")},
			{Interface: "eth0", Prefix: prefix("fd00:10::5/64")},
		},
		routes: []HostRoute{
			{Interface: "eth0", Prefix: prefix("0.0.0.0/0"), Gateway: true},
			{Interface: "ppp0", Prefix: prefix("0.0.0.0/0")},
			{Interface: "eth0", Prefix: prefix("240.0.0.0/4"), Gateway: true},
			{Interface: "lo", Prefix: prefix("240.0.0.0/4")},
			{Interface: "eth0", Prefix: prefix("10.0.0.0/24")},
			{Interface: "eth0", Prefix: prefix("fd00:10::/64")},
		},
	}
	_, stop := runAddress(t, k, advertised, nil)
	eventually(t, "the address", func() bool { return k.holds(heldV4) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestAddressHoldsIPv4WithItsLabel(t *testing.T) {
	k := &fakeKernel{addresses: []HostAddress{{Interface: "lo", Prefix: prefix("127.0.0.1/8"), Label: "lo"}}}
	_, stop := runAddress(t, k, advertised, nil)
	eventually(t, "the labelled address", func() bool { return k.holds(heldV4) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestAddressHoldsIPv6WithoutALabel(t *testing.T) {
	k := &fakeKernel{addresses: []HostAddress{{Interface: "lo", Prefix: prefix("fd00:20::9/128")}}}
	want := HostAddress{Interface: "lo", Prefix: prefix("fd00:10::1/128")}
	_, stop := runAddress(t, k, "fd00:10::1", nil)
	eventually(t, "the address", func() bool { return k.holds(want) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if len(k.deleted) != 0 {
		t.Fatalf("released %v: an IPv6 address has no label to find it by", k.deleted)
	}
}

func TestAddressReleasesOnlyOtherLabelledAddressesOnLo(t *testing.T) {
	leftover := HostAddress{Interface: "lo", Prefix: prefix("240.0.0.9/32"), Label: "lo:lodia"}
	kept := []HostAddress{
		{Interface: "lo", Prefix: prefix("127.0.0.1/8"), Label: "lo"},
		{Interface: "lo", Prefix: prefix("240.0.0.10/32"), Label: "lo:other"},
		{Interface: "lo", Prefix: prefix("240.0.0.11/32")},
		{Interface: "eth0", Prefix: prefix("10.0.0.5/24"), Label: "lo:lodia"},
		heldV4,
	}
	k := &fakeKernel{addresses: append(slices.Clone(kept), leftover)}
	_, stop := runAddress(t, k, advertised, nil)
	eventually(t, "the release", func() bool { return !k.holds(leftover) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(k.deleted, []HostAddress{leftover}) {
		t.Fatalf("deleted %v", k.deleted)
	}
	for _, a := range kept {
		if !k.holds(a) {
			t.Fatalf("%v is gone", a)
		}
	}
}

func TestAnIPv6AddressReleasesLabelledIPv4Leftovers(t *testing.T) {
	leftover := HostAddress{Interface: "lo", Prefix: prefix("240.0.0.9/32"), Label: "lo:lodia"}
	k := &fakeKernel{addresses: []HostAddress{leftover}}
	_, stop := runAddress(t, k, "fd00:10::1", nil)
	eventually(t, "the release", func() bool { return !k.holds(leftover) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestAddressStartsOverWhatIsAlreadyThere(t *testing.T) {
	for name, have := range map[string]HostAddress{
		"its own, labelled":    heldV4,
		"the same, unlabelled": {Interface: "lo", Prefix: prefix(advertised + "/32")},
	} {
		t.Run(name, func(t *testing.T) {
			k := &fakeKernel{addresses: []HostAddress{have}}
			_, stop := runAddress(t, k, advertised, nil)
			eventually(t, "the add", func() bool { return k.addCalls() > 0 })
			if err := stop(); err != nil {
				t.Fatalf("EEXIST is success: %v", err)
			}
			if len(k.deleted) != 0 {
				t.Fatalf("deleted %v", k.deleted)
			}
		})
	}
}

func TestADeleteThatFindsNothingIsSuccess(t *testing.T) {
	leftover := HostAddress{Interface: "lo", Prefix: prefix("240.0.0.9/32"), Label: "lo:lodia"}
	k := &fakeKernel{addresses: []HostAddress{leftover}, deleteErr: syscall.EADDRNOTAVAIL}
	_, stop := runAddress(t, k, advertised, nil)
	eventually(t, "the address", func() bool { return k.holds(heldV4) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestAddressStopsOnAFailedStart(t *testing.T) {
	broken := errors.New("broken")
	leftover := HostAddress{Interface: "lo", Prefix: prefix("240.0.0.9/32"), Label: "lo:lodia"}
	for name, k := range map[string]*fakeKernel{
		"listing addresses": {listErr: broken},
		"listing routes":    {routesErr: broken},
		"releasing":         {addresses: []HostAddress{leftover}, deleteErr: broken},
		"adding":            {addErr: []error{broken}},
	} {
		t.Run(name, func(t *testing.T) {
			err := NewAddress(netip.MustParseAddr(advertised), k, discardLog()).Run(context.Background())
			if !errors.Is(err, broken) {
				t.Fatalf("Run returned %v", err)
			}
		})
	}
}

func TestARestartAfterAFailedWriteFinishesTheStart(t *testing.T) {
	leftover := HostAddress{Interface: "lo", Prefix: prefix("240.0.0.9/32"), Label: "lo:lodia"}
	k := &fakeKernel{addresses: []HostAddress{leftover}, addErr: []error{syscall.EPERM}}
	if err := NewAddress(netip.MustParseAddr(advertised), k, discardLog()).Run(context.Background()); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("Run returned %v", err)
	}
	if k.holds(leftover) {
		t.Fatal("the release did not come first")
	}
	_, stop := runAddress(t, k, advertised, nil)
	eventually(t, "the address after the restart", func() bool { return k.holds(heldV4) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestAddressReassertsOnAnEvent(t *testing.T) {
	k := &fakeKernel{}
	_, _ = runAddress(t, k, advertised, nil)
	eventually(t, "the address", func() bool { return k.holds(heldV4) })

	k.mu.Lock()
	k.remove(heldV4)
	k.mu.Unlock()
	k.lastSubscription() <- struct{}{}
	eventually(t, "the address back", func() bool { return k.holds(heldV4) })
}

func TestAddressReassertsOnItsPeriod(t *testing.T) {
	k := &fakeKernel{}
	_, _ = runAddress(t, k, advertised, func(a *Address) { a.Period = 10 * time.Millisecond })
	eventually(t, "the address", func() bool { return k.holds(heldV4) })

	k.mu.Lock()
	k.remove(heldV4)
	k.mu.Unlock()
	eventually(t, "the address back", func() bool { return k.holds(heldV4) })
}

func TestAddressReassertsEveryMinute(t *testing.T) {
	if p := NewAddress(netip.MustParseAddr(advertised), &fakeKernel{}, nil).Period; p != time.Minute {
		t.Fatalf("period %s", p)
	}
}

func TestAFailedReassertIsLoggedAndRetried(t *testing.T) {
	k := &fakeKernel{addErr: []error{nil, syscall.ENOMEM}}
	logs, _ := runAddress(t, k, advertised, func(a *Address) { a.Period = 10 * time.Millisecond })
	eventually(t, "the address", func() bool { return k.holds(heldV4) })
	k.mu.Lock()
	k.remove(heldV4)
	k.mu.Unlock()
	eventually(t, "the failure in the log", func() bool { return logs.count("re-assert failed") == 1 })
	eventually(t, "the retry", func() bool { return k.holds(heldV4) })
}

func TestAnEndedSubscriptionIsSubscribedAgain(t *testing.T) {
	k := &fakeKernel{}
	_, _ = runAddress(t, k, advertised, nil)
	eventually(t, "the address", func() bool { return k.holds(heldV4) })
	close(k.lastSubscription())
	eventually(t, "a new subscription", func() bool { return k.subscriptions() == 2 })

	k.mu.Lock()
	k.remove(heldV4)
	k.mu.Unlock()
	k.lastSubscription() <- struct{}{}
	eventually(t, "the address back", func() bool { return k.holds(heldV4) })
}

func TestAFailedSubscriptionIsRetriedOnThePeriod(t *testing.T) {
	k := &fakeKernel{subscribeErr: []error{errors.New("no socket")}}
	logs, _ := runAddress(t, k, advertised, func(a *Address) { a.Period = 10 * time.Millisecond })
	eventually(t, "the address", func() bool { return k.holds(heldV4) })
	eventually(t, "a subscription", func() bool { return k.subscriptions() == 1 })
	if logs.count("not subscribed") != 1 {
		t.Fatal("the failure is not in the log")
	}
}

func TestAddressLeavesTheAddressOnItsSignal(t *testing.T) {
	k := &fakeKernel{}
	_, stop := runAddress(t, k, advertised, nil)
	eventually(t, "the address", func() bool { return k.holds(heldV4) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if !k.holds(heldV4) || len(k.deleted) != 0 {
		t.Fatalf("addresses %v, deleted %v", k.addresses, k.deleted)
	}
}

func TestCheckAnswersForTheAddressOnLo(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ip        string
		addresses []HostAddress
		ok        bool
	}{
		{"on lo", advertised, []HostAddress{heldV4}, true},
		{"on lo without its label", advertised, []HostAddress{{Interface: "lo", Prefix: prefix(advertised + "/32")}}, true},
		{"IPv6 on lo", "fd00:10::1", []HostAddress{{Interface: "lo", Prefix: prefix("fd00:10::1/128")}}, true},
		{"not there", advertised, []HostAddress{{Interface: "lo", Prefix: prefix("127.0.0.1/8")}}, false},
		{"inside the prefix of another on lo", "127.0.0.2", []HostAddress{{Interface: "lo", Prefix: prefix("127.0.0.1/8")}}, false},
		{"on another interface", advertised, []HostAddress{{Interface: "eth0", Prefix: prefix(advertised + "/32")}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := &fakeKernel{addresses: tc.addresses}
			err := NewAddress(netip.MustParseAddr(tc.ip), k, discardLog()).Check()
			if (err == nil) != tc.ok {
				t.Fatalf("Check returned %v", err)
			}
			if len(k.added) != 0 || len(k.deleted) != 0 {
				t.Fatal("Check wrote to the kernel")
			}
		})
	}
	broken := errors.New("broken")
	if err := NewAddress(netip.MustParseAddr(advertised), &fakeKernel{listErr: broken}, discardLog()).Check(); !errors.Is(err, broken) {
		t.Fatalf("Check returned %v", err)
	}
}
