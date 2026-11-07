// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package tunnel

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"slices"
	"testing"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

func TestTheKernelAnswersWithoutACapability(t *testing.T) {
	kernel := NewKernel()
	if err := NewAddress(netip.MustParseAddr("127.0.0.1"), kernel, discardLog()).Check(); err != nil {
		t.Fatal(err)
	}
	if _, err := kernel.Routes(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	events, err := kernel.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	for range events {
	}
}

func interruptedOnce[T any](answer []T, err error) func() ([]T, error) {
	interrupted := false
	return func() ([]T, error) {
		if !interrupted {
			interrupted = true
			return nil, netlink.ErrDumpInterrupted
		}
		return answer, err
	}
}

func cidr(t *testing.T, s string) *net.IPNet {
	t.Helper()
	n, err := netlink.ParseIPNet(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func links() []netlink.Link {
	return []netlink.Link{
		&netlink.Device{LinkAttrs: netlink.LinkAttrs{Index: 1, Name: "lo"}},
		&netlink.Device{LinkAttrs: netlink.LinkAttrs{Index: 2, Name: "eth0"}},
		&netlink.Device{LinkAttrs: netlink.LinkAttrs{Index: 3, Name: "eth1"}},
	}
}

func TestAddressesComeFromEveryFamilyWithTheirLabels(t *testing.T) {
	var family int
	answer := interruptedOnce([]netlink.Addr{
		{IPNet: cidr(t, "127.0.0.1/8"), LinkIndex: 1, Label: "lo"},
		{IPNet: cidr(t, "240.0.0.1/32"), LinkIndex: 1, Label: "lo:lodia"},
		{IPNet: cidr(t, "fd00:10::5/64"), LinkIndex: 2},
		{LinkIndex: 2},
	}, nil)
	got, err := hostAddresses(func(_ netlink.Link, f int) ([]netlink.Addr, error) {
		family = f
		return answer()
	}, interruptedOnce(links(), nil))
	if err != nil {
		t.Fatal(err)
	}
	if family != netlink.FAMILY_ALL {
		t.Fatalf("listed family %d", family)
	}
	want := []HostAddress{
		{Interface: "lo", Prefix: prefix("127.0.0.1/8"), Label: "lo"},
		{Interface: "lo", Prefix: prefix("240.0.0.1/32"), Label: "lo:lodia"},
		{Interface: "eth0", Prefix: prefix("fd00:10::5/64")},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestRoutesAreTheUnicastOnesOfEveryFamilyOnePerHop(t *testing.T) {
	gw := net.ParseIP("10.0.0.1")
	via := &netlink.Via{AddrFamily: unix.AF_INET6, Addr: net.ParseIP("fe80::1")}
	var family int
	answer := interruptedOnce([]netlink.Route{
		{LinkIndex: 2, Dst: cidr(t, "0.0.0.0/0"), Gw: gw, Type: unix.RTN_UNICAST},
		{LinkIndex: 2, Dst: cidr(t, "240.0.0.0/4"), Via: via, Type: unix.RTN_UNICAST},
		{LinkIndex: 3, Dst: cidr(t, "10.0.0.0/24"), Type: unix.RTN_UNICAST},
		{LinkIndex: 3, Dst: cidr(t, "fd00:10::/64"), Type: unix.RTN_UNICAST},
		{LinkIndex: 1, Dst: cidr(t, "192.0.2.0/24"), Type: unix.RTN_BLACKHOLE},
		{Dst: cidr(t, "198.51.100.0/24"), Type: unix.RTN_UNREACHABLE},
		{LinkIndex: 2, Type: unix.RTN_UNICAST},
		{Dst: cidr(t, "203.0.113.0/24"), Type: unix.RTN_UNICAST, MultiPath: []*netlink.NexthopInfo{
			{LinkIndex: 2, Gw: gw},
			{LinkIndex: 3, Via: via},
			{LinkIndex: 3},
		}},
	}, nil)
	got, err := hostRoutes(func(_ netlink.Link, f int) ([]netlink.Route, error) {
		family = f
		return answer()
	}, interruptedOnce(links(), nil))
	if err != nil {
		t.Fatal(err)
	}
	if family != netlink.FAMILY_ALL {
		t.Fatalf("listed family %d", family)
	}
	want := []HostRoute{
		{Interface: "eth0", Prefix: prefix("0.0.0.0/0"), Gateway: true},
		{Interface: "eth0", Prefix: prefix("240.0.0.0/4"), Gateway: true},
		{Interface: "eth1", Prefix: prefix("10.0.0.0/24")},
		{Interface: "eth1", Prefix: prefix("fd00:10::/64")},
		{Interface: "eth0", Prefix: prefix("203.0.113.0/24"), Gateway: true},
		{Interface: "eth1", Prefix: prefix("203.0.113.0/24"), Gateway: true},
		{Interface: "eth1", Prefix: prefix("203.0.113.0/24")},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestAFailedListingIsReturned(t *testing.T) {
	broken := errors.New("broken")
	noLinks := interruptedOnce[netlink.Link](nil, broken)
	listLinks := func() ([]netlink.Link, error) { return links(), nil }
	for name, list := range map[string]func() error{
		"addresses": func() error {
			_, err := hostAddresses(func(netlink.Link, int) ([]netlink.Addr, error) { return nil, broken }, listLinks)
			return err
		},
		"the links of addresses": func() error {
			_, err := hostAddresses(func(netlink.Link, int) ([]netlink.Addr, error) { return nil, nil }, noLinks)
			return err
		},
		"routes": func() error {
			_, err := hostRoutes(func(netlink.Link, int) ([]netlink.Route, error) { return nil, broken }, listLinks)
			return err
		},
		"the links of routes": func() error {
			_, err := hostRoutes(func(netlink.Link, int) ([]netlink.Route, error) { return nil, nil }, noLinks)
			return err
		},
	} {
		if err := list(); !errors.Is(err, broken) {
			t.Errorf("%s: returned %v", name, err)
		}
	}
}

func TestADumpRetriesOnlyAnInterruptionAndGivesUp(t *testing.T) {
	calls := 0
	listing := func(interruptions int, last error) func() ([]int, error) {
		calls = 0
		return func() ([]int, error) {
			calls++
			if calls <= interruptions {
				return nil, netlink.ErrDumpInterrupted
			}
			return []int{1}, last
		}
	}
	if got, err := dump(listing(4, nil)); err != nil || !slices.Equal(got, []int{1}) {
		t.Fatalf("after 4 interruptions: %v, %v", got, err)
	}
	if got, err := dump(listing(5, nil)); !errors.Is(err, netlink.ErrDumpInterrupted) || got != nil {
		t.Fatalf("after 5 interruptions: %v, %v", got, err)
	}
	if calls != 5 {
		t.Fatalf("listed %d times, want 5", calls)
	}
	broken := errors.New("broken")
	if _, err := dump(listing(0, broken)); !errors.Is(err, broken) || calls != 1 {
		t.Fatalf("after a failure: %v, listed %d times", err, calls)
	}
}
