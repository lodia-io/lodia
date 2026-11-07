// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package tunnel

import (
	"context"
	"errors"
	"net"
	"net/netip"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

const dumpAttempts = 5

type netlinkKernel struct{}

// NewKernel returns the real Kernel.
func NewKernel() Kernel {
	return netlinkKernel{}
}

func (netlinkKernel) Addresses() ([]HostAddress, error) {
	addresses, err := dump(func() ([]netlink.Addr, error) { return netlink.AddrList(nil, netlink.FAMILY_ALL) })
	if err != nil {
		return nil, err
	}
	names, err := linkNames()
	if err != nil {
		return nil, err
	}
	out := make([]HostAddress, 0, len(addresses))
	for _, a := range addresses {
		if prefix, ok := toPrefix(a.IPNet); ok {
			out = append(out, HostAddress{Interface: names[a.LinkIndex], Prefix: prefix, Label: a.Label})
		}
	}
	return out, nil
}

func (netlinkKernel) Routes() ([]HostRoute, error) {
	routes, err := dump(func() ([]netlink.Route, error) { return netlink.RouteList(nil, netlink.FAMILY_ALL) })
	if err != nil {
		return nil, err
	}
	names, err := linkNames()
	if err != nil {
		return nil, err
	}
	var out []HostRoute
	for _, r := range routes {
		prefix, ok := toPrefix(r.Dst)
		if !ok || r.Type != unix.RTN_UNICAST {
			continue
		}
		if r.LinkIndex != 0 {
			out = append(out, HostRoute{Interface: names[r.LinkIndex], Prefix: prefix, Gateway: r.Gw != nil || r.Via != nil})
		}
		for _, hop := range r.MultiPath {
			out = append(out, HostRoute{Interface: names[hop.LinkIndex], Prefix: prefix, Gateway: hop.Gw != nil || hop.Via != nil})
		}
	}
	return out, nil
}

func (netlinkKernel) AddAddress(a HostAddress) error {
	link, err := netlink.LinkByName(a.Interface)
	if err != nil {
		return err
	}
	return netlink.AddrAdd(link, &netlink.Addr{IPNet: toIPNet(a.Prefix), Label: a.Label})
}

func (netlinkKernel) DeleteAddress(a HostAddress) error {
	link, err := netlink.LinkByName(a.Interface)
	if err != nil {
		return err
	}
	return netlink.AddrDel(link, &netlink.Addr{IPNet: toIPNet(a.Prefix), Label: a.Label})
}

func (netlinkKernel) Subscribe(ctx context.Context) (<-chan struct{}, error) {
	subscription, cancel := context.WithCancel(ctx)
	updates := make(chan netlink.AddrUpdate, 16)
	if err := netlink.AddrSubscribe(updates, subscription.Done()); err != nil {
		cancel()
		return nil, err
	}
	events := make(chan struct{}, 1)
	go func() {
		defer close(events)
		defer cancel()
		for range updates {
			select {
			case events <- struct{}{}:
			default:
			}
		}
	}()
	return events, nil
}

// dump retries a listing the kernel interrupted: its result is incomplete, and
// a busy node's address and route changes interrupt one often enough.
func dump[T any](list func() ([]T, error)) ([]T, error) {
	var out []T
	var err error
	for range dumpAttempts {
		if out, err = list(); !errors.Is(err, netlink.ErrDumpInterrupted) {
			return out, err
		}
	}
	return nil, err
}

func linkNames() (map[int]string, error) {
	links, err := dump(netlink.LinkList)
	if err != nil {
		return nil, err
	}
	names := make(map[int]string, len(links))
	for _, l := range links {
		names[l.Attrs().Index] = l.Attrs().Name
	}
	return names, nil
}

func toPrefix(n *net.IPNet) (netip.Prefix, bool) {
	if n == nil {
		return netip.Prefix{}, false
	}
	addr, ok := netip.AddrFromSlice(n.IP)
	if !ok {
		return netip.Prefix{}, false
	}
	ones, _ := n.Mask.Size()
	return netip.PrefixFrom(addr.Unmap(), ones), true
}

func toIPNet(p netip.Prefix) *net.IPNet {
	return &net.IPNet{IP: p.Addr().AsSlice(), Mask: net.CIDRMask(p.Bits(), p.Addr().BitLen())}
}
