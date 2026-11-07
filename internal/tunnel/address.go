// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tunnel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"syscall"
	"time"
)

const (
	loopback = "lo"
	// addressLabel marks Lodia's IPv4 addresses on lo, so a later Address finds
	// a predecessor's without remembering it. IPv6 addresses have no labels.
	addressLabel = "lo:lodia"
)

// HostAddress is an address on an interface of the node.
type HostAddress struct {
	Interface string
	Prefix    netip.Prefix
	Label     string
}

// HostRoute is a route of the node's main table, one per next hop.
type HostRoute struct {
	Interface string
	Prefix    netip.Prefix
	Gateway   bool
}

// Kernel is what an Address needs of the node.
type Kernel interface {
	Addresses() ([]HostAddress, error)
	Routes() ([]HostRoute, error)
	AddAddress(HostAddress) error
	DeleteAddress(HostAddress) error
	// Subscribe sends on address changes until ctx is done or the subscription
	// fails, then closes the channel.
	Subscribe(ctx context.Context) (<-chan struct{}, error)
}

// Address is lodia-tunnel addr: it holds an address on the node's loopback.
type Address struct {
	IP     netip.Addr
	Kernel Kernel
	Period time.Duration
	Log    *slog.Logger
}

// NewAddress returns an Address that re-asserts every minute.
func NewAddress(ip netip.Addr, kernel Kernel, log *slog.Logger) *Address {
	return &Address{IP: ip, Kernel: kernel, Period: time.Minute, Log: log}
}

// Check returns an error unless the address is on lo.
func (a *Address) Check() error {
	addresses, err := a.Kernel.Addresses()
	if err != nil {
		return err
	}
	for _, have := range addresses {
		if have.Interface == loopback && have.Prefix.Addr() == a.IP {
			return nil
		}
	}
	return fmt.Errorf("%s is not on %s", a.IP, loopback)
}

// Run puts the address on lo and keeps it there until ctx is done, and does not
// remove it then: the next pod of a rollout holds the same address.
func (a *Address) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	events := a.subscribe(ctx)
	if err := a.start(ctx); err != nil {
		return err
	}
	a.Log.InfoContext(ctx, "holding the address", "address", a.IP.String(), "interface", loopback)
	tick := time.NewTicker(a.Period)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
		case <-tick.C:
		case _, open := <-events:
			if !open {
				events = nil
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if events == nil {
			events = a.subscribe(ctx)
		}
		if err := a.add(); err != nil {
			a.Log.ErrorContext(ctx, "re-assert failed, retrying", "address", a.IP.String(), "error", err)
		}
	}
}

func (a *Address) subscribe(ctx context.Context) <-chan struct{} {
	events, err := a.Kernel.Subscribe(ctx)
	if err != nil {
		a.Log.WarnContext(ctx, "not subscribed to address changes, retrying", "error", err)
		return nil
	}
	return events
}

func (a *Address) start(ctx context.Context) error {
	addresses, err := a.Kernel.Addresses()
	if err != nil {
		return err
	}
	routes, err := a.Kernel.Routes()
	if err != nil {
		return err
	}
	if err := refuseOnLink(a.IP, addresses, routes); err != nil {
		return err
	}
	for _, have := range addresses {
		if have.Interface != loopback || have.Label != addressLabel || have.Prefix.Addr() == a.IP {
			continue
		}
		if err := a.Kernel.DeleteAddress(have); err != nil && !errors.Is(err, syscall.EADDRNOTAVAIL) {
			return fmt.Errorf("release %s: %w", have.Prefix, err)
		}
		a.Log.InfoContext(ctx, "released a predecessor's address", "address", have.Prefix.String())
	}
	return a.add()
}

// refuseOnLink refuses an address a neighbour could own, since a node answers
// ARP for what it holds on lo. A default route without a gateway contains
// every address and says nothing about this one, so it does not count.
func refuseOnLink(ip netip.Addr, addresses []HostAddress, routes []HostRoute) error {
	for _, have := range addresses {
		if have.Interface != loopback && have.Prefix.Contains(ip) {
			return fmt.Errorf("%s is on-link on %s, through its address %s", ip, have.Interface, have.Prefix)
		}
	}
	for _, r := range routes {
		if r.Interface != loopback && !r.Gateway && r.Prefix.Bits() > 0 && r.Prefix.Contains(ip) {
			return fmt.Errorf("%s is on-link on %s, through the route %s", ip, r.Interface, r.Prefix)
		}
	}
	return nil
}

func (a *Address) add() error {
	want := HostAddress{Interface: loopback, Prefix: netip.PrefixFrom(a.IP, a.IP.BitLen())}
	if a.IP.Is4() {
		want.Label = addressLabel
	}
	if err := a.Kernel.AddAddress(want); err != nil && !errors.Is(err, syscall.EEXIST) {
		return fmt.Errorf("add %s to %s: %w", want.Prefix, loopback, err)
	}
	return nil
}
