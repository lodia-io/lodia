// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package tunnel

import (
	"context"
	"net/netip"
	"testing"
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
