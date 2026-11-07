// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package tunnel

import (
	"context"
	"testing"
)

func TestTheKernelIsLinuxOnly(t *testing.T) {
	k := NewKernel()
	_, addresses := k.Addresses()
	_, routes := k.Routes()
	_, subscribe := k.Subscribe(context.Background())
	for name, err := range map[string]error{
		"Addresses": addresses, "Routes": routes, "Subscribe": subscribe,
		"AddAddress": k.AddAddress(HostAddress{}), "DeleteAddress": k.DeleteAddress(HostAddress{}),
	} {
		if err == nil {
			t.Errorf("%s succeeded off Linux", name)
		}
	}
}
