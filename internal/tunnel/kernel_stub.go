// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package tunnel

import (
	"context"
	"fmt"
	"runtime"
)

// unsupportedKernel lets the module build on a developer's machine; addr only
// ever runs on Linux.
type unsupportedKernel struct{}

// NewKernel returns the real Kernel, which only Linux has.
func NewKernel() Kernel {
	return unsupportedKernel{}
}

func (unsupportedKernel) Addresses() ([]HostAddress, error) { return nil, errUnsupported() }
func (unsupportedKernel) Routes() ([]HostRoute, error)      { return nil, errUnsupported() }
func (unsupportedKernel) AddAddress(HostAddress) error      { return errUnsupported() }
func (unsupportedKernel) DeleteAddress(HostAddress) error   { return errUnsupported() }
func (unsupportedKernel) Subscribe(context.Context) (<-chan struct{}, error) {
	return nil, errUnsupported()
}

func errUnsupported() error {
	return fmt.Errorf("the kernel's addresses are not supported on %s", runtime.GOOS)
}
