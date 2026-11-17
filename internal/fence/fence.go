// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package fence serialises the writes whose check reads the objects of a kind
// that may stand in their way, so that the check can read the cache and still
// see every write made before it.
package fence

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/features"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// seenWithin is how long a write waits for the cache to hold it.
	seenWithin = 10 * time.Second
	pollEvery  = 5 * time.Millisecond
)

// A Fence lets one check and its write run at a time. Until a write made
// under it has shown in the cache, its holder's check reads the API server;
// from then on, the cache. A watch delivers the changes of a kind in the order
// of their resourceVersion, and client-go's informer applies them in that
// order: a watch's events under the feature gate InOrderInformers, GA and
// locked on, and the list of a relist, after a watch it cannot resume, at once
// under AtomicFIFO, Beta, on by default and not locked
// (https://github.com/kubernetes/client-go/blob/v0.37.1/features/known_features.go#L112-L130,
// https://github.com/kubernetes/client-go/blob/v0.37.1/tools/cache/controller.go#L1063-L1094),
// which Ordered checks. So a cache that holds a write holds every earlier
// change of the kind, an earlier leader's writes included, and each write
// waits for the cache to hold it before the fence is released. A write whose
// outcome is unknown, or that the cache does not hold within seenWithin, sends
// the next check back to the API server.
type Fence struct {
	once   sync.Once
	lock   *semaphore.Weighted
	cached bool
}

// Ordered returns an error unless gates have client-go's informers apply a
// kind's changes in order, as a Fence needs.
func Ordered(gates features.Gates) error {
	for _, gate := range []features.Feature{features.InOrderInformers, features.AtomicFIFO} {
		if !gates.Enabled(gate) {
			return fmt.Errorf("client-go's feature gate %s is off, so the cache may apply a kind's changes out of order and a route's write or a slot's grant could miss an earlier one: unset KUBE_FEATURE_%s", gate, gate)
		}
	}
	return nil
}

// Acquire takes the fence, or returns ctx's error.
func (f *Fence) Acquire(ctx context.Context) error {
	f.once.Do(func() { f.lock = semaphore.NewWeighted(1) })
	return f.lock.Acquire(ctx, 1)
}

func (f *Fence) Release() {
	f.lock.Release(1)
}

// Cached says, to the holder, whether the cache holds every write made under
// the fence.
func (f *Fence) Cached() bool {
	return f.cached
}

// Wrote records the holder's write of obj, which ended with err and left obj
// at its resourceVersion, empty when nothing was written, and waits until
// cache holds obj at that version or a later one. A write the API server
// refused, with a status of 4xx, wrote nothing. It returns err.
func (f *Fence) Wrote(ctx context.Context, cache client.Reader, obj client.Object, err error) error {
	var status apierrors.APIStatus
	switch {
	case errors.As(err, &status) && status.Status().Code >= 400 && status.Status().Code < 500:
	case err != nil:
		f.cached = false
	case obj.GetResourceVersion() != "":
		f.cached = holds(ctx, cache, obj) == nil
	}
	return err
}

func holds(ctx context.Context, cache client.Reader, obj client.Object) error {
	ctx, cancel := context.WithTimeout(ctx, seenWithin)
	defer cancel()
	key, want := client.ObjectKeyFromObject(obj), obj.GetResourceVersion()
	current := obj.DeepCopyObject().(client.Object)
	return wait.PollUntilContextCancel(ctx, pollEvery, true, func(ctx context.Context) (bool, error) {
		if err := cache.Get(ctx, key, current); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		order, err := resourceversion.CompareResourceVersion(current.GetResourceVersion(), want)
		return order >= 0, err
	})
}
