// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package fence_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"go.lodia.io/lodia/internal/fence"
)

type cache struct {
	version atomic.Value
	gets    atomic.Int32
}

func (c *cache) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	c.gets.Add(1)
	version, _ := c.version.Load().(string)
	if version == "" {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, key.Name)
	}
	obj.SetResourceVersion(version)
	return nil
}

func (c *cache) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("not listed")
}

func showing(version string) *cache {
	c := &cache{}
	c.version.Store(version)
	return c
}

func written(version string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "slot", ResourceVersion: version}}
}

func TestTheCacheIsReadOnlyOnceAWriteUnderTheFenceHasShownInIt(t *testing.T) {
	var f fence.Fence
	if err := f.Acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer f.Release()
	if f.Cached() {
		t.Fatal("a fence no write has gone under reads the cache")
	}
	if err := f.Wrote(t.Context(), showing("41"), written(""), nil); err != nil || f.Cached() {
		t.Errorf("a pass that wrote nothing returned %v and left Cached %t", err, f.Cached())
	}
	if err := f.Wrote(t.Context(), showing("43"), written("42"), nil); err != nil || !f.Cached() {
		t.Fatalf("a write the cache holds at a later version returned %v and left Cached %t", err, f.Cached())
	}
	if err := f.Wrote(t.Context(), showing("41"), written(""), nil); err != nil || !f.Cached() {
		t.Errorf("a pass that wrote nothing returned %v and sent the next check back to the API server", err)
	}
	refused := apierrors.NewInvalid(schema.GroupKind{Kind: "ConfigMap"}, "slot", nil)
	if err := f.Wrote(t.Context(), showing("41"), written(""), refused); !errors.Is(err, refused) || !f.Cached() {
		t.Errorf("a write the API server refused returned %v and left Cached %t, want its error and the cache, since nothing was written", err, f.Cached())
	}
	for _, failed := range []error{errors.New("the connection broke"), apierrors.NewTimeoutError("the write took too long", 1), apierrors.NewInternalError(errors.New("etcd"))} {
		if err := f.Wrote(t.Context(), showing("100"), written("100"), nil); err != nil || !f.Cached() {
			t.Fatalf("a write the cache holds at its version returned %v and left Cached %t", err, f.Cached())
		}
		if err := f.Wrote(t.Context(), showing("100"), written("100"), failed); !errors.Is(err, failed) || f.Cached() {
			t.Errorf("a write whose outcome is unknown, %v, returned %v and left Cached %t, want its error and the API server", failed, err, f.Cached())
		}
	}
	if err := f.Wrote(t.Context(), showing("100"), written("100"), nil); err != nil || !f.Cached() {
		t.Fatalf("a write the cache holds at its version returned %v and left Cached %t", err, f.Cached())
	}
	short, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if err := f.Wrote(short, showing("99"), written("101"), nil); err != nil || f.Cached() {
		t.Errorf("a write the cache never showed returned %v and left Cached %t, want no error and the API server", err, f.Cached())
	}
}

func TestAWriteHoldsTheFenceUntilTheCacheShowsIt(t *testing.T) {
	for name, before := range map[string]string{"an older version": "7", "no object": ""} {
		t.Run(name, func(t *testing.T) {
			var f fence.Fence
			if err := f.Acquire(t.Context()); err != nil {
				t.Fatal(err)
			}
			lagging := showing(before)
			go func() {
				time.Sleep(200 * time.Millisecond)
				lagging.version.Store("8")
			}()
			start := time.Now()
			if err := f.Wrote(t.Context(), lagging, written("8"), nil); err != nil {
				t.Fatal(err)
			}
			if waited := time.Since(start); waited < 200*time.Millisecond || !f.Cached() {
				t.Errorf("the write returned after %s with Cached %t, want once the cache showed it after 200ms", waited, f.Cached())
			}
			f.Release()
			if err := f.Acquire(t.Context()); err != nil {
				t.Fatal(err)
			}
			f.Release()
		})
	}
}

func TestAWriteTheCacheNeverShowsWaitsTenSecondsWithNoDeadlineOfItsOwn(t *testing.T) {
	var f fence.Fence
	if err := f.Acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer f.Release()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- f.Wrote(t.Context(), showing("99"), written("101"), nil) }()
	select {
	case err := <-done:
		if waited := time.Since(start); err != nil || f.Cached() || waited < 10*time.Second {
			t.Errorf("a write the cache never showed returned %v after %s with Cached %t, want no error after 10s and the API server", err, waited.Round(time.Millisecond), f.Cached())
		}
	case <-time.After(12 * time.Second):
		t.Fatal("a write the cache never showed still waits after 12s, want 10s")
	}
}

func TestAFenceIsHeldByOneHolderAtATimeUntilItsDeadline(t *testing.T) {
	var f fence.Fence
	if err := f.Acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if err := f.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a second holder got %v, want its deadline", err)
	}
	f.Release()
	if err := f.Acquire(t.Context()); err != nil {
		t.Errorf("the fence released was not taken again: %v", err)
	}
}
