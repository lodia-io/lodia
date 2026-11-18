// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package secretcache serves the data of the Secrets whose metadata the
// manager's cache holds, from memory for as long as their resourceVersion
// stays (docs/decisions/one-client-for-a-tenant.md).
package secretcache

import (
	"context"
	"sync"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Reader is a client.Reader: a Get of a Secret goes through the copies it keeps,
// every other Get and every List to live. A copy is never changed once kept, and
// kept only at the UID and resourceVersion the cache holds, so memory holds
// nothing the cache does not.
type Reader struct {
	cache cache.Cache
	live  client.Reader

	registered atomic.Bool
	register   sync.Mutex

	mu     sync.Mutex
	copies map[types.NamespacedName]*corev1.Secret
}

func New(c cache.Cache, live client.Reader) *Reader {
	return &Reader{cache: c, live: live, copies: map[types.NamespacedName]*corev1.Secret{}}
}

var _ client.Reader = (*Reader)(nil)

func (r *Reader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return r.live.List(ctx, list, opts...)
}

func (r *Reader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	secret, ok := obj.(*corev1.Secret)
	if !ok {
		return r.live.Get(ctx, key, obj, opts...)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.watch(ctx); err != nil {
		return err
	}
	held, err := r.held(ctx, key)
	if err != nil {
		return err
	}
	if held == nil {
		return r.read(ctx, key, secret, opts)
	}
	r.mu.Lock()
	kept := r.copies[key]
	r.mu.Unlock()
	if kept == nil || kept.UID != held.UID || kept.ResourceVersion != held.ResourceVersion {
		return r.read(ctx, key, secret, opts)
	}
	kept.DeepCopyInto(secret)
	return nil
}

// read reads the Secret from the API server and keeps what it read when the
// cache, read again under the lock of the copies, holds that UID and
// resourceVersion. A failed read leaves the copy as it was.
func (r *Reader) read(ctx context.Context, key client.ObjectKey, secret *corev1.Secret, opts []client.GetOption) error {
	if err := r.live.Get(ctx, key, secret, opts...); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	held, err := r.held(ctx, key)
	if err == nil && held != nil && held.UID == secret.UID && held.ResourceVersion == secret.ResourceVersion {
		r.copies[key] = secret.DeepCopy()
		return nil
	}
	delete(r.copies, key)
	return nil
}

func (r *Reader) held(ctx context.Context, key client.ObjectKey) (*metav1.PartialObjectMetadata, error) {
	held := secretMetadata()
	if err := r.cache.Get(ctx, key, held); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return held, nil
}

// watch has the delete of a Secret drop its copy, so that a deleted tenant
// leaves nothing in memory.
func (r *Reader) watch(ctx context.Context) error {
	if r.registered.Load() {
		return nil
	}
	r.register.Lock()
	defer r.register.Unlock()
	if r.registered.Load() {
		return nil
	}
	informer, err := r.cache.GetInformer(ctx, secretMetadata())
	if err != nil {
		return err
	}
	if _, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{DeleteFunc: r.deleted}); err != nil {
		return err
	}
	r.registered.Store(true)
	return nil
}

func (r *Reader) deleted(obj any) {
	if tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	object, ok := obj.(metav1.Object)
	if !ok {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.copies, types.NamespacedName{Namespace: object.GetNamespace(), Name: object.GetName()})
}

func secretMetadata() *metav1.PartialObjectMetadata {
	m := &metav1.PartialObjectMetadata{}
	m.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	return m
}
