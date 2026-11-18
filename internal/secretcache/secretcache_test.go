// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package secretcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var key = client.ObjectKey{Namespace: "team-a", Name: "dev-lodia-kubeconfig"}

type store struct {
	cache.Cache
	mu         sync.Mutex
	server     *corev1.Secret
	seen       *metav1.PartialObjectMetadata
	reads      atomic.Int32
	failNext   error
	handler    toolscache.ResourceEventHandler
	handlers   atomic.Int32
	onRead     func()
	cacheReads int
	afterCache func(n int)
}

func secret(uid types.UID, version, data string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, UID: uid, ResourceVersion: version},
		Data:       map[string][]byte{"kubeconfig": []byte(data)},
	}
}

func metadata(s *corev1.Secret) *metav1.PartialObjectMetadata {
	m := &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Namespace: s.Namespace, Name: s.Name, UID: s.UID, ResourceVersion: s.ResourceVersion}}
	m.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	return m
}

func (s *store) write(written *corev1.Secret) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.server = written
}

func (s *store) observe(seen *corev1.Secret) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = nil
	if seen != nil {
		s.seen = metadata(seen)
	}
}

func (s *store) Get(_ context.Context, k client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	s.mu.Lock()
	s.cacheReads++
	n := s.cacheReads
	var err error
	if s.seen == nil || k != client.ObjectKeyFromObject(s.seen) {
		err = apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, k.Name)
	} else {
		s.seen.DeepCopyInto(obj.(*metav1.PartialObjectMetadata))
	}
	s.mu.Unlock()
	if s.afterCache != nil {
		s.afterCache(n)
	}
	return err
}

func (s *store) GetInformer(context.Context, client.Object, ...cache.InformerGetOption) (cache.Informer, error) {
	return informer{s: s}, nil
}

type informer struct {
	cache.Informer
	s *store
}

func (i informer) AddEventHandler(h toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	i.s.handler = h
	i.s.handlers.Add(1)
	return nil, nil
}

type api struct{ s *store }

func (a api) Get(_ context.Context, k client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	a.s.reads.Add(1)
	if configMap, ok := obj.(*corev1.ConfigMap); ok {
		configMap.Name, configMap.Data = k.Name, map[string]string{"read": "live"}
		return nil
	}
	if a.s.onRead != nil {
		a.s.onRead()
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	if err := a.s.failNext; err != nil {
		a.s.failNext = nil
		return err
	}
	if a.s.server == nil || k != client.ObjectKeyFromObject(a.s.server) {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, k.Name)
	}
	a.s.server.DeepCopyInto(obj.(*corev1.Secret))
	return nil
}

func (a api) List(context.Context, client.ObjectList, ...client.ListOption) error {
	a.s.reads.Add(100)
	return nil
}

func start(initial *corev1.Secret) (*Reader, *store) {
	s := &store{}
	s.write(initial)
	s.observe(initial)
	return New(s, api{s}), s
}

func read(t *testing.T, r *Reader) *corev1.Secret {
	t.Helper()
	got := &corev1.Secret{}
	if err := r.Get(t.Context(), key, got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestAHitSendsNothingAndReturnsACopyNobodyShares(t *testing.T) {
	r, s := start(secret("a", "1", "first"))
	read(t, r)
	if n := s.reads.Load(); n != 1 {
		t.Fatalf("the first read sent %d requests, want 1", n)
	}
	got := read(t, r)
	if n := s.reads.Load(); n != 1 || string(got.Data["kubeconfig"]) != "first" {
		t.Fatalf("the second read sent %d requests in all and returned %q, want 1 and first", n, got.Data["kubeconfig"])
	}
	got.Data["kubeconfig"][0] = 'X'
	if again := read(t, r); string(again.Data["kubeconfig"]) != "first" {
		t.Errorf("a caller's change reached the copy: %q", again.Data["kubeconfig"])
	}
}

func TestAChangedResourceVersionIsReadAgain(t *testing.T) {
	r, s := start(secret("a", "1", "first"))
	read(t, r)
	changed := secret("a", "2", "second")
	s.write(changed)
	s.observe(changed)
	if got := read(t, r); string(got.Data["kubeconfig"]) != "second" || s.reads.Load() != 2 {
		t.Errorf("read %q after %d requests, want second after 2", got.Data["kubeconfig"], s.reads.Load())
	}
	read(t, r)
	if s.reads.Load() != 2 {
		t.Errorf("the new version was not kept: %d requests", s.reads.Load())
	}
}

func TestARecreatedSecretIsReadAgainAtTheSameResourceVersion(t *testing.T) {
	r, s := start(secret("a", "7", "first"))
	read(t, r)
	recreated := secret("b", "7", "second")
	s.write(recreated)
	s.observe(recreated)
	if got := read(t, r); string(got.Data["kubeconfig"]) != "second" {
		t.Errorf("read %q, want the Secret of the new UID", got.Data["kubeconfig"])
	}
}

func TestASecretTheCacheDoesNotHoldIsReadEveryTimeAndKeepsNoCopy(t *testing.T) {
	r, s := start(secret("a", "1", "first"))
	s.observe(nil)
	for range 3 {
		read(t, r)
	}
	if s.reads.Load() != 3 {
		t.Errorf("sent %d requests for 3 reads of a Secret the cache lacks, want 3", s.reads.Load())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.copies) != 0 {
		t.Errorf("kept %d copies of a Secret the cache does not hold", len(r.copies))
	}
}

func TestACopyOfASecretTheCacheNoLongerHoldsIsDropped(t *testing.T) {
	r, s := start(secret("a", "1", "first"))
	read(t, r)
	s.observe(nil)
	read(t, r)
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.copies) != 0 {
		t.Errorf("kept %d copies of a Secret the cache dropped", len(r.copies))
	}
}

func TestAFailedLiveReadKeepsTheCopyAsItWas(t *testing.T) {
	r, s := start(secret("a", "1", "first"))
	read(t, r)
	changed := secret("a", "2", "second")
	s.write(changed)
	s.observe(changed)
	s.failNext = errors.New("the API server is down")
	if err := r.Get(t.Context(), key, &corev1.Secret{}); err == nil {
		t.Fatal("a failed live read returned no error")
	}
	r.mu.Lock()
	kept := r.copies[key]
	r.mu.Unlock()
	if kept == nil || kept.ResourceVersion != "1" {
		t.Errorf("the copy after a failed read is %v, want the one at resourceVersion 1", kept)
	}
}

func TestALiveReadNewerThanTheCacheIsReturnedAndNotKept(t *testing.T) {
	r, s := start(secret("a", "1", "first"))
	s.write(secret("a", "2", "second"))
	if got := read(t, r); string(got.Data["kubeconfig"]) != "second" {
		t.Fatalf("read %q, want what the API server says", got.Data["kubeconfig"])
	}
	read(t, r)
	if s.reads.Load() != 2 {
		t.Errorf("sent %d requests for 2 reads that the cache lags behind, want 2: the live read was kept", s.reads.Load())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.copies) != 0 {
		t.Errorf("kept %d copies of a version the cache does not hold", len(r.copies))
	}
}

func TestALiveReadThatTheCacheOvertookWhileItRanIsNotKept(t *testing.T) {
	r, s := start(secret("a", "1", "first"))
	s.onRead = func() { s.observe(secret("a", "2", "second")) }
	if got := read(t, r); string(got.Data["kubeconfig"]) != "first" {
		t.Fatalf("read %q, want what the API server said when it was asked", got.Data["kubeconfig"])
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.copies) != 0 {
		t.Errorf("kept %d copies of a version the cache had moved past", len(r.copies))
	}
}

func TestADeleteAndATombstoneDropTheCopy(t *testing.T) {
	for name, event := range map[string]func(*metav1.PartialObjectMetadata) any{
		"a delete": func(m *metav1.PartialObjectMetadata) any { return m },
		"a tombstone": func(m *metav1.PartialObjectMetadata) any {
			return toolscache.DeletedFinalStateUnknown{Key: "team-a/dev-lodia-kubeconfig", Obj: m}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, s := start(secret("a", "1", "first"))
			read(t, r)
			if s.handler == nil {
				t.Fatal("no handler on the informer of the Secrets' metadata")
			}
			s.handler.OnDelete(event(metadata(secret("a", "1", ""))))
			r.mu.Lock()
			defer r.mu.Unlock()
			if len(r.copies) != 0 {
				t.Errorf("%s left %d copies", name, len(r.copies))
			}
		})
	}
}

func TestACancelledContextSendsNothingAndLeavesTheCopy(t *testing.T) {
	r, s := start(secret("a", "1", "first"))
	read(t, r)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := r.Get(ctx, key, &corev1.Secret{}); !errors.Is(err, context.Canceled) {
		t.Errorf("a cancelled context returned %v", err)
	}
	if s.reads.Load() != 1 {
		t.Errorf("sent %d requests, want the first read's only", s.reads.Load())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.copies) != 1 {
		t.Errorf("kept %d copies, want 1", len(r.copies))
	}
}

func TestEveryOtherReadGoesToTheAPIServer(t *testing.T) {
	r, s := start(secret("a", "1", "first"))
	if err := r.List(t.Context(), &corev1.SecretList{}); err != nil || s.reads.Load() != 100 {
		t.Errorf("a List sent %d requests and returned %v, want the live reader's", s.reads.Load(), err)
	}
	s.reads.Store(0)
	configMap := &corev1.ConfigMap{}
	if err := r.Get(t.Context(), key, configMap); err != nil || s.reads.Load() != 1 || configMap.Data["read"] != "live" {
		t.Errorf("a Get of a ConfigMap sent %d requests and returned %v with %v, want the live reader's answer", s.reads.Load(), err, configMap.Data)
	}
}

func TestACallerThatChangesWhatALiveReadReturnedLeavesTheCopyAsItWas(t *testing.T) {
	r, _ := start(secret("a", "1", "first"))
	got := read(t, r)
	got.Data["kubeconfig"][0] = 'X'
	if again := read(t, r); string(again.Data["kubeconfig"]) != "first" {
		t.Errorf("a caller's change to what a live read returned reached the copy: %q", again.Data["kubeconfig"])
	}
}

func TestTheReaderAddsOneDeleteHandlerForItsLife(t *testing.T) {
	r, s := start(secret("a", "1", "first"))
	for range 3 {
		read(t, r)
	}
	if err := r.Get(t.Context(), client.ObjectKey{Namespace: "team-a", Name: "other"}, &corev1.Secret{}); err == nil {
		t.Fatal("a Secret that does not exist was read")
	}
	if n := s.handlers.Load(); n != 1 {
		t.Errorf("the informer has %d handlers after 4 reads, want 1", n)
	}
}

func TestTheCacheIsReadAgainUnderTheLockThatGuardsTheCopies(t *testing.T) {
	r, s := start(secret("a", "1", "first"))
	deleted := make(chan struct{})
	s.afterCache = func(n int) {
		if n != 2 {
			return
		}
		s.observe(nil)
		go func() {
			defer close(deleted)
			s.handler.OnDelete(metadata(secret("a", "1", "")))
		}()
		select {
		case <-deleted:
		case <-time.After(200 * time.Millisecond):
		}
	}
	read(t, r)
	<-deleted
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.copies) != 0 {
		t.Errorf("a delete that reached the cache and its handler between the second read and the store left %d copies", len(r.copies))
	}
}
