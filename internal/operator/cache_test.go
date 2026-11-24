// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package operator_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/operator"
)

func TestTheCacheHoldsNoDataOfASecretWhicheverInformerReadsIt(t *testing.T) {
	cfg := start(t)
	options, err := operator.Cache(false)
	if err != nil {
		t.Fatal(err)
	}
	options.Scheme = operator.Scheme()
	informers, err := cache.New(cfg, options)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- informers.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
	admin, err := client.New(cfg, client.Options{Scheme: operator.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	written := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "dev-lodia-kubeconfig", Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev"}},
		Data:       map[string][]byte{"kubeconfig": []byte("key material")},
	}
	if err := admin.Create(t.Context(), written); err != nil {
		t.Fatal(err)
	}
	typed, metadata := &corev1.Secret{}, &metav1.PartialObjectMetadata{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"}}
	untyped := &unstructured.Unstructured{}
	untyped.SetAPIVersion("v1")
	untyped.SetKind("Secret")
	if err := wait.PollUntilContextTimeout(t.Context(), 50*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		key := client.ObjectKeyFromObject(written)
		return informers.Get(ctx, key, typed) == nil && informers.Get(ctx, key, metadata) == nil && informers.Get(ctx, key, untyped) == nil, nil
	}); err != nil {
		t.Fatalf("the cache never held the Secret: %v", err)
	}
	if typed.ResourceVersion != written.ResourceVersion || metadata.ResourceVersion != written.ResourceVersion {
		t.Errorf("the cache holds the Secret at resourceVersion %q as a Secret and %q as metadata, want %q", typed.ResourceVersion, metadata.ResourceVersion, written.ResourceVersion)
	}
	if len(typed.Data) != 0 {
		t.Errorf("a Secret read from the cache holds %d keys, want none", len(typed.Data))
	}
	if data, found, _ := unstructured.NestedMap(untyped.Object, "data"); found {
		t.Errorf("an unstructured Secret read from the cache holds %d keys, want none", len(data))
	}
	if len(typed.ManagedFields) != 0 || len(metadata.ManagedFields) != 0 {
		t.Errorf("the cache holds %d managed fields of the Secret and %d of its metadata, want none", len(typed.ManagedFields), len(metadata.ManagedFields))
	}
}
