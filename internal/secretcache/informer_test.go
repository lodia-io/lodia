// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package secretcache

import (
	"context"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestTheDeleteOfASecretInTheClusterDropsItsCopyAndARecreatedOneIsReadAgain(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run make test")
	}
	env := &envtest.Environment{UseExistingCluster: new(false)}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	labelled, err := labels.NewRequirement(lodiav1alpha1.ControlPlaneLabel, "exists", nil)
	if err != nil {
		t.Fatal(err)
	}
	informers, err := cache.New(cfg, cache.Options{Scheme: scheme, DefaultLabelSelector: labels.NewSelector().Add(*labelled)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- informers.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Error(err)
		}
	})
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	r := New(informers, admin)

	written := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "dev-lodia-kubeconfig", Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev"}}, Data: map[string][]byte{"kubeconfig": []byte("first")}}
	if err := admin.Create(t.Context(), written); err != nil {
		t.Fatal(err)
	}
	copies := func() int {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.copies)
	}
	eventually(t, "a copy of the Secret the cache holds", func() bool {
		return r.Get(t.Context(), client.ObjectKeyFromObject(written), &corev1.Secret{}) == nil && copies() == 1
	})
	if err := admin.Delete(t.Context(), written); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the delete to drop the copy", func() bool { return copies() == 0 })

	recreated := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "dev-lodia-kubeconfig", Labels: written.Labels}, Data: map[string][]byte{"kubeconfig": []byte("second")}}
	if err := admin.Create(t.Context(), recreated); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the recreated Secret", func() bool {
		got := &corev1.Secret{}
		return r.Get(t.Context(), client.ObjectKeyFromObject(recreated), got) == nil && string(got.Data["kubeconfig"]) == "second" && got.UID == recreated.UID && copies() == 1
	})
}

func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
