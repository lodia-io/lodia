// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestACacheWithoutAPIServerReadyLeavesTheServersCondition(t *testing.T) {
	cp := running(t)
	pass := func(step string) {
		t.Helper()
		before := get(t, cp)
		cached := before.DeepCopy()
		cached.Spec.Paused = false
		meta.RemoveStatusCondition(&cached.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
		copies := serverCopies(t, cp, func() { reconcileThrough(t, cp, caching(cached)) })
		for _, c := range copies {
			if got := meta.FindStatusCondition(c.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady); got == nil || got.Reason == lodiav1alpha1.ReasonReconciling {
				t.Errorf("%s: the server held APIServerReady %+v at resourceVersion %s", step, got, c.ResourceVersion)
			}
		}
		after := copies[len(copies)-1]
		want := *meta.FindStatusCondition(before.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
		want.ObservedGeneration = after.Generation
		if got := meta.FindStatusCondition(after.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady); !equality.Semantic.DeepEqual(*got, want) {
			t.Errorf("%s: APIServerReady is %+v, want %+v", step, got, want)
		}
	}
	pass("at its generation")
	pause(t, cp)
	pass("at an earlier generation")
}

func TestANewControlPlaneStillGetsReconcilingFromWorkloads(t *testing.T) {
	ns := namespace(t)
	events, err := k8s.Watch(t.Context(), &lodiav1alpha1.ControlPlaneList{}, client.InNamespace(ns))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Stop()
	create(t, ns, loadBalancer())
	for {
		select {
		case event := <-events.ResultChan():
			fresh, ok := event.Object.(*lodiav1alpha1.ControlPlane)
			if !ok || event.Type != watch.Modified {
				continue
			}
			if c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady); c != nil {
				if c.Status != metav1.ConditionUnknown || c.Reason != lodiav1alpha1.ReasonReconciling {
					t.Errorf("the first APIServerReady is %+v, want Unknown/%s", c, lodiav1alpha1.ReasonReconciling)
				}
				return
			}
		case <-time.After(30 * time.Second):
			t.Fatal("no APIServerReady condition was written")
		}
	}
}

func caching(cp *lodiav1alpha1.ControlPlane) client.WithWatch {
	return interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if plane, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				cp.DeepCopyInto(plane)
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
}

func serverCopies(t *testing.T, cp *lodiav1alpha1.ControlPlane, during func()) []lodiav1alpha1.ControlPlane {
	t.Helper()
	before := get(t, cp)
	events, err := k8s.Watch(t.Context(), &lodiav1alpha1.ControlPlaneList{}, &client.ListOptions{Namespace: cp.Namespace, Raw: &metav1.ListOptions{ResourceVersion: before.ResourceVersion}})
	if err != nil {
		t.Fatal(err)
	}
	defer events.Stop()
	during()
	current := get(t, cp).ResourceVersion
	copies := []lodiav1alpha1.ControlPlane{*before}
	for copies[len(copies)-1].ResourceVersion != current {
		select {
		case event := <-events.ResultChan():
			if fresh, ok := event.Object.(*lodiav1alpha1.ControlPlane); ok && fresh.Name == cp.Name {
				copies = append(copies, *fresh)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("the watch did not reach resourceVersion %s", current)
		}
	}
	return copies
}
