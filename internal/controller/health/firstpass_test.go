// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/health"
	"go.lodia.io/lodia/internal/testenv"
)

var healthConditions = []string{lodiav1alpha1.ConditionReachable, lodiav1alpha1.ConditionControllersLeading}

func TestACacheWithoutHealthsConditionsLeavesTheServers(t *testing.T) {
	cp := controlPlane(t)
	e := serve(t)
	writeBundle(t, cp, e)
	publish(t, cp, e)
	testenv.Eventually(t, func() error {
		return expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200")
	})
	r := &health.Reconciler{Client: k8s, Reader: k8s, FailuresBeforeFalse: failuresBeforeFalse, DialContext: dialService, Period: time.Hour, Timeout: 2 * time.Second, Clock: clock.RealClock{}}
	reconcile(t, r, cp)
	if err := expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200"); err != nil {
		t.Fatal(err)
	}
	pass := func(step string) {
		t.Helper()
		before := get(t, cp)
		cached := before.DeepCopy()
		cached.Spec.Paused = false
		for _, conditionType := range healthConditions {
			meta.RemoveStatusCondition(&cached.Status.Conditions, conditionType)
		}
		r.Client = caching(cached)
		copies := serverCopies(t, cp, func() { reconcile(t, r, cp) })
		after := copies[len(copies)-1]
		for _, conditionType := range healthConditions {
			for _, c := range copies {
				if got := meta.FindStatusCondition(c.Status.Conditions, conditionType); got == nil || got.Reason == lodiav1alpha1.ReasonReconciling {
					t.Errorf("%s: the server held %s %+v at resourceVersion %s", step, conditionType, got, c.ResourceVersion)
				}
			}
			want := *meta.FindStatusCondition(before.Status.Conditions, conditionType)
			want.ObservedGeneration = after.Generation
			if got := meta.FindStatusCondition(after.Status.Conditions, conditionType); !equality.Semantic.DeepEqual(*got, want) {
				t.Errorf("%s: %s is %+v, want %+v", step, conditionType, got, want)
			}
		}
	}
	pass("at its generation")
	paused := get(t, cp)
	paused.Spec.Paused = true
	if err := k8s.Update(t.Context(), paused); err != nil {
		t.Fatal(err)
	}
	pass("at an earlier generation")
}

func TestANewControlPlaneStillGetsReconcilingFromHealth(t *testing.T) {
	ns := namespace(t)
	events, err := k8s.Watch(t.Context(), &lodiav1alpha1.ControlPlaneList{}, client.InNamespace(ns))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Stop()
	controlPlaneIn(t, ns, lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}})
	for {
		select {
		case event := <-events.ResultChan():
			fresh, ok := event.Object.(*lodiav1alpha1.ControlPlane)
			if !ok || event.Type != watch.Modified || meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionReachable) == nil && meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionControllersLeading) == nil {
				continue
			}
			for _, conditionType := range healthConditions {
				if c := meta.FindStatusCondition(fresh.Status.Conditions, conditionType); c == nil || c.Status != metav1.ConditionUnknown || c.Reason != lodiav1alpha1.ReasonReconciling {
					t.Errorf("the first %s is %+v, want Unknown/%s", conditionType, c, lodiav1alpha1.ReasonReconciling)
				}
			}
			return
		case <-time.After(30 * time.Second):
			t.Fatal("no condition of Health's was written")
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
