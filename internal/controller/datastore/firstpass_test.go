// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
)

var datastoreConditions = []string{lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ConditionBackupReady}

func TestACacheWithoutDatastoreReadyLeavesTheServersConditions(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	const cp = "cached"
	createClass(t, newClass("cached", "Retain"))
	create(t, controlPlane(ns, cp, "cached"))
	writeGeneration(t, ns, cp, 1)
	waitEtcd(t, ns, cp, namesGeneration(1))
	readyOn(t, ns, cp, 1)
	before := getControlPlane(t, ns, cp)
	cached := before.DeepCopy()
	for _, conditionType := range datastoreConditions {
		meta.RemoveStatusCondition(&cached.Status.Conditions, conditionType)
	}
	copies := serverCopies(t, before, func() {
		if _, err := (&datastore.Reconciler{Client: caching(cached), Reader: k8s, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace, Revision: revision, EtcdDruidDeployment: druidDeployment, EtcdDruidVersion: druidVersion}).Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: cp}}); err != nil {
			t.Fatal(err)
		}
	})
	after := copies[len(copies)-1]
	for _, conditionType := range datastoreConditions {
		for _, c := range copies {
			if got := meta.FindStatusCondition(c.Status.Conditions, conditionType); got == nil || got.Reason == lodiav1alpha1.ReasonReconciling {
				t.Errorf("the server held %s %+v at resourceVersion %s", conditionType, got, c.ResourceVersion)
			}
		}
		if got, want := meta.FindStatusCondition(after.Status.Conditions, conditionType), meta.FindStatusCondition(before.Status.Conditions, conditionType); !equality.Semantic.DeepEqual(got, want) {
			t.Errorf("%s is %+v, want %+v", conditionType, got, want)
		}
	}
}

func TestANewControlPlaneStillGetsReconcilingFromDatastore(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	createClass(t, newClass("new", "Retain"))
	events, err := k8s.Watch(t.Context(), &lodiav1alpha1.ControlPlaneList{}, client.InNamespace(ns))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Stop()
	create(t, controlPlane(ns, "dev", "new"))
	for {
		select {
		case event := <-events.ResultChan():
			fresh, ok := event.Object.(*lodiav1alpha1.ControlPlane)
			if !ok || event.Type != watch.Modified || meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady) == nil && meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionBackupReady) == nil {
				continue
			}
			for _, conditionType := range datastoreConditions {
				if c := meta.FindStatusCondition(fresh.Status.Conditions, conditionType); c == nil || c.Status != metav1.ConditionUnknown || c.Reason != lodiav1alpha1.ReasonReconciling {
					t.Errorf("the first %s is %+v, want Unknown/%s", conditionType, c, lodiav1alpha1.ReasonReconciling)
				}
			}
			return
		case <-time.After(30 * time.Second):
			t.Fatal("no condition of Datastore's was written")
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
	before := getControlPlane(t, cp.Namespace, cp.Name)
	events, err := k8s.Watch(t.Context(), &lodiav1alpha1.ControlPlaneList{}, &client.ListOptions{Namespace: cp.Namespace, Raw: &metav1.ListOptions{ResourceVersion: before.ResourceVersion}})
	if err != nil {
		t.Fatal(err)
	}
	defer events.Stop()
	during()
	current := getControlPlane(t, cp.Namespace, cp.Name).ResourceVersion
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
