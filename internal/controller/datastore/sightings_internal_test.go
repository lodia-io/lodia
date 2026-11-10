// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	druidapicommon "github.com/gardener/etcd-druid/api/common"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func withoutControlPlanes(t *testing.T) client.WithWatch {
	t.Helper()
	s := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(s).Build()
}

func running(name string, state druidapicommon.LastOperationState) *druidv1alpha1.Etcd {
	return &druidv1alpha1.Etcd{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a"},
		Status:     druidv1alpha1.EtcdStatus{LastOperation: &druidapicommon.LastOperation{RunID: "y", Type: druidv1alpha1.LastOperationTypeReconcile, State: state}},
	}
}

func sighted(r *Reconciler) []string {
	var names []string
	for key := range maps.Keys(r.sightings) {
		names = append(names, key.Name)
	}
	slices.Sort(names)
	return names
}

func passOver(t *testing.T, r *Reconciler, name string) error {
	t.Helper()
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "team-a", Name: name}})
	return err
}

func TestASightingIsDroppedWithItsRunOrItsControlPlane(t *testing.T) {
	r := &Reconciler{Client: withoutControlPlanes(t), Clock: clocktesting.NewFakePassiveClock(time.Now())}
	for _, name := range []string{"dev-etcd", "prod-etcd", "test-etcd"} {
		if !r.inFlight(running(name, druidv1alpha1.LastOperationStateProcessing)) {
			t.Fatalf("Etcd %s Processing under a run ID the record does not name is not in flight", name)
		}
	}
	for _, ended := range []*druidv1alpha1.Etcd{
		running("dev-etcd", druidv1alpha1.LastOperationStateSucceeded),
		running("dev-etcd", druidv1alpha1.LastOperationStateError),
		{ObjectMeta: metav1.ObjectMeta{Name: "dev-etcd", Namespace: "team-a"}},
	} {
		r.inFlight(running("dev-etcd", druidv1alpha1.LastOperationStateProcessing))
		if r.inFlight(ended) {
			t.Errorf("an Etcd whose last operation is %+v is in flight", ended.Status.LastOperation)
		}
		if got := sighted(r); !slices.Equal(got, []string{"prod-etcd", "test-etcd"}) {
			t.Errorf("once the last operation of dev-etcd is %+v the sightings are of %v, want prod-etcd and test-etcd", ended.Status.LastOperation, got)
		}
	}

	unavailable := *r
	unavailable.Client = interceptor.NewClient(withoutControlPlanes(t), interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return apierrors.NewServiceUnavailable("the API server is unavailable")
		},
	})
	if err := passOver(t, &unavailable, "prod"); !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("a pass whose read fails returned %v", err)
	}
	if got := sighted(r); !slices.Equal(got, []string{"prod-etcd", "test-etcd"}) {
		t.Errorf("a pass whose read of the ControlPlane fails leaves the sightings of %v, want prod-etcd and test-etcd", got)
	}
	if err := passOver(t, r, "prod"); err != nil {
		t.Fatalf("a pass over a ControlPlane that is gone returned %v", err)
	}
	if got := sighted(r); !slices.Equal(got, []string{"test-etcd"}) {
		t.Errorf("once ControlPlane prod is gone the sightings are of %v, want test-etcd", got)
	}
}

func TestTheWaitEndsAtTheBoundFromTheFirstSighting(t *testing.T) {
	first := time.Now()
	now := clocktesting.NewFakePassiveClock(first)
	r := &Reconciler{Clock: now}
	for _, at := range []time.Duration{0, specReconcileBound - time.Nanosecond} {
		now.SetTime(first.Add(at))
		if !r.inFlight(running("dev-etcd", druidv1alpha1.LastOperationStateProcessing)) {
			t.Errorf("a run first seen %s ago is not in flight", at)
		}
	}
	now.SetTime(first.Add(specReconcileBound))
	if r.inFlight(running("dev-etcd", druidv1alpha1.LastOperationStateProcessing)) {
		t.Errorf("a run first seen %s ago is in flight", specReconcileBound)
	}
}

func TestTwoWorkersOnTheSameEtcdShareItsSighting(t *testing.T) {
	r := &Reconciler{Client: withoutControlPlanes(t), Clock: clocktesting.NewFakePassiveClock(time.Now())}
	held := make([]bool, 2)
	var workers sync.WaitGroup
	for i := range held {
		workers.Go(func() { held[i] = r.inFlight(running("dev-etcd", druidv1alpha1.LastOperationStateProcessing)) })
	}
	workers.Go(func() {
		if err := passOver(t, r, "dev"); err != nil {
			t.Errorf("a pass over a ControlPlane that is gone returned %v", err)
		}
	})
	workers.Wait()
	if !held[0] || !held[1] {
		t.Errorf("two workers on an Etcd Processing under a run ID the record does not name hold %v, want both", held)
	}
}
