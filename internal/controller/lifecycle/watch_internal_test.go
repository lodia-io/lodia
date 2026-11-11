// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestAClassMapsToTheControlPlanesWhoseEtcdRunsIt(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	tenant := func(namespace, specClass, runs string) *lodiav1alpha1.ControlPlane {
		cp := &lodiav1alpha1.ControlPlane{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "dev"},
			Spec:       lodiav1alpha1.ControlPlaneSpec{Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{ClassName: specClass}}},
		}
		if runs != "" {
			cp.Status.Datastore = &lodiav1alpha1.DatastoreStatus{Class: &lodiav1alpha1.DatastoreClassStatus{Name: runs, Generation: 1}}
		}
		return cp
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		tenant("runs-it", "shared", "shared"),
		tenant("runs-it-by-default", "", "shared"),
		tenant("names-it-but-runs-another", "shared", "other"),
		tenant("names-it-with-no-etcd", "shared", ""),
		tenant("runs-another", "other", "other"),
	).Build()
	r := &Reconciler{Client: c}
	var got []string
	for _, request := range r.controlPlanesOn(t.Context(), &lodiav1alpha1.DatastoreClass{ObjectMeta: metav1.ObjectMeta{Name: "shared"}}) {
		got = append(got, request.Namespace)
	}
	slices.Sort(got)
	if want := []string{"runs-it", "runs-it-by-default"}; !slices.Equal(got, want) {
		t.Errorf("an edit of DatastoreClass shared enqueues %v, want %v", got, want)
	}
	if got := r.controlPlanesOn(t.Context(), &lodiav1alpha1.DatastoreClass{ObjectMeta: metav1.ObjectMeta{Name: "unused"}}); len(got) != 0 {
		t.Errorf("an edit of a class no Etcd runs enqueues %v", got)
	}
}
