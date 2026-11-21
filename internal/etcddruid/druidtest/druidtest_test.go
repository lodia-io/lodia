// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package druidtest_test

import (
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"go.lodia.io/lodia/internal/contract"
	"go.lodia.io/lodia/internal/etcddruid/druidtest"
)

func TestASyncKeepsATransitionTimeWhileTheStatusHolds(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := druidv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKey{Namespace: "ns", Name: contract.EtcdName("cp")}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&druidv1alpha1.Etcd{}).
		WithObjects(&druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name, Generation: 1}}).Build()
	earlier := time.Now().Add(-time.Hour).Truncate(time.Second)
	sync := func(options ...druidtest.Option) time.Time {
		t.Helper()
		if err := druidtest.SetStatus(t.Context(), c, key.Namespace, "cp", options...); err != nil {
			t.Fatal(err)
		}
		etcd := &druidv1alpha1.Etcd{}
		if err := c.Get(t.Context(), key, etcd); err != nil {
			t.Fatal(err)
		}
		return druidv1alpha1.GetCondition(etcd, druidv1alpha1.ConditionTypeReady).LastTransitionTime.Time
	}

	sync(druidtest.Condition(druidtest.Ready(true, earlier, earlier)))
	if at := sync(); !at.Equal(earlier) {
		t.Errorf("Ready stayed True through a sync and its transition moved to %s from %s", at, earlier)
	}
	sync(druidtest.Condition(druidtest.Ready(false, earlier, earlier)))
	if at := sync(); !at.After(earlier.Add(time.Minute)) {
		t.Errorf("Ready turned from False to True and its transition is %s, want now", at)
	}
}
