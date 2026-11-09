// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && (ha || hold)

package e2e

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

var readyAndInputs = []string{
	lodiav1alpha1.ConditionReady,
	lodiav1alpha1.ConditionCertificatesReady,
	lodiav1alpha1.ConditionDatastoreReady,
	lodiav1alpha1.ConditionAPIServerReady,
	lodiav1alpha1.ConditionAccessReady,
	lodiav1alpha1.ConditionAddonsApplied,
	lodiav1alpha1.ConditionReachable,
	lodiav1alpha1.ConditionControllersLeading,
}

type transition struct {
	at              time.Time
	condition       string
	status          metav1.ConditionStatus
	reason, message string
}

type recorder struct {
	mu          sync.Mutex
	transitions []transition
	broken      error
	stop        func()
}

func record(ctx context.Context, t *testing.T, k8s client.WithWatch, key client.ObjectKey) *recorder {
	t.Helper()
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		t.Fatal(err)
	}
	r := &recorder{}
	r.observe(cp, time.Now())
	lw := &cache.ListWatch{WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
		return k8s.Watch(ctx, &lodiav1alpha1.ControlPlaneList{}, &client.ListOptions{
			Namespace: key.Namespace, FieldSelector: fields.OneTermEqualSelector("metadata.name", key.Name), Raw: &options,
		})
	}}
	watching, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w, err := watchtools.NewRetryWatcherWithContext(watching, cp.ResourceVersion, lw)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range w.ResultChan() {
			switch fresh, ok := event.Object.(*lodiav1alpha1.ControlPlane); {
			case event.Type == watch.Error:
				r.mu.Lock()
				r.broken = apierrors.FromObject(event.Object)
				r.mu.Unlock()
			case ok && (event.Type == watch.Added || event.Type == watch.Modified):
				r.observe(fresh, time.Now())
			}
		}
	}()
	r.stop = func() {
		cancel()
		w.Stop()
		<-done
	}
	return r
}

func (r *recorder) observe(cp *lodiav1alpha1.ControlPlane, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, conditionType := range readyAndInputs {
		next := transition{at: at, condition: conditionType, status: metav1.ConditionUnknown, reason: "Absent"}
		if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c != nil {
			next.status, next.reason, next.message = c.Status, c.Reason, c.Message
		}
		if last, ok := r.last(conditionType); ok && last.status == next.status && last.reason == next.reason && last.message == next.message {
			continue
		}
		r.transitions = append(r.transitions, next)
	}
}

func (r *recorder) last(conditionType string) (transition, bool) {
	for _, tr := range slices.Backward(r.transitions) {
		if tr.condition == conditionType {
			return tr, true
		}
	}
	return transition{}, false
}

func (r *recorder) log(t *testing.T, from, to time.Time) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.broken != nil {
		t.Errorf("the watch of the ControlPlane broke, so its record is incomplete: %v", r.broken)
	}
	for _, tr := range r.transitions {
		if tr.at.After(from) && !tr.at.After(to) {
			t.Logf("%s %s=%s %s: %s", tr.at.Format(time.RFC3339Nano), tr.condition, tr.status, tr.reason, tr.message)
		}
	}
	for _, conditionType := range readyAndInputs {
		t.Logf("%s was not True for at most %s in the %s of the step", conditionType, r.longestNotTrue(conditionType, from, to), to.Sub(from).Round(time.Second))
	}
}

func (r *recorder) alwaysTrue(t *testing.T, from, to time.Time, conditionTypes ...string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, conditionType := range conditionTypes {
		if longest := r.longestNotTrue(conditionType, from, to); longest > 0 {
			t.Errorf("%s was not True for %s", conditionType, longest)
		}
	}
}

func (r *recorder) longestNotTrue(conditionType string, from, to time.Time) time.Duration {
	status, since := metav1.ConditionUnknown, from
	var longest time.Duration
	for _, tr := range r.transitions {
		if tr.condition != conditionType || tr.at.After(to) || tr.status == status {
			continue
		}
		at := tr.at
		if at.Before(from) {
			at = from
		}
		if status != metav1.ConditionTrue {
			longest = max(longest, at.Sub(since))
		}
		status, since = tr.status, at
	}
	if status != metav1.ConditionTrue {
		longest = max(longest, to.Sub(since))
	}
	return longest
}
