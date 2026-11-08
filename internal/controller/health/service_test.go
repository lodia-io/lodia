// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/health"
)

func settled(t *testing.T, before int) {
	t.Helper()
	var now int
	err := wait.PollUntilContextTimeout(t.Context(), 50*time.Millisecond, 10*time.Second, true, func(context.Context) (bool, error) {
		now = runtime.NumGoroutine()
		return now <= before, nil
	})
	if err != nil {
		t.Errorf("%d goroutines 10s after the pass, %d before it", now, before)
	}
}

func TestAServerThatAcceptsAndNeverAnswersEndsWithinTimeoutInEveryMode(t *testing.T) {
	const timeout = time.Second
	for name, endpoint := range map[string]lodiav1alpha1.Endpoint{
		"loadBalancer": {LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
		"nodePort":     {NodePort: &lodiav1alpha1.NodePortEndpoint{}},
	} {
		t.Run(name, func(t *testing.T) {
			cp := controlPlaneWith(t, endpoint)
			e := serve(t)
			writeBundle(t, cp, e)
			publish(t, cp, e)
			eventually(t, func() error {
				return expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200")
			})
			stalled, closed := stallListener(t)
			at := addressOf(t, stalled)
			pointed := interceptor.NewClient(k8s, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := c.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
						cp.Status.Endpoint.APIServer = at
					}
					return nil
				},
			})
			probe := &health.Reconciler{Client: pointed, Reader: k8s, FailuresBeforeFalse: failuresBeforeFalse, Period: time.Hour, Timeout: timeout, Clock: clock.RealClock{}}
			before := runtime.NumGoroutine()
			start := time.Now()
			reconcile(t, probe, cp)
			if took := time.Since(start); took < timeout || took >= 2*timeout {
				t.Errorf("a pass over a server that never answers took %s, want one Timeout of %s", took, timeout)
			}
			select {
			case <-closed:
			case <-time.After(3 * timeout):
				t.Fatalf("the probe left the stalled connection open longer than %s", 3*timeout)
			}
			settled(t, before)
		})
	}
}
