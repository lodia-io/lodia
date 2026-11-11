// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"context"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestAClientThatComesTooLateToAnEtcdThatDoesNotAnswerKeepsNoSample(t *testing.T) {
	t.Parallel()
	unreachable := closedURL(t)
	for _, left := range []time.Duration{2 * time.Millisecond, 500 * time.Millisecond} {
		t.Run(left.String(), func(t *testing.T) {
			cp, etcd := sampledControlPlane("late", "uid-late")
			r, clk := sampler(cp, etcd)
			r.EtcdClient = func(ctx context.Context, e *druidv1alpha1.Etcd) (*clientv3.Client, error) {
				deadline, _ := ctx.Deadline()
				time.Sleep(time.Until(deadline) - left)
				return dialURL(unreachable.String())(ctx, e)
			}
			samplePass(t, r, client.ObjectKeyFromObject(cp))
			clk.Step(5 * time.Second)
			if got, ok := r.Sample(client.ObjectKeyFromObject(cp), cp.UID); ok {
				t.Errorf("a client that comes %s before the deadline is served %+v with size %v, want no sample", left, got, got.DBSize)
			}
		})
	}
}
