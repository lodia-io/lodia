// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

const restricted = "lodia-restricted"

func TestOperatorFromTheChart(t *testing.T) {
	var k8s client.Client
	f := features.New("the operator from the chart").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			return ctx
		}).
		Assess("the operator Deployment is Available", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			deployment := &appsv1.Deployment{}
			until(ctx, t, 2*time.Minute, "the operator Deployment is Available", func(ctx context.Context) error {
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: operatorNamespace, Name: release}, deployment); err != nil {
					return err
				}
				for _, c := range deployment.Status.Conditions {
					if c.Type == appsv1.DeploymentAvailable && c.Status == corev1.ConditionTrue {
						return nil
					}
				}
				return fmt.Errorf("%d of %d replicas available: %+v", deployment.Status.AvailableReplicas, ptr.Deref(deployment.Spec.Replicas, 1), deployment.Status.Conditions)
			})
			t.Logf("%d of %d replicas available, image %s", deployment.Status.AvailableReplicas, ptr.Deref(deployment.Spec.Replicas, 1), deployment.Spec.Template.Spec.Containers[0].Image)
			return ctx
		}).
		Assess("one replica holds the leader lease", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			deployment := operatorDeployment(ctx, t, k8s)
			var holder string
			until(ctx, t, 2*time.Minute, "a running replica holds the lease lodia.io", func(ctx context.Context) error {
				lease := &coordinationv1.Lease{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: operatorNamespace, Name: "lodia.io"}, lease); err != nil {
					return err
				}
				pods := &corev1.PodList{}
				if err := k8s.List(ctx, pods, client.InNamespace(operatorNamespace), client.MatchingLabels(deployment.Spec.Selector.MatchLabels)); err != nil {
					return err
				}
				identity := ptr.Deref(lease.Spec.HolderIdentity, "")
				var names []string
				for i := range pods.Items {
					pod := &pods.Items[i]
					if strings.HasPrefix(identity, pod.Name+"_") && running(pod) == nil {
						holder = pod.Name
						return nil
					}
					names = append(names, pod.Name)
				}
				return fmt.Errorf("the lease is held by %q, none of the running replicas %s", identity, strings.Join(names, ", "))
			})
			t.Logf("replica %s holds the lease", holder)
			return ctx
		}).
		Assess("the operator's pod passes Pod Security restricted", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			spec := operatorDeployment(ctx, t, k8s).Spec.Template.Spec
			create(ctx, t, k8s, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: restricted, Labels: map[string]string{"pod-security.kubernetes.io/enforce": "restricted"}}})
			create(ctx, t, k8s, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: restricted, Name: spec.ServiceAccountName}})
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: restricted, Name: release}, Spec: spec}
			if err := k8s.Create(ctx, pod, client.DryRunAll); err != nil {
				t.Fatalf("a namespace that enforces Pod Security restricted refuses the operator's pod: %v", err)
			}
			t.Logf("a namespace that enforces Pod Security restricted admits the operator's pod")
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if k8s != nil {
				if err := client.IgnoreNotFound(k8s.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: restricted}})); err != nil {
					t.Error(err)
				}
			}
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func operatorDeployment(ctx context.Context, t *testing.T, k8s client.Client) *appsv1.Deployment {
	t.Helper()
	deployment := &appsv1.Deployment{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: operatorNamespace, Name: release}, deployment); err != nil {
		t.Fatal(err)
	}
	return deployment
}
