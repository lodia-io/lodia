// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const apiTenant = "team-api"

func TestAPIServerOnKind(t *testing.T) {
	var k8s client.Client
	key := client.ObjectKey{Namespace: apiTenant, Name: controlPlane}
	f := features.New("the PKI, Datastore, Lifecycle and Workloads controllers run a tenant API server on a node port").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			create(ctx, t, k8s, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: apiTenant}})
			create(ctx, t, k8s, &lodiav1alpha1.DatastoreClass{
				ObjectMeta: metav1.ObjectMeta{Name: "e2e-api"},
				Spec:       lodiav1alpha1.DatastoreClassSpec{Storage: lodiav1alpha1.EtcdStorage{Size: resource.MustParse("1Gi"), ReclaimPolicy: "Delete"}},
			})
			create(ctx, t, k8s, &lodiav1alpha1.ControlPlane{
				ObjectMeta: metav1.ObjectMeta{Name: controlPlane, Namespace: apiTenant},
				Spec: lodiav1alpha1.ControlPlaneSpec{
					Version:   "v1.37.1",
					Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{ClassName: "e2e-api"}},
					Endpoint:  lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}},
					Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
				},
			})
			return ctx
		}).
		Assess("APIServerReady=True: the API server answers /readyz, the controller manager and the scheduler run", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 8*time.Minute, "APIServerReady=True", func(ctx context.Context) error {
				cp := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, key, cp); err != nil {
					return err
				}
				c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
				if c == nil || c.Status != metav1.ConditionTrue || c.ObservedGeneration != cp.Generation {
					return fmt.Errorf("APIServerReady is %+v; DatastoreReady %+v", c, meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady))
				}
				return nil
			})
			cp := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, key, cp); err != nil {
				t.Fatal(err)
			}
			e := cp.Status.Endpoint
			t.Logf("status.endpoint: API server %s:%d, konnectivity %s:%d", e.APIServer.Host, e.APIServer.Port, e.Konnectivity.Host, e.Konnectivity.Port)
			pods := &corev1.PodList{}
			if err := k8s.List(ctx, pods, client.InNamespace(apiTenant)); err != nil {
				t.Fatal(err)
			}
			for _, pod := range pods.Items {
				t.Logf("pod %s is %s", pod.Name, pod.Status.Phase)
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if k8s != nil {
				if err := client.IgnoreNotFound(k8s.Delete(ctx, &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: controlPlane, Namespace: apiTenant}})); err != nil {
					t.Error(err)
				}
				until(ctx, t, 3*time.Minute, "the ControlPlane is gone", func(ctx context.Context) error {
					return gone(k8s.Get(ctx, key, &lodiav1alpha1.ControlPlane{}))
				})
				for _, obj := range []client.Object{
					&lodiav1alpha1.DatastoreClass{ObjectMeta: metav1.ObjectMeta{Name: "e2e-api"}},
					&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: apiTenant}},
				} {
					if err := client.IgnoreNotFound(k8s.Delete(ctx, obj)); err != nil {
						t.Error(err)
					}
				}
			}
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}
