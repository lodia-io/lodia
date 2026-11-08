// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package controller_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestReadyInGatewayModeSaysWhyTheAPIServerIsDownWhileItsRoutesHaveNoEndpoints(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run make test")
	}
	k8s := start(t)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "ready-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	class, gateway, cp := gatewayTenant(ns.Name)
	for _, obj := range []client.Object{class, gateway, cp} {
		if err := k8s.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}

	eventually(t, func() error {
		etcd := &druidv1alpha1.Etcd{}
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns.Name, Name: cp.Name + "-etcd"}, etcd); err != nil {
			return err
		}
		etcd.Status = druidv1alpha1.EtcdStatus{ObservedGeneration: new(etcd.Generation), Ready: new(true), Replicas: 1, ReadyReplicas: 1}
		return k8s.Status().Update(t.Context(), etcd)
	})
	for _, suffix := range []string{"kube-apiserver", "konnectivity", "tunnel"} {
		eventually(t, func() error {
			route := &gatewayv1.TLSRoute{}
			if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns.Name, Name: cp.Name + "-" + suffix}, route); err != nil {
				return err
			}
			now := metav1.Now()
			route.Status.Parents = []gatewayv1.RouteParentStatus{{
				ParentRef:      route.Spec.ParentRefs[0],
				ControllerName: "gateway.envoyproxy.io/gatewayclass-controller",
				Conditions: []metav1.Condition{
					{Type: string(gatewayv1.RouteConditionAccepted), Status: metav1.ConditionTrue, Reason: string(gatewayv1.RouteReasonAccepted), Message: "Route is accepted", ObservedGeneration: route.Generation, LastTransitionTime: now},
					{Type: string(gatewayv1.RouteConditionResolvedRefs), Status: metav1.ConditionFalse, Reason: "EndpointsNotFound",
						Message: "no ready endpoints for the related Service " + ns.Name + "/" + cp.Name + "-kube-apiserver", ObservedGeneration: route.Generation, LastTransitionTime: now},
				},
			}}
			return k8s.Status().Update(t.Context(), route)
		})
	}

	eventually(t, func() error {
		fresh := &lodiav1alpha1.ControlPlane{}
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
			return err
		}
		routes := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionRoutesReady)
		ready := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionReady)
		if routes == nil || routes.Status != metav1.ConditionTrue || ready == nil || ready.Status != metav1.ConditionFalse ||
			ready.Reason != lodiav1alpha1.ReasonUnavailable || !strings.HasPrefix(ready.Message, "APIServerReady: kube-apiserver has 0 of 1 replicas ready") {
			return fmt.Errorf("RoutesReady is %+v and Ready %+v", routes, ready)
		}
		return nil
	})
}

func eventually(t *testing.T, check func() error) {
	t.Helper()
	var last error
	if err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 30*time.Second, true, func(context.Context) (bool, error) {
		last = check()
		return last == nil, nil
	}); err != nil {
		t.Fatalf("%v: %v", err, last)
	}
}
