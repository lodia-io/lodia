// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func rolledOutAt(cp *lodiav1alpha1.ControlPlane, version, revision string, generation int64) []runtime.Object {
	one := int32(1)
	var objs []runtime.Object
	for _, component := range components {
		meta := objectMeta(cp, deploymentName(cp.Name, component))
		meta.Annotations = map[string]string{lodiav1alpha1.OperatorRevisionAnnotation: revision}
		meta.Generation = generation
		objs = append(objs, &appsv1.Deployment{
			ObjectMeta: meta,
			Spec: appsv1.DeploymentSpec{
				Replicas: &one,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: component, Image: "registry.k8s.io/" + component + ":" + version}}}},
			},
			Status: appsv1.DeploymentStatus{ObservedGeneration: generation, Replicas: 1, UpdatedReplicas: 1, AvailableReplicas: 1, ReadyReplicas: 1},
		})
	}
	return objs
}

// A cache behind the last roll moves no report back, while a move the API server holds, a hand edit back, is reported, and another owner's Deployment the API server holds under the name moves none.
func TestAReportMovesOnlyToWhatTheAPIServerHolds(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "team", Namespace: "ns", UID: "uid-1"}}
	one := int32(1)
	rolled := lodiav1alpha1.ComponentStatus{
		RolloutCount: lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: new(int32(1)), Ready: new(int32(1))}, Updated: &one},
		Rendered:     lodiav1alpha1.Rendered{Version: "v1.37.2", OperatorRevision: "r2"},
	}
	cp.Status.Components = &lodiav1alpha1.ComponentsStatus{APIServer: *rolled.DeepCopy(), ControllerManager: *rolled.DeepCopy(), Scheduler: *rolled.DeepCopy()}
	expect := func(r *Reconciler, version, revision string) {
		t.Helper()
		controlled, err := r.controlled(context.Background(), cp)
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.components(context.Background(), cp, controlled)
		if err != nil {
			t.Fatal(err)
		}
		for _, report := range []lodiav1alpha1.ComponentStatus{got.APIServer, got.ControllerManager, got.Scheduler} {
			if report.Version != version || report.OperatorRevision != revision {
				t.Errorf("a component reports %s/%s, want %s/%s", report.Version, report.OperatorRevision, version, revision)
			}
		}
	}

	server := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(rolledOutAt(cp, "v1.37.2", "r2", 2)...).Build()
	expect(&Reconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(rolledOutAt(cp, "v1.36.6", "r1", 1)...).Build(), Reader: server}, "v1.37.2", "r2")

	edited := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(rolledOutAt(cp, "v1.36.6", "r1", 3)...).Build()
	expect(&Reconciler{Client: edited, Reader: edited}, "v1.36.6", "r1")

	var reads int
	counting := interceptor.NewClient(server, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		reads++
		return c.Get(ctx, key, obj, opts...)
	}})
	expect(&Reconciler{Client: server, Reader: counting}, "v1.37.2", "r2")
	if reads != 0 {
		t.Errorf("a pass at rest read %d Deployments from the API server, want none", reads)
	}

	successor := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: cp.Name, Namespace: cp.Namespace, UID: "uid-2"}}
	another := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(rolledOutAt(successor, "v1.37.3", "r3", 2)...).Build()
	expect(&Reconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(rolledOutAt(cp, "v1.37.3", "r3", 2)...).Build(), Reader: another}, "v1.37.2", "r2")
}
