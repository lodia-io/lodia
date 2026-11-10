// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/testenv"
)

func labelledOnly() client.WithWatch {
	return interceptor.NewClient(unpaused(), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if _, ok := obj.(*lodiav1alpha1.ControlPlane); !ok && obj.GetLabels()[lodiav1alpha1.ControlPlaneLabel] == "" {
				return apierrors.NewNotFound(schema.GroupResource{Resource: "unlabelled"}, key.Name)
			}
			return nil
		},
	})
}

func foreignObject(kind, ns string, owner *metav1.OwnerReference) client.Object {
	meta := metav1.ObjectMeta{Namespace: ns}
	if owner != nil {
		meta.OwnerReferences = []metav1.OwnerReference{*owner}
	}
	pod := map[string]string{"app": "foreign"}
	switch kind {
	case "Service":
		meta.Name = lodiav1alpha1.APIServerServiceName("dev")
		return &corev1.Service{ObjectMeta: meta, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Ports: []corev1.ServicePort{{Name: "https", Port: 443, TargetPort: intstr.FromInt32(8443)}}}}
	case "ConfigMap":
		meta.Name = "dev-egress-selector"
		return &corev1.ConfigMap{ObjectMeta: meta, Data: map[string]string{"foreign": "yes"}}
	case "NetworkPolicy":
		meta.Name = "dev-kube-scheduler"
		return &networkingv1.NetworkPolicy{ObjectMeta: meta, Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: pod}}}
	case "Deployment":
		meta.Name = "dev-kube-scheduler"
		return &appsv1.Deployment{ObjectMeta: meta, Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: pod},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: pod}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "foreign", Image: "registry.example/foreign"}}}},
		}}
	case "PodDisruptionBudget":
		meta.Name = "dev-kube-apiserver"
		return &policyv1.PodDisruptionBudget{ObjectMeta: meta, Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: new(intstr.FromInt32(1)), Selector: &metav1.LabelSelector{MatchLabels: pod}}}
	}
	panic(kind)
}

func TestAnObjectAnotherOwnerKeepsOutsideTheCacheIsLeftAlone(t *testing.T) {
	owners := map[string]metav1.OwnerReference{
		"deployment-controls": {APIVersion: "apps/v1", Kind: "Deployment", Name: "someone", UID: "22222222-2222-4222-8222-222222222222", Controller: new(true)},
		"other-cp-referenced": {APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "other", UID: "33333333-3333-4333-8333-333333333333"},
	}
	for _, kind := range []string{"Service", "ConfigMap", "NetworkPolicy", "Deployment", "PodDisruptionBudget"} {
		for ownerName, owner := range owners {
			for mode, rev := range map[string]string{"held": previous, "unheld": revision} {
				t.Run(kind+"/"+ownerName+"/"+mode, func(t *testing.T) {
					ns := namespace(t)
					foreign := foreignObject(kind, ns, &owner)
					if err := k8s.Create(t.Context(), foreign); err != nil {
						t.Fatal(err)
					}
					if svc, ok := foreign.(*corev1.Service); ok {
						svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "198.51.100.7", IPMode: new(corev1.LoadBalancerIPModeVIP)}}
						if err := k8s.Status().Update(t.Context(), svc); err != nil {
							t.Fatal(err)
						}
					}
					rv := foreign.GetResourceVersion()
					cp := create(t, ns, loadBalancer(), highAvailability)
					setTargetAt(t, cp, version, rev)
					if kind != "Service" {
						setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
						inputsReadyBesidesTheTarget(t, cp)
					}
					testenv.Eventually(t, func() error {
						return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere)
					})
					settle(t, cp)
					want := fmt.Sprintf("%s %s by %s %s (UID %s)", kind, foreign.GetName(), owner.Kind, owner.Name, owner.UID)
					if c := condition(t, cp); c.Reason != lodiav1alpha1.ReasonObjectsControlledElsewhere || !strings.Contains(c.Message, want) {
						t.Errorf("APIServerReady %s %q, want it to name %s", c.Reason, c.Message, want)
					}
					fresh := foreign.DeepCopyObject().(client.Object)
					if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(foreign), fresh); err != nil {
						t.Fatal(err)
					}
					if fresh.GetResourceVersion() != rv {
						t.Errorf("the foreign %s was written: %s then %s, labels %v, refs %v", kind, rv, fresh.GetResourceVersion(), fresh.GetLabels(), fresh.GetOwnerReferences())
					}
					wantDeployments := 0
					if kind == "Deployment" {
						wantDeployments = 1
					}
					if n := deployments(t, cp); n != wantDeployments {
						t.Errorf("%d Deployments, want %d", n, wantDeployments)
					}
					if kind == "Service" {
						if e := get(t, cp).Status.Endpoint; e != nil {
							t.Errorf("endpoint %+v from the foreign Service", e)
						}
					}
					r := operatorAt(labelledOnly(), revision)
					res, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
					if err != nil {
						t.Fatal(err)
					}
					wantRequeue := time.Minute
					if ownerName == "predecessor-controls" {
						wantRequeue = 10 * time.Second
					}
					if res.RequeueAfter != wantRequeue {
						t.Errorf("requeue after %s, want %s", res.RequeueAfter, wantRequeue)
					}
				})
			}
		}
	}
}

func TestAnUnheldPassThatCannotReadWhatTheCacheLacksWritesNothing(t *testing.T) {
	cp := running(t)
	pause(t, cp)
	before := objectsOf(t, cp)
	blind := interceptor.NewClient(unpaused(), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok {
				return apierrors.NewNotFound(appsv1.Resource("deployments"), key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	for name, c := range map[string]struct {
		err  error
		want func(error) bool
	}{
		"503":      {apierrors.NewServiceUnavailable("injected"), apierrors.IsServiceUnavailable},
		"deadline": {context.DeadlineExceeded, func(err error) bool { return errors.Is(err, context.DeadlineExceeded) }},
	} {
		t.Run(name, func(t *testing.T) {
			r := operatorAt(noControlPlaneWrite(t, blind), revision)
			r.Reader = interceptor.NewClient(k8s, interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*appsv1.Deployment); ok {
						return c.err
					}
					return cl.Get(ctx, key, obj, opts...)
				},
			})
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
			if !c.want(err) {
				t.Errorf("the pass returned %v", err)
			}
		})
	}
	expectNoWrites(t, cp, before, "an unheld pass that cannot read what the cache lacks")
}

func TestAServiceAnotherOwnerKeepsAfterTheCheckIsNotRead(t *testing.T) {
	ns := namespace(t)
	owner := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "Deployment", Name: "someone", UID: "22222222-2222-4222-8222-222222222222", Controller: new(true)}
	foreign := foreignObject("Service", ns, &owner)
	if err := k8s.Create(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}
	svc := foreign.(*corev1.Service)
	svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "198.51.100.7", IPMode: new(corev1.LoadBalancerIPModeVIP)}}
	if err := k8s.Status().Update(t.Context(), svc); err != nil {
		t.Fatal(err)
	}
	cp := create(t, ns, loadBalancer(), func(spec *lodiav1alpha1.ControlPlaneSpec) { spec.Paused = true })
	setTargetAt(t, cp, version, previous)
	var mu sync.Mutex
	seen := 0
	r := operatorAt(labelledOnly(), next)
	r.Reader = interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Service); ok {
				mu.Lock()
				seen++
				first := seen == 1
				mu.Unlock()
				if first {
					return apierrors.NewNotFound(corev1.Resource("services"), key.Name)
				}
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Error("the pass over another owner's Service returned no error, want the refusal for the backoff")
	}
	c := condition(t, cp)
	if e := get(t, cp).Status.Endpoint; e != nil {
		t.Errorf("endpoint %+v read from another owner's Service", e)
	}
	if c.Reason != lodiav1alpha1.ReasonRefused {
		t.Errorf("APIServerReady %s, want Refused", c.Reason)
	}
}
