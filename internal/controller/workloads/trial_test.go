// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/controller/workloads"
	"go.lodia.io/lodia/internal/testenv"
)

func dryRun(opts []client.ApplyOption) bool {
	return len((&client.ApplyOptions{}).ApplyOptions(opts).DryRun) > 0
}

// A held pass whose dry run of the Service, of another object or of a route gets no answer stops there and writes no status: no answer is no observation.
func TestAHeldDryRunWithoutAnAnswerChangesNothing(t *testing.T) {
	for _, c := range []struct {
		name, kind string
		start      func(*testing.T, ...func(*lodiav1alpha1.ControlPlaneSpec)) *lodiav1alpha1.ControlPlane
	}{
		{"the Service", "Service", running},
		{"a ConfigMap", "ConfigMap", running},
		{"a route", "TLSRoute", gatewayRunning},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := c.start(t)
			pause(t, cp)
			refuseScheduler(t, cp.Namespace)
			if err := heldPass(t, operatorAt(unpaused(), revision), cp); !apistatus.Refused(err) {
				t.Fatalf("a pass whose Deployment is refused returned %v", err)
			}
			before := get(t, cp)
			unanswered := interceptor.NewClient(unpaused(func(_ client.ObjectKey, obj client.Object) {
				if route, ok := obj.(*gatewayv1.TLSRoute); ok && route.Name == "dev-tunnel" {
					delete(route.Labels, lodiav1alpha1.ControlPlaneLabel)
				}
			}), interceptor.Funcs{Apply: func(ctx context.Context, cl client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
				if dryRun(opts) && kindOf(t, obj) == c.kind {
					return apierrors.NewServiceUnavailable("the API server is starting")
				}
				return cl.Apply(ctx, obj, opts...)
			}})
			if err := heldPass(t, operatorAt(unanswered, next), cp); err == nil || apistatus.Refused(err) {
				t.Fatalf("a held pass whose dry run gets no answer returned %v", err)
			}
			if after := get(t, cp); after.ResourceVersion != before.ResourceVersion {
				t.Errorf("a held pass whose dry run got no answer wrote the ControlPlane: resourceVersion %s, then %s", before.ResourceVersion, after.ResourceVersion)
			}
		})
	}
}

// A held pass sends no dry run while no condition of Workloads holds a refusal, nor a pass at the granted revision under one, and a held pass under one sends one of every object Workloads writes.
func TestAHeldPassTriesTheGrantOnlyUnderARefusal(t *testing.T) {
	cp := running(t)
	pause(t, cp)
	var dryRuns atomic.Int32
	counting := interceptor.NewClient(unpaused(), interceptor.Funcs{Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
		if dryRun(opts) {
			dryRuns.Add(1)
		}
		return c.Apply(ctx, obj, opts...)
	}})
	if err := heldPass(t, operatorAt(counting, next), cp); err != nil {
		t.Fatal(err)
	}
	if n := dryRuns.Load(); n != 0 {
		t.Errorf("a held pass without a refusal sent %d dry runs", n)
	}
	refuseScheduler(t, cp.Namespace)
	for range 2 {
		if err := heldPass(t, operatorAt(counting, revision), cp); !apistatus.Refused(err) {
			t.Fatalf("a pass whose Deployment is refused returned %v", err)
		}
	}
	if n := dryRuns.Load(); n != 0 {
		t.Errorf("a pass at the granted revision sent %d dry runs", n)
	}
	if err := heldPass(t, operatorAt(counting, next), cp); !apistatus.Refused(err) {
		t.Fatalf("a held pass under the refusal returned %v", err)
	}
	if n, want := dryRuns.Load(), int32(len(objectsOf(t, cp))); n != want {
		t.Errorf("a held pass under a refusal sent %d dry runs, want %d, one of each object Workloads writes", n, want)
	}
}

// A held pass that writes a listener problem over a refused route update reports that problem, and with the listener back tries the route again while RoutesReady is not True, so the refusal stands before the grant's first pass.
func TestAHeldPassOverAListenerProblemTriesTheGrantAgain(t *testing.T) {
	cp := gatewayRunning(t)
	pause(t, cp)
	refusing := func(gatewayGone bool) client.WithWatch {
		return interceptor.NewClient(unpaused(func(_ client.ObjectKey, obj client.Object) {
			if route, ok := obj.(*gatewayv1.TLSRoute); ok && route.Name == "dev-tunnel" {
				delete(route.Labels, lodiav1alpha1.ControlPlaneLabel)
			}
		}), interceptor.Funcs{
			Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
				if kindOf(t, obj) == "TLSRoute" {
					return apierrors.NewForbidden(gatewayv1.Resource("tlsroutes"), "dev-tunnel", errors.New("denied by a policy"))
				}
				return c.Apply(ctx, obj, opts...)
			},
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*gatewayv1.Gateway); ok && gatewayGone {
					return apierrors.NewNotFound(gatewayv1.Resource("gateways"), key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
	}
	expect := func(status metav1.ConditionStatus, reason string) {
		t.Helper()
		if c := routesReady(t, cp); c.Status != status || c.Reason != reason {
			t.Fatalf("RoutesReady is %+v, want %s %s", c, status, reason)
		}
	}
	if err := heldPass(t, operatorAt(refusing(false), revision), cp); !apistatus.Refused(err) {
		t.Fatalf("a pass whose route is refused returned %v", err)
	}
	if err := heldPass(t, operatorAt(refusing(false), next), cp); !apistatus.Refused(err) {
		t.Fatalf("a held pass whose dry run is refused returned %v", err)
	}
	expect(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused)
	if err := heldPass(t, operatorAt(refusing(true), next), cp); err != nil {
		t.Fatal(err)
	}
	expect(metav1.ConditionFalse, lodiav1alpha1.ReasonGatewayNotFound)
	if err := heldPass(t, operatorAt(refusing(false), next), cp); !apistatus.Refused(err) {
		t.Fatalf("a held pass over the listener problem returned %v, want the refusal of its dry run", err)
	}
	expect(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused)
	if err := heldPass(t, operatorAt(refusing(false), revision), cp); !apistatus.Refused(err) {
		t.Fatalf("the granted revision's pass returned %v, want the refusal again", err)
	}
	expect(metav1.ConditionTrue, lodiav1alpha1.ReasonRefused)
}

// A held pass whose dry run of a ConfigMap is refused tries no Deployment that mounts it, as the grant's first pass sends none.
func TestAHeldPassTriesNoDeploymentThatMountsARefusedConfigMap(t *testing.T) {
	cp := running(t)
	taken := get(t, cp).Generation
	pause(t, cp)
	var tried []string
	refusing := interceptor.NewClient(unpaused(), interceptor.Funcs{Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
		key := kindOf(t, obj) + " " + keyOf(t, obj).Name
		if dryRun(opts) {
			tried = append(tried, key)
		}
		if key == "ConfigMap dev-egress-selector" {
			return apierrors.NewForbidden(corev1.Resource("configmaps"), "dev-egress-selector", errors.New("denied by a policy"))
		}
		return c.Apply(ctx, obj, opts...)
	}})
	if err := heldPass(t, operatorAt(refusing, revision), cp); !apistatus.Refused(err) {
		t.Fatalf("a pass whose ConfigMap is refused returned %v", err)
	}
	if err := heldPass(t, operatorAt(refusing, next), cp); !apistatus.Refused(err) {
		t.Fatalf("a held pass whose dry run is refused returned %v", err)
	}
	if slices.Contains(tried, "Deployment dev-kube-apiserver") || !slices.Contains(tried, "Deployment dev-kube-scheduler") {
		t.Errorf("the held pass tried %v, the scheduler's Deployment and not the API server's, which mounts the refused ConfigMap", tried)
	}
	if err := expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReady+"the grant's write of ConfigMap dev-egress-selector would be refused: the API server refuses the dry run; the reason is in Lodia's log"); err != nil {
		t.Error(err)
	}
}

// A held pass names the refused dry run of one route after another route's problem, under the problem's reason, False.
func TestAHeldPassNamesARefusedRouteBesideARouteNotAccepted(t *testing.T) {
	cp := gatewayRunning(t)
	pause(t, cp)
	refusing := interceptor.NewClient(unpaused(func(_ client.ObjectKey, obj client.Object) {
		if route, ok := obj.(*gatewayv1.TLSRoute); ok && route.Name == "dev-tunnel" {
			delete(route.Labels, lodiav1alpha1.ControlPlaneLabel)
		}
	}), interceptor.Funcs{Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
		if kindOf(t, obj) == "TLSRoute" && keyOf(t, obj).Name == "dev-tunnel" {
			return apierrors.NewForbidden(gatewayv1.Resource("tlsroutes"), "dev-tunnel", errors.New("denied by a policy"))
		}
		return c.Apply(ctx, obj, opts...)
	}})
	if err := heldPass(t, operatorAt(refusing, revision), cp); !apistatus.Refused(err) {
		t.Fatalf("a pass whose route is refused returned %v", err)
	}
	route := tlsRoute(t, cp.Namespace, "dev-konnectivity")
	setRouteStatus(t, route, metav1.ConditionTrue, metav1.ConditionFalse, "BackendNotFound", route.Generation)
	testenv.Eventually(t, func() error {
		_ = heldPass(t, operatorAt(refusing, next), cp)
		want := "TLSRoute dev-konnectivity is not ResolvedRefs by Gateway " + cp.Namespace + "/shared: BackendNotFound; the grant's write of TLSRoute dev-tunnel would be refused: the API server refuses the dry run; the reason is in Lodia's log"
		if c := routesReady(t, cp); c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonRouteNotAccepted || c.Message != want {
			return fmt.Errorf("RoutesReady is %+v, want False RouteNotAccepted %q", c, want)
		}
		return nil
	})
}

// A held pass that writes an outage over a refusal its dry run found tries the grant again on the next held pass, so the refusal stands once the component is back and Lifecycle grants nothing.
func TestAHeldPassOverAnOutageTriesTheGrantAgain(t *testing.T) {
	cp := running(t)
	taken := get(t, cp).Generation
	pause(t, cp)
	refuseScheduler(t, cp.Namespace)
	if err := heldPass(t, operatorAt(unpaused(), revision), cp); !apistatus.Refused(err) {
		t.Fatalf("a pass whose Deployment is refused returned %v", err)
	}
	held := operatorAt(unpaused(), next)
	if err := heldPass(t, held, cp); !apistatus.Refused(err) {
		t.Fatalf("a held pass whose dry run is refused returned %v", err)
	}
	if err := expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReady+triedScheduler); err != nil {
		t.Fatal(err)
	}
	setDeploymentStatus(t, cp, "kube-controller-manager", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = 0, 0 })
	testenv.Eventually(t, func() error {
		_ = heldPass(t, held, cp)
		return expectAt(cp, taken, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-controller-manager has 0 of 1 replicas ready; "+triedScheduler)
	})
	setDeploymentStatus(t, cp, "kube-controller-manager", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = 1, 1 })
	testenv.Eventually(t, func() error {
		if err := heldPass(t, held, cp); !apistatus.Refused(err) {
			return fmt.Errorf("a held pass over the outage returned %w, want the refusal of its dry run", err)
		}
		return expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReady+triedScheduler)
	})
	if got := lifecyclePassThrough(t, unpaused(), cp, next).Status.Target.OperatorRevision; got != revision {
		t.Errorf("Lifecycle granted %s over the refusal", got)
	}
}

// A held pass tries the grant's writes with the grant's force, so a field another manager took over by hand, which the grant takes back, neither stops the trial nor keeps the refusal once its cause is gone.
func TestAHeldPassTriesTheGrantWithItsForce(t *testing.T) {
	cp := running(t)
	taken := get(t, cp).Generation
	pause(t, cp)
	binding := refuseScheduler(t, cp.Namespace)
	if err := heldPass(t, operatorAt(unpaused(), revision), cp); !apistatus.Refused(err) {
		t.Fatalf("a pass whose Deployment is refused returned %v", err)
	}
	edited := &corev1.ConfigMap{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: "dev-egress-selector"}, edited); err != nil {
		t.Fatal(err)
	}
	for key := range edited.Data {
		edited.Data[key] = "edited by hand"
	}
	if err := k8s.Update(t.Context(), edited, client.FieldOwner("kubectl-edit")); err != nil {
		t.Fatal(err)
	}
	held := operatorAt(unpaused(), next)
	if err := heldPass(t, held, cp); !apistatus.Refused(err) {
		t.Fatalf("a held pass whose dry run is refused returned %v", err)
	}
	if err := expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReady+triedScheduler); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		if err := heldPass(t, held, cp); err != nil {
			return err
		}
		return expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, strings.TrimSuffix(allReady, "; "))
	})
}

// A held wait under a refusal tries the grant's writes as a held pass does, so a count that dips and recovers inside the wait names the refusal again, False and then True under it, writing nothing, and Lifecycle grants no slot past the wait.
func TestAHeldWaitTriesTheGrantThroughADipOfItsCount(t *testing.T) {
	cp := running(t)
	taken := get(t, cp).Generation
	pause(t, cp)
	refuseScheduler(t, cp.Namespace)
	if err := heldPass(t, operatorAt(unpaused(), revision), cp); !apistatus.Refused(err) {
		t.Fatalf("a pass whose Deployment is refused returned %v", err)
	}
	held := operatorAt(unpaused(), next)
	if err := heldPass(t, held, cp); !apistatus.Refused(err) {
		t.Fatalf("a held pass whose dry run is refused returned %v", err)
	}
	setFalse(t, cp, lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ReasonQuorumLost)
	for _, step := range []struct {
		ready   int32
		status  metav1.ConditionStatus
		reason  string
		message string
	}{
		{1, metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReady + triedScheduler},
		{0, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready; " + triedScheduler},
		{1, metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReady + triedScheduler},
	} {
		setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = step.ready, step.ready })
		before := objectsOf(t, cp)
		testenv.Eventually(t, func() error {
			if err := heldPass(t, held, cp); !apistatus.Refused(err) {
				return fmt.Errorf("a held wait returned %w, want the refusal of its dry run", err)
			}
			return expectAt(cp, taken, step.status, step.reason, step.message)
		})
		expectNoWrites(t, cp, before, "a held wait")
	}
	setTrue(t, cp, lodiav1alpha1.ConditionDatastoreReady, "Ready")
	if got := lifecyclePassThrough(t, unpaused(), cp, next).Status.Target.OperatorRevision; got != revision {
		t.Fatalf("Lifecycle granted %s over the refusal the wait named", got)
	}
	if err := heldPass(t, held, cp); !apistatus.Refused(err) {
		t.Fatalf("the held pass past the wait returned %v, want the refusal of its dry run", err)
	}
	if err := expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReady+triedScheduler); err != nil {
		t.Error(err)
	}
	if got := lifecyclePassThrough(t, unpaused(), cp, next).Status.Target.OperatorRevision; got != revision {
		t.Errorf("Lifecycle granted %s over the refusal", got)
	}
}

// A held wait under a refusal whose cause is removed finds every dry run accepted and reports what runs, as at rest, so Lifecycle grants the revision in the window.
func TestAHeldWaitEndsARefusalWhoseCauseIsGone(t *testing.T) {
	cp := running(t)
	taken := get(t, cp).Generation
	pause(t, cp)
	binding := refuseScheduler(t, cp.Namespace)
	if err := heldPass(t, operatorAt(unpaused(), revision), cp); !apistatus.Refused(err) {
		t.Fatalf("a pass whose Deployment is refused returned %v", err)
	}
	held := operatorAt(unpaused(), next)
	if err := heldPass(t, held, cp); !apistatus.Refused(err) {
		t.Fatalf("a held pass whose dry run is refused returned %v", err)
	}
	setFalse(t, cp, lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ReasonQuorumLost)
	if err := heldPass(t, held, cp); !apistatus.Refused(err) {
		t.Fatalf("a held wait whose dry run is refused returned %v", err)
	}
	if err := expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReady+triedScheduler); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		if err := heldPass(t, held, cp); err != nil {
			return err
		}
		return expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, strings.TrimSuffix(allReady, "; "))
	})
	setTrue(t, cp, lodiav1alpha1.ConditionDatastoreReady, "Ready")
	if got := lifecyclePassThrough(t, unpaused(), cp, next).Status.Target.OperatorRevision; got != next {
		t.Errorf("Lifecycle left status.target.operatorRevision at %s once the wait found the refusal gone, want %s", got, next)
	}
}

// A held wait sends no dry run and writes nothing while no condition of Workloads holds a refusal or is not True, and under one sends one dry run of each object Workloads writes, a missing one's included, and writes nothing, neither that object nor the deletion of a PodDisruptionBudget a held pass deletes.
func TestAHeldWaitTriesTheGrantOnlyUnderARefusal(t *testing.T) {
	cp := running(t)
	pause(t, cp)
	var dryRuns, writes atomic.Int32
	write := func() { writes.Add(1) }
	counting := interceptor.NewClient(unpaused(), interceptor.Funcs{
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			if dryRun(opts) {
				dryRuns.Add(1)
			} else {
				write()
			}
			return c.Apply(ctx, obj, opts...)
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			write()
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			write()
			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			write()
			return c.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			write()
			return c.Delete(ctx, obj, opts...)
		},
		SubResourceApply: func(ctx context.Context, c client.Client, name string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
			write()
			return c.SubResource(name).Apply(ctx, obj, opts...)
		},
	})
	setFalse(t, cp, lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ReasonQuorumLost)
	if err := heldPass(t, operatorAt(counting, next), cp); err != nil {
		t.Fatal(err)
	}
	if d, w := dryRuns.Load(), writes.Load(); d != 0 || w != 0 {
		t.Errorf("a held wait without a refusal sent %d dry runs and %d writes", d, w)
	}
	setTrue(t, cp, lodiav1alpha1.ConditionDatastoreReady, "Ready")
	refuseScheduler(t, cp.Namespace)
	if err := heldPass(t, operatorAt(unpaused(), revision), cp); !apistatus.Refused(err) {
		t.Fatalf("a pass whose Deployment is refused returned %v", err)
	}
	if err := heldPass(t, operatorAt(unpaused(), next), cp); !apistatus.Refused(err) {
		t.Fatalf("a held pass whose dry run is refused returned %v", err)
	}
	want := int32(len(objectsOf(t, cp)))
	missing := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: "dev-kube-controller-manager"}}
	if err := k8s.Delete(t.Context(), missing); err != nil {
		t.Fatal(err)
	}
	budget := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name: "dev-kube-scheduler", Namespace: cp.Namespace,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: cp.Name, UID: get(t, cp).UID, Controller: new(true)}},
		},
		Spec: policyv1.PodDisruptionBudgetSpec{MaxUnavailable: new(intstr.FromInt32(1)), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "left"}}},
	}
	if err := k8s.Create(t.Context(), budget); err != nil {
		t.Fatal(err)
	}
	setFalse(t, cp, lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ReasonQuorumLost)
	if err := heldPass(t, operatorAt(counting, next), cp); !apistatus.Refused(err) {
		t.Fatalf("a held wait under the refusal returned %v", err)
	}
	if d, w := dryRuns.Load(), writes.Load(); d != want || w != 0 {
		t.Errorf("a held wait under a refusal sent %d dry runs and %d writes, want %d dry runs, one of each object Workloads writes, and no write", d, w, want)
	}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(missing), &networkingv1.NetworkPolicy{}); !apierrors.IsNotFound(err) {
		t.Errorf("after a held wait NetworkPolicy %s, which was missing, is there: %v", missing.Name, err)
	}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(budget), &policyv1.PodDisruptionBudget{}); err != nil {
		t.Errorf("after a held wait PodDisruptionBudget %s: %v", budget.Name, err)
	}
}

// A held wait over a condition it does not count, a wait of an older generation, keeps it as it is and tries nothing past what it waits for, the refused authentication configuration of the spec unnamed.
func TestAHeldWaitOverAConditionItDoesNotCountKeepsIt(t *testing.T) {
	cp := controlPlane(t, loadBalancer(), withJWT(oidc("user.nope == 'x'")))
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	setTarget(t, cp)
	testenv.Eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates)
	})
	waiting := *condition(t, cp)
	pause(t, cp)
	inputsReadyBesidesTheTarget(t, cp)
	setFalse(t, cp, lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ReasonQuorumLost)
	held := operatorAt(unpaused(), next)
	for range 2 {
		if err := heldPass(t, held, cp); err != nil {
			t.Fatal(err)
		}
		if got := *condition(t, cp); got.Status != waiting.Status || got.Reason != waiting.Reason || got.Message != waiting.Message || got.ObservedGeneration != waiting.ObservedGeneration {
			t.Fatalf("APIServerReady is %+v, want %+v as it was", got, waiting)
		}
	}
}

// A held wait that cannot build the render, for a Secret not hashed yet or for no Ready node at a nodePort endpoint, keeps a condition that is not True as it is while what runs recovers, and the first held pass past it tries the grant's writes and names the refusal.
func TestAHeldWaitWithoutARenderKeepsAConditionNotTrue(t *testing.T) {
	const down = "kube-apiserver has 0 of 1 replicas ready; " + triedScheduler
	for name, c := range map[string]struct {
		start        func(*testing.T, ...func(*lodiav1alpha1.ControlPlaneSpec)) *lodiav1alpha1.ControlPlane
		stop, resume func(*testing.T, *lodiav1alpha1.ControlPlane, *workloads.Reconciler)
	}{
		"a Secret not hashed yet": {
			running,
			func(t *testing.T, cp *lodiav1alpha1.ControlPlane, _ *workloads.Reconciler) {
				setSecret(t, cp, lodiav1alpha1.KonnectivityKubeconfigSecret, "")
			},
			func(t *testing.T, cp *lodiav1alpha1.ControlPlane, _ *workloads.Reconciler) {
				setSecret(t, cp, lodiav1alpha1.KonnectivityKubeconfigSecret, "one")
			},
		},
		"no Ready node at a nodePort endpoint": {
			runningOnNodePort,
			func(_ *testing.T, _ *lodiav1alpha1.ControlPlane, r *workloads.Reconciler) {
				r.Reader = nodeless{apiReader}
			},
			func(_ *testing.T, _ *lodiav1alpha1.ControlPlane, r *workloads.Reconciler) { r.Reader = apiReader },
		},
	} {
		t.Run(name, func(t *testing.T) {
			cp := c.start(t)
			taken := get(t, cp).Generation
			pause(t, cp)
			refuseScheduler(t, cp.Namespace)
			if err := heldPass(t, operatorAt(unpaused(), revision), cp); !apistatus.Refused(err) {
				t.Fatalf("a pass whose Deployment is refused returned %v", err)
			}
			held := operatorAt(unpaused(), next)
			setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = 0, 0 })
			testenv.Eventually(t, func() error {
				if err := heldPass(t, held, cp); !apistatus.Refused(err) {
					return fmt.Errorf("a held pass whose dry run is refused returned %w", err)
				}
				return expectAt(cp, taken, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, down)
			})
			c.stop(t, cp, held)
			setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = 1, 1 })
			for range 2 {
				if err := heldPass(t, held, cp); err != nil {
					t.Fatal(err)
				}
				if err := expectAt(cp, taken, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, down); err != nil {
					t.Fatalf("a held wait without a render: %v", err)
				}
			}
			c.resume(t, cp, held)
			if err := heldPass(t, held, cp); !apistatus.Refused(err) {
				t.Fatalf("the held pass past the wait returned %v, want the refusal of its dry run", err)
			}
			if err := expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReady+triedScheduler); err != nil {
				t.Error(err)
			}
		})
	}
}

type nodeless struct{ client.Reader }

func (r nodeless) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, nodes := list.(*corev1.NodeList); nodes {
		return nil
	}
	return r.Reader.List(ctx, list, opts...)
}

// A held wait on a target the running operator's table does not hold, which no pass under this revision passes, counts what runs as at the granted revision, so a tenant that serves is True again once what dipped recovers, though Lifecycle grants nothing over the target.
func TestAHeldWaitOnATargetTheTableDoesNotHoldCountsWhatRuns(t *testing.T) {
	cp := running(t)
	taken := get(t, cp).Generation
	pause(t, cp)
	refuseScheduler(t, cp.Namespace)
	if err := heldPass(t, operatorAt(unpaused(), revision), cp); !apistatus.Refused(err) {
		t.Fatalf("a pass whose Deployment is refused returned %v", err)
	}
	held := operatorAt(unpaused(), next)
	setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = 0, 0 })
	testenv.Eventually(t, func() error {
		if err := heldPass(t, held, cp); !apistatus.Refused(err) {
			return fmt.Errorf("a held pass whose dry run is refused returned %w", err)
		}
		return expectAt(cp, taken, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready; "+triedScheduler)
	})
	setTargetAt(t, cp, "v1.34.1", revision)
	setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = 1, 1 })
	for range 2 {
		if err := heldPass(t, held, cp); err != nil {
			t.Fatal(err)
		}
		if err := expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, strings.TrimSuffix(allReady, "; ")); err != nil {
			t.Fatalf("a held wait on a target outside the table: %v", err)
		}
	}
	after := lifecyclePassThrough(t, unpaused(), cp, next)
	if p := meta.FindStatusCondition(after.Status.Conditions, lodiav1alpha1.ConditionProgressing); after.Status.Target.OperatorRevision != revision || p == nil || p.Reason != lodiav1alpha1.ReasonUnsupported {
		t.Errorf("Lifecycle left status.target.operatorRevision at %s with Progressing %+v, want %s and Unsupported", after.Status.Target.OperatorRevision, p, revision)
	}
	if ready := meta.FindStatusCondition(after.Status.Conditions, lodiav1alpha1.ConditionReady); ready == nil || ready.Status != metav1.ConditionTrue {
		t.Errorf("Ready is %+v over a tenant that serves", ready)
	}
}

// Under the suite's manager a held pass over a wait kept from an older generation keeps it as it is, with nothing of what its check and its trial find appended, so its message stays as it was and no status write brings another pass.
func TestAHeldPassOverAKeptWaitAppendsNothingUnderTheManager(t *testing.T) {
	cp := controlPlane(t, loadBalancer(), withJWT(oidc("user.nope == 'x'")))
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	setTarget(t, cp)
	testenv.Eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates)
	})
	kept := *condition(t, cp)
	changeSpec(t, cp, func(s *lodiav1alpha1.ControlPlaneSpec) { s.APIServer.ExtraArgs = map[string]string{"event-ttl": "2h"} })
	setTargetAt(t, cp, version, previous)
	inputsReadyBesidesTheTarget(t, cp)
	deadline := time.Now().Add(time.Minute)
	last, quiet := "", time.Now()
	for time.Since(quiet) < 3*time.Second {
		if time.Now().After(deadline) {
			t.Fatal("the ControlPlane was still written a minute after its inputs were ready")
		}
		fresh := get(t, cp)
		c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
		if c.Status != kept.Status || c.Reason != kept.Reason || c.Message != kept.Message || c.ObservedGeneration != kept.ObservedGeneration {
			t.Fatalf("APIServerReady is %s %s at generation %d, %d bytes, resourceVersion %s: %q, want %+v as it was", c.Status, c.Reason, c.ObservedGeneration, len(c.Message), fresh.ResourceVersion, c.Message, kept)
		}
		if fresh.ResourceVersion != last {
			last, quiet = fresh.ResourceVersion, time.Now()
		}
		time.Sleep(100 * time.Millisecond)
	}
	pause(t, cp)
}

// In gateway mode a held wait that cannot build the render names the Service's refused dry run in the APIServerReady it counts anew, through the wait and past it, so Lifecycle grants nothing over it.
func TestAHeldGatewayWaitWithoutARenderNamesARefusedService(t *testing.T) {
	const triedService = "the grant's write of Service dev-kube-apiserver would be refused: the API server refuses the dry run; the reason is in Lodia's log"
	cp := gatewayRunning(t)
	taken := get(t, cp).Generation
	pause(t, cp)
	held := operatorAt(interceptor.NewClient(unpaused(), interceptor.Funcs{
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			if kindOf(t, obj) == "Service" {
				return apierrors.NewForbidden(corev1.Resource("services"), lodiav1alpha1.APIServerServiceName(cp.Name), errors.New("denied by a policy"))
			}
			return c.Apply(ctx, obj, opts...)
		},
	}), next)
	if err := heldPass(t, held, cp); err != nil {
		t.Fatalf("a held pass at rest returned %v", err)
	}
	route := tlsRoute(t, cp.Namespace, cp.Name+"-tunnel")
	setRouteStatus(t, route, metav1.ConditionFalse, metav1.ConditionFalse, "NotAllowedByListeners", route.Generation)
	setSecret(t, cp, lodiav1alpha1.KonnectivityKubeconfigSecret, "")
	if err := heldPass(t, held, cp); err != nil {
		t.Fatalf("a held wait at rest returned %v", err)
	}
	for _, accepted := range []bool{false, true, true} {
		if accepted {
			accept(t, cp)
		}
		if err := heldPass(t, held, cp); !apistatus.Refused(err) {
			t.Fatalf("a held wait whose Service's dry run is refused returned %v", err)
		}
		if err := expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReady+triedService); err != nil {
			t.Fatalf("a held wait, routes accepted %t: %v", accepted, err)
		}
	}
	setSecret(t, cp, lodiav1alpha1.KonnectivityKubeconfigSecret, "one")
	if err := heldPass(t, held, cp); !apistatus.Refused(err) {
		t.Fatalf("the held pass past the wait returned %v", err)
	}
	if err := expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonRefused, allReady+triedService); err != nil {
		t.Fatal(err)
	}
	if got := lifecyclePassThrough(t, unpaused(), cp, next).Status.Target.OperatorRevision; got != revision {
		t.Errorf("Lifecycle granted %s over the Service's refusal", got)
	}
}

// In loadBalancer mode LoadBalancerReady not True fires the trial on its own: a held wait that cannot build the render tries the Service then, and the refusal it finds holds the grant past the wait.
func TestAHeldWaitTriesTheServiceWhileLoadBalancerReadyIsNotTrue(t *testing.T) {
	cp := running(t, atLBAddress)
	pause(t, cp)
	held := operatorAt(interceptor.NewClient(unpaused(), interceptor.Funcs{
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			if kindOf(t, obj) == "Service" {
				return apierrors.NewForbidden(corev1.Resource("services"), lodiav1alpha1.APIServerServiceName(cp.Name), errors.New("denied by a policy"))
			}
			return c.Apply(ctx, obj, opts...)
		},
	}), next)
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: "203.0.113.99"})
	setSecret(t, cp, lodiav1alpha1.KonnectivityKubeconfigSecret, "")
	for range 2 {
		_ = heldPass(t, held, cp)
	}
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	for range 2 {
		_ = heldPass(t, held, cp)
	}
	setSecret(t, cp, lodiav1alpha1.KonnectivityKubeconfigSecret, "one")
	if err := heldPass(t, held, cp); !apistatus.Refused(err) {
		t.Errorf("the held pass past the wait returned %v, want the Service's refusal", err)
	}
	if c := meta.FindStatusCondition(get(t, cp).Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady); c == nil || c.Reason != lodiav1alpha1.ReasonRefused {
		t.Errorf("LoadBalancerReady is %+v past the wait, want the Service's refusal", c)
	}
	if got := lifecyclePassThrough(t, unpaused(), cp, next).Status.Target.OperatorRevision; got != revision {
		t.Errorf("Lifecycle granted %s over the Service's refusal", got)
	}
}

// A held wait over a wait at the object's generation, which counts nothing, reports that wait and counts no Deployment.
func TestAHeldWaitOverAWaitAtTheObjectsGenerationReportsThatWait(t *testing.T) {
	cp := controlPlane(t, loadBalancer(), withJWT(oidc("user.nope == 'x'")))
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	setTarget(t, cp)
	testenv.Eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates)
	})
	pause(t, cp)
	inputsReadyBesidesTheTarget(t, cp)
	setFalse(t, cp, lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ReasonQuorumLost)
	g := get(t, cp).Generation
	applyStatus(t, cp, workloads.FieldManager, map[string]any{"conditions": []any{map[string]any{
		"type": lodiav1alpha1.ConditionAPIServerReady, "status": "False", "reason": lodiav1alpha1.ReasonWaitingForDatastore,
		"message": "DatastoreReady is not True for generation 2", "observedGeneration": g, "lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}}})
	held := operatorAt(unpaused(), next)
	for range 2 {
		if err := heldPass(t, held, cp); err != nil {
			t.Fatal(err)
		}
		if c := condition(t, cp); c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonWaitingForDatastore || c.ObservedGeneration != g {
			t.Fatalf("APIServerReady is %+v, want False WaitingForDatastore at generation %d", c, g)
		}
	}
}

func closedWindow(spec *lodiav1alpha1.ControlPlaneSpec) {
	now := time.Now().UTC()
	spec.Maintenance = &lodiav1alpha1.Maintenance{Window: &lodiav1alpha1.MaintenanceWindow{Begin: now.Add(2 * time.Hour).Format("15:04"), End: now.Add(3 * time.Hour).Format("15:04")}}
}

// A held nodePort pass without a Ready node at the endpoint waits as the other waits do, so over a tenant that serves it keeps the availability it reports and the next pass, with the node back, counts again: Lifecycle grants nothing at once outside the window, and the release is scheduled for its window at rest and waits for its cause under a refusal.
func TestAHeldNodePortPassWithoutANodeAtItsEndpointKeepsWhatServes(t *testing.T) {
	for _, refused := range []bool{false, true} {
		t.Run(fmt.Sprintf("refused %t", refused), func(t *testing.T) {
			cp := runningOnNodePort(t, closedWindow)
			taken := get(t, cp).Generation
			pause(t, cp)
			reason, message := lodiav1alpha1.ReasonReady, strings.TrimSuffix(allReady, "; ")
			if refused {
				refuseScheduler(t, cp.Namespace)
				if err := heldPass(t, operatorAt(unpaused(), revision), cp); !apistatus.Refused(err) {
					t.Fatalf("a pass whose Deployment is refused returned %v", err)
				}
				reason, message = lodiav1alpha1.ReasonRefused, allReady+triedScheduler
			}
			held := operatorAt(unpaused(), next)
			lost := operatorAt(unpaused(), next)
			lost.Reader = nodeless{apiReader}
			for _, step := range []struct {
				name    string
				r       *workloads.Reconciler
				refuses bool
			}{{"a held pass", held, refused}, {"a held pass without a Ready node", lost, false}, {"a held pass with the node back", held, refused}} {
				if err := heldPass(t, step.r, cp); step.refuses && !apistatus.Refused(err) || !step.refuses && err != nil {
					t.Fatalf("%s returned %v", step.name, err)
				}
				if err := expectAt(cp, taken, metav1.ConditionTrue, reason, message); err != nil {
					t.Fatalf("%s: %v", step.name, err)
				}
			}
			after := lifecyclePassThrough(t, unpaused(), cp, next)
			p := meta.FindStatusCondition(after.Status.Conditions, lodiav1alpha1.ConditionProgressing)
			if after.Status.Target.OperatorRevision != revision || refused != (p.Reason == lodiav1alpha1.ReasonFailed) || refused == (after.Status.Target.ScheduledFor != nil) {
				t.Errorf("Lifecycle left status.target.operatorRevision at %s, scheduledFor %v, with Progressing %+v, want %s, scheduled for the window unless refused", after.Status.Target.OperatorRevision, after.Status.Target.ScheduledFor, p, revision)
			}
		})
	}
}

type sortsFirst struct{ client.Reader }

func (r sortsFirst) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := r.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	if nodes, ok := list.(*corev1.NodeList); ok {
		nodes.Items = append(nodes.Items, corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "0-sorts-first"},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
				Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.66"}},
			},
		})
	}
	return nil
}

func TestANodePortPassWithoutAReadyNodeKeepsTheEndpointForItsNodeToComeBack(t *testing.T) {
	for name, rev := range map[string]string{"granted": revision, "held": next} {
		t.Run(name, func(t *testing.T) {
			cp := runningOnNodePort(t)
			published := *get(t, cp).Status.Endpoint
			pause(t, cp)
			lost := operatorAt(unpaused(), rev)
			lost.Reader = nodeless{apiReader}
			back := operatorAt(unpaused(), rev)
			back.Reader = sortsFirst{apiReader}
			for _, step := range []struct {
				name string
				r    *workloads.Reconciler
			}{{"a pass without a Ready node", lost}, {"a pass with the node back and another Ready node sorting first", back}} {
				passOf(t, step.r, cp)
				if e := get(t, cp).Status.Endpoint; e == nil || *e != published {
					t.Fatalf("%s: status.endpoint is %+v, want %+v", step.name, e, published)
				}
			}
		})
	}
}

func TestAGrantedNodePortPassWithoutAReadyNodeCountsWhatRuns(t *testing.T) {
	cp := runningOnNodePort(t)
	pause(t, cp)
	taken := get(t, cp).Generation
	granted := operatorAt(unpaused(), revision)
	setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = 0, 0 })
	passOf(t, granted, cp)
	if err := expectAt(cp, taken, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-apiserver has 0 of 1 replicas ready"); err != nil {
		t.Fatal(err)
	}
	setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) { s.ReadyReplicas, s.AvailableReplicas = 1, 1 })
	lost := operatorAt(unpaused(), revision)
	lost.Reader = nodeless{apiReader}
	passOf(t, lost, cp)
	if err := expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, strings.TrimSuffix(allReady, "; ")); err != nil {
		t.Error(err)
	}
}

// A held pass that waits for the grant over a tenant that never served names a refusal its trial finds under the refusal's own reason, False, so Lifecycle holds the grant, and once the cause is gone it waits for the grant again, which Lifecycle gives at once.
func TestAHeldPassOverTheGrantsWaitHoldsTheGrantUnderARefusalItsTrialFinds(t *testing.T) {
	cp := controlPlane(t, loadBalancer(), func(spec *lodiav1alpha1.ControlPlaneSpec) { spec.Paused = true })
	setTargetAt(t, cp, version, previous)
	held := operatorAt(unpaused(), next)
	passOf(t, held, cp)
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReadyBesidesTheTarget(t, cp)
	passOf(t, held, cp)
	for _, component := range componentNames {
		markAvailable(t, deployment(t, cp, component))
	}
	binding := refuseScheduler(t, cp.Namespace)
	if err := heldPass(t, held, cp); !apistatus.Refused(err) {
		t.Fatalf("a held pass whose dry run is refused returned %v", err)
	}
	if err := expectAt(cp, 1, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, waitingForTheGrant(previous)+"; "+triedScheduler); err != nil {
		t.Fatal(err)
	}
	if got := lifecyclePassThrough(t, unpaused(), cp, next).Status.Target.OperatorRevision; got != previous {
		t.Errorf("Lifecycle granted %s over the refusal", got)
	}
	if err := k8s.Delete(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		if err := heldPass(t, held, cp); err != nil {
			return err
		}
		return expectAt(cp, 1, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, waitingForTheGrant(previous))
	})
	if got := lifecyclePassThrough(t, unpaused(), cp, next).Status.Target.OperatorRevision; got != next {
		t.Errorf("Lifecycle left status.target.operatorRevision at %s once the refusal was gone, want %s at once", got, next)
	}
}

func TestAHeldPassOverTheGrantsWaitHoldsTheGrantWhileTheAuthenticationConfigurationIsRefused(t *testing.T) {
	jwt := oidc()
	jwt[0].Issuer.URL = "https://" + lbAddress
	cp := controlPlane(t, loadBalancer(), func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.Paused = true
		withJWT(jwt)(spec)
	}, closedWindow)
	setTargetAt(t, cp, version, previous)
	held := operatorAt(unpaused(), next)
	passOf(t, held, cp)
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReadyBesidesTheTarget(t, cp)
	passOf(t, held, cp)
	for _, component := range componentNames {
		markAvailable(t, deployment(t, cp, component))
	}
	refused := waitingForTheGrant(previous) + "; " + notWritten + overlapErrors("https://"+lbAddress) + servesNone
	for _, pass := range []string{"a held pass", "the next held pass"} {
		passOf(t, held, cp)
		if err := expectAt(cp, 1, metav1.ConditionFalse, lodiav1alpha1.ReasonAuthenticationInvalid, refused); err != nil {
			t.Fatalf("%s: %v", pass, err)
		}
	}
	if got := lifecyclePassThrough(t, unpaused(), cp, next).Status.Target.OperatorRevision; got != previous {
		t.Errorf("Lifecycle granted %s over the refusal", got)
	}
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: "203.0.113.20"})
	passOf(t, held, cp)
	if err := expectAt(cp, 1, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, waitingForTheGrant(previous)); err != nil {
		t.Fatal(err)
	}
	if got := lifecyclePassThrough(t, unpaused(), cp, next).Status.Target.OperatorRevision; got != next {
		t.Errorf("Lifecycle left status.target.operatorRevision at %s once the refusal ended, want %s at once", got, next)
	}
}

func TestAHeldWaitOverTheGrantsWaitUnderARefusalKeepsThatWait(t *testing.T) {
	jwt := oidc()
	jwt[0].Issuer.URL = "https://" + lbAddress
	for _, c := range []struct {
		name                  string
		spec                  func(*lodiav1alpha1.ControlPlaneSpec)
		refuse                func(t *testing.T, cp *lodiav1alpha1.ControlPlane) (end func())
		refuses               bool
		input, lost, restored string
		reason, refusal       string
	}{
		{
			name: "the authentication configuration",
			spec: withJWT(jwt),
			refuse: func(t *testing.T, cp *lodiav1alpha1.ControlPlane) func() {
				return func() { setIngress(t, cp, corev1.LoadBalancerIngress{IP: "203.0.113.20"}) }
			},
			input: lodiav1alpha1.ConditionCertificatesReady, lost: "Issuing", restored: "Issued",
			reason: lodiav1alpha1.ReasonAuthenticationInvalid, refusal: notWritten + overlapErrors("https://"+lbAddress) + servesNone,
		},
		{
			name: "a refused dry run",
			spec: func(*lodiav1alpha1.ControlPlaneSpec) {},
			refuse: func(t *testing.T, cp *lodiav1alpha1.ControlPlane) func() {
				binding := refuseScheduler(t, cp.Namespace)
				return func() {
					if err := k8s.Delete(t.Context(), binding); err != nil {
						t.Fatal(err)
					}
				}
			},
			refuses: true,
			input:   lodiav1alpha1.ConditionDatastoreReady, lost: lodiav1alpha1.ReasonQuorumLost, restored: "Ready",
			reason: lodiav1alpha1.ReasonRefused, refusal: triedScheduler,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := controlPlane(t, loadBalancer(), func(spec *lodiav1alpha1.ControlPlaneSpec) {
				spec.Paused = true
				c.spec(spec)
			}, closedWindow)
			setTargetAt(t, cp, version, previous)
			held := operatorAt(unpaused(), next)
			passOf(t, held, cp)
			setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
			inputsReadyBesidesTheTarget(t, cp)
			passOf(t, held, cp)
			for _, component := range componentNames {
				markAvailable(t, deployment(t, cp, component))
			}
			end := c.refuse(t, cp)
			for _, step := range []struct {
				name string
				set  func()
			}{
				{"a held pass", func() {}},
				{"a held wait for " + c.input, func() { setFalse(t, cp, c.input, c.lost) }},
				{"the held pass past the wait", func() { setTrue(t, cp, c.input, c.restored) }},
			} {
				step.set()
				if err := heldPass(t, held, cp); c.refuses != apistatus.Refused(err) || !c.refuses && err != nil {
					t.Fatalf("%s returned %v", step.name, err)
				}
				if err := expectAt(cp, 1, metav1.ConditionFalse, c.reason, waitingForTheGrant(previous)+"; "+c.refusal); err != nil {
					t.Fatalf("%s: %v", step.name, err)
				}
			}
			if got := lifecyclePassThrough(t, unpaused(), cp, next).Status.Target.OperatorRevision; got != previous {
				t.Errorf("Lifecycle granted %s over the refusal", got)
			}
			end()
			testenv.Eventually(t, func() error {
				if err := heldPass(t, held, cp); err != nil {
					return err
				}
				return expectAt(cp, 1, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, waitingForTheGrant(previous))
			})
			if got := lifecyclePassThrough(t, unpaused(), cp, next).Status.Target.OperatorRevision; got != next {
				t.Errorf("Lifecycle left status.target.operatorRevision at %s once the refusal ended, want %s at once", got, next)
			}
		})
	}
}
