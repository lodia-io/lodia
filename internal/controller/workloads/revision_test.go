// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/controller/workloads"
	"go.lodia.io/lodia/internal/testenv"
)

const (
	previous = "v0.0.0-test+previous"
	next     = "v0.0.1-test+next"
)

func highAvailability(spec *lodiav1alpha1.ControlPlaneSpec) {
	spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
}

func operatorAt(c client.WithWatch, rev string) *workloads.Reconciler {
	return &workloads.Reconciler{Client: c, Reader: apiReader, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true, Revision: rev}
}

func passOf(t *testing.T, r *workloads.Reconciler, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
}

func grant(t *testing.T, cp *lodiav1alpha1.ControlPlane, rev string) {
	t.Helper()
	setTargetAt(t, cp, get(t, cp).Status.Target.Version, rev)
}

func objectsOf(t *testing.T, cp *lodiav1alpha1.ControlPlane) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, list := range []client.ObjectList{
		&corev1.ServiceList{}, &corev1.ConfigMapList{}, &networkingv1.NetworkPolicyList{},
		&appsv1.DeploymentList{}, &policyv1.PodDisruptionBudgetList{}, &gatewayv1.TLSRouteList{},
	} {
		if err := k8s.List(t.Context(), list, client.InNamespace(cp.Namespace)); err != nil {
			t.Fatal(err)
		}
		items, err := meta.ExtractList(list)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			o := item.(client.Object)
			out[fmt.Sprintf("%T %s", o, o.GetName())] = fmt.Sprintf("resourceVersion %s, generation %d", o.GetResourceVersion(), o.GetGeneration())
		}
	}
	return out
}

func expectNoWrites(t *testing.T, cp *lodiav1alpha1.ControlPlane, before map[string]string, what string) {
	t.Helper()
	if after := objectsOf(t, cp); !maps.Equal(after, before) {
		t.Errorf("%s wrote:\n%v\nthen\n%v", what, before, after)
	}
}

func settle(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	state := func() string { return get(t, cp).ResourceVersion + fmt.Sprint(objectsOf(t, cp)) }
	last, since := state(), time.Now()
	testenv.Eventually(t, func() error {
		if now := state(); now != last {
			last, since = now, time.Now()
		}
		if time.Since(since) < 2*time.Second {
			return errors.New("the ControlPlane or an object Workloads writes changed in the last two seconds")
		}
		return nil
	})
}

func byHand(t *testing.T, obj client.Object, change func()) {
	t.Helper()
	testenv.Eventually(t, func() error {
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(obj), obj); err != nil {
			return err
		}
		change()
		return k8s.Update(t.Context(), obj, client.FieldOwner("intruder"))
	})
}

func editEveryObject(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	named := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Namespace: cp.Namespace, Name: name} }
	svc := &corev1.Service{ObjectMeta: named(lodiav1alpha1.APIServerServiceName(cp.Name))}
	byHand(t, svc, func() { svc.Spec.Selector[lodiav1alpha1.ComponentLabel] = "edited" })
	for _, name := range []string{cp.Name + "-egress-selector", cp.Name + "-authentication-config"} {
		c := &corev1.ConfigMap{ObjectMeta: named(name)}
		byHand(t, c, func() {
			for key := range c.Data {
				c.Data[key] = "edited"
			}
		})
	}
	for _, component := range componentNames {
		p := &networkingv1.NetworkPolicy{ObjectMeta: named(cp.Name + "-" + component)}
		byHand(t, p, func() { p.Spec.PodSelector.MatchLabels[lodiav1alpha1.ComponentLabel] = "edited" })
		d := &appsv1.Deployment{ObjectMeta: named(cp.Name + "-" + component)}
		byHand(t, d, func() { d.Spec.Template.Spec.Containers[0].Image = "registry.example/edited:" + version })
		b := &policyv1.PodDisruptionBudget{ObjectMeta: named(cp.Name + "-" + component)}
		byHand(t, b, func() { b.Spec.MaxUnavailable = new(intstr.FromInt32(2)) })
	}
	if cp.Spec.Endpoint.Gateway == nil {
		return
	}
	for _, name := range routeNames(cp) {
		route := &gatewayv1.TLSRoute{ObjectMeta: named(name)}
		byHand(t, route, func() { route.Spec.Rules[0].BackendRefs[0].Weight = new(int32(2)) })
	}
	accept(t, cp)
}

func revisions(c *lodiav1alpha1.ComponentsStatus) string {
	if c == nil {
		return "absent"
	}
	var got []string
	for _, s := range []*lodiav1alpha1.ComponentStatus{c.APIServer, c.ControllerManager, c.Scheduler} {
		switch {
		case s == nil:
			got = append(got, "absent")
		case s.OperatorRevision == "":
			got = append(got, "none")
		default:
			got = append(got, s.OperatorRevision)
		}
	}
	return strings.Join(got, " ")
}

func expectRevisions(t *testing.T, cp *lodiav1alpha1.ControlPlane, want ...string) {
	t.Helper()
	testenv.Eventually(t, func() error {
		if got := revisions(get(t, cp).Status.Components); got != strings.Join(want, " ") {
			return fmt.Errorf("status.components reports the revisions %s, want %s", got, strings.Join(want, " "))
		}
		return nil
	})
}

func expectStamps(t *testing.T, cp *lodiav1alpha1.ControlPlane, want string) {
	t.Helper()
	for _, component := range componentNames {
		if stamp := deployment(t, cp, component).Annotations[lodiav1alpha1.OperatorRevisionAnnotation]; stamp != want {
			t.Errorf("Deployment %s is stamped %q, want %s", component, stamp, want)
		}
	}
}

func templatesOf(t *testing.T, cp *lodiav1alpha1.ControlPlane) map[string]corev1.PodTemplateSpec {
	t.Helper()
	out := map[string]corev1.PodTemplateSpec{}
	for _, component := range componentNames {
		out[component] = deployment(t, cp, component).Spec.Template
	}
	return out
}

func written(obj client.Object) bool {
	switch o := obj.(type) {
	case *corev1.Service, *appsv1.Deployment, *networkingv1.NetworkPolicy, *policyv1.PodDisruptionBudget, *gatewayv1.TLSRoute:
		return true
	case *metav1.PartialObjectMetadata:
		return o.Kind == "ConfigMap"
	}
	return false
}

func applying(t *testing.T, c client.WithWatch, applied **lodiav1alpha1.ControlPlane) client.WithWatch {
	t.Helper()
	return interceptor.NewClient(c, interceptor.Funcs{
		SubResourceApply: func(_ context.Context, _ client.Client, _ string, obj runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
			*applied = decoded(t, obj)
			return nil
		},
	})
}

func TestAHeldPassWritesNothingThatExistsAndReportsWhatRuns(t *testing.T) {
	for mode, start := range map[string]func(*testing.T, ...func(*lodiav1alpha1.ControlPlaneSpec)) *lodiav1alpha1.ControlPlane{
		"loadBalancer": running, "nodePort": runningOnNodePort, "gateway": gatewayRunning,
	} {
		t.Run(mode, func(t *testing.T) {
			cp := start(t, highAvailability)
			grant(t, cp, previous)
			settle(t, cp)
			published := get(t, cp).Status.Endpoint
			ready := *condition(t, cp)
			editEveryObject(t, cp)
			settle(t, cp)
			before := objectsOf(t, cp)

			held := operatorAt(k8s, next)
			passOf(t, held, cp)
			passOf(t, held, cp)
			expectNoWrites(t, cp, before, "a held pass over objects its render changes")
			if err := expectAt(cp, ready.ObservedGeneration, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, ready.Message); err != nil {
				t.Error(err)
			}
			if e := get(t, cp).Status.Endpoint; published == nil || !equality.Semantic.DeepEqual(e, published) {
				t.Errorf("status.endpoint is %+v, want the published %+v", e, published)
			}
			if cp.Spec.Endpoint.Gateway != nil {
				if err := expectRoutes(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted); err != nil {
					t.Error(err)
				}
			}

			grant(t, cp, next)
			passOf(t, held, cp)
			after := objectsOf(t, cp)
			for name, was := range before {
				if after[name] == was {
					t.Errorf("the pass that sees the grant left %s at %s, though its render changes it", name, was)
				}
			}
			expectStamps(t, cp, next)
		})
	}
}

func TestAGrantThatChangesNoTemplateRollsNoPod(t *testing.T) {
	cp := running(t)
	expectRevisions(t, cp, revision, revision, revision)
	expectStamps(t, cp, revision)
	templates := templatesOf(t, cp)

	grant(t, cp, next)
	granted := operatorAt(k8s, next)
	passOf(t, granted, cp)
	expectStamps(t, cp, next)
	for component, template := range templatesOf(t, cp) {
		if !equality.Semantic.DeepEqual(template, templates[component]) {
			t.Errorf("a grant that renders nothing new changed the pod template of %s", component)
		}
	}
	passOf(t, granted, cp)
	if got := revisions(get(t, cp).Status.Components); got != strings.Join([]string{revision, revision, revision}, " ") {
		t.Errorf("status.components reports the revisions %s before the Deployment controller observed the stamps", got)
	}
	for _, component := range componentNames {
		markAvailable(t, deployment(t, cp, component))
	}
	expectRevisions(t, cp, next, next, next)
}

func TestARevisionIsReportedOnceItsTemplateHasRolledOut(t *testing.T) {
	cp := running(t)
	templates := templatesOf(t, cp)
	grant(t, cp, next)
	r := operatorAt(k8s, next)
	r.SignerImage = "registry.example/lodia-signer:next"
	passOf(t, r, cp)
	after := templatesOf(t, cp)
	for component, template := range templates {
		if changed := !equality.Semantic.DeepEqual(after[component], template); changed != (component == "kube-apiserver") {
			t.Fatalf("a new signer image changed the template of %s: %t, want the API server's alone", component, changed)
		}
	}
	expectStamps(t, cp, next)
	markAvailable(t, deployment(t, cp, "kube-controller-manager"))
	markAvailable(t, deployment(t, cp, "kube-scheduler"))
	setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) {
		*s = appsv1.DeploymentStatus{ObservedGeneration: deployment(t, cp, "kube-apiserver").Generation, Replicas: 2, UpdatedReplicas: 1, ReadyReplicas: 2, AvailableReplicas: 1}
	})
	expectRevisions(t, cp, revision, next, next)
	markAvailable(t, deployment(t, cp, "kube-apiserver"))
	expectRevisions(t, cp, next, next, next)
}

func TestAHeldPassCreatesWhatIsMissing(t *testing.T) {
	cp := gatewayRunning(t, highAvailability)
	pause(t, cp)
	grant(t, cp, previous)
	named := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Namespace: cp.Namespace, Name: name} }
	missing := []client.Object{
		&corev1.ConfigMap{ObjectMeta: named("dev-egress-selector")},
		&networkingv1.NetworkPolicy{ObjectMeta: named("dev-kube-controller-manager")},
		&appsv1.Deployment{ObjectMeta: named("dev-kube-scheduler")},
		&policyv1.PodDisruptionBudget{ObjectMeta: named("dev-kube-apiserver")},
		&gatewayv1.TLSRoute{ObjectMeta: named("dev-tunnel")},
	}
	for _, obj := range missing {
		if err := k8s.Delete(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	before := objectsOf(t, cp)

	passOf(t, operatorAt(unpaused(), next), cp)
	after := objectsOf(t, cp)
	for name, was := range before {
		if after[name] != was {
			t.Errorf("a held pass wrote %s: %s, then %s", name, was, after[name])
		}
	}
	for _, obj := range missing {
		if _, ok := after[fmt.Sprintf("%T %s", obj, obj.GetName())]; !ok {
			t.Errorf("a held pass did not create the missing %T %s", obj, obj.GetName())
		}
	}
	if stamp := deployment(t, cp, "kube-scheduler").Annotations[lodiav1alpha1.OperatorRevisionAnnotation]; stamp != next {
		t.Errorf("the Deployment a held pass created is stamped %q, want the running %s", stamp, next)
	}
}

func TestAHeldPassReadsWhatTheCacheLacksAndWritesNothing(t *testing.T) {
	cp := gatewayRunning(t, highAvailability)
	ready := *condition(t, cp)
	pause(t, cp)
	grant(t, cp, previous)
	for _, name := range routeNames(cp) {
		route := &gatewayv1.TLSRoute{ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: name}}
		byHand(t, route, func() { route.Spec.Rules[0].BackendRefs[0].Weight = new(int32(2)) })
	}
	accept(t, cp)
	published := get(t, cp).Status.Endpoint
	before := objectsOf(t, cp)
	blind := interceptor.NewClient(unpaused(), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if written(obj) {
				return apierrors.NewNotFound(schema.GroupResource{Resource: fmt.Sprintf("%T", obj)}, key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	var applied *lodiav1alpha1.ControlPlane
	passOf(t, operatorAt(applying(t, blind, &applied), next), cp)
	expectNoWrites(t, cp, before, "a held pass whose cache lacks every object")
	if applied == nil {
		t.Fatal("the pass applied no status")
	}
	if c := meta.FindStatusCondition(applied.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady); c == nil || c.Status != metav1.ConditionTrue ||
		c.Reason != lodiav1alpha1.ReasonReady || c.Message != ready.Message || c.ObservedGeneration != ready.ObservedGeneration {
		t.Errorf("APIServerReady is %+v, want %s %q at generation %d, from the Deployments read without the cache", c, ready.Reason, ready.Message, ready.ObservedGeneration)
	}
	if c := meta.FindStatusCondition(applied.Status.Conditions, lodiav1alpha1.ConditionRoutesReady); c == nil || c.Status != metav1.ConditionTrue || c.Reason != lodiav1alpha1.ReasonAccepted {
		t.Errorf("RoutesReady is %+v, want True, Accepted, from the routes read without the cache", c)
	}
	if !equality.Semantic.DeepEqual(applied.Status.Endpoint, published) {
		t.Errorf("the pass applied the endpoint %+v, want the published %+v", applied.Status.Endpoint, published)
	}
}

func TestAHeldPassAtANewGenerationKeepsTheGenerationItDescribes(t *testing.T) {
	cp := running(t)
	ready := *condition(t, cp)
	generations := generationsOf(t, cp)
	pause(t, cp)
	grant(t, cp, previous)
	settle(t, cp)
	update(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.APIServer = &lodiav1alpha1.APIServer{ExtraArgs: map[string]string{"audit-log-maxage": "7"}}
	})
	if get(t, cp).Generation == ready.ObservedGeneration {
		t.Fatal("the spec change did not move the generation")
	}

	held := operatorAt(unpaused(), next)
	passOf(t, held, cp)
	if err := expectAt(cp, ready.ObservedGeneration, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, ready.Message); err != nil {
		t.Error(err)
	}
	if after := generationsOf(t, cp); !maps.Equal(after, generations) {
		t.Errorf("a held pass rendered the new generation: generations %v, then %v", generations, after)
	}

	grant(t, cp, next)
	passOf(t, held, cp)
	expectFlags(t, deployment(t, cp, "kube-apiserver").Spec.Template.Spec.Containers[0].Command, "--audit-log-maxage=7")
	if err := expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady); err != nil {
		t.Error(err)
	}
}

func TestAHeldPassOutsideTheRunningTableReportsWhatRuns(t *testing.T) {
	for name, c := range map[string]struct {
		target, report, unheld string
	}{
		"a target":   {target: "v1.34.1", unheld: "status.target.version v1.34.1 is not a version Lodia runs"},
		"a reported": {target: version, report: "v1.33.2", unheld: "status.components.apiServer.version v1.33.2 is not a version Lodia runs"},
	} {
		t.Run(name, func(t *testing.T) {
			cp := running(t)
			ready := *condition(t, cp)
			pause(t, cp)
			reader := interceptor.NewClient(k8s, interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := cl.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					if live, ok := obj.(*lodiav1alpha1.ControlPlane); ok && c.report != "" {
						live.Status.Components.APIServer.Version = c.report
					}
					return nil
				},
			})
			pass := func(rev string) {
				t.Helper()
				setTargetAt(t, cp, c.target, rev)
				r := operatorAt(unpaused(), revision)
				r.Reader = reader
				passOf(t, r, cp)
			}
			before := objectsOf(t, cp)
			pass(previous)
			if err := expectAt(cp, ready.ObservedGeneration, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, ready.Message); err != nil {
				t.Errorf("a held pass does not report what runs: %v", err)
			}
			expectNoWrites(t, cp, before, "a held pass outside the running table")
			pass(revision)
			if err := expectAt(cp, get(t, cp).Generation, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, c.unheld); err != nil {
				t.Errorf("a pass that is not held: %v", err)
			}
		})
	}
}

func waitingForTheGrant(running, granted string) string {
	return fmt.Sprintf("waiting for Lifecycle to grant the running operator revision %s; status.target.operatorRevision is %s", running, granted)
}

func TestAHeldPassOverAConditionThatReportsNoAvailabilityWaitsForTheGrant(t *testing.T) {
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
	passOf(t, held, cp)
	if err := expectAt(cp, 1, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, waitingForTheGrant(next, previous)); err != nil {
		t.Error(err)
	}
}

func TestAHeldPassWithATargetOutsideTheTableOverARefusalWaitsForTheGrant(t *testing.T) {
	cp := running(t)
	pause(t, cp)
	refuseScheduler(t, cp.Namespace)
	unheld := operatorAt(unpaused(), revision)
	if _, err := unheld.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !apistatus.Refused(err) {
		t.Fatalf("a pass whose Deployment is refused returned %v", err)
	}
	generation := get(t, cp).Generation
	if err := expectAt(cp, generation, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, "the API server refuses Deployment dev-kube-scheduler; the reason is in Lodia's log"); err != nil {
		t.Fatal(err)
	}

	setTargetAt(t, cp, "v1.34.1", previous)
	passOf(t, operatorAt(unpaused(), next), cp)
	if err := expectAt(cp, generation, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, waitingForTheGrant(next, previous)); err != nil {
		t.Error(err)
	}
}

func TestWithoutAGrantedRevisionWorkloadsWaitsForTheTarget(t *testing.T) {
	cp := controlPlane(t, loadBalancer())
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	setTargetAt(t, cp, version, "")
	inputsReadyBesidesTheTarget(t, cp)
	testenv.Eventually(t, func() error {
		return expectAt(cp, 1, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, "waiting for Lifecycle to set status.target.operatorRevision")
	})
	if e := get(t, cp).Status.Endpoint; e == nil || e.APIServer.Host != lbAddress {
		t.Errorf("status.endpoint is %+v, want it published from the Service", e)
	}
	if n := deployments(t, cp); n != 0 {
		t.Errorf("%d Deployments before a revision is granted", n)
	}

	changeService(t, cp, func(svc *corev1.Service) { svc.Spec.Selector[lodiav1alpha1.ComponentLabel] = "edited" })
	passOf(t, operatorAt(k8s, revision), cp)
	if got := getService(t, cp).Spec.Selector[lodiav1alpha1.ComponentLabel]; got != "edited" {
		t.Errorf("a pass without a granted revision rewrote the Service a person edited: the selector says %q", got)
	}
}

func TestAHeldPassReadsTheStoredRoutes(t *testing.T) {
	cp := gatewayRunning(t)
	grant(t, cp, previous)
	settle(t, cp)
	for _, name := range routeNames(cp) {
		route := &gatewayv1.TLSRoute{ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: name}}
		byHand(t, route, func() { route.Spec.Rules[0].BackendRefs[0].Weight = new(int32(2)) })
	}
	accept(t, cp)
	konnectivity := tlsRoute(t, cp.Namespace, "dev-konnectivity")
	setRouteStatus(t, konnectivity, metav1.ConditionFalse, metav1.ConditionTrue, "NotAllowedByListeners", konnectivity.Generation)
	settle(t, cp)
	before := objectsOf(t, cp)

	var lists, gets atomic.Int32
	held := operatorAt(k8s, next)
	held.Reader = interceptor.NewClient(k8s, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*gatewayv1.TLSRouteList); ok {
				lists.Add(1)
			}
			return c.List(ctx, list, opts...)
		},
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*lodiav1alpha1.ControlPlane); !ok {
				gets.Add(1)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	passOf(t, held, cp)
	expectNoWrites(t, cp, before, "a held pass over routes whose render differs")
	if n := lists.Load(); n != 0 {
		t.Errorf("a held pass over routes the cache holds read the routes without the cache %d times, want none", n)
	}
	if n := gets.Load(); n != 0 {
		t.Errorf("a held pass over objects the cache holds read %d of them without the cache, want none", n)
	}
	want := "TLSRoute dev-konnectivity is not Accepted by Gateway " + cp.Namespace + "/shared: NotAllowedByListeners"
	if c := routesReady(t, cp); c.Reason != lodiav1alpha1.ReasonRouteNotAccepted || c.Message != want {
		t.Errorf("RoutesReady is %s %q, want %s %q", c.Reason, c.Message, lodiav1alpha1.ReasonRouteNotAccepted, want)
	}
	accept(t, cp)
	passOf(t, held, cp)
	if err := expectRoutes(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted); err != nil {
		t.Error(err)
	}
}

func TestAHeldPassDeletesTheRoutesOfAHostnameAnotherControlPlaneOwns(t *testing.T) {
	cp := gatewayRunning(t)
	pause(t, cp)
	grant(t, cp, previous)
	owner := lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: "first", UID: "4c7d2e19-6b3a-4f58-9e0c-1a2b3c4d5e6f", CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour))},
		Spec:       lodiav1alpha1.ControlPlaneSpec{Endpoint: cp.Spec.Endpoint},
	}
	claimed := interceptor.NewClient(unpaused(), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := c.List(ctx, list, opts...); err != nil {
				return err
			}
			if planes, ok := list.(*lodiav1alpha1.ControlPlaneList); ok {
				planes.Items = append(planes.Items, owner)
			}
			return nil
		},
	})
	passOf(t, operatorAt(claimed, next), cp)
	expectNoRoutes(t, cp)
	if err := expectRoutes(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonHostnameConflict); err != nil {
		t.Error(err)
	}
}

func TestAHeldPassDeletesThePodDisruptionBudgetsARemovedHighAvailabilityLeaves(t *testing.T) {
	cp := running(t, highAvailability)
	pause(t, cp)
	grant(t, cp, previous)
	for _, component := range componentNames {
		eventuallyBudget(t, cp, component)
	}
	update(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) { spec.HighAvailability = nil })

	passOf(t, operatorAt(unpaused(), next), cp)
	for _, component := range componentNames {
		if budgetOrNil(t, cp, component) != nil {
			t.Errorf("a held pass left the PodDisruptionBudget of %s", component)
		}
	}
}

func TestAStaleCacheWithAnOlderRevisionHoldsThePass(t *testing.T) {
	cp := running(t)
	grant(t, cp, next)
	settle(t, cp)
	before := objectsOf(t, cp)
	r := operatorAt(interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if cached, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				cached.Status.Target.OperatorRevision = revision
			}
			return nil
		},
	}), next)
	r.SignerImage = "registry.example/lodia-signer:next"
	passOf(t, r, cp)
	expectNoWrites(t, cp, before, "a pass on a cache that holds the previous revision")
	if err := expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady); err != nil {
		t.Error(err)
	}

	r.Client = k8s
	passOf(t, r, cp)
	if signer := deployment(t, cp, "kube-apiserver").Spec.Template.Spec.InitContainers[0].Image; signer != r.SignerImage {
		t.Errorf("the signer runs %s once the cache holds the grant, want %s", signer, r.SignerImage)
	}
	expectStamps(t, cp, next)
}

func TestAHeldCreateThatFailsMidPassIsFinishedByTheNextPass(t *testing.T) {
	cp := running(t)
	pause(t, cp)
	grant(t, cp, previous)
	named := metav1.ObjectMeta{Namespace: cp.Namespace, Name: "dev-kube-scheduler"}
	for _, obj := range []client.Object{&networkingv1.NetworkPolicy{ObjectMeta: named}, &appsv1.Deployment{ObjectMeta: named}} {
		if err := k8s.Delete(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	unavailable := apierrors.NewServiceUnavailable("injected")
	failing := interceptor.NewClient(unpaused(), interceptor.Funcs{
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			if kindOf(t, obj) == "Deployment" {
				return unavailable
			}
			return c.Apply(ctx, obj, opts...)
		},
	})
	if _, err := operatorAt(noControlPlaneWrite(t, failing), next).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("the pass returned %v, want the injected error", err)
	}
	created := objectsOf(t, cp)
	if _, ok := created["*v1.NetworkPolicy dev-kube-scheduler"]; !ok {
		t.Error("the NetworkPolicy, applied before the failure, is missing")
	}
	if _, ok := created["*v1.Deployment dev-kube-scheduler"]; ok {
		t.Fatal("the Deployment whose create failed exists")
	}

	passOf(t, operatorAt(unpaused(), next), cp)
	after := objectsOf(t, cp)
	if after["*v1.NetworkPolicy dev-kube-scheduler"] != created["*v1.NetworkPolicy dev-kube-scheduler"] {
		t.Error("the next pass wrote the NetworkPolicy the failed pass created")
	}
	if stamp := deployment(t, cp, "kube-scheduler").Annotations[lodiav1alpha1.OperatorRevisionAnnotation]; stamp != next {
		t.Errorf("the scheduler is stamped %q, want %s", stamp, next)
	}
}

func TestAHeldPassThatCannotReadWritesNothing(t *testing.T) {
	cp := running(t)
	pause(t, cp)
	grant(t, cp, previous)
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
		"the API server answers 503":   {apierrors.NewServiceUnavailable("injected"), apierrors.IsServiceUnavailable},
		"the pass's deadline is spent": {context.DeadlineExceeded, func(err error) bool { return errors.Is(err, context.DeadlineExceeded) }},
	} {
		t.Run(name, func(t *testing.T) {
			r := operatorAt(noControlPlaneWrite(t, blind), next)
			r.Reader = interceptor.NewClient(k8s, interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*appsv1.Deployment); ok {
						return c.err
					}
					return cl.Get(ctx, key, obj, opts...)
				},
			})
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !c.want(err) {
				t.Errorf("the pass returned %v", err)
			}
		})
	}
	expectNoWrites(t, cp, before, "a held pass that cannot read what the cache lacks")
}

func TestAHeldPassWritesNothingOverAPredecessorsService(t *testing.T) {
	ns := namespace(t)
	predecessor := types.UID("6a0f3c2d-1e4b-4d7a-9c85-2f1e0d3b4a69")
	leftover := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      lodiav1alpha1.APIServerServiceName("dev"),
			Namespace: ns,
			Labels:    map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "dev", UID: predecessor, Controller: new(true),
			}},
		},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "https", Port: 443}}},
	}
	if err := k8s.Create(t.Context(), leftover); err != nil {
		t.Fatal(err)
	}
	cp := create(t, ns, loadBalancer())
	setTargetAt(t, cp, version, previous)
	testenv.Eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere)
	})
	if svc := getService(t, cp); svc.ResourceVersion != leftover.ResourceVersion {
		t.Error("a held pass wrote the predecessor's Service")
	}
	if e := get(t, cp).Status.Endpoint; e != nil {
		t.Errorf("status.endpoint is %+v, from the predecessor's Service", e)
	}
}

func TestAStampThatDoesNotFitTheFieldIsNotReported(t *testing.T) {
	cp := running(t)
	expectRevisions(t, cp, revision, revision, revision)
	pause(t, cp)
	for _, row := range []struct{ stamp, want string }{{strings.Repeat("v", 129), revision}, {strings.Repeat("v", 128), strings.Repeat("v", 128)}} {
		edited := interceptor.NewClient(unpaused(), interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if d, ok := obj.(*appsv1.Deployment); ok && key.Name == "dev-kube-apiserver" {
					d.Annotations[lodiav1alpha1.OperatorRevisionAnnotation] = row.stamp
				}
				return nil
			},
		})
		reconcileThrough(t, cp, edited)
		expectRevisions(t, cp, row.want, revision, revision)
	}
}
