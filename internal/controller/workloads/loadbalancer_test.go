// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/testenv"
)

const (
	of     = "the load balancer of Service dev-kube-apiserver"
	other  = "198.51.100.7"
	repair = "the annotation of endpoint.loadBalancer.annotations that asks the load balancer for it is missing or was ignored"
)

func atLBAddress(spec *lodiav1alpha1.ControlPlaneSpec) {
	spec.Endpoint.LoadBalancer.Host = lbAddress
	spec.Endpoint.LoadBalancer.Annotations = map[string]string{"example.com/ip": other}
}

func TestAnAddressMismatchBuildsTheTenantMovesNothingAndIsRepairedInPlace(t *testing.T) {
	cp := controlPlane(t, loadBalancer(), atLBAddress)
	want := lodiav1alpha1.EndpointStatus{
		APIServer:    lodiav1alpha1.HostPort{Host: lbAddress, Port: 443},
		Konnectivity: lodiav1alpha1.HostPort{Host: lbAddress, Port: 8132},
		InCluster:    lodiav1alpha1.HostPort{Host: lbAddress, Port: 443},
	}
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: other})
	testenv.Eventually(t, func() error {
		return expectBalancer(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAddressMismatch, of+" reports "+other+", not endpoint.loadBalancer.host "+lbAddress+": "+repair)
	})
	inputsReady(t, cp)
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	testenv.Eventually(t, func() error { return expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady) })
	if e := get(t, cp).Status.Endpoint; e == nil || *e != want {
		t.Errorf("status.endpoint is %+v while the load balancer is elsewhere, want %+v", e, want)
	}
	expectFlags(t, deployment(t, cp, "kube-apiserver").Spec.Template.Spec.Containers[0].Command,
		"--advertise-address="+lbAddress, "--service-account-issuer=https://"+lbAddress)

	update(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.Endpoint.LoadBalancer.Annotations = map[string]string{"example.com/ip": lbAddress}
		spec.Endpoint.LoadBalancer.SourceRanges = []lodiav1alpha1.CIDR{"198.51.100.0/24"}
	})
	testenv.Eventually(t, func() error {
		if svc := getService(t, cp); svc.Annotations["example.com/ip"] != lbAddress || !slices.Equal(svc.Spec.LoadBalancerSourceRanges, []string{"198.51.100.0/24"}) {
			return fmt.Errorf("the Service has the annotations %v and the source ranges %v", svc.Annotations, svc.Spec.LoadBalancerSourceRanges)
		}
		return expectBalancer(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAddressMismatch, of+" reports "+other+", not endpoint.loadBalancer.host "+lbAddress+": "+repair)
	})
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	testenv.Eventually(t, func() error {
		return expectBalancer(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, of+" reports endpoint.loadBalancer.host "+lbAddress)
	})
	setIngress(t, cp, corev1.LoadBalancerIngress{Hostname: "a.elb.example.com"})
	testenv.Eventually(t, func() error {
		return expectBalancer(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonUnverified,
			of+" reports the host names a.elb.example.com and no IP address, so endpoint.loadBalancer.host "+lbAddress+" cannot be compared with them")
	})
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: other})
	testenv.Eventually(t, func() error {
		list := &eventsv1.EventList{}
		if err := k8s.List(t.Context(), list, client.InNamespace(cp.Namespace)); err != nil {
			return err
		}
		if !slices.ContainsFunc(list.Items, func(e eventsv1.Event) bool {
			return e.ReportingController == "lodia.io/workloads" && e.Reason == lodiav1alpha1.ReasonAddressMismatch && e.Type == corev1.EventTypeWarning && e.Action == lodiav1alpha1.ConditionLoadBalancerReady
		}) {
			return fmt.Errorf("no Warning event of lodia.io/workloads says AddressMismatch: %d events", len(list.Items))
		}
		return nil
	})
	if e := get(t, cp).Status.Endpoint; e == nil || *e != want {
		t.Errorf("status.endpoint is %+v after the load balancer moved, want %+v", e, want)
	}
	if err := expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady); err != nil {
		t.Error(err)
	}
}

func TestARefusedServiceAtAHostKeepsTheSpecsEndpointFromTheStart(t *testing.T) {
	for host, inCluster := range map[lodiav1alpha1.EndpointHost]lodiav1alpha1.HostPort{
		lbAddress:             {Host: lbAddress, Port: 443},
		"api.dev.example.com": {Host: "240.0.0.1", Port: 6443},
	} {
		t.Run(string(host), func(t *testing.T) {
			ns := namespace(t)
			refuseLoadBalancers(t, ns)
			cp := create(t, ns, loadBalancerAt(host))
			want := lodiav1alpha1.EndpointStatus{
				APIServer:    lodiav1alpha1.HostPort{Host: string(host), Port: 443},
				Konnectivity: lodiav1alpha1.HostPort{Host: string(host), Port: 8132},
				InCluster:    inCluster,
			}
			testenv.Eventually(t, func() error {
				if err := expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused); err != nil {
					return err
				}
				fresh := get(t, cp)
				if b := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady); b == nil || b.Reason != lodiav1alpha1.ReasonRefused {
					return fmt.Errorf("LoadBalancerReady is %+v", b)
				}
				if e := fresh.Status.Endpoint; e == nil || *e != want {
					return fmt.Errorf("status.endpoint is %+v while the Service is refused, want the spec's %+v", e, want)
				}
				return nil
			})
			if _, err := operatorAt(k8s, revision).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !apistatus.Refused(err) {
				t.Errorf("a second pass whose Service is refused returned %v, want the refusal", err)
			}
			if e := get(t, cp).Status.Endpoint; e == nil || *e != want {
				t.Errorf("status.endpoint is %+v after a second refused pass, want the spec's %+v", e, want)
			}
		})
	}
}

func TestADiscoveredAddressStaysThroughWhatTheLoadBalancerReportsUntilAnotherReplacesIt(t *testing.T) {
	cp := running(t)
	discovered := func(host string) lodiav1alpha1.EndpointStatus {
		return lodiav1alpha1.EndpointStatus{
			APIServer:    lodiav1alpha1.HostPort{Host: host, Port: 443},
			Konnectivity: lodiav1alpha1.HostPort{Host: host, Port: 8132},
			InCluster:    lodiav1alpha1.HostPort{Host: "240.0.0.1", Port: 6443},
		}
	}
	for _, step := range []struct {
		name    string
		ingress []corev1.LoadBalancerIngress
		want    string
		ready   metav1.ConditionStatus
	}{
		{"an entry added before it", []corev1.LoadBalancerIngress{{IP: other}, {IP: lbAddress}}, lbAddress, metav1.ConditionTrue},
		{"the entries reordered", []corev1.LoadBalancerIngress{{IP: lbAddress}, {IP: other}}, lbAddress, metav1.ConditionTrue},
		{"behind a host name", []corev1.LoadBalancerIngress{{Hostname: "lb.example.com"}, {IP: lbAddress}}, lbAddress, metav1.ConditionTrue},
		{"an ingress that empties", nil, lbAddress, metav1.ConditionFalse},
		{"an ingress that comes back", []corev1.LoadBalancerIngress{{IP: lbAddress}}, lbAddress, metav1.ConditionTrue},
		{"another address", []corev1.LoadBalancerIngress{{Hostname: "lb.example.com", IP: other}}, "lb.example.com", metav1.ConditionTrue},
		{"its IP replaced beside the name", []corev1.LoadBalancerIngress{{Hostname: "lb.example.com", IP: "198.51.100.8"}}, "lb.example.com", metav1.ConditionTrue},
	} {
		setIngresses(t, cp, step.ingress...)
		testenv.Eventually(t, func() error {
			fresh := get(t, cp)
			if c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady); c == nil || c.Status != step.ready {
				return fmt.Errorf("%s: LoadBalancerReady is %+v, want %s", step.name, c, step.ready)
			}
			if e := fresh.Status.Endpoint; e == nil || *e != discovered(step.want) {
				return fmt.Errorf("%s: status.endpoint is %+v, want %+v", step.name, e, discovered(step.want))
			}
			return nil
		})
	}
}

func TestAKeptDiscoveredAddressIsRebuiltInTheSpecsShapeOnEveryPass(t *testing.T) {
	cp := running(t)
	want := lodiav1alpha1.EndpointStatus{
		APIServer:    lodiav1alpha1.HostPort{Host: lbAddress, Port: 443},
		Konnectivity: lodiav1alpha1.HostPort{Host: lbAddress, Port: 8132},
		InCluster:    lodiav1alpha1.HostPort{Host: "240.0.0.1", Port: 6443},
	}
	if e := get(t, cp).Status.Endpoint; e == nil || *e != want {
		t.Fatalf("status.endpoint is %+v, want %+v", e, want)
	}
	pause(t, cp)
	applyStatus(t, cp, "test/endpoint", map[string]any{"endpoint": map[string]any{
		"apiServer":    map[string]any{"host": lbAddress, "port": int64(443)},
		"konnectivity": map[string]any{"host": lbAddress, "port": int64(443)},
		"inCluster":    map[string]any{"host": lbAddress, "port": int64(6443)},
	}})
	for exit, funcs := range map[string]interceptor.Funcs{
		"a pass": {},
		"held": {Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if svc, ok := obj.(*corev1.Service); ok {
				svc.OwnerReferences = []metav1.OwnerReference{foreignController()}
			}
			return nil
		}},
		"refused": {Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			if kindOf(t, obj) == "Service" {
				return apierrors.NewForbidden(corev1.Resource("services"), lodiav1alpha1.APIServerServiceName(cp.Name), errors.New("denied by a policy"))
			}
			return c.Apply(ctx, obj, opts...)
		}},
	} {
		t.Run(exit, func(t *testing.T) {
			var applied *lodiav1alpha1.ControlPlane
			r := operatorAt(applying(t, interceptor.NewClient(unpaused(), funcs), &applied), revision)
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil && !apistatus.Refused(err) {
				t.Fatal(err)
			}
			if applied == nil {
				t.Fatal("the pass applied no status")
			}
			if e := applied.Status.Endpoint; e == nil || *e != want {
				t.Errorf("over a stored endpoint of another shape at the host the load balancer reports, the pass applied %+v, want %+v", e, want)
			}
		})
	}
}

func TestARestartBetweenTheServiceAndTheStatusLeavesThePassToTheNext(t *testing.T) {
	cp := controlPlane(t, loadBalancer(), atLBAddress)
	setTarget(t, cp)
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: other})
	testenv.Eventually(t, func() error {
		return expectBalancer(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAddressMismatch, of+" reports "+other+", not endpoint.loadBalancer.host "+lbAddress+": "+repair)
	})
	pause(t, cp)
	changeSpec(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.Endpoint.LoadBalancer.Annotations = map[string]string{"example.com/ip": lbAddress}
	})
	before := get(t, cp)
	stopped := interceptor.NewClient(unpaused(), interceptor.Funcs{
		SubResourceApply: func(context.Context, client.Client, string, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
			return errors.New("the operator stopped before its status apply")
		},
	})
	if _, err := operatorAt(stopped, revision).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Fatal("the pass wrote its status")
	}
	if svc := getService(t, cp); svc.Annotations["example.com/ip"] != lbAddress {
		t.Fatalf("the Service has the annotations %v", svc.Annotations)
	}
	if after := get(t, cp); after.ResourceVersion != before.ResourceVersion {
		t.Fatalf("the ControlPlane went from resourceVersion %s to %s", before.ResourceVersion, after.ResourceVersion)
	}
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	passOf(t, operatorAt(unpaused(), revision), cp)
	if err := expectBalancer(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, of+" reports endpoint.loadBalancer.host "+lbAddress); err != nil {
		t.Error(err)
	}
}

func TestAHeldPassKeepsARefusalOfAServiceItDidNotApply(t *testing.T) {
	cp := running(t, atLBAddress)
	testenv.Eventually(t, func() error {
		return expectBalancer(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, of+" reports endpoint.loadBalancer.host "+lbAddress)
	})
	pause(t, cp)
	changeSpec(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.Endpoint.LoadBalancer.SourceRanges = []lodiav1alpha1.CIDR{"198.51.100.0/24"}
	})
	refusing := interceptor.NewClient(unpaused(), interceptor.Funcs{Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
		if kindOf(t, obj) == "Service" {
			return apierrors.NewForbidden(corev1.Resource("services"), lodiav1alpha1.APIServerServiceName(cp.Name), errors.New("denied by a policy"))
		}
		return c.Apply(ctx, obj, opts...)
	}})
	if _, err := operatorAt(refusing, revision).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !apistatus.Refused(err) {
		t.Fatalf("the refused pass returned %v", err)
	}
	refused := meta.FindStatusCondition(get(t, cp).Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady)
	if refused.Reason != lodiav1alpha1.ReasonRefused {
		t.Fatalf("after the refused pass LoadBalancerReady is %+v", refused)
	}
	passOf(t, operatorAt(unpaused(), next), cp)
	if svc := getService(t, cp); len(svc.Spec.LoadBalancerSourceRanges) != 0 {
		t.Fatalf("a held pass applied %v", svc.Spec.LoadBalancerSourceRanges)
	}
	held := meta.FindStatusCondition(get(t, cp).Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady)
	if held.Status != refused.Status || held.Reason != refused.Reason || held.Message != refused.Message || held.ObservedGeneration != refused.ObservedGeneration {
		t.Errorf("a held pass that left the Service as stored wrote LoadBalancerReady %+v over the refusal %+v", held, refused)
	}
}

func TestAHeldPassAfterTheServiceIsReleasedReportsItsLoadBalancer(t *testing.T) {
	cp := running(t, atLBAddress)
	testenv.Eventually(t, func() error {
		return expectBalancer(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, of+" reports endpoint.loadBalancer.host "+lbAddress)
	})
	pause(t, cp)
	setOwners := func(refs []metav1.OwnerReference) {
		testenv.Eventually(t, func() error {
			svc := getService(t, cp)
			svc.OwnerReferences = refs
			return k8s.Update(t.Context(), svc)
		})
	}
	setOwners([]metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "someone", UID: types.UID("0f1e2d3c-4b5a-4968-8776-5a4b3c2d1e0f"), Controller: new(true)}})
	passOf(t, operatorAt(unpaused(), revision), cp)
	elsewhere := meta.FindStatusCondition(get(t, cp).Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady)
	if elsewhere == nil || elsewhere.Reason != lodiav1alpha1.ReasonObjectsControlledElsewhere {
		t.Fatalf("LoadBalancerReady is %+v, want ObjectsControlledElsewhere", elsewhere)
	}
	setOwners(nil)
	changeSpec(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.Endpoint.LoadBalancer.SourceRanges = []lodiav1alpha1.CIDR{"198.51.100.0/24"}
	})
	passOf(t, operatorAt(unpaused(), next), cp)
	if svc := getService(t, cp); len(svc.OwnerReferences) != 0 {
		t.Fatalf("the held pass wrote the Service: owners %v", svc.OwnerReferences)
	}
	held := meta.FindStatusCondition(get(t, cp).Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady)
	if held.Status != metav1.ConditionTrue || held.Reason != lodiav1alpha1.ReasonReady || held.ObservedGeneration != elsewhere.ObservedGeneration {
		t.Errorf("a held pass after the Service was released wrote LoadBalancerReady %+v, want the stored Service's Ready at generation %d", held, elsewhere.ObservedGeneration)
	}
}

func TestAHeldPassLeavesLoadBalancerReadyAtTheGenerationItDescribes(t *testing.T) {
	cp := running(t, atLBAddress)
	testenv.Eventually(t, func() error {
		return expectBalancer(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, of+" reports endpoint.loadBalancer.host "+lbAddress)
	})
	described := meta.FindStatusCondition(get(t, cp).Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady).ObservedGeneration
	pause(t, cp)
	changeSpec(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.Endpoint.LoadBalancer.SourceRanges = []lodiav1alpha1.CIDR{"198.51.100.0/24"}
	})
	lagging := unpaused(func(_ client.ObjectKey, obj client.Object) {
		if cached, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
			meta.RemoveStatusCondition(&cached.Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady)
		}
	})
	passOf(t, operatorAt(lagging, next), cp)
	if svc := getService(t, cp); len(svc.Spec.LoadBalancerSourceRanges) != 0 {
		t.Fatalf("a held pass applied the source ranges %v", svc.Spec.LoadBalancerSourceRanges)
	}
	fresh := get(t, cp)
	if b := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady); b == nil || b.Reason != lodiav1alpha1.ReasonReady || b.ObservedGeneration != described {
		t.Errorf("a held pass at generation %d that left the Service as stored wrote LoadBalancerReady %+v, want Ready at generation %d", fresh.Generation, b, described)
	}

	if err := k8s.Delete(t.Context(), getService(t, cp)); err != nil {
		t.Fatal(err)
	}
	passOf(t, operatorAt(lagging, next), cp)
	if svc := getService(t, cp); !slices.Equal(svc.Spec.LoadBalancerSourceRanges, []string{"198.51.100.0/24"}) {
		t.Fatalf("a held pass wrote the missing Service with the source ranges %v", svc.Spec.LoadBalancerSourceRanges)
	}
	if err := expectBalancer(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForLoadBalancer, of+" reports no address yet"); err != nil {
		t.Errorf("a held pass that wrote the missing Service: %v", err)
	}
}

func TestLoadBalancerReadyOnTheEdgesOfAPass(t *testing.T) {
	cp := running(t, atLBAddress)
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	testenv.Eventually(t, func() error {
		return expectBalancer(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, of+" reports endpoint.loadBalancer.host "+lbAddress)
	})
	pause(t, cp)
	published := get(t, cp).Status.Endpoint
	serviceGet := func(err error) client.WithWatch {
		return interceptor.NewClient(unpaused(), interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Service); ok {
					return err
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
	}
	for name, c := range map[string]struct {
		cache, reader error
	}{
		"a transient error": {apierrors.NewServiceUnavailable("injected"), nil},
		"a deadline":        {context.DeadlineExceeded, nil},
		"outside the cache": {apierrors.NewNotFound(corev1.Resource("services"), "dev-kube-apiserver"), nil},
		"unreadable outside the cache": {apierrors.NewNotFound(corev1.Resource("services"), "dev-kube-apiserver"),
			apierrors.NewServiceUnavailable("injected")},
	} {
		t.Run(name, func(t *testing.T) {
			var applied *lodiav1alpha1.ControlPlane
			r := operatorAt(applying(t, serviceGet(c.cache), &applied), revision)
			if c.reader != nil {
				r.Reader = serviceGet(c.reader)
			}
			_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
			if apierrors.IsNotFound(c.cache) && c.reader == nil {
				if err != nil || applied == nil {
					t.Fatalf("a pass that reads the Service from the API server returned %v and applied %+v", err, applied)
				}
				b := meta.FindStatusCondition(applied.Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady)
				if b == nil || b.Reason != lodiav1alpha1.ReasonReady || !equality.Semantic.DeepEqual(applied.Status.Endpoint, published) {
					t.Errorf("a pass that reads the Service from the API server applied LoadBalancerReady %+v and the endpoint %+v", b, applied.Status.Endpoint)
				}
				return
			}
			if err == nil || applied != nil {
				t.Errorf("a pass that cannot read the Service returned %v and applied %+v", err, applied)
			}
		})
	}
}

func TestAPredecessorsServiceHoldsTheLoadBalancerButNotTheEndpointAtAHost(t *testing.T) {
	ns := namespace(t)
	predecessor := types.UID("4c2b1a0f-9e8d-4c7b-a6f5-e4d3c2b1a098")
	leftover := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: lodiav1alpha1.APIServerServiceName("dev"), Namespace: ns, Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "dev", UID: predecessor, Controller: new(true)}},
		},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Ports: []corev1.ServicePort{{Name: "https", Port: 443}}},
	}
	if err := k8s.Create(t.Context(), leftover); err != nil {
		t.Fatal(err)
	}
	leftover.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: lbAddress, IPMode: new(corev1.LoadBalancerIPModeVIP)}}
	if err := k8s.Status().Update(t.Context(), leftover); err != nil {
		t.Fatal(err)
	}
	cp := create(t, ns, loadBalancerAt(lbAddress))
	testenv.Eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere)
	})
	fresh := get(t, cp)
	held := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
	if b := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionLoadBalancerReady); b == nil || b.Status != metav1.ConditionFalse ||
		b.Reason != lodiav1alpha1.ReasonObjectsControlledElsewhere || b.Message != held.Message {
		t.Errorf("LoadBalancerReady is %+v, want the hold of APIServerReady %q", b, held.Message)
	}
	if e := fresh.Status.Endpoint; e == nil || e.APIServer.Host != lbAddress || e.InCluster != (lodiav1alpha1.HostPort{Host: lbAddress, Port: 443}) {
		t.Errorf("status.endpoint is %+v, want the spec's", e)
	}
	if result := reconcileNow(t, cp); result.RequeueAfter != 10*time.Second {
		t.Errorf("requeue after %s, want 10s while the garbage collector deletes a predecessor's Service", result.RequeueAfter)
	}
	if svc := getService(t, cp); svc.ResourceVersion != leftover.ResourceVersion {
		t.Error("the predecessor's Service was written")
	}
}

func TestANodePortEndpointIsOfTheFamilyOfTheFirstServiceRange(t *testing.T) {
	cp := controlPlane(t, lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.Network = lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"fd00:10:96::/108"}, PodCIDRs: []lodiav1alpha1.CIDR{"fd00:10:244::/56"}}
	})
	testenv.Eventually(t, func() error {
		if err := expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint); err != nil {
			return err
		}
		if c := condition(t, cp); c.Message != "no Ready node of the management cluster has an IPv6 InternalIP, the family of network.serviceCIDRs[0]" {
			return fmt.Errorf("APIServerReady says %q", c.Message)
		}
		return nil
	})
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "zz-dual-stack"}}
	if err := k8s.Create(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.IgnoreNotFound(k8s.Delete(context.Background(), node)); err != nil {
			t.Error(err)
		}
	})
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	node.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.200"}, {Type: corev1.NodeInternalIP, Address: "2001:db8::200"}}
	if err := k8s.Status().Update(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	reconcileNow(t, cp)
	svc := getService(t, cp)
	want := lodiav1alpha1.EndpointStatus{
		APIServer:    lodiav1alpha1.HostPort{Host: "2001:db8::200", Port: nodePort(svc, 443)},
		Konnectivity: lodiav1alpha1.HostPort{Host: "2001:db8::200", Port: nodePort(svc, 8132)},
		InCluster:    lodiav1alpha1.HostPort{Host: "2001:db8::200", Port: nodePort(svc, 443)},
	}
	if e := get(t, cp).Status.Endpoint; e == nil || *e != want {
		t.Errorf("status.endpoint is %+v, want %+v", e, want)
	}
}
