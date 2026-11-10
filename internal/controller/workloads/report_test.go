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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/workloads"
	"go.lodia.io/lodia/internal/testenv"
)

var componentNames = []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"}

func highlyAvailable(t *testing.T) *lodiav1alpha1.ControlPlane {
	t.Helper()
	cp := controlPlane(t, loadBalancer(), func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"}
	})
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReady(t, cp)
	return cp
}

func expectVersions(t *testing.T, cp *lodiav1alpha1.ControlPlane, want string) {
	t.Helper()
	testenv.Eventually(t, func() error {
		if got := reported(get(t, cp).Status.Components); got != want {
			return fmt.Errorf("status.components reports the versions %s, want %s", got, want)
		}
		return nil
	})
}

func reported(c *lodiav1alpha1.ComponentsStatus) string {
	if c == nil {
		return "absent"
	}
	var got []string
	for _, s := range []*lodiav1alpha1.ComponentStatus{c.APIServer, c.ControllerManager, c.Scheduler} {
		switch {
		case s == nil:
			got = append(got, "absent")
		case s.Version == "":
			got = append(got, "none")
		default:
			got = append(got, s.Version)
		}
	}
	return strings.Join(got, " ")
}

func setTargetVersion(t *testing.T, cp *lodiav1alpha1.ControlPlane, to string) {
	t.Helper()
	setTargetAt(t, cp, to, revision)
}

func eventuallyImages(t *testing.T, cp *lodiav1alpha1.ControlPlane, tag string, components ...string) {
	t.Helper()
	if len(components) == 0 {
		components = componentNames
	}
	testenv.Eventually(t, func() error { return imagesAt(t, cp, tag, components) })
}

func expectImagesAfterAPass(t *testing.T, cp *lodiav1alpha1.ControlPlane, tag string, components ...string) {
	t.Helper()
	reconcileNow(t, cp)
	if err := imagesAt(t, cp, tag, components); err != nil {
		t.Error(err)
	}
}

func imagesAt(t *testing.T, cp *lodiav1alpha1.ControlPlane, tag string, components []string) error {
	t.Helper()
	for _, component := range components {
		for _, c := range deployment(t, cp, component).Spec.Template.Spec.Containers {
			if c.Name == component && !strings.HasSuffix(c.Image, ":"+tag) {
				return fmt.Errorf("%s runs %s, want the tag %s", component, c.Image, tag)
			}
		}
	}
	return nil
}

func pause(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	update(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) { spec.Paused = true })
	generation := get(t, cp).Generation
	testenv.Eventually(t, func() error {
		if read := managerReads.pausedAt(client.ObjectKeyFromObject(cp)); read < generation {
			return fmt.Errorf("the suite's manager has read the ControlPlane paused at generation %d, want %d", read, generation)
		}
		return nil
	})
}

func appliedStatus(t *testing.T, cp *lodiav1alpha1.ControlPlane, c client.WithWatch) lodiav1alpha1.ControlPlaneStatus {
	t.Helper()
	var applied *lodiav1alpha1.ControlPlane
	reconcileThrough(t, cp, interceptor.NewClient(c, interceptor.Funcs{
		SubResourceApply: func(_ context.Context, _ client.Client, _ string, obj runtime.ApplyConfiguration, _ ...client.SubResourceApplyOption) error {
			applied = decoded(t, obj)
			return nil
		},
	}))
	if applied == nil {
		t.Fatal("the pass applied no status")
	}
	return applied.Status
}

func withoutDeployments(t *testing.T, c client.WithWatch, names ...string) client.WithWatch {
	t.Helper()
	return interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok && (len(names) == 0 || slices.Contains(names, key.Name)) {
				return apierrors.NewNotFound(schema.GroupResource{Group: appsv1.GroupName, Resource: "deployments"}, key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
}

func TestAComponentReportsTheVersionOfItsImageOnceEveryReplicaRunsIt(t *testing.T) {
	cp := highlyAvailable(t)
	for _, component := range componentNames {
		eventuallyDeployment(t, cp, component)
	}
	expectVersions(t, cp, "none none none")

	for _, component := range componentNames {
		markAvailable(t, deployment(t, cp, component))
	}
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
	expectComponents(t, cp, "2/2/2 2/2/2 2/2/2")

	setTargetVersion(t, cp, "v1.37.2")
	eventuallyImages(t, cp, "v1.37.2", "kube-apiserver")
	expectImagesAfterAPass(t, cp, "v1.37.1", "kube-controller-manager", "kube-scheduler")
	expectComponents(t, cp, "2/2/absent 2/2/2 2/2/2")
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")

	setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) {
		*s = appsv1.DeploymentStatus{ObservedGeneration: deployment(t, cp, "kube-apiserver").Generation, Replicas: 2, UpdatedReplicas: 1, ReadyReplicas: 2, AvailableReplicas: 2}
	})
	expectComponents(t, cp, "2/2/1 2/2/2 2/2/2")
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
	expectImagesAfterAPass(t, cp, "v1.37.1", "kube-controller-manager", "kube-scheduler")

	markAvailable(t, deployment(t, cp, "kube-apiserver"))
	expectVersions(t, cp, "v1.37.2 v1.37.1 v1.37.1")
	eventuallyImages(t, cp, "v1.37.2", "kube-controller-manager", "kube-scheduler")
	markAvailable(t, deployment(t, cp, "kube-controller-manager"))
	markAvailable(t, deployment(t, cp, "kube-scheduler"))
	expectVersions(t, cp, "v1.37.2 v1.37.2 v1.37.2")
}

func TestAKonnectivityPinChangesNoVersion(t *testing.T) {
	cp := running(t, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.Addons = &lodiav1alpha1.Addons{Konnectivity: &lodiav1alpha1.Konnectivity{Version: "v0.37.2"}}
	})
	pod := deployment(t, cp, "kube-apiserver").Spec.Template.Spec
	if image := pod.InitContainers[1].Image; !strings.HasSuffix(image, ":v0.37.2") {
		t.Fatalf("konnectivity-server runs %s, want the pinned v0.37.2", image)
	}
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
}

func TestAControllerManagerHeldAtZeroReportsItsTemplate(t *testing.T) {
	cp := restored(t, namespace(t), lodiav1alpha1.RestoreHeld)
	expectComponents(t, cp, "1/1/1 0/0/0 1/1/1")
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
}

func TestAnUnobservedTemplateOfAHeldControllerManagerReportsNothing(t *testing.T) {
	cp := restored(t, namespace(t), lodiav1alpha1.RestoreHeld)
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
	setTargetVersion(t, cp, "v1.37.2")
	eventuallyImages(t, cp, "v1.37.2", "kube-apiserver")
	markAvailable(t, deployment(t, cp, "kube-apiserver"))
	expectVersions(t, cp, "v1.37.2 v1.37.1 v1.37.1")
	eventuallyImages(t, cp, "v1.37.2", "kube-controller-manager", "kube-scheduler")
	expectComponents(t, cp, "1/1/1 0/0/absent 1/1/absent")
	expectVersions(t, cp, "v1.37.2 v1.37.1 v1.37.1")
	markAvailable(t, deployment(t, cp, "kube-controller-manager"))
	expectVersions(t, cp, "v1.37.2 v1.37.2 v1.37.1")
}

func TestAGoneDeploymentKeepsItsReportOnBothPaths(t *testing.T) {
	cp := running(t)
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
	pause(t, cp)
	c := unpaused()

	status := appliedStatus(t, cp, withoutDeployments(t, c, "dev-kube-scheduler"))
	if got := reported(status.Components); got != "v1.37.1 v1.37.1 v1.37.1" {
		t.Fatalf("a pass that finds the scheduler's Deployment gone reports %s, want every version kept", got)
	}
	if s := status.Components.Scheduler; s.Total != 0 || s.Ready != 0 || s.Updated != nil {
		t.Errorf("the scheduler without a Deployment counts %+v, want no replicas and no updated count", s.RolloutCount)
	}
	if s := status.Components.APIServer; s.Total != 1 || s.Ready != 1 || s.Updated == nil || *s.Updated != 1 {
		t.Errorf("the API server, whose Deployment is there, counts %+v, want 1 of 1 updated", s.RolloutCount)
	}

	status = appliedStatus(t, cp, withoutDeployments(t, c))
	if got := reported(status.Components); got != "v1.37.1 v1.37.1 v1.37.1" {
		t.Fatalf("a pass that controls no Deployment reports %s, want every version kept", got)
	}
	for _, s := range []*lodiav1alpha1.ComponentStatus{status.Components.APIServer, status.Components.ControllerManager, status.Components.Scheduler} {
		if s.Total != 0 || s.Ready != 0 || s.Updated != nil {
			t.Errorf("a component without a Deployment counts %+v, want no replicas and no updated count", s.RolloutCount)
		}
	}
}

func TestAComponentThatNeverReportedAndHasNoDeploymentStaysAbsent(t *testing.T) {
	cp := highlyAvailable(t)
	for _, component := range componentNames {
		eventuallyDeployment(t, cp, component)
	}
	expectVersions(t, cp, "none none none")
	pause(t, cp)
	status := appliedStatus(t, cp, withoutDeployments(t, unpaused()))
	if status.Components != nil {
		t.Errorf("a pass that controls no Deployment of a ControlPlane that reported no version applies the components %+v, want none", status.Components)
	}
}

func TestADeploymentAnotherControlPlaneControlsReportsNothing(t *testing.T) {
	cp := running(t)
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
	pause(t, cp)
	foreign := interceptor.NewClient(unpaused(), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if d, ok := obj.(*appsv1.Deployment); ok && key.Name == "dev-kube-scheduler" {
				d.OwnerReferences = []metav1.OwnerReference{foreignController()}
				d.Spec.Template.Spec.Containers[0].Image = "registry.k8s.io/kube-scheduler:v1.99.9"
			}
			return nil
		},
	})
	status := appliedStatus(t, cp, foreign)
	if got := reported(status.Components); got != "v1.37.1 v1.37.1 v1.37.1" {
		t.Errorf("a pass over a scheduler another ControlPlane controls reports %s, want the previous versions", got)
	}
}

func TestAHandEditedImageDoesNotRefuseTheStatus(t *testing.T) {
	cp := running(t)
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
	pause(t, cp)
	for _, tag := range []string{"v1.37.1234567890123456", "v1.37.1-rc.1", "latest"} {
		edited := interceptor.NewClient(unpaused(), interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if d, ok := obj.(*appsv1.Deployment); ok && key.Name == "dev-kube-apiserver" {
					d.Spec.Template.Spec.Containers[0].Image = "registry.k8s.io/kube-apiserver:" + tag
				}
				return nil
			},
		})
		reconcileThrough(t, cp, edited)
		expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
	}
}

func TestThePreviousReportIsReadFromTheAPIServerNotTheCache(t *testing.T) {
	cp := highlyAvailable(t)
	for _, component := range componentNames {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
	pause(t, cp)
	cache := get(t, cp)

	applyStatus(t, cp, workloads.FieldManager, map[string]any{"components": map[string]any{
		"apiServer":         map[string]any{"total": int64(2), "ready": int64(2), "updated": int64(2), "version": "v1.37.2"},
		"controllerManager": map[string]any{"total": int64(2), "ready": int64(2), "updated": int64(2), "version": "v1.37.1"},
		"scheduler":         map[string]any{"total": int64(2), "ready": int64(2), "updated": int64(2), "version": "v1.37.1"},
	}})
	setDeploymentStatus(t, cp, "kube-apiserver", func(s *appsv1.DeploymentStatus) {
		*s = appsv1.DeploymentStatus{ObservedGeneration: deployment(t, cp, "kube-apiserver").Generation, Replicas: 2, UpdatedReplicas: 1, ReadyReplicas: 2, AvailableReplicas: 2}
	})
	if got := reported(get(t, cp).Status.Components); got != "v1.37.2 v1.37.1 v1.37.1" {
		t.Fatalf("the API server holds the versions %s", got)
	}
	if got := reported(cache.Status.Components); got != "v1.37.1 v1.37.1 v1.37.1" {
		t.Fatalf("the cache holds the versions %s", got)
	}

	stale := interceptor.NewClient(unpaused(), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				cache.DeepCopyInto(cp)
				cp.Spec.Paused = false
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	status := appliedStatus(t, cp, stale)
	if got := reported(status.Components); got != "v1.37.2 v1.37.1 v1.37.1" {
		t.Errorf("a pass on a cache older than the API server applies the versions %s, want v1.37.2 v1.37.1 v1.37.1", got)
	}
	if a := status.Components.APIServer; a.Updated == nil || *a.Updated != 1 {
		t.Errorf("the API server counts %+v, want 1 of 2 updated: the roll is under way", a.RolloutCount)
	}
}

func TestAGatewayPassReadsTheControlPlaneLiveOnce(t *testing.T) {
	cp := gatewayRunning(t)
	var live atomic.Int32
	counting := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				live.Add(1)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	r := &workloads.Reconciler{Client: k8s, Reader: counting, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true, Revision: revision}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if n := live.Load(); n != 1 {
		t.Errorf("the pass read the ControlPlane live %d times, want once for the endpoint, RoutesReady and the components", n)
	}
}

func TestAPassThatCannotReadTheControlPlaneLiveWritesNothing(t *testing.T) {
	cp := running(t)
	cache := get(t, cp)
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	for name, c := range map[string]struct {
		ctx    context.Context
		reader client.Reader
		want   func(error) bool
	}{
		"the API server answers 503": {t.Context(), interceptor.NewClient(k8s, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return apierrors.NewServiceUnavailable("injected")
			},
		}), apierrors.IsServiceUnavailable},
		"the pass's deadline is spent": {expired, k8s, func(err error) bool { return errors.Is(err, context.DeadlineExceeded) }},
	} {
		t.Run(name, func(t *testing.T) {
			cached := interceptor.NewClient(noControlPlaneWrite(t, k8s), interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
						cache.DeepCopyInto(cp)
						return nil
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			r := &workloads.Reconciler{Client: cached, Reader: c.reader, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true, Revision: revision}
			if _, err := r.Reconcile(c.ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !c.want(err) {
				t.Errorf("the pass returned %v", err)
			}
		})
	}
}
