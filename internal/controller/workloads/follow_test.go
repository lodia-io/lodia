// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/workloads"
)

func reports(apiServer, controllerManager, scheduler string) map[string]any {
	report := func(version string) map[string]any {
		return map[string]any{"total": int64(2), "ready": int64(2), "updated": int64(2), "version": version}
	}
	return map[string]any{"components": map[string]any{
		"apiServer":         report(apiServer),
		"controllerManager": report(controllerManager),
		"scheduler":         report(scheduler),
	}}
}

func TestTheControllerManagerAndSchedulerFollowTheAPIServerAMinorApart(t *testing.T) {
	cp := controlPlane(t, loadBalancer())
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	setTargetVersion(t, cp, "v1.36.4")
	inputsReadyBesidesTheTarget(t, cp)
	for _, component := range componentNames {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	expectVersions(t, cp, "v1.36.4 v1.36.4 v1.36.4")

	setTargetVersion(t, cp, "v1.37.1")
	eventuallyImages(t, cp, "v1.37.1", "kube-apiserver")
	konnectivity := deployment(t, cp, "kube-apiserver").Spec.Template.Spec.InitContainers
	if i := indexOfContainer(konnectivity, "konnectivity-server"); i < 0 || !strings.HasSuffix(konnectivity[i].Image, ":v0.37.0") {
		t.Errorf("the API server's init containers are %+v, want konnectivity-server at the target's minor, v0.37.0", konnectivity)
	}
	expectImagesAfterAPass(t, cp, "v1.36.4", "kube-controller-manager", "kube-scheduler")

	markAvailable(t, deployment(t, cp, "kube-apiserver"))
	expectVersions(t, cp, "v1.37.1 v1.36.4 v1.36.4")
	eventuallyImages(t, cp, "v1.37.1", "kube-controller-manager", "kube-scheduler")
	markAvailable(t, deployment(t, cp, "kube-controller-manager"))
	markAvailable(t, deployment(t, cp, "kube-scheduler"))
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
}

func indexOfContainer(containers []corev1.Container, name string) int {
	for i := range containers {
		if containers[i].Name == name {
			return i
		}
	}
	return -1
}

func TestAnAbsentReportRendersTheTarget(t *testing.T) {
	cp := highlyAvailable(t)
	for _, component := range componentNames {
		eventuallyDeployment(t, cp, component)
	}
	expectVersions(t, cp, "none none none")
	setTargetVersion(t, cp, "v1.37.2")
	eventuallyImages(t, cp, "v1.37.2")
}

func TestAReportOutsideTheTableWaitsAndWritesNoDeployment(t *testing.T) {
	cp := highlyAvailable(t)
	for _, component := range componentNames {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
	pause(t, cp)
	setTargetVersion(t, cp, "v1.37.2")
	applyStatus(t, cp, workloads.FieldManager, reports("v1.33.2", "v1.37.1", "v1.37.1"))
	before := resourceVersions(t, cp.Namespace, &appsv1.DeploymentList{})

	status := appliedStatus(t, cp, unpaused())
	waiting := meta.FindStatusCondition(status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
	if waiting == nil || waiting.Status != metav1.ConditionFalse || waiting.Reason != lodiav1alpha1.ReasonWaitingForTarget ||
		waiting.Message != "status.components.apiServer.version v1.33.2 is not a version Lodia runs" {
		t.Errorf("APIServerReady is %+v, want False, WaitingForTarget naming v1.33.2", waiting)
	}
	if after := resourceVersions(t, cp.Namespace, &appsv1.DeploymentList{}); !maps.Equal(before, after) {
		t.Errorf("the pass wrote Deployments: resourceVersions %v, then %v", before, after)
	}
	if err := imagesAt(t, cp, "v1.37.1", componentNames); err != nil {
		t.Error(err)
	}

	applyStatus(t, cp, workloads.FieldManager, reports("v1.37.1", "v1.37.1", "v1.37.1"))
	reconcileThrough(t, cp, unpaused())
	if err := imagesAt(t, cp, "v1.37.2", []string{"kube-apiserver"}); err != nil {
		t.Errorf("with a report in the table: %v", err)
	}
	if err := imagesAt(t, cp, "v1.37.1", []string{"kube-controller-manager", "kube-scheduler"}); err != nil {
		t.Errorf("with a report in the table: %v", err)
	}
}

func TestAStaleReportInTheCacheDoesNotRollTheControllerManagerBack(t *testing.T) {
	cp := highlyAvailable(t)
	for _, component := range componentNames {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
	pause(t, cp)
	setTargetVersion(t, cp, "v1.37.2")
	cache := get(t, cp)
	if got := reported(cache.Status.Components); got != "v1.37.1 v1.37.1 v1.37.1" {
		t.Fatalf("the cache holds the versions %s", got)
	}

	applyStatus(t, cp, workloads.FieldManager, reports("v1.37.2", "v1.37.1", "v1.37.1"))
	reconcileThrough(t, cp, unpaused())
	if err := imagesAt(t, cp, "v1.37.2", componentNames); err != nil {
		t.Fatalf("with the API server's report at v1.37.2: %v", err)
	}
	before := resourceVersions(t, cp.Namespace, &appsv1.DeploymentList{})

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
	reconcileThrough(t, cp, stale)
	if err := imagesAt(t, cp, "v1.37.2", componentNames); err != nil {
		t.Errorf("a pass on a cache older than the API server: %v", err)
	}
	if after := resourceVersions(t, cp.Namespace, &appsv1.DeploymentList{}); !maps.Equal(before, after) {
		t.Errorf("a pass on a cache older than the API server wrote Deployments: resourceVersions %v, then %v", before, after)
	}
	if got := reported(get(t, cp).Status.Components); got != "v1.37.2 v1.37.1 v1.37.1" {
		t.Errorf("a pass on a cache older than the API server leaves the versions %s, want v1.37.2 v1.37.1 v1.37.1", got)
	}
}

func TestAWriteThatFailsAfterTheAPIServerLeavesTheControllerManagerAtTheReport(t *testing.T) {
	cp := highlyAvailable(t)
	for _, component := range componentNames {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
	pause(t, cp)
	setTargetVersion(t, cp, "v1.37.2")

	injected := apierrors.NewServiceUnavailable("injected")
	failing := interceptor.NewClient(unpaused(), interceptor.Funcs{
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			raw, err := json.Marshal(obj)
			if err != nil {
				t.Fatal(err)
			}
			var applied metav1.PartialObjectMetadata
			if err := json.Unmarshal(raw, &applied); err != nil {
				t.Fatal(err)
			}
			if applied.Kind == "Deployment" && applied.Name == "dev-kube-controller-manager" {
				return injected
			}
			return c.Apply(ctx, obj, opts...)
		},
	})
	r := &workloads.Reconciler{Client: failing, Reader: k8s, SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: true}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("the pass returned %v, want the injected error", err)
	}
	if err := imagesAt(t, cp, "v1.37.2", []string{"kube-apiserver"}); err != nil {
		t.Errorf("after the failed pass: %v", err)
	}
	if err := imagesAt(t, cp, "v1.37.1", []string{"kube-controller-manager", "kube-scheduler"}); err != nil {
		t.Errorf("after the failed pass: %v", err)
	}

	reconcileThrough(t, cp, unpaused())
	if err := imagesAt(t, cp, "v1.37.2", []string{"kube-apiserver"}); err != nil {
		t.Errorf("after the pass that follows: %v", err)
	}
	if err := imagesAt(t, cp, "v1.37.1", []string{"kube-controller-manager", "kube-scheduler"}); err != nil {
		t.Errorf("after the pass that follows: %v", err)
	}
}

func TestAControllerManagerAheadOfTheAPIServersReportIsRenderedBackToIt(t *testing.T) {
	cp := highlyAvailable(t)
	for _, component := range componentNames {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	expectVersions(t, cp, "v1.37.1 v1.37.1 v1.37.1")
	pause(t, cp)
	setTargetVersion(t, cp, "v1.37.2")
	for _, component := range []string{"kube-controller-manager", "kube-scheduler"} {
		d := deployment(t, cp, component)
		for i := range d.Spec.Template.Spec.Containers {
			if c := &d.Spec.Template.Spec.Containers[i]; c.Name == component {
				c.Image = strings.Replace(c.Image, ":v1.37.1", ":v1.37.2", 1)
			}
		}
		if err := k8s.Update(t.Context(), d); err != nil {
			t.Fatal(err)
		}
	}
	if err := imagesAt(t, cp, "v1.37.2", []string{"kube-controller-manager", "kube-scheduler"}); err != nil {
		t.Fatalf("the leftover of a render at the target: %v", err)
	}

	reconcileThrough(t, cp, unpaused())
	if err := imagesAt(t, cp, "v1.37.1", []string{"kube-controller-manager", "kube-scheduler"}); err != nil {
		t.Error(err)
	}
	if err := imagesAt(t, cp, "v1.37.2", []string{"kube-apiserver"}); err != nil {
		t.Error(err)
	}
}
