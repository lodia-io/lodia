// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/controller/lifecycle"
)

var lifecycleRules = []rbacv1.PolicyRule{
	{APIGroups: []string{bryggav1alpha1.GroupVersion.Group}, Resources: []string{"controlplanes"}, Verbs: []string{"list", "watch"}},
	{APIGroups: []string{bryggav1alpha1.GroupVersion.Group}, Resources: []string{"controlplanes/status"}, Verbs: []string{"patch"}},
}

var readiness = []string{
	bryggav1alpha1.ConditionCertificatesReady,
	bryggav1alpha1.ConditionDatastoreReady,
	bryggav1alpha1.ConditionAPIServerReady,
	bryggav1alpha1.ConditionAccessReady,
	bryggav1alpha1.ConditionAddonsApplied,
	bryggav1alpha1.ConditionReachable,
}

var k8s client.WithWatch

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("skipping the lifecycle controller tests: KUBEBUILDER_ASSETS is not set; run make test")
		os.Exit(0)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		UseExistingCluster:    new(false),
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		os.Exit(1)
	}
	stop := func(what string, err error) {
		fmt.Fprintln(os.Stderr, what+":", err)
		_ = env.Stop()
		os.Exit(1)
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, bryggav1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			stop("scheme", err)
		}
	}
	if k8s, err = client.NewWithWatch(cfg, client.Options{Scheme: scheme}); err != nil {
		stop("client", err)
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	controller, err := asLifecycleController(cfg)
	if err != nil {
		stop("the controller's ClusterRole", err)
	}
	labelled, err := labels.NewRequirement(bryggav1alpha1.ControlPlaneLabel, selection.Exists, nil)
	if err != nil {
		stop("label selector", err)
	}
	everything := cache.ByObject{Label: labels.Everything()}
	mgr, err := ctrl.NewManager(controller, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			DefaultLabelSelector:        labels.NewSelector().Add(*labelled),
			DefaultTransform:            cache.TransformStripManagedFields(),
			ReaderFailOnMissingInformer: true,
			ByObject: map[client.Object]cache.ByObject{
				&bryggav1alpha1.ControlPlane{}:      everything,
				&bryggav1alpha1.DatastoreClass{}:    everything,
				&bryggav1alpha1.Datastore{}:         everything,
				&bryggav1alpha1.KubeconfigRequest{}: everything,
				&bryggav1alpha1.Backup{}:            everything,
			},
		},
	})
	if err != nil {
		stop("manager", err)
	}
	if err := (&lifecycle.Reconciler{Client: mgr.GetClient()}).SetupWithManager(mgr, 1); err != nil {
		stop("controller", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() { started <- mgr.Start(ctx) }()
	code := m.Run()
	cancel()
	if err := <-started; err != nil {
		fmt.Fprintln(os.Stderr, "manager:", err)
		code = 1
	}
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop envtest:", err)
	}
	os.Exit(code)
}

func asLifecycleController(cfg *rest.Config) (*rest.Config, error) {
	const user = "brygga-lifecycle"
	ctx := context.Background()
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: user}, Rules: lifecycleRules}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: user},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}
	for _, obj := range []client.Object{role, binding} {
		if err := k8s.Create(ctx, obj); err != nil {
			return nil, err
		}
	}
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
			User:               user,
			ResourceAttributes: &authorizationv1.ResourceAttributes{Group: bryggav1alpha1.GroupVersion.Group, Resource: "controlplanes", Verb: "watch"},
		}}
		if err := k8s.Create(ctx, review); err != nil {
			return false, err
		}
		return review.Status.Allowed, nil
	})
	if err != nil {
		return nil, err
	}
	controller := rest.CopyConfig(cfg)
	controller.Impersonate = rest.ImpersonationConfig{UserName: user}
	return controller, nil
}

func TestTheTargetFollowsTheVersionWithinAMinor(t *testing.T) {
	cp := controlPlane(t, "v1.35.0")
	eventually(t, func() error { return expect(t, cp, "v1.35.0", "", bryggav1alpha1.ReasonIdle) })

	setVersion(t, cp, "v1.35.1")
	eventually(t, func() error { return expect(t, cp, "v1.35.1", "", bryggav1alpha1.ReasonIdle) })

	setVersion(t, cp, "v1.36.0")
	eventually(t, func() error {
		return expect(t, cp, "v1.35.1", bryggav1alpha1.ReasonUnsupported, bryggav1alpha1.ReasonUnsupported)
	})

	setVersion(t, cp, "v1.38.0")
	eventually(t, func() error {
		return expect(t, cp, "v1.35.1", bryggav1alpha1.ReasonUnsupported, bryggav1alpha1.ReasonUnsupported)
	})
	if c := condition(t, cp, bryggav1alpha1.ConditionProgressing); !strings.Contains(c.Message, "1.35, 1.36, 1.37") {
		t.Errorf("Progressing does not name the minors Brygga runs: %s", c.Message)
	}
}

func TestAnUnsupportedMinorGetsNoTarget(t *testing.T) {
	cp := controlPlane(t, "v1.34.2")
	eventually(t, func() error {
		return expect(t, cp, "", bryggav1alpha1.ReasonUnsupported, bryggav1alpha1.ReasonUnsupported)
	})

	setVersion(t, cp, "v1.35.0")
	eventually(t, func() error { return expect(t, cp, "v1.35.0", "", bryggav1alpha1.ReasonIdle) })
}

func TestReadyFollowsTheReadinessConditions(t *testing.T) {
	cp := controlPlane(t, "v1.37.1")
	eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionUnknown, bryggav1alpha1.ReasonLatestGenerationNotObserved, "CertificatesReady is not reported for generation 1", 0)
	})

	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady,
			"CertificatesReady, DatastoreReady, APIServerReady, AccessReady, AddonsApplied and Reachable are True", 1)
	})

	setCondition(t, cp, bryggav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, "RollingOut", "0 of 1 replicas updated")
	eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionFalse, "RollingOut", "APIServerReady: 0 of 1 replicas updated", 1)
	})

	setCondition(t, cp, bryggav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, "Issuing", "dev-ca-bundle is not written yet")
	eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionFalse, "Issuing", "CertificatesReady: dev-ca-bundle is not written yet", 1)
	})

	setVersion(t, cp, "v1.37.2")
	eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionUnknown, bryggav1alpha1.ReasonLatestGenerationNotObserved, "CertificatesReady is not reported for generation 2", 1)
	})
}

func TestAPausedControlPlaneKeepsItsTarget(t *testing.T) {
	cp := controlPlane(t, "v1.35.0")
	eventually(t, func() error { return expect(t, cp, "v1.35.0", "", bryggav1alpha1.ReasonIdle) })

	update(t, cp, func(cp *bryggav1alpha1.ControlPlane) {
		cp.Spec.Paused = true
		cp.Spec.Version = "v1.35.1"
	})
	eventually(t, func() error { return expect(t, cp, "v1.35.0", "", bryggav1alpha1.ReasonPaused) })

	update(t, cp, func(cp *bryggav1alpha1.ControlPlane) { cp.Spec.Paused = false })
	eventually(t, func() error { return expect(t, cp, "v1.35.1", "", bryggav1alpha1.ReasonIdle) })
}

func TestReadyDuringDeletion(t *testing.T) {
	cp := controlPlane(t, "v1.35.0")
	setFinalizers(t, cp, "test.brygga.segla.io/hold")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionTrue, bryggav1alpha1.ReasonReady,
			"CertificatesReady, DatastoreReady, APIServerReady, AccessReady, AddonsApplied and Reachable are True", 1)
	})

	if err := k8s.Delete(t.Context(), get(t, cp)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		c := condition(t, cp, bryggav1alpha1.ConditionReady)
		if c == nil || c.Status != metav1.ConditionFalse || c.Reason != bryggav1alpha1.ReasonDeleting {
			return fmt.Errorf("Ready is %+v, want False %s", c, bryggav1alpha1.ReasonDeleting)
		}
		return nil
	})
	setFinalizers(t, cp)
}

func setFinalizers(t *testing.T, cp *bryggav1alpha1.ControlPlane, finalizers ...string) {
	t.Helper()
	fresh := get(t, cp)
	patch := client.MergeFrom(fresh.DeepCopy())
	fresh.Finalizers = finalizers
	if err := k8s.Patch(t.Context(), fresh, patch); err != nil {
		t.Fatal(err)
	}
}

func TestAPassThatChangesNothingWritesNothing(t *testing.T) {
	cp := controlPlane(t, "v1.36.0")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	var settled string
	eventually(t, func() error {
		fresh := get(t, cp)
		ready := meta.FindStatusCondition(fresh.Status.Conditions, bryggav1alpha1.ConditionReady)
		if ready == nil || ready.Status != metav1.ConditionTrue || settled != fresh.ResourceVersion {
			settled = fresh.ResourceVersion
			return fmt.Errorf("resourceVersion %s, Ready %v", fresh.ResourceVersion, ready)
		}
		return nil
	})
	applies := 0
	counting := interceptor.NewClient(k8s, interceptor.Funcs{
		SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
			applies++
			return c.SubResource(subResource).Apply(ctx, obj, opts...)
		},
	})
	if _, err := (&lifecycle.Reconciler{Client: counting}).Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if after := get(t, cp).ResourceVersion; applies > 0 || after != settled {
		t.Errorf("a pass over an unchanged ControlPlane sent %d applies; resourceVersion %s, then %s", applies, settled, after)
	}
}

func controlPlane(t *testing.T, version string) *bryggav1alpha1.ControlPlane {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "lifecycle-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	cp := &bryggav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns.Name},
		Spec: bryggav1alpha1.ControlPlaneSpec{
			Version:   version,
			Datastore: bryggav1alpha1.DatastoreChoice{Dedicated: &bryggav1alpha1.DedicatedDatastore{}},
			Endpoint:  bryggav1alpha1.Endpoint{LoadBalancer: &bryggav1alpha1.LoadBalancerEndpoint{}},
			Network:   bryggav1alpha1.Network{ServiceCIDRs: []bryggav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []bryggav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	if err := k8s.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func get(t *testing.T, cp *bryggav1alpha1.ControlPlane) *bryggav1alpha1.ControlPlane {
	t.Helper()
	fresh := &bryggav1alpha1.ControlPlane{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		t.Fatal(err)
	}
	return fresh
}

func update(t *testing.T, cp *bryggav1alpha1.ControlPlane, change func(*bryggav1alpha1.ControlPlane)) {
	t.Helper()
	fresh := get(t, cp)
	change(fresh)
	if err := k8s.Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
}

func setVersion(t *testing.T, cp *bryggav1alpha1.ControlPlane, version string) {
	t.Helper()
	update(t, cp, func(cp *bryggav1alpha1.ControlPlane) { cp.Spec.Version = version })
}

func setCondition(t *testing.T, cp *bryggav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason, message string) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": bryggav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status": map[string]any{"conditions": []any{map[string]any{
			"type":               conditionType,
			"status":             string(status),
			"reason":             reason,
			"message":            message,
			"observedGeneration": get(t, cp).Generation,
			"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		}}},
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner("test/"+conditionType), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func condition(t *testing.T, cp *bryggav1alpha1.ControlPlane, conditionType string) *metav1.Condition {
	t.Helper()
	return meta.FindStatusCondition(get(t, cp).Status.Conditions, conditionType)
}

func expect(t *testing.T, cp *bryggav1alpha1.ControlPlane, version, targetReason, progressing string) error {
	t.Helper()
	fresh := get(t, cp)
	target := bryggav1alpha1.Target{}
	if fresh.Status.Target != nil {
		target = *fresh.Status.Target
	}
	c := meta.FindStatusCondition(fresh.Status.Conditions, bryggav1alpha1.ConditionProgressing)
	switch {
	case target.Version != version || target.Reason != targetReason:
		return fmt.Errorf("target is %q with reason %q, want %q with %q", target.Version, target.Reason, version, targetReason)
	case c == nil || c.Status != metav1.ConditionFalse || c.Reason != progressing || c.ObservedGeneration != fresh.Generation:
		return fmt.Errorf("Progressing is %+v, want False %s at generation %d", c, progressing, fresh.Generation)
	}
	return nil
}

func expectReady(t *testing.T, cp *bryggav1alpha1.ControlPlane, status metav1.ConditionStatus, reason, message string, observedGeneration int64) error {
	t.Helper()
	fresh := get(t, cp)
	c := meta.FindStatusCondition(fresh.Status.Conditions, bryggav1alpha1.ConditionReady)
	switch {
	case c == nil || c.Status != status || c.Reason != reason || c.Message != message || c.ObservedGeneration != fresh.Generation:
		return fmt.Errorf("Ready is %+v, want %s %s %q at generation %d", c, status, reason, message, fresh.Generation)
	case fresh.Status.ObservedGeneration != observedGeneration:
		return fmt.Errorf("status.observedGeneration is %d, want %d", fresh.Status.ObservedGeneration, observedGeneration)
	}
	return nil
}

func eventually(t *testing.T, check func() error) {
	t.Helper()
	var last error
	err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 30*time.Second, true, func(context.Context) (bool, error) {
		last = check()
		return last == nil, nil
	})
	if err != nil {
		t.Fatalf("%v: %v", err, last)
	}
}
