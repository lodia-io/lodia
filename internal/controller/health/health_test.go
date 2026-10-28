// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
	"go.lodia.io/lodia/internal/controller/health"
)

var (
	k8s              client.WithWatch
	tenant           client.Client
	tenantKubeconfig []byte
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("skipping the health controller tests: KUBEBUILDER_ASSETS is not set; run make test")
		os.Exit(0)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "api", "crds")},
		ErrorIfCRDPathMissing: true,
		UseExistingCluster:    new(false),
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		os.Exit(1)
	}
	tenantEnv := &envtest.Environment{UseExistingCluster: new(false)}
	stop := func(what string, err error) {
		fmt.Fprintln(os.Stderr, what+":", err)
		_ = tenantEnv.Stop()
		_ = env.Stop()
		os.Exit(1)
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, lodiav1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			stop("scheme", err)
		}
	}
	if k8s, err = client.NewWithWatch(cfg, client.Options{Scheme: scheme}); err != nil {
		stop("client", err)
	}
	tenantCfg, err := tenantEnv.Start()
	if err != nil {
		stop("start the tenant's envtest", err)
	}
	if tenant, err = client.New(tenantCfg, client.Options{Scheme: scheme}); err != nil {
		stop("tenant client", err)
	}
	if tenantKubeconfig, err = kubeconfigOf(tenantCfg.Host, tenantCfg); err != nil {
		stop("tenant kubeconfig", err)
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	controller, err := asHealthController(cfg)
	if err != nil {
		stop("the controller's ClusterRole", err)
	}
	labelled, err := labels.NewRequirement(lodiav1alpha1.ControlPlaneLabel, selection.Exists, nil)
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
				&lodiav1alpha1.ControlPlane{}:      everything,
				&lodiav1alpha1.DatastoreClass{}:    everything,
				&lodiav1alpha1.Datastore{}:         everything,
				&lodiav1alpha1.KubeconfigRequest{}: everything,
				&lodiav1alpha1.Backup{}:            everything,
			},
		},
	})
	if err != nil {
		stop("manager", err)
	}
	if err := (&health.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Period: time.Hour, Timeout: 2 * time.Second, Clock: clock.RealClock{}}).SetupWithManager(mgr, 1); err != nil {
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
	if err := tenantEnv.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop the tenant's envtest:", err)
	}
	os.Exit(code)
}

func asHealthController(cfg *rest.Config) (*rest.Config, error) {
	const user = "lodia-health"
	ctx := context.Background()
	manifest, err := os.ReadFile(filepath.Join("..", "..", "..", "charts", "lodia", "files", "rbac", "health.yaml"))
	if err != nil {
		return nil, err
	}
	role := &rbacv1.ClusterRole{}
	if err := yaml.UnmarshalStrict(manifest, role); err != nil {
		return nil, err
	}
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
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
			User:               user,
			ResourceAttributes: &authorizationv1.ResourceAttributes{Resource: "secrets", Verb: "get"},
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

func TestReachableFollowsTheProbe(t *testing.T) {
	cp := controlPlane(t)
	eventually(t, func() error {
		return expect(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint")
	})

	e := serve(t)
	publish(t, cp, e)
	eventually(t, func() error {
		return expect(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates, "waiting for PKI to write the CA bundle dev-ca-bundle")
	})

	probe := &health.Reconciler{Client: k8s, Reader: k8s, Period: time.Hour, Timeout: 300 * time.Millisecond}
	writeBundle(t, cp, e)
	reconcile(t, probe, cp)
	if err := expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200"); err != nil {
		t.Fatal(err)
	}

	e.code.Store(http.StatusServiceUnavailable)
	for failed := 1; failed < health.FailuresBeforeFalse; failed++ {
		reconcile(t, probe, cp)
		if err := expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200"); err != nil {
			t.Fatalf("after %d failed probes: %v", failed, err)
		}
	}
	reconcile(t, probe, cp)
	if err := expect(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed, e.url+" answered 503"); err != nil {
		t.Fatal(err)
	}

	e.code.Store(http.StatusOK)
	reconcile(t, probe, cp)
	if err := expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200"); err != nil {
		t.Fatal(err)
	}

	e.hang.Store(true)
	for range health.FailuresBeforeFalse {
		reconcile(t, probe, cp)
	}
	if err := expect(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed, e.url+" did not answer"); err != nil {
		t.Fatal(err)
	}
}

func TestAPassThatChangesNothingWritesNothing(t *testing.T) {
	cp := controlPlane(t)
	e := serve(t)
	writeBundle(t, cp, e)
	publish(t, cp, e)
	eventually(t, func() error {
		return expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200")
	})

	applies := 0
	counting := interceptor.NewClient(k8s, interceptor.Funcs{
		SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
			applies++
			return c.SubResource(subResource).Apply(ctx, obj, opts...)
		},
	})
	before := get(t, cp).ResourceVersion
	reconcile(t, &health.Reconciler{Client: counting, Reader: k8s, Period: time.Hour, Timeout: time.Second}, cp)
	if after := get(t, cp).ResourceVersion; applies > 0 || after != before {
		t.Errorf("a pass over an unchanged endpoint sent %d applies; resourceVersion %s, then %s", applies, before, after)
	}
}

func TestAFailedReadLeavesTheStatus(t *testing.T) {
	cp := controlPlane(t)
	e := serve(t)
	writeBundle(t, cp, e)
	publish(t, cp, e)
	eventually(t, func() error {
		return expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200")
	})

	e.code.Store(http.StatusServiceUnavailable)
	failing := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Secret); ok {
				return apierrors.NewServiceUnavailable("the API server is restarting")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	probe := &health.Reconciler{Client: k8s, Reader: failing, Period: time.Hour, Timeout: time.Second}
	before := get(t, cp).ResourceVersion
	for range health.FailuresBeforeFalse {
		if _, err := probe.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
			t.Fatal("a pass whose read of the CA bundle failed returned no error")
		}
	}
	if after := get(t, cp).ResourceVersion; after != before {
		t.Errorf("a pass whose read failed wrote the status: resourceVersion %s, then %s", before, after)
	}
}

func TestAPassThatFailsAfterAFailedProbeCountsNoFailure(t *testing.T) {
	cp := controlPlane(t)
	e := serve(t)
	writeBundle(t, cp, e)
	publish(t, cp, e)
	setAccess(t, cp)
	eventually(t, func() error {
		return expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200")
	})

	e.code.Store(http.StatusServiceUnavailable)
	injected := errors.New("injected: the API server did not answer")
	probe := &health.Reconciler{Client: k8s, Period: time.Hour, Timeout: time.Second, Clock: clock.RealClock{}}
	for _, failed := range []struct {
		read  string
		fails func(client.ObjectKey, client.Object) bool
	}{
		{"the live Reachable", func(_ client.ObjectKey, obj client.Object) bool {
			_, ok := obj.(*lodiav1alpha1.ControlPlane)
			return ok
		}},
		{"Lodia's kubeconfig", func(key client.ObjectKey, _ client.Object) bool {
			return key.Name == lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name)
		}},
	} {
		probe.Reader = interceptor.NewClient(k8s, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if failed.fails(key, obj) {
					return injected
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
		for range health.FailuresBeforeFalse - 1 {
			if _, err := probe.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !errors.Is(err, injected) {
				t.Fatalf("a pass whose read of %s failed returned %v, want the read's error", failed.read, err)
			}
		}
	}

	probe.Reader = k8s
	reconcile(t, probe, cp)
	if err := expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200"); err != nil {
		t.Errorf("one failed probe after passes that failed after theirs: %v", err)
	}
}

func TestControllersLeadingFollowsTheLeaderLeases(t *testing.T) {
	cp := controlPlane(t)
	eventually(t, func() error {
		return expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, "AccessReady is not True")
	})
	setAccess(t, cp)
	eventually(t, func() error {
		return expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, "waiting for PKI to write dev-lodia-kubeconfig")
	})
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := &health.Reconciler{Client: k8s, Reader: k8s, Period: 10 * time.Second, Timeout: 2 * time.Second, Clock: now}
	writeKubeconfig(t, cp, tenantKubeconfig)
	removeLeases(t)
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerNotLeading,
		"Lease kube-system/kube-controller-manager does not exist; Lease kube-system/kube-scheduler does not exist"); err != nil {
		t.Fatal(err)
	}

	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", start)
	renew(t, lodiav1alpha1.SchedulerLease, "dev-kube-scheduler-5c4_2", start)
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, "the leader Leases of kube-controller-manager and kube-scheduler are renewed"); err != nil {
		t.Fatal(err)
	}
	now.SetTime(start.Add(25 * time.Second))
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, "the leader Leases of kube-controller-manager and kube-scheduler are renewed"); err != nil {
		t.Fatalf("within each lease's 15 seconds and the period: %v", err)
	}
	now.SetTime(start.Add(26 * time.Second))
	reconcile(t, r, cp)
	renewed := start.UTC().Format(time.RFC3339)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerNotLeading,
		"Lease kube-system/kube-controller-manager of dev-kube-controller-manager-7d9_1 was last renewed "+renewed+
			"; Lease kube-system/kube-scheduler of dev-kube-scheduler-5c4_2 was last renewed "+renewed); err != nil {
		t.Fatal(err)
	}

	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", start.Add(26*time.Second))
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonSchedulerNotLeading,
		"Lease kube-system/kube-scheduler of dev-kube-scheduler-5c4_2 was last renewed "+renewed); err != nil {
		t.Fatal(err)
	}
	renew(t, lodiav1alpha1.SchedulerLease, "dev-kube-scheduler-8f1_3", start.Add(26*time.Second))
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, "the leader Leases of kube-controller-manager and kube-scheduler are renewed"); err != nil {
		t.Fatal(err)
	}

	now.SetTime(start.Add(27 * time.Second))
	release(t, lodiav1alpha1.SchedulerLease, start.Add(27*time.Second))
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, "the leader Leases of kube-controller-manager and kube-scheduler are renewed"); err != nil {
		t.Fatalf("the pass after a release: %v", err)
	}
	now.SetTime(start.Add(37 * time.Second))
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, "the leader Leases of kube-controller-manager and kube-scheduler are renewed"); err != nil {
		t.Fatalf("a pass within a period of a release: %v", err)
	}
	now.SetTime(start.Add(38 * time.Second))
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, "the leader Leases of kube-controller-manager and kube-scheduler are renewed"); err != nil {
		t.Fatalf("within the released lease's 1 second and the period: %v", err)
	}
	now.SetTime(start.Add(39 * time.Second))
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonSchedulerNotLeading,
		"Lease kube-system/kube-scheduler has no holder; it was last renewed "+start.Add(27*time.Second).UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
}

func TestAFailedLeaseReadKeepsControllersLeading(t *testing.T) {
	cp := controlPlane(t)
	setAccess(t, cp)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := &health.Reconciler{Client: k8s, Reader: k8s, Period: 10 * time.Second, Timeout: 2 * time.Second, Clock: now}
	writeKubeconfig(t, cp, tenantKubeconfig)
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", start)
	renew(t, lodiav1alpha1.SchedulerLease, "dev-kube-scheduler-5c4_2", start)
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, "the leader Leases of kube-controller-manager and kube-scheduler are renewed"); err != nil {
		t.Fatal(err)
	}

	unreachable, err := kubeconfigOf("https://127.0.0.1:1", nil)
	if err != nil {
		t.Fatal(err)
	}
	writeKubeconfig(t, cp, unreachable)
	now.SetTime(start.Add(time.Hour))
	before := get(t, cp).ResourceVersion
	reconcile(t, r, cp)
	if after := get(t, cp).ResourceVersion; after != before {
		t.Errorf("a pass whose lease reads failed wrote the status: resourceVersion %s, then %s", before, after)
	}

	writeKubeconfig(t, cp, tenantKubeconfig)
	reconcile(t, r, cp)
	renewed := start.UTC().Format(time.RFC3339)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerNotLeading,
		"Lease kube-system/kube-controller-manager of dev-kube-controller-manager-7d9_1 was last renewed "+renewed+
			"; Lease kube-system/kube-scheduler of dev-kube-scheduler-5c4_2 was last renewed "+renewed); err != nil {
		t.Fatalf("after a failed read, the leases unchanged since they were first seen: %v", err)
	}
}

func TestAKeptConditionIsWrittenAtTheObjectsGeneration(t *testing.T) {
	cp := controlPlane(t)
	e := serve(t)
	writeBundle(t, cp, e)
	publish(t, cp, e)
	setAccess(t, cp)
	writeKubeconfig(t, cp, tenantKubeconfig)
	start := time.Now().Truncate(time.Second)
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", start)
	renew(t, lodiav1alpha1.SchedulerLease, "dev-kube-scheduler-5c4_2", start)
	r := &health.Reconciler{Client: k8s, Reader: k8s, Period: 10 * time.Second, Timeout: 2 * time.Second, Clock: clocktesting.NewFakePassiveClock(start)}
	reconcile(t, r, cp)
	const leading = "the leader Leases of kube-controller-manager and kube-scheduler are renewed"
	if err := expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200"); err != nil {
		t.Fatal(err)
	}
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, leading); err != nil {
		t.Fatal(err)
	}

	e.code.Store(http.StatusServiceUnavailable)
	unreachable, err := kubeconfigOf("https://127.0.0.1:1", nil)
	if err != nil {
		t.Fatal(err)
	}
	writeKubeconfig(t, cp, unreachable)
	changed := get(t, cp)
	changed.Spec.Placement = &lodiav1alpha1.Placement{PriorityClassName: "system-cluster-critical"}
	if err := k8s.Update(t.Context(), changed); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		if err := expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200"); err != nil {
			return err
		}
		return expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, leading)
	})
}

func TestAKeptConditionIsDecidedFromTheServer(t *testing.T) {
	cp := controlPlane(t)
	e := serve(t)
	writeBundle(t, cp, e)
	publish(t, cp, e)
	setAccess(t, cp)
	writeKubeconfig(t, cp, tenantKubeconfig)
	start := time.Now().Truncate(time.Second)
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", start)
	renew(t, lodiav1alpha1.SchedulerLease, "dev-kube-scheduler-5c4_2", start)
	const leading = "the leader Leases of kube-controller-manager and kube-scheduler are renewed"
	eventually(t, func() error {
		return expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, leading)
	})
	now := clocktesting.NewFakePassiveClock(start)
	r := &health.Reconciler{Client: k8s, Reader: k8s, Period: 10 * time.Second, Timeout: 2 * time.Second, Clock: now}
	e.code.Store(http.StatusServiceUnavailable)
	for range health.FailuresBeforeFalse {
		reconcile(t, r, cp)
	}
	if err := expect(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed, e.url+" answered 503"); err != nil {
		t.Fatal(err)
	}
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, leading); err != nil {
		t.Fatal(err)
	}

	paused := get(t, cp)
	paused.Spec.Paused = true
	if err := k8s.Update(t.Context(), paused); err != nil {
		t.Fatal(err)
	}
	stale := get(t, cp)
	pass := func(at *lodiav1alpha1.ControlPlane, reader client.Reader) error {
		held := at.DeepCopy()
		held.Spec.Paused = false
		r.Client = interceptor.NewClient(k8s, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if plane, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
					held.DeepCopyInto(plane)
					return nil
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
		r.Reader = reader
		_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		return err
	}

	e.code.Store(http.StatusOK)
	now.SetTime(start.Add(26 * time.Second))
	if err := pass(get(t, cp), k8s); err != nil {
		t.Fatal(err)
	}
	renewed := start.UTC().Format(time.RFC3339)
	notLeading := "Lease kube-system/kube-controller-manager of dev-kube-controller-manager-7d9_1 was last renewed " + renewed +
		"; Lease kube-system/kube-scheduler of dev-kube-scheduler-5c4_2 was last renewed " + renewed
	if err := expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200"); err != nil {
		t.Fatal(err)
	}
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerNotLeading, notLeading); err != nil {
		t.Fatal(err)
	}

	unreachable, err := kubeconfigOf("https://127.0.0.1:1", nil)
	if err != nil {
		t.Fatal(err)
	}
	writeKubeconfig(t, cp, unreachable)
	injected := errors.New("injected: the API server did not answer")
	failing := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				return injected
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	written := get(t, cp).ResourceVersion
	for _, kept := range []struct {
		condition string
		code      int32
	}{{lodiav1alpha1.ConditionControllersLeading, http.StatusOK}, {lodiav1alpha1.ConditionReachable, http.StatusServiceUnavailable}} {
		e.code.Store(kept.code)
		if err := pass(stale, failing); !errors.Is(err, injected) {
			t.Errorf("a pass that keeps %s and cannot read it from the server returned %v, want the read's error", kept.condition, err)
		}
	}
	if after := get(t, cp).ResourceVersion; after != written {
		t.Errorf("a pass that cannot read what it keeps from the server wrote the ControlPlane: resourceVersion %s, then %s", written, after)
	}

	hanging := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	t.Cleanup(hanging.Close)
	timingOut, err := kubeconfigOf(hanging.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	writeKubeconfig(t, cp, timingOut)
	if err := pass(stale, k8s); err != nil {
		t.Fatal(err)
	}
	if err := expect(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, e.url+" answered 200"); err != nil {
		t.Errorf("Reachable kept after a failed probe on a read without Health's last write: %v", err)
	}
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerNotLeading, notLeading); err != nil {
		t.Errorf("ControllersLeading kept after a failed Lease read on a read without Health's last write: %v", err)
	}
}

func TestControllersLeadingWaitsForAccessReadyAtAnyGeneration(t *testing.T) {
	cp := controlPlane(t)
	setAccess(t, cp)
	eventually(t, func() error {
		return expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, "waiting for PKI to write dev-lodia-kubeconfig")
	})
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := &health.Reconciler{Client: k8s, Reader: k8s, Period: 10 * time.Second, Timeout: 2 * time.Second, Clock: now}
	writeKubeconfig(t, cp, tenantKubeconfig)
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", start)
	renew(t, lodiav1alpha1.SchedulerLease, "dev-kube-scheduler-5c4_2", start)
	reconcile(t, r, cp)
	const leading = "the leader Leases of kube-controller-manager and kube-scheduler are renewed"
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, leading); err != nil {
		t.Fatal(err)
	}

	changed := get(t, cp)
	stale := changed.Generation
	changed.Spec.Placement = &lodiav1alpha1.Placement{PriorityClassName: "system-cluster-critical"}
	if err := k8s.Update(t.Context(), changed); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		return expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, leading)
	})
	now.SetTime(start.Add(26 * time.Second))
	reconcile(t, r, cp)
	renewed := start.UTC().Format(time.RFC3339)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerNotLeading,
		"Lease kube-system/kube-controller-manager of dev-kube-controller-manager-7d9_1 was last renewed "+renewed+
			"; Lease kube-system/kube-scheduler of dev-kube-scheduler-5c4_2 was last renewed "+renewed); err != nil {
		t.Fatalf("the Leases seen before the change: %v", err)
	}

	setAccessAt(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding, stale)
	eventually(t, func() error {
		return expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, "AccessReady is not True")
	})
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, "AccessReady is not True"); err != nil {
		t.Fatal(err)
	}
	setAccessAt(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonBound, stale)
	reconcile(t, r, cp)
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, leading); err != nil {
		t.Fatalf("the Leases seen again after the wait: %v", err)
	}
}

type endpoint struct {
	*httptest.Server
	url    string
	bundle []byte
	code   atomic.Int32
	hang   atomic.Bool
}

func serve(t *testing.T) *endpoint {
	t.Helper()
	now := time.Now()
	ca, err := certs.NewCA("health-test-ca", now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue(certs.Profile{CommonName: "kube-apiserver", IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := certs.CertPEM(leaf.Cert)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := certs.KeyPEM(leaf.Key)
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	e := &endpoint{}
	e.code.Store(http.StatusOK)
	e.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if e.hang.Load() {
			<-r.Context().Done()
			return
		}
		if r.URL.Path != "/readyz" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(int(e.code.Load()))
	}))
	e.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	e.StartTLS()
	t.Cleanup(e.Close)
	e.url = "https://" + e.Listener.Addr().String() + "/readyz"
	if e.bundle, err = certs.CertPEM(ca.Cert); err != nil {
		t.Fatal(err)
	}
	return e
}

func writeBundle(t *testing.T, cp *lodiav1alpha1.ControlPlane, e *endpoint) {
	t.Helper()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: lodiav1alpha1.CABundleSecret.Name(cp.Name), Namespace: cp.Namespace},
		Data:       map[string][]byte{lodiav1alpha1.CACertKey: e.bundle},
	}
	if err := k8s.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
}

func publish(t *testing.T, cp *lodiav1alpha1.ControlPlane, e *endpoint) {
	t.Helper()
	host, port, err := net.SplitHostPort(e.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	address := map[string]any{"host": host, "port": int64(p)}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     map[string]any{"endpoint": map[string]any{"apiServer": address, "konnectivity": address, "inCluster": address}},
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner("test/endpoint"), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func controlPlane(t *testing.T) *lodiav1alpha1.ControlPlane {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "health-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns.Name},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   "v1.35.0",
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint:  lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	if err := k8s.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func reconcile(t *testing.T, r *health.Reconciler, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != r.Period {
		t.Errorf("requeue after %s, want the probe period %s", result.RequeueAfter, r.Period)
	}
}

func get(t *testing.T, cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.ControlPlane {
	t.Helper()
	fresh := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		t.Fatal(err)
	}
	return fresh
}

func expect(t *testing.T, cp *lodiav1alpha1.ControlPlane, status metav1.ConditionStatus, reason, message string) error {
	t.Helper()
	fresh := get(t, cp)
	c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionReachable)
	if c == nil || c.Status != status || c.Reason != reason || c.Message != message || c.ObservedGeneration != fresh.Generation {
		return fmt.Errorf("Reachable is %+v, want %s %s %q at generation %d", c, status, reason, message, fresh.Generation)
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

func kubeconfigOf(server string, cfg *rest.Config) ([]byte, error) {
	user := &clientcmdapi.AuthInfo{}
	cluster := &clientcmdapi.Cluster{Server: server}
	if cfg != nil {
		user.ClientCertificateData, user.ClientKeyData, cluster.CertificateAuthorityData = cfg.CertData, cfg.KeyData, cfg.CAData
	}
	return clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"tenant": cluster},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": user},
		Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
		CurrentContext: "tenant",
	})
}

func writeKubeconfig(t *testing.T, cp *lodiav1alpha1.ControlPlane, kubeconfig []byte) {
	t.Helper()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name), Namespace: cp.Namespace}}
	err := k8s.Get(t.Context(), client.ObjectKeyFromObject(secret), secret)
	if client.IgnoreNotFound(err) != nil {
		t.Fatal(err)
	}
	secret.Data = map[string][]byte{lodiav1alpha1.KubeconfigKey: kubeconfig}
	if err == nil {
		err = k8s.Update(t.Context(), secret)
	} else {
		err = k8s.Create(t.Context(), secret)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func setAccess(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	setAccessAt(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonBound, get(t, cp).Generation)
}

func setAccessAt(t *testing.T, cp *lodiav1alpha1.ControlPlane, status metav1.ConditionStatus, reason string, generation int64) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status": map[string]any{"conditions": []any{map[string]any{
			"type": lodiav1alpha1.ConditionAccessReady, "status": string(status), "reason": reason, "message": "set by the test",
			"observedGeneration": generation, "lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		}}},
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner("test/access"), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func removeLeases(t *testing.T) {
	t.Helper()
	for _, name := range []string{lodiav1alpha1.ControllerManagerLease, lodiav1alpha1.SchedulerLease} {
		lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: lodiav1alpha1.LeaderElectionNamespace, Name: name}}
		if err := client.IgnoreNotFound(tenant.Delete(t.Context(), lease)); err != nil {
			t.Fatal(err)
		}
	}
}

func release(t *testing.T, name string, at time.Time) {
	t.Helper()
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: lodiav1alpha1.LeaderElectionNamespace, Name: name}}
	if err := tenant.Get(t.Context(), client.ObjectKeyFromObject(lease), lease); err != nil {
		t.Fatal(err)
	}
	lease.Spec.HolderIdentity, lease.Spec.LeaseDurationSeconds = nil, new(int32(1))
	lease.Spec.AcquireTime, lease.Spec.RenewTime = &metav1.MicroTime{Time: at}, &metav1.MicroTime{Time: at}
	if err := tenant.Update(t.Context(), lease); err != nil {
		t.Fatal(err)
	}
}

func renew(t *testing.T, name, holder string, at time.Time) {
	t.Helper()
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: lodiav1alpha1.LeaderElectionNamespace, Name: name}}
	err := tenant.Get(t.Context(), client.ObjectKeyFromObject(lease), lease)
	if client.IgnoreNotFound(err) != nil {
		t.Fatal(err)
	}
	lease.Spec = coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: new(int32(15)), RenewTime: &metav1.MicroTime{Time: at}}
	if err == nil {
		err = tenant.Update(t.Context(), lease)
	} else {
		err = tenant.Create(t.Context(), lease)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func expectLeading(t *testing.T, cp *lodiav1alpha1.ControlPlane, status metav1.ConditionStatus, reason, message string) error {
	t.Helper()
	fresh := get(t, cp)
	c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionControllersLeading)
	if c == nil || c.Status != status || c.Reason != reason || c.Message != message || c.ObservedGeneration != fresh.Generation {
		return fmt.Errorf("ControllersLeading is %+v, want %s %s %q at generation %d", c, status, reason, message, fresh.Generation)
	}
	return nil
}
