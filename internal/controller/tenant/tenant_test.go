// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant_test

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
	"go.lodia.io/lodia/internal/controller/pki"
	"go.lodia.io/lodia/internal/controller/tenant"
	"go.lodia.io/lodia/internal/operator"
)

const (
	version          = "v1.35.8"
	endpointHost     = "203.0.113.10"
	apiServerPort    = 443
	konnectivityPort = 8132
	inClusterPort    = 6443
)

type dialFunc = func(ctx context.Context, network, address string) (net.Conn, error)

var (
	k8s    client.Client
	scheme = runtime.NewScheme()
	dial   atomic.Pointer[dialFunc]

	unreachable dialFunc = func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("connection refused")
	}
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Exit(m.Run())
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
	stop := func(what string, err error) {
		fmt.Fprintln(os.Stderr, what+":", err)
		_ = env.Stop()
		os.Exit(1)
	}
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, lodiav1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			stop("scheme", err)
		}
	}
	if k8s, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		stop("client", err)
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	controller, err := asTenantController(cfg)
	if err != nil {
		stop("the controller's ClusterRole", err)
	}
	cache, err := operator.Cache()
	if err != nil {
		stop("cache options", err)
	}
	cache.ReaderFailOnMissingInformer = true
	mgr, err := ctrl.NewManager(controller, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, Cache: cache})
	if err != nil {
		stop("manager", err)
	}
	dial.Store(&unreachable)
	r := &tenant.Reconciler{
		Client:      mgr.GetClient(),
		Reader:      mgr.GetAPIReader(),
		ApplyPeriod: 2 * time.Second,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (*dial.Load())(ctx, network, address)
		},
	}
	if err := r.SetupWithManager(mgr, 1); err != nil {
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

func asTenantController(cfg *rest.Config) (*rest.Config, error) {
	const user = "lodia-tenant"
	ctx := context.Background()
	manifest, err := os.ReadFile(filepath.Join("..", "..", "..", "charts", "lodia", "files", "rbac", "tenant.yaml"))
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
			ResourceAttributes: &authorizationv1.ResourceAttributes{Group: lodiav1alpha1.GroupVersion.Group, Resource: "controlplanes", Verb: "watch"},
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

func TestTheFirstVisitWaitsForAccess(t *testing.T) {
	cp := controlPlane(t, "tenant-first-visit")
	eventually(t, 10*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess); err != nil {
			return err
		}
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	})
}

func TestTheAddonsConditionsStayAsTheyWereUntilAccessTakesUpAChange(t *testing.T) {
	cp, admin := servedTenant(t, "tenant-change")
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
			return err
		}
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending)
	})
	applied, available := *condition(t, cp, lodiav1alpha1.ConditionAddonsApplied), *condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable)

	setReplicas(t, cp, 3)
	written := cp.ResourceVersion
	time.Sleep(3 * time.Second)
	for _, before := range []metav1.Condition{applied, available} {
		if after := *condition(t, cp, before.Type); !equality.Semantic.DeepEqual(after, before) {
			t.Errorf("generation %d, which AccessReady does not describe yet, changed %s from %+v to %+v", cp.Generation, before.Type, before, after)
		}
	}
	if cp.ResourceVersion != written {
		t.Errorf("the ControlPlane was written while AccessReady describes generation %d of %d", condition(t, cp, lodiav1alpha1.ConditionAccessReady).ObservedGeneration, cp.Generation)
	}

	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
			return err
		}
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending); err != nil {
			return err
		}
		if replicas := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns").Spec.Replicas; replicas == nil || *replicas != 3 {
			return fmt.Errorf("CoreDNS has %v replicas", replicas)
		}
		return nil
	})

	applyStatus(t, cp, "test/"+lodiav1alpha1.ConditionAccessReady, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAPIServer, cp.Generation),
	}})
	eventually(t, 10*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess); err != nil {
			return err
		}
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	})
}

func TestOnlyAConditionAtTheGenerationWaitsForAccess(t *testing.T) {
	cp := controlPlane(t, "tenant-one-waits")
	eventually(t, 10*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	})
	setReplicas(t, cp, 3)
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation-1),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying, cp.Generation),
	}})
	applied := *condition(t, cp, lodiav1alpha1.ConditionAddonsApplied)

	applyStatus(t, cp, "test/"+lodiav1alpha1.ConditionAccessReady, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAPIServer, cp.Generation),
	}})
	eventually(t, 10*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess); err != nil {
			return err
		}
		if after := *condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); !equality.Semantic.DeepEqual(after, applied) {
			return fmt.Errorf("AddonsApplied went from %+v to %+v", applied, after)
		}
		return nil
	})
}

func TestAWaitOnAStaleReadIsDecidedFromTheServer(t *testing.T) {
	cp := controlPlane(t, "tenant-stale")
	eventually(t, 10*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	})
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	pass := func(at *lodiav1alpha1.ControlPlane, reader client.Reader) error {
		r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: at}, Reader: reader}
		_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		return err
	}
	written := cp.ResourceVersion
	if err := pass(cp.DeepCopy(), k8s); err != nil {
		t.Fatal(err)
	}
	if refresh(t, cp); cp.ResourceVersion != written {
		t.Errorf("a pass that keeps both conditions wrote the ControlPlane: resourceVersion %s, then %s", written, cp.ResourceVersion)
	}

	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation-1),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying, cp.Generation-1),
	}})
	applyStatus(t, cp, "test/"+lodiav1alpha1.ConditionAccessReady, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAPIServer, cp.Generation),
	}})
	refresh(t, cp)
	stale := cp.DeepCopy()
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying, cp.Generation),
	}})

	refresh(t, cp)
	written = cp.ResourceVersion
	if err := pass(stale, failingRead{Reader: k8s}); !errors.Is(err, errReadFailed) {
		t.Errorf("a pass that cannot read its conditions from the server returned %v, want the read's error", err)
	}
	if refresh(t, cp); cp.ResourceVersion != written {
		t.Errorf("a pass that cannot read its conditions from the server wrote the ControlPlane: resourceVersion %s, then %s", written, cp.ResourceVersion)
	}

	if err := pass(stale, k8s); err != nil {
		t.Fatal(err)
	}
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
}

func TestAConditionKeptWithoutAnAnswerOnAStaleReadIsDecidedFromTheServer(t *testing.T) {
	cp := controlPlane(t, "tenant-kept-stale")
	publish(t, cp)
	reconcilePKI(t, &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}, cp)
	eventually(t, 10*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	})
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation-1),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying, cp.Generation-1),
	}})
	refresh(t, cp)
	stale := cp.DeepCopy()
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable, cp.Generation),
	}})
	pass := func(reader client.Reader) error {
		r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: stale}, Reader: reader, ApplyPeriod: time.Hour, Dial: unreachable}
		_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		return err
	}

	refresh(t, cp)
	written := cp.ResourceVersion
	if err := pass(failingRead{Reader: k8s}); !errors.Is(err, errReadFailed) {
		t.Errorf("a pass that cannot read its conditions from the server returned %v, want the read's error", err)
	}
	if refresh(t, cp); cp.ResourceVersion != written {
		t.Errorf("a pass that cannot read its conditions from the server wrote the ControlPlane: resourceVersion %s, then %s", written, cp.ResourceVersion)
	}

	refresh(t, cp)
	written = cp.ResourceVersion
	if err := pass(k8s); err == nil {
		t.Fatal("a pass that could not reach the tenant succeeded")
	}
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
		t.Error(err)
	}
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable); err != nil {
		t.Error(err)
	}
	if cp.ResourceVersion != written {
		t.Errorf("a pass that keeps both conditions as the server holds them wrote the ControlPlane: resourceVersion %s, then %s", written, cp.ResourceVersion)
	}

	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, cp.Generation),
	}})
	if err := pass(k8s); err == nil {
		t.Fatal("a pass that could not reach the tenant succeeded")
	}
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
		t.Error(err)
	}
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying); err != nil {
		t.Error(err)
	}
}

func TestARolloutReadThatFailsOnAStaleReadIsDecidedFromTheServer(t *testing.T) {
	cp, admin := servedTenant(t, "tenant-rollout-stale")
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	ctx := t.Context()
	rebindOperator(t, admin, []rbacv1.PolicyRule{
		{APIGroups: []string{"", "discovery.k8s.io", rbacv1.GroupName}, Resources: []string{"*"}, Verbs: []string{"*"}},
		{APIGroups: []string{appsv1.GroupName}, Resources: []string{"deployments", "daemonsets"}, Verbs: []string{"create", "patch", "update", "delete", "list", "watch"}},
		{NonResourceURLs: []string{"*"}, Verbs: []string{"*"}},
	}, authorizationv1.ResourceAttributes{Group: appsv1.GroupName, Resource: "deployments", Verb: "get", Namespace: "kube-system"})

	refresh(t, cp)
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending, cp.Generation),
	}})
	refresh(t, cp)
	stale := cp.DeepCopy()
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation-1),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable, cp.Generation),
	}})
	pass := func(reader client.Reader) error {
		r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: stale}, Reader: reader, ApplyPeriod: time.Hour, Dial: *dial.Load()}
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		return err
	}

	refresh(t, cp)
	written := cp.ResourceVersion
	if err := pass(failingRead{Reader: k8s}); !errors.Is(err, errReadFailed) || !apierrors.IsForbidden(err) {
		t.Errorf("a pass whose rollout read and live read both failed returned %v, want both errors", err)
	}
	if refresh(t, cp); cp.ResourceVersion != written {
		t.Errorf("a pass that cannot read its conditions from the server wrote the ControlPlane: resourceVersion %s, then %s", written, cp.ResourceVersion)
	}

	if err := pass(k8s); !apierrors.IsForbidden(err) {
		t.Errorf("a pass whose rollout read was refused returned %v, want the refusal", err)
	}
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable); err != nil {
		t.Error(err)
	}
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
		t.Error(err)
	}

	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation-1),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying, cp.Generation-1),
	}})
	if err := pass(k8s); !apierrors.IsForbidden(err) {
		t.Errorf("a pass whose rollout read was refused returned %v, want the refusal", err)
	}
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
		t.Error(err)
	}
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused); err != nil {
		t.Error(err)
	}
}

func TestARefusedApplyStopsThePass(t *testing.T) {
	cp, admin := servedTenant(t, "tenant-refused-apply")
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	ctx := t.Context()
	rebindOperator(t, admin, []rbacv1.PolicyRule{
		{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"get", "list", "watch"}},
		{NonResourceURLs: []string{"*"}, Verbs: []string{"*"}},
	}, authorizationv1.ResourceAttributes{Resource: "configmaps", Verb: "patch", Namespace: "kube-system"})
	refresh(t, cp)
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation-1),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying, cp.Generation-1),
	}})
	refresh(t, cp)
	r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: cp.DeepCopy()}, Reader: k8s, ApplyPeriod: time.Hour, Dial: *dial.Load()}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !apierrors.IsForbidden(err) {
		t.Errorf("a pass whose apply was refused returned %v, want the refusal", err)
	}
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused); err != nil {
		t.Error(err)
	}
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused); err != nil {
		t.Error(err)
	}
}

func TestAnotherOwnersObjectKeepsApplyingThoughTheRolloutReadFails(t *testing.T) {
	ctx := t.Context()
	cp, admin := servedTenant(t, "tenant-owners-rollout")
	setCoreDNS(t, cp, false)
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	for _, obj := range []client.Object{
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns"}, Data: map[string]string{"Corefile": ".:53 { forward . 9.9.9.9 }"}},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns"},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "owners-dns"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "owners-dns"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "dns", Image: "owners/coredns"}}},
				},
			},
		},
	} {
		if err := admin.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	setCoreDNS(t, cp, true)
	const others = "ConfigMap kube-system/coredns exists and is not Lodia's; delete it or disable the addon"
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying)
	})
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	rebindOperator(t, admin, []rbacv1.PolicyRule{
		{APIGroups: []string{"", "discovery.k8s.io", rbacv1.GroupName}, Resources: []string{"*"}, Verbs: []string{"*"}},
		{APIGroups: []string{appsv1.GroupName}, Resources: []string{"deployments"}, Verbs: []string{"*"}},
		{APIGroups: []string{appsv1.GroupName}, Resources: []string{"daemonsets"}, Verbs: []string{"create", "patch", "update", "delete", "list", "watch"}},
		{NonResourceURLs: []string{"*"}, Verbs: []string{"*"}},
	}, authorizationv1.ResourceAttributes{Group: appsv1.GroupName, Resource: "daemonsets", Verb: "get", Namespace: "kube-system"})
	refresh(t, cp)
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation-1),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable, cp.Generation-1),
	}})
	refresh(t, cp)
	r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: cp.DeepCopy()}, Reader: k8s, ApplyPeriod: time.Hour, Dial: *dial.Load()}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !apierrors.IsForbidden(err) {
		t.Errorf("a pass whose rollout read was refused returned %v, want the refusal", err)
	}
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying); err != nil {
		t.Error(err)
	}
	if c := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); c.Message != others {
		t.Errorf("AddonsApplied says %q", c.Message)
	}
}

func TestAWaitComparesItsWriteWithTheServer(t *testing.T) {
	cp := controlPlane(t, "tenant-gate-compare")
	eventually(t, 10*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	})
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	waiting := func(conditionType string) map[string]any {
		return map[string]any{
			"type": conditionType, "status": string(metav1.ConditionFalse), "reason": lodiav1alpha1.ReasonWaitingForAccess,
			"message": "AccessReady is not True for this generation", "observedGeneration": cp.Generation,
			"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		}
	}
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		waiting(lodiav1alpha1.ConditionAddonsApplied), waiting(lodiav1alpha1.ConditionAddonsAvailable),
	}})
	applyStatus(t, cp, "test/"+lodiav1alpha1.ConditionAccessReady, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding, cp.Generation),
	}})
	refresh(t, cp)
	stale := cp.DeepCopy()
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable, cp.Generation),
	}})
	r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: stale}, Reader: k8s}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	refresh(t, cp)
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
}

func TestAFailedSyncComparesItsWriteWithTheServer(t *testing.T) {
	cp := controlPlane(t, "tenant-sync-compare")
	publish(t, cp)
	reconcilePKI(t, &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}, cp)
	eventually(t, 10*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	})
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	applying := func(conditionType string) map[string]any {
		return map[string]any{
			"type": conditionType, "status": string(metav1.ConditionFalse), "reason": lodiav1alpha1.ReasonApplying,
			"message": "the tenant's API server does not answer; the error is in Lodia's log", "observedGeneration": cp.Generation,
			"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		}
	}
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		applying(lodiav1alpha1.ConditionAddonsApplied), applying(lodiav1alpha1.ConditionAddonsAvailable),
	}})
	refresh(t, cp)
	stale := cp.DeepCopy()
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, cp.Generation),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, cp.Generation),
	}})
	r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: stale}, Reader: k8s, ApplyPeriod: time.Hour, Dial: unreachable}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Fatal("a pass that could not reach the tenant succeeded")
	}
	refresh(t, cp)
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying)
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying)
}

type snapshot struct {
	client.Client
	cp *lodiav1alpha1.ControlPlane
}

func (c snapshot) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
		c.cp.DeepCopyInto(cp)
		cp.Spec.Paused = false
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

var errReadFailed = errors.New("injected: the API server did not answer")

type failingRead struct {
	client.Reader
}

func (r failingRead) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
		return errReadFailed
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestTheTenantGetsItsObjects(t *testing.T) {
	ctx := t.Context()
	cp := controlPlane(t, "tenant-objects")
	publish(t, cp)
	issuer := &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}
	reconcilePKI(t, issuer, cp)
	admin := startTenant(t, cp)
	setDial(t, admin.host)
	issuer.Dial = *dial.Load()
	setCondition(t, cp, lodiav1alpha1.ConditionAPIServerReady, "Ready")
	reconcilePKI(t, issuer, cp)
	expectNow(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionTrue, lodiav1alpha1.ReasonBound)

	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending)

	info := get(t, admin, &corev1.ConfigMap{}, "kube-public", "cluster-info")
	kubeconfig, err := clientcmd.Load([]byte(info.Data["kubeconfig"]))
	if err != nil {
		t.Fatal(err)
	}
	bundle := &corev1.Secret{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: "dev-ca-bundle"}, bundle); err != nil {
		t.Fatal(err)
	}
	if cluster := kubeconfig.Clusters[""]; cluster == nil || cluster.Server != "https://203.0.113.10:443" || !bytes.Equal(cluster.CertificateAuthorityData, bundle.Data["ca.crt"]) {
		t.Errorf("cluster-info names %+v", kubeconfig.Clusters)
	}
	var clusterConfiguration map[string]any
	if err := yaml.Unmarshal([]byte(get(t, admin, &corev1.ConfigMap{}, "kube-system", "kubeadm-config").Data["ClusterConfiguration"]), &clusterConfiguration); err != nil {
		t.Fatal(err)
	}
	if clusterConfiguration["controlPlaneEndpoint"] != "203.0.113.10:443" || clusterConfiguration["kubernetesVersion"] != version {
		t.Errorf("kubeadm-config is %v", clusterConfiguration)
	}
	if !strings.Contains(get(t, admin, &corev1.ConfigMap{}, "kube-system", "kubelet-config").Data["kubelet"], `- "10.96.0.10"`) {
		t.Error("kubelet-config does not name CoreDNS at the tenth service address")
	}
	if dns := get(t, admin, &corev1.Service{}, "kube-system", "kube-dns"); dns.Spec.ClusterIP != "10.96.0.10" {
		t.Errorf("kube-dns has the ClusterIP %s", dns.Spec.ClusterIP)
	}
	if image := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns").Spec.Template.Spec.Containers[0].Image; image != "registry.k8s.io/coredns/coredns:v1.13.1" {
		t.Errorf("CoreDNS runs %s", image)
	}
	if image := get(t, admin, &appsv1.DaemonSet{}, "kube-system", "kube-proxy").Spec.Template.Spec.Containers[0].Image; image != "registry.k8s.io/kube-proxy:"+version {
		t.Errorf("kube-proxy runs %s", image)
	}
	agent := get(t, admin, &appsv1.DaemonSet{}, "kube-system", "konnectivity-agent").Spec.Template.Spec.Containers[0]
	for _, arg := range []string{"--proxy-server-host=203.0.113.10", "--proxy-server-port=8132", "--count-server-leases", "--keepalive-time=30s", "--agent-identifiers=ipv4=$(HOST_IP)"} {
		if !slices.Contains(agent.Command, arg) {
			t.Errorf("the konnectivity agent runs without %s: %v", arg, agent.Command)
		}
	}
	if slices.Contains(agent.Command, "--sync-forever") {
		t.Errorf("the konnectivity agent dials on every sync: %v", agent.Command)
	}
	inventory := inventoryOf(t, admin)
	if !slices.Contains(inventory, "ConfigMap kube-public/cluster-info") || !slices.Contains(inventory, "Deployment kube-system/coredns") {
		t.Errorf("the inventory lists %v", inventory)
	}
	for _, entry := range inventory {
		obj := objectOf(t, admin, entry)
		if obj.GetLabels()["app.kubernetes.io/managed-by"] != "lodia" || !slices.ContainsFunc(obj.GetManagedFields(), func(f metav1.ManagedFieldsEntry) bool {
			return f.Manager == tenant.FieldManager && f.Operation == metav1.ManagedFieldsOperationApply
		}) {
			t.Errorf("%s is not applied as %s with the managed-by label", entry, tenant.FieldManager)
		}
	}

	setCoreDNS(t, cp, false)
	eventually(t, 30*time.Second, func() error {
		if inventory := inventoryOf(t, admin); slices.ContainsFunc(inventory, func(e string) bool { return strings.Contains(e, "coredns") || strings.Contains(e, "kube-dns") }) {
			return fmt.Errorf("the inventory still lists CoreDNS: %v", inventory)
		}
		return nil
	})
	for _, gone := range []string{"Deployment kube-system/coredns", "Service kube-system/kube-dns", "ConfigMap kube-system/coredns", "ServiceAccount kube-system/coredns", "ClusterRole system:coredns", "ClusterRoleBinding system:coredns"} {
		if exists(t, admin, gone) {
			t.Errorf("%s survived disabling CoreDNS", gone)
		}
	}
	setCoreDNS(t, cp, true)
	eventually(t, 30*time.Second, func() error {
		if !exists(t, admin, "Deployment kube-system/coredns") || !exists(t, admin, "Service kube-system/kube-dns") {
			return errors.New("CoreDNS is not back")
		}
		return nil
	})

	if err := admin.Delete(ctx, &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "kube-proxy"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		if !exists(t, admin, "DaemonSet kube-system/kube-proxy") {
			return errors.New("the deleted kube-proxy DaemonSet is not back")
		}
		return nil
	})

	for _, name := range []string{"kube-proxy", "konnectivity-agent"} {
		ds := get(t, admin, &appsv1.DaemonSet{}, "kube-system", name)
		ds.Status = appsv1.DaemonSetStatus{ObservedGeneration: ds.Generation}
		if err := admin.Status().Update(ctx, ds); err != nil {
			t.Fatal(err)
		}
	}
	coredns := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns")
	coredns.Status = appsv1.DeploymentStatus{ObservedGeneration: coredns.Generation, Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2}
	if err := admin.Status().Update(ctx, coredns); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable)
	})

	setInventory(t, admin, "not a list")
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying); err != nil {
			return err
		}
		if c := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); !strings.Contains(c.Message, "ConfigMap kube-system/lodia-inventory cannot be read") {
			return fmt.Errorf("AddonsApplied says %q", c.Message)
		}
		return nil
	})
	if err := admin.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "lodia-inventory"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
}

func setInventory(t *testing.T, c client.Client, objects string) {
	t.Helper()
	inventory := get(t, c, &corev1.ConfigMap{}, "kube-system", "lodia-inventory")
	inventory.Data["objects"] = objects
	if err := c.Update(t.Context(), inventory); err != nil {
		t.Fatal(err)
	}
}

func TestTheKubernetesEndpointsAreTheInClusterEndpoint(t *testing.T) {
	cp, admin := servedTenant(t, "tenant-endpoints")
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
			return err
		}
		return kubernetesEndpoints(t, admin, inClusterPort)
	})
	inventory := inventoryOf(t, admin)
	for _, entry := range []string{"Endpoints default/kubernetes", "EndpointSlice default/kubernetes"} {
		if !slices.Contains(inventory, entry) {
			t.Errorf("the inventory does not list %s: %v", entry, inventory)
		}
	}

	applyStatus(t, cp, "test/endpoint", map[string]any{"endpoint": map[string]any{
		"apiServer":    map[string]any{"host": endpointHost, "port": int64(apiServerPort)},
		"konnectivity": map[string]any{"host": endpointHost, "port": int64(konnectivityPort)},
		"inCluster":    map[string]any{"host": endpointHost, "port": int64(31443)},
	}})
	eventually(t, 30*time.Second, func() error {
		return kubernetesEndpoints(t, admin, 31443)
	})
}

func kubernetesEndpoints(t *testing.T, c client.Client, port int32) error {
	t.Helper()
	key := client.ObjectKey{Namespace: "default", Name: "kubernetes"}
	endpoints, slice := &unstructured.Unstructured{}, &discoveryv1.EndpointSlice{}
	endpoints.SetAPIVersion("v1")
	endpoints.SetKind("Endpoints")
	if err := c.Get(t.Context(), key, endpoints); err != nil {
		return err
	}
	if err := c.Get(t.Context(), key, slice); err != nil {
		return err
	}
	for _, obj := range []client.Object{endpoints, slice} {
		if !slices.ContainsFunc(obj.GetManagedFields(), func(f metav1.ManagedFieldsEntry) bool {
			return f.Manager == tenant.FieldManager && f.Operation == metav1.ManagedFieldsOperationApply
		}) {
			return fmt.Errorf("%T default/kubernetes is not applied as %s", obj, tenant.FieldManager)
		}
	}
	subsets, err := json.Marshal(endpoints.Object["subsets"])
	if want := fmt.Sprintf(`[{"addresses":[{"ip":%q}],"ports":[{"name":"https","port":%d,"protocol":"TCP"}]}]`, endpointHost, port); err != nil || string(subsets) != want ||
		endpoints.GetLabels()[discoveryv1.LabelSkipMirror] != "true" {
		return fmt.Errorf("the Endpoints' subsets are %s labelled %v, want %s labelled %s", subsets, endpoints.GetLabels(), want, discoveryv1.LabelSkipMirror)
	}
	if len(slice.Endpoints) != 1 || !slices.Equal(slice.Endpoints[0].Addresses, []string{endpointHost}) || slice.Endpoints[0].Conditions.Ready == nil || !*slice.Endpoints[0].Conditions.Ready ||
		len(slice.Ports) != 1 || *slice.Ports[0].Name != "https" || *slice.Ports[0].Port != port || *slice.Ports[0].Protocol != corev1.ProtocolTCP ||
		slice.AddressType != discoveryv1.AddressTypeIPv4 ||
		slice.Labels[discoveryv1.LabelServiceName] != "kubernetes" || slice.Labels[discoveryv1.LabelManagedBy] != "lodia.io" {
		return fmt.Errorf("the EndpointSlice is %s %+v %+v labelled %v, want %s:%d named https, managed by lodia.io", slice.AddressType, slice.Endpoints, slice.Ports, slice.Labels, endpointHost, port)
	}
	return nil
}

func TestKubeProxyTakesConntrackMaxPerCore(t *testing.T) {
	cp, admin := servedTenant(t, "tenant-conntrack")
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	if conntrack := kubeProxyConfig(t, admin).Conntrack; conntrack != nil {
		t.Errorf("kube-proxy's configuration sets conntrack %+v without addons.kubeProxy.conntrack", *conntrack)
	}

	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.Addons = &lodiav1alpha1.Addons{KubeProxy: &lodiav1alpha1.KubeProxy{Conntrack: &lodiav1alpha1.KubeProxyConntrack{MaxPerCore: new(int32(0))}}}
	})
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		conntrack := kubeProxyConfig(t, admin).Conntrack
		if conntrack == nil || conntrack.MaxPerCore == nil || *conntrack.MaxPerCore != 0 {
			return fmt.Errorf("kube-proxy's configuration sets conntrack %+v, want maxPerCore 0", conntrack)
		}
		return nil
	})
}

type kubeProxyConfiguration struct {
	Conntrack *struct {
		MaxPerCore *int32 `json:"maxPerCore"`
	} `json:"conntrack"`
}

func kubeProxyConfig(t *testing.T, c client.Client) kubeProxyConfiguration {
	t.Helper()
	var config kubeProxyConfiguration
	if err := yaml.Unmarshal([]byte(get(t, c, &corev1.ConfigMap{}, "kube-system", "kube-proxy").Data["config.conf"]), &config); err != nil {
		t.Fatal(err)
	}
	return config
}

func TestAnObjectTheTenantRefusesIsReportedAndNeverLeaks(t *testing.T) {
	ctx := t.Context()
	cp := controlPlane(t, "tenant-refused")
	publish(t, cp)
	issuer := &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}
	reconcilePKI(t, issuer, cp)
	admin := startTenant(t, cp)
	setDial(t, admin.host)
	issuer.Dial = *dial.Load()
	setCondition(t, cp, lodiav1alpha1.ConditionAPIServerReady, "Ready")
	reconcilePKI(t, issuer, cp)
	setCoreDNS(t, cp, false)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})

	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "no-coredns"},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			MatchConstraints: &admissionregistrationv1.MatchResources{ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
				RuleWithOperations: admissionregistrationv1.RuleWithOperations{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
					Rule:       admissionregistrationv1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1"}, Resources: []string{"deployments"}},
				},
			}}},
			Validations: []admissionregistrationv1.Validation{{Expression: "object.metadata.name != 'coredns'", Message: "no CoreDNS here"}},
		},
	}
	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "no-coredns"},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        policy.Name,
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
		},
	}
	for _, obj := range []client.Object{policy, binding} {
		if err := admin.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 30*time.Second, func() error {
		err := admin.Create(ctx, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "coredns"},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "probe"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "probe"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "probe", Image: "probe"}}},
				},
			},
		}, client.DryRunAll)
		if err == nil {
			return errors.New("the policy does not refuse a coredns Deployment yet")
		}
		return nil
	})

	setCoreDNS(t, cp, true)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused); err != nil {
			return err
		}
		if c := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); !strings.Contains(c.Message, "Deployment kube-system/coredns is refused") {
			return fmt.Errorf("AddonsApplied says %q", c.Message)
		}
		return nil
	})
	if !exists(t, admin, "ConfigMap kube-system/coredns") || !slices.Contains(inventoryOf(t, admin), "ConfigMap kube-system/coredns") {
		t.Fatal("the objects before the refused one were not applied, or not recorded before they were")
	}

	setCoreDNS(t, cp, false)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	for _, gone := range []string{"ConfigMap kube-system/coredns", "ServiceAccount kube-system/coredns", "ClusterRole system:coredns"} {
		if exists(t, admin, gone) {
			t.Errorf("%s, applied by the pass the tenant refused, leaked", gone)
		}
	}
}

func TestATenantThatDoesNotAnswerKeepsATrueAddonsApplied(t *testing.T) {
	cp := controlPlane(t, "tenant-unanswered")
	publish(t, cp)
	issuer := &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}
	reconcilePKI(t, issuer, cp)
	admin := startTenant(t, cp)
	setDial(t, admin.host)
	issuer.Dial = *dial.Load()
	setCondition(t, cp, lodiav1alpha1.ConditionAPIServerReady, "Ready")
	reconcilePKI(t, issuer, cp)
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	before := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied)

	dial.Store(&unreachable)
	time.Sleep(5 * time.Second)
	if after := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); after.Status != before.Status || after.Reason != before.Reason || after.Message != before.Message || after.ObservedGeneration != before.ObservedGeneration {
		t.Fatalf("passes that could not reach the tenant changed AddonsApplied from %+v to %+v", before, after)
	}

	setDial(t, admin.host)
	setReplicas(t, cp, 3)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
			return err
		}
		if replicas := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns").Spec.Replicas; replicas == nil || *replicas != 3 {
			return fmt.Errorf("CoreDNS has %v replicas", replicas)
		}
		return nil
	})
}

func TestATenantThatDoesNotAnswerAtANewGenerationKeepsWhatItDescribed(t *testing.T) {
	cp := controlPlane(t, "tenant-unanswered-generation")
	publish(t, cp)
	issuer := &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}
	reconcilePKI(t, issuer, cp)
	admin := startTenant(t, cp)
	setDial(t, admin.host)
	issuer.Dial = *dial.Load()
	setCondition(t, cp, lodiav1alpha1.ConditionAPIServerReady, "Ready")
	reconcilePKI(t, issuer, cp)
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
			return err
		}
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending)
	})
	applied, available := *condition(t, cp, lodiav1alpha1.ConditionAddonsApplied), *condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable)

	dial.Store(&unreachable)
	setReplicas(t, cp, 2)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	r := &tenant.Reconciler{Client: k8s, Reader: k8s, ApplyPeriod: time.Hour, Dial: unreachable}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err == nil {
		t.Fatal("a pass that could not reach the tenant succeeded")
	}
	for _, before := range []metav1.Condition{applied, available} {
		after := *condition(t, cp, before.Type)
		if after.Status != before.Status || after.Reason != before.Reason || after.Message != before.Message || after.ObservedGeneration != before.ObservedGeneration {
			t.Errorf("a tenant that does not answer at generation %d changed %s from %+v to %+v", cp.Generation, before.Type, before, after)
		}
	}
}

func TestATenantThatNeverAnsweredIsApplying(t *testing.T) {
	cp := controlPlane(t, "tenant-never-answered")
	publish(t, cp)
	reconcilePKI(t, &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}, cp)
	dial.Store(&unreachable)
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	})
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		for _, conditionType := range []string{lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ConditionAddonsAvailable} {
			if err := expectCondition(t, cp, conditionType, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying); err != nil {
				return err
			}
			if c := condition(t, cp, conditionType); !strings.Contains(c.Message, "does not answer") {
				return fmt.Errorf("%s says %q", conditionType, c.Message)
			}
		}
		return nil
	})
}

func TestASecretAnotherOwnerKeepsIsNotUsed(t *testing.T) {
	cp := controlPlane(t, "tenant-predecessor")
	publish(t, cp)
	reconcilePKI(t, &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}, cp)
	dial.Store(&unreachable)
	bundle := get(t, k8s, &corev1.Secret{}, cp.Namespace, "dev-ca-bundle")
	own := bundle.OwnerReferences
	bundle.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "dev", UID: "predecessor", Controller: new(true),
	}}
	if err := k8s.Update(t.Context(), bundle); err != nil {
		t.Fatal(err)
	}
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess); err != nil {
			return err
		}
		if c := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); !strings.Contains(c.Message, "Secret dev-ca-bundle is kept by ControlPlane dev with UID predecessor") {
			return fmt.Errorf("AddonsApplied says %q", c.Message)
		}
		return nil
	})

	bundle.OwnerReferences = own
	if err := k8s.Update(t.Context(), bundle); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying)
	})
}

func TestThePruneDeletesOnlyWhatLodiaApplied(t *testing.T) {
	ctx := t.Context()
	cp, admin := servedTenant(t, "tenant-prune")
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	if err := admin.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "precious"}}); err != nil {
		t.Fatal(err)
	}
	if err := admin.Apply(ctx, rbacv1ac.ClusterRole("left-the-render").WithLabels(map[string]string{lodiav1alpha1.ManagedByLabel: lodiav1alpha1.ManagedBy}), client.FieldOwner(tenant.FieldManager)); err != nil {
		t.Fatal(err)
	}
	var entries []map[string]string
	if err := json.Unmarshal([]byte(get(t, admin, &corev1.ConfigMap{}, "kube-system", "lodia-inventory").Data["objects"]), &entries); err != nil {
		t.Fatal(err)
	}
	added := []map[string]string{
		{"group": "", "version": "v1", "kind": "ConfigMap", "namespace": "default", "name": "precious"},
		{"group": "rbac.authorization.k8s.io", "version": "v1", "kind": "ClusterRoleBinding", "name": "lodia:cluster-admins"},
		{"group": "rbac.authorization.k8s.io", "version": "v1beta1", "kind": "ClusterRole", "name": "left-the-render"},
		{"group": "example.com", "version": "v1", "kind": "Widget", "namespace": "default", "name": "w"},
	}
	objects, err := json.Marshal(append(entries, added...))
	if err != nil {
		t.Fatal(err)
	}
	setInventory(t, admin, string(objects))

	eventually(t, 30*time.Second, func() error {
		inventory := inventoryOf(t, admin)
		for _, entry := range []string{"ConfigMap default/precious", "ClusterRoleBinding lodia:cluster-admins", "ClusterRole left-the-render", "Widget default/w"} {
			if slices.Contains(inventory, entry) {
				return fmt.Errorf("the inventory still lists %s", entry)
			}
		}
		return nil
	})
	if exists(t, admin, "ClusterRole left-the-render") {
		t.Error("ClusterRole left-the-render, applied by Lodia and listed at a version no longer served, survived the prune")
	}
	for _, kept := range []string{"ConfigMap default/precious", "ClusterRoleBinding lodia:cluster-admins"} {
		if !exists(t, admin, kept) {
			t.Errorf("%s, which Lodia never applied, was deleted", kept)
		}
	}
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
}

func TestAnotherOwnersObjectsAreNeitherAdoptedNorPruned(t *testing.T) {
	ctx := t.Context()
	cp, admin := servedTenant(t, "tenant-owners-dns")
	setCoreDNS(t, cp, false)
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	corefile := ".:53 {\n    forward . 9.9.9.9\n}\n"
	for _, obj := range []client.Object{
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns"}, Data: map[string]string{"Corefile": corefile}},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns"},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "owners-dns"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "owners-dns"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "dns", Image: "owners/coredns"}}},
				},
			},
		},
	} {
		if err := admin.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}

	setCoreDNS(t, cp, true)
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying); err != nil {
			return err
		}
		if c := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); c.Message != "ConfigMap kube-system/coredns exists and is not Lodia's; delete it or disable the addon" {
			return fmt.Errorf("AddonsApplied says %q", c.Message)
		}
		return nil
	})
	owners := []string{"ConfigMap kube-system/coredns", "Deployment kube-system/coredns"}
	inventory := inventoryOf(t, admin)
	for _, entry := range owners {
		if slices.Contains(inventory, entry) {
			t.Errorf("the inventory lists the owner's %s", entry)
		}
		if slices.ContainsFunc(objectOf(t, admin, entry).GetManagedFields(), func(f metav1.ManagedFieldsEntry) bool { return f.Manager == tenant.FieldManager }) {
			t.Errorf("Lodia wrote the owner's %s", entry)
		}
	}
	if got := get(t, admin, &corev1.ConfigMap{}, "kube-system", "coredns").Data["Corefile"]; got != corefile {
		t.Errorf("the owner's Corefile is %q", got)
	}
	if !exists(t, admin, "Service kube-system/kube-dns") {
		t.Error("Lodia's own CoreDNS objects were not applied beside the owner's")
	}

	setCoreDNS(t, cp, false)
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	for _, entry := range owners {
		if !exists(t, admin, entry) {
			t.Errorf("the owner's %s was deleted", entry)
		}
	}
}

func TestAnInventoryReadTheTenantRefusesIsReported(t *testing.T) {
	cp, admin := servedTenant(t, "tenant-refuses-inventory")
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	if err := admin.Delete(t.Context(), &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "lodia:operator"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused); err != nil {
			return err
		}
		if c := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); !strings.HasPrefix(c.Message, "ConfigMap kube-system/lodia-inventory is refused by the tenant's API server") {
			return fmt.Errorf("AddonsApplied says %q", c.Message)
		}
		return nil
	})
}

func rebindOperator(t *testing.T, admin *tenantClient, rules []rbacv1.PolicyRule, denied authorizationv1.ResourceAttributes) {
	t.Helper()
	ctx := t.Context()
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "lodia:operator-restricted"}, Rules: rules}
	if err := admin.Create(ctx, role); err != nil {
		t.Fatal(err)
	}
	if err := admin.Delete(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "lodia:operator"}}); err != nil {
		t.Fatal(err)
	}
	if err := admin.Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "lodia:operator"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: "lodia:operator"}},
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{User: "lodia:operator", ResourceAttributes: &denied}}
		if err := admin.Create(ctx, review); err != nil {
			return err
		}
		if review.Status.Allowed {
			return fmt.Errorf("lodia:operator may still %s %s", denied.Verb, denied.Resource)
		}
		return nil
	})
}

func servedTenant(t *testing.T, namespace string) (*lodiav1alpha1.ControlPlane, *tenantClient) {
	t.Helper()
	cp := controlPlane(t, namespace)
	publish(t, cp)
	issuer := &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}
	reconcilePKI(t, issuer, cp)
	admin := startTenant(t, cp)
	setDial(t, admin.host)
	issuer.Dial = *dial.Load()
	setCondition(t, cp, lodiav1alpha1.ConditionAPIServerReady, "Ready")
	reconcilePKI(t, issuer, cp)
	return cp, admin
}

type tenantClient struct {
	client.Client
	host string
}

func startTenant(t *testing.T, cp *lodiav1alpha1.ControlPlane) *tenantClient {
	t.Helper()
	secrets := map[string]*corev1.Secret{}
	var list corev1.SecretList
	if err := k8s.List(t.Context(), &list, client.InNamespace(cp.Namespace)); err != nil {
		t.Fatal(err)
	}
	for i := range list.Items {
		secrets[list.Items[i].Name] = &list.Items[i]
	}
	dir := t.TempDir()
	file := func(secret, key string) string {
		t.Helper()
		path := filepath.Join(dir, secret+"-"+key)
		if err := os.WriteFile(path, secrets[secret].Data[key], 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	clientCA, err := certs.Parse(secrets["dev-cakey-client"].Data["tls.crt"], secrets["dev-cakey-client"].Data["tls.key"])
	if err != nil {
		t.Fatal(err)
	}
	env := &envtest.Environment{UseExistingCluster: new(false)}
	api := env.ControlPlane.GetAPIServer()
	api.Authn = &tenantAuthn{api: api, clientCA: clientCA, clusterCA: secrets["dev-ca-bundle"].Data["ca.crt"], bundle: file("dev-client-ca", "ca.crt")}
	saKey := file("dev-sa-keys", "signing.key")
	api.Configure().
		Set("tls-cert-file", file("dev-apiserver-serving", "tls.crt")).
		Set("tls-private-key-file", file("dev-apiserver-serving", "tls.key")).
		Set("service-account-signing-key-file", saKey).
		Set("service-account-key-file", saKey).
		Set("service-cluster-ip-range", string(cp.Spec.Network.ServiceCIDRs[0])).
		Set("endpoint-reconciler-type", "none")
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return &tenantClient{Client: admin, host: cfg.Host}
}

type tenantAuthn struct {
	api       *envtest.APIServer
	clientCA  *certs.Certificate
	clusterCA []byte
	bundle    string
}

func (a *tenantAuthn) Configure(_ string, args *envtest.Arguments) error {
	args.Set("client-ca-file", a.bundle)
	return nil
}

func (a *tenantAuthn) Start() error {
	a.api.CA = a.clusterCA
	return nil
}

func (a *tenantAuthn) Stop() error {
	return nil
}

func (a *tenantAuthn) AddUser(user envtest.User, base *rest.Config) (*rest.Config, error) {
	c, err := a.clientCA.Issue(certs.Profile{CommonName: user.Name, Organizations: user.Groups, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, time.Now(), time.Hour)
	if err != nil {
		return nil, err
	}
	out := rest.CopyConfig(base)
	if out.CertData, err = certs.CertPEM(c.Cert); err != nil {
		return nil, err
	}
	out.KeyData, err = certs.KeyPEM(c.Key)
	return out, err
}

func setDial(t *testing.T, host string) {
	t.Helper()
	u, err := url.Parse(host)
	if err != nil {
		t.Fatal(err)
	}
	to := func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, u.Host)
	}
	dial.Store(&to)
}

func controlPlane(t *testing.T, namespace string) *lodiav1alpha1.ControlPlane {
	t.Helper()
	if k8s == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set; run make test")
	}
	if err := k8s.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: namespace},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   version,
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

func reconcilePKI(t *testing.T, r *pki.Reconciler, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	refresh(t, cp)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
}

func applyStatus(t *testing.T, cp *lodiav1alpha1.ControlPlane, manager string, status map[string]any) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     status,
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(manager), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func publish(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	applyStatus(t, cp, "test/target", map[string]any{"target": map[string]any{"version": version}})
	applyStatus(t, cp, "test/endpoint", map[string]any{"endpoint": map[string]any{
		"apiServer":    map[string]any{"host": endpointHost, "port": int64(apiServerPort)},
		"konnectivity": map[string]any{"host": endpointHost, "port": int64(konnectivityPort)},
		"inCluster":    map[string]any{"host": endpointHost, "port": int64(inClusterPort)},
	}})
}

func setCondition(t *testing.T, cp *lodiav1alpha1.ControlPlane, conditionType, reason string) {
	t.Helper()
	refresh(t, cp)
	applyStatus(t, cp, "test/"+conditionType, map[string]any{"conditions": []any{conditionAt(conditionType, metav1.ConditionTrue, reason, cp.Generation)}})
}

func conditionAt(conditionType string, status metav1.ConditionStatus, reason string, generation int64) map[string]any {
	return map[string]any{
		"type":               conditionType,
		"status":             string(status),
		"reason":             reason,
		"message":            "",
		"observedGeneration": generation,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}
}

func setCoreDNS(t *testing.T, cp *lodiav1alpha1.ControlPlane, enabled bool) {
	t.Helper()
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) {
		if cp.Spec.Addons == nil {
			cp.Spec.Addons = &lodiav1alpha1.Addons{}
		}
		if cp.Spec.Addons.CoreDNS == nil {
			cp.Spec.Addons.CoreDNS = &lodiav1alpha1.CoreDNS{}
		}
		cp.Spec.Addons.CoreDNS.Enabled = &enabled
	})
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
}

func setReplicas(t *testing.T, cp *lodiav1alpha1.ControlPlane, replicas int32) {
	t.Helper()
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) {
		if cp.Spec.Addons == nil {
			cp.Spec.Addons = &lodiav1alpha1.Addons{}
		}
		if cp.Spec.Addons.CoreDNS == nil {
			cp.Spec.Addons.CoreDNS = &lodiav1alpha1.CoreDNS{}
		}
		cp.Spec.Addons.CoreDNS.Replicas = &replicas
	})
}

func update(t *testing.T, cp *lodiav1alpha1.ControlPlane, change func(*lodiav1alpha1.ControlPlane)) {
	t.Helper()
	err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		refresh(t, cp)
		change(cp)
		err := k8s.Update(ctx, cp)
		return err == nil, client.IgnoreNotFound(ignoreConflict(err))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func ignoreConflict(err error) error {
	if apierrors.IsConflict(err) {
		return nil
	}
	return err
}

func refresh(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), cp); err != nil {
		t.Fatal(err)
	}
}

func condition(t *testing.T, cp *lodiav1alpha1.ControlPlane, conditionType string) *metav1.Condition {
	t.Helper()
	refresh(t, cp)
	c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
	if c == nil {
		return &metav1.Condition{Type: conditionType}
	}
	return c
}

func expectCondition(t *testing.T, cp *lodiav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason string) error {
	t.Helper()
	c := condition(t, cp, conditionType)
	if c.Status != status || c.Reason != reason || c.ObservedGeneration != cp.Generation {
		return fmt.Errorf("%s is %s %s at generation %d (%s), want %s %s at %d", conditionType, c.Status, c.Reason, c.ObservedGeneration, c.Message, status, reason, cp.Generation)
	}
	return nil
}

func expectNow(t *testing.T, cp *lodiav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	if err := expectCondition(t, cp, conditionType, status, reason); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, timeout time.Duration, check func() error) {
	t.Helper()
	var last error
	err := wait.PollUntilContextTimeout(t.Context(), 200*time.Millisecond, timeout, true, func(context.Context) (bool, error) {
		last = check()
		return last == nil, nil
	})
	if err != nil {
		t.Fatal(last)
	}
}

func get[T client.Object](t *testing.T, c client.Client, obj T, namespace, name string) T {
	t.Helper()
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
		t.Fatal(err)
	}
	return obj
}

func inventoryOf(t *testing.T, c client.Client) []string {
	t.Helper()
	var entries []struct{ Group, Version, Kind, Namespace, Name string }
	if err := json.Unmarshal([]byte(get(t, c, &corev1.ConfigMap{}, "kube-system", "lodia-inventory").Data["objects"]), &entries); err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Namespace == "" {
			out = append(out, e.Kind+" "+e.Name)
		} else {
			out = append(out, e.Kind+" "+e.Namespace+"/"+e.Name)
		}
	}
	return out
}

var kinds = map[string]string{
	"ConfigMap": "v1", "ServiceAccount": "v1", "Service": "v1", "Endpoints": "v1",
	"Deployment": "apps/v1", "DaemonSet": "apps/v1", "EndpointSlice": "discovery.k8s.io/v1",
	"ClusterRole": "rbac.authorization.k8s.io/v1", "ClusterRoleBinding": "rbac.authorization.k8s.io/v1",
	"Role": "rbac.authorization.k8s.io/v1", "RoleBinding": "rbac.authorization.k8s.io/v1",
}

func lookup(t *testing.T, c client.Client, entry string) (*unstructured.Unstructured, error) {
	t.Helper()
	kind, rest, _ := strings.Cut(entry, " ")
	namespace, name, namespaced := strings.Cut(rest, "/")
	if !namespaced {
		namespace, name = "", rest
	}
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(kinds[kind])
	obj.SetKind(kind)
	return obj, c.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: name}, obj)
}

func objectOf(t *testing.T, c client.Client, entry string) *unstructured.Unstructured {
	t.Helper()
	obj, err := lookup(t, c, entry)
	if err != nil {
		t.Fatal(err)
	}
	return obj
}

func exists(t *testing.T, c client.Client, entry string) bool {
	t.Helper()
	_, err := lookup(t, c, entry)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}
