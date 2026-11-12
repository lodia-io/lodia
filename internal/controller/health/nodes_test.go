// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/health"
	"go.lodia.io/lodia/internal/testenv"
)

const (
	leaseListPath  = "/apis/coordination.k8s.io/v1/namespaces/kube-node-lease/leases"
	nodeListPath   = "/api/v1/nodes"
	managerLeases  = "/apis/coordination.k8s.io/v1/namespaces/kube-system/leases"
	managerLease   = managerLeases + "/kube-controller-manager"
	fewerThanTwo   = "; fewer than 2 are counted"
	atOrAbove      = ", at or above "
	lodiaHoldsIt   = "; Lodia holds the controller manager's leader Lease"
	failedLeaseMsg = "the Leases of kube-node-lease could not be listed"
)

func silent(n, of int) string {
	return fmt.Sprintf("%d of %d counted nodes are silent", n, of)
}

func taken(n, of int, percent string) string {
	return silent(n, of) + atOrAbove + percent + "%" + lodiaHoldsIt
}

func nodesReconciler(now clock.PassiveClock) *health.NodesReconciler {
	return &health.NodesReconciler{
		Client: k8s, Reader: k8s, Period: 5 * time.Second, Timeout: 2 * time.Second,
		SilentAfter: 20 * time.Second, HoldFraction: 600, ReleaseAfter: 2 * time.Minute, MaxNodes: 5000, Clock: now,
	}
}

func tenantControlPlane(t *testing.T) *lodiav1alpha1.ControlPlane {
	t.Helper()
	clearNodes(t)
	cp := controlPlane(t)
	setAccess(t, cp)
	writeKubeconfig(t, cp, tenantKubeconfig)
	return cp
}

func sample(t *testing.T, r *health.NodesReconciler, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != r.Period {
		t.Errorf("requeue after %s, want the period %s", result.RequeueAfter, r.Period)
	}
}

func clearNodes(t *testing.T) {
	t.Helper()
	if err := tenant.DeleteAllOf(t.Context(), &corev1.Node{}); err != nil {
		t.Fatal(err)
	}
	if err := tenant.DeleteAllOf(t.Context(), &coordinationv1.Lease{}, client.InNamespace(corev1.NamespaceNodeLease)); err != nil {
		t.Fatal(err)
	}
	removeLeases(t)
}

func addNode(t *testing.T, name string, ready corev1.ConditionStatus, since time.Time, kubelet string) {
	t.Helper()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	create(t, tenant, node)
	if ready == "" {
		return
	}
	node.Status.NodeInfo.KubeletVersion = kubelet
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready, LastTransitionTime: metav1.NewTime(since)}}
	if err := tenant.Status().Update(t.Context(), node); err != nil {
		t.Fatal(err)
	}
}

func joined(t *testing.T, name string, ready corev1.ConditionStatus, since time.Time) {
	t.Helper()
	addNode(t, name, ready, since, "v1.35.0")
	renewNodeLease(t, name, time.Now())
}

func renewNodeLease(t *testing.T, name string, at time.Time) {
	t.Helper()
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: corev1.NamespaceNodeLease, Name: name}}
	err := tenant.Get(t.Context(), client.ObjectKeyFromObject(lease), lease)
	if client.IgnoreNotFound(err) != nil {
		t.Fatal(err)
	}
	lease.Spec = coordinationv1.LeaseSpec{HolderIdentity: &name, LeaseDurationSeconds: new(int32(40)), RenewTime: &metav1.MicroTime{Time: at}}
	if err == nil {
		err = tenant.Update(t.Context(), lease)
	} else {
		err = tenant.Create(t.Context(), lease)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func updateSpec(t *testing.T, cp *lodiav1alpha1.ControlPlane, change func(*lodiav1alpha1.ControlPlane)) {
	t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh := &lodiav1alpha1.ControlPlane{}
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
			return err
		}
		change(fresh)
		return k8s.Update(t.Context(), fresh)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func expectHeld(t *testing.T, cp *lodiav1alpha1.ControlPlane, status metav1.ConditionStatus, reason, message string) error {
	t.Helper()
	fresh := get(t, cp)
	c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionControllerManagerHeld)
	if c == nil || c.Status != status || c.Reason != reason || c.Message != message || c.ObservedGeneration != fresh.Generation {
		return fmt.Errorf("ControllerManagerHeld is %+v, want %s %s %q at generation %d", c, status, reason, message, fresh.Generation)
	}
	return nil
}

func expectNotHeld(t *testing.T, cp *lodiav1alpha1.ControlPlane, message string) {
	t.Helper()
	if err := expectHeld(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, message); err != nil {
		t.Error(err)
	}
}

func expectNodes(t *testing.T, cp *lodiav1alpha1.ControlPlane, want *lodiav1alpha1.NodesStatus) {
	t.Helper()
	if got := get(t, cp).Status.Nodes; !equality.Semantic.DeepEqual(got, want) {
		t.Errorf("status.nodes is %+v, want %+v", got, want)
	}
}

func nodesOf(total, ready, outdated int32, oldest string) *lodiav1alpha1.NodesStatus {
	return &lodiav1alpha1.NodesStatus{Count: lodiav1alpha1.Count{Total: total, Ready: ready}, OutdatedKubelets: outdated, OldestKubelet: oldest}
}

func throughProxy(t *testing.T, intercept func(http.ResponseWriter, *http.Request) bool) []byte {
	t.Helper()
	cfg, err := clientcmd.RESTConfigFromKubeConfig(tenantKubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := rest.TransportFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	target, err := url.Parse(cfg.Host)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = transport
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if intercept != nil && intercept(w, req) {
			return
		}
		forward.ServeHTTP(w, req)
	}))
	t.Cleanup(proxy.Close)
	kubeconfig, err := kubeconfigOf(proxy.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	return kubeconfig
}

func recordingApplies(c client.WithWatch) (client.WithWatch, func() []string) {
	var mu sync.Mutex
	var applied []string
	return interceptor.NewClient(c, interceptor.Funcs{
			SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
				raw, err := json.Marshal(obj)
				if err != nil {
					return err
				}
				mu.Lock()
				applied = append(applied, string(raw))
				mu.Unlock()
				return c.SubResource(subResource).Apply(ctx, obj, opts...)
			},
		}), func() []string {
			mu.Lock()
			defer mu.Unlock()
			return append([]string(nil), applied...)
		}
}

func TestOnlyAReadyNodeOrARecentlyUnknownOneWithALeaseCounts(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start.Add(-20 * time.Second))
	r := nodesReconciler(now)
	r.HoldFraction = 1000
	joined(t, "ready-silent", corev1.ConditionTrue, start)
	joined(t, "ready-renewed", corev1.ConditionTrue, start)
	joined(t, "unknown-recent", corev1.ConditionUnknown, start.Add(-5*time.Minute+time.Second))
	joined(t, "unknown-exactly-5-minutes", corev1.ConditionUnknown, start.Add(-5*time.Minute))
	joined(t, "unknown-old", corev1.ConditionUnknown, start.Add(-5*time.Minute-time.Second))
	joined(t, "not-ready", corev1.ConditionFalse, start)
	joined(t, "no-ready-condition", "", start)
	addNode(t, "no-lease", corev1.ConditionTrue, start, "v1.35.0")
	renewNodeLease(t, "no-node", time.Now())

	sample(t, r, cp)
	expectNotHeld(t, cp, silent(0, 5))

	renewNodeLease(t, "ready-renewed", time.Now())
	now.SetTime(start)
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(2, 3))
}

func TestALeaseIsSilentOnceItHasNotChangedForSilentAfterByTheOperatorsClock(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	r.HoldFraction = 1000
	for _, name := range []string{"a", "b", "c"} {
		joined(t, name, corev1.ConditionTrue, start)
	}
	for _, step := range []struct {
		at     time.Duration
		before func()
		want   string
	}{
		{0, nil, silent(0, 3)},
		{19 * time.Second, func() { renewNodeLease(t, "b", time.Now()) }, silent(0, 3)},
		{20 * time.Second, nil, silent(2, 3)},
		{25 * time.Second, func() { renewNodeLease(t, "a", time.Now()) }, silent(1, 3)},
		{38 * time.Second, nil, silent(1, 3)},
		{39 * time.Second, nil, silent(2, 3)},
		{70 * time.Second, func() {
			joined(t, "d", corev1.ConditionTrue, start)
			renewNodeLease(t, "d", start.Add(-time.Hour))
		}, silent(3, 4)},
	} {
		if step.before != nil {
			step.before()
		}
		now.SetTime(start.Add(step.at))
		sample(t, r, cp)
		if err := expectHeld(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, step.want); err != nil {
			t.Fatalf("%s after the first pass: %v", step.at, err)
		}
	}
}

func TestAFailedLeaseListRestartsTheClockOfEveryLease(t *testing.T) {
	cp := tenantControlPlane(t)
	var failing atomic.Bool
	writeKubeconfig(t, cp, throughProxy(t, func(w http.ResponseWriter, req *http.Request) bool {
		if failing.Load() && req.URL.Path == leaseListPath {
			http.Error(w, "injected", http.StatusInternalServerError)
			return true
		}
		return false
	}))
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	r.HoldFraction = 1000
	for _, name := range []string{"a", "b", "c"} {
		joined(t, name, corev1.ConditionTrue, start)
	}
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(0, 3))

	failing.Store(true)
	now.SetTime(start.Add(15 * time.Second))
	sample(t, r, cp)
	expectNotHeld(t, cp, failedLeaseMsg)

	failing.Store(false)
	now.SetTime(start.Add(25 * time.Second))
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(0, 3))
	now.SetTime(start.Add(44 * time.Second))
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(0, 3))
	now.SetTime(start.Add(45 * time.Second))
	sample(t, r, cp)
	if err := expectHeld(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent, taken(3, 3, "100")); err != nil {
		t.Error(err)
	}
}

func TestANodeNamedLikeALeaderLeaseSharesNoClockWithIt(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	nodes := nodesReconciler(now)
	nodes.HoldFraction = 1000
	leaders := &health.Reconciler{Client: k8s, Reader: k8s, FailuresBeforeFalse: failuresBeforeFalse, DialContext: dialService, Period: 5 * time.Second, Timeout: 2 * time.Second, Clock: now}
	joined(t, lodiav1alpha1.SchedulerLease, corev1.ConditionTrue, start)
	removeLeases(t)
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", start)
	renew(t, lodiav1alpha1.SchedulerLease, "dev-kube-scheduler-5c4_2", start)
	both := func(at time.Duration) {
		now.SetTime(start.Add(at))
		sample(t, nodes, cp)
		reconcile(t, leaders, cp)
	}

	both(0)
	renewNodeLease(t, lodiav1alpha1.SchedulerLease, time.Now())
	both(10 * time.Second)
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonLeading, "the leader Leases of kube-controller-manager and kube-scheduler are renewed"); err != nil {
		t.Fatal(err)
	}
	both(26 * time.Second)
	renewed := start.UTC().Format(time.RFC3339)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonControllerManagerNotLeading,
		"Lease kube-system/kube-controller-manager of dev-kube-controller-manager-7d9_1 was last renewed "+renewed+
			"; Lease kube-system/kube-scheduler of dev-kube-scheduler-5c4_2 was last renewed "+renewed); err != nil {
		t.Fatalf("the node Lease of the same name renewed 16 seconds before: %v", err)
	}

	renew(t, lodiav1alpha1.SchedulerLease, "dev-kube-scheduler-5c4_2", start.Add(27*time.Second))
	both(30 * time.Second)
	expectNotHeld(t, cp, silent(1, 1)+fewerThanTwo)
}

func TestTheShareOfSilentNodesIsComparedInThousandths(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		joined(t, name, corev1.ConditionTrue, start)
	}
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(0, 5))

	renewNodeLease(t, "a", time.Now())
	renewNodeLease(t, "b", time.Now())
	now.SetTime(start.Add(20 * time.Second))
	for _, c := range []struct {
		fraction int
		status   metav1.ConditionStatus
		reason   string
		want     string
	}{
		{600, metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent, taken(3, 5, "60")},
		{601, metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(3, 5)},
		{599, metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent, taken(3, 5, "59.9")},
		{1000, metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(3, 5)},
	} {
		removeLeases(t)
		r.HoldFraction = c.fraction
		sample(t, r, cp)
		if err := expectHeld(t, cp, c.status, c.reason, c.want); err != nil {
			t.Errorf("3 of 5 silent at %d thousandths: %v", c.fraction, err)
		}
	}

	removeLeases(t)
	r.HoldFraction = 600
	renewNodeLease(t, "c", time.Now())
	now.SetTime(start.Add(25 * time.Second))
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(2, 5))
}

func TestFewerThanTwoCountedNodesIsBelowTheFractionWhateverTheShare(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	r.HoldFraction = 500
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(0, 0)+fewerThanTwo)
	expectNodes(t, cp, nodesOf(0, 0, 0, ""))

	joined(t, "only", corev1.ConditionTrue, start)
	joined(t, "unready", corev1.ConditionFalse, start)
	now.SetTime(start.Add(30 * time.Second))
	sample(t, r, cp)
	now.SetTime(start.Add(60 * time.Second))
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(1, 1)+fewerThanTwo)

	joined(t, "second", corev1.ConditionTrue, start)
	now.SetTime(start.Add(90 * time.Second))
	sample(t, r, cp)
	if err := expectHeld(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent, taken(1, 2, "50")); err != nil {
		t.Error(err)
	}
}

func TestNodesAreListedOnceIn30SecondsAndLeasesEveryPassConsistently(t *testing.T) {
	cp := tenantControlPlane(t)
	var nodeLists, leaseLists atomic.Int32
	var inconsistent atomic.Value
	writeKubeconfig(t, cp, throughProxy(t, func(_ http.ResponseWriter, req *http.Request) bool {
		switch req.URL.Path {
		case nodeListPath:
			nodeLists.Add(1)
		case leaseListPath:
			leaseLists.Add(1)
			if q := req.URL.Query(); q.Has("resourceVersion") || q.Has("resourceVersionMatch") {
				inconsistent.Store(req.URL.RawQuery)
			}
		}
		return false
	}))
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	joined(t, "a", corev1.ConditionTrue, start)
	for _, step := range []struct {
		at                int
		nodeLists, leases int32
	}{{0, 1, 1}, {5, 1, 2}, {10, 1, 3}, {29, 1, 4}, {30, 2, 5}, {35, 2, 6}, {59, 2, 7}, {60, 3, 8}} {
		now.SetTime(start.Add(time.Duration(step.at) * time.Second))
		sample(t, r, cp)
		if nodeLists.Load() != step.nodeLists || leaseLists.Load() != step.leases {
			t.Fatalf("%ds after the first pass the Nodes were listed %d times and the Leases %d, want %d and %d", step.at, nodeLists.Load(), leaseLists.Load(), step.nodeLists, step.leases)
		}
	}
	if query := inconsistent.Load(); query != nil {
		t.Errorf("a list of the Leases read from the cache: %v", query)
	}
}

func TestAFailedListOfNodesLeavesTheLastGoodOne(t *testing.T) {
	cp := tenantControlPlane(t)
	var failing atomic.Bool
	writeKubeconfig(t, cp, throughProxy(t, func(w http.ResponseWriter, req *http.Request) bool {
		if failing.Load() && req.URL.Path == nodeListPath {
			http.Error(w, "injected", http.StatusInternalServerError)
			return true
		}
		return false
	}))
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	joined(t, "a", corev1.ConditionTrue, start)
	joined(t, "b", corev1.ConditionTrue, start)
	sample(t, r, cp)
	expectNodes(t, cp, nodesOf(2, 2, 0, "v1.35.0"))

	joined(t, "c", corev1.ConditionTrue, start)
	failing.Store(true)
	now.SetTime(start.Add(30 * time.Second))
	sample(t, r, cp)
	expectNodes(t, cp, nodesOf(2, 2, 0, "v1.35.0"))
	if err := expectHeld(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent, taken(2, 2, "60")); err != nil {
		t.Error(err)
	}

	failing.Store(false)
	now.SetTime(start.Add(31 * time.Second))
	sample(t, r, cp)
	expectNodes(t, cp, nodesOf(3, 3, 0, "v1.35.0"))
}

func TestStatusNodesCountsTheNodes(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	sample(t, r, cp)
	expectNodes(t, cp, nodesOf(0, 0, 0, ""))

	addNode(t, "a", corev1.ConditionTrue, start, "v1.35.10")
	addNode(t, "b", corev1.ConditionFalse, start, "v1.34.9")
	addNode(t, "c", corev1.ConditionTrue, start, "v1.35.2")
	addNode(t, "d", corev1.ConditionTrue, start, "not a version")
	now.SetTime(start.Add(30 * time.Second))
	sample(t, r, cp)
	expectNodes(t, cp, nodesOf(4, 3, 0, "v1.34.9"))

	kubernetes := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     map[string]any{"kubernetes": map[string]any{"version": "v1.35.1"}},
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(kubernetes), client.FieldOwner("test/lifecycle"), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
	now.SetTime(start.Add(35 * time.Second))
	sample(t, r, cp)
	expectNodes(t, cp, nodesOf(4, 3, 1, "v1.34.9"))

	clearNodes(t)
	now.SetTime(start.Add(65 * time.Second))
	sample(t, r, cp)
	expectNodes(t, cp, nodesOf(0, 0, 0, ""))
}

func TestASamplingPassThatChangesNothingWritesNothing(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	joined(t, "a", corev1.ConditionTrue, start)
	joined(t, "b", corev1.ConditionTrue, start)
	sample(t, r, cp)
	expectNodes(t, cp, nodesOf(2, 2, 0, "v1.35.0"))

	r.Client = noControlPlaneWrite(t, k8s)
	for _, at := range []int{5, 10, 30} {
		renewNodeLease(t, "a", time.Now())
		renewNodeLease(t, "b", time.Now())
		now.SetTime(start.Add(time.Duration(at) * time.Second))
		sample(t, r, cp)
	}
}

func TestAWaitForAccessLeavesStatusNodesAndRestartsTheClock(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	r.HoldFraction = 1000
	joined(t, "a", corev1.ConditionTrue, start)
	joined(t, "b", corev1.ConditionTrue, start)
	sample(t, r, cp)
	expectNodes(t, cp, nodesOf(2, 2, 0, "v1.35.0"))

	generation := get(t, cp).Generation
	setAccessAt(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding, generation)
	now.SetTime(start.Add(21 * time.Second))
	sample(t, r, cp)
	if err := expectHeld(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, "AccessReady is not True"); err != nil {
		t.Error(err)
	}
	expectNodes(t, cp, nodesOf(2, 2, 0, "v1.35.0"))

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name)}}
	if err := k8s.Delete(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	setAccessAt(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonBound, generation)
	sample(t, r, cp)
	if err := expectHeld(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, "waiting for PKI to write dev-lodia-kubeconfig"); err != nil {
		t.Error(err)
	}

	secret.OwnerReferences = []metav1.OwnerReference{{APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "predecessor", UID: "5e1f7c3a-0000-4000-8000-0000000000aa", Controller: new(true)}}
	secret.Data = map[string][]byte{lodiav1alpha1.KubeconfigKey: tenantKubeconfig}
	create(t, k8s, secret)
	sample(t, r, cp)
	if err := expectHeld(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess,
		"Secret dev-lodia-kubeconfig is kept by ControlPlane predecessor with UID 5e1f7c3a-0000-4000-8000-0000000000aa"); err != nil {
		t.Error(err)
	}
	expectNodes(t, cp, nodesOf(2, 2, 0, "v1.35.0"))

	if err := k8s.Delete(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	writeKubeconfig(t, cp, tenantKubeconfig)
	now.SetTime(start.Add(25 * time.Second))
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(0, 2))
	expectNodes(t, cp, nodesOf(2, 2, 0, "v1.35.0"))
}

func TestARestartWhoseListOfNodesFailsKeepsStatusNodes(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	joined(t, "a", corev1.ConditionTrue, start)
	joined(t, "b", corev1.ConditionTrue, start)
	joined(t, "c", corev1.ConditionFalse, start)
	sample(t, nodesReconciler(now), cp)
	expectNodes(t, cp, nodesOf(3, 2, 0, "v1.35.0"))
	expectNotHeld(t, cp, silent(0, 2))

	writeKubeconfig(t, cp, throughProxy(t, func(w http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path == nodeListPath {
			http.Error(w, "injected", http.StatusInternalServerError)
			return true
		}
		return false
	}))
	restarted := nodesReconciler(now)
	restarted.Client = interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if plane, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				plane.Status.Nodes = nil
			}
			return nil
		},
	})
	sample(t, restarted, cp)
	expectNotHeld(t, cp, "the nodes are not listed yet")
	expectNodes(t, cp, nodesOf(3, 2, 0, "v1.35.0"))
}

func TestAPassOnACacheHoldingAPredecessorLeavesTheSuccessorAlone(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	joined(t, "a", corev1.ConditionTrue, start)
	joined(t, "b", corev1.ConditionTrue, start)
	r := nodesReconciler(clocktesting.NewFakePassiveClock(start))
	r.Client = interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if plane, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				plane.UID = "5e1f7c3a-0000-4000-8000-00000000dead"
				meta.SetStatusCondition(&plane.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionControllerManagerHeld, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonNotHeld, Message: "the predecessor's"})
			}
			return nil
		},
	})
	sample(t, r, cp)
	got := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Nodes != nil {
		t.Errorf("the successor carries the predecessor's status.nodes: %+v", got.Status.Nodes)
	}
}

func TestDisabledHoldsWithNoListOfNodes(t *testing.T) {
	cp := tenantControlPlane(t)
	updateSpec(t, cp, func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.Health = &lodiav1alpha1.Health{HoldControllerManager: new(false)}
	})
	writeKubeconfig(t, cp, throughProxy(t, func(w http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path == nodeListPath {
			http.Error(w, "injected", http.StatusInternalServerError)
			return true
		}
		return false
	}))
	sample(t, nodesReconciler(clocktesting.NewFakePassiveClock(time.Now())), cp)
	if err := expectHeld(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonDisabled, "spec.health.holdControllerManager is false"); err != nil {
		t.Error(err)
	}
}

func TestWithNoListOfNodesAtAllThereIsNoStatusNodes(t *testing.T) {
	cp := tenantControlPlane(t)
	writeKubeconfig(t, cp, throughProxy(t, func(w http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path == nodeListPath {
			<-req.Context().Done()
			return true
		}
		return false
	}))
	r := nodesReconciler(clock.RealClock{})
	r.Timeout = 500 * time.Millisecond
	started := time.Now()
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(started); took > 4*r.Timeout {
		t.Errorf("a pass whose list of Nodes hangs took %s with a Timeout of %s", took, r.Timeout)
	}
	expectNotHeld(t, cp, "the nodes are not listed yet")
	expectNodes(t, cp, nil)
}

func TestReconcilingIsAppliedBeforeATenantThatNeverAnswersIsCalled(t *testing.T) {
	cp := controlPlane(t)
	setAccess(t, cp)
	var asked, reconciling atomic.Bool
	silentTenant := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		if sampling := req.URL.Path == managerLease || req.URL.Path == leaseListPath || req.URL.Path == nodeListPath; sampling && asked.CompareAndSwap(false, true) {
			fresh := &lodiav1alpha1.ControlPlane{}
			if k8s.Get(req.Context(), client.ObjectKeyFromObject(cp), fresh) == nil {
				c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionControllerManagerHeld)
				reconciling.Store(c != nil && c.Status == metav1.ConditionUnknown && c.Reason == lodiav1alpha1.ReasonReconciling)
			}
		}
		<-req.Context().Done()
	}))
	t.Cleanup(silentTenant.Close)
	kubeconfig, err := kubeconfigOf(silentTenant.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	writeKubeconfig(t, cp, tenantKubeconfig)
	r := nodesReconciler(clock.RealClock{})
	r.Reader = kubeconfigOverride{Reader: k8s, key: client.ObjectKey{Namespace: cp.Namespace, Name: lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name)}, kubeconfig: kubeconfig}
	r.Timeout = 500 * time.Millisecond
	started := time.Now()
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(started); took > 4*r.Timeout {
		t.Errorf("a pass against a tenant that does not answer took %s with a Timeout of %s", took, r.Timeout)
	}
	if !asked.Load() || !reconciling.Load() {
		t.Errorf("the tenant was asked %t, and ControllerManagerHeld was Reconciling at the first call %t", asked.Load(), reconciling.Load())
	}
	if err := expectHeld(t, cp, metav1.ConditionUnknown, lodiav1alpha1.ReasonReconciling, "the first pass has not finished"); err != nil {
		t.Error(err)
	}
	expectNodes(t, cp, nil)
}

func TestTheHoldIsDisabledWithTheFieldFalse(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	joined(t, "a", corev1.ConditionTrue, start)
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(0, 1)+fewerThanTwo)

	updateSpec(t, cp, func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.Health = &lodiav1alpha1.Health{HoldControllerManager: new(false)}
	})
	now.SetTime(start.Add(5 * time.Second))
	sample(t, r, cp)
	if err := expectHeld(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonDisabled, "spec.health.holdControllerManager is false"); err != nil {
		t.Error(err)
	}
	expectNodes(t, cp, nodesOf(1, 1, 0, "v1.35.0"))

	updateSpec(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Health.HoldControllerManager = new(true) })
	now.SetTime(start.Add(10 * time.Second))
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(0, 1)+fewerThanTwo)
}

func TestAPassLongerThanThePeriodStillRequeues(t *testing.T) {
	cp := tenantControlPlane(t)
	writeKubeconfig(t, cp, throughProxy(t, func(_ http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path == leaseListPath {
			time.Sleep(400 * time.Millisecond)
		}
		return false
	}))
	r := nodesReconciler(clock.RealClock{})
	r.Period = 100 * time.Millisecond
	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != time.Second {
		t.Errorf("a pass of 400 ms at a period of 100 ms requeues after %s, want 1s", result.RequeueAfter)
	}
}

func TestAFailingPassReturnsNoError(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	injected := errors.New("injected")
	failingGet := func(fails func(client.ObjectKey, client.Object) bool) client.WithWatch {
		return interceptor.NewClient(k8s, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if fails(key, obj) {
					return injected
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
	}
	isControlPlane := func(_ client.ObjectKey, obj client.Object) bool {
		_, ok := obj.(*lodiav1alpha1.ControlPlane)
		return ok
	}
	isKubeconfig := func(key client.ObjectKey, _ client.Object) bool {
		return key.Name == lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name)
	}
	failingApply := interceptor.NewClient(k8s, interceptor.Funcs{
		SubResourceApply: func(context.Context, client.Client, string, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
			return injected
		},
	})
	for _, c := range []struct {
		failing string
		client  client.WithWatch
		reader  client.Reader
	}{
		{"the read of the ControlPlane", failingGet(isControlPlane), k8s},
		{"the live read of the ControlPlane", k8s, failingGet(isControlPlane)},
		{"the read of Lodia's kubeconfig", k8s, failingGet(isKubeconfig)},
		{"the status apply", failingApply, k8s},
	} {
		r := nodesReconciler(now)
		r.Client, r.Reader = c.client, c.reader
		sample(t, r, cp)
		if t.Failed() {
			t.Fatalf("a pass whose %s failed", c.failing)
		}
	}

	joined(t, "a", corev1.ConditionTrue, start)
	r := nodesReconciler(now)
	r.Client = failingApply
	sample(t, r, cp)
	r.Client = k8s
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(0, 1)+fewerThanTwo)
	expectNodes(t, cp, nodesOf(1, 1, 0, "v1.35.0"))
}

func TestAPausedOrDeletedControlPlaneGetsNoPassAndNoRequeue(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	r.HoldFraction = 1000
	joined(t, "a", corev1.ConditionTrue, start)
	joined(t, "b", corev1.ConditionTrue, start)
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(0, 2))

	updateSpec(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	r.Client = noControlPlaneWrite(t, k8s)
	for _, at := range []int{10, 25} {
		now.SetTime(start.Add(time.Duration(at) * time.Second))
		result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		if err != nil || result != (ctrl.Result{}) {
			t.Fatalf("a pass over a paused ControlPlane returned %+v, %v, want no requeue and no error", result, err)
		}
	}
	r.Client = k8s
	updateSpec(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = false })
	sample(t, r, cp)
	expectNotHeld(t, cp, silent(0, 2))

	updateSpec(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Finalizers = []string{"test.lodia.io/keep"} })
	if err := k8s.Delete(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	r.Client = noControlPlaneWrite(t, k8s)
	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	if err != nil || result != (ctrl.Result{}) {
		t.Fatalf("a pass over a ControlPlane being deleted returned %+v, %v, want no requeue and no error", result, err)
	}
	updateSpec(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Finalizers = nil })
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), &lodiav1alpha1.ControlPlane{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the ControlPlane is still there: %v", err)
	}
	result, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	if err != nil || result != (ctrl.Result{}) {
		t.Fatalf("a pass over a deleted ControlPlane returned %+v, %v, want no requeue and no error", result, err)
	}
}

func TestAControlPlaneRecreatedUnderTheSameNameStartsAfresh(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	r.HoldFraction = 1000
	joined(t, "a", corev1.ConditionTrue, start)
	joined(t, "b", corev1.ConditionTrue, start)
	sample(t, r, cp)
	expectNodes(t, cp, nodesOf(2, 2, 0, "v1.35.0"))

	if err := k8s.Delete(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	successor := controlPlaneIn(t, cp.Namespace, lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}})
	setAccess(t, successor)
	joined(t, "c", corev1.ConditionTrue, start)
	now.SetTime(start.Add(25 * time.Second))
	sample(t, r, successor)
	expectNodes(t, successor, nodesOf(3, 3, 0, "v1.35.0"))
	expectNotHeld(t, successor, silent(0, 3))
}

func TestAStaleCacheDoesNotPutReconcilingOverACondition(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	joined(t, "a", corev1.ConditionTrue, start)
	joined(t, "b", corev1.ConditionTrue, start)
	sample(t, nodesReconciler(now), cp)
	expectNodes(t, cp, nodesOf(2, 2, 0, "v1.35.0"))

	stale := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if plane, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				meta.RemoveStatusCondition(&plane.Status.Conditions, lodiav1alpha1.ConditionControllerManagerHeld)
				plane.Status.Nodes = nil
			}
			return nil
		},
	})
	recording, applied := recordingApplies(stale)
	restarted := nodesReconciler(now)
	restarted.Client = recording
	sample(t, restarted, cp)
	for _, a := range applied() {
		if strings.Contains(a, lodiav1alpha1.ReasonReconciling) {
			t.Errorf("a stale cache put %s over a condition the API server holds", a)
		}
	}
	expectNotHeld(t, cp, silent(0, 2))
	expectNodes(t, cp, nodesOf(2, 2, 0, "v1.35.0"))
}

type kubeconfigOverride struct {
	client.Reader
	key        client.ObjectKey
	kubeconfig []byte
}

func (o kubeconfigOverride) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := o.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if secret, ok := obj.(*corev1.Secret); ok && key == o.key {
		secret.Data = map[string][]byte{lodiav1alpha1.KubeconfigKey: o.kubeconfig}
	}
	return nil
}

func startNodes(t *testing.T, namespaces []string, register func(ctrl.Manager) error) {
	t.Helper()
	mgr, err := newManager(k8s.Scheme(), true, namespaces...)
	if err != nil {
		t.Fatal(err)
	}
	if err := register(mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() { started <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-started; err != nil {
			t.Errorf("manager: %v", err)
		}
	})
}

func TestATenantWhoseAPIServerHangsDoesNotStopAnothersSamples(t *testing.T) {
	clearNodes(t)
	hanging, healthy := namespace(t), namespace(t)
	hung := controlPlaneIn(t, hanging, lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}})
	good := controlPlaneIn(t, healthy, lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}})
	var firstAsked atomic.Int64
	silentTenant := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		if req.URL.Path == managerLease || req.URL.Path == leaseListPath || req.URL.Path == nodeListPath {
			firstAsked.CompareAndSwap(0, time.Now().UnixNano())
		}
		<-req.Context().Done()
	}))
	t.Cleanup(silentTenant.Close)
	kubeconfig, err := kubeconfigOf(silentTenant.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().Truncate(time.Second)
	joined(t, "a", corev1.ConditionTrue, start)
	joined(t, "b", corev1.ConditionTrue, start)
	for _, cp := range []*lodiav1alpha1.ControlPlane{hung, good} {
		setAccess(t, cp)
	}
	writeKubeconfig(t, hung, tenantKubeconfig)
	writeKubeconfig(t, good, tenantKubeconfig)

	const timeout = 5 * time.Second
	startNodes(t, []string{hanging, healthy}, func(mgr ctrl.Manager) error {
		return (&health.NodesReconciler{
			Client: mgr.GetClient(), Reader: kubeconfigOverride{Reader: mgr.GetAPIReader(), key: client.ObjectKey{Namespace: hung.Namespace, Name: lodiav1alpha1.LodiaKubeconfigSecret.Name(hung.Name)}, kubeconfig: kubeconfig},
			Period: 100 * time.Millisecond, Timeout: timeout,
			SilentAfter: time.Second, HoldFraction: 600, ReleaseAfter: time.Minute, MaxNodes: 5000, Clock: clock.RealClock{},
		}).SetupWithManager(mgr, 2)
	})
	testenv.Eventually(t, func() error {
		return expectHeld(t, good, metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent, taken(2, 2, "60"))
	})
	asked := firstAsked.Load()
	if asked == 0 {
		t.Fatal("the hanging tenant was never asked")
	}
	if waited := time.Since(time.Unix(0, asked)); waited >= timeout {
		t.Errorf("the healthy tenant's silent nodes were seen %s after the hanging tenant was asked, past its Timeout of %s", waited, timeout)
	}
}

func TestAPassDoesNotDependOnThePublicEndpoint(t *testing.T) {
	clearNodes(t)
	ns := namespace(t)
	cp := controlPlaneIn(t, ns, lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}})
	writeBundle(t, cp, serve(t))
	start := time.Now().Truncate(time.Second)
	joined(t, "a", corev1.ConditionTrue, start)
	joined(t, "b", corev1.ConditionTrue, start)
	writeKubeconfig(t, cp, tenantKubeconfig)

	var dialing, dialed atomic.Int32
	startNodes(t, []string{ns}, func(mgr ctrl.Manager) error {
		if err := (&health.Reconciler{
			Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), FailuresBeforeFalse: failuresBeforeFalse,
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				dialing.Add(1)
				defer dialed.Add(1)
				return blackHole(ctx, network, address)
			},
			Period: time.Hour, Timeout: 10 * time.Second, Clock: clock.RealClock{},
		}).SetupWithManager(mgr, 1); err != nil {
			return err
		}
		return (&health.NodesReconciler{
			Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Period: time.Hour, Timeout: 10 * time.Second,
			SilentAfter: 20 * time.Second, HoldFraction: 600, MaxNodes: 5000, Clock: clock.RealClock{},
		}).SetupWithManager(mgr, 1)
	})
	testenv.Eventually(t, func() error {
		return expect(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint")
	})
	address := map[string]any{"host": "127.0.0.1", "port": int64(1)}
	endpoint := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     map[string]any{"endpoint": map[string]any{"apiServer": address, "konnectivity": address, "inCluster": address}},
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(endpoint), client.FieldOwner("test/endpoint"), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		if dialing.Load() == 0 {
			return errors.New("Health has not dialled the endpoint yet")
		}
		return nil
	})
	setAccess(t, cp)
	testenv.Eventually(t, func() error {
		return expectHeld(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 2))
	})
	if dialed.Load() != 0 {
		t.Errorf("Health's probe of the black-holed endpoint ended before the sample did")
	}
}
