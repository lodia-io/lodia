// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/health"
)

const eightMiB = 8 << 20

func writeOversizeList(w http.ResponseWriter, apiVersion, kind string) {
	const tail = `"}}}]}`
	head := `{"kind":"` + kind + `","apiVersion":"` + apiVersion + `","metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"big","annotations":{"pad":"`
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, head+strings.Repeat("x", eightMiB+1-len(head)-len(tail))+tail)
}

func TestAResponseLongerThanTheBoundIsAListThatCouldNotBeRead(t *testing.T) {
	cp := restoredControlPlane(t, lodiav1alpha1.RestoreHeld)
	setAccess(t, cp)
	writeKubeconfig(t, cp, throughProxy(t, func(w http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path != "/api/v1/services" {
			return false
		}
		writeOversizeList(w, "v1", "ServiceList")
		return true
	}))
	reconcile(t, &health.Reconciler{Client: k8s, Reader: k8s, FailuresBeforeFalse: failuresBeforeFalse, Period: time.Hour, Timeout: 2 * time.Second, Clock: clock.RealClock{}}, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease, actsOn+"Services could not be listed; 0 PersistentVolumes with reclaimPolicy Delete"+
		"; 0 pending PersistentVolumeClaims; 0 webhooks that call a URL or fail closed; 0 APIServices served by a Service; 0 CronJobs not suspended"); err != nil {
		t.Error(err)
	}
}

const beyond = ": Lodia does not count this tenant's nodes"

func tooManyLeases(n int) string {
	return fmt.Sprintf("more than %d Leases of kube-node-lease (health.maxNodes)", n) + beyond
}

func tooManyNodes(n int) string {
	return fmt.Sprintf("more than %d Nodes (health.maxNodes)", n) + beyond
}

func tooManyPagesOfLeases(n int) string {
	return fmt.Sprintf("more than %d pages of Leases of kube-node-lease (health.maxNodes)", n) + beyond
}

func tooManyPagesOfNodes(n int) string {
	return fmt.Sprintf("more than %d pages of Nodes (health.maxNodes)", n) + beyond
}

func truncatedNodes(total, ready int32) *lodiav1alpha1.NodesStatus {
	return &lodiav1alpha1.NodesStatus{Count: lodiav1alpha1.Count{Total: total, Ready: ready}, Truncated: true}
}

func expectTooMany(t *testing.T, cp *lodiav1alpha1.ControlPlane, message string) {
	t.Helper()
	if err := expectHeld(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, message); err != nil {
		t.Error(err)
	}
}

func removeNode(t *testing.T, name string) {
	t.Helper()
	if err := tenant.Delete(t.Context(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		t.Fatal(err)
	}
	if err := tenant.Delete(t.Context(), &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: corev1.NamespaceNodeLease, Name: name}}); err != nil {
		t.Fatal(err)
	}
}

type askedPaths struct {
	mu    sync.Mutex
	asked map[string][]url.Values
}

func (a *askedPaths) add(req *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.asked == nil {
		a.asked = map[string][]url.Values{}
	}
	a.asked[req.URL.Path] = append(a.asked[req.URL.Path], req.URL.Query())
}

func (a *askedPaths) take(path string) []url.Values {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := a.asked[path]
	delete(a.asked, path)
	return out
}

func (a *askedPaths) count() (n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, asked := range a.asked {
		n += len(asked)
	}
	return n
}

func writePageWithNoItems(w http.ResponseWriter, apiVersion, kind string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"kind":"`+kind+`","apiVersion":"`+apiVersion+`","metadata":{"resourceVersion":"1","continue":"more"},"items":[]}`)
}

func expectPages(t *testing.T, name string, asked []url.Values, limit string, continues int) {
	t.Helper()
	if len(asked) != continues+1 {
		t.Errorf("%s was asked for %d times, want %d", name, len(asked), continues+1)
		return
	}
	for i, q := range asked {
		if q.Get("limit") != limit || q.Has("continue") != (i > 0) || q.Has("resourceVersion") || q.Has("resourceVersionMatch") {
			t.Errorf("%s page %d asked %v, want limit %s, a continue token on every page but the first and no resourceVersion", name, i+1, q, limit)
		}
	}
}

func TestBothListsPageAndTheTenantsBeyondTheBoundsAreNotCounted(t *testing.T) {
	cp := tenantControlPlane(t)
	var asked askedPaths
	var inject atomic.Pointer[func(http.ResponseWriter, *http.Request) bool]
	writeKubeconfig(t, cp, throughProxy(t, func(w http.ResponseWriter, req *http.Request) bool {
		asked.add(req)
		if fn := inject.Load(); fn != nil {
			return (*fn)(w, req)
		}
		return false
	}))
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	for i := range 120 {
		joined(t, fmt.Sprintf("n%03d", i), corev1.ConditionTrue, start)
	}
	for i := range 381 {
		renewNodeLease(t, fmt.Sprintf("x%03d", i), time.Now())
	}
	pass := func(r *health.NodesReconciler, seconds int) {
		t.Helper()
		now.SetTime(start.Add(time.Duration(seconds) * time.Second))
		sample(t, r, cp)
	}
	on := func(path string, fn func(http.ResponseWriter, *http.Request)) {
		injected := func(w http.ResponseWriter, req *http.Request) bool {
			if req.URL.Path != path {
				return false
			}
			fn(w, req)
			return true
		}
		inject.Store(&injected)
	}

	t.Run("more Nodes and Leases than a page are counted whole", func(t *testing.T) {
		r := nodesReconciler(now)
		pass(r, 0)
		expectNotHeld(t, cp, silent(0, 120))
		expectNodes(t, cp, nodesOf(120, 120, 0, "v1.35.0"))
		expectPages(t, "the Leases", asked.take(leaseListPath), "500", 1)
		expectPages(t, "the Nodes", asked.take(nodeListPath), "100", 1)
	})

	t.Run("more than maxNodes stops both lists at their first page", func(t *testing.T) {
		kubernetes := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": lodiav1alpha1.GroupVersion.String(),
			"kind":       "ControlPlane",
			"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
			"status":     map[string]any{"kubernetes": map[string]any{"version": "v1.36.1"}},
		}}
		if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(kubernetes), client.FieldOwner("test/lifecycle"), client.ForceOwnership); err != nil {
			t.Fatal(err)
		}
		r := nodesReconciler(now)
		r.MaxNodes = 10
		pass(r, 5)
		expectTooMany(t, cp, tooManyLeases(10))
		expectNodes(t, cp, truncatedNodes(10, 10))
		expectPages(t, "the Leases", asked.take(leaseListPath), "500", 0)
		expectPages(t, "the Nodes", asked.take(nodeListPath), "100", 0)
		if _, _, ok := r.Counts(client.ObjectKeyFromObject(cp), cp.UID); ok {
			t.Error("a tenant beyond the bound has a count of silent nodes")
		}
	})

	t.Run("a later page that fails is a failed list", func(t *testing.T) {
		failing := func(w http.ResponseWriter, req *http.Request) bool {
			if req.URL.Path != leaseListPath || !req.URL.Query().Has("continue") {
				return false
			}
			refuse(w, http.StatusInternalServerError, metav1.StatusReasonInternalError)
			return true
		}
		inject.Store(&failing)
		pass(nodesReconciler(now), 10)
		expectNotHeld(t, cp, failedLeaseMsg)
		expectNodes(t, cp, nodesOf(120, 120, 120, "v1.35.0"))
	})

	t.Run("a later page past the deadline is a failed list", func(t *testing.T) {
		hanging := func(w http.ResponseWriter, req *http.Request) bool {
			if req.URL.Path != leaseListPath || !req.URL.Query().Has("continue") {
				return false
			}
			hang(w, req)
			return true
		}
		inject.Store(&hanging)
		r := nodesReconciler(now)
		r.Timeout = 300 * time.Millisecond
		pass(r, 15)
		expectNotHeld(t, cp, failedLeaseMsg)
		inject.Store(nil)
	})

	t.Run("a response longer than the bound stops the list that read it", func(t *testing.T) {
		on(leaseListPath, func(w http.ResponseWriter, _ *http.Request) {
			writeOversizeList(w, "coordination.k8s.io/v1", "LeaseList")
		})
		pass(nodesReconciler(now), 20)
		expectTooMany(t, cp, "a response listing the Leases of kube-node-lease is longer than 8388608 bytes"+beyond)
		expectNodes(t, cp, nodesOf(120, 120, 120, "v1.35.0"))

		on(nodeListPath, func(w http.ResponseWriter, _ *http.Request) { writeOversizeList(w, "v1", "NodeList") })
		pass(nodesReconciler(now), 25)
		expectTooMany(t, cp, "a response listing the Nodes is longer than 8388608 bytes"+beyond)
		expectNodes(t, cp, truncatedNodes(0, 0))
		inject.Store(nil)
	})
}

func TestAPassOverListsOfPagesWithNoItemsSendsAtMostTheRequestsTheBoundsAllow(t *testing.T) {
	const leaderLeaseReads = 5
	for _, c := range []struct {
		name                string
		leaseAsks, nodeAsks int
		message             string
		nodes               *lodiav1alpha1.NodesStatus
	}{
		{"the Leases", 11, 1, tooManyPagesOfLeases(11), nodesOf(0, 0, 0, "")},
		{"the Nodes", 1, 51, tooManyPagesOfNodes(51), truncatedNodes(0, 0)},
		{"both", 11, 51, tooManyPagesOfLeases(11), truncatedNodes(0, 0)},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := tenantControlPlane(t)
			var asked askedPaths
			writeKubeconfig(t, cp, throughProxy(t, func(w http.ResponseWriter, req *http.Request) bool {
				asked.add(req)
				switch {
				case req.URL.Path == leaseListPath && c.leaseAsks > 1:
					writePageWithNoItems(w, "coordination.k8s.io/v1", "LeaseList")
				case req.URL.Path == nodeListPath && c.nodeAsks > 1:
					writePageWithNoItems(w, "v1", "NodeList")
				default:
					return false
				}
				return true
			}))
			r := nodesReconciler(clocktesting.NewFakePassiveClock(time.Now().Truncate(time.Second)))
			begun := time.Now()
			sample(t, r, cp)
			t.Logf("a pass over lists that never end took %v", time.Since(begun))
			if sent, most := asked.count(), c.leaseAsks+c.nodeAsks+leaderLeaseReads; sent > most {
				t.Errorf("the pass sent %d requests, more than the %d its bounds allow", sent, most)
			}
			expectPages(t, "the Leases", asked.take(leaseListPath), "500", c.leaseAsks-1)
			expectPages(t, "the Nodes", asked.take(nodeListPath), "100", c.nodeAsks-1)
			expectTooMany(t, cp, c.message)
			expectNodes(t, cp, c.nodes)
		})
	}
}

func TestRow5AHeldTenantThatGrowsBeyondTheBoundIsReleasedAtOnce(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.r.MaxNodes = 3
	joined(t, "d", corev1.ConditionTrue, h.start)
	h.at(25)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyLeases(3))
	h.expectNoHealth()
	if holder := h.holder(); holder != "" {
		t.Fatalf("a tenant beyond the bound left the Lease with holder %q", holder)
	}
	lease := h.lease()
	if ptr.Deref(lease.Spec.LeaseDurationSeconds, 0) != 1 || !lease.Spec.AcquireTime.Time.Equal(h.start.Add(25*time.Second)) {
		t.Errorf("the release left %+v, want a duration of 1 s and acquireTime now", lease.Spec)
	}
	h.at(30)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyLeases(3))
	if holder := h.holder(); holder != "" {
		t.Fatalf("the next pass took the Lease again: holder %q", holder)
	}
}

func TestRow6ATenantBeyondTheBoundIsNotHeldWhateverIsSilent(t *testing.T) {
	h := sampling(t, "a", "b", "c", "d")
	h.r.MaxNodes = 3
	for _, at := range []int{0, 20, 40} {
		h.at(at)
		h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyLeases(3))
		if lease := h.lease(); lease != nil {
			t.Fatalf("a tenant beyond the bound got the Lease %+v at %d s", lease.Spec, at)
		}
	}
}

func TestRow5AHoldIsReleasedByAStopKnownFromAnEarlierPassWhileAccessReadyIsNotTrue(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.r.MaxNodes = 3
	joined(t, "d", corev1.ConditionTrue, h.start)
	h.fail(managerLease, http.MethodPut, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	h.at(25)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("a failed release left holder %q", holder)
	}
	h.inject(nil)
	setAccessAt(t, h.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding, get(t, h.cp).Generation)
	h.at(30)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyLeases(3))
	h.expectNoHealth()
	if holder := h.holder(); holder != "" {
		t.Fatalf("a tenant beyond the bound left holder %q while AccessReady was not True", holder)
	}
}

func TestRow5AHoldIsReleasedByAStopOfTheNodesWhileTheLeaseListFails(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.r.MaxNodes = 3
	addNode(t, "d", corev1.ConditionTrue, h.start, "v1.35.0")
	h.fail(leaseListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	h.at(30)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyNodes(3))
	h.expectNoHealth()
	expectNodes(t, h.cp, truncatedNodes(3, 3))
	if holder := h.holder(); holder != "" {
		t.Fatalf("a tenant beyond the bound left holder %q while its Lease list failed", holder)
	}
}

func TestRow6ATenantBeyondTheBoundWithAFailingLeaseListOrNoAccessIsTooManyNodes(t *testing.T) {
	for name, setup := range map[string]func(*holdRun){
		"a failing Lease list": func(h *holdRun) {
			h.fail(leaseListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
		},
		"AccessReady not True": func(h *holdRun) {
			setAccessAt(h.t, h.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding, get(h.t, h.cp).Generation)
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := sampling(t, "a", "b", "c")
			h.r.MaxNodes = 3
			h.at(0)
			addNode(t, "d", corev1.ConditionTrue, h.start, "v1.35.0")
			h.at(30)
			h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyNodes(3))
			setup(h)
			h.at(35)
			h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyNodes(3))
			if lease := h.lease(); lease != nil {
				t.Fatalf("a tenant beyond the bound got the Lease %+v", lease.Spec)
			}
		})
	}
}

func TestRows3And4DisabledComeBeforeATenantBeyondTheBound(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.r.MaxNodes = 3
	joined(t, "d", corev1.ConditionTrue, h.start)
	updateSpec(t, h.cp, func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.Health = &lodiav1alpha1.Health{HoldControllerManager: new(false)}
	})
	h.at(25)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonDisabled, disabled)
	if holder := h.holder(); holder != "" {
		t.Fatalf("a disabled hold of a tenant beyond the bound left holder %q", holder)
	}
	h.at(30)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonDisabled, disabled)
}

func TestBackUnderTheBoundsTheClockOfTheLeasesStartsAfresh(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.r.MaxNodes = 3
	h.at(0)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
	joined(t, "d", corev1.ConditionTrue, h.start)
	h.at(5)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyLeases(3))
	removeNode(t, "d")
	for _, at := range []int{10, 25, 29} {
		h.at(at)
		h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
		if lease := h.lease(); lease != nil {
			t.Fatalf("the Lease was taken at %d s", at)
		}
	}
	h.at(30)
	h.expectCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent, taken(3, 3, "60"))
}

func TestAStopOfTheLeaseListLeavesStatusNodesAsTheLastListOfNodesHasIt(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.r.MaxNodes = 3
	renewNodeLease(t, "x", time.Now())
	for _, at := range []int{0, 5} {
		h.at(at)
		h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyLeases(3))
		expectNodes(t, h.cp, nodesOf(3, 3, 0, "v1.35.0"))
	}
	addNode(t, "d", corev1.ConditionTrue, h.start, "v1.34.0")
	h.at(30)
	expectNodes(t, h.cp, truncatedNodes(3, 3))
	if err := tenant.Delete(t.Context(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "d"}}); err != nil {
		t.Fatal(err)
	}
	h.at(60)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyLeases(3))
	expectNodes(t, h.cp, nodesOf(3, 3, 0, "v1.35.0"))
}

func TestAStopOfTheNodesStandsWhileTheNextListOfNodesFails(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.r.MaxNodes = 3
	addNode(t, "d", corev1.ConditionTrue, h.start, "v1.34.0")
	h.at(0)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyNodes(3))
	expectNodes(t, h.cp, truncatedNodes(3, 3))
	h.fail(nodeListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	h.at(30)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyNodes(3))
	expectNodes(t, h.cp, truncatedNodes(3, 3))
	if lease := h.lease(); lease != nil {
		t.Fatalf("a tenant beyond the bound got the Lease %+v", lease.Spec)
	}
}

func TestAStopOfTheLeaseListStandsWhileTheNextLeaseListFails(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.r.MaxNodes = 3
	renewNodeLease(t, "x", time.Now())
	h.at(0)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyLeases(3))
	h.fail(leaseListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	h.at(5)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyLeases(3))
	expectNodes(t, h.cp, nodesOf(3, 3, 0, "v1.35.0"))
	if lease := h.lease(); lease != nil {
		t.Fatalf("a tenant beyond the bound got the Lease %+v", lease.Spec)
	}
}

func TestBackUnderTheBoundOfTheNodesTheClockOfTheLeasesStartsAfresh(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.r.MaxNodes = 3
	h.at(0)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
	addNode(t, "d", corev1.ConditionTrue, h.start, "v1.35.0")
	h.at(30)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyNodes(3))
	if err := tenant.Delete(t.Context(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "d"}}); err != nil {
		t.Fatal(err)
	}
	h.at(60)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
	if lease := h.lease(); lease != nil {
		t.Fatalf("the Lease was taken on the first pass back under the bound: %+v", lease.Spec)
	}
	h.at(80)
	h.expectCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent, taken(3, 3, "60"))
}

func TestAReleaseThatFailsIsTriedAgainByTheNextPass(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.r.MaxNodes = 3
	joined(t, "d", corev1.ConditionTrue, h.start)
	h.fail(managerLease, http.MethodPut, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	h.at(25)
	h.expectSilent(taken(3, 3, "60"))
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("a failed release left holder %q", holder)
	}
	h.inject(nil)
	h.at(30)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyLeases(3))
	if holder := h.holder(); holder != "" {
		t.Fatalf("the next pass left holder %q", holder)
	}
}

func TestARestartedOperatorReleasesAHoldOfATenantBeyondTheBound(t *testing.T) {
	h := holding(t, "a", "b", "c")
	joined(t, "d", corev1.ConditionTrue, h.start)
	h.r = nodesReconciler(h.now)
	h.r.MaxNodes = 3
	h.at(25)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyLeases(3))
	if holder := h.holder(); holder != "" {
		t.Fatalf("a restarted operator left holder %q", holder)
	}
}

func TestATenantBeyondTheBoundLeavesNothingToTheControlPlaneRecreatedUnderItsName(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	r := nodesReconciler(now)
	r.MaxNodes = 3
	for _, name := range []string{"a", "b", "c", "d"} {
		joined(t, name, corev1.ConditionTrue, start)
	}
	sample(t, r, cp)
	expectTooMany(t, cp, tooManyLeases(3))
	expectNodes(t, cp, truncatedNodes(3, 3))

	if err := k8s.Delete(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	removeNode(t, "d")
	successor := controlPlaneIn(t, cp.Namespace, lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}})
	setAccess(t, successor)
	now.SetTime(start.Add(25 * time.Second))
	sample(t, r, successor)
	expectNodes(t, successor, nodesOf(3, 3, 0, "v1.35.0"))
	expectNotHeld(t, successor, silent(0, 3))
}

func TestRow5ReleasesAHoldOfATenantBeyondTheBoundBeforeAnyListOfNodes(t *testing.T) {
	h := holding(t, "a", "b", "c")
	joined(t, "d", corev1.ConditionTrue, h.start)
	h.fail(nodeListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	h.r = nodesReconciler(h.now)
	h.r.MaxNodes = 3
	h.at(25)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonTooManyNodes, tooManyLeases(3))
	expectNodes(t, h.cp, nodesOf(3, 3, 0, "v1.35.0"))
	if holder := h.holder(); holder != "" {
		t.Fatalf("a restarted operator with no list of Nodes left holder %q", holder)
	}
}
