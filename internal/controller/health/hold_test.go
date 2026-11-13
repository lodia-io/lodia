// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/health"
)

const (
	keepsHolding = "; Lodia keeps holding the controller manager's leader Lease"
	holdsUntil   = "; Lodia holds the controller manager's leader Lease until they have been back for 2m0s"
	notTrue      = "AccessReady is not True"
	notListed    = "the nodes are not listed yet"
	disabled     = "spec.health.holdControllerManager is false"
)

type holdRun struct {
	t         *testing.T
	cp        *lodiav1alpha1.ControlPlane
	now       *clocktesting.FakePassiveClock
	r         *health.NodesReconciler
	start     time.Time
	nodes     []string
	intercept atomic.Pointer[func(http.ResponseWriter, *http.Request) bool]
}

func sampling(t *testing.T, nodes ...string) *holdRun {
	t.Helper()
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	h := &holdRun{t: t, cp: tenantControlPlane(t), now: now, r: nodesReconciler(now), start: start, nodes: nodes}
	writeKubeconfig(t, h.cp, throughProxy(t, func(w http.ResponseWriter, req *http.Request) bool {
		if fn := h.intercept.Load(); fn != nil {
			return (*fn)(w, req)
		}
		return false
	}))
	for _, name := range nodes {
		joined(t, name, corev1.ConditionTrue, start)
	}
	return h
}

func holding(t *testing.T, nodes ...string) *holdRun {
	t.Helper()
	h := sampling(t, nodes...)
	h.at(0)
	h.at(20)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("20 seconds of silence left the Lease with holder %q", holder)
	}
	return h
}

func (h *holdRun) at(seconds int) {
	h.t.Helper()
	h.now.SetTime(h.start.Add(time.Duration(seconds) * time.Second))
	sample(h.t, h.r, h.cp)
}

func (h *holdRun) back(seconds ...int) {
	h.t.Helper()
	for _, s := range seconds {
		for _, name := range h.nodes {
			renewNodeLease(h.t, name, time.Now())
		}
		h.at(s)
	}
}

func (h *holdRun) inject(fn func(http.ResponseWriter, *http.Request) bool) {
	if fn == nil {
		h.intercept.Store(nil)
		return
	}
	h.intercept.Store(&fn)
}

func (h *holdRun) fail(path string, method string, code int, reason metav1.StatusReason) {
	h.inject(func(w http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path != path || method != "" && req.Method != method {
			return false
		}
		refuse(w, code, reason)
		return true
	})
}

func refuse(w http.ResponseWriter, code int, reason metav1.StatusReason) {
	refuseSaying(w, code, reason, "injected: "+string(reason))
}

func refuseSaying(w http.ResponseWriter, code int, reason metav1.StatusReason, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(metav1.Status{
		TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   metav1.StatusFailure, Reason: reason, Code: int32(code), Message: message,
	})
}

func hang(_ http.ResponseWriter, req *http.Request) {
	_, _ = io.Copy(io.Discard, req.Body)
	<-req.Context().Done()
}

func (h *holdRun) lease() *coordinationv1.Lease {
	h.t.Helper()
	lease := &coordinationv1.Lease{}
	err := tenant.Get(h.t.Context(), client.ObjectKey{Namespace: lodiav1alpha1.LeaderElectionNamespace, Name: lodiav1alpha1.ControllerManagerLease}, lease)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		h.t.Fatal(err)
	}
	return lease
}

func (h *holdRun) holder() string {
	h.t.Helper()
	lease := h.lease()
	if lease == nil {
		return ""
	}
	return ptr.Deref(lease.Spec.HolderIdentity, "")
}

func (h *holdRun) acquiredBy(holder string, at time.Time) {
	h.t.Helper()
	renew(h.t, lodiav1alpha1.ControllerManagerLease, holder, time.Now())
	lease := h.lease()
	lease.Spec.AcquireTime = &metav1.MicroTime{Time: at}
	if err := tenant.Update(h.t.Context(), lease); err != nil {
		h.t.Fatal(err)
	}
}

func (h *holdRun) renewedAt(seconds int) {
	h.t.Helper()
	lease := h.lease()
	want := h.start.Add(time.Duration(seconds) * time.Second)
	if lease.Spec.RenewTime == nil || !lease.Spec.RenewTime.Time.Equal(want) || ptr.Deref(lease.Spec.LeaseDurationSeconds, 0) != 60 {
		h.t.Fatalf("the Lease is renewed at %v for %d s, want %v for 60 s", lease.Spec.RenewTime, ptr.Deref(lease.Spec.LeaseDurationSeconds, 0), want)
	}
}

func (h *holdRun) expectUntouched(before *coordinationv1.Lease) {
	h.t.Helper()
	after := h.lease()
	if before == nil && after == nil {
		return
	}
	if before == nil || after == nil || before.ResourceVersion != after.ResourceVersion {
		h.t.Fatalf("the pass wrote the Lease: %+v, was %+v", after, before)
	}
}

func (h *holdRun) expectCondition(status metav1.ConditionStatus, reason, message string) {
	h.t.Helper()
	if err := expectHeld(h.t, h.cp, status, reason, message); err != nil {
		h.t.Fatal(err)
	}
}

func (h *holdRun) expectSilent(message string) {
	h.t.Helper()
	h.expectCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent, message)
}

func (h *holdRun) expectHealth(silent, counted int32) {
	h.t.Helper()
	status := get(h.t, h.cp).Status.Health
	if status == nil || status.ControllerManagerHold == nil {
		h.t.Fatalf("status.health is %+v, want a hold", status)
	}
	hold := status.ControllerManagerHold
	lease := h.lease()
	if hold.SilentNodes != silent || hold.CountedNodes != counted {
		h.t.Fatalf("status.health counts %d silent of %d, want %d of %d", hold.SilentNodes, hold.CountedNodes, silent, counted)
	}
	if lease.Spec.AcquireTime == nil || hold.Since == nil || !hold.Since.Equal(new(metav1.NewTime(lease.Spec.AcquireTime.Time).Rfc3339Copy())) {
		h.t.Fatalf("status.health says since %v, the Lease %v", hold.Since, lease.Spec.AcquireTime)
	}
}

func (h *holdRun) expectNoHealth() {
	h.t.Helper()
	if status := get(h.t, h.cp).Status.Health; status != nil {
		h.t.Fatalf("status.health is %+v, want none", status)
	}
}

func createManagerPod(t *testing.T, cp *lodiav1alpha1.ControlPlane, name string) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: name, Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "i"}}},
	}
	create(t, k8s, pod)
}

func TestRow1AFailedReadWithoutAccessWaitsForAccessAndWritesNothing(t *testing.T) {
	for name, intercept := range map[string]func(http.ResponseWriter, *http.Request) bool{
		"Forbidden": func(w http.ResponseWriter, req *http.Request) bool {
			if req.URL.Path != managerLease {
				return false
			}
			refuse(w, http.StatusForbidden, metav1.StatusReasonForbidden)
			return true
		},
		"no answer": func(w http.ResponseWriter, req *http.Request) bool {
			if req.URL.Path != managerLease {
				return false
			}
			hang(w, req)
			return true
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := sampling(t, "a", "b", "c")
			h.r.Timeout = 300 * time.Millisecond
			h.at(0)
			setAccessAt(t, h.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding, get(t, h.cp).Generation)
			h.inject(intercept)
			before := h.lease()
			h.at(5)
			h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, notTrue)
			h.expectNoHealth()
			h.expectUntouched(before)
		})
	}
}

func TestRow2AFailedReadWithAccessKeepsTheConditionAndWritesNothing(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
	h.fail(managerLease, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	h.r.Client = noControlPlaneWrite(t, k8s)
	h.at(5)
	h.at(10)
	if h.lease() != nil {
		t.Fatal("a pass whose read failed created the Lease")
	}
}

func TestRow3DisabledReleasesAHoldAtOnce(t *testing.T) {
	for name, setup := range map[string]func(*holdRun){
		"as is": func(*holdRun) {},
		"with AccessReady not True": func(h *holdRun) {
			setAccessAt(h.t, h.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding, get(h.t, h.cp).Generation)
		},
		"with a failed Lease list": func(h *holdRun) {
			h.fail(leaseListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := holding(t, "a", "b", "c")
			updateSpec(t, h.cp, func(cp *lodiav1alpha1.ControlPlane) {
				cp.Spec.Health = &lodiav1alpha1.Health{HoldControllerManager: new(false)}
			})
			setup(h)
			h.at(25)
			h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonDisabled, disabled)
			h.expectNoHealth()
			if holder := h.holder(); holder != "" {
				t.Fatalf("a disabled hold left the Lease with holder %q", holder)
			}
			lease := h.lease()
			if ptr.Deref(lease.Spec.LeaseDurationSeconds, 0) != 1 || !lease.Spec.AcquireTime.Time.Equal(h.start.Add(25*time.Second)) {
				t.Errorf("the release left %+v, want a duration of 1 s and acquireTime now", lease.Spec)
			}
		})
	}
}

func TestRow3DisabledReleasesAtOnceWithNoListOfNodes(t *testing.T) {
	h := holding(t, "a", "b", "c")
	updateSpec(t, h.cp, func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.Health = &lodiav1alpha1.Health{HoldControllerManager: new(false)}
	})
	h.fail(nodeListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	h.r = nodesReconciler(h.now)
	h.at(25)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonDisabled, disabled)
	if holder := h.holder(); holder != "" {
		t.Fatalf("a restarted operator with no list of Nodes left holder %q", holder)
	}
}

func TestRow4DisabledTakesNothingAtOrAboveTheFraction(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	updateSpec(t, h.cp, func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.Health = &lodiav1alpha1.Health{HoldControllerManager: new(false)}
	})
	h.at(0)
	h.at(20)
	h.at(40)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonDisabled, disabled)
	h.expectNoHealth()
	if h.lease() != nil {
		t.Fatal("a disabled hold wrote the Lease")
	}
	expectNodes(t, h.cp, nodesOf(3, 3, 0, "v1.35.0"))
}

func TestRow7WithoutAccessAHoldRenews(t *testing.T) {
	h := holding(t, "a", "b", "c")
	acquired := h.lease().Spec.AcquireTime
	setAccessAt(t, h.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding, get(t, h.cp).Generation)
	h.at(25)
	h.expectSilent(notTrue + keepsHolding)
	h.expectHealth(0, 0)
	h.renewedAt(25)
	if lease := h.lease(); !lease.Spec.AcquireTime.Equal(acquired) || ptr.Deref(lease.Spec.LeaseTransitions, 0) != 1 {
		t.Errorf("a renewal changed acquireTime to %v and transitions to %d", lease.Spec.AcquireTime, ptr.Deref(lease.Spec.LeaseTransitions, 0))
	}
}

func TestRow8WithoutAccessNotHeldWaits(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	setAccessAt(t, h.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding, get(t, h.cp).Generation)
	h.at(20)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, notTrue)
	if h.lease() != nil {
		t.Fatal("a wait for access wrote the Lease")
	}
}

func TestRow9AFailedLeaseListRenewsAHold(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.fail(leaseListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	h.at(25)
	h.expectSilent(failedLeaseMsg + keepsHolding)
	h.expectHealth(0, 0)
	h.renewedAt(25)
}

func TestRow10AFailedLeaseListWithoutAHoldReportsNotHeld(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	renew(t, lodiav1alpha1.ControllerManagerLease, "another_1", time.Now())
	h.fail(leaseListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	before := h.lease()
	h.at(0)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, failedLeaseMsg)
	h.expectNoHealth()
	h.expectUntouched(before)
}

func TestRow11WithNoNodeListAHoldRenews(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.r = nodesReconciler(h.now)
	h.fail(nodeListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	h.at(25)
	h.expectSilent(notListed + keepsHolding)
	h.expectHealth(0, 0)
	h.renewedAt(25)
}

func TestRow12WithNoNodeListAndAnothersLeaseNotHeld(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	renew(t, lodiav1alpha1.ControllerManagerLease, "another_1", time.Now())
	h.fail(nodeListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	before := h.lease()
	h.at(0)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, notListed)
	h.expectNoHealth()
	h.expectUntouched(before)
}

func TestRow13AtOrAboveAHoldRenewsAndKeepsAcquireTime(t *testing.T) {
	h := holding(t, "a", "b", "c")
	acquired := h.lease().Spec.AcquireTime
	h.at(25)
	h.expectSilent(taken(3, 3, "60"))
	h.expectHealth(3, 3)
	h.renewedAt(25)
	if lease := h.lease(); !lease.Spec.AcquireTime.Equal(acquired) || ptr.Deref(lease.Spec.LeaseTransitions, 0) != 1 {
		t.Errorf("a renewal changed acquireTime to %v and transitions to %d", lease.Spec.AcquireTime, ptr.Deref(lease.Spec.LeaseTransitions, 0))
	}
}

func TestRow14TakesAFreeLeaseOrAnExpiredOneOrAPodsOwn(t *testing.T) {
	t.Run("no Lease", func(t *testing.T) {
		h := sampling(t, "a", "b", "c")
		h.at(0)
		h.at(20)
		h.expectSilent(taken(3, 3, "60"))
		h.expectHealth(3, 3)
		lease := h.lease()
		at := h.start.Add(20 * time.Second)
		if ptr.Deref(lease.Spec.HolderIdentity, "") != lodiav1alpha1.HoldHolderIdentity || ptr.Deref(lease.Spec.LeaseDurationSeconds, 0) != 60 ||
			!lease.Spec.AcquireTime.Time.Equal(at) || !lease.Spec.RenewTime.Time.Equal(at) || ptr.Deref(lease.Spec.LeaseTransitions, 0) != 1 {
			t.Errorf("the take left %+v", lease.Spec)
		}
	})
	t.Run("a Lease without a holder", func(t *testing.T) {
		h := sampling(t, "a", "b", "c")
		h.at(0)
		renew(t, lodiav1alpha1.ControllerManagerLease, "", time.Now())
		h.at(20)
		h.expectSilent(taken(3, 3, "60"))
		if lease := h.lease(); ptr.Deref(lease.Spec.HolderIdentity, "") != lodiav1alpha1.HoldHolderIdentity || ptr.Deref(lease.Spec.LeaseTransitions, 0) != 1 {
			t.Errorf("the take left %+v", lease.Spec)
		}
	})
	t.Run("an expired Lease", func(t *testing.T) {
		h := sampling(t, "a", "b", "c")
		renew(t, lodiav1alpha1.ControllerManagerLease, "another_1", time.Now())
		h.at(0)
		before := h.lease().ResourceVersion
		h.at(20)
		if h.lease().ResourceVersion == before {
			t.Fatal("a Lease unchanged for its duration was not taken")
		}
		h.expectSilent(taken(3, 3, "60"))
		if transitions := ptr.Deref(h.lease().Spec.LeaseTransitions, 0); transitions != 1 {
			t.Errorf("the take left %d transitions", transitions)
		}
	})
	t.Run("a Lease of a pod of this ControlPlane", func(t *testing.T) {
		h := sampling(t, "a", "b", "c")
		h.at(0)
		createManagerPod(t, h.cp, "dev-kube-controller-manager-7d9")
		renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", time.Now())
		h.at(20)
		h.expectSilent(taken(3, 3, "60"))
		if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
			t.Fatalf("a pod's Lease left holder %q", holder)
		}
	})
}

func TestATakeWritesItsOwnAcquireTimeOverTheLeaders(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	createManagerPod(t, h.cp, "dev-kube-controller-manager-7d9")
	h.acquiredBy("dev-kube-controller-manager-7d9_1", h.start.Add(-time.Hour))
	h.at(20)
	h.expectSilent(taken(3, 3, "60"))
	h.expectHealth(3, 3)
	if at := h.lease().Spec.AcquireTime; !at.Time.Equal(h.start.Add(20 * time.Second)) {
		t.Errorf("the take left acquireTime %v, want the time of the take", at)
	}
}

func TestRow15AnotherHolderIsNamedAndLeftAlone(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	renew(t, lodiav1alpha1.ControllerManagerLease, "a-watchdog_1", time.Now())
	before := h.lease()
	h.at(20)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, silent(3, 3)+atOrAbove+"60%, but a-watchdog_1 holds Lease kube-system/kube-controller-manager")
	h.expectNoHealth()
	h.expectUntouched(before)
}

func TestRow16BelowAHoldRenewsUntilReleaseAfter(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.back(25, 55)
	h.expectSilent(silent(0, 3) + ", below 60%" + holdsUntil)
	h.expectHealth(0, 3)
	h.renewedAt(55)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the hold ended early, holder %q", holder)
	}
}

func TestRow17BelowForReleaseAfterReleasesLikeClientGo(t *testing.T) {
	h := holding(t, "a", "b", "c")
	transitions := ptr.Deref(h.lease().Spec.LeaseTransitions, 0)
	h.back(25, 145)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
	h.expectNoHealth()
	lease := h.lease()
	at := h.start.Add(145 * time.Second)
	if ptr.Deref(lease.Spec.HolderIdentity, "x") != "" || ptr.Deref(lease.Spec.LeaseDurationSeconds, 0) != 1 ||
		!lease.Spec.AcquireTime.Time.Equal(at) || !lease.Spec.RenewTime.Time.Equal(at) || ptr.Deref(lease.Spec.LeaseTransitions, 0) != transitions {
		t.Errorf("the release left %+v", lease.Spec)
	}
}

func TestRow18BelowWithoutAHoldWritesNothing(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", time.Now())
	before := h.lease()
	h.at(0)
	h.back(5, 25)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
	h.expectNoHealth()
	h.expectUntouched(before)
}

func TestAMissingOrForeignKubeconfigWaitsForAccessWithNoCallToTheTenant(t *testing.T) {
	h := holding(t, "a", "b", "c")
	var called atomic.Bool
	h.inject(func(w http.ResponseWriter, req *http.Request) bool {
		called.Store(true)
		return false
	})
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: h.cp.Namespace, Name: lodiav1alpha1.LodiaKubeconfigSecret.Name(h.cp.Name)}}
	if err := k8s.Delete(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	before := h.lease()
	h.at(25)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, "waiting for PKI to write dev-lodia-kubeconfig")
	h.expectNoHealth()
	h.expectUntouched(before)
	if called.Load() {
		t.Error("a pass with no kubeconfig called the tenant")
	}
}

func TestAnUnparsableKubeconfigRestartsTheClockAndCallsNothing(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	writeKubeconfig(t, h.cp, []byte("not a kubeconfig"))
	h.now.SetTime(h.start.Add(15 * time.Second))
	sample(t, h.r, h.cp)
	writeKubeconfig(t, h.cp, tenantKubeconfig)
	h.at(25)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
	h.at(44)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
	if h.lease() != nil {
		t.Fatal("the pass after a restart took the Lease on a clock that did not run")
	}
}

func TestALeaseDeletedDuringAHoldIsCreatedAndSinceRestarts(t *testing.T) {
	h := holding(t, "a", "b", "c")
	first := get(t, h.cp).Status.Health.ControllerManagerHold.Since
	removeLeases(t)
	h.at(25)
	h.expectSilent(taken(3, 3, "60"))
	h.expectHealth(3, 3)
	since := get(t, h.cp).Status.Health.ControllerManagerHold.Since
	if !since.Time.Equal(h.start.Add(25*time.Second)) || since.Equal(first) {
		t.Errorf("status.health says since %v, want the new Lease's acquireTime, not %v", since, first)
	}
}

func TestAStatusApplyFailingAfterATakeIsRepairedByTheNextPass(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	h.r.Client = interceptor.NewClient(k8s, interceptor.Funcs{
		SubResourceApply: func(context.Context, client.Client, string, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
			return errors.New("injected")
		},
	})
	h.at(20)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the take left holder %q", holder)
	}
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
	h.expectNoHealth()

	h.r.Client = k8s
	h.at(25)
	h.expectSilent(taken(3, 3, "60"))
	h.expectHealth(3, 3)
	h.renewedAt(25)
}

func TestAPausedTenantWritesNothingAndItsHoldLapses(t *testing.T) {
	h := holding(t, "a", "b", "c")
	before := h.lease()
	updateSpec(t, h.cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	h.r.Client = noControlPlaneWrite(t, k8s)
	h.now.SetTime(h.start.Add(25 * time.Second))
	result, err := h.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.cp)})
	if err != nil || result != (ctrl.Result{}) {
		t.Fatalf("a pass over a paused ControlPlane returned %+v, %v", result, err)
	}
	h.expectUntouched(before)
}

func TestTheStatusHealthGoesWithNodesSilentOnly(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.expectHealth(3, 3)
	updateSpec(t, h.cp, func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.Health = &lodiav1alpha1.Health{HoldControllerManager: new(false)}
	})
	h.at(25)
	h.expectNoHealth()
	h.r.Client = noControlPlaneWrite(t, k8s)
	h.at(30)
}

func TestAPassThatChangesNothingWritesNothingDuringAHold(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.r.Client = noControlPlaneWrite(t, k8s)
	h.at(25)
	h.at(30)
	h.at(35)
}

func TestAnUnknownNodeCountsBackFromTheHoldsAcquireTime(t *testing.T) {
	h := sampling(t, "a")
	joined(t, "unknown", corev1.ConditionUnknown, h.start.Add(-4*time.Minute-30*time.Second))
	h.at(0)
	h.at(20)
	h.expectSilent(taken(2, 2, "60"))
	h.at(60)
	h.expectSilent(taken(2, 2, "60"))
	h.expectHealth(2, 2)
}

func TestAnotherHoldersAcquireTimeDecidesNoUnknownNode(t *testing.T) {
	h := sampling(t, "a")
	joined(t, "unknown", corev1.ConditionUnknown, h.start.Add(-4*time.Minute-50*time.Second))
	h.at(0)
	createManagerPod(t, h.cp, "dev-kube-controller-manager-7d9")
	h.acquiredBy("dev-kube-controller-manager-7d9_1", h.start)
	before := h.lease()
	h.at(20)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(1, 1)+fewerThanTwo)
	h.expectUntouched(before)
}

func TestWithoutAHoldUnknownCountsFromNow(t *testing.T) {
	h := sampling(t, "a")
	unknownSince := h.start.Add(-4*time.Minute - 50*time.Second)
	joined(t, "unknown", corev1.ConditionUnknown, unknownSince)
	h.at(0)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 2))
	h.at(10)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 1)+fewerThanTwo)
}

func TestAKCMWriteBetweenReadAndWriteRetriesAndWins(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	createManagerPod(t, h.cp, "dev-kube-controller-manager-7d9")
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", time.Now())
	var puts atomic.Int32
	h.inject(func(w http.ResponseWriter, req *http.Request) bool {
		if req.Method == http.MethodPatch && req.URL.Path == managerLease && puts.Add(1) == 1 {
			renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", time.Now())
		}
		return false
	})
	h.at(20)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the retry left holder %q", holder)
	}
	if puts.Load() != 2 {
		t.Errorf("the Lease was written %d times, want 2: the first conflicted, the second took it", puts.Load())
	}
	h.expectSilent(taken(3, 3, "60"))
}

func TestAConflictWhoseReReadFindsAHolderOutsideThePodsWritesNothing(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	createManagerPod(t, h.cp, "dev-kube-controller-manager-7d9")
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", time.Now())
	var puts atomic.Int32
	h.inject(func(w http.ResponseWriter, req *http.Request) bool {
		if req.Method == http.MethodPatch && req.URL.Path == managerLease && puts.Add(1) == 1 {
			renew(t, lodiav1alpha1.ControllerManagerLease, "a-watchdog_1", time.Now())
		}
		return false
	})
	h.at(20)
	if holder := h.holder(); holder != "a-watchdog_1" {
		t.Fatalf("the retry wrote over the holder: %q", holder)
	}
	if puts.Load() != 1 {
		t.Errorf("the Lease was written %d times, want 1", puts.Load())
	}
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, silent(3, 3)+atOrAbove+"60%, but a-watchdog_1 holds Lease kube-system/kube-controller-manager")
}

func TestAConflictWhoseReReadFailsAppliesNothingAndRestartsTheClock(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	renew(t, lodiav1alpha1.ControllerManagerLease, "", time.Now())
	var puts, gets atomic.Int32
	h.inject(func(w http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path != managerLease {
			return false
		}
		switch {
		case req.Method == http.MethodPatch && puts.Add(1) == 1:
			refuse(w, http.StatusConflict, metav1.StatusReasonConflict)
			return true
		case req.Method == http.MethodGet && gets.Add(1) == 2:
			refuse(w, http.StatusInternalServerError, metav1.StatusReasonInternalError)
			return true
		}
		return false
	})
	before := h.lease()
	h.r.Client = noControlPlaneWrite(t, k8s)
	h.at(20)
	h.r.Client = k8s
	h.expectUntouched(before)
	h.inject(nil)
	h.at(25)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
}

func TestAConflictThatRunsOutAppliesNothing(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	renew(t, lodiav1alpha1.ControllerManagerLease, "", time.Now())
	h.fail(managerLease, http.MethodPatch, http.StatusConflict, metav1.StatusReasonConflict)
	h.r.Client = noControlPlaneWrite(t, k8s)
	h.at(20)
}

func TestAWriteThatConflictsEveryTryIsRepeatedByTheNextPass(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	renew(t, lodiav1alpha1.ControllerManagerLease, "", time.Now())
	var patches atomic.Int32
	h.inject(func(w http.ResponseWriter, req *http.Request) bool {
		if req.Method != http.MethodPatch || req.URL.Path != managerLease {
			return false
		}
		patches.Add(1)
		refuse(w, http.StatusConflict, metav1.StatusReasonConflict)
		return true
	})
	h.at(20)
	if patches.Load() < 2 {
		t.Errorf("the write was tried %d times, want a retry on every conflict", patches.Load())
	}
	if holder := h.holder(); holder != "" {
		t.Fatalf("a write that conflicted every try left holder %q", holder)
	}
	h.inject(nil)
	h.at(25)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the next pass left holder %q, want its write repeated", holder)
	}
	h.expectSilent(taken(3, 3, "60"))
}

func TestAWriteKeepsWhatTheHoldDoesNotWriteOfTheLease(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	renew(t, lodiav1alpha1.ControllerManagerLease, "", time.Now())
	lease := h.lease()
	lease.Labels, lease.Annotations = map[string]string{"owner": "someone"}, map[string]string{"note": "kept"}
	if err := tenant.Update(t.Context(), lease); err != nil {
		t.Fatal(err)
	}
	stored := h.lease()
	if len(stored.Labels) == 0 || len(stored.Annotations) == 0 {
		t.Fatalf("the API server kept no label or annotation of the Lease: %+v", stored.ObjectMeta)
	}
	kept := func(step string) {
		t.Helper()
		got := h.lease()
		if !maps.Equal(got.Labels, stored.Labels) || !maps.Equal(got.Annotations, stored.Annotations) {
			t.Errorf("after %s the Lease has labels %v and annotations %v, want %v and %v", step, got.Labels, got.Annotations, stored.Labels, stored.Annotations)
		}
	}
	h.at(20)
	if h.holder() != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the take left holder %q", h.holder())
	}
	kept("the take")
	h.back(25, 130, 145)
	if h.holder() != "" {
		t.Fatalf("the release left holder %q", h.holder())
	}
	kept("the release")
}

func TestNeverFightsAHolderOutsideThePodsWhileItRenews(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	blocked := silent(3, 3) + atOrAbove + "60%, but a-watchdog_1 holds Lease kube-system/kube-controller-manager"
	for _, at := range []int{18, 35, 49} {
		renew(t, lodiav1alpha1.ControllerManagerLease, "a-watchdog_1", time.Now())
		before := h.lease()
		h.at(at + 2)
		h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, blocked)
		h.expectUntouched(before)
	}
	h.at(66)
	h.expectCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent, taken(3, 3, "60"))
}

func TestAHolderWhosePodIsGoneIsTakenOnlyOnceExpired(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-gone_1", time.Now())
	blocked := silent(3, 3) + atOrAbove + "60%, but dev-kube-controller-manager-gone_1 holds Lease kube-system/kube-controller-manager"
	h.at(20)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, blocked)
	h.at(34)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, blocked)
	if holder := h.holder(); holder == lodiav1alpha1.HoldHolderIdentity {
		t.Fatal("a holder not yet expired was taken over")
	}
	h.at(35)
	h.expectSilent(taken(3, 3, "60"))
}

func TestAHolderLikeAPodOfAnotherControlPlaneIsLeftAlone(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	other := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: h.cp.Namespace, Name: "other-kube-controller-manager", Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: "other"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "i"}}},
	}
	create(t, k8s, other)
	renew(t, lodiav1alpha1.ControllerManagerLease, "other-kube-controller-manager_1", time.Now())
	before := h.lease()
	h.at(20)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, silent(3, 3)+atOrAbove+"60%, but other-kube-controller-manager_1 holds Lease kube-system/kube-controller-manager")
	h.expectUntouched(before)
}

func TestAHolderLikeAPodOfTheSameNameInAnotherNamespaceIsLeftAlone(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	other := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace(t), Name: "dev-kube-controller-manager-7d9", Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: h.cp.Name}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "i"}}},
	}
	create(t, k8s, other)
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", time.Now())
	before := h.lease()
	h.at(20)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, silent(3, 3)+atOrAbove+"60%, but dev-kube-controller-manager-7d9_1 holds Lease kube-system/kube-controller-manager")
	h.expectUntouched(before)
}

func TestAHolderWhoseNameStartsLikeAPodIsLeftAlone(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	createManagerPod(t, h.cp, "dev-kube-controller-manager")
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-9_1", time.Now())
	before := h.lease()
	h.at(20)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, silent(3, 3)+atOrAbove+"60%, but dev-kube-controller-manager-9_1 holds Lease kube-system/kube-controller-manager")
	h.expectUntouched(before)
}

func TestTheReleaseComesAtExactlyReleaseAfterAndNotBefore(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.back(25, 144)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the hold ended 1 second before releaseAfter, holder %q", holder)
	}
	h.back(145)
	if holder := h.holder(); holder != "" {
		t.Fatalf("the hold did not end at exactly releaseAfter, holder %q", holder)
	}
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
}

func TestASecondHoldCountsTowardsItsReleaseAfresh(t *testing.T) {
	released := func(t *testing.T) *holdRun {
		t.Helper()
		h := holding(t, "a", "b", "c")
		h.back(25, 145)
		if holder := h.holder(); holder != "" {
			t.Fatalf("the first hold did not end, holder %q", holder)
		}
		return h
	}
	t.Run("after a take", func(t *testing.T) {
		h := released(t)
		h.at(165)
		h.expectSilent(taken(3, 3, "60"))
		h.back(170)
		h.expectSilent(silent(0, 3) + ", below 60%" + holdsUntil)
		h.back(289)
		if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
			t.Fatalf("the second hold ended on the first one's count, holder %q", holder)
		}
		h.back(290)
		if holder := h.holder(); holder != "" {
			t.Fatalf("the second hold did not end 2 minutes after its first pass below the fraction, holder %q", holder)
		}
	})
	t.Run("after another holder blocked it", func(t *testing.T) {
		h := released(t)
		renew(t, lodiav1alpha1.ControllerManagerLease, "a-watchdog_1", time.Now())
		h.at(165)
		h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, silent(3, 3)+atOrAbove+"60%, but a-watchdog_1 holds Lease kube-system/kube-controller-manager")
		renew(t, lodiav1alpha1.ControllerManagerLease, lodiav1alpha1.HoldHolderIdentity, time.Now())
		h.back(170)
		h.expectSilent(silent(0, 3) + ", below 60%" + holdsUntil)
		if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
			t.Fatalf("the hold ended on a count from before the blocked pass, holder %q", holder)
		}
	})
}

func TestAReturnAboveTheFractionRestartsTheTwoMinutes(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.back(25, 105)
	h.at(125)
	h.expectSilent(taken(3, 3, "60"))
	h.back(130, 249)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the two minutes were not restarted, holder %q", holder)
	}
	h.back(250)
	if holder := h.holder(); holder != "" {
		t.Fatalf("the hold did not end 2 minutes after the return, holder %q", holder)
	}
}

func TestAFailingReadEvery15SecondsKeepsTheHold(t *testing.T) {
	h := holding(t, "a", "b", "c")
	var failing atomic.Bool
	h.inject(func(w http.ResponseWriter, req *http.Request) bool {
		if failing.Load() && req.URL.Path == leaseListPath {
			refuse(w, http.StatusInternalServerError, metav1.StatusReasonInternalError)
			return true
		}
		return false
	})
	at := 20
	for range 12 {
		failing.Store(false)
		h.back(at + 5)
		h.expectSilent(silent(0, 3) + ", below 60%" + holdsUntil)
		failing.Store(true)
		h.back(at + 15)
		h.expectSilent(failedLeaseMsg + keepsHolding)
		at += 15
		if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
			t.Fatalf("%d s after the hold the failing read let it end, holder %q", at-20, holder)
		}
	}
	h.renewedAt(at)

	failing.Store(false)
	h.back(at+5, at+25, at+144)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the release came before the clock had run silentAfter and releaseAfter, holder %q", holder)
	}
	h.back(at + 145)
	if holder := h.holder(); holder != "" {
		t.Fatalf("the hold did not end at silentAfter and releaseAfter after the last failure, holder %q", holder)
	}
}

func TestAFailedReadOfTheLeaseClearsTheCount(t *testing.T) {
	h := holding(t, "a", "b", "c")
	var failing atomic.Bool
	h.inject(func(w http.ResponseWriter, req *http.Request) bool {
		if failing.Load() && req.URL.Path == managerLease && req.Method == http.MethodGet {
			refuse(w, http.StatusInternalServerError, metav1.StatusReasonInternalError)
			return true
		}
		return false
	})
	h.back(25, 144)
	failing.Store(true)
	h.at(145)
	failing.Store(false)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("a failed read released the hold, holder %q", holder)
	}
	h.back(150, 170, 289)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the count survived the failed read, holder %q", holder)
	}
	h.back(290)
	if holder := h.holder(); holder != "" {
		t.Fatalf("the hold did not end 2 minutes after the clock restarted, holder %q", holder)
	}
}

func TestARestartAdoptsTheHoldWithANodeListFailingLongerThanReleaseAfter(t *testing.T) {
	h := holding(t, "a", "b", "c")
	restarted := nodesReconciler(h.now)
	h.r = restarted
	h.fail(nodeListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	for at := 25; at <= 205; at += 30 {
		h.back(at)
		h.expectSilent(notListed + keepsHolding)
		h.renewedAt(at)
	}
	h.inject(nil)
	h.back(220)
	h.expectSilent(silent(0, 3) + ", below 60%" + holdsUntil)
	h.back(339)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the hold ended before releaseAfter of counted passes, holder %q", holder)
	}
	h.back(340)
	if holder := h.holder(); holder != "" {
		t.Fatalf("the adopted hold did not end, holder %q", holder)
	}
}

func TestARestartCountsNoPassTowardsTheReleaseBeforeSilentAfter(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.back(25, 55)
	h.r = nodesReconciler(h.now)
	h.back(60, 75, 80, 199)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the release counted passes the clock did not trust, holder %q", holder)
	}
	h.back(200)
	if holder := h.holder(); holder != "" {
		t.Fatalf("the hold did not end at silentAfter and releaseAfter after the restart, holder %q", holder)
	}
}

func TestALeaseListFailingLongerThan60SecondsKeepsRenewing(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.fail(leaseListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	for at := 25; at <= 115; at += 30 {
		h.at(at)
		h.expectSilent(failedLeaseMsg + keepsHolding)
		h.renewedAt(at)
		h.expectHealth(0, 0)
	}
}

func TestAWaitForAccessClearsTheCount(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.back(25, 130)
	setAccessAt(t, h.cp, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding, get(t, h.cp).Generation)
	h.back(135)
	h.expectSilent(notTrue + keepsHolding)
	setAccessAt(t, h.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonBound, get(t, h.cp).Generation)
	h.back(140, 160, 279)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the count survived the wait for access, holder %q", holder)
	}
	h.back(280)
	if holder := h.holder(); holder != "" {
		t.Fatalf("the hold did not end at silentAfter and releaseAfter after the wait, holder %q", holder)
	}
}

func TestAMissingKubeconfigClearsTheCount(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.back(25, 130)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: h.cp.Namespace, Name: lodiav1alpha1.LodiaKubeconfigSecret.Name(h.cp.Name)}}
	if err := k8s.Delete(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	h.at(135)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, "waiting for PKI to write dev-lodia-kubeconfig")
	writeKubeconfig(t, h.cp, tenantKubeconfig)
	h.back(140, 160, 279)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the count survived the missing kubeconfig, holder %q", holder)
	}
	h.back(280)
	if holder := h.holder(); holder != "" {
		t.Fatalf("the hold did not end after the missing kubeconfig, holder %q", holder)
	}
}

func TestAFailedReadOfTheKubeconfigRestartsTheCountAndKeepsTheClock(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.back(25, 55)
	reader := h.r.Reader
	h.r.Reader = interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Secret); ok {
				return errors.New("injected")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	h.at(80)
	h.r.Reader = reader
	h.back(85, 204)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the count survived the failed read of the kubeconfig, holder %q", holder)
	}
	h.back(205)
	if holder := h.holder(); holder != "" {
		t.Fatalf("the hold did not end 2 minutes after the first pass past the failed read, holder %q", holder)
	}
}

func TestAForeignFieldOfTheLeaseSurvivesEveryWrite(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: lodiav1alpha1.LeaderElectionNamespace, Name: lodiav1alpha1.ControllerManagerLease,
			Labels: map[string]string{"foreign.example.com/label": "kept"}, Annotations: map[string]string{"foreign.example.com/note": "kept"},
		},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: new(""), LeaseDurationSeconds: new(int32(15))},
	}
	create(t, tenant, lease)
	expectForeign := func(when string) {
		t.Helper()
		l := h.lease()
		if l.Labels["foreign.example.com/label"] != "kept" || l.Annotations["foreign.example.com/note"] != "kept" {
			t.Fatalf("%s dropped a foreign field: labels %v, annotations %v", when, l.Labels, l.Annotations)
		}
	}
	h.at(20)
	expectForeign("the take")
	h.at(25)
	expectForeign("the renewal")
	updateSpec(t, h.cp, func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.Health = &lodiav1alpha1.Health{HoldControllerManager: new(false)}
	})
	h.at(30)
	expectForeign("the release")
}

func TestAHoldOfOneCountedNodeEndsByTheRelease(t *testing.T) {
	h := holding(t, "a", "b")
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "b"}}
	if err := tenant.Delete(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	h.nodes = []string{"a"}
	h.back(30)
	h.expectSilent(silent(0, 1) + fewerThanTwo + ", below 60%" + holdsUntil)
	h.expectHealth(0, 1)
	h.back(149)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the hold ended before releaseAfter, holder %q", holder)
	}
	h.back(150)
	if holder := h.holder(); holder != "" {
		t.Fatalf("a hold of one counted node did not end, holder %q", holder)
	}
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 1)+fewerThanTwo)
}

func TestAHoldOfNoCountedNodeAfterTheOwnerDeletesThemEndsByTheRelease(t *testing.T) {
	h := holding(t, "a", "b", "c")
	if err := tenant.DeleteAllOf(t.Context(), &corev1.Node{}); err != nil {
		t.Fatal(err)
	}
	h.nodes = nil
	h.at(30)
	h.expectSilent(silent(0, 0) + fewerThanTwo + ", below 60%" + holdsUntil)
	h.at(149)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("the hold ended before releaseAfter, holder %q", holder)
	}
	h.at(150)
	if holder := h.holder(); holder != "" {
		t.Fatalf("a hold of no counted node did not end, holder %q", holder)
	}
}

func TestAHolderAtItsBoundIsQuotedWholeAndOneBytePastItIsAFailedRead(t *testing.T) {
	for name, letter := range map[string]string{"of ASCII": "h", "of two-byte characters": "я"} {
		holder := strings.Repeat(letter, 512/len(letter))
		t.Run(name+" at the bound", func(t *testing.T) {
			h := sampling(t, "a", "b", "c")
			h.at(0)
			renew(t, lodiav1alpha1.ControllerManagerLease, holder, time.Now())
			before := h.lease()
			h.at(20)
			h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, silent(3, 3)+atOrAbove+"60%, but "+holder+" holds Lease kube-system/kube-controller-manager")
			h.expectUntouched(before)
			expectNodes(t, h.cp, nodesOf(3, 3, 0, "v1.35.0"))
			h.r.Client = noControlPlaneWrite(t, k8s)
			h.at(25)
		})
		t.Run(name+" one byte past it", func(t *testing.T) {
			h := sampling(t, "a", "b", "c")
			h.at(0)
			renew(t, lodiav1alpha1.ControllerManagerLease, holder+"h", time.Now())
			before := h.lease()
			h.at(20)
			h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
			h.expectUntouched(before)
		})
	}
}

func TestALongRefusalIsCutInTheMessageAndTheStatusIsApplied(t *testing.T) {
	denyWith := func(answer func(http.ResponseWriter)) func(http.ResponseWriter, *http.Request) bool {
		return func(w http.ResponseWriter, req *http.Request) bool {
			if req.URL.Path != managerLease || req.Method != http.MethodPatch {
				return false
			}
			answer(w)
			return true
		}
	}
	text := denyWith(func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(strings.Repeat("d", 40000)))
	})
	denial := strings.Repeat("d", 1024)
	t.Run("of a renewal", func(t *testing.T) {
		h := holding(t, "a", "b", "c")
		h.inject(text)
		h.at(25)
		h.expectSilent("the API server refused the renewal of Lease kube-system/kube-controller-manager: " + denial)
		h.expectHealth(3, 3)
	})
	t.Run("of a take", func(t *testing.T) {
		h := sampling(t, "a", "b", "c")
		h.at(0)
		renew(t, lodiav1alpha1.ControllerManagerLease, "", time.Now())
		h.inject(text)
		h.at(20)
		h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, "the API server refused the take of Lease kube-system/kube-controller-manager: "+denial)
	})
	t.Run("whose Status says more than 1,024 bytes", func(t *testing.T) {
		h := holding(t, "a", "b", "c")
		h.inject(denyWith(func(w http.ResponseWriter) {
			refuseSaying(w, http.StatusForbidden, metav1.StatusReasonForbidden, strings.Repeat("d", 1025))
		}))
		h.at(25)
		h.expectSilent("the API server refused the renewal of Lease kube-system/kube-controller-manager: the server's message is longer than 1024 bytes")
	})
	t.Run("whose Status says 1,024 bytes", func(t *testing.T) {
		h := holding(t, "a", "b", "c")
		h.inject(denyWith(func(w http.ResponseWriter) {
			refuseSaying(w, http.StatusForbidden, metav1.StatusReasonForbidden, denial)
		}))
		h.at(25)
		h.expectSilent("the API server refused the renewal of Lease kube-system/kube-controller-manager: " + denial)
	})
}

func TestARefusedTakeIsHoldBlockedAndNeverNodesSilent(t *testing.T) {
	t.Run("an Update", func(t *testing.T) {
		h := sampling(t, "a", "b", "c")
		h.at(0)
		renew(t, lodiav1alpha1.ControllerManagerLease, "", time.Now())
		h.fail(managerLease, http.MethodPatch, http.StatusForbidden, metav1.StatusReasonForbidden)
		h.at(20)
		h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, "the API server refused the take of Lease kube-system/kube-controller-manager: injected: Forbidden")
		h.expectNoHealth()
		if holder := h.holder(); holder != "" {
			t.Fatalf("a refused take left holder %q", holder)
		}
	})
	t.Run("a Create", func(t *testing.T) {
		h := sampling(t, "a", "b", "c")
		h.at(0)
		h.fail(managerLeases, http.MethodPost, http.StatusForbidden, metav1.StatusReasonForbidden)
		h.at(20)
		h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonHoldBlocked, "the API server refused the take of Lease kube-system/kube-controller-manager: injected: Forbidden")
		h.expectNoHealth()
		if h.lease() != nil {
			t.Fatal("a refused Create wrote the Lease")
		}
	})
}

func TestARefusedRenewalKeepsNodesSilentWithTheLeaseStillLodias(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.fail(managerLease, http.MethodPatch, http.StatusForbidden, metav1.StatusReasonForbidden)
	before := h.lease()
	h.at(25)
	h.expectSilent("the API server refused the renewal of Lease kube-system/kube-controller-manager: injected: Forbidden")
	h.expectHealth(3, 3)
	h.expectUntouched(before)
}

func TestARefusedReleaseKeepsNodesSilentAndTriesAgain(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.back(25, 130)
	h.fail(managerLease, http.MethodPatch, http.StatusForbidden, metav1.StatusReasonForbidden)
	h.back(145)
	h.expectSilent("the API server refused the release of Lease kube-system/kube-controller-manager: injected: Forbidden")
	h.expectHealth(0, 3)
	if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
		t.Fatalf("a refused release left holder %q", holder)
	}
	h.inject(nil)
	h.back(150)
	if holder := h.holder(); holder != "" {
		t.Fatalf("the next pass did not release, holder %q", holder)
	}
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
	h.expectNoHealth()
}

func TestAWriteTimingOutOrFailingAppliesNothing(t *testing.T) {
	for name, method := range map[string]func(http.ResponseWriter, *http.Request) bool{
		"timing out": func(w http.ResponseWriter, req *http.Request) bool {
			if req.URL.Path != managerLease || req.Method != http.MethodPatch {
				return false
			}
			hang(w, req)
			return true
		},
		"a 500": func(w http.ResponseWriter, req *http.Request) bool {
			if req.URL.Path != managerLease || req.Method != http.MethodPatch {
				return false
			}
			refuse(w, http.StatusInternalServerError, metav1.StatusReasonInternalError)
			return true
		},
		"a 404": func(w http.ResponseWriter, req *http.Request) bool {
			if req.URL.Path != managerLease || req.Method != http.MethodPatch {
				return false
			}
			refuse(w, http.StatusNotFound, metav1.StatusReasonNotFound)
			return true
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := sampling(t, "a", "b", "c")
			h.r.Timeout = 300 * time.Millisecond
			h.at(0)
			h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
			renew(t, lodiav1alpha1.ControllerManagerLease, "", time.Now())
			h.inject(method)
			h.r.Client = noControlPlaneWrite(t, k8s)
			h.at(20)
			h.r.Client = k8s
			h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
			h.inject(nil)
			h.at(25)
			if holder := h.holder(); holder != lodiav1alpha1.HoldHolderIdentity {
				t.Fatalf("the next pass did not take the Lease, holder %q", holder)
			}
		})
	}
}

func TestACreateThatMeetsAlreadyExistsAppliesNothing(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	h.fail(managerLeases, http.MethodPost, http.StatusConflict, metav1.StatusReasonAlreadyExists)
	h.r.Client = noControlPlaneWrite(t, k8s)
	h.at(20)
	h.r.Client = k8s
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonNotHeld, silent(0, 3))
}

func TestAFailedPodListKeepsTheConditionAndTakesNothing(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", time.Now())
	before := h.lease()
	h.r.Reader = interceptor.NewClient(k8s, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return errors.New("injected")
			}
			return c.List(ctx, list, opts...)
		},
	})
	h.r.Client = noControlPlaneWrite(t, k8s)
	h.at(20)
	h.expectUntouched(before)
}

func TestThePodListGoesThroughTheHealthControllersRole(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	createManagerPod(t, h.cp, "dev-kube-controller-manager-7d9")
	renew(t, lodiav1alpha1.ControllerManagerLease, "dev-kube-controller-manager-7d9_1", time.Now())
	reader, err := client.New(healthRole, client.Options{Scheme: k8s.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	h.r.Reader = reader
	h.at(20)
	h.expectSilent(taken(3, 3, "60"))
}

func TestPodsAreListedOnlyOnTheBranchThatNeedsThem(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	var lists atomic.Int32
	h.r.Reader = interceptor.NewClient(k8s, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				lists.Add(1)
			}
			return c.List(ctx, list, opts...)
		},
	})
	h.at(0)
	h.at(20)
	h.expectSilent(taken(3, 3, "60"))
	h.back(25)
	if lists.Load() != 0 {
		t.Errorf("the pods were listed %d times with no holder and with Lodia's own hold", lists.Load())
	}
}

func TestAHolderLodiaReadsHeldAndAStaleSchedulerNotLeading(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	leaders := &health.Reconciler{Client: k8s, Reader: k8s, FailuresBeforeFalse: failuresBeforeFalse, DialContext: dialService, Period: 5 * time.Second, Timeout: 2 * time.Second, Clock: now}
	renew(t, lodiav1alpha1.ControllerManagerLease, lodiav1alpha1.HoldHolderIdentity, start)
	renew(t, lodiav1alpha1.SchedulerLease, "dev-kube-scheduler-5c4_2", start)
	reconcile(t, leaders, cp)
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonHeld, "Lodia holds the leader Lease of kube-controller-manager, and that of kube-scheduler is renewed"); err != nil {
		t.Fatal(err)
	}
	now.SetTime(start.Add(26 * time.Second))
	reconcile(t, leaders, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonSchedulerNotLeading,
		"Lease kube-system/kube-scheduler of dev-kube-scheduler-5c4_2 was last renewed "+start.UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
}

func TestAHeldLeaseOfNoRecentRenewalStaysHeld(t *testing.T) {
	cp := tenantControlPlane(t)
	start := time.Now().Truncate(time.Second)
	now := clocktesting.NewFakePassiveClock(start)
	leaders := &health.Reconciler{Client: k8s, Reader: k8s, FailuresBeforeFalse: failuresBeforeFalse, DialContext: dialService, Period: 5 * time.Second, Timeout: 2 * time.Second, Clock: now}
	renew(t, lodiav1alpha1.ControllerManagerLease, lodiav1alpha1.HoldHolderIdentity, start)
	renew(t, lodiav1alpha1.SchedulerLease, "dev-kube-scheduler-5c4_2", start)
	reconcile(t, leaders, cp)
	now.SetTime(start.Add(26 * time.Second))
	renew(t, lodiav1alpha1.SchedulerLease, "dev-kube-scheduler-5c4_2", start.Add(26*time.Second))
	reconcile(t, leaders, cp)
	if err := expectLeading(t, cp, metav1.ConditionTrue, lodiav1alpha1.ReasonHeld, "Lodia holds the leader Lease of kube-controller-manager, and that of kube-scheduler is renewed"); err != nil {
		t.Fatal(err)
	}
}
