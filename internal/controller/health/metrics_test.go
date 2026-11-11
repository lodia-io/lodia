// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/health"
	"go.lodia.io/lodia/internal/metrics"
)

type lagged struct {
	count, above10 uint64
	sum            float64
}

func lagOf(t *testing.T, lag prometheus.Histogram) lagged {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(lag)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	h := families[0].GetMetric()[0].GetHistogram()
	got := lagged{count: h.GetSampleCount(), sum: h.GetSampleSum()}
	for _, b := range h.GetBucket() {
		if b.GetUpperBound() == 10 {
			got.above10 = got.count - b.GetCumulativeCount()
		}
	}
	return got
}

func lagging(t *testing.T) (*holdRun, prometheus.Histogram) {
	t.Helper()
	h := sampling(t, "a", "b", "c")
	lag := metrics.NewSampleLag()
	h.r.SampleLag = lag
	return h, lag
}

func (h *holdRun) taking(at, took time.Duration) {
	h.t.Helper()
	h.inject(func(_ http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path == managerLease && req.Method == http.MethodGet {
			h.now.SetTime(h.now.Now().Add(took))
		}
		return false
	})
	defer h.inject(nil)
	h.now.SetTime(h.start.Add(at))
	result, err := h.r.Reconcile(h.t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.cp)})
	if err != nil {
		h.t.Fatal(err)
	}
	if want := max(h.r.Period-took, time.Second); result.RequeueAfter != want {
		h.t.Fatalf("a pass of %s requeues after %s, want %s", took, result.RequeueAfter, want)
	}
}

func (h *holdRun) expectLag(lag prometheus.Histogram, want lagged) {
	h.t.Helper()
	if got := lagOf(h.t, lag); got != want {
		h.t.Fatalf("the lag holds %+v, want %+v", got, want)
	}
}

func TestAPassTwelveSecondsPastItsDueTimeLandsAboveTheTenSecondBucket(t *testing.T) {
	h, lag := lagging(t)
	h.taking(0, 3*time.Second)
	h.at(5 + 12)
	h.expectLag(lag, lagged{count: 1, above10: 1, sum: 12})
}

func TestAPassOnTimeOrEarlyObservesZero(t *testing.T) {
	h, lag := lagging(t)
	h.taking(0, 3*time.Second)
	h.at(5)
	h.at(8)
	h.expectLag(lag, lagged{count: 2})
}

func TestAPassAfterOneThatRanPastThePeriodObservesZeroOnTime(t *testing.T) {
	h, lag := lagging(t)
	h.taking(0, 7*time.Second)
	h.at(8)
	h.expectLag(lag, lagged{count: 1})
}

func TestLeaseReadsFailingFor30SecondsObserveNothingAboveTenSeconds(t *testing.T) {
	h, lag := lagging(t)
	h.at(0)
	h.inject(func(w http.ResponseWriter, req *http.Request) bool {
		if !strings.HasSuffix(req.URL.Path, "/leases") && req.URL.Path != managerLease {
			return false
		}
		http.Error(w, "injected", http.StatusInternalServerError)
		return true
	})
	for at := 5; at <= 30; at += 5 {
		h.at(at)
	}
	h.inject(nil)
	h.at(35)
	h.expectLag(lag, lagged{count: 7})
}

func TestAfterAFailedReadOfTheControlPlaneTheNextPassObservesNothing(t *testing.T) {
	h, lag := lagging(t)
	h.at(0)
	h.at(20)
	h.expectLag(lag, lagged{count: 1, above10: 1, sum: 15})
	key := client.ObjectKeyFromObject(h.cp)
	if _, _, ok := h.r.Counts(key, h.cp.UID); !ok {
		t.Fatal("a pass 20 seconds after the first published no count")
	}
	injected := errors.New("injected: the cache did not answer")
	h.r.Client = interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				return injected
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	h.at(25)
	if _, _, ok := h.r.Counts(key, h.cp.UID); ok {
		t.Error("a pass that could not read the ControlPlane kept its count")
	}
	h.r.Client = k8s
	h.at(30 + 12)
	h.expectLag(lag, lagged{count: 1, above10: 1, sum: 15})
	h.at(47)
	h.expectLag(lag, lagged{count: 2, above10: 1, sum: 15})
}

func TestTheFirstPassAndTheFirstAfterAForgetObserveNothing(t *testing.T) {
	h, lag := lagging(t)
	h.at(0)
	h.expectLag(lag, lagged{})
	updateSpec(t, h.cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	h.now.SetTime(h.start.Add(5 * time.Second))
	if _, err := h.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.cp)}); err != nil {
		t.Fatal(err)
	}
	updateSpec(t, h.cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = false })
	h.at(5 + 12)
	h.expectLag(lag, lagged{})
	h.at(22)
	h.expectLag(lag, lagged{count: 1})
}

func expectCounts(t *testing.T, r *health.NodesReconciler, cp *lodiav1alpha1.ControlPlane, silent, counted int) {
	t.Helper()
	gotSilent, gotCounted, ok := r.Counts(client.ObjectKeyFromObject(cp), cp.UID)
	if !ok || gotSilent != silent || gotCounted != counted {
		t.Fatalf("the counts are %d of %d (%t), want %d of %d", gotSilent, gotCounted, ok, silent, counted)
	}
}

func expectNoCounts(t *testing.T, r *health.NodesReconciler, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	if silent, counted, ok := r.Counts(client.ObjectKeyFromObject(cp), cp.UID); ok {
		t.Fatalf("the counts are %d of %d, want none", silent, counted)
	}
}

func (h *holdRun) expectCountsOfTheHold() {
	h.t.Helper()
	hold := get(h.t, h.cp).Status.Health.ControllerManagerHold
	expectCounts(h.t, h.r, h.cp, int(hold.SilentNodes), int(hold.CountedNodes))
}

func TestTheCountsWaitForTheClockAndFollowTheHold(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	expectNoCounts(t, h.r, h.cp)
	h.at(19)
	expectNoCounts(t, h.r, h.cp)
	h.at(20)
	h.expectHealth(3, 3)
	h.expectCountsOfTheHold()
	h.back(25)
	h.expectHealth(0, 3)
	h.expectCountsOfTheHold()
}

func TestAPassWithoutAListOfTheLeasesLeavesNoCount(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.expectCountsOfTheHold()
	h.fail(leaseListPath, http.MethodGet, http.StatusInternalServerError, metav1.StatusReasonInternalError)
	h.at(25)
	h.expectSilent(failedLeaseMsg + keepsHolding)
	expectNoCounts(t, h.r, h.cp)
	h.inject(nil)
	h.at(30)
	expectNoCounts(t, h.r, h.cp)
	h.at(50)
	expectCounts(t, h.r, h.cp, 3, 3)
}

func TestAHoldTurnedOffLeavesNoCount(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.expectCountsOfTheHold()
	updateSpec(t, h.cp, func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.Health = &lodiav1alpha1.Health{HoldControllerManager: new(false)}
	})
	h.at(25)
	h.expectCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonDisabled, disabled)
	expectNoCounts(t, h.r, h.cp)
}

func TestAPassWhoseStatusApplyFailsStillPublishesItsCount(t *testing.T) {
	h := sampling(t, "a", "b", "c")
	h.at(0)
	h.r.Client = interceptor.NewClient(k8s, interceptor.Funcs{
		SubResourceApply: func(context.Context, client.Client, string, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
			return errors.New("injected")
		},
	})
	h.at(20)
	h.expectNoHealth()
	expectCounts(t, h.r, h.cp, 3, 3)
}

func TestARestartedOperatorCountsAgainOnlyOnceItsClockHasRunSilentAfter(t *testing.T) {
	h := holding(t, "a", "b", "c")
	h.expectCountsOfTheHold()
	h.r = nodesReconciler(h.now)
	h.at(25)
	h.expectSilent(silent(0, 3) + ", below 60%" + holdsUntil)
	expectNoCounts(t, h.r, h.cp)
	h.at(45)
	h.expectCountsOfTheHold()
}

func TestAForgottenOrRecreatedControlPlaneHasNoCount(t *testing.T) {
	h, lag := lagging(t)
	h.at(0)
	h.at(20)
	h.expectCountsOfTheHold()
	updateSpec(t, h.cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	h.now.SetTime(h.start.Add(25 * time.Second))
	if _, err := h.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.cp)}); err != nil {
		t.Fatal(err)
	}
	expectNoCounts(t, h.r, h.cp)

	updateSpec(t, h.cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = false })
	h.at(30)
	h.at(50)
	h.expectCountsOfTheHold()
	predecessor := h.cp
	if err := k8s.Delete(t.Context(), predecessor); err != nil {
		t.Fatal(err)
	}
	h.cp = controlPlaneIn(t, predecessor.Namespace, predecessor.Spec.Endpoint)
	setAccess(t, h.cp)
	expectNoCounts(t, h.r, h.cp)
	h.at(55)
	expectNoCounts(t, h.r, h.cp)
	h.expectLag(lag, lagged{count: 2, above10: 2, sum: 30})
	if _, _, ok := h.r.Counts(client.ObjectKeyFromObject(predecessor), predecessor.UID); ok {
		t.Error("the successor's pass left the predecessor's count")
	}
	h.at(75)
	expectCounts(t, h.r, h.cp, 3, 3)
}

func TestEachFailedProbeCountsOnceByItsPath(t *testing.T) {
	f := newGateway(t)
	f.publishWorking(t)
	failures := metrics.NewProbeFailures()
	probe := f.probe(500 * time.Millisecond)
	probe.ProbeFailures = failures
	expectFailures := func(when string, want map[string]float64) {
		t.Helper()
		for _, path := range []string{lodiav1alpha1.MetricProbeAPI, lodiav1alpha1.MetricProbeInternal, lodiav1alpha1.MetricProbeTunnel, lodiav1alpha1.MetricProbeKonnectivity} {
			if got := testutil.ToFloat64(failures.WithLabelValues(path)); got != want[path] {
				t.Errorf("%s: path %s counts %v failures, want %v", when, path, got, want[path])
			}
		}
	}
	reconcile(t, probe, f.cp)
	expectFailures("every path answered", nil)

	f.api.code.Store(http.StatusServiceUnavailable)
	f.tunnel.code.Store(http.StatusBadGateway)
	reconcile(t, probe, f.cp)
	if err := expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered()); err != nil {
		t.Fatal(err)
	}
	expectFailures("the first pass with two paths failing", map[string]float64{lodiav1alpha1.MetricProbeAPI: 1, lodiav1alpha1.MetricProbeTunnel: 1})
	reconcile(t, probe, f.cp)
	expectFailures("the second", map[string]float64{lodiav1alpha1.MetricProbeAPI: 2, lodiav1alpha1.MetricProbeTunnel: 2})

	f.api.code.Store(http.StatusOK)
	f.tunnel.code.Store(http.StatusOK)
	f.api.service.code.Store(http.StatusServiceUnavailable)
	publishGateway(t, f.cp, f.apiAddress(), deadAddress(t), f.tunnel.address)
	reconcile(t, probe, f.cp)
	expectFailures("the in-namespace Service and konnectivity failing", map[string]float64{
		lodiav1alpha1.MetricProbeAPI: 2, lodiav1alpha1.MetricProbeTunnel: 2, lodiav1alpha1.MetricProbeInternal: 1, lodiav1alpha1.MetricProbeKonnectivity: 1,
	})
}

func TestOutsideGatewayModeAFailedPublicEndpointCountsAsAPI(t *testing.T) {
	cp := controlPlane(t)
	e := serve(t)
	writeBundle(t, cp, e)
	publish(t, cp, e)
	failures := metrics.NewProbeFailures()
	probe := newReconciler(500 * time.Millisecond)
	probe.ProbeFailures = failures
	e.code.Store(http.StatusServiceUnavailable)
	reconcile(t, probe, cp)
	if got := testutil.ToFloat64(failures.WithLabelValues(lodiav1alpha1.MetricProbeAPI)); got != 1 {
		t.Errorf("the public endpoint's failure counts %v under api, want 1", got)
	}
	if got := testutil.ToFloat64(failures.WithLabelValues(lodiav1alpha1.MetricProbeInternal)); got != 0 {
		t.Errorf("the in-namespace Service, which answered, counts %v failures, want none", got)
	}
}

func TestCollectionsDuringPassesShareNothingUnlocked(t *testing.T) {
	h, lag := lagging(t)
	elected := make(chan struct{})
	close(elected)
	collector := &metrics.Collector{Reader: k8s, Elected: elected, MaxConcurrentRollouts: 1, NodeCounts: h.r.Counts, SampleLag: lag, ProbeFailures: metrics.NewProbeFailures()}
	stop := make(chan struct{})
	stopped := make(chan struct{})
	var collections atomic.Int32
	go func() {
		defer close(stopped)
		for {
			select {
			case <-stop:
				return
			default:
				testutil.CollectAndCount(collector)
				collections.Add(1)
			}
		}
	}()
	for at := 0; at <= 60; at += 5 {
		h.at(at)
	}
	close(stop)
	<-stopped
	if collections.Load() < 2 {
		t.Fatalf("%d collections ran beside the passes", collections.Load())
	}
	want := `
# HELP lodia_controlplane_nodes_silent The nodes that count whose Lease Health has not seen change for health.silentAfter, in its last list of the node Leases. Absent while that list is missing or the tenant's hold is off, and until Health's clock of the Leases has run health.silentAfter.
# TYPE lodia_controlplane_nodes_silent gauge
lodia_controlplane_nodes_silent{controlplane="dev",namespace="` + h.cp.Namespace + `"} 3
`
	if err := testutil.CollectAndCompare(collector, strings.NewReader(want), "lodia_controlplane_nodes_silent"); err != nil {
		t.Error(err)
	}
}
