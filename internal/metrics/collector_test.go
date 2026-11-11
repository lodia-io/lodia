// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package metrics_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/metrics"
	"go.lodia.io/lodia/internal/operator"
	"go.lodia.io/lodia/internal/testenv"
)

const goldenFile = "testdata/metrics.prom"

var update = flag.Bool("update", false, "rewrite "+goldenFile+" from the collector")

var (
	env  *testenv.Env
	role *rest.Config
)

func TestMain(m *testing.M) {
	if !testenv.Available() {
		os.Exit(m.Run())
	}
	env = testenv.Start(testenv.Options{})
	role = env.AsController("lifecycle")
	os.Exit(env.Finish(m.Run()))
}

func elected() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func collector(objs ...client.Object) *metrics.Collector {
	return collectorOf(fake.NewClientBuilder().WithScheme(operator.Scheme()).WithObjects(objs...).Build(), elected())
}

func collectorOf(reader client.Reader, elected <-chan struct{}) *metrics.Collector {
	lag := metrics.NewSampleLag()
	for _, seconds := range []float64{0, 0, 0.3, 12} {
		lag.Observe(seconds)
	}
	failures := metrics.NewProbeFailures()
	failures.WithLabelValues(lodiav1alpha1.MetricProbeAPI).Add(3)
	failures.WithLabelValues(lodiav1alpha1.MetricProbeTunnel).Inc()
	return &metrics.Collector{Reader: reader, Elected: elected, Revision: running, MaxConcurrentRollouts: 3, NodeCounts: goldenCounts, SampleLag: lag, ProbeFailures: failures}
}

func TestEveryShapeOfControlPlaneMatchesTheGolden(t *testing.T) {
	c := collector(golden()...)
	if *update {
		registry := prometheus.NewPedanticRegistry()
		registry.MustRegister(c)
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		var text bytes.Buffer
		encoder := expfmt.NewEncoder(&text, expfmt.NewFormat(expfmt.TypeTextPlain))
		for _, family := range families {
			if err := encoder.Encode(family); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(goldenFile, text.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := testutil.CollectAndCompare(c, bytes.NewReader(want)); err != nil {
		t.Error(err)
	}
}

func TestATypeAbsentFromStatusHasNoSeries(t *testing.T) {
	cp := controlPlane("young", 10)
	cp.Status.Conditions = []metav1.Condition{{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonReconciling, Message: "m", LastTransitionTime: *ago(time.Minute)}}
	want := `
# HELP lodia_controlplane_condition 1 for the status of each condition in status.conditions and 0 for its other two; a type absent from status has no series.
# TYPE lodia_controlplane_condition gauge
lodia_controlplane_condition{controlplane="young",namespace="tenants",status="False",type="Ready"} 1
lodia_controlplane_condition{controlplane="young",namespace="tenants",status="True",type="Ready"} 0
lodia_controlplane_condition{controlplane="young",namespace="tenants",status="Unknown",type="Ready"} 0
# HELP lodia_controlplane_condition_reason 1 for the reason of each condition in status.conditions.
# TYPE lodia_controlplane_condition_reason gauge
lodia_controlplane_condition_reason{controlplane="young",namespace="tenants",reason="Reconciling",type="Ready"} 1
`
	if err := testutil.CollectAndCompare(collector(cp), strings.NewReader(want), "lodia_controlplane_condition", "lodia_controlplane_condition_reason"); err != nil {
		t.Error(err)
	}
}

func TestUnknownIsItsOwnSeries(t *testing.T) {
	cp := controlPlane("young", 10)
	cp.Status.Conditions = []metav1.Condition{{Type: lodiav1alpha1.ConditionReachable, Status: metav1.ConditionUnknown, Reason: lodiav1alpha1.ReasonReconciling, Message: "m", LastTransitionTime: *ago(time.Minute)}}
	want := `
# HELP lodia_controlplane_condition 1 for the status of each condition in status.conditions and 0 for its other two; a type absent from status has no series.
# TYPE lodia_controlplane_condition gauge
lodia_controlplane_condition{controlplane="young",namespace="tenants",status="False",type="Reachable"} 0
lodia_controlplane_condition{controlplane="young",namespace="tenants",status="True",type="Reachable"} 0
lodia_controlplane_condition{controlplane="young",namespace="tenants",status="Unknown",type="Reachable"} 1
`
	if err := testutil.CollectAndCompare(collector(cp), strings.NewReader(want), "lodia_controlplane_condition"); err != nil {
		t.Error(err)
	}
}

func TestAnAbsentKubernetesVersionKeepsTheInfoSeries(t *testing.T) {
	without := controlPlane("young", 10)
	upgrading := controlPlane("stepping", 11)
	upgrading.Status.Kubernetes = &lodiav1alpha1.KubernetesStatus{Upgrade: &lodiav1alpha1.UpgradeStatus{Step: lodiav1alpha1.UpgradeStepAPIServer, StartedAt: *ago(time.Minute)}}
	want := `
# HELP lodia_controlplane_info 1 for every ControlPlane: its UID, status.kubernetes.version, empty while absent, spec.layout, and the members of spec.datastore and spec.endpoint that are set.
# TYPE lodia_controlplane_info gauge
lodia_controlplane_info{controlplane="stepping",datastore="dedicated",endpoint_mode="loadBalancer",kubernetes_version="",layout="Split",namespace="tenants",uid="00000000-0000-0000-0000-000000000011"} 1
lodia_controlplane_info{controlplane="young",datastore="dedicated",endpoint_mode="loadBalancer",kubernetes_version="",layout="Split",namespace="tenants",uid="00000000-0000-0000-0000-000000000010"} 1
`
	if err := testutil.CollectAndCompare(collector(without, upgrading), strings.NewReader(want), "lodia_controlplane_info"); err != nil {
		t.Error(err)
	}
}

func TestTwoCollectionsAtOnceAgree(t *testing.T) {
	want, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector(golden()...))
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() { errs[i] = testutil.GatherAndCompare(registry, bytes.NewReader(want)) })
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Error(err)
	}
}

func TestAStandbyServesFromTheScrapeAfterItIsElected(t *testing.T) {
	c := collector(golden()...)
	elected := make(chan struct{})
	c.Elected = elected
	if n := testutil.CollectAndCount(c); n != 0 {
		t.Fatalf("a standby emits %d series, want none", n)
	}
	close(elected)
	want, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := testutil.CollectAndCompare(c, bytes.NewReader(want)); err != nil {
		t.Error(err)
	}
}

func TestARecreatedControlPlaneIsServedUnderItsNewUID(t *testing.T) {
	requireEnv(t)
	ns := namespace(t)
	cp := gatewayHA("recreated", 21)
	create(t, ns, cp)
	reader := startCache(t, nil)
	c := collectorOf(reader, elected())
	uids := func() ([]string, error) {
		registry := prometheus.NewPedanticRegistry()
		registry.MustRegister(c)
		families, err := registry.Gather()
		if err != nil {
			return nil, err
		}
		var out []string
		for _, family := range families {
			for _, m := range family.GetMetric() {
				labels := map[string]string{}
				for _, l := range m.GetLabel() {
					labels[l.GetName()] = l.GetValue()
				}
				if labels["namespace"] == ns && labels["controlplane"] == cp.Name && family.GetName() == "lodia_controlplane_info" {
					out = append(out, labels["uid"])
				}
			}
		}
		return out, nil
	}
	first := string(cp.UID)
	testenv.Eventually(t, func() error {
		got, err := uids()
		if err != nil {
			return err
		}
		if len(got) != 1 || got[0] != first {
			return fmt.Errorf("the info series carry the UIDs %v, want %s", got, first)
		}
		return nil
	})
	if err := env.Client.Delete(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	successor := gatewayHA("recreated", 22)
	create(t, ns, successor)
	testenv.Eventually(t, func() error {
		got, err := uids()
		if err != nil {
			return err
		}
		if len(got) != 1 || got[0] != string(successor.UID) {
			return fmt.Errorf("the info series carry the UIDs %v, want only the successor's %s", got, successor.UID)
		}
		return nil
	})
}

func TestAStandbyEmitsNothingAndStartsNoInformer(t *testing.T) {
	requireEnv(t)
	create(t, namespace(t), gatewayHA("standby", 20))
	var requests atomic.Int32
	reader := startCache(t, func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "controlplanes") {
			requests.Add(1)
		}
		return next.RoundTrip(req)
	})
	c := collectorOf(reader, make(chan struct{}))
	if n := testutil.CollectAndCount(c); n != 0 {
		t.Errorf("a standby emits %d series, want none", n)
	}
	if n := requests.Load(); n != 0 {
		t.Errorf("a standby's collection sent %d requests for ControlPlanes, want none", n)
	}
}

func TestACollectionBeforeTheInformerSyncsEndsWithinTheDeadline(t *testing.T) {
	requireEnv(t)
	release := make(chan struct{})
	reader := startCache(t, func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/controlplanes") {
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-release:
				return nil, errors.New("released")
			}
		}
		return next.RoundTrip(req)
	})
	t.Cleanup(func() { close(release) })
	c := collectorOf(reader, elected())
	start := time.Now()
	collected := make(chan int, 1)
	go func() { collected <- testutil.CollectAndCount(c) }()
	select {
	case n := <-collected:
		if elapsed := time.Since(start); elapsed > 6*time.Second {
			t.Errorf("the collection took %s, want at most the 5 s deadline", elapsed)
		}
		if n != 0 {
			t.Errorf("a collection on an informer that has not synced emits %d series, want none", n)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the collection did not return in 15 s")
	}
}

func BenchmarkCollect(b *testing.B) {
	if env == nil {
		b.Skip("KUBEBUILDER_ASSETS is not set; run make test")
	}
	ns := namespace(b)
	b.Cleanup(func() {
		if err := env.Client.DeleteAllOf(context.Background(), &lodiav1alpha1.ControlPlane{}, client.InNamespace(ns)); err != nil {
			b.Error(err)
		}
	})
	for i := range 200 {
		cp := gatewayHA(fmt.Sprintf("tenant-%03d", i), 100+i)
		cp.Status.Maintenance = nil
		create(b, ns, cp)
	}
	reader := startCache(b, nil)
	registry := prometheus.NewRegistry()
	c := collectorOf(reader, elected())
	c.NodeCounts = func(types.NamespacedName, types.UID) (int, int, bool) { return 1, 4, true }
	registry.MustRegister(c)
	var series, size int
	b.ReportAllocs()
	for b.Loop() {
		families, err := registry.Gather()
		if err != nil {
			b.Fatal(err)
		}
		var text bytes.Buffer
		encoder := expfmt.NewEncoder(&text, expfmt.NewFormat(expfmt.TypeTextPlain))
		series = 0
		for _, family := range families {
			series += len(family.GetMetric())
			if err := encoder.Encode(family); err != nil {
				b.Fatal(err)
			}
		}
		size = text.Len()
	}
	b.ReportMetric(float64(series), "series")
	b.ReportMetric(float64(size), "text-bytes")
}

func requireEnv(t *testing.T) {
	t.Helper()
	if env == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set; run make test")
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func startCache(tb testing.TB, intercept func(*http.Request, http.RoundTripper) (*http.Response, error)) cache.Cache {
	tb.Helper()
	cfg := rest.CopyConfig(role)
	if intercept != nil {
		cfg.WrapTransport = func(next http.RoundTripper) http.RoundTripper {
			return roundTripper(func(req *http.Request) (*http.Response, error) { return intercept(req, next) })
		}
	}
	options, err := operator.Cache(false)
	if err != nil {
		tb.Fatal(err)
	}
	options.Scheme = operator.Scheme()
	c, err := cache.New(cfg, options)
	if err != nil {
		tb.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- c.Start(ctx) }()
	tb.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			tb.Errorf("cache: %v", err)
		}
	})
	if !c.WaitForCacheSync(ctx) {
		tb.Fatal("the cache did not sync")
	}
	return c
}

func namespace(tb testing.TB) string {
	tb.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "metrics-"}}
	if err := env.Client.Create(tb.Context(), ns); err != nil {
		tb.Fatal(err)
	}
	return ns.Name
}

func create(tb testing.TB, ns string, cp *lodiav1alpha1.ControlPlane) {
	tb.Helper()
	status := cp.Status
	cp.Namespace, cp.UID, cp.Generation = ns, "", 0
	if err := env.Client.Create(tb.Context(), cp); err != nil {
		tb.Fatal(err)
	}
	cp.Status = status
	if err := env.Client.Status().Update(tb.Context(), cp); err != nil {
		tb.Fatal(err)
	}
}
