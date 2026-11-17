// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package fleet runs the operator against a synthetic fleet of ControlPlanes
// on envtest, with their tenants and the management cluster's other
// controllers stood in for, and measures what it costs (docs/operations.md,
// Scale).
package fleet

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/controller/health"
	"go.lodia.io/lodia/internal/controller/tenant"
	"go.lodia.io/lodia/internal/operator"
)

const (
	// tenantsEnv is the size of the fleet; the test runs only when it is set.
	tenantsEnv = "LODIA_FLEET_TENANTS"
	// hangingEnv is the share of the fleet whose tenants hang, 0.1 when unset.
	hangingEnv = "LODIA_FLEET_HANGING"
	// reportEnv, when set, is a file the report is also written to.
	reportEnv = "LODIA_FLEET_REPORT"

	nodesPerTenant = 3
	restWindow     = time.Minute
	hangWindow     = 60 * time.Second
	boundsWindow   = 30 * time.Second
	settledFor     = 5 * time.Second
	// idleTimeout outlasts client-go's IdleConnTimeout of 90 s, after which
	// the connections Tenant and PKI left idle in the creation are closed.
	idleTimeout  = 100 * time.Second
	firstVersion = "v0.0.0-fleet"
	nextVersion  = "v0.0.1-fleet"
	thirdVersion = "v0.0.2-fleet"
	defaultWave  = 10 * time.Minute
)

// periodic are the controllers that pass every tenant on a period of their
// own, which a settled fleet keeps doing.
var periodic = []string{health.Name, health.NodesName, datastore.SampleName, tenant.Name}

func TestMain(m *testing.M) {
	if settings := os.Getenv(operatorEnv); settings != "" {
		os.Exit(runOperator(settings))
	}
	os.Exit(m.Run())
}

type fleet struct {
	t        *testing.T
	cfg      *rest.Config
	admin    client.Client
	cache    cache.Cache
	platform *platform
	tenants  *tenantServer
	settings operatorSettings
	dir      string
	op       *operatorProcess
	size     int
	lines    []string
	atRest   map[string]float64
}

func (f *fleet) report(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	f.lines = append(f.lines, line)
	f.t.Log(line)
}

func TestFleet(t *testing.T) {
	size, hanging := knobs(t)
	dir := t.TempDir()
	if keep := os.Getenv(reportEnv); keep != "" {
		dir = filepath.Dir(keep)
	}
	f := startFleet(t, dir)
	f.report("fleet of %d gateway tenants on one Gateway, %d nodes each, %.0f%% of them hanging in their phase; GOMAXPROCS %s, %s", size, nodesPerTenant, hanging*100, os.Getenv("GOMAXPROCS"), runtimeVersion())
	f.op = startOperator(t, dir, f.cfg, f.settings)

	f.create(1, size)
	f.rest()
	f.idleConnections()
	f.objectSize()
	f.updateOne()
	f.updateAll()
	f.batch()
	f.op.stop(t)
	f.lists()
	f.restart()
	f.wave()
	f.hang(hanging)
	f.bound()
	f.defaultWave()
	f.write()
}

func TestEveryConnectionToATenantGoesThroughTheDial(t *testing.T) {
	dir := t.TempDir()
	f := startFleet(t, dir)
	f.op = startOperator(t, dir, f.cfg, f.settings)
	f.createOnly(1, 2)
	f.waitFor("every ControlPlane Ready, its nodes counted", time.Now(), 2*time.Minute, func(cp *lodiav1alpha1.ControlPlane) bool {
		return readyAt(cp) && cp.Status.Nodes != nil && cp.Status.Nodes.Total == nodesPerTenant
	})
}

func TestTheRestIsSubtractedForEachTenantTheFleetHasInTheWindow(t *testing.T) {
	f := &fleet{size: 1000}
	f.rested(map[string]int64{"get secrets": 50_000}, time.Minute)
	f.size = 1050
	if got, want := f.net(map[string]int64{"get secrets": 30_000, "patch services": 7}, 30*time.Second), map[string]int64{"get secrets": 3_750, "patch services": 7}; !maps.Equal(got, want) {
		t.Errorf("30 s of a fleet of 1,050 beside a rest of 50,000 reads a minute at 1,000 tenants nets %v, want %v", got, want)
	}
	if got, want := f.net(map[string]int64{"get secrets": 26_000, "patch services": 7}, 30*time.Second), map[string]int64{"patch services": 7}; !maps.Equal(got, want) {
		t.Errorf("30 s of a fleet of 1,050 that read fewer Secrets than at rest nets %v, want %v", got, want)
	}
	if got, want := f.less(map[string]int64{"get secrets": 7, "patch services": 1}, 6*time.Second, 1), map[string]int64{"get secrets": 2, "patch services": 1}; !maps.Equal(got, want) {
		t.Errorf("6 s of one tenant nets %v, want %v", got, want)
	}
}

func startFleet(t *testing.T, dir string) *fleet {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run make fleet")
	}
	logs, err := os.Create(filepath.Join(dir, "platform.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logs.Close() })
	ctrl.SetLogger(logger(logs))
	cfg, admin := startManagement(t)
	grantOperator(t, admin)
	f := &fleet{t: t, cfg: cfg, admin: admin, dir: dir}
	f.platform = startPlatform(t, cfg, admin, logs)
	f.cache = f.platform.mgr.GetCache()
	f.tenants = startTenants(t, admin, cfg, nodesPerTenant)
	f.settings = operatorSettings{Tenants: f.tenants.address(), Etcd: startEtcd(t), Version: firstVersion, MaxConcurrentRollouts: operator.DefaultConfig().Fleet.MaxConcurrentRollouts}
	return f
}

func knobs(t *testing.T) (int, float64) {
	raw := os.Getenv(tenantsEnv)
	if raw == "" {
		t.Skip(tenantsEnv + " is not set; run make fleet")
	}
	size, err := strconv.Atoi(raw)
	if err != nil || size < 1 {
		t.Fatalf("%s is %q, want a positive number", tenantsEnv, raw)
	}
	hanging := 0.1
	if raw := os.Getenv(hangingEnv); raw != "" {
		if hanging, err = strconv.ParseFloat(raw, 64); err != nil || hanging < 0 || hanging >= 1 {
			t.Fatalf("%s is %q, want a share from 0 to below 1", hangingEnv, raw)
		}
	}
	return size, hanging
}

func runtimeVersion() string {
	return fmt.Sprintf("%d CPUs, %s %s/%s", runtime.NumCPU(), runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

func startEtcd(t *testing.T) string {
	t.Helper()
	member := testserver.NewTestConfig(t)
	server, err := embed.StartEtcd(member)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(time.Minute):
		t.Fatal("the etcd member took too long to start")
	}
	return member.ListenClientUrls[0].String()
}

func (f *fleet) create(from, to int) {
	start := time.Now()
	created := f.createOnly(from, to)
	s := sample(f.op)
	times := f.waitFor("every ControlPlane Ready", start, max(10*time.Minute, time.Duration(to-from+1)*2*time.Second), readyAt)
	peak, _, _ := s.end()
	f.report("create %d tenants: created in %s, all Ready %s after the first; each Ready after the start: %s; peak %s",
		to-from+1, created.Round(time.Second), time.Since(start).Round(time.Second), quantiles(since(times, from)), peak)
}

func since(times map[string]time.Duration, from int) []time.Duration {
	var out []time.Duration
	for ns, d := range times {
		if i, _ := strconv.Atoi(strings.TrimPrefix(ns, "fleet-")); i >= from {
			out = append(out, d)
		}
	}
	return out
}

func (f *fleet) createOnly(from, to int) time.Duration {
	start := time.Now()
	indices := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed error
	for range 16 {
		wg.Go(func() {
			for i := range indices {
				ns := namespaceOf(i)
				f.tenants.add(ns)
				err := f.admin.Create(f.t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
				if err == nil {
					err = f.admin.Create(f.t.Context(), controlPlane(i))
				}
				if err != nil {
					mu.Lock()
					failed = err
					mu.Unlock()
				}
			}
		})
	}
	for i := from; i <= to; i++ {
		indices <- i
	}
	close(indices)
	wg.Wait()
	if failed != nil {
		f.t.Fatal(failed)
	}
	f.size = to
	return time.Since(start)
}

func (f *fleet) list() []lodiav1alpha1.ControlPlane {
	list := &lodiav1alpha1.ControlPlaneList{}
	if err := f.cache.List(f.t.Context(), list); err != nil {
		f.t.Fatal(err)
	}
	return list.Items
}

func readyAt(cp *lodiav1alpha1.ControlPlane) bool {
	c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady)
	return c != nil && c.Status == metav1.ConditionTrue && c.ObservedGeneration == cp.Generation
}

func (f *fleet) waitReady(start time.Time, timeout time.Duration) {
	f.t.Helper()
	f.waitFor("every ControlPlane Ready", start, timeout, readyAt)
}

func (f *fleet) waitFor(what string, start time.Time, timeout time.Duration, done func(*lodiav1alpha1.ControlPlane) bool) map[string]time.Duration {
	f.t.Helper()
	deadline, said := start.Add(timeout), time.Now()
	seen := map[string]time.Duration{}
	for {
		items := f.list()
		waiting := map[string]int{}
		for i := range items {
			if !done(&items[i]) {
				waiting[waitsFor(&items[i])]++
			} else if _, ok := seen[items[i].Namespace]; !ok {
				seen[items[i].Namespace] = time.Since(start)
			}
		}
		if len(items) == f.size && len(waiting) == 0 {
			return seen
		}
		if time.Since(said) > 30*time.Second {
			said = time.Now()
			f.t.Logf("waiting for %s, %s in: %d of %d ControlPlanes wait: %s", what, time.Since(start).Round(time.Second), total(intCounts(waiting)), f.size, counts(intCounts(waiting), 0))
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("not %s within %s: %s", what, timeout, counts(intCounts(waiting), 0))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func intCounts(m map[string]int) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = int64(v)
	}
	return out
}

func waitsFor(cp *lodiav1alpha1.ControlPlane) string {
	c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady)
	if c == nil {
		return "no Ready"
	}
	reason := c.Reason
	if first, _, ok := strings.Cut(c.Message, ":"); ok && c.Status != metav1.ConditionTrue {
		reason = first + " " + reason
	}
	return fmt.Sprintf("Ready %s %s at %d of %d", c.Status, reason, c.ObservedGeneration, cp.Generation)
}

func (f *fleet) settle(timeout time.Duration) (operatorStats, time.Time) {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	var last operatorStats
	var changed time.Time
	for {
		s, err := f.op.read(f.t.Context())
		if err != nil {
			f.t.Fatal(err)
		}
		if why := busy(last, s); !changed.IsZero() && why == "" {
			if time.Since(changed) >= settledFor {
				return s, changed
			}
		} else {
			if time.Now().After(deadline) {
				f.t.Fatalf("the operator did not settle within %s: %s", timeout, why)
			}
			last, changed = s, time.Now()
		}
		time.Sleep(time.Second)
	}
}

func busy(a, b operatorStats) string {
	writes := map[string]int64{}
	for k, v := range b.Requests {
		if verb, _, _ := strings.Cut(k, " "); slices.Contains([]string{"create", "update", "patch", "delete"}, verb) && v != a.Requests[k] {
			writes[k] = v - a.Requests[k]
		}
	}
	queued := map[string]float64{}
	for name, n := range b.Depth {
		if n > 0 && !slices.Contains(periodic, name) {
			queued[name] = n
		}
	}
	if len(writes) == 0 && len(queued) == 0 {
		return ""
	}
	return fmt.Sprintf("writes %s; queued %s", counts(writes, 0), counts(queued, 0))
}

func (f *fleet) rested(requests map[string]int64, window time.Duration) {
	f.atRest = map[string]float64{}
	for k, v := range requests {
		f.atRest[k] = float64(v) / window.Seconds() / float64(f.size)
	}
}

// net is requests less what the fleet's tenants, as many as it has now, send
// at rest in window.
func (f *fleet) net(requests map[string]int64, window time.Duration) map[string]int64 {
	return f.less(requests, window, f.size)
}

func (f *fleet) less(requests map[string]int64, window time.Duration, tenants int) map[string]int64 {
	out := map[string]int64{}
	for k, v := range requests {
		if n := v - int64(math.Round(f.atRest[k]*window.Seconds()*float64(tenants))); n > 0 {
			out[k] = n
		}
	}
	return out
}

func (f *fleet) rest() {
	f.settle(10 * time.Minute)
	time.Sleep(idleTimeout)
	before, err := f.op.read(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	f.tenants.record.reset()
	s := sample(f.op)
	refreshed := f.platform.refresh.Load()
	time.Sleep(restWindow)
	peak, last, _ := s.end()
	after, err := f.op.read(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	intervals := f.tenants.record.take()
	d := diff(before, after)
	f.rested(d.requests, after.at.Sub(before.at))
	secrets := float64(d.requests["get secrets"]) / restWindow.Seconds()
	f.report("at rest, %d tenants, over %s: %.1f requests/s to the API server: %s", f.size, restWindow, float64(total(d.requests))/restWindow.Seconds(), counts(d.requests, restWindow))
	f.report("at rest: uncached Secret reads %.1f/s, %.2f per tenant per 5 s; status writes %.2f/s; etcd-druid's refresh %.1f status writes/s",
		secrets, secrets*5/float64(f.size), float64(d.requests[statusWrites])/restWindow.Seconds(), float64(f.platform.refresh.Load()-refreshed)/restWindow.Seconds())
	f.report("at rest: passes %s; errors %s", counts(d.reconciles, restWindow), counts(d.errors, 0))
	f.report("at rest: health passes a tenant every %s; health-nodes every %s", quantiles(intervals[kindProbe+"/"+classHonest]), quantiles(intervals[kindNodeLeases+"/"+classHonest]))
	f.report("at rest: memory at the end %s; peak %s; %s a tenant by RSS, %s by heap in use", last, peak, kib(last.RSS, f.size), kib(last.HeapInuse, f.size))
}

func (f *fleet) idleConnections() {
	type reading struct {
		held       uint64
		rss        uint64
		cpu        float64
		handshakes int64
		idle       int
		by         map[string]int64
	}
	measure := func() reading {
		var r reading
		start, err := f.op.read(f.t.Context())
		if err != nil {
			f.t.Fatal(err)
		}
		shook := f.tenants.handshakes.Load()
		var idle []int
		for range 3 {
			time.Sleep(5 * time.Second)
			r.by = f.tenants.idle()
			idle = append(idle, int(total(r.by)))
			var s operatorStats
			if err := f.op.get(f.t.Context(), "/stats?gc", &s); err != nil {
				f.t.Fatal(err)
			}
			rss, err := f.op.rss(f.t.Context())
			if err != nil {
				f.t.Fatal(err)
			}
			r.held, r.rss = r.held+(s.HeapAlloc+s.Stacks)/3, r.rss+rss/3
		}
		end, err := f.op.read(f.t.Context())
		if err != nil {
			f.t.Fatal(err)
		}
		slices.Sort(idle)
		r.cpu, r.handshakes, r.idle = (end.CPU-start.CPU)/15, (f.tenants.handshakes.Load()-shook)/15, idle[1]
		return r
	}
	f.settle(5 * time.Minute)
	kept := measure()
	f.tenants.closing.Store(true)
	time.Sleep(2 * settledFor)
	closed := measure()
	f.tenants.closing.Store(false)
	conns := float64(kept.idle - closed.idle)
	f.report("idle connections by the client that last used them: kept alive %s; closing %s", counts(kept.by, 0), counts(closed.by, 0))
	f.report("idle connections: kept alive, the tenants hold %d idle, see %d handshakes/s, and the operator holds %s of live heap and stacks, %s RSS, spending %.2f CPU; with every answer closing its connection %d idle, %d handshakes/s, %s, %s, %.2f CPU",
		kept.idle, kept.handshakes, mib(kept.held), mib(kept.rss), kept.cpu, closed.idle, closed.handshakes, mib(closed.held), mib(closed.rss), closed.cpu)
	f.report("idle connections: %.0f KiB of live heap and stacks and %.0f KiB of RSS for each idle connection; %.2f ms of CPU for each handshake more",
		(float64(kept.held)-float64(closed.held))/conns/1024, (float64(kept.rss)-float64(closed.rss))/conns/1024, (closed.cpu-kept.cpu)*1000/float64(closed.handshakes-kept.handshakes))
}

func kib(b uint64, tenants int) string {
	return fmt.Sprintf("%.0f KiB", float64(b)/1024/float64(tenants))
}

func (f *fleet) objectSize() {
	var whole, status, managed []int
	for i := 1; i <= min(f.size, 20); i++ {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(lodiav1alpha1.GroupVersion.WithKind("ControlPlane"))
		if err := f.admin.Get(f.t.Context(), types.NamespacedName{Namespace: namespaceOf(i), Name: controlPlaneName}, u); err != nil {
			f.t.Fatal(err)
		}
		size := func(v any) int {
			out, err := json.Marshal(v)
			if err != nil {
				f.t.Fatal(err)
			}
			return len(out)
		}
		whole, status, managed = append(whole, size(u.Object)), append(status, size(u.Object["status"])), append(managed, size(u.GetManagedFields()))
	}
	median := func(xs []int) int { slices.Sort(xs); return xs[len(xs)/2] }
	f.report("a ControlPlane at rest: %d bytes of JSON (most %d), of which status %d and managedFields %d", median(whole), slices.Max(whole), median(status), median(managed))
}

func (f *fleet) updateOne() {
	ns := namespaceOf(1)
	base, _ := f.settle(5 * time.Minute)
	if err := f.op.get(f.t.Context(), "/track?namespace="+ns, nil); err != nil {
		f.t.Fatal(err)
	}
	time.Sleep(settledFor)
	quiet, err := f.op.read(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	background := diff(base, quiet)
	f.label(1, "one")
	after, _ := f.settle(time.Minute)
	d := diff(quiet, after)
	window := after.at.Sub(quiet.at)
	f.report("one ControlPlane updated (a label): passes %s (beside the fleet's %s in as long before it); its namespace's requests %s, less the reads a tenant sends at rest in the %s it took to settle: %s",
		counts(only(d.reconciles), 0), counts(only(background.reconciles), 0), counts(d.tracked, 0), window.Round(time.Second), counts(f.less(d.tracked, window, 1), 0))
}

func only(m map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for k, v := range m {
		if !slices.Contains(periodic, k) {
			out[k] = v
		}
	}
	return out
}

func (f *fleet) label(i int, value string) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: namespaceOf(i), Name: controlPlaneName}}
	patch := []byte(`{"metadata":{"labels":{"fleet.test/touched":"` + value + `"}}}`)
	if err := f.admin.Patch(f.t.Context(), cp, client.RawPatch(types.MergePatchType, patch)); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fleet) updateAll() {
	before, _ := f.settle(5 * time.Minute)
	s := sample(f.op)
	start := time.Now()
	f.each(1, f.size, func(i int) { f.label(i, "all") })
	after, changed := f.settle(10 * time.Minute)
	took := changed.Sub(start)
	peak, _, _ := s.end()
	d := diff(before, after)
	caused := f.net(d.requests, after.at.Sub(before.at))
	f.report("every ControlPlane updated at once: settled in %s; passes %s; requests %d, of which besides those of %d tenants at rest over %s %d: %s; peak %s",
		took.Round(time.Second), counts(only(d.reconciles), 0), total(d.requests), f.size, after.at.Sub(before.at).Round(time.Second), total(caused), counts(caused, 0), peak)
}

func (f *fleet) each(from, to int, do func(int)) {
	indices := make(chan int)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for i := range indices {
				do(i)
			}
		})
	}
	for i := from; i <= to; i++ {
		indices <- i
	}
	close(indices)
	wg.Wait()
}

func (f *fleet) batch() {
	f.settle(5 * time.Minute)
	n := max(5, f.size/20)
	routes := &gatewayv1.TLSRouteList{}
	if err := f.admin.List(f.t.Context(), routes); err != nil {
		f.t.Fatal(err)
	}
	start := time.Now()
	from := f.size + 1
	f.createOnly(from, f.size+n)
	accepted := f.waitFor("every route Accepted", start, 10*time.Minute, func(cp *lodiav1alpha1.ControlPlane) bool {
		return meta.IsStatusConditionTrue(cp.Status.Conditions, lodiav1alpha1.ConditionRoutesReady)
	})
	ready := f.waitFor("every ControlPlane Ready", start, 10*time.Minute, readyAt)
	f.report("a batch of %d gateway tenants beside %d routes: RoutesReady after the batch's start %s; Ready %s", n, len(routes.Items), quantiles(since(accepted, from)), quantiles(since(ready, from)))
}

func (f *fleet) lists() {
	f.platform.paused.Store(true)
	defer f.platform.paused.Store(false)
	cached, cachedBytes, err := timed(20, func() error { return f.cache.List(f.t.Context(), &lodiav1alpha1.ControlPlaneList{}) })
	if err != nil {
		f.t.Fatal(err)
	}
	reader, wire, err := uncached(f.cfg)
	if err != nil {
		f.t.Fatal(err)
	}
	read := func(list client.ObjectList) (time.Duration, uint64, int64) {
		wire.n.Store(0)
		took, allocated, err := timed(10, func() error { return reader.List(f.t.Context(), list) })
		if err != nil {
			f.t.Fatal(err)
		}
		return took, allocated, wire.n.Load() / 10
	}
	cps, cpsBytes, cpsWire := read(&lodiav1alpha1.ControlPlaneList{})
	routes := &gatewayv1.TLSRouteList{}
	tls, tlsBytes, tlsWire := read(routes)
	shared, sharedBytes, err := timed(20, func() error {
		return f.cache.List(f.t.Context(), &lodiav1alpha1.ControlPlaneList{}, client.UnsafeDisableDeepCopy)
	})
	if err != nil {
		f.t.Fatal(err)
	}
	f.report("a list of the %d ControlPlanes from the cache with copies, a gateway tenant's pass: %s, %s allocated", f.size, cached, mib(cachedBytes))
	f.report("a list of the %d ControlPlanes from the cache without copies, a slot's count: %s, %s allocated", f.size, shared, mib(sharedBytes))
	f.report("an uncached list of the %d ControlPlanes, the first grant of an operator: %s, %s on the wire, %s allocated", f.size, cps, mib(uint64(cpsWire)), mib(cpsBytes))
	f.report("an uncached list of the %d TLSRoutes, the first route write of an operator on a Gateway: %s, %s on the wire, %s allocated", len(routes.Items), tls, mib(uint64(tlsWire)), mib(tlsBytes))
}

func (f *fleet) restart() {
	s := f.start(f.settings)
	f.waitReady(f.op.started, 10*time.Minute)
	after, changed := f.settle(30 * time.Minute)
	took := changed.Sub(f.op.started)
	peak, last, _ := s.end()
	caused := f.net(after.Requests, after.at.Sub(f.op.started))
	f.report("a restart over %d tenants: settled in %s; passes %s; requests %d, of which besides those of %d tenants at rest over %s %d: %s", f.size, took.Round(time.Second), counts(after.Reconciles, 0), total(after.Requests),
		f.size, after.at.Sub(f.op.started).Round(time.Second), total(caused), counts(caused, 0))
	f.report("a restart: peak %s; after it %s", peak, last)
}

func (f *fleet) start(settings operatorSettings) *sampler {
	f.op = startOperator(f.t, f.dir, f.cfg, settings)
	return sample(f.op)
}

func (f *fleet) wave() {
	f.op.stop(f.t)
	settings := f.settings
	settings.Version, settings.MaxConcurrentRollouts = nextVersion, f.size
	s := f.start(settings)
	revision, err := operator.Inputs{Version: nextVersion, SignerImage: "registry.example/lodia-signer:fleet", TunnelImage: "registry.example/lodia-tunnel:fleet",
		EtcdDruidVersion: druidVersion, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace}.Revision()
	if err != nil {
		f.t.Fatal(err)
	}
	f.waitFor("every tenant on the next revision", f.op.started, 30*time.Minute, func(cp *lodiav1alpha1.ControlPlane) bool { return rolled(cp, revision) && readyAt(cp) })
	rolledIn := time.Since(f.op.started)
	f.waitFor("every slot free", f.op.started, 30*time.Minute, func(cp *lodiav1alpha1.ControlPlane) bool {
		return readyAt(cp) && (cp.Status.Maintenance == nil || cp.Status.Maintenance.SlotSince == nil)
	})
	freed := time.Since(f.op.started) - rolledIn
	after, changed := f.settle(30 * time.Minute)
	took := max(changed.Sub(f.op.started), rolledIn+freed)
	peak, _, busiest := s.end()
	f.report("a wave over %d tenants with %d slots: every tenant on the new revision and Ready in %s, settled in %s; status writes %d, %.1f/s, at most %d in a second",
		f.size, f.size, rolledIn.Round(time.Second), took.Round(time.Second), after.Requests[statusWrites], float64(after.Requests[statusWrites])/took.Seconds(), busiest)
	f.report("a wave: passes %s; requests %d: %s; peak %s", counts(after.Reconciles, 0), total(after.Requests), counts(after.Requests, 0), peak)
}

func rolled(cp *lodiav1alpha1.ControlPlane, revision string) bool {
	s := cp.Status
	if s.Target == nil || s.Target.OperatorRevision != revision || s.Addons == nil || s.Addons.OperatorRevision != revision || s.Datastore == nil || s.Datastore.OperatorRevision != revision || s.Components == nil {
		return false
	}
	for _, c := range []*lodiav1alpha1.ComponentStatus{s.Components.APIServer, s.Components.ControllerManager, s.Components.Scheduler} {
		if c == nil || c.OperatorRevision != revision {
			return false
		}
	}
	return true
}

func (f *fleet) pick(n int) []int {
	var out []int
	for k := range n {
		out = append(out, 1+k*f.size/n)
	}
	return out
}

func (f *fleet) hang(share float64) {
	n := int(math.Round(share * float64(f.size)))
	if n == 0 {
		return
	}
	f.settle(5 * time.Minute)
	picked := f.pick(n)
	for _, i := range picked {
		classes.Store(namespaceOf(i), classHanging)
		f.tenants.tenant(namespaceOf(i)).hanging.Store(true)
	}
	before, err := f.op.read(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	f.tenants.record.reset()
	s := sample(f.op)
	time.Sleep(hangWindow)
	peak, _, _ := s.end()
	after, err := f.op.read(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	intervals := f.tenants.record.take()
	for _, i := range picked {
		f.tenants.tenant(namespaceOf(i)).hanging.Store(false)
		classes.Delete(namespaceOf(i))
	}
	d := diff(before, after)
	f.report("%d of %d tenants hanging, over %s: health passes an honest tenant every %s; health-nodes every %s; passes %s; peak %s",
		n, f.size, hangWindow, quantiles(intervals[kindProbe+"/"+classHonest]), quantiles(intervals[kindNodeLeases+"/"+classHonest]), counts(d.reconciles, hangWindow), peak)
	f.waitReady(time.Now(), 10*time.Minute)
}

func (f *fleet) bound() {
	n := max(1, min(100, f.size/10))
	f.settle(5 * time.Minute)
	picked := f.pick(n)
	for _, i := range picked {
		classes.Store(namespaceOf(i), classBounded)
		f.tenants.tenant(namespaceOf(i)).bounded.Store(true)
	}
	f.tenants.record.reset()
	s := sample(f.op)
	time.Sleep(boundsWindow)
	peak, last, _ := s.end()
	intervals := f.tenants.record.take()
	for _, i := range picked {
		f.tenants.tenant(namespaceOf(i)).bounded.Store(false)
		classes.Delete(namespaceOf(i))
	}
	f.report("%d of %d tenants answering at Health's bounds, over %s: peak %s; at the end %s; health-nodes passes an honest tenant every %s, a bounded one every %s",
		n, f.size, boundsWindow, peak, last, quantiles(intervals[kindNodeLeases+"/"+classHonest]), quantiles(intervals[kindNodeLeases+"/"+classBounded]))
}

func (f *fleet) defaultWave() {
	f.op.stop(f.t)
	settings := f.settings
	settings.Version = thirdVersion
	s := f.start(settings)
	revision, err := operator.Inputs{Version: thirdVersion, SignerImage: "registry.example/lodia-signer:fleet", TunnelImage: "registry.example/lodia-tunnel:fleet",
		EtcdDruidVersion: druidVersion, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace}.Revision()
	if err != nil {
		f.t.Fatal(err)
	}
	granted := func() (int, int) {
		g, waiting := 0, 0
		for _, cp := range f.list() {
			switch {
			case cp.Status.Target != nil && cp.Status.Target.OperatorRevision == revision:
				g++
			case cp.Status.Target != nil && cp.Status.Target.ScheduledFor != nil:
				waiting++
			}
		}
		return g, waiting
	}
	time.Sleep(time.Minute)
	from, err := f.op.read(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	before, _ := granted()
	time.Sleep(defaultWave - time.Minute)
	to, err := f.op.read(f.t.Context())
	if err != nil {
		f.t.Fatal(err)
	}
	after, waiting := granted()
	peak, _, _ := s.end()
	window := to.at.Sub(from.at)
	d := diff(from, to)
	caused := f.net(d.requests, window)
	f.report("a wave at the default %d slots, from its second minute to its %s: %.1f tenants granted a minute, %d granted and %d queued at the end; passes %s; requests besides those of %d tenants at rest %.1f/s: %s; peak %s",
		settings.MaxConcurrentRollouts, defaultWave, float64(after-before)/window.Minutes(), after, waiting, counts(only(d.reconciles), window), f.size, float64(total(caused))/window.Seconds(), counts(caused, window), peak)
}

func (f *fleet) write() {
	if path := os.Getenv(reportEnv); path != "" {
		if err := os.WriteFile(path, []byte(strings.Join(f.lines, "\n")+"\n"), 0o600); err != nil {
			f.t.Error(err)
		}
	}
	if n := f.tenants.unexpected.Load(); n > 0 {
		f.t.Errorf("the stand-in tenants answered %d requests they do not serve; the operator's log is in %s", n, f.dir)
	}
}
