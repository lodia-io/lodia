// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
	"go.lodia.io/lodia/internal/metrics"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/util/workqueue"
	_ "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sigs.k8s.io/yaml"
)

const (
	ruleFile      = "../../charts/lodia/files/rules/lodia.yaml"
	dashboardGlob = "../../charts/lodia/files/dashboards/*.json"
	goldenFile    = "../../internal/metrics/testdata/metrics.prom"
)

var (
	fqName        = regexp.MustCompile(`fqName: "([^"]+)"`)
	rangeVariable = regexp.MustCompile(`\[\$\{?\w+(:\w+)?\}?\]`)
	variable      = regexp.MustCompile(`\$\{?\w+(:\w+)?\}?`)
	labelValues   = regexp.MustCompile(`^label_values\((.+),\s*([A-Za-z_]\w*)\)$`)

	histogramSuffixes = []string{"_bucket", "_count", "_sum"}
	targetLabels      = []string{"job", "instance", "namespace", "service", "endpoint", "pod", "container"}
)

func TestMain(m *testing.M) {
	if err := fillControllerRuntimesRegistry(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func fillControllerRuntimesRegistry() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := controller.NewUnmanaged("monitoring", controller.Options{
		Reconciler: reconcile.Func(func(context.Context, reconcile.Request) (reconcile.Result, error) {
			cancel()
			return reconcile.Result{}, nil
		}),
	})
	if err != nil {
		return err
	}
	if err := c.Watch(source.Func(func(_ context.Context, queue workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
		queue.Add(reconcile.Request{})
		return nil
	})); err != nil {
		return err
	}
	if err := c.Start(ctx); err != nil {
		return err
	}
	_, err = leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          &resourcelock.LeaseLock{LockConfig: resourcelock.ResourceLockConfig{Identity: "monitoring"}},
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks:     leaderelection.LeaderCallbacks{OnStartedLeading: func(context.Context) {}, OnStoppedLeading: func() {}},
		Name:          "lodia.io",
	})
	return err
}

type family struct {
	kind   dto.MetricType
	labels map[string]bool
}

type served struct {
	described map[string]bool
	families  map[string]family
}

func (s served) lookup(name string) (family, string, string) {
	if f, ok := s.families[name]; ok {
		if f.kind == dto.MetricType_HISTOGRAM {
			return family{}, "", "the bare name of a histogram, which a scrape serves as its _bucket, _count and _sum alone"
		}
		if f.kind == dto.MetricType_SUMMARY {
			return f, "quantile", ""
		}
		return f, "", ""
	}
	for _, suffix := range histogramSuffixes {
		base, ok := strings.CutSuffix(name, suffix)
		f, found := s.families[base]
		if !ok || !found {
			continue
		}
		switch {
		case f.kind == dto.MetricType_HISTOGRAM && suffix == "_bucket":
			return f, "le", ""
		case f.kind == dto.MetricType_HISTOGRAM || f.kind == dto.MetricType_SUMMARY && suffix != "_bucket":
			return f, "", ""
		}
	}
	for _, suffix := range append([]string{""}, histogramSuffixes...) {
		base, ok := strings.CutSuffix(name, suffix)
		if _, found := s.families[base]; ok && !found && s.described[base] {
			return family{}, "", "described, but with no series in the test process, so its type and labels are unknown"
		}
	}
	return family{}, "", "which neither Lodia's collector nor controller-runtime's registry serves"
}

func servedMetrics(t *testing.T) served {
	t.Helper()
	s := served{described: map[string]bool{}, families: map[string]family{}}
	lodia := &metrics.Collector{SampleLag: metrics.NewSampleLag(), ProbeFailures: metrics.NewProbeFailures()}
	for _, c := range []prometheus.Collector{lodia, ctrlmetrics.Registry.(prometheus.Collector)} {
		descs := make(chan *prometheus.Desc)
		go func() {
			defer close(descs)
			c.Describe(descs)
		}()
		for desc := range descs {
			match := fqName.FindStringSubmatch(desc.String())
			if match == nil {
				t.Fatalf("a collector describes %s, which has no name", desc)
			}
			s.described[match[1]] = true
		}
	}
	gathered, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	textParser := expfmt.NewTextParser(model.UTF8Validation)
	parsed, err := textParser.TextToMetricFamilies(bytes.NewReader(golden))
	if err != nil {
		t.Fatal(err)
	}
	for _, families := range [][]*dto.MetricFamily{gathered, slices.Collect(maps.Values(parsed))} {
		for _, f := range families {
			labels := map[string]bool{}
			for _, m := range f.GetMetric() {
				for _, l := range m.GetLabel() {
					labels[l.GetName()] = true
				}
			}
			s.families[f.GetName()] = family{kind: f.GetType(), labels: labels}
		}
	}
	return s
}

type read struct {
	where string
	expr  string
}

func (r read) problems(s served) []string {
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(r.expr)
	if err != nil {
		return []string{err.Error()}
	}
	var problems, grouped []string
	carried := map[string]bool{}
	for _, l := range targetLabels {
		carried[l] = true
	}
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		switch n := node.(type) {
		case *parser.VectorSelector:
			if n.Name == "" {
				problems = append(problems, fmt.Sprintf("the selector %s has no metric name", n))
				return nil
			}
			f, extra, problem := s.lookup(n.Name)
			if problem != "" {
				problems = append(problems, fmt.Sprintf("reads %s, %s", n.Name, problem))
				return nil
			}
			own := maps.Clone(f.labels)
			if extra != "" {
				own[extra] = true
			}
			for _, m := range n.LabelMatchers {
				if m.Name != model.MetricNameLabel && !own[m.Name] && !slices.Contains(targetLabels, m.Name) {
					problems = append(problems, fmt.Sprintf("matches %s on %s, which none of its series carries", n.Name, m.Name))
				}
			}
			maps.Copy(carried, own)
		case *parser.AggregateExpr:
			grouped = append(grouped, n.Grouping...)
		case *parser.BinaryExpr:
			if n.VectorMatching != nil {
				grouped = append(grouped, n.VectorMatching.MatchingLabels...)
				grouped = append(grouped, n.VectorMatching.Include...)
			}
		}
		return nil
	})
	for _, l := range grouped {
		if !carried[l] {
			problems = append(problems, fmt.Sprintf("groups or matches on %s, which no series it reads carries", l))
		}
	}
	return problems
}

func checkReads(t *testing.T, served served, file string, reads []read) {
	t.Helper()
	if len(reads) == 0 {
		t.Fatalf("%s: found no expression to check", file)
	}
	for _, r := range reads {
		for _, problem := range r.problems(served) {
			t.Errorf("%s: %s %s", file, r.where, problem)
		}
	}
}

func TestRulesReadServedMetrics(t *testing.T) {
	raw, err := os.ReadFile(ruleFile)
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Groups []struct {
			Rules []struct {
				Alert  string `json:"alert"`
				Record string `json:"record"`
				Expr   string `json:"expr"`
			} `json:"rules"`
		} `json:"groups"`
	}
	if err := yaml.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	var reads []read
	for _, group := range file.Groups {
		for _, rule := range group.Rules {
			reads = append(reads, read{where: rule.Alert + rule.Record, expr: rule.Expr})
		}
	}
	checkReads(t, servedMetrics(t), ruleFile, reads)
}

type datasourceRef struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

var templatedDatasource = datasourceRef{Type: "prometheus", UID: "${datasource}"}

type target struct {
	RefID      string         `json:"refId"`
	Expr       string         `json:"expr"`
	Instant    bool           `json:"instant"`
	Range      bool           `json:"range"`
	Datasource *datasourceRef `json:"datasource"`
}

type panel struct {
	Type       string         `json:"type"`
	Title      string         `json:"title"`
	Datasource *datasourceRef `json:"datasource"`
	Panels     []panel        `json:"panels"`
	Targets    []target       `json:"targets"`
}

type dashboard struct {
	Title      string  `json:"title"`
	Panels     []panel `json:"panels"`
	Templating struct {
		List []struct {
			Name       string         `json:"name"`
			Type       string         `json:"type"`
			Datasource *datasourceRef `json:"datasource"`
			Query      any            `json:"query"`
		} `json:"list"`
	} `json:"templating"`
}

func expand(grafana string) string {
	return variable.ReplaceAllString(rangeVariable.ReplaceAllString(grafana, "[5m]"), "1")
}

func (d dashboard) panels() []panel {
	var all []panel
	var walk func(panels []panel)
	walk = func(panels []panel) {
		for _, p := range panels {
			all = append(all, p)
			walk(p.Panels)
		}
	}
	walk(d.Panels)
	return all
}

func (d dashboard) reads() ([]read, error) {
	var reads []read
	for _, p := range d.panels() {
		for _, target := range p.Targets {
			reads = append(reads, read{where: fmt.Sprintf("panel %q target %s", p.Title, target.RefID), expr: expand(target.Expr)})
		}
	}
	for _, v := range d.Templating.List {
		if v.Type != "query" {
			continue
		}
		query := v.Query
		if wrapped, ok := query.(map[string]any); ok {
			query = wrapped["query"]
		}
		text, _ := query.(string)
		match := labelValues.FindStringSubmatch(text)
		if match == nil {
			return nil, fmt.Errorf("variable %s: the query %q is not label_values(<selector>, <label>)", v.Name, text)
		}
		reads = append(reads, read{where: "variable " + v.Name, expr: expand("group by (" + match[2] + ") (" + match[1] + ")")})
	}
	return reads, nil
}

func dashboards(t *testing.T) map[string]dashboard {
	t.Helper()
	files, err := filepath.Glob(dashboardGlob)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("no dashboard matches %s", dashboardGlob)
	}
	all := map[string]dashboard{}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var d dashboard
		if err := json.Unmarshal(raw, &d); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		all[file] = d
	}
	return all
}

func TestDashboardsReadServedMetrics(t *testing.T) {
	served := servedMetrics(t)
	for file, d := range dashboards(t) {
		t.Run(filepath.Base(file), func(t *testing.T) {
			reads, err := d.reads()
			if err != nil {
				t.Fatal(err)
			}
			checkReads(t, served, file, reads)
		})
	}
}

func TestStatPanelsShowTheCurrentSeries(t *testing.T) {
	for file, d := range dashboards(t) {
		for _, p := range d.panels() {
			if p.Type != "stat" {
				continue
			}
			for _, target := range p.Targets {
				if !target.Instant || target.Range {
					t.Errorf("%s: panel %q target %s is a range query, so the stat keeps a tile for every series of the time range", file, p.Title, target.RefID)
				}
			}
		}
	}
}

func TestEveryQueryReadsTheDatasourceVariable(t *testing.T) {
	for file, d := range dashboards(t) {
		for _, p := range d.panels() {
			if len(p.Targets) == 0 {
				continue
			}
			if p.Datasource == nil || *p.Datasource != templatedDatasource {
				t.Errorf("%s: panel %q reads the data source %+v, want %+v", file, p.Title, p.Datasource, templatedDatasource)
			}
			for _, target := range p.Targets {
				if target.Datasource != nil && *target.Datasource != templatedDatasource {
					t.Errorf("%s: panel %q target %s reads the data source %+v, want %+v", file, p.Title, target.RefID, *target.Datasource, templatedDatasource)
				}
			}
		}
		for _, v := range d.Templating.List {
			if v.Type == "query" && (v.Datasource == nil || *v.Datasource != templatedDatasource) {
				t.Errorf("%s: variable %s reads the data source %+v, want %+v", file, v.Name, v.Datasource, templatedDatasource)
			}
		}
	}
}

func TestANameIsReadAsAScrapeServesIt(t *testing.T) {
	s := served{
		described: map[string]bool{"a_gauge": true, "a_histogram": true, "a_summary": true, "an_empty_vector": true},
		families: map[string]family{
			"a_gauge":     {kind: dto.MetricType_GAUGE},
			"a_histogram": {kind: dto.MetricType_HISTOGRAM},
			"a_summary":   {kind: dto.MetricType_SUMMARY},
		},
	}
	for name, want := range map[string]struct {
		served bool
		extra  string
	}{
		"a_gauge":               {true, ""},
		"a_gauge_count":         {false, ""},
		"a_histogram":           {false, ""},
		"a_histogram_bucket":    {true, "le"},
		"a_histogram_count":     {true, ""},
		"a_histogram_sum":       {true, ""},
		"a_histogram_total":     {false, ""},
		"a_summary":             {true, "quantile"},
		"a_summary_sum":         {true, ""},
		"a_summary_bucket":      {false, ""},
		"an_empty_vector":       {false, ""},
		"an_empty_vector_count": {false, ""},
		"a_misspelling":         {false, ""},
		"a_misspelling_sum":     {false, ""},
	} {
		_, extra, problem := s.lookup(name)
		if served := problem == ""; served != want.served || extra != want.extra {
			t.Errorf("lookup(%s) serves it %t with the label %q (%s), want %t and %q", name, served, extra, problem, want.served, want.extra)
		}
	}
}

func TestALabelIsOneTheSeriesCarry(t *testing.T) {
	s := served{families: map[string]family{
		"a_gauge":     {kind: dto.MetricType_GAUGE, labels: map[string]bool{"type": true}},
		"other":       {kind: dto.MetricType_GAUGE, labels: map[string]bool{"reason": true}},
		"a_histogram": {kind: dto.MetricType_HISTOGRAM, labels: map[string]bool{"path": true}},
	}}
	for expr, want := range map[string]int{
		`a_gauge{type="Ready",job="lodia",pod="a"}`:                                    0,
		`a_gauge{tpye="Ready"}`:                                                        1,
		`sum by (type, instance) (a_gauge)`:                                            0,
		`sum by (tpye) (a_gauge)`:                                                      1,
		`sum without (le, path) (rate(a_histogram_bucket{le="10.0"}[5m]))`:             0,
		`a_histogram_sum{le="10.0"}`:                                                   1,
		`a_gauge * on (namespace) group_left (reason) other`:                           0,
		`a_gauge * on (controlplane) group_left (reason) other`:                        1,
		`a_gauge * on (namespace) group_left (resaon) other`:                           1,
		`count by (reason) (max by (namespace, reason) (other)) or vector(0) > bool 1`: 0,
	} {
		if got := (read{expr: expr}).problems(s); len(got) != want {
			t.Errorf("%s: %d problems %q, want %d", expr, len(got), got, want)
		}
	}
}
