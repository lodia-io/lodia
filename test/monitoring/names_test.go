// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package monitoring

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
	"go.lodia.io/lodia/internal/metrics"
	_ "sigs.k8s.io/controller-runtime"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
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
	labelValues   = regexp.MustCompile(`^label_values\((.+),\s*[A-Za-z_]\w*\)$`)

	histogramSuffixes = []string{"_bucket", "_count", "_sum"}
)

type served struct {
	described map[string]bool
	types     map[string]dto.MetricType
}

func (s served) has(name string) bool {
	if s.described[name] {
		return true
	}
	for _, suffix := range histogramSuffixes {
		base, ok := strings.CutSuffix(name, suffix)
		if !ok || !s.described[base] {
			continue
		}
		kind, known := s.types[base]
		return !known || kind == dto.MetricType_HISTOGRAM || kind == dto.MetricType_SUMMARY
	}
	return false
}

func servedMetrics(t *testing.T) served {
	t.Helper()
	s := served{described: map[string]bool{}, types: map[string]dto.MetricType{}}
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
	families, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		s.types[family.GetName()] = family.GetType()
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
	for name, family := range parsed {
		s.types[name] = family.GetType()
	}
	return s
}

type read struct {
	where string
	expr  string
}

func (r read) names() ([]string, error) {
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(r.expr)
	if err != nil {
		return nil, err
	}
	var names []string
	var unnamed error
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		selector, ok := node.(*parser.VectorSelector)
		if !ok {
			return nil
		}
		if selector.Name == "" {
			unnamed = fmt.Errorf("the selector %s has no metric name", selector)
			return nil
		}
		names = append(names, selector.Name)
		return nil
	})
	return names, unnamed
}

func checkReads(t *testing.T, served served, file string, reads []read) {
	t.Helper()
	if len(reads) == 0 {
		t.Fatalf("%s: found no expression to check", file)
	}
	for _, r := range reads {
		names, err := r.names()
		if err != nil {
			t.Errorf("%s: %s: %v", file, r.where, err)
		}
		for _, name := range names {
			if !served.has(name) {
				t.Errorf("%s: %s reads %s, which neither Lodia's collector nor controller-runtime's registry describes", file, r.where, name)
			}
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

type target struct {
	RefID   string `json:"refId"`
	Expr    string `json:"expr"`
	Instant bool   `json:"instant"`
	Range   bool   `json:"range"`
}

type panel struct {
	Type    string   `json:"type"`
	Title   string   `json:"title"`
	Panels  []panel  `json:"panels"`
	Targets []target `json:"targets"`
}

type dashboard struct {
	Title      string  `json:"title"`
	Panels     []panel `json:"panels"`
	Templating struct {
		List []struct {
			Name  string `json:"name"`
			Type  string `json:"type"`
			Query any    `json:"query"`
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
		reads = append(reads, read{where: "variable " + v.Name, expr: expand(match[1])})
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

func TestHistogramSuffixesNeedAHistogram(t *testing.T) {
	s := served{
		described: map[string]bool{"a_gauge": true, "a_histogram": true, "an_unknown": true},
		types:     map[string]dto.MetricType{"a_gauge": dto.MetricType_GAUGE, "a_histogram": dto.MetricType_HISTOGRAM},
	}
	for name, want := range map[string]bool{
		"a_gauge":            true,
		"a_gauge_count":      false,
		"a_histogram":        true,
		"a_histogram_bucket": true,
		"a_histogram_sum":    true,
		"a_histogram_total":  false,
		"an_unknown_bucket":  true,
		"a_misspelling":      false,
		"a_misspelling_sum":  false,
	} {
		if got := s.has(name); got != want {
			t.Errorf("has(%s) = %t, want %t", name, got, want)
		}
	}
}
