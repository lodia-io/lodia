// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package rules

import (
	"bytes"
	"flag"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"text/template"

	"sigs.k8s.io/yaml"
)

const (
	ruleFile   = "../../charts/lodia/files/rules/lodia.yaml"
	goldenFile = "lodia_test.yaml"
)

var update = flag.Bool("update", false, "rewrite "+goldenFile+" from the cases in cases_test.go")

type rule struct {
	Alert       string            `json:"alert"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

func TestPromtoolCasesMatchTheirGenerator(t *testing.T) {
	rules, err := loadRules()
	if err != nil {
		t.Fatal(err)
	}
	got, err := generate(rules, cases())
	if err != nil {
		t.Fatal(err)
	}
	if *update {
		if err := os.WriteFile(goldenFile, []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(goldenFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, []byte(got)) {
		t.Errorf("%s differs from what cases_test.go and %s generate: run go test ./test/rules/ -update and review the diff", goldenFile, ruleFile)
	}
}

func loadRules() (map[string]rule, error) {
	raw, err := os.ReadFile(ruleFile)
	if err != nil {
		return nil, err
	}
	var file struct {
		Groups []struct {
			Rules []rule `json:"rules"`
		} `json:"groups"`
	}
	if err := yaml.Unmarshal(raw, &file); err != nil {
		return nil, err
	}
	rules := map[string]rule{}
	for _, group := range file.Groups {
		for _, r := range group.Rules {
			rules[r.Alert] = r
		}
	}
	return rules, nil
}

func (l alertLabels) lines() []string {
	lines := make([]string, 0, len(l))
	for _, k := range slices.Sorted(maps.Keys(l)) {
		lines = append(lines, k+": "+l[k])
	}
	return lines
}

func annotation(text string, labels alertLabels) (string, error) {
	tmpl, err := template.New("annotation").Parse("{{ $labels := . }}" + text)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	if err := tmpl.Execute(&out, labels); err != nil {
		return "", err
	}
	return out.String(), nil
}

func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func duration(seconds int) string {
	if seconds == 0 {
		return "0s"
	}
	var out strings.Builder
	if h := seconds / 3600; h > 0 {
		fmt.Fprintf(&out, "%dh", h)
	}
	if m := seconds % 3600 / 60; m > 0 {
		fmt.Fprintf(&out, "%dm", m)
	}
	if s := seconds % 60; s > 0 {
		fmt.Fprintf(&out, "%ds", s)
	}
	return out.String()
}

func generate(rules map[string]rule, cases []testCase) (string, error) {
	var out strings.Builder
	fmt.Fprintf(&out, "rule_files:\n  - %s\n\nevaluation_interval: 30s\n\ntests:\n", ruleFile)
	anchors := map[string]string{}
	checked := map[string]bool{}
	for _, c := range cases {
		input := c.series
		if !c.asIs {
			var err error
			if input, err = complete(input); err != nil {
				return "", fmt.Errorf("%s: %w", c.name, err)
			}
		}
		fmt.Fprintf(&out, "  - name: %s\n    interval: 30s\n    input_series:\n", quote(c.name))
		for _, s := range input {
			fmt.Fprintf(&out, "      - series: %s\n        values: %s\n", quote(s.String()), quote(s.values))
		}
		out.WriteString("    alert_rule_test:\n")
		for _, ck := range c.checks {
			r, ok := rules[ck.alert]
			if !ok {
				return "", fmt.Errorf("%s: %s is not in %s", c.name, ck.alert, ruleFile)
			}
			checked[ck.alert] = true
			fmt.Fprintf(&out, "      - eval_time: %s\n        alertname: %s\n", duration(ck.at), ck.alert)
			if len(ck.want) == 0 {
				continue
			}
			out.WriteString("        exp_alerts:\n")
			expected := make([]alertLabels, len(ck.want))
			for i, w := range ck.want {
				expected[i] = alertLabels{}
				maps.Copy(expected[i], w)
				maps.Copy(expected[i], r.Labels)
			}
			slices.SortFunc(expected, func(a, b alertLabels) int { return slices.Compare(a.lines(), b.lines()) })
			for _, labels := range expected {
				out.WriteString("          - exp_labels:\n")
				for _, line := range labels.lines() {
					fmt.Fprintf(&out, "              %s\n", line)
				}
				var rendered []string
				for _, k := range slices.Sorted(maps.Keys(r.Annotations)) {
					text, err := annotation(r.Annotations[k], labels)
					if err != nil {
						return "", fmt.Errorf("%s: %s: %w", c.name, ck.alert, err)
					}
					rendered = append(rendered, fmt.Sprintf("              %s: %s", k, quote(text)))
				}
				key := ck.alert + "\n" + strings.Join(rendered, "\n")
				if anchor, ok := anchors[key]; ok {
					fmt.Fprintf(&out, "            exp_annotations: *%s\n", anchor)
					continue
				}
				anchor := fmt.Sprintf("a%d", len(anchors)+1)
				anchors[key] = anchor
				fmt.Fprintf(&out, "            exp_annotations: &%s\n%s\n", anchor, strings.Join(rendered, "\n"))
			}
		}
		out.WriteString("\n")
	}
	for _, alert := range slices.Sorted(maps.Keys(rules)) {
		if !checked[alert] {
			return "", fmt.Errorf("%s in %s has no case in cases_test.go", alert, ruleFile)
		}
	}
	return strings.TrimRight(out.String(), "\n") + "\n", nil
}
