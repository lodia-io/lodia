// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package rules

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"

	"sigs.k8s.io/yaml"
)

const runbooks = "https://github.com/lodia-io/lodia/blob/main/docs/operations.md#"

func TestEveryAlertLinksItsSectionOfOperations(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "lodia", "files", "rules", "lodia.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Groups []struct {
			Rules []struct {
				Alert       string            `json:"alert"`
				Annotations map[string]string `json:"annotations"`
			} `json:"rules"`
		} `json:"groups"`
	}
	if err := yaml.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "operations.md"))
	if err != nil {
		t.Fatal(err)
	}
	anchors := headingAnchors(string(doc))
	alerts := 0
	for _, group := range file.Groups {
		for _, rule := range group.Rules {
			if rule.Alert == "" {
				continue
			}
			alerts++
			anchor := slug(rule.Alert)
			if got, want := rule.Annotations["runbook_url"], runbooks+anchor; got != want {
				t.Errorf("%s: runbook_url %q, want %q", rule.Alert, got, want)
			}
			if !anchors[anchor] {
				t.Errorf("%s: docs/operations.md has no heading whose anchor is %q", rule.Alert, anchor)
			}
		}
	}
	if alerts == 0 {
		t.Fatal("the rule file holds no alert")
	}
}

func headingAnchors(doc string) map[string]bool {
	anchors := map[string]bool{}
	fenced := false
	for line := range strings.SplitSeq(doc, "\n") {
		if strings.HasPrefix(line, "```") {
			fenced = !fenced
		}
		if fenced || !strings.HasPrefix(line, "#") {
			continue
		}
		anchors[slug(strings.TrimSpace(strings.TrimLeft(line, "#")))] = true
	}
	return anchors
}

func slug(heading string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(heading) {
		switch {
		case r == ' ':
			b.WriteRune('-')
		case r == '-', r == '_', unicode.IsLetter(r), unicode.IsDigit(r):
			b.WriteRune(r)
		}
	}
	return b.String()
}
