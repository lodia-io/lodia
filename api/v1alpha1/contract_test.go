// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

type constant struct {
	name, value, comment string
}

func constants(t *testing.T, file string) []constant {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	var out []constant
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			v := spec.(*ast.ValueSpec)
			lit, ok := v.Values[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				t.Fatalf("%s in %s is not a string literal", v.Names[0].Name, file)
			}
			value, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, constant{v.Names[0].Name, value, strings.TrimSpace(v.Comment.Text())})
		}
	}
	return out
}

func table(t *testing.T, doc, after string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", doc))
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := strings.Cut(string(b), after)
	if !ok {
		t.Fatalf("docs/%s has no %q", doc, after)
	}
	var rows []string
	for line := range strings.Lines(rest) {
		if strings.HasPrefix(line, "|") {
			rows = append(rows, line)
		} else if len(rows) > 0 {
			break
		}
	}
	return rows
}

func TestEveryReasonIsNamedByItsValueAndDocumented(t *testing.T) {
	rows := strings.Join(table(t, filepath.Join("api", "controlplane.md"), "## Conditions and their reasons"), "")
	for _, c := range constants(t, "conditions.go") {
		reason, ok := strings.CutPrefix(c.name, "Reason")
		if !ok {
			continue
		}
		if c.value != reason {
			t.Errorf("%s is %q", c.name, c.value)
		}
		if !strings.Contains(rows, "`"+c.value+"`") {
			t.Errorf("the conditions table of docs/api/controlplane.md does not name the reason %s", c.value)
		}
	}
}

func TestEveryConditionIsDocumentedWithItsWriter(t *testing.T) {
	rows := table(t, filepath.Join("api", "controlplane.md"), "## Conditions and their reasons")
	for _, c := range constants(t, "conditions.go") {
		if !strings.HasPrefix(c.name, "Condition") {
			continue
		}
		row := "| `" + c.value + "` | " + c.comment + " |"
		if !strings.Contains(strings.Join(rows, ""), row) {
			t.Errorf("the conditions table of docs/api/controlplane.md has no row %q", row)
		}
	}
}

func TestEveryLabelIsDocumented(t *testing.T) {
	rows := strings.Join(table(t, "README.md", "`api/v1alpha1/labels.go`:"), "")
	keys := map[string]string{}
	all := constants(t, "labels.go")
	for _, c := range all {
		keys[c.name] = c.value
	}
	for _, c := range all {
		want := []string{"`" + c.value + "`", "`" + c.value + ": "}
		if key, ok := keys[c.name+"Label"]; ok {
			want = []string{"`" + key + ": " + c.value + "`"}
		}
		if !slices.ContainsFunc(want, func(s string) bool { return strings.Contains(rows, s) }) {
			t.Errorf("the labels table of docs/README.md has no %s for %s", want[0], c.name)
		}
	}
}
