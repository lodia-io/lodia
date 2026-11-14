// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestNothingIsPassedToEncodingJSONButAScalarToken pins the rule of
// docs/operations.md: no object and no array of a tenant's answer is decoded
// by encoding/json, whose decoding multiplies a tenant's bytes.
func TestNothingIsPassedToEncodingJSONButAScalarToken(t *testing.T) {
	allowed := map[string]bool{"jsonscan.go: Text": true, "jsonscan.go: keyOf": true}
	found := map[string]bool{}
	for _, pattern := range []string{"*.go", "../../jsonscan/*.go"} {
		files, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, decl := range parsed.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok {
					continue
				}
				ast.Inspect(fn, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					selector, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					if selector.Sel.Name == "UnmarshalJSON" {
						t.Errorf("%s: %s calls an UnmarshalJSON, which hands its token to encoding/json", filepath.Base(file), fn.Name.Name)
					}
					pkg, ok := selector.X.(*ast.Ident)
					if ok && pkg.Name == "json" && (selector.Sel.Name == "Unmarshal" || selector.Sel.Name == "NewDecoder") {
						where := filepath.Base(file) + ": " + fn.Name.Name
						found[where] = true
						if !allowed[where] {
							t.Errorf("%s calls json.%s", where, selector.Sel.Name)
						}
					}
					return true
				})
			}
		}
	}
	for where := range allowed {
		if !found[where] {
			t.Errorf("%s no longer calls encoding/json: the test's list of calls is out of date", where)
		}
	}
}
