// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestOnlyTheEngineImportsEtcdDruid(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatal(err)
	}
	engine := filepath.Join(root, "internal", "etcddruid")
	exempt := filepath.Join(root, "test")
	var files int
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case d.IsDir() && (path == engine || path == exempt || strings.HasPrefix(d.Name(), ".") || d.Name() == "testdata"):
			return filepath.SkipDir
		case d.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go"):
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		files++
		for _, spec := range file.Imports {
			imported, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if imported == "github.com/gardener/etcd-druid" || strings.HasPrefix(imported, "github.com/gardener/etcd-druid/") {
				rel, _ := filepath.Rel(root, path)
				t.Errorf("%s imports %s; only internal/etcddruid may", rel, imported)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files == 0 {
		t.Fatalf("no Go file under %s", root)
	}
}
