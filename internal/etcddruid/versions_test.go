// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"golang.org/x/mod/modfile"
	"sigs.k8s.io/yaml"
)

func TestEtcdDruidIsOneVersionInGoModTheChartItsLockAndTheValues(t *testing.T) {
	versions := map[string]string{}
	raw, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	module, err := modfile.Parse("go.mod", raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, require := range module.Require {
		if require.Mod.Path == "github.com/gardener/etcd-druid/api" {
			versions["go.mod"] = require.Mod.Version
		}
	}
	for _, name := range []string{"Chart.yaml", "Chart.lock"} {
		var chart struct {
			Dependencies []struct {
				Name    string `json:"name"`
				Version string `json:"version"`
			} `json:"dependencies"`
		}
		readChartFile(t, name, &chart)
		for _, dependency := range chart.Dependencies {
			if dependency.Name == "etcd-druid" {
				versions[name] = dependency.Version
			}
		}
	}
	var values struct {
		EtcdDruid struct {
			Image struct {
				Tag string `json:"tag"`
			} `json:"image"`
		} `json:"etcdDruid"`
	}
	readChartFile(t, "values.yaml", &values)
	versions["etcdDruid.image.tag"], _, _ = strings.Cut(values.EtcdDruid.Image.Tag, "@")

	places := []string{"go.mod", "Chart.yaml", "Chart.lock", "etcdDruid.image.tag"}
	var named []string
	for _, place := range places {
		named = append(named, fmt.Sprintf("%q in %s", versions[place], place))
	}
	if versions["go.mod"] == "" || slices.ContainsFunc(places, func(place string) bool { return versions[place] != versions["go.mod"] }) {
		t.Errorf("etcd-druid is %s, want one version", strings.Join(named, ", "))
	}
}

func readChartFile(t *testing.T, name string, into any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "lodia", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, into); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}
