// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

func TestEveryKindOfTheRenderIsInTheTableAtTheVersionItIsRenderedAt(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "render", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("the goldens of the render: %v, %v", files, err)
	}
	seen := map[schema.GroupKind]bool{}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		docs := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(data)))
		for {
			doc, err := docs.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			obj := &unstructured.Unstructured{}
			if err := yaml.Unmarshal(doc, &obj.Object); err != nil || len(obj.Object) == 0 {
				continue
			}
			gvk := obj.GroupVersionKind()
			k, ok := kinds[gvk.GroupKind()]
			if !ok {
				t.Errorf("%s renders a %s, which the table does not hold", file, gvk.GroupKind())
				continue
			}
			seen[gvk.GroupKind()] = true
			if k.version != gvk.Version || k.namespaced != (obj.GetNamespace() != "") {
				t.Errorf("%s renders %s at %s, namespaced %t; the table has %s, namespaced %t", file, gvk.GroupKind(), gvk.Version, obj.GetNamespace() != "", k.version, k.namespaced)
			}
		}
	}
	if len(seen) == 0 {
		t.Error("the goldens render nothing")
	}
}

func TestAnEntryAtAnotherVersionIsAddressedAtTheTables(t *testing.T) {
	api := standingIn(t, http.NotFoundHandler())
	for _, version := range []string{"v1", "v1beta1", "v1alpha1", "v2"} {
		request, ok := addressed(api.client.Get(), entry{Group: "rbac.authorization.k8s.io", Version: version, Kind: "ClusterRole", Name: "left-the-render"})
		if !ok || request.URL().Path != "/apis/rbac.authorization.k8s.io/v1/clusterroles/left-the-render" {
			t.Errorf("an entry at %s is addressed at %v, %t", version, request.URL(), ok)
		}
	}
}
