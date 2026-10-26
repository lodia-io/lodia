// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const durationRule = "duration(self) >= duration('0s')"

func TestEveryDurationIsOneGoCanParse(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join("..", "..", "api", "crds", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no CRDs found: %v", err)
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.Unmarshal(raw, crd); err != nil {
			t.Fatal(err)
		}
		obj, err := scheme.New(lodiav1alpha1.GroupVersion.WithKind(crd.Spec.Names.Kind))
		if err != nil {
			t.Fatal(err)
		}
		durations(t, reflect.TypeOf(obj).Elem(), *crd.Spec.Versions[0].Schema.OpenAPIV3Schema, crd.Spec.Names.Kind)
	}
}

func durations(t *testing.T, typ reflect.Type, schema apiextensionsv1.JSONSchemaProps, path string) {
	t.Helper()
	switch typ.Kind() {
	case reflect.Pointer:
		durations(t, typ.Elem(), schema, path)
	case reflect.Slice:
		if schema.Items != nil && schema.Items.Schema != nil {
			durations(t, typ.Elem(), *schema.Items.Schema, path+"[]")
		}
	case reflect.Map:
		if schema.AdditionalProperties != nil && schema.AdditionalProperties.Schema != nil {
			durations(t, typ.Elem(), *schema.AdditionalProperties.Schema, path+"{}")
		}
	case reflect.Struct:
		if typ == reflect.TypeFor[metav1.Duration]() {
			if !slices.Contains(rules(schema), durationRule) {
				t.Errorf("%s is a metav1.Duration without the rule %s: a value Go cannot parse fails the informer's list", path, durationRule)
			}
			return
		}
		for field := range typ.Fields() {
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if field.Anonymous && name == "" {
				durations(t, field.Type, schema, path)
				continue
			}
			if child, ok := schema.Properties[name]; ok {
				durations(t, field.Type, child, path+"."+name)
			}
		}
	default:
	}
}
