// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package druidtest serves etcd-druid v0.39.2's CRDs to tests that run no
// etcd-druid; it belongs to test files.
package druidtest

import (
	"fmt"

	"github.com/gardener/etcd-druid/api/core/v1alpha1/crds"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

// CRDs returns etcd-druid's CRDs for Kubernetes 1.35, the minor of
// ENVTEST_K8S_VERSION.
func CRDs() ([]*apiextensionsv1.CustomResourceDefinition, error) {
	manifests, err := crds.GetAll("1.35")
	if err != nil {
		return nil, err
	}
	var out []*apiextensionsv1.CustomResourceDefinition
	for name, manifest := range manifests {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.Unmarshal([]byte(manifest), crd); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		out = append(out, crd)
	}
	return out, nil
}
