// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package controller_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gardener/etcd-druid/api/core/v1alpha1/crds"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/operator"
)

var conditionWriters = map[string]string{
	bryggav1alpha1.ConditionCertificatesReady:    "brygga/pki",
	bryggav1alpha1.ConditionCAValidityAcceptable: "brygga/pki",
	bryggav1alpha1.ConditionAccessReady:          "brygga/pki",
	bryggav1alpha1.ConditionDatastoreReady:       "brygga/datastore",
	bryggav1alpha1.ConditionAddonsApplied:        "brygga/tenant",
	bryggav1alpha1.ConditionAddonsAvailable:      "brygga/tenant",
	bryggav1alpha1.ConditionProgressing:          "brygga/lifecycle",
	bryggav1alpha1.ConditionReady:                "brygga/lifecycle",
}

var fieldWriters = map[string]string{
	"credentials":        "brygga/pki",
	"datastore":          "brygga/datastore",
	"target":             "brygga/lifecycle",
	"observedGeneration": "brygga/lifecycle",
}

func TestEveryStatusFieldHasOneWriter(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run make test")
	}
	k8s := start(t)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "writers-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	updates, err := k8s.Watch(t.Context(), &bryggav1alpha1.ControlPlaneList{}, client.InNamespace(ns.Name))
	if err != nil {
		t.Fatal(err)
	}
	defer updates.Stop()
	class := &bryggav1alpha1.DatastoreClass{
		ObjectMeta: metav1.ObjectMeta{Name: "writers", Annotations: map[string]string{bryggav1alpha1.DefaultClassAnnotation: "true"}},
		Spec:       bryggav1alpha1.DatastoreClassSpec{Storage: bryggav1alpha1.EtcdStorage{Size: resource.MustParse("1Gi"), ReclaimPolicy: "Delete"}},
	}
	cp := &bryggav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns.Name},
		Spec: bryggav1alpha1.ControlPlaneSpec{
			Version:   "v1.35.0",
			Datastore: bryggav1alpha1.DatastoreChoice{Dedicated: &bryggav1alpha1.DedicatedDatastore{}},
			Endpoint:  bryggav1alpha1.Endpoint{LoadBalancer: &bryggav1alpha1.LoadBalancerEndpoint{}},
			Network:   bryggav1alpha1.Network{ServiceCIDRs: []bryggav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []bryggav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	for _, obj := range []client.Object{class, cp} {
		if err := k8s.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}

	owners := map[string][]string{}
	deadline := time.After(time.Minute)
	var settled <-chan time.Time
	written := false
watching:
	for {
		select {
		case event, open := <-updates.ResultChan():
			if !open {
				t.Fatal("the watch of the ControlPlane ended")
			}
			if event.Type == watch.Error {
				t.Fatalf("watch: %v", apierrors.FromObject(event.Object))
			}
			fresh := event.Object.(*bryggav1alpha1.ControlPlane)
			current, err := statusOwners(fresh)
			if err != nil {
				t.Fatal(err)
			}
			for key, managers := range current {
				merged := slices.Concat(owners[key], managers)
				slices.Sort(merged)
				owners[key] = slices.Compact(merged)
			}
			if written = written || complete(fresh); written {
				settled = time.After(2 * time.Second)
			}
		case <-settled:
			break watching
		case <-deadline:
			t.Fatalf("the controllers did not write their status and stop within a minute: written %t", written)
		}
	}
	for _, key := range slices.Sorted(maps.Keys(owners)) {
		writer, known := conditionWriters[strings.TrimPrefix(key, "conditions/")]
		if !strings.HasPrefix(key, "conditions/") {
			writer, known = fieldWriters[key]
		}
		switch {
		case !known:
			t.Errorf("status %s is written by %v and has no writer in the table", key, owners[key])
		case !slices.Equal(owners[key], []string{writer}):
			t.Errorf("status %s is written by %v, want %s alone", key, owners[key], writer)
		}
	}
}

func complete(cp *bryggav1alpha1.ControlPlane) bool {
	if cp.Status.Datastore == nil || cp.Status.Credentials == nil || cp.Status.Target == nil {
		return false
	}
	for conditionType := range conditionWriters {
		if meta.FindStatusCondition(cp.Status.Conditions, conditionType) == nil {
			return false
		}
	}
	return true
}

func statusOwners(cp *bryggav1alpha1.ControlPlane) (map[string][]string, error) {
	owners := map[string][]string{}
	for _, entry := range cp.ManagedFields {
		if entry.Subresource != "status" || entry.FieldsV1 == nil {
			continue
		}
		var fields map[string]map[string]json.RawMessage
		if err := json.Unmarshal(entry.FieldsV1.GetRawBytes(), &fields); err != nil {
			return nil, err
		}
		for field, raw := range fields["f:status"] {
			name := strings.TrimPrefix(field, "f:")
			if name != "conditions" {
				owners[name] = append(owners[name], entry.Manager)
				continue
			}
			var conditions map[string]json.RawMessage
			if err := json.Unmarshal(raw, &conditions); err != nil {
				return nil, err
			}
			for key := range conditions {
				var condition struct{ Type string }
				if !strings.HasPrefix(key, "k:") || json.Unmarshal([]byte(strings.TrimPrefix(key, "k:")), &condition) != nil {
					continue
				}
				owners["conditions/"+condition.Type] = append(owners["conditions/"+condition.Type], entry.Manager)
			}
		}
	}
	for key := range owners {
		slices.Sort(owners[key])
	}
	return owners, nil
}

func start(t *testing.T) client.WithWatch {
	t.Helper()
	druid, err := druidCRDs()
	if err != nil {
		t.Fatal(err)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		CRDs:                  druid,
		UseExistingCluster:    new(false),
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	scheme := operator.Scheme()
	k8s, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := operator.Cache()
	if err != nil {
		t.Fatal(err)
	}
	cache.ReaderFailOnMissingInformer = true
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, Cache: cache})
	if err != nil {
		t.Fatal(err)
	}
	if err := operator.Setup(mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() { started <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-started; err != nil {
			t.Errorf("manager: %v", err)
		}
	})
	return k8s
}

func druidCRDs() ([]*apiextensionsv1.CustomResourceDefinition, error) {
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
