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

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"github.com/gardener/etcd-druid/api/core/v1alpha1/crds"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/controller/datastore"
	"github.com/segla-io/brygga/internal/controller/lifecycle"
	"github.com/segla-io/brygga/internal/controller/pki"
)

var conditionWriters = map[string]string{
	bryggav1alpha1.ConditionCertificatesReady:    "brygga/pki",
	bryggav1alpha1.ConditionCAValidityAcceptable: "brygga/pki",
	bryggav1alpha1.ConditionAccessReady:          "brygga/pki",
	bryggav1alpha1.ConditionDatastoreReady:       datastore.FieldManager,
	bryggav1alpha1.ConditionProgressing:          lifecycle.FieldManager,
	bryggav1alpha1.ConditionReady:                lifecycle.FieldManager,
}

var fieldWriters = map[string]string{
	"credentials":        "brygga/pki",
	"datastore":          datastore.FieldManager,
	"target":             lifecycle.FieldManager,
	"observedGeneration": lifecycle.FieldManager,
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

	fresh := &bryggav1alpha1.ControlPlane{}
	err := wait.PollUntilContextTimeout(t.Context(), 200*time.Millisecond, 60*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(cp), fresh); err != nil {
			return false, err
		}
		if fresh.Status.Datastore == nil || fresh.Status.Credentials == nil || fresh.Status.Target == nil {
			return false, nil
		}
		for conditionType := range conditionWriters {
			if meta.FindStatusCondition(fresh.Status.Conditions, conditionType) == nil {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("the controllers did not write their status: %v", err)
	}
	owners, err := statusOwners(fresh)
	if err != nil {
		t.Fatal(err)
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

func start(t *testing.T) client.Client {
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
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, bryggav1alpha1.AddToScheme, druidv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	labelled, err := labels.NewRequirement(bryggav1alpha1.ControlPlaneLabel, selection.Exists, nil)
	if err != nil {
		t.Fatal(err)
	}
	everything := cache.ByObject{Label: labels.Everything()}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			DefaultLabelSelector:        labels.NewSelector().Add(*labelled),
			DefaultTransform:            cache.TransformStripManagedFields(),
			ReaderFailOnMissingInformer: true,
			ByObject: map[client.Object]cache.ByObject{
				&bryggav1alpha1.ControlPlane{}:      everything,
				&bryggav1alpha1.DatastoreClass{}:    everything,
				&bryggav1alpha1.Datastore{}:         everything,
				&bryggav1alpha1.KubeconfigRequest{}: everything,
				&bryggav1alpha1.Backup{}:            everything,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, setup := range map[string]func() error{
		"pki": func() error {
			return (&pki.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), LeafLifetime: 24 * time.Hour, CALifetime: 48 * time.Hour, TrustAnchorLifetime: 96 * time.Hour, CAExpiryNotice: time.Hour}).SetupWithManager(mgr, 1)
		},
		"datastore": func() error {
			return (&datastore.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader()}).SetupWithManager(mgr, 1)
		},
		"lifecycle": func() error { return (&lifecycle.Reconciler{Client: mgr.GetClient()}).SetupWithManager(mgr, 1) },
	} {
		if err := setup(); err != nil {
			t.Fatalf("set up %s: %v", name, err)
		}
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
