// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package controller_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
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
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/operator"
)

var managers atomic.Int32

var conditionWriters = map[string]string{
	lodiav1alpha1.ConditionCertificatesReady:     "lodia/pki",
	lodiav1alpha1.ConditionCAValidityAcceptable:  "lodia/pki",
	lodiav1alpha1.ConditionAccessReady:           "lodia/pki",
	lodiav1alpha1.ConditionDatastoreReady:        "lodia/datastore",
	lodiav1alpha1.ConditionBackupReady:           "lodia/datastore",
	lodiav1alpha1.ConditionAPIServerReady:        "lodia/workloads",
	lodiav1alpha1.ConditionRoutesReady:           "lodia/workloads",
	lodiav1alpha1.ConditionAddonsApplied:         "lodia/tenant",
	lodiav1alpha1.ConditionAddonsAvailable:       "lodia/tenant",
	lodiav1alpha1.ConditionReachable:             "lodia/health",
	lodiav1alpha1.ConditionControllersLeading:    "lodia/health",
	lodiav1alpha1.ConditionControllerManagerHeld: "lodia/health-nodes",
	lodiav1alpha1.ConditionProgressing:           "lodia/lifecycle",
	lodiav1alpha1.ConditionReady:                 "lodia/lifecycle",
}

var fieldWriters = map[string]string{
	"addons":                "lodia/tenant",
	"components":            "lodia/workloads",
	"credentials":           "lodia/pki",
	"datastore":             "lodia/datastore",
	"endpoint.apiServer":    "lodia/workloads",
	"endpoint.konnectivity": "lodia/workloads",
	"endpoint.inCluster":    "lodia/workloads",
	"endpoint.tunnel":       "lodia/workloads",
	"kubernetes":            "lodia/lifecycle",
	"nodes":                 "lodia/health-nodes",
	"restore":               "lodia/datastore",
	"target":                "lodia/lifecycle",
	"observedGeneration":    "lodia/lifecycle",
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
	updates, err := k8s.Watch(t.Context(), &lodiav1alpha1.ControlPlaneList{}, client.InNamespace(ns.Name))
	if err != nil {
		t.Fatal(err)
	}
	defer updates.Stop()
	class, gateway, cp := gatewayTenant(ns.Name)
	backup := &lodiav1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "dev-backup", Namespace: ns.Name},
		Spec:       lodiav1alpha1.BackupSpec{ControlPlaneRef: lodiav1alpha1.LocalReference{Name: cp.Name}},
	}
	for _, obj := range []client.Object{class, gateway, cp, backup} {
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
			fresh := event.Object.(*lodiav1alpha1.ControlPlane)
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
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(backup), backup); err != nil {
		t.Fatal(err)
	}
	backupOwners, err := statusOwners(backup)
	if err != nil {
		t.Fatal(err)
	}
	if len(backupOwners) == 0 {
		t.Error("nothing wrote the Backup's status")
	}
	for key, managers := range backupOwners {
		if !slices.Equal(managers, []string{"lodia/backup"}) {
			t.Errorf("the Backup's status %s is written by %v, want lodia/backup alone", key, managers)
		}
	}
}

func gatewayTenant(ns string) (*lodiav1alpha1.DatastoreClass, *gatewayv1.Gateway, *lodiav1alpha1.ControlPlane) {
	class := &lodiav1alpha1.DatastoreClass{
		ObjectMeta: metav1.ObjectMeta{Name: "writers", Annotations: map[string]string{lodiav1alpha1.DefaultClassAnnotation: "true"}},
		Spec:       lodiav1alpha1.DatastoreClassSpec{Storage: lodiav1alpha1.EtcdStorage{Size: resource.MustParse("8Gi"), ReclaimPolicy: "Delete"}},
	}
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: ns},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: "example", Listeners: []gatewayv1.Listener{{
			Name: "tls", Port: 443, Protocol: gatewayv1.TLSProtocolType, Hostname: new(gatewayv1.Hostname("*.dev.example.com")),
			TLS: &gatewayv1.ListenerTLSConfig{Mode: new(gatewayv1.TLSModePassthrough)},
		}}},
	}
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   "v1.35.0",
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint: lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
				ParentRef: lodiav1alpha1.ParentReference{Name: "shared", Namespace: ns, SectionName: "tls"},
				Hostnames: lodiav1alpha1.GatewayHostnames{APIServer: "api.dev.example.com", Konnectivity: "konnectivity.dev.example.com", Tunnel: "tunnel.dev.example.com"},
			}},
			Network: lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	return class, gateway, cp
}

func complete(cp *lodiav1alpha1.ControlPlane) bool {
	if cp.Status.Datastore == nil || cp.Status.Credentials == nil || cp.Status.Target == nil || cp.Status.Endpoint == nil || cp.Status.Endpoint.Tunnel == nil {
		return false
	}
	for conditionType := range conditionWriters {
		if meta.FindStatusCondition(cp.Status.Conditions, conditionType) == nil {
			return false
		}
	}
	return true
}

func statusOwners(obj metav1.Object) (map[string][]string, error) {
	owners := map[string][]string{}
	for _, entry := range obj.GetManagedFields() {
		if entry.Subresource != "status" || entry.FieldsV1 == nil {
			continue
		}
		var fields map[string]map[string]json.RawMessage
		if err := json.Unmarshal(entry.FieldsV1.GetRawBytes(), &fields); err != nil {
			return nil, err
		}
		for field, raw := range fields["f:status"] {
			name := strings.TrimPrefix(field, "f:")
			switch name {
			case "conditions":
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
			case "endpoint":
				var addresses map[string]json.RawMessage
				if err := json.Unmarshal(raw, &addresses); err != nil {
					return nil, err
				}
				for key := range addresses {
					if address, ok := strings.CutPrefix(key, "f:"); ok {
						owners["endpoint."+address] = append(owners["endpoint."+address], entry.Manager)
					}
				}
			default:
				owners[name] = append(owners[name], entry.Manager)
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
	gatewayAPI, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "sigs.k8s.io/gateway-api").Output()
	if err != nil {
		t.Fatal(err)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "api", "crds"), filepath.Join(strings.TrimSpace(string(gatewayAPI)), "config", "crd", "standard")},
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
	cache, err := operator.Cache(true)
	if err != nil {
		t.Fatal(err)
	}
	cache.ReaderFailOnMissingInformer = true
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, Cache: cache, Controller: config.Controller{SkipNameValidation: new(managers.Add(1) > 1)}})
	if err != nil {
		t.Fatal(err)
	}
	if err := operator.Setup(mgr, operator.Inputs{Config: operator.DefaultConfig(), Version: "v0.0.0-test", SignerImage: "registry.example/lodia-signer:test", TunnelImage: "registry.example/lodia-tunnel:test", EtcdDruidVersion: "v0.39.0", EtcdDruidNamespace: "etcd-druid", EtcdDruidDeployment: "etcd-druid", OperatorNamespace: "lodia-system", GatewayAPI: true}); err != nil {
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
