// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package operator_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gardener/etcd-druid/api/core/v1alpha1/crds"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/operator"
)

func TestTheCacheHoldsTheGatewayAPIWholeOnlyWhereItIsServed(t *testing.T) {
	for _, served := range []bool{false, true} {
		options, err := operator.Cache(served)
		if err != nil {
			t.Fatal(err)
		}
		for _, kind := range []client.Object{&gatewayv1.TLSRoute{}, &gatewayv1.Gateway{}} {
			var held []string
			for obj, byObject := range options.ByObject {
				if reflect.TypeOf(obj) == reflect.TypeOf(kind) {
					held = append(held, fmt.Sprintf("label %q", byObject.Label))
					if byObject.Label == nil || !byObject.Label.Empty() {
						t.Errorf("Cache(%t) holds %T by label %v, want every one", served, kind, byObject.Label)
					}
				}
			}
			if len(held) > 0 != served {
				t.Errorf("Cache(%t) holds %T by %v", served, kind, held)
			}
		}
	}
}

const cacheSyncTimeout = 3 * time.Second

func TestWithoutTheGatewayAPIsCRDs(t *testing.T) {
	cfg := start(t)
	if served, err := operator.GatewayAPI(cfg); err != nil || served {
		t.Fatalf("GatewayAPI says %t, %v without the CRDs, want false", served, err)
	}
	options, err := operator.Cache(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: operator.Scheme(), Metrics: metricsserver.Options{BindAddress: "0"}, Cache: options}); !meta.IsNoMatchError(err) {
		t.Errorf("a manager whose cache holds the Gateway API without its CRDs: %v, want no match", err)
	}
	k8s, stopped := run(t, cfg, false)
	expectAPass(t, k8s, stopped, lodiav1alpha1.ReasonGatewayUnsupported,
		"the management cluster did not serve TLSRoute and Gateway at gateway.networking.k8s.io/v1 when the operator started, the only time it looks: restart the operator once they are served")
}

func TestWithTheGatewayAPIsCRDs(t *testing.T) {
	cfg := start(t, gatewayAPI(t)...)
	if served, err := operator.GatewayAPI(cfg); err != nil || !served {
		t.Fatalf("GatewayAPI says %t, %v with the CRDs, want true", served, err)
	}
	k8s, stopped := run(t, cfg, true)
	expectAPass(t, k8s, stopped, lodiav1alpha1.ReasonGatewayNotFound, "Gateway gateways/edge does not exist")
}

func TestTheGatewayAPIIsServedWithBothKindsAtV1(t *testing.T) {
	kinds := map[string]*apiextensionsv1.CustomResourceDefinition{}
	for _, crd := range gatewayAPI(t) {
		kinds[crd.Spec.Names.Kind] = crd
	}
	tlsRouteV1Alpha2 := kinds["TLSRoute"].DeepCopy()
	for i := range tlsRouteV1Alpha2.Spec.Versions {
		v := &tlsRouteV1Alpha2.Spec.Versions[i]
		v.Served, v.Storage = v.Name == "v1alpha2", v.Name == "v1alpha2"
	}
	for _, c := range []struct {
		name string
		crds []*apiextensionsv1.CustomResourceDefinition
	}{
		{"TLSRoute without Gateway", []*apiextensionsv1.CustomResourceDefinition{kinds["TLSRoute"]}},
		{"Gateway with TLSRoute at v1alpha2 alone", []*apiextensionsv1.CustomResourceDefinition{kinds["Gateway"], tlsRouteV1Alpha2}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if served, err := operator.GatewayAPI(start(t, c.crds...)); err != nil || served {
				t.Errorf("GatewayAPI says %t, %v, want false", served, err)
			}
		})
	}
}

func TestAnUnreachableAPIServerIsAnError(t *testing.T) {
	gone := httptest.NewServer(http.NotFoundHandler())
	gone.Close()
	if served, err := operator.GatewayAPI(&rest.Config{Host: gone.URL, Timeout: 5 * time.Second}); err == nil || served {
		t.Errorf("GatewayAPI says %t, %v for %s, which refuses connections, want an error", served, err, gone.URL)
	}
}

func TestADiscoveryErrorIsAnError(t *testing.T) {
	cfg := rest.CopyConfig(start(t, gatewayAPI(t)...))
	cfg.WrapTransport = func(next http.RoundTripper) http.RoundTripper {
		return roundTripper(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/apis" {
				return next.RoundTrip(req)
			}
			body := `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"ServiceUnavailable","code":503}`
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(body)),
				Request:    req,
			}, nil
		})
	}
	served, err := operator.GatewayAPI(cfg)
	if served || !apierrors.IsServiceUnavailable(err) || meta.IsNoMatchError(err) {
		t.Errorf("GatewayAPI says %t, %v while /apis answers 503, want that error", served, err)
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func start(t *testing.T, extra ...*apiextensionsv1.CustomResourceDefinition) *rest.Config {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run make test")
	}
	manifests, err := crds.GetAll("1.35")
	if err != nil {
		t.Fatal(err)
	}
	var all []*apiextensionsv1.CustomResourceDefinition
	for name, manifest := range manifests {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.Unmarshal([]byte(manifest), crd); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		all = append(all, crd)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "api", "crds")},
		ErrorIfCRDPathMissing: true,
		CRDs:                  append(all, extra...),
		UseExistingCluster:    new(false),
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	return cfg
}

func gatewayAPI(t *testing.T) []*apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	module, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "sigs.k8s.io/gateway-api").Output()
	if err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(strings.TrimSpace(string(module)), "config", "crd", "standard", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("the Gateway API module has no standard CRDs: %v", err)
	}
	var out []*apiextensionsv1.CustomResourceDefinition
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		reader := k8syaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
		for {
			doc, err := reader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			crd := &apiextensionsv1.CustomResourceDefinition{}
			if err := yaml.Unmarshal(doc, crd); err != nil {
				t.Fatal(err)
			}
			if crd.Kind == "CustomResourceDefinition" {
				out = append(out, crd)
			}
		}
	}
	return out
}

func run(t *testing.T, cfg *rest.Config, gatewayAPI bool) (client.Client, <-chan struct{}) {
	t.Helper()
	scheme := operator.Scheme()
	options, err := operator.Cache(gatewayAPI)
	if err != nil {
		t.Fatal(err)
	}
	options.ReaderFailOnMissingInformer = true
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Cache:      options,
		Controller: config.Controller{SkipNameValidation: new(true), CacheSyncTimeout: cacheSyncTimeout},
	})
	if err != nil {
		t.Fatal(err)
	}
	in := testInputs
	in.GatewayAPI = gatewayAPI
	if _, err := operator.Setup(mgr, in); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	var stopErr error
	go func() {
		stopErr = mgr.Start(ctx)
		close(stopped)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
		if stopErr != nil {
			t.Errorf("manager: %v", stopErr)
		}
	})
	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return k8s, stopped
}

var testInputs = operator.Inputs{Config: operator.DefaultConfig(), Version: "v0.0.0-test", SignerImage: "registry.example/lodia-signer:test", TunnelImage: "registry.example/lodia-tunnel:test", EtcdDruidVersion: "v0.39.0", EtcdDruidNamespace: "etcd-druid", EtcdDruidDeployment: "etcd-druid", OperatorNamespace: "lodia-system"}

func expectAPass(t *testing.T, k8s client.Client, stopped <-chan struct{}, reason, message string) {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "operator-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns.Name},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   "v1.37.1",
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint: lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
				ParentRef: lodiav1alpha1.ParentReference{Name: "edge", Namespace: "gateways", SectionName: "tls"},
				Hostnames: lodiav1alpha1.GatewayHostnames{APIServer: "api.dev.example.com", Konnectivity: "konnectivity.dev.example.com", Tunnel: "tunnel.dev.example.com"},
			}},
			Network: lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	if err := k8s.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	revision, err := testInputs.Revision()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		lodiav1alpha1.ConditionRoutesReady: message,
		lodiav1alpha1.ConditionReady:       "RoutesReady: " + message,
	}
	var last error
	if err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(cp), cp); err != nil {
			return false, err
		}
		last = nil
		for conditionType, message := range want {
			if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c == nil || c.Status != metav1.ConditionFalse || c.Reason != reason || c.Message != message {
				last = errors.Join(last, fmt.Errorf("%s is %+v, want False %s %q", conditionType, c, reason, message))
			}
		}
		if cp.Status.Target == nil || cp.Status.Target.OperatorRevision != revision {
			last = errors.Join(last, fmt.Errorf("status.target is %+v, want the operator revision of the Inputs", cp.Status.Target))
		}
		return last == nil, nil
	}); err != nil {
		t.Fatalf("%v: %v", err, last)
	}
	select {
	case <-stopped:
		t.Fatal("the manager stopped")
	case <-time.After(cacheSyncTimeout + time.Second):
	}
}

func TestHealthsMetricsAreServedByLodiasCollectorAloneOnceElected(t *testing.T) {
	cfg := start(t)
	options, err := operator.Cache(false)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     operator.Scheme(),
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Cache:      options,
		Controller: config.Controller{SkipNameValidation: new(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	sources, err := operator.Setup(mgr, testInputs)
	if err != nil {
		t.Fatal(err)
	}
	collector, err := operator.Metrics(mgr.GetCache(), mgr.Elected(), testInputs, sources)
	if err != nil {
		t.Fatal(err)
	}
	for name, gatherer := range map[string]prometheus.Gatherer{"controller-runtime's": ctrlmetrics.Registry, "the default": prometheus.DefaultGatherer} {
		families, err := gatherer.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range families {
			if strings.HasPrefix(family.GetName(), "lodia_") {
				t.Errorf("%s registry serves %s beside Lodia's collector", name, family.GetName())
			}
		}
	}
	if n := testutil.CollectAndCount(collector); n != 0 {
		t.Errorf("before the election Lodia's collector serves %d series, want none", n)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Errorf("manager: %v", err)
		}
	})
	k8s, err := client.New(cfg, client.Options{Scheme: operator.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "operator-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Create(t.Context(), &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns.Name},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   "v1.37.1",
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint:  lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)
	var last error
	if err := wait.PollUntilContextTimeout(t.Context(), 200*time.Millisecond, 30*time.Second, true, func(context.Context) (bool, error) {
		families, err := registry.Gather()
		if err != nil {
			return false, err
		}
		served := map[string]int{}
		var observed uint64
		for _, family := range families {
			served[family.GetName()] = len(family.GetMetric())
			if family.GetName() == "lodia_health_nodes_sample_lag_seconds" {
				observed = family.GetMetric()[0].GetHistogram().GetSampleCount()
			}
		}
		if observed == 0 || served["lodia_health_probe_failures_total"] != 4 {
			last = fmt.Errorf("the collector serves %v, with %d passes of health-nodes observed", served, observed)
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Fatalf("%v: %v", err, last)
	}
}
