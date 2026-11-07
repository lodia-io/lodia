// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package operator_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gardener/etcd-druid/api/core/v1alpha1/crds"
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
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/operator"
)

var gatewayAPICRDs = filepath.Join("..", "..", "test", "testdata", "gateway-api", "v1.6.3", "standard-install.yaml")

func TestTheGatewayAPICRDsAreTheReleases(t *testing.T) {
	const (
		release = "https://github.com/kubernetes-sigs/gateway-api/releases/tag/v1.6.3"
		digest  = "356c2b8e286a964dc20995435659ad1095386329974793af10bbfe3eff977fe5"
	)
	raw, err := os.ReadFile(gatewayAPICRDs)
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(raw); hex.EncodeToString(sum[:]) != digest {
		t.Errorf("%s has sha256 %x, want %s, the digest of standard-install.yaml at %s", gatewayAPICRDs, sum, digest, release)
	}
}

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
	expectAPass(t, run(t, cfg, false))
}

func TestWithTheGatewayAPIsCRDs(t *testing.T) {
	cfg := start(t, gatewayAPI(t)...)
	if served, err := operator.GatewayAPI(cfg); err != nil || !served {
		t.Fatalf("GatewayAPI says %t, %v with the CRDs, want true", served, err)
	}
	expectAPass(t, run(t, cfg, true))
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
	raw, err := os.ReadFile(gatewayAPICRDs)
	if err != nil {
		t.Fatal(err)
	}
	reader := k8syaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
	var out []*apiextensionsv1.CustomResourceDefinition
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return out
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

func run(t *testing.T, cfg *rest.Config, gatewayAPI bool) client.Client {
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
		Controller: config.Controller{SkipNameValidation: new(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := operator.Setup(mgr, "registry.example/lodia-signer:test", "registry.example/lodia-tunnel:test", "etcd-druid", "lodia-system", gatewayAPI); err != nil {
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
	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return k8s
}

func expectAPass(t *testing.T, k8s client.Client) {
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
	want := map[string]string{
		lodiav1alpha1.ConditionAPIServerReady: "not built yet: endpoint.gateway",
		lodiav1alpha1.ConditionRoutesReady:    "not built yet: endpoint.gateway",
		lodiav1alpha1.ConditionReady:          "RoutesReady: not built yet: endpoint.gateway",
	}
	var last error
	if err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(cp), cp); err != nil {
			return false, err
		}
		last = nil
		for conditionType, message := range want {
			if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c == nil || c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonUnsupported || c.Message != message {
				last = errors.Join(last, fmt.Errorf("%s is %+v, want False %s %q", conditionType, c, lodiav1alpha1.ReasonUnsupported, message))
			}
		}
		return last == nil, nil
	}); err != nil {
		t.Fatalf("%v: %v", err, last)
	}
}
