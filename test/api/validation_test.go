// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

var (
	k8s client.Client
	env *envtest.Environment
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Exit(m.Run())
	}
	env = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "api", "crds")},
		ErrorIfCRDPathMissing: true,
		UseExistingCluster:    new(false),
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		os.Exit(1)
	}
	k8s, err = client.New(cfg, client.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		_ = env.Stop()
		os.Exit(1)
	}
	code := m.Run()
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop envtest:", err)
	}
	os.Exit(code)
}

func examples(t *testing.T) []*unstructured.Unstructured {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "docs", "api", "examples", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no examples found: %v", err)
	}
	var out []*unstructured.Unstructured
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, decode(t, string(raw)))
	}
	return out
}

func example(t *testing.T, file, name string) *unstructured.Unstructured {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "api", "examples", file))
	if err != nil {
		t.Fatal(err)
	}
	obj := decode(t, string(raw))
	obj.SetName(name)
	return obj
}

func decode(t *testing.T, manifest string) *unstructured.Unstructured {
	t.Helper()
	var m map[string]any
	if err := yaml.Unmarshal([]byte(manifest), &m); err != nil {
		t.Fatal(err)
	}
	return &unstructured.Unstructured{Object: m}
}

func set(t *testing.T, obj *unstructured.Unstructured, value any, path ...string) {
	t.Helper()
	if err := unstructured.SetNestedField(obj.Object, value, path...); err != nil {
		t.Fatal(err)
	}
}

func needsEnvtest(t *testing.T) {
	t.Helper()
	if k8s == nil {
		t.Skip("KUBEBUILDER_ASSETS is not set; run make test")
	}
}

func ensureNamespaces(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n}}
		if err := k8s.Create(context.Background(), ns); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
	}
}

func expect(t *testing.T, err error, want string) {
	t.Helper()
	switch {
	case want == "" && err != nil:
		t.Fatalf("want accepted, got %v", err)
	case want != "" && err == nil:
		t.Fatalf("want rejected with %q, got accepted", want)
	case want != "" && !apierrors.IsInvalid(err):
		t.Fatalf("want an Invalid error with %q, got %v", want, err)
	case want != "" && !strings.Contains(err.Error(), want):
		t.Fatalf("want %q in the error, got %v", want, err)
	}
}

func TestExamplesAreAccepted(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a", "team-b")
	for _, obj := range examples(t) {
		t.Run(obj.GetKind()+"/"+obj.GetName(), func(t *testing.T) {
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), "")
		})
	}
}

func TestControlPlaneDefaults(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	obj := example(t, "controlplane-single.yaml", "defaults")
	expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), "")
	if layout, _, _ := unstructured.NestedString(obj.Object, "spec", "layout"); layout != "Split" {
		t.Fatalf("layout = %q, want Split", layout)
	}
	if domain, _, _ := unstructured.NestedString(obj.Object, "spec", "network", "clusterDomain"); domain != "cluster.local" {
		t.Fatalf("clusterDomain = %q, want cluster.local", domain)
	}
}

func TestControlPlaneValidation(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	cases := []struct {
		name   string
		mutate func(*testing.T, *unstructured.Unstructured)
		want   string
	}{
		{"version without v", func(t *testing.T, o *unstructured.Unstructured) { set(t, o, "1.35.2", "spec", "version") }, "spec.version"},
		{"major other than 1", func(t *testing.T, o *unstructured.Unstructured) { set(t, o, "v2.0.0", "spec", "version") }, "spec.version"},
		{"two datastores", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"dedicated": map[string]any{}, "shared": map[string]any{"name": "pool-a"}}, "spec", "datastore")
		}, "set exactly one of dedicated and shared"},
		{"no datastore", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{}, "spec", "datastore")
		}, "set exactly one of dedicated and shared"},
		{"two endpoints", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"loadBalancer": map[string]any{}, "nodePort": map[string]any{}}, "spec", "endpoint")
		}, "set exactly one of gateway, loadBalancer and nodePort"},
		{"bad CIDR", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{"10.0.0.0/33"}, "spec", "network", "serviceCIDRs")
		}, "must be a network in CIDR notation"},
		{"compact with high availability", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "Compact", "spec", "layout")
			set(t, o, map[string]any{"failureTolerance": "Node"}, "spec", "highAvailability")
		}, "the Compact layout runs one replica of everything"},
		{"compact with two API servers", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "Compact", "spec", "layout")
			set(t, o, int64(2), "spec", "apiServer", "replicas")
		}, "the Compact layout runs one API server"},
		{"high availability with one API server", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"failureTolerance": "Node"}, "spec", "highAvailability")
			set(t, o, int64(1), "spec", "apiServer", "replicas")
		}, "highAvailability runs at least two API servers"},
		{"high availability with two API servers", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"failureTolerance": "Zone"}, "spec", "highAvailability")
			set(t, o, int64(2), "spec", "apiServer", "replicas")
		}, ""},
		{"high availability with the default replicas", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"failureTolerance": "Node"}, "spec", "highAvailability")
		}, ""},
		{"a flag Lodia sets", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"etcd-servers": "https://elsewhere:2379"}, "spec", "apiServer", "extraArgs")
		}, "Lodia sets this flag"},
		{"a flag Lodia leaves alone", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"max-requests-inflight": "800"}, "spec", "apiServer", "extraArgs")
		}, ""},
		{"uppercase hostname", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{"API.example.com"}, "spec", "endpoint", "certSANs")
		}, "spec.endpoint.certSANs"},
		{"a hostname label of 63 characters", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{strings.Repeat("a", 63) + ".example.com"}, "spec", "endpoint", "certSANs")
		}, ""},
		{"a hostname label of 64 characters", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{strings.Repeat("a", 64) + ".example.com"}, "spec", "endpoint", "certSANs")
		}, "must be a lowercase DNS name"},
		{"a hostname of 256.256.256.256", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{"256.256.256.256"}, "spec", "endpoint", "certSANs")
		}, "must be a lowercase DNS name"},
		{"a hostname of 1.2.3", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{"1.2.3"}, "spec", "endpoint", "certSANs")
		}, "must be a lowercase DNS name"},
		{"plain http issuer", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "http://id.example.com", "spec", "serviceAccount", "issuer")
		}, "must be an https URL"},
		{"window at 25:00", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"begin": "25:00", "end": "04:00"}, "spec", "maintenance", "window")
		}, "spec.maintenance.window.begin"},
		{"a window that ends where it begins", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"begin": "02:00", "end": "02:00"}, "spec", "maintenance", "window")
		}, "begin and end must differ"},
		{"a window across midnight", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"begin": "22:00", "end": "02:00", "timeZone": "Europe/Kyiv"}, "spec", "maintenance", "window")
		}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-single.yaml", "validation")
			c.mutate(t, obj)
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
		})
	}
}

func TestExtraArgsRefuseTheFlagsLodiaSets(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const refused = "Lodia sets this flag"
	for component, flags := range map[string]map[string]string{
		"apiServer": {
			"goaway-chance": refused, "etcd-prefix": refused, "etcd-servers-overrides": refused, "max-requests-inflight": "",
			"service-account-jwks-uri": refused, "service-account-max-token-expiration": "",
		},
		"controllerManager": {
			"leader-elect-lease-duration": refused, "leader-elect-renew-deadline": refused, "leader-elect-retry-period": refused,
			"leader-elect-resource-lock": refused, "leader-elect-resource-name": refused, "leader-elect-resource-namespace": refused,
			"node-monitor-grace-period": refused, "node-monitor-period": refused, "terminated-pod-gc-threshold": "",
		},
		"scheduler": {
			"leader-elect-resource-lock": refused, "leader-elect-resource-name": refused, "leader-elect-resource-namespace": refused,
			"leader-elect-lease-duration": "",
		},
	} {
		for flag, want := range flags {
			t.Run(component+"/"+flag, func(t *testing.T) {
				obj := example(t, "controlplane-single.yaml", "flags")
				set(t, obj, map[string]any{flag: "1"}, "spec", component, "extraArgs")
				expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), want)
			})
		}
	}
}

func TestControlPlaneNames(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	for _, c := range []struct{ name, want string }{
		{strings.Repeat("a", 32), ""},
		{strings.Repeat("a", 33), "at most 32 characters"},
		{"1a", "at most 32 characters"},
		{"a.b", "at most 32 characters"},
	} {
		t.Run(c.name, func(t *testing.T) {
			expect(t, k8s.Create(context.Background(), example(t, "controlplane-single.yaml", c.name), client.DryRunAll), c.want)
		})
	}
}

func TestControlPlaneTransitions(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	cases := []struct {
		name   string
		mutate func(*testing.T, *unstructured.Unstructured)
		want   string
	}{
		{"minor down", func(t *testing.T, o *unstructured.Unstructured) { set(t, o, "v1.36.4", "spec", "version") }, "the Kubernetes minor version cannot decrease"},
		{"patch down", func(t *testing.T, o *unstructured.Unstructured) { set(t, o, "v1.37.0", "spec", "version") }, ""},
		{"minor up", func(t *testing.T, o *unstructured.Unstructured) { set(t, o, "v1.38.0", "spec", "version") }, ""},
		{"layout changed", func(t *testing.T, o *unstructured.Unstructured) { set(t, o, "Compact", "spec", "layout") }, "layout is immutable"},
		{"datastore changed", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"shared": map[string]any{"name": "pool-a"}}, "spec", "datastore")
		}, "datastore is immutable"},
		{"network changed", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{"10.100.0.0/16"}, "spec", "network", "podCIDRs")
		}, "network is immutable"},
		{"restoreFrom added", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"backupRef": map[string]any{"name": "nightly"}}, "spec", "restoreFrom")
		}, "restoreFrom can be set only when the ControlPlane is created, and never removed"},
		{"paused", func(t *testing.T, o *unstructured.Unstructured) { set(t, o, true, "spec", "paused") }, ""},
		{"high availability added", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"failureTolerance": "Zone"}, "spec", "highAvailability")
		}, ""},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			update(t, example(t, "controlplane-single.yaml", fmt.Sprintf("transition-%d", i)), c.mutate, c.want)
		})
	}
}

func TestExtraArgsStoredBeforeTheirRefusalStayUntilTheMapChanges(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	stored := example(t, "controlplane-single.yaml", "ratcheting")
	set(t, stored, map[string]any{"node-monitor-grace-period": "40s"}, "spec", "controllerManager", "extraArgs")
	storeUnderAnOlderSchema(t, "extraargs.ratcheting.lodia.io", stored, "Lodia sets this flag", func(schema map[string]any) {
		unstructured.RemoveNestedField(schema, "properties", "spec", "properties", "controllerManager", "properties", "extraArgs", "x-kubernetes-validations")
	})

	set(t, stored, true, "spec", "paused")
	expect(t, k8s.Update(context.Background(), stored), "")
	set(t, stored, map[string]any{"node-monitor-grace-period": "40s", "terminated-pod-gc-threshold": "100"}, "spec", "controllerManager", "extraArgs")
	expect(t, k8s.Update(context.Background(), stored), "Lodia sets this flag")
}

func TestAGatewayStoredWithoutATunnelStaysUntilItsHostnamesChange(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	stored := example(t, "controlplane-ha.yaml", "tunnel-ratcheting")
	unstructured.RemoveNestedField(stored.Object, "spec", "endpoint", "gateway", "hostnames", "tunnel")
	storeUnderAnOlderSchema(t, "tunnel.ratcheting.lodia.io", stored, "tunnel: Required value", func(schema map[string]any) {
		hostnames := []string{"properties", "spec", "properties", "endpoint", "properties", "gateway", "properties", "hostnames"}
		unstructured.RemoveNestedField(schema, append(hostnames, "properties", "tunnel")...)
		unstructured.RemoveNestedField(schema, append(hostnames, "x-kubernetes-validations")...)
		if err := unstructured.SetNestedStringSlice(schema, []string{"apiServer", "konnectivity"}, append(hostnames, "required")...); err != nil {
			t.Fatal(err)
		}
	})
	ctx := context.Background()
	stored.SetFinalizers([]string{lodiav1alpha1.DatastoreFinalizer})
	expect(t, k8s.Update(ctx, stored), "")
	stored.SetFinalizers(nil)
	expect(t, k8s.Update(ctx, stored), "")
	set(t, stored, true, "spec", "paused")
	expect(t, k8s.Update(ctx, stored), "")
	set(t, stored, "api2.prod-eu.k8s.example.com", "spec", "endpoint", "gateway", "hostnames", "apiServer")
	expect(t, k8s.Update(ctx, stored), "tunnel: Required value")
}

func TestAnAdvertiseAddressStoredOutsideGatewayModeCanOnlyBeRemoved(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const gatewayOnly = "apiServer.advertiseAddress is set only with endpoint.gateway"
	stored := example(t, "controlplane-single.yaml", "advertise-ratcheting")
	set(t, stored, "240.0.0.2", "spec", "apiServer", "advertiseAddress")
	storeUnderAnOlderSchema(t, "advertise.ratcheting.lodia.io", stored, gatewayOnly, func(schema map[string]any) {
		rules, _, err := unstructured.NestedSlice(schema, "properties", "spec", "x-kubernetes-validations")
		if err != nil {
			t.Fatal(err)
		}
		kept := slices.DeleteFunc(rules, func(r any) bool { return r.(map[string]any)["message"] == gatewayOnly })
		if err := unstructured.SetNestedSlice(schema, kept, "properties", "spec", "x-kubernetes-validations"); err != nil {
			t.Fatal(err)
		}
	})
	ctx := context.Background()
	stored.SetFinalizers([]string{lodiav1alpha1.DatastoreFinalizer})
	expect(t, k8s.Update(ctx, stored), "")
	set(t, stored, true, "spec", "paused")
	expect(t, k8s.Update(ctx, stored), gatewayOnly)
	unstructured.RemoveNestedField(stored.Object, "spec", "apiServer", "advertiseAddress")
	expect(t, k8s.Update(ctx, stored), "")
}

func eventually(t *testing.T, f func() error) {
	t.Helper()
	var last error
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 30*time.Second, true, func(context.Context) (bool, error) {
		last = f()
		return last == nil, nil
	}); err != nil {
		t.Fatalf("%v: %v", err, last)
	}
}

func storeUnderAnOlderSchema(t *testing.T, group string, stored *unstructured.Unstructured, refusal string, older func(schema map[string]any)) {
	t.Helper()
	ctx := context.Background()
	raw, err := os.ReadFile(filepath.Join("..", "..", "api", "crds", "lodia.io_controlplanes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	crd := decode(t, string(raw))
	crd.SetName("controlplanes." + group)
	set(t, crd, group, "spec", "group")
	current, _, err := unstructured.NestedSlice(crd.Object, "spec", "versions")
	if err != nil {
		t.Fatal(err)
	}
	previous := runtime.DeepCopyJSONValue(current).([]any)
	older(previous[0].(map[string]any)["schema"].(map[string]any)["openAPIV3Schema"].(map[string]any))
	set(t, crd, previous, "spec", "versions")
	if err := k8s.Create(ctx, crd); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), crd) })

	stored.SetAPIVersion(group + "/v1alpha1")
	eventually(t, func() error { return k8s.Create(ctx, stored) })
	eventually(t, func() error {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(crd), crd); err != nil {
			return err
		}
		set(t, crd, current, "spec", "versions")
		return k8s.Update(ctx, crd)
	})
	probe := stored.DeepCopy()
	probe.SetName(stored.GetName() + "-probe")
	probe.SetResourceVersion("")
	eventually(t, func() error {
		err := k8s.Create(ctx, probe, client.DryRunAll)
		switch {
		case err == nil:
			return errors.New("the current schema is not served yet")
		case !strings.Contains(err.Error(), refusal):
			return err
		}
		return nil
	})
}

func TestHighAvailabilityTransitions(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	cases := []struct {
		name   string
		mutate func(*testing.T, *unstructured.Unstructured)
		want   string
	}{
		{"failure tolerance changed", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "Zone", "spec", "highAvailability", "failureTolerance")
		}, "failureTolerance is immutable once set"},
		{"high availability removed", func(t *testing.T, o *unstructured.Unstructured) {
			unstructured.RemoveNestedField(o.Object, "spec", "highAvailability")
		}, ""},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-single.yaml", fmt.Sprintf("ha-transition-%d", i))
			set(t, obj, map[string]any{"failureTolerance": "Node"}, "spec", "highAvailability")
			update(t, obj, c.mutate, c.want)
		})
	}
}

func update(t *testing.T, obj *unstructured.Unstructured, mutate func(*testing.T, *unstructured.Unstructured), want string) {
	t.Helper()
	ctx := context.Background()
	if err := k8s.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
	mutate(t, obj)
	expect(t, k8s.Update(ctx, obj), want)
}

func TestGatewayHostnames(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const differ = "apiServer, konnectivity and tunnel must be three different hostnames"
	api, konnectivity, tunnel := "api.a.example.com", "konnectivity.a.example.com", "tunnel.a.example.com"
	for _, c := range []struct {
		name      string
		hostnames map[string]any
		want      string
	}{
		{"three hostnames", map[string]any{"apiServer": api, "konnectivity": konnectivity, "tunnel": tunnel}, ""},
		{"no tunnel", map[string]any{"apiServer": api, "konnectivity": konnectivity}, "tunnel: Required value"},
		{"the API server's for konnectivity", map[string]any{"apiServer": api, "konnectivity": api, "tunnel": tunnel}, differ},
		{"the API server's for the tunnel", map[string]any{"apiServer": api, "konnectivity": konnectivity, "tunnel": api}, differ},
		{"konnectivity's for the tunnel", map[string]any{"apiServer": api, "konnectivity": konnectivity, "tunnel": konnectivity}, differ},
	} {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-ha.yaml", "hostnames")
			set(t, obj, c.hostnames, "spec", "endpoint", "gateway", "hostnames")
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
		})
	}
}

func TestAdvertiseAddress(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const (
		unicast = "must be a unicast IP address in canonical form, neither loopback nor link-local"
		family  = "must be of the family of the first service range"
		outside = "must lie outside the service and pod ranges"
		ipv6    = "needs apiServer.advertiseAddress, which has no IPv6 default"
		gateway = "apiServer.advertiseAddress is set only with endpoint.gateway"
	)
	address := func(a string) func(*testing.T, *unstructured.Unstructured) {
		return func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, a, "spec", "apiServer", "advertiseAddress")
		}
	}
	ipv6Ranges := func(t *testing.T, o *unstructured.Unstructured) {
		set(t, o, []any{"fd00:10:96::/112"}, "spec", "network", "serviceCIDRs")
		set(t, o, []any{"fd00:10:244::/56"}, "spec", "network", "podCIDRs")
	}
	cases := []struct {
		name, file string
		mutate     []func(*testing.T, *unstructured.Unstructured)
		want       string
	}{
		{"the default", "controlplane-ha.yaml", nil, ""},
		{"another address of class E", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){address("240.0.0.2")}, ""},
		{"behind a load balancer", "controlplane-single.yaml", []func(*testing.T, *unstructured.Unstructured){address("240.0.0.2")}, gateway},
		{"behind a node port", "controlplane-single.yaml", []func(*testing.T, *unstructured.Unstructured){func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"nodePort": map[string]any{}}, "spec", "endpoint")
		}, address("240.0.0.2")}, gateway},
		{"a private address", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){address("172.20.0.1")}, ""},
		{"loopback", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){address("127.0.0.1")}, unicast},
		{"link-local", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){address("169.254.0.1")}, unicast},
		{"unspecified", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){address("0.0.0.0")}, unicast},
		{"multicast", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){address("239.1.1.1")}, unicast},
		{"broadcast", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){address("255.255.255.255")}, unicast},
		{"a name", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){address("proxy.example.com")}, unicast},
		{"in the service range", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){address("10.96.0.1")}, outside},
		{"in the pod range", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){address("10.244.3.1")}, outside},
		{"the default in the pod range", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{"240.0.0.0/16"}, "spec", "network", "podCIDRs")
		}}, outside},
		{"the default's range for pods outside gateway mode", "controlplane-single.yaml", []func(*testing.T, *unstructured.Unstructured){func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{"240.0.0.0/16"}, "spec", "network", "podCIDRs")
		}}, ""},
		{"IPv6 beside an IPv4 service range", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){address("fd12:3456:789a::1")}, family},
		{"IPv6 not in canonical form", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){ipv6Ranges, address("FD12:3456:789A::1")}, unicast},
		{"an IPv6 service range without an address", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){ipv6Ranges}, ipv6},
		{"an IPv6 service range with a unique local address", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){ipv6Ranges, address("fd12:3456:789a::1")}, ""},
		{"an IPv4 address beside an IPv6 service range", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){ipv6Ranges, address("240.0.0.1")}, family},
		{"dual stack, IPv4 first, with the default", "controlplane-ha.yaml", []func(*testing.T, *unstructured.Unstructured){func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{"10.96.0.0/16", "fd00:10:96::/112"}, "spec", "network", "serviceCIDRs")
		}}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, c.file, "advertised")
			for _, mutate := range c.mutate {
				mutate(t, obj)
			}
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
		})
	}
}

func TestAdvertiseAddressTransitions(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const immutable = "apiServer.advertiseAddress is immutable"
	cases := []struct {
		name, from string
		mutate     func(*testing.T, *unstructured.Unstructured)
		want       string
	}{
		{"the default named", "", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "240.0.0.1", "spec", "apiServer", "advertiseAddress")
		}, immutable},
		{"changed", "240.0.0.2", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "240.0.0.3", "spec", "apiServer", "advertiseAddress")
		}, immutable},
		{"removed", "240.0.0.2", func(t *testing.T, o *unstructured.Unstructured) {
			unstructured.RemoveNestedField(o.Object, "spec", "apiServer", "advertiseAddress")
		}, immutable},
		{"kept while something else changes", "240.0.0.2", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, true, "spec", "paused")
		}, ""},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-ha.yaml", fmt.Sprintf("advertised-%d", i))
			if c.from != "" {
				set(t, obj, c.from, "spec", "apiServer", "advertiseAddress")
			}
			update(t, obj, c.mutate, c.want)
		})
	}
}

func TestTheSchemaDefaultsTheAdvertiseAddressAsTheAPI(t *testing.T) {
	manifest, err := os.ReadFile(filepath.Join("..", "..", "api", "crds", "lodia.io_controlplanes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	quoted := "'" + lodiav1alpha1.DefaultAdvertiseAddress + "'"
	if !slices.ContainsFunc(rules(at(t, crdSchema(t, manifest), "spec")), func(rule string) bool { return strings.Contains(rule, "["+quoted+"]") }) {
		t.Errorf("no rule of spec takes %s for the advertise address of gateway mode", quoted)
	}
}

func TestRestoreFromAtCreation(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	for _, c := range []struct {
		name, release, want, stored string
	}{
		{"held by default", "", "", "Held"},
		{"released at once", "Released", "", "Released"},
		{"another release", "Paused", `supported values: "Held", "Released"`, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-single.yaml", "restored")
			restoreFrom := map[string]any{"backupRef": map[string]any{"name": "nightly"}}
			if c.release != "" {
				restoreFrom["release"] = c.release
			}
			set(t, obj, restoreFrom, "spec", "restoreFrom")
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
			if release, _, _ := unstructured.NestedString(obj.Object, "spec", "restoreFrom", "release"); c.want == "" && release != c.stored {
				t.Errorf("restoreFrom.release is %q, want %q", release, c.stored)
			}
		})
	}
}

func TestRestoreFromTransitions(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	cases := []struct {
		name, from string
		mutate     func(*testing.T, *unstructured.Unstructured)
		want       string
	}{
		{"removed", "Held", func(t *testing.T, o *unstructured.Unstructured) {
			unstructured.RemoveNestedField(o.Object, "spec", "restoreFrom")
		}, "restoreFrom can be set only when the ControlPlane is created, and never removed"},
		{"another Backup", "Held", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "weekly", "spec", "restoreFrom", "backupRef", "name")
		}, "restoreFrom.backupRef is immutable"},
		{"released", "Held", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "Released", "spec", "restoreFrom", "release")
		}, ""},
		{"held again", "Released", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "Held", "spec", "restoreFrom", "release")
		}, "restoreFrom.release moves only from Held to Released"},
		{"kept while something else changes", "Held", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, true, "spec", "paused")
		}, ""},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-single.yaml", fmt.Sprintf("restore-transition-%d", i))
			set(t, obj, map[string]any{"backupRef": map[string]any{"name": "nightly"}, "release": c.from}, "spec", "restoreFrom")
			update(t, obj, c.mutate, c.want)
		})
	}
}

func TestBackupNames(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	for _, c := range []struct{ name, want string }{
		{strings.Repeat("b", 235), ""},
		{strings.Repeat("b", 236), "the name must be at most 235 characters"},
	} {
		t.Run(fmt.Sprintf("%d characters", len(c.name)), func(t *testing.T) {
			expect(t, k8s.Create(context.Background(), example(t, "backup.yaml", c.name), client.DryRunAll), c.want)
		})
	}
}

func TestBackupStatus(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	obj := example(t, "backup.yaml", "status")
	if err := k8s.Create(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
	status := func(revision int64) map[string]any {
		return map[string]any{
			"conditions": []any{map[string]any{
				"type": "Ready", "status": "True", "reason": "Completed", "message": "sealed at revision 184739",
				"lastTransitionTime": "2026-10-27T02:00:07Z",
			}},
			"source":            map[string]any{"name": "prod-eu", "uid": "6f1c1b6e-58a4-4b8a-9f5e-2b0a5d3c7e91"},
			"kubernetesVersion": "v1.37.1",
			"network":           map[string]any{"serviceCIDRs": []any{"10.96.0.0/16"}, "podCIDRs": []any{"10.244.0.0/16"}},
			"snapshots": map[string]any{
				"provider": "S3", "bucket": "lodia-backups",
				"prefix": "prod/8d2f2f7e-0c1a-4b7e-9d43-6a1f0b9e2c55/backups/3b9e0c2a-7d41-4f6e-8a5c-1e2d3f4a5b6c",
			},
			"seal":        map[string]any{"revision": revision, "quotaBackendBytes": "4Gi"},
			"startedAt":   "2026-10-27T02:00:00Z",
			"completedAt": "2026-10-27T02:00:07Z",
		}
	}
	for _, c := range []struct {
		name     string
		revision int64
		want     string
	}{
		{"sealed", 184739, ""},
		{"a seal at revision 0", 0, "status.seal.revision"},
	} {
		t.Run(c.name, func(t *testing.T) {
			written := obj.DeepCopy()
			set(t, written, status(c.revision), "status")
			expect(t, k8s.Status().Update(context.Background(), written, client.DryRunAll), c.want)
		})
	}
}

func TestEndpointStatus(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	obj := example(t, "controlplane-single.yaml", "endpoint-status")
	if err := k8s.Create(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
	address := func(host string, port int64) map[string]any { return map[string]any{"host": host, "port": port} }
	inCluster := address("203.0.113.10", 6443)
	cases := []struct {
		name     string
		endpoint map[string]any
		want     string
	}{
		{"an IPv4 address", map[string]any{"apiServer": address("203.0.113.10", 443), "konnectivity": address("203.0.113.10", 8132), "inCluster": inCluster}, ""},
		{"an IPv6 address", map[string]any{"apiServer": address("2001:db8::1", 443), "konnectivity": address("2001:db8::1", 8132), "inCluster": inCluster}, ""},
		{"DNS names", map[string]any{"apiServer": address("api.example.test", 443), "konnectivity": address("konnectivity.example.test", 443), "inCluster": inCluster}, ""},
		{"an IPv6 address in brackets", map[string]any{"apiServer": address("[2001:db8::1]", 443), "konnectivity": address("2001:db8::1", 8132), "inCluster": inCluster}, "must be an IP address or a lowercase DNS name"},
		{"an uppercase name", map[string]any{"apiServer": address("API.example.test", 443), "konnectivity": address("api.example.test", 8132), "inCluster": inCluster}, "must be an IP address or a lowercase DNS name"},
		{"a label of 63 characters", map[string]any{"apiServer": address(strings.Repeat("a", 63)+".example.test", 443), "konnectivity": address("api.example.test", 8132), "inCluster": inCluster}, ""},
		{"a label of 64 characters", map[string]any{"apiServer": address(strings.Repeat("a", 64)+".example.test", 443), "konnectivity": address("api.example.test", 8132), "inCluster": inCluster}, "must be an IP address or a lowercase DNS name"},
		{"256.256.256.256", map[string]any{"apiServer": address("256.256.256.256", 443), "konnectivity": address("203.0.113.10", 8132), "inCluster": inCluster}, "must be an IP address or a lowercase DNS name"},
		{"1.2.3", map[string]any{"apiServer": address("1.2.3", 443), "konnectivity": address("203.0.113.10", 8132), "inCluster": inCluster}, "must be an IP address or a lowercase DNS name"},
		{"port 0", map[string]any{"apiServer": address("203.0.113.10", 0), "konnectivity": address("203.0.113.10", 8132), "inCluster": inCluster}, "greater than or equal to 1"},
		{"no konnectivity", map[string]any{"apiServer": address("203.0.113.10", 443), "inCluster": inCluster}, "konnectivity: Required value"},
		{"no inCluster", map[string]any{"apiServer": address("203.0.113.10", 443), "konnectivity": address("203.0.113.10", 8132)}, "inCluster: Required value"},
		{"an IPv6 inCluster", map[string]any{"apiServer": address("api.example.test", 443), "konnectivity": address("api.example.test", 443), "inCluster": address("2001:db8::1", 6443)}, ""},
		{"an inCluster DNS name", map[string]any{"apiServer": address("api.example.test", 443), "konnectivity": address("api.example.test", 443), "inCluster": address("api.example.test", 6443)}, "status.endpoint.inCluster: Invalid value"},
		{"a tunnel", map[string]any{"apiServer": address("api.example.test", 443), "konnectivity": address("konnectivity.example.test", 443), "inCluster": inCluster, "tunnel": address("tunnel.example.test", 443)}, ""},
		{"a tunnel of an uppercase name", map[string]any{"apiServer": address("api.example.test", 443), "konnectivity": address("konnectivity.example.test", 443), "inCluster": inCluster, "tunnel": address("Tunnel.example.test", 443)}, "must be an IP address or a lowercase DNS name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			written := obj.DeepCopy()
			set(t, written, tc.endpoint, "status", "endpoint")
			expect(t, k8s.Status().Update(context.Background(), written, client.DryRunAll), tc.want)
		})
	}
}

func TestKubernetesStatus(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	obj := example(t, "controlplane-single.yaml", "kubernetes-status")
	if err := k8s.Create(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
	for _, c := range []struct {
		name    string
		upgrade map[string]any
		want    string
	}{
		{"a step", map[string]any{"step": "ControllerManager", "startedAt": "2026-10-27T02:00:00Z"}, ""},
		{"another step", map[string]any{"step": "Nodes", "startedAt": "2026-10-27T02:00:00Z"}, `supported values: "Backup", "APIServer", "ControllerManager", "Scheduler", "Addons"`},
		{"no start", map[string]any{"step": "APIServer"}, "startedAt: Required value"},
	} {
		t.Run(c.name, func(t *testing.T) {
			written := obj.DeepCopy()
			set(t, written, map[string]any{"version": "v1.36.4", "upgrade": c.upgrade}, "status", "kubernetes")
			expect(t, k8s.Status().Update(context.Background(), written, client.DryRunAll), c.want)
		})
	}
}

func TestTheRestoreRecordIsWrittenWhole(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	obj := example(t, "controlplane-single.yaml", "restore-record")
	if err := k8s.Create(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
	backup := map[string]any{"name": "nightly", "uid": "6a1f4c1e-2f47-4c58-9a40-0d2e31f6a7b1"}
	source := map[string]any{"name": "old", "uid": "0b5d9e3a-81c2-4f4e-b6d7-5c2a1e9f8d30"}
	for _, c := range []struct {
		name    string
		restore map[string]any
		want    string
	}{
		{"completed", map[string]any{"backup": backup, "source": source, "completedAt": "2026-10-27T02:00:00Z"}, ""},
		{"not completed", map[string]any{"backup": backup, "source": source}, "completedAt: Required value"},
		{"without its source", map[string]any{"backup": backup, "completedAt": "2026-10-27T02:00:00Z"}, "source: Required value"},
	} {
		t.Run(c.name, func(t *testing.T) {
			written := obj.DeepCopy()
			set(t, written, map[string]any{
				"endpoints": []any{map[string]any{"host": "restore-record-etcd-client.team-a.svc", "port": int64(2379)}},
				"members":   map[string]any{"total": int64(1), "ready": int64(1)},
				"restore":   c.restore,
			}, "status", "datastore")
			expect(t, k8s.Status().Update(context.Background(), written, client.DryRunAll), c.want)
		})
	}
}

func TestAHostTakesTheHostnameRule(t *testing.T) {
	manifest, err := os.ReadFile(filepath.Join("..", "..", "api", "crds", "lodia.io_controlplanes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	schema := crdSchema(t, manifest)
	hostname := rules(at(t, schema, "spec.network.clusterDomain"))
	host := rules(at(t, schema, "status.endpoint.apiServer.host"))
	if len(hostname) != 1 || !slices.Equal(host, []string{"isIP(self) || " + hostname[0]}) {
		t.Errorf("HostPort.host has the rules %q, want isIP(self) or the Hostname's %q", host, hostname)
	}
}

func TestSmallerShapes(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	cases := []struct {
		name   string
		mutate func(*testing.T, *unstructured.Unstructured)
		want   string
	}{
		{"kube-proxy's health address an IP", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "0.0.0.0", "spec", "addons", "kubeProxy", "healthzBindAddress")
		}, ""},
		{"kube-proxy's health address with a port", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "0.0.0.0:10256", "spec", "addons", "kubeProxy", "healthzBindAddress")
		}, "must be an IP address"},
		{"kube-proxy's conntrack.maxPerCore 0", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, int64(0), "spec", "addons", "kubeProxy", "conntrack", "maxPerCore")
		}, ""},
		{"kube-proxy's conntrack.maxPerCore -1", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, int64(-1), "spec", "addons", "kubeProxy", "conntrack", "maxPerCore")
		}, "spec.addons.kubeProxy.conntrack.maxPerCore in body should be greater than or equal to 0"},
		{"a Backup in another namespace", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"backupRef": map[string]any{"name": "nightly", "namespace": "team-b"}}, "spec", "restoreFrom")
		}, `unknown field "spec.restoreFrom.backupRef.namespace"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := example(t, "controlplane-single.yaml", "shapes")
			tc.mutate(t, obj)
			err := k8s.Create(context.Background(), obj, client.DryRunAll, client.FieldValidation("Strict"))
			if tc.want == "" || !strings.Contains(tc.want, "unknown field") {
				expect(t, err, tc.want)
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
		})
	}
}

func TestKubeconfigRequestValidation(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "alice"}}, key)
	if err != nil {
		t.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	sec1, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	spki, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	block := func(typ string, der []byte) []byte { return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}) }
	sized := func(n int) []byte {
		header, footer := "-----BEGIN CERTIFICATE REQUEST-----\n", "\n-----END CERTIFICATE REQUEST-----\n"
		return []byte(header + strings.Repeat("A", n-len(header)-len(footer)) + footer)
	}
	request := func(parts ...[]byte) func(*testing.T, *unstructured.Unstructured) {
		return func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, base64.StdEncoding.EncodeToString(bytes.Join(parts, nil)), "spec", "request")
		}
	}
	const notARequest = "must be one PEM certificate request"
	cases := []struct {
		name   string
		mutate func(*testing.T, *unstructured.Unstructured)
		want   string
	}{
		{"ten minutes less a second", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, int64(599), "spec", "expirationSeconds")
		}, "spec.expirationSeconds"},
		{"ten minutes", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, int64(600), "spec", "expirationSeconds")
		}, ""},
		{"a day", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, int64(86400), "spec", "expirationSeconds")
		}, ""},
		{"a day and a second", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, int64(86401), "spec", "expirationSeconds")
		}, "spec.expirationSeconds"},
		{"no controlPlaneRef", func(t *testing.T, o *unstructured.Unstructured) {
			unstructured.RemoveNestedField(o.Object, "spec", "controlPlaneRef")
		}, "spec.controlPlaneRef: Required value"},
		{"an unknown access", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "View", "spec", "access")
		}, "spec.access"},
		{"no request", func(t *testing.T, o *unstructured.Unstructured) {
			unstructured.RemoveNestedField(o.Object, "spec", "request")
		}, "spec.request"},
		{"a request of 5120 characters", request(sized(3840)), ""},
		{"a request of more than 5120 characters", request(sized(3843)), "spec.request"},
		{"a PKCS#8 key", request(block("PRIVATE KEY", pkcs8)), notARequest},
		{"an EC key", request(block("EC PRIVATE KEY", sec1)), notARequest},
		{"a request followed by its key", request(block("CERTIFICATE REQUEST", csr), block("PRIVATE KEY", pkcs8)), notARequest},
		{"a public key", request(block("PUBLIC KEY", spki)), notARequest},
		{"a request after a line of text", request([]byte("Certificate Request:\n"), block("CERTIFICATE REQUEST", csr)), notARequest},
		{"a request before a line of text", request(block("CERTIFICATE REQUEST", csr), []byte("Certificate Request:\n")), notARequest},
		{"a request followed by its key unarmoured", request(block("CERTIFICATE REQUEST", csr), []byte(base64.StdEncoding.EncodeToString(pkcs8))), notARequest},
		{"two requests", request(block("CERTIFICATE REQUEST", csr), block("CERTIFICATE REQUEST", csr)), notARequest},
		{"a request with CRLF line ends", request(bytes.ReplaceAll(block("CERTIFICATE REQUEST", csr), []byte("\n"), []byte("\r\n"))), ""},
		{"no user", func(t *testing.T, o *unstructured.Unstructured) {
			unstructured.RemoveNestedField(o.Object, "spec", "user")
		}, "spec.user: Required value"},
		{"an empty user", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "", "spec", "user")
		}, "spec.user"},
		{"a user of 512 characters", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, strings.Repeat("u", 512), "spec", "user")
		}, ""},
		{"a user of 513 characters", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, strings.Repeat("u", 513), "spec", "user")
		}, "spec.user"},
		{"a user with a letter beyond ASCII", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "åsa@example.com", "spec", "user")
		}, ""},
		{"a user with a DEL", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "alice\u007f", "spec", "user")
		}, "must hold no control character"},
		{"a user with a C1 control", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "alice\u0085", "spec", "user")
		}, "must hold no control character"},
		{"a user with U+FFFE", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "alice\ufffe", "spec", "user")
		}, "must hold no control character"},
		{"a user with U+FFFF", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "alice\uffff", "spec", "user")
		}, "must hold no control character"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "kubeconfigrequest.yaml", "validation")
			c.mutate(t, obj)
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
		})
	}
}

func TestKubeconfigRequestDefaults(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	obj := example(t, "kubeconfigrequest.yaml", "defaults")
	unstructured.RemoveNestedField(obj.Object, "spec", "expirationSeconds")
	expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), "")
	if seconds, _, _ := unstructured.NestedInt64(obj.Object, "spec", "expirationSeconds"); seconds != 3600 {
		t.Fatalf("expirationSeconds = %d, want 3600", seconds)
	}
}

func TestKubeconfigRequestStatus(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	obj := example(t, "kubeconfigrequest.yaml", "status")
	if err := k8s.Create(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
	for _, c := range []struct {
		name  string
		bytes int
		want  string
	}{
		{"a kubeconfig of 40960 characters", 30720, ""},
		{"a kubeconfig of more than 40960 characters", 30723, "status.kubeconfig"},
	} {
		t.Run(c.name, func(t *testing.T) {
			written := obj.DeepCopy()
			set(t, written, base64.StdEncoding.EncodeToString(make([]byte, c.bytes)), "status", "kubeconfig")
			expect(t, k8s.Status().Update(context.Background(), written, client.DryRunAll), c.want)
		})
	}
}

func TestRequestsAndBackupsCannotChange(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	cases := []struct {
		file, want string
		path       []string
	}{
		{"kubeconfigrequest.yaml", "a KubeconfigRequest cannot change", []string{"spec", "user"}},
		{"backup.yaml", "a Backup cannot change", []string{"spec", "controlPlaneRef", "name"}},
	}
	for _, c := range cases {
		t.Run(c.file, func(t *testing.T) {
			ctx := context.Background()
			obj := example(t, c.file, "immutable")
			if err := k8s.Create(ctx, obj); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
			set(t, obj, "someone-else", c.path...)
			expect(t, k8s.Update(ctx, obj), c.want)
		})
	}
}

func TestDatastoreEndpointsAreHTTPS(t *testing.T) {
	needsEnvtest(t)
	obj := example(t, "datastore.yaml", "plain")
	set(t, obj, []any{"http://etcd-0.pool-a.etcd.svc:2379"}, "spec", "endpoints")
	expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), "must be an https URL")
}

func TestDatastoreClassValidation(t *testing.T) {
	needsEnvtest(t)
	cases := []struct {
		name  string
		value any
		path  []string
		want  string
	}{
		{"delta snapshots every 30 days", "30d", []string{"spec", "backup", "deltaSnapshotPeriod"}, "must be a duration"},
		{"delta snapshots beyond a Go duration", "2562048h", []string{"spec", "backup", "deltaSnapshotPeriod"}, "must be a duration"},
		{"delta snapshots every 90 seconds", "1m30s", []string{"spec", "backup", "deltaSnapshotPeriod"}, ""},
		{"snapshots kept 30 days after deletion", "30d", []string{"spec", "backup", "retention", "afterDelete"}, "must be a duration"},
		{"snapshots kept beyond a Go duration", "2562048h", []string{"spec", "backup", "retention", "afterDelete"}, "must be a duration"},
		{"snapshots kept a negative time", "-1h", []string{"spec", "backup", "retention", "afterDelete"}, "must be a duration"},
		{"a two-letter bucket", "ab", []string{"spec", "backup", "store", "bucket"}, "should match"},
		{"a bucket with a slash", "backups/prod", []string{"spec", "backup", "store", "bucket"}, "should match"},
		{"a 64-letter bucket", strings.Repeat("b", 64), []string{"spec", "backup", "store", "bucket"}, "spec.backup.store.bucket"},
		{"an endpoint that is no URL", "https://s3 example.com", []string{"spec", "backup", "store", "endpoint"}, "must be a URL"},
		{"a prefix v1", "v1", []string{"spec", "backup", "store", "prefix"}, "etcd-backup-restore's snapshot parser"},
		{"a prefix x/v1", "x/v1", []string{"spec", "backup", "store", "prefix"}, "etcd-backup-restore's snapshot parser"},
		{"a prefix team-v1", "team-v1", []string{"spec", "backup", "store", "prefix"}, "etcd-backup-restore's snapshot parser"},
		{"a prefix a/team-v1/b", "a/team-v1/b", []string{"spec", "backup", "store", "prefix"}, "etcd-backup-restore's snapshot parser"},
		{"a prefix /v1", "/v1", []string{"spec", "backup", "store", "prefix"}, "etcd-backup-restore's snapshot parser"},
		{"a prefix v1/", "v1/", []string{"spec", "backup", "store", "prefix"}, "etcd-backup-restore's snapshot parser"},
		{"a prefix v10", "v10", []string{"spec", "backup", "store", "prefix"}, ""},
		{"a prefix team-v2", "team-v2", []string{"spec", "backup", "store", "prefix"}, ""},
		{"an empty prefix", "", []string{"spec", "backup", "store", "prefix"}, ""},
		{"a prefix /lodia", "/lodia", []string{"spec", "backup", "store", "prefix"}, ""},
		{"a prefix lodia/", "lodia/", []string{"spec", "backup", "store", "prefix"}, ""},
		{"a prefix of 253 characters", strings.Repeat("a/v", 84) + "1", []string{"spec", "backup", "store", "prefix"}, "etcd-backup-restore's snapshot parser"},
		{"a zero quota", int64(0), []string{"spec", "etcd", "quotaBackendBytes"}, "must be greater than zero"},
		{"a negative quota", "-1Gi", []string{"spec", "etcd", "quotaBackendBytes"}, "must be greater than zero"},
		{"a quota in bytes", int64(4294967296), []string{"spec", "etcd", "quotaBackendBytes"}, ""},
		{"periodic retention in hours", map[string]any{"mode": "periodic", "retention": "5"}, []string{"spec", "etcd", "autoCompaction"}, ""},
		{"revision retention as a count", map[string]any{"mode": "revision", "retention": "1000"}, []string{"spec", "etcd", "autoCompaction"}, ""},
		{"revision retention as a duration", map[string]any{"mode": "revision", "retention": "15m"}, []string{"spec", "etcd", "autoCompaction"}, "retention must be a number, or in periodic mode a duration"},
		{"periodic retention in words", map[string]any{"mode": "periodic", "retention": "15 minutes"}, []string{"spec", "etcd", "autoCompaction"}, "retention must be a number, or in periodic mode a duration"},
		{"negative periodic retention", map[string]any{"mode": "periodic", "retention": "-15m"}, []string{"spec", "etcd", "autoCompaction"}, "retention must be a number, or in periodic mode a duration"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "datastoreclass.yaml", "validation")
			set(t, obj, c.value, c.path...)
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
		})
	}
}

func TestStorageHoldsTwiceTheQuota(t *testing.T) {
	needsEnvtest(t)
	const refused = "storage.size must be at least twice the etcd quota, etcd.quotaBackendBytes or 4Gi: defragmentation writes a copy of the database beside it"
	cases := []struct {
		name string
		size any
		etcd map[string]any
		want string
	}{
		{"8Gi without etcd", "8Gi", nil, ""},
		{"7Gi without etcd", "7Gi", nil, refused},
		{"8Gi with an empty etcd", "8Gi", map[string]any{}, ""},
		{"7Gi with an empty etcd", "7Gi", map[string]any{}, refused},
		{"3Gi with 1.5Gi", "3Gi", map[string]any{"quotaBackendBytes": "1.5Gi"}, ""},
		{"2999Mi with 1.5Gi", "2999Mi", map[string]any{"quotaBackendBytes": "1.5Gi"}, refused},
		{"8192Mi without etcd", "8192Mi", nil, ""},
		{"16Gi with 8Gi", "16Gi", map[string]any{"quotaBackendBytes": "8Gi"}, ""},
		{"8589934592 without etcd", int64(8589934592), nil, ""},
		{"8589934591 without etcd", int64(8589934591), nil, refused},
		{"2Gi with 1073741824", "2Gi", map[string]any{"quotaBackendBytes": int64(1073741824)}, ""},
		{"2047Mi with 1073741824", "2047Mi", map[string]any{"quotaBackendBytes": int64(1073741824)}, refused},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "datastoreclass.yaml", "quota")
			set(t, obj, c.size, "spec", "storage", "size")
			if c.etcd == nil {
				unstructured.RemoveNestedField(obj.Object, "spec", "etcd")
			} else {
				set(t, obj, c.etcd, "spec", "etcd")
			}
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
		})
	}
}

var schedules = []struct {
	schedule, want string
	parses         bool
}{
	{"0 3 * * *", "", true},
	{"*/15 * * * *", "", true},
	{"0 */24 * * *", "", true},
	{"0 3 1 */2 *", "", true},
	{"0 3 * * 1-6", "", true},
	{"0 3 * * */7", "", true},
	{"0 3 * * 0", "should match", true},
	{"0 3 * * MON", "should match", true},
	{"0 3,15 * * *", "should match", true},
	{"@daily", "should match", true},
	{"0 3 * * 7", "the day of the week cannot be 7", false},
	{"0 3 * * 1-7", "the day of the week cannot be 7", false},
	{"0 23-20 * * *", "every range must ascend", false},
	{"59-0 * * * *", "every range must ascend", false},
	{"0  3 * * *", "must be five fields separated by single spaces", true},
}

func TestScheduleValidation(t *testing.T) {
	needsEnvtest(t)
	for _, path := range [][]string{{"spec", "etcd", "defragmentation", "schedule"}, {"spec", "backup", "fullSnapshotSchedule"}} {
		for _, c := range schedules {
			t.Run(strings.Join(path[1:], ".")+"/"+c.schedule, func(t *testing.T) {
				obj := example(t, "datastoreclass.yaml", "schedule")
				set(t, obj, c.schedule, path...)
				expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
			})
		}
	}
}

func TestTheBackupSidecarParsesEveryAcceptedSchedule(t *testing.T) {
	for _, c := range schedules {
		_, err := cron.ParseStandard(c.schedule)
		if (err == nil) != c.parses || c.want == "" && err != nil {
			t.Errorf("robfig/cron parses %q: %v, the table says %t", c.schedule, err, c.parses)
		}
	}
}
