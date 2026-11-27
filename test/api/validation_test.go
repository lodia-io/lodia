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
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll, client.FieldValidation("Strict")), "")
		})
	}
}

func TestControlPlaneDefaults(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	obj := example(t, "controlplane-single.yaml", "defaults")
	expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), "")
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
		{"a minor with a leading zero", func(t *testing.T, o *unstructured.Unstructured) { set(t, o, "v1.037.1", "spec", "version") }, "spec.version"},
		{"a patch with a leading zero", func(t *testing.T, o *unstructured.Unstructured) { set(t, o, "v1.37.01", "spec", "version") }, "spec.version"},
		{"an empty datastore", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{}, "spec", "datastore")
		}, ""},
		{"a class name of 253 characters", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, strings.Repeat("c", 253), "spec", "datastore", "className")
		}, ""},
		{"a class name of 254 characters", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, strings.Repeat("c", 254), "spec", "datastore", "className")
		}, "spec.datastore.className: Too long"},
		{"two endpoints", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"loadBalancer": map[string]any{}, "nodePort": map[string]any{}}, "spec", "endpoint")
		}, "set exactly one of gateway, loadBalancer and nodePort"},
		{"bad CIDR", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{"10.0.0.0/33"}, "spec", "network", "serviceCIDRs")
		}, "must be a network in CIDR notation"},
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
		{"an encryption provider that is not built", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "kms", "spec", "apiServer", "encryption", "provider")
		}, "spec.apiServer.encryption.provider: Unsupported value"},
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
	const (
		sets     = "Lodia sets this flag"
		defaults = "Lodia relies on this flag's default"
		conflict = "this flag conflicts with one Lodia sets"
		exits    = "this flag makes the component exit, or report another version than the one it runs"
		file     = "this flag names a file"
		peer     = "the mixed-version proxy would reach every replica of the API server at the one address Lodia advertises for all of them"
	)
	for component, flags := range map[string]map[string]string{
		"apiServer": {
			"goaway-chance": sets, "service-account-jwks-uri": sets, "authentication-config": sets, "authorization-mode": sets,
			"etcd-prefix": defaults, "etcd-servers-overrides": defaults, "shutdown-send-retry-after": defaults, "bind-address": defaults,
			"request-timeout": defaults, "emulated-version": defaults, "emulation-forward-compatible": defaults,
			"min-compatibility-version": defaults, "runtime-config-emulation-forward-compatible": defaults,
			"oidc-issuer-url": conflict, "oidc-client-id": conflict, "oidc-ca-file": conflict, "oidc-username-claim": conflict,
			"oidc-username-prefix": conflict, "oidc-groups-claim": conflict, "oidc-groups-prefix": conflict, "oidc-signing-algs": conflict,
			"oidc-required-claim": conflict, "anonymous-auth": conflict, "authorization-config": conflict, "authorization-policy-file": conflict,
			"authorization-webhook-config-file": conflict, "authorization-webhook-version": conflict,
			"authorization-webhook-cache-authorized-ttl": conflict, "authorization-webhook-cache-unauthorized-ttl": conflict,
			"admission-control": conflict, "cert-dir": conflict, "service-account-key-file": conflict, "service-account-signing-key-file": conflict,
			"apiserver-count": conflict, "show-hidden-metrics-for-version": "",
			"admission-control-config-file": file, "allow-metric-labels-manifest": file, "kubelet-certificate-authority": file,
			"tls-sni-cert-key": file, "token-auth-file": file, "tracing-config-file": file,
			"audit-policy-file": file, "audit-log-path": file, "audit-webhook-config-file": file, "audit-log-maxage": file,
			"authentication-token-webhook-config-file": file, "authentication-token-webhook-cache-ttl": file, "authentication-token-webhook-version": file,
			"peer-ca-file": peer, "peer-advertise-ip": peer, "peer-advertise-port": peer,
			"help": exits, "version": exits,
			"max-requests-inflight": "", "disable-admission-plugins": "", "tls-cipher-suites": "",
		},
		"controllerManager": {
			"leader-elect-lease-duration": sets, "leader-elect-renew-deadline": sets, "leader-elect-retry-period": sets,
			"leader-elect-resource-lock": sets, "leader-elect-resource-name": sets, "leader-elect-resource-namespace": sets,
			"node-monitor-grace-period": sets, "node-monitor-period": sets, "cluster-signing-kubelet-client-cert-file": sets,
			"secure-port": defaults, "cluster-signing-cert-file": conflict, "cluster-signing-key-file": conflict, "master": conflict,
			"service-account-private-key-file": conflict, "help": exits, "version": exits, "authorization-always-allow-paths": defaults,
			"cidr-allocator-type": defaults, "emulated-version": defaults, "min-compatibility-version": defaults,
			"allow-metric-labels-manifest": file, "cloud-config": file,
			"cluster-signing-kube-apiserver-client-cert-file": file, "cluster-signing-kube-apiserver-client-key-file": file,
			"cluster-signing-kubelet-serving-cert-file": file, "cluster-signing-kubelet-serving-key-file": file,
			"cluster-signing-legacy-unknown-cert-file": file, "cluster-signing-legacy-unknown-key-file": file,
			"leader-migration-config": file, "pv-recycler-pod-template-filepath-hostpath": file, "pv-recycler-pod-template-filepath-nfs": file,
			"tls-cert-file": file, "tls-private-key-file": file, "tls-sni-cert-key": file,
			"terminated-pod-gc-threshold": "", "authentication-token-webhook-cache-ttl": "", "cert-dir": "",
		},
		"scheduler": {
			"leader-elect-resource-lock": sets, "leader-elect-resource-name": sets, "leader-elect-resource-namespace": sets, "kubeconfig": sets,
			"secure-port": defaults, "authorization-always-allow-paths": defaults, "config": conflict, "master": conflict,
			"emulated-version": defaults, "min-compatibility-version": defaults,
			"allow-metric-labels-manifest": file, "client-ca-file": file, "requestheader-client-ca-file": file,
			"tls-cert-file": file, "tls-private-key-file": file, "tls-sni-cert-key": file,
			"help": exits, "version": exits, "write-config-to": exits,
			"leader-elect-lease-duration": "", "authentication-token-webhook-cache-ttl": "",
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

func TestExtraArgsRefuseWhatConflictsWithTheirValues(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const (
		disabled = "disable-admission-plugins cannot name NodeRestriction, which Lodia enables"
		dual     = "kube-controller-manager refuses node-cidr-mask-size with two pod ranges"
		ipv4     = "kube-controller-manager refuses node-cidr-mask-size-ipv4 with an IPv6 pod range alone"
		ipv6     = "kube-controller-manager refuses node-cidr-mask-size-ipv6 with an IPv4 pod range alone"
		signer   = "kube-apiserver refuses service-account-signing-endpoint, which Lodia sets, without ExternalServiceAccountTokenSigner"
		tokens   = "kube-apiserver refuses a service-account-max-token-expiration other than 0 under 1h, and one above 8760h"
		narrow   = "kube-controller-manager refuses a pod range narrower than the node mask of its family"
		plugins  = "disable-admission-plugins is plugin names of letters and digits joined by commas, and nothing else"
		lifetime = "service-account-max-token-expiration is a duration as Go prints it, without its zero units"
		masks    = "a node mask size is a positive decimal number without a sign or a leading zero"
		grammar  = "event-ttl is a duration as Go's time.ParseDuration reads it"
		floor    = "event-ttl is at least 1h, kube-apiserver's default"
		bound4   = "node-cidr-mask-size-ipv4 is at most 32"
		bound6   = "a node mask size is at most 128"
		single4  = "node-cidr-mask-size beside an IPv4 pod range is at most 32"
		pairs    = "runtime-config is pairs of an API or api/all, api/ga, api/beta or api/alpha and true or false"
		relied   = "runtime-config cannot turn off api/all, api/ga, or a group version Lodia's controllers use or a resource of one"
		named    = "controllers is names of controllers"
		joining  = "controllers cannot turn off a controller nodes join by"
		ready    = "runtime-config cannot turn off a group version kube-apiserver needs to become ready, or a resource of one"
		leader   = "CoordinatedLeaderElection on the scheduler needs it on kube-apiserver too"
		elects   = "controllerManager.featureGates sets CoordinatedLeaderElection, which apiServer.featureGates must set to the same value"
		served   = "CoordinatedLeaderElection on kube-apiserver needs apiServer.extraArgs' runtime-config to hold coordination.k8s.io/v1beta1=true"
	)
	args := func(component string, flags map[string]any) func(*testing.T, *unstructured.Unstructured) {
		return func(t *testing.T, o *unstructured.Unstructured) { set(t, o, flags, "spec", component, "extraArgs") }
	}
	gates := func(component string, gates map[string]any) func(*testing.T, *unstructured.Unstructured) {
		return func(t *testing.T, o *unstructured.Unstructured) { set(t, o, gates, "spec", component, "featureGates") }
	}
	beta := args("apiServer", map[string]any{"runtime-config": "coordination.k8s.io/v1beta1=true"})
	elected := gates("apiServer", map[string]any{"CoordinatedLeaderElection": true})
	pods := func(cidrs ...any) func(*testing.T, *unstructured.Unstructured) {
		return func(t *testing.T, o *unstructured.Unstructured) { set(t, o, cidrs, "spec", "network", "podCIDRs") }
	}
	for _, c := range []struct {
		name   string
		mutate []func(*testing.T, *unstructured.Unstructured)
		want   string
	}{
		{"NodeRestriction disabled", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "NodeRestriction"})}, disabled},
		{"NodeRestriction disabled among others", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "PodSecurity,NodeRestriction"})}, disabled},
		{"NodeRestriction disabled first among others", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "NodeRestriction,PodSecurity"})}, disabled},
		{"NodeRestriction disabled between others", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "PodSecurity,NodeRestriction,DefaultStorageClass"})}, disabled},
		{"NodeRestriction disabled quoted", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": `"NodeRestriction"`})}, plugins},
		{"NodeRestriction disabled quoted among others", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": `PodSecurity,"NodeRestriction",DefaultStorageClass`})}, plugins},
		{"NodeRestriction disabled before a line break", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "NodeRestriction\n"})}, plugins},
		{"NodeRestriction disabled after a line break", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "\nNodeRestriction"})}, plugins},
		{"NodeRestriction disabled before a carriage return", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "NodeRestriction\r"})}, plugins},
		{"NodeRestriction disabled before a line break and another plugin", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "NodeRestriction\nPodSecurity"})}, plugins},
		{"NodeRestriction disabled quoted before a line break", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "\"NodeRestriction\"\n"})}, plugins},
		{"plugins disabled with a space", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "PodSecurity, DefaultStorageClass"})}, plugins},
		{"plugins disabled with an empty entry", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "PodSecurity,,DefaultStorageClass"})}, plugins},
		{"plugins disabled with a trailing comma", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "PodSecurity,"})}, plugins},
		{"no plugin disabled", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": ""})}, plugins},
		{"another plugin disabled", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "PodSecurity,DefaultStorageClass"})}, ""},
		{"one other plugin disabled", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"disable-admission-plugins": "PodSecurity"})}, ""},
		{"a node mask size with one IPv4 range", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size": "26"})}, ""},
		{"a node mask size with a leading zero", []func(*testing.T, *unstructured.Unstructured){pods("10.244.0.0/22"), args("controllerManager", map[string]any{"node-cidr-mask-size": "024"})}, masks},
		{"an IPv4 node mask size with a leading zero", []func(*testing.T, *unstructured.Unstructured){pods("10.244.0.0/22"), args("controllerManager", map[string]any{"node-cidr-mask-size-ipv4": "024"})}, masks},
		{"an IPv6 node mask size with a leading zero", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/56"), args("controllerManager", map[string]any{"node-cidr-mask-size-ipv6": "064"})}, masks},
		{"a node mask size of 0 beside an IPv6 range of everything", []func(*testing.T, *unstructured.Unstructured){pods("::/0"), args("controllerManager", map[string]any{"node-cidr-mask-size-ipv6": "0"})}, masks},
		{"a node mask size of 0", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size": "0"})}, masks},
		{"a node mask size in hexadecimal", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size": "0x18"})}, masks},
		{"a node mask size with a sign", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size": "+24"})}, masks},
		{"a negative node mask size", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size": "-24"})}, masks},
		{"a node mask size with a space", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size": " 24"})}, masks},
		{"a node mask size with two ranges", []func(*testing.T, *unstructured.Unstructured){pods("10.244.0.0/16", "fd00:10:244::/56"), args("controllerManager", map[string]any{"node-cidr-mask-size": "26"})}, dual},
		{"both node mask sizes with two ranges", []func(*testing.T, *unstructured.Unstructured){pods("10.244.0.0/16", "fd00:10:244::/56"), args("controllerManager", map[string]any{"node-cidr-mask-size-ipv4": "26", "node-cidr-mask-size-ipv6": "64"})}, ""},
		{"an IPv4 node mask size with two ranges, the IPv6 one first", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/56", "10.244.0.0/16"), args("controllerManager", map[string]any{"node-cidr-mask-size-ipv4": "26"})}, ""},
		{"an IPv4 node mask size with an IPv4 range", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size-ipv4": "26"})}, ""},
		{"an IPv4 node mask size with an IPv6 range", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/56"), args("controllerManager", map[string]any{"node-cidr-mask-size-ipv4": "26"})}, ipv4},
		{"an IPv6 node mask size with an IPv6 range", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/56"), args("controllerManager", map[string]any{"node-cidr-mask-size-ipv6": "64"})}, ""},
		{"an IPv6 node mask size with an IPv4 range", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size-ipv6": "64"})}, ipv6},
		{"a pod range of the default IPv4 node mask", []func(*testing.T, *unstructured.Unstructured){pods("10.244.0.0/24")}, ""},
		{"a pod range narrower than the default IPv4 node mask", []func(*testing.T, *unstructured.Unstructured){pods("10.244.0.0/25")}, narrow},
		{"a pod range of a node mask size", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size": "16"})}, ""},
		{"a pod range narrower than a node mask size", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size": "12"})}, narrow},
		{"a pod range narrower than an IPv4 node mask size", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size-ipv4": "15"})}, narrow},
		{"an IPv6 pod range of the default node mask", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/64")}, ""},
		{"an IPv6 pod range narrower than the default node mask", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/80")}, narrow},
		{"an IPv6 pod range one bit narrower than the default node mask", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/65")}, narrow},
		{"an IPv6 pod range of an IPv6 node mask size", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/96"), args("controllerManager", map[string]any{"node-cidr-mask-size-ipv6": "112"})}, ""},
		{"an IPv6 pod range narrower than an IPv6 node mask size", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/60"), args("controllerManager", map[string]any{"node-cidr-mask-size-ipv6": "56"})}, narrow},
		{"an IPv6 pod range 16 bits wider than a node mask size", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10::/40"), args("controllerManager", map[string]any{"node-cidr-mask-size": "56"})}, ""},
		{"an IPv6 pod range 17 bits wider than a node mask size", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/64"), args("controllerManager", map[string]any{"node-cidr-mask-size": "81"})}, narrow},
		{"an IPv6 pod range narrower than a node mask size", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/60"), args("controllerManager", map[string]any{"node-cidr-mask-size": "56"})}, narrow},
		{"an IPv6 pod range 16 bits wider than the default node mask", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10::/48")}, ""},
		{"an IPv6 pod range 17 bits wider than the default node mask", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10::/47")}, narrow},
		{"an IPv6 pod range 17 bits wider than an IPv6 node mask size", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/56"), args("controllerManager", map[string]any{"node-cidr-mask-size-ipv6": "73"})}, narrow},
		{"the IPv6 range of two narrower than its node mask", []func(*testing.T, *unstructured.Unstructured){pods("10.244.0.0/16", "fd00:10:244::/96")}, narrow},
		{"tokens of the signer's longest life", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "8760h"})}, ""},
		{"tokens of an hour", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "1h"})}, ""},
		{"tokens of more than the signer's longest life", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "8760h1s"})}, tokens},
		{"tokens of an hour and a half", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "1h30m"})}, ""},
		{"tokens of less than an hour", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "59m59s"})}, tokens},
		{"tokens with hours, minutes and seconds", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "2h1m1s"})}, ""},
		{"tokens with hours and seconds", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "1h30s"})}, ""},
		{"tokens with sixty seconds", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "1h60s"})}, lifetime},
		{"tokens of half an hour", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "30m"})}, tokens},
		{"tokens of a negative life", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "-1h"})}, lifetime},
		{"tokens of an hour and a half in minutes", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "90m"})}, lifetime},
		{"tokens of an hour in seconds", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "3600s"})}, lifetime},
		{"tokens of an hour and a half as a fraction", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "1.5h"})}, lifetime},
		{"tokens of an hour with a zero unit", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "1h0m"})}, lifetime},
		{"tokens of the signer's longest life as Go prints it", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "8760h0m0s"})}, lifetime},
		{"tokens of an hour with a leading zero", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "01h"})}, lifetime},
		{"tokens of an hour and sixty minutes", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": "1h60m"})}, lifetime},
		{"tokens of an empty life", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"service-account-max-token-expiration": ""})}, lifetime},
		{"event-ttl of an hour", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "1h"})}, ""},
		{"event-ttl of an hour as Go prints it", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "1h0m0s"})}, ""},
		{"event-ttl of an hour in minutes", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "60m"})}, ""},
		{"event-ttl of an hour in seconds", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "3600s"})}, ""},
		{"event-ttl of an hour in milliseconds", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "3600000ms"})}, ""},
		{"event-ttl of an hour in microseconds", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "3600000000us"})}, ""},
		{"event-ttl of an hour in microseconds with a micro sign", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "3600000000µs"})}, ""},
		{"event-ttl of an hour in microseconds with a Greek mu", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "3600000000μs"})}, ""},
		{"event-ttl of an hour in nanoseconds", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "3600000000000ns"})}, ""},
		{"event-ttl of an hour with a sign", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "+1h"})}, ""},
		{"event-ttl of an hour with a leading zero", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "01h"})}, ""},
		{"event-ttl of an hour in two halves", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "30m30m"})}, ""},
		{"event-ttl of two and a half hours as a fraction", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "2.5h"})}, ""},
		{"event-ttl of a week", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "168h"})}, ""},
		{"event-ttl of an hour with a point and no fraction", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "1.h"})}, ""},
		{"event-ttl of an hour and a quarter with a leading point", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": ".5h45m"})}, ""},
		{"event-ttl of an hour in minutes after zero hours", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "0h60m"})}, ""},
		{"event-ttl of a second short of an hour", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "59m59s"})}, floor},
		{"event-ttl of a nanosecond short of an hour", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "3599999999999ns"})}, floor},
		{"event-ttl of half an hour", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "30m"})}, floor},
		{"event-ttl of half an hour with a leading point", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": ".5h"})}, floor},
		{"event-ttl of zero", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "0"})}, floor},
		{"event-ttl of zero with a unit", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "0h"})}, floor},
		{"event-ttl of zero with a sign", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "-0"})}, floor},
		{"event-ttl of a negative hour", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "-1h"})}, floor},
		{"event-ttl of more than a Go duration holds", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "2562048h"})}, floor},
		{"event-ttl of a day", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "1d"})}, grammar},
		{"event-ttl of an empty duration", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": ""})}, grammar},
		{"event-ttl of an hour and a half with a space", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "1h 30m"})}, grammar},
		{"event-ttl of an hour in capitals", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "1H"})}, grammar},
		{"event-ttl of an hour without a unit", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "3600"})}, grammar},
		{"event-ttl of an hour with a last number without a unit", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "1h30"})}, grammar},
		{"event-ttl of an hour with a doubled point", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "1.5.5h"})}, grammar},
		{"event-ttl of an hour in scientific notation", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "3.6e3s"})}, grammar},
		{"event-ttl of an hour with an unknown unit", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "1hr"})}, grammar},
		{"event-ttl of a unit alone", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "h"})}, grammar},
		{"event-ttl of a zero with a point", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "0.0"})}, grammar},
		{"event-ttl of a double zero", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "00"})}, grammar},
		{"event-ttl of a quoted hour", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"event-ttl": "\"1h\""})}, grammar},
		{"the external signer's gate off", []func(*testing.T, *unstructured.Unstructured){func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"ExternalServiceAccountTokenSigner": false}, "spec", "apiServer", "featureGates")
		}}, signer},
		{"the external signer's gate on", []func(*testing.T, *unstructured.Unstructured){func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"ExternalServiceAccountTokenSigner": true}, "spec", "apiServer", "featureGates")
		}}, ""},
		{"an API server value of 4096 characters", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"max-requests-inflight": strings.Repeat("8", 4096)})}, ""},
		{"an API server value of 4097 characters", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"max-requests-inflight": strings.Repeat("8", 4097)})}, "spec.apiServer.extraArgs.max-requests-inflight: Too long"},
		{"a controller manager value of 4096 characters", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"terminated-pod-gc-threshold": strings.Repeat("8", 4096)})}, ""},
		{"a controller manager value of 4097 characters", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"terminated-pod-gc-threshold": strings.Repeat("8", 4097)})}, "spec.controllerManager.extraArgs.terminated-pod-gc-threshold: Too long"},
		{"a scheduler value of 4096 characters", []func(*testing.T, *unstructured.Unstructured){args("scheduler", map[string]any{"leader-elect-lease-duration": strings.Repeat("8", 4096)})}, ""},
		{"a scheduler value of 4097 characters", []func(*testing.T, *unstructured.Unstructured){args("scheduler", map[string]any{"leader-elect-lease-duration": strings.Repeat("8", 4097)})}, "spec.scheduler.extraArgs.leader-elect-lease-duration: Too long"},
		{"an IPv4 node mask of 32", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size-ipv4": "32"})}, ""},
		{"an IPv4 node mask of 33", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size-ipv4": "33"})}, bound4},
		{"an IPv6 node mask of 128", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/113"), args("controllerManager", map[string]any{"node-cidr-mask-size-ipv6": "128"})}, ""},
		{"an IPv6 node mask of 129", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/113"), args("controllerManager", map[string]any{"node-cidr-mask-size-ipv6": "129"})}, bound6},
		{"a node mask of 32 beside an IPv4 range", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size": "32"})}, ""},
		{"a node mask of 33 beside an IPv4 range", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"node-cidr-mask-size": "33"})}, single4},
		{"a node mask of 128 beside an IPv6 range", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/113"), args("controllerManager", map[string]any{"node-cidr-mask-size": "128"})}, ""},
		{"a node mask of 129 beside an IPv6 range", []func(*testing.T, *unstructured.Unstructured){pods("fd00:10:244::/113"), args("controllerManager", map[string]any{"node-cidr-mask-size": "129"})}, bound6},
		{"controllers turning off the TTL controller by its alias", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-ttl"})}, ""},
		{"controllers turning off two others", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-ttl-controller,-cronjob-controller"})}, ""},
		{"controllers turning on every default one", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "*"})}, ""},
		{"controllers turning on the bootstrap signer by its alias", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "bootstrapsigner"})}, ""},
		{"controllers ending in a comma", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-ttl,"})}, named},
		{"controllers with a space", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-ttl, -cronjob-controller"})}, named},
		{"controllers quoted", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": `"-csrsigning"`})}, named},
		{"controllers with two dashes", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "--ttl"})}, named},
		{"controllers in capitals", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-TTL"})}, named},
		{"no controllers", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": ""})}, named},
		{"controllers turning off the signer after another", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-ttl,-csrsigning"})}, joining},
		{"controllers turning off the signer before another", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-certificatesigningrequest-signing-controller,-ttl"})}, joining},
		{"controllers turning off certificatesigningrequest-signing-controller", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-certificatesigningrequest-signing-controller"})}, joining},
		{"controllers turning off csrsigning", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-csrsigning"})}, joining},
		{"controllers turning off certificatesigningrequest-approving-controller", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-certificatesigningrequest-approving-controller"})}, joining},
		{"controllers turning off csrapproving", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-csrapproving"})}, joining},
		{"controllers turning off bootstrap-signer-controller", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-bootstrap-signer-controller"})}, joining},
		{"controllers turning off bootstrapsigner", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-bootstrapsigner"})}, joining},
		{"controllers turning off token-cleaner-controller", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-token-cleaner-controller"})}, joining},
		{"controllers turning off tokencleaner", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-tokencleaner"})}, joining},
		{"controllers turning off root-ca-certificate-publisher-controller", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-root-ca-certificate-publisher-controller"})}, joining},
		{"controllers turning off root-ca-cert-publisher", []func(*testing.T, *unstructured.Unstructured){args("controllerManager", map[string]any{"controllers": "-root-ca-cert-publisher"})}, joining},
		{"runtime-config apps/v1=true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1=true"})}, ""},
		{"runtime-config batch/v1=false,api/beta=true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "batch/v1=false,api/beta=true"})}, ""},
		{"runtime-config api/alpha=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "api/alpha=false"})}, ""},
		{"runtime-config storage.k8s.io/v1beta1=true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "storage.k8s.io/v1beta1=true"})}, ""},
		{"runtime-config batch/v1/jobs=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "batch/v1/jobs=false"})}, ""},
		{"runtime-config apps/v1beta1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1beta1=false"})}, ""},
		{"runtime-config api/legacy=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "api/legacy=false"})}, ""},
		{"runtime-config apps/v1/deployments=true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1/deployments=true"})}, ""},
		{"runtime-config apps/v1", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1"})}, pairs},
		{"runtime-config apps/v1=", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1="})}, pairs},
		{"runtime-config apps/v1=1", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1=1"})}, pairs},
		{"runtime-config apps/v1=t", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1=t"})}, pairs},
		{"runtime-config apps/v1= true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1= true"})}, pairs},
		{"runtime-config apps/v1=true, batch/v1=true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1=true, batch/v1=true"})}, pairs},
		{"runtime-config apps/v1 =true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1 =true"})}, pairs},
		{"runtime-config apps/v1=true,", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1=true,"})}, pairs},
		{"runtime-config ,apps/v1=true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": ",apps/v1=true"})}, pairs},
		{"runtime-config Apps/v1=true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "Apps/v1=true"})}, pairs},
		{"runtime-config apps/v1=True", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1=True"})}, pairs},
		{"runtime-config apps/v1=true,,batch/v1=true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1=true,,batch/v1=true"})}, pairs},
		{"runtime-config empty", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": ""})}, pairs},
		{"runtime-config api/all=FALSE", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "api/all=FALSE"})}, pairs},
		{"runtime-config with a line break", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1=true\nbatch/v1=false"})}, pairs},
		{"runtime-config api/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "api/v1=false"})}, relied},
		{"runtime-config v1/pods=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "v1/pods=false"})}, relied},
		{"runtime-config api/v1/pods=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "api/v1/pods=false"})}, relied},
		{"runtime-config apps/v1/deployments=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1/deployments=false"})}, relied},
		{"runtime-config batch/v1=true,apps/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "batch/v1=true,apps/v1=false"})}, relied},
		{"runtime-config api/all=false,v1=true,apps/v1=true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "api/all=false,v1=true,apps/v1=true"})}, relied},
		{"runtime-config v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "v1=false"})}, relied},
		{"runtime-config v1=0", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "v1=0"})}, pairs},
		{"runtime-config v1=f", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "v1=f"})}, pairs},
		{"runtime-config apps/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1=false"})}, relied},
		{"runtime-config apps/v1=0", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1=0"})}, pairs},
		{"runtime-config apps/v1=f", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apps/v1=f"})}, pairs},
		{"runtime-config rbac.authorization.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "rbac.authorization.k8s.io/v1=false"})}, relied},
		{"runtime-config rbac.authorization.k8s.io/v1=0", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "rbac.authorization.k8s.io/v1=0"})}, pairs},
		{"runtime-config rbac.authorization.k8s.io/v1=f", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "rbac.authorization.k8s.io/v1=f"})}, pairs},
		{"runtime-config discovery.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "discovery.k8s.io/v1=false"})}, relied},
		{"runtime-config discovery.k8s.io/v1=0", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "discovery.k8s.io/v1=0"})}, pairs},
		{"runtime-config discovery.k8s.io/v1=f", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "discovery.k8s.io/v1=f"})}, pairs},
		{"runtime-config coordination.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "coordination.k8s.io/v1=false"})}, relied},
		{"runtime-config coordination.k8s.io/v1=0", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "coordination.k8s.io/v1=0"})}, pairs},
		{"runtime-config coordination.k8s.io/v1=f", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "coordination.k8s.io/v1=f"})}, pairs},
		{"runtime-config certificates.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "certificates.k8s.io/v1=false"})}, relied},
		{"runtime-config certificates.k8s.io/v1=0", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "certificates.k8s.io/v1=0"})}, pairs},
		{"runtime-config certificates.k8s.io/v1=f", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "certificates.k8s.io/v1=f"})}, pairs},
		{"runtime-config authentication.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "authentication.k8s.io/v1=false"})}, relied},
		{"runtime-config authentication.k8s.io/v1=0", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "authentication.k8s.io/v1=0"})}, pairs},
		{"runtime-config authentication.k8s.io/v1=f", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "authentication.k8s.io/v1=f"})}, pairs},
		{"runtime-config authorization.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "authorization.k8s.io/v1=false"})}, relied},
		{"runtime-config authorization.k8s.io/v1=0", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "authorization.k8s.io/v1=0"})}, pairs},
		{"runtime-config authorization.k8s.io/v1=f", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "authorization.k8s.io/v1=f"})}, pairs},
		{"runtime-config apiregistration.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apiregistration.k8s.io/v1=false"})}, relied},
		{"runtime-config apiregistration.k8s.io/v1=0", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apiregistration.k8s.io/v1=0"})}, pairs},
		{"runtime-config apiregistration.k8s.io/v1=f", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apiregistration.k8s.io/v1=f"})}, pairs},
		{"runtime-config api/all=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "api/all=false"})}, relied},
		{"runtime-config api/all=0", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "api/all=0"})}, pairs},
		{"runtime-config api/all=f", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "api/all=f"})}, pairs},
		{"runtime-config api/ga=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "api/ga=false"})}, relied},
		{"runtime-config api/ga=0", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "api/ga=0"})}, pairs},
		{"runtime-config api/ga=f", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "api/ga=f"})}, pairs},
		{"runtime-config admissionregistration.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "admissionregistration.k8s.io/v1=false"})}, ready},
		{"runtime-config apiextensions.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "apiextensions.k8s.io/v1=false"})}, ready},
		{"runtime-config networking.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "networking.k8s.io/v1=false"})}, ready},
		{"runtime-config node.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "node.k8s.io/v1=false"})}, ready},
		{"runtime-config resource.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "resource.k8s.io/v1=false"})}, ready},
		{"runtime-config scheduling.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "scheduling.k8s.io/v1=false"})}, ready},
		{"runtime-config storage.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "storage.k8s.io/v1=false"})}, ready},
		{"runtime-config storage.k8s.io/v1/csistoragecapacities=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "storage.k8s.io/v1/csistoragecapacities=false"})}, ready},
		{"runtime-config batch/v1=true,node.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "batch/v1=true,node.k8s.io/v1=false"})}, ready},
		{"runtime-config node.k8s.io/v1=false,batch/v1=true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "node.k8s.io/v1=false,batch/v1=true"})}, ready},
		{"runtime-config of a group whose name ends in node.k8s.io", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "examplenode.k8s.io/v1=false"})}, ""},
		{"runtime-config networking.k8s.io/v1beta1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "networking.k8s.io/v1beta1=false"})}, ""},
		{"runtime-config scheduling.k8s.io/v1=true", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "scheduling.k8s.io/v1=true"})}, ""},
		{"runtime-config events.k8s.io/v1=false", []func(*testing.T, *unstructured.Unstructured){args("apiServer", map[string]any{"runtime-config": "events.k8s.io/v1=false"})}, ""},
		{"coordinated election on the controller manager alone", []func(*testing.T, *unstructured.Unstructured){gates("controllerManager", map[string]any{"CoordinatedLeaderElection": true})}, elects},
		{"coordinated election on the scheduler alone", []func(*testing.T, *unstructured.Unstructured){gates("scheduler", map[string]any{"CoordinatedLeaderElection": true})}, leader},
		{"coordinated election off on the controller manager alone", []func(*testing.T, *unstructured.Unstructured){gates("controllerManager", map[string]any{"CoordinatedLeaderElection": false})}, elects},
		{"coordinated election off on the controller manager and kube-apiserver", []func(*testing.T, *unstructured.Unstructured){gates("controllerManager", map[string]any{"CoordinatedLeaderElection": false}), gates("apiServer", map[string]any{"CoordinatedLeaderElection": false})}, ""},
		{"coordinated election off on the scheduler", []func(*testing.T, *unstructured.Unstructured){gates("scheduler", map[string]any{"CoordinatedLeaderElection": false})}, ""},
		{"coordinated election on the controller manager and kube-apiserver", []func(*testing.T, *unstructured.Unstructured){gates("controllerManager", map[string]any{"CoordinatedLeaderElection": true}), elected, beta}, ""},
		{"coordinated election on the controller manager and kube-apiserver without runtime-config", []func(*testing.T, *unstructured.Unstructured){gates("controllerManager", map[string]any{"CoordinatedLeaderElection": true}), elected}, served},
		{"coordinated election on the scheduler and kube-apiserver", []func(*testing.T, *unstructured.Unstructured){gates("scheduler", map[string]any{"CoordinatedLeaderElection": true}), elected, beta}, ""},
		{"coordinated election on the controller manager beside it off on kube-apiserver", []func(*testing.T, *unstructured.Unstructured){gates("controllerManager", map[string]any{"CoordinatedLeaderElection": true}), gates("apiServer", map[string]any{"CoordinatedLeaderElection": false})}, elects},
		{"coordinated election on the scheduler beside it off on kube-apiserver", []func(*testing.T, *unstructured.Unstructured){gates("scheduler", map[string]any{"CoordinatedLeaderElection": true}), gates("apiServer", map[string]any{"CoordinatedLeaderElection": false})}, leader},
		{"coordinated election on kube-apiserver alone", []func(*testing.T, *unstructured.Unstructured){elected, beta}, ""},
		{"coordinated election on kube-apiserver without runtime-config", []func(*testing.T, *unstructured.Unstructured){elected}, served},
		{"coordinated election on kube-apiserver with runtime-config of other groups", []func(*testing.T, *unstructured.Unstructured){elected, args("apiServer", map[string]any{"runtime-config": "batch/v1=true"})}, served},
		{"coordinated election on kube-apiserver with api/beta=true", []func(*testing.T, *unstructured.Unstructured){elected, args("apiServer", map[string]any{"runtime-config": "api/beta=true"})}, served},
		{"coordinated election on kube-apiserver with its group version off", []func(*testing.T, *unstructured.Unstructured){elected, args("apiServer", map[string]any{"runtime-config": "coordination.k8s.io/v1beta1=false"})}, served},
		{"coordinated election on kube-apiserver with its group version on, then off", []func(*testing.T, *unstructured.Unstructured){elected, args("apiServer", map[string]any{"runtime-config": "coordination.k8s.io/v1beta1=true,coordination.k8s.io/v1beta1=false"})}, served},
		{"coordinated election on kube-apiserver with its LeaseCandidates off", []func(*testing.T, *unstructured.Unstructured){elected, args("apiServer", map[string]any{"runtime-config": "coordination.k8s.io/v1beta1=true,coordination.k8s.io/v1beta1/leasecandidates=false"})}, served},
		{"coordinated election on kube-apiserver with its group version among others", []func(*testing.T, *unstructured.Unstructured){elected, args("apiServer", map[string]any{"runtime-config": "batch/v1=true,coordination.k8s.io/v1beta1=true,api/alpha=false"})}, ""},
		{"coordinated election off on kube-apiserver without runtime-config", []func(*testing.T, *unstructured.Unstructured){gates("apiServer", map[string]any{"CoordinatedLeaderElection": false})}, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-single.yaml", "values")
			for _, mutate := range c.mutate {
				mutate(t, obj)
			}
			err := k8s.Create(context.Background(), obj, client.DryRunAll)
			expect(t, err, c.want)
			if c.want == floor && strings.Contains(err.Error(), grammar) {
				t.Fatalf("the format refused a value the floor refuses: %v", err)
			}
		})
	}
}

// TestFeatureGatesRefuseAllAlphaAndAllBetaAndAdmitAGateByName refuses AllAlpha and AllBeta, true or false, on each component, and admits a gate set by name, the controller manager's with the same on kube-apiserver.
func TestFeatureGatesRefuseAllAlphaAndAllBetaAndAdmitAGateByName(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const stages = "AllAlpha and AllBeta are refused, true or false"
	for component, named := range map[string][]string{
		"apiServer":         {"PodCertificateRequest", "MutatingAdmissionPolicy"},
		"controllerManager": {"StorageVersionMigrator"},
		"scheduler":         {"GenericWorkload", "DRADeviceTaintRules"},
	} {
		rows := map[string]string{"AllAlpha=true": stages, "AllAlpha=false": stages, "AllBeta=true": stages, "AllBeta=false": stages}
		for _, gate := range named {
			rows[gate+"=true"], rows[gate+"=false"] = "", ""
		}
		for row, want := range rows {
			t.Run(component+"/"+row, func(t *testing.T) {
				gate, value, _ := strings.Cut(row, "=")
				obj := example(t, "controlplane-single.yaml", "gates")
				set(t, obj, map[string]any{gate: value == "true"}, "spec", component, "featureGates")
				if component == "controllerManager" && want == "" {
					set(t, obj, map[string]any{gate: value == "true"}, "spec", "apiServer", "featureGates")
				}
				expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), want)
			})
		}
	}
}

// TestAControllerManagerGateHasTheSameValueOnKubeAPIServer refuses a gate of the controller manager that kube-apiserver lacks or sets otherwise, naming the first such gate by name, and admits it at the same value.
func TestAControllerManagerGateHasTheSameValueOnKubeAPIServer(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	sets := func(gate string) string {
		return "controllerManager.featureGates sets " + gate + ", which apiServer.featureGates must set to the same value"
	}
	on := func(component string, gates map[string]any) func(*testing.T, *unstructured.Unstructured) {
		return func(t *testing.T, o *unstructured.Unstructured) { set(t, o, gates, "spec", component, "featureGates") }
	}
	bare := func(t *testing.T, o *unstructured.Unstructured) {
		set(t, o, map[string]any{"max-requests-inflight": "700"}, "spec", "apiServer", "extraArgs")
	}
	agreeing := map[string]any{}
	for i := range 63 {
		agreeing[fmt.Sprintf("Gate%02d", i)] = i%2 == 0
	}
	with := func(gate string, value bool) map[string]any {
		out := maps.Clone(agreeing)
		out[gate] = value
		return out
	}
	otherwise := map[string]any{}
	for gate, value := range agreeing {
		otherwise[gate] = !value.(bool)
	}
	svm := func(value bool) map[string]any { return map[string]any{"StorageVersionMigrator": value} }
	workloads := map[string]any{"GenericWorkload": true, "WorkloadWithJob": true}
	for _, c := range []struct {
		name   string
		mutate []func(*testing.T, *unstructured.Unstructured)
		want   string
	}{
		{"on the controller manager alone", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", svm(true))}, sets("StorageVersionMigrator")},
		{"on the controller manager beside kube-apiserver without featureGates", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", svm(true)), bare}, sets("StorageVersionMigrator")},
		{"on the controller manager beside no gate on kube-apiserver", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", svm(true)), on("apiServer", map[string]any{})}, sets("StorageVersionMigrator")},
		{"on the controller manager beside another gate on kube-apiserver", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", svm(true)), on("apiServer", map[string]any{"PodCertificateRequest": true})}, sets("StorageVersionMigrator")},
		{"off on the controller manager alone", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", svm(false))}, sets("StorageVersionMigrator")},
		{"on both", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", svm(true)), on("apiServer", svm(true))}, ""},
		{"off on both", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", svm(false)), on("apiServer", svm(false))}, ""},
		{"on the controller manager and off on kube-apiserver", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", svm(true)), on("apiServer", svm(false))}, sets("StorageVersionMigrator")},
		{"off on the controller manager and on on kube-apiserver", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", svm(false)), on("apiServer", svm(true))}, sets("StorageVersionMigrator")},
		{"on kube-apiserver alone", []func(*testing.T, *unstructured.Unstructured){on("apiServer", svm(true))}, ""},
		{"none on the controller manager", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", map[string]any{})}, ""},
		{"GenericWorkload and WorkloadWithJob on the controller manager alone", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", workloads)}, sets("GenericWorkload")},
		{"GenericWorkload and WorkloadWithJob beside kube-apiserver without featureGates", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", workloads), bare}, sets("GenericWorkload")},
		{"GenericWorkload and WorkloadWithJob beside another gate on kube-apiserver", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", workloads), on("apiServer", map[string]any{"PodCertificateRequest": true})}, sets("GenericWorkload")},
		{"GenericWorkload and WorkloadWithJob beside GenericWorkload alone on kube-apiserver", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", workloads), on("apiServer", map[string]any{"GenericWorkload": true})}, sets("WorkloadWithJob")},
		{"GenericWorkload and WorkloadWithJob on both", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", workloads), on("apiServer", workloads)}, ""},
		{"one gate among many that agree", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", with("WorkloadWithJob", true)), on("apiServer", with("WorkloadWithJob", false))}, sets("WorkloadWithJob")},
		{"many that agree", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", agreeing), on("apiServer", agreeing)}, ""},
		{"many on the controller manager alone", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", agreeing)}, sets("Gate00")},
		{"many set otherwise on kube-apiserver", []func(*testing.T, *unstructured.Unstructured){on("controllerManager", agreeing), on("apiServer", otherwise)}, sets("Gate00")},
	} {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-single.yaml", "gates")
			for _, mutate := range c.mutate {
				mutate(t, obj)
			}
			for range 8 {
				expect(t, k8s.Create(context.Background(), obj.DeepCopy(), client.DryRunAll), c.want)
			}
		})
	}
}

// TestAFeatureGateIsKeyedByItsBareName refuses, on each component, a key that is not a gate's bare name, the prefixed names of the gates other rules name among them, and admits a bare name with digits.
func TestAFeatureGateIsKeyedByItsBareName(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	bare := func(component string) string {
		return "spec." + component + ".featureGates: Invalid value: a key must be a gate's bare name"
	}
	on := func(component string, gates map[string]any) func(*testing.T, *unstructured.Unstructured) {
		return func(t *testing.T, o *unstructured.Unstructured) { set(t, o, gates, "spec", component, "featureGates") }
	}
	type row struct {
		name   string
		mutate []func(*testing.T, *unstructured.Unstructured)
		want   string
	}
	var rows []row
	for _, component := range []string{"apiServer", "controllerManager", "scheduler"} {
		for _, key := range []string{"kube:AllBeta", " AllBeta", "AllBeta ", "wardle:Foo", "allBeta", "AllBeta=true,GenericWorkload", "GenericWorkload=true"} {
			rows = append(rows, row{component + " " + key, []func(*testing.T, *unstructured.Unstructured){on(component, map[string]any{key: true})}, bare(component)})
		}
		rows = append(rows, row{component + " a name with digits", []func(*testing.T, *unstructured.Unstructured){on(component, map[string]any{"UnauthenticatedHTTP2DOSMitigation": true}), on("apiServer", map[string]any{"UnauthenticatedHTTP2DOSMitigation": true})}, ""})
	}
	rows = append(rows,
		row{"kube:CoordinatedLeaderElection on the scheduler alone", []func(*testing.T, *unstructured.Unstructured){on("scheduler", map[string]any{"kube:CoordinatedLeaderElection": true})}, bare("scheduler")},
		row{"kube:CoordinatedLeaderElection on kube-apiserver without runtime-config", []func(*testing.T, *unstructured.Unstructured){on("apiServer", map[string]any{"kube:CoordinatedLeaderElection": true})}, bare("apiServer")},
		row{"kube:ExternalServiceAccountTokenSigner off", []func(*testing.T, *unstructured.Unstructured){on("apiServer", map[string]any{"kube:ExternalServiceAccountTokenSigner": false})}, bare("apiServer")},
	)
	for _, c := range rows {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-single.yaml", "gates")
			for _, mutate := range c.mutate {
				mutate(t, obj)
			}
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
		})
	}
}

func TestExtraArgsRefuseBindAddressInEverySpellingAndMode(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const spelling = "a key must be a flag's name as the component spells it"
	for mode, endpoint := range map[string]map[string]any{
		"gateway":      nil,
		"loadBalancer": {"loadBalancer": map[string]any{}},
		"nodePort":     {"nodePort": map[string]any{}},
	} {
		for component, flags := range map[string]struct{ admitted, bind string }{
			"apiServer":         {"max-requests-inflight", "Lodia relies on this flag's default"},
			"controllerManager": {"terminated-pod-gc-threshold", "Lodia sets this flag"},
			"scheduler":         {"leader-elect-lease-duration", "Lodia sets this flag"},
		} {
			for key, want := range map[string]string{
				"bind-address": flags.bind, "bind_address": spelling, "secure_port": spelling, "advertise_address": spelling,
				"-bind-address": spelling, "Bind-Address": spelling, "etcd-prefix=x": spelling, flags.admitted: "",
			} {
				t.Run(mode+"/"+component+"/"+key, func(t *testing.T) {
					obj := example(t, "controlplane-ha.yaml", "bind")
					if endpoint != nil {
						set(t, obj, endpoint, "spec", "endpoint")
					}
					set(t, obj, map[string]any{key: "127.0.0.1"}, "spec", component, "extraArgs")
					expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), want)
				})
			}
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
	set(t, stored, map[string]any{"node-monitor-grace-period": "40s", "node_monitor_period": "5s"}, "spec", "controllerManager", "extraArgs")
	storeUnderAnOlderSchema(t, "extraargs.ratcheting.lodia.io", stored, "Lodia sets this flag", func(schema map[string]any) {
		unstructured.RemoveNestedField(schema, "properties", "spec", "properties", "controllerManager", "properties", "extraArgs", "x-kubernetes-validations")
	})

	set(t, stored, true, "spec", "paused")
	expect(t, k8s.Update(context.Background(), stored), "")
	set(t, stored, map[string]any{"node-monitor-grace-period": "40s", "node_monitor_period": "5s", "terminated-pod-gc-threshold": "100"}, "spec", "controllerManager", "extraArgs")
	err := k8s.Update(context.Background(), stored)
	expect(t, err, "Lodia sets this flag")
	expect(t, err, "a key must be a flag's name as the component spells it")
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

func TestAGatewayStoredWithoutASectionNameKeepsItsOtherUpdates(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	stored := example(t, "controlplane-ha.yaml", "section-ratcheting")
	unstructured.RemoveNestedField(stored.Object, "spec", "endpoint", "gateway", "parentRef", "sectionName")
	storeUnderAnOlderSchema(t, "section.ratcheting.lodia.io", stored, "sectionName: Required value", func(schema map[string]any) {
		parentRef := []string{"properties", "spec", "properties", "endpoint", "properties", "gateway", "properties", "parentRef"}
		if err := unstructured.SetNestedStringSlice(schema, []string{"name", "namespace"}, append(parentRef, "required")...); err != nil {
			t.Fatal(err)
		}
	})
	ctx := context.Background()
	stored.SetFinalizers([]string{lodiav1alpha1.DatastoreFinalizer})
	expect(t, k8s.Update(ctx, stored), "")
	set(t, stored, true, "spec", "paused")
	expect(t, k8s.Update(ctx, stored), "")
	stored.SetFinalizers(nil)
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

func TestTheDatastoreClassCannotChange(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const immutable = "datastore.className is immutable in v1alpha1"
	for i, c := range []struct {
		name     string
		from, to map[string]any
		want     string
	}{
		{"absent, then empty", nil, map[string]any{}, ""},
		{"absent, then an empty class name", nil, map[string]any{"className": ""}, ""},
		{"an empty class name, then absent", map[string]any{"className": ""}, nil, ""},
		{"empty, then absent", map[string]any{}, nil, ""},
		{"empty, then a class", map[string]any{}, map[string]any{"className": "fast-ssd"}, immutable},
		{"absent, then a class", nil, map[string]any{"className": "fast-ssd"}, immutable},
		{"a class, then absent", map[string]any{"className": "fast-ssd"}, nil, immutable},
		{"a class, then another", map[string]any{"className": "fast-ssd"}, map[string]any{"className": "slow-hdd"}, immutable},
		{"a class kept", map[string]any{"className": "fast-ssd"}, map[string]any{"className": "fast-ssd"}, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-single.yaml", fmt.Sprintf("datastore-%d", i))
			if c.from != nil {
				set(t, obj, c.from, "spec", "datastore")
			}
			update(t, obj, func(t *testing.T, o *unstructured.Unstructured) {
				if c.to == nil {
					unstructured.RemoveNestedField(o.Object, "spec", "datastore")
					return
				}
				set(t, o, c.to, "spec", "datastore")
			}, c.want)
		})
	}
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

func TestGatewaySectionName(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const pattern = "should match '^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$'"
	for _, c := range []struct {
		name, sectionName, want string
	}{
		{"a label", "tls", ""},
		{"a subdomain", "tls-443.edge.example.com", ""},
		{"253 characters", strings.Repeat("a", 253), ""},
		{"empty", "", "sectionName in body should be at least 1 chars long"},
		{"254 characters", strings.Repeat("a", 254), "sectionName: Too long: may not be more than 253 bytes"},
		{"uppercase", "TLS", pattern},
		{"a slash", "tls/443", pattern},
		{"a leading hyphen", "-tls", pattern},
		{"a trailing dot", "tls.", pattern},
	} {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-ha.yaml", "section")
			set(t, obj, c.sectionName, "spec", "endpoint", "gateway", "parentRef", "sectionName")
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
		})
	}
	t.Run("absent", func(t *testing.T) {
		obj := example(t, "controlplane-ha.yaml", "section")
		unstructured.RemoveNestedField(obj.Object, "spec", "endpoint", "gateway", "parentRef", "sectionName")
		expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), "spec.endpoint.gateway.parentRef.sectionName: Required value")
	})
}

const (
	proxyOnly          = "apiServer.advertiseAddress is set only where apiserver-proxy runs: with endpoint.gateway, or with endpoint.loadBalancer unless its host is an IP address"
	ipv6NeedsAnAddress = "an IPv6 first service range needs apiServer.advertiseAddress where apiserver-proxy runs, since the address has no IPv6 default"
	advertiseOutside   = "must lie outside the service and pod ranges"
	advertiseImmutable = "apiServer.advertiseAddress is immutable"
)

func ipv6Ranges(t *testing.T, o *unstructured.Unstructured) {
	t.Helper()
	set(t, o, []any{"fd00:10:96::/112"}, "spec", "network", "serviceCIDRs")
	set(t, o, []any{"fd00:10:244::/56"}, "spec", "network", "podCIDRs")
}

func TestAdvertiseAddress(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const (
		unicast = "must be a unicast IP address in canonical form, neither loopback nor link-local"
		family  = "must be of the family of the first service range"
	)
	address := func(a string) func(*testing.T, *unstructured.Unstructured) {
		return func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, a, "spec", "apiServer", "advertiseAddress")
		}
	}
	cases := []struct {
		name   string
		mutate []func(*testing.T, *unstructured.Unstructured)
		want   string
	}{
		{"the default", nil, ""},
		{"another address of class E", []func(*testing.T, *unstructured.Unstructured){address("240.0.0.2")}, ""},
		{"a private address", []func(*testing.T, *unstructured.Unstructured){address("172.20.0.1")}, ""},
		{"loopback", []func(*testing.T, *unstructured.Unstructured){address("127.0.0.1")}, unicast},
		{"link-local", []func(*testing.T, *unstructured.Unstructured){address("169.254.0.1")}, unicast},
		{"unspecified", []func(*testing.T, *unstructured.Unstructured){address("0.0.0.0")}, unicast},
		{"multicast", []func(*testing.T, *unstructured.Unstructured){address("239.1.1.1")}, unicast},
		{"broadcast", []func(*testing.T, *unstructured.Unstructured){address("255.255.255.255")}, unicast},
		{"a name", []func(*testing.T, *unstructured.Unstructured){address("proxy.example.com")}, unicast},
		{"in the service range", []func(*testing.T, *unstructured.Unstructured){address("10.96.0.1")}, advertiseOutside},
		{"in the pod range", []func(*testing.T, *unstructured.Unstructured){address("10.244.3.1")}, advertiseOutside},
		{"IPv6 beside an IPv4 service range", []func(*testing.T, *unstructured.Unstructured){address("fd12:3456:789a::1")}, family},
		{"IPv6 not in canonical form", []func(*testing.T, *unstructured.Unstructured){ipv6Ranges, address("FD12:3456:789A::1")}, unicast},
		{"an IPv6 service range with a unique local address", []func(*testing.T, *unstructured.Unstructured){ipv6Ranges, address("fd12:3456:789a::1")}, ""},
		{"an IPv4 address beside an IPv6 service range", []func(*testing.T, *unstructured.Unstructured){ipv6Ranges, address("240.0.0.1")}, family},
		{"dual stack, IPv4 first, with the default", []func(*testing.T, *unstructured.Unstructured){func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{"10.96.0.0/16", "fd00:10:96::/112"}, "spec", "network", "serviceCIDRs")
		}}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-ha.yaml", "advertised")
			for _, mutate := range c.mutate {
				mutate(t, obj)
			}
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
		})
	}
}

type proxyScope struct {
	name                string
	endpoint, endpoint6 map[string]any
	proxy               bool
}

var proxyScopes = []proxyScope{
	{"gateway", nil, nil, true},
	{"a load balancer without a host", map[string]any{"loadBalancer": map[string]any{}}, nil, true},
	{"a load balancer at a name", map[string]any{"loadBalancer": map[string]any{"host": "api.scope.example.com"}}, nil, true},
	{"a load balancer at an IP address", map[string]any{"loadBalancer": map[string]any{"host": "203.0.113.10"}}, map[string]any{"loadBalancer": map[string]any{"host": "2001:db8::10"}}, false},
	{"a node port", map[string]any{"nodePort": map[string]any{}}, nil, false},
}

func (s proxyScope) controlPlane(t *testing.T, name string, ipv6 bool) *unstructured.Unstructured {
	t.Helper()
	obj := example(t, "controlplane-ha.yaml", name)
	endpoint := s.endpoint
	if ipv6 {
		ipv6Ranges(t, obj)
		if s.endpoint6 != nil {
			endpoint = s.endpoint6
		}
	}
	if endpoint != nil {
		set(t, obj, runtime.DeepCopyJSONValue(endpoint), "spec", "endpoint")
	}
	if got := typedSpec(t, obj).Endpoint.NodeProxy(); got != s.proxy {
		t.Fatalf("NodeProxy() of %v is %t, want %t", obj.Object["spec"], got, s.proxy)
	}
	return obj
}

func typedSpec(t *testing.T, obj *unstructured.Unstructured) lodiav1alpha1.ControlPlaneSpec {
	t.Helper()
	typed := lodiav1alpha1.ControlPlane{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &typed); err != nil {
		t.Fatal(err)
	}
	return typed.Spec
}

func TestAdvertiseAddressScope(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	where := func(proxy bool, there, elsewhere string) string {
		if proxy {
			return there
		}
		return elsewhere
	}
	for _, s := range proxyScopes {
		t.Run(s.name, func(t *testing.T) {
			for _, c := range []struct {
				name   string
				ipv6   bool
				mutate func(*testing.T, *unstructured.Unstructured)
				want   string
			}{
				{"an address", false, func(t *testing.T, o *unstructured.Unstructured) {
					set(t, o, "240.0.0.2", "spec", "apiServer", "advertiseAddress")
				}, where(s.proxy, "", proxyOnly)},
				{"no address", false, nil, ""},
				{"no address beside a pod range that holds the default", false, func(t *testing.T, o *unstructured.Unstructured) {
					advertise := typedSpec(t, o).AdvertiseAddress()
					if want := where(s.proxy, lodiav1alpha1.DefaultAdvertiseAddress, ""); advertise != want {
						t.Fatalf("AdvertiseAddress() is %q, want %q", advertise, want)
					}
					set(t, o, []any{"240.0.0.0/16"}, "spec", "network", "podCIDRs")
				}, where(s.proxy, advertiseOutside, "")},
				{"an address beside a pod range that holds it", false, func(t *testing.T, o *unstructured.Unstructured) {
					set(t, o, "172.20.0.1", "spec", "apiServer", "advertiseAddress")
					if advertise, want := typedSpec(t, o).AdvertiseAddress(), where(s.proxy, "172.20.0.1", ""); advertise != want {
						t.Fatalf("AdvertiseAddress() is %q, want %q", advertise, want)
					}
					set(t, o, []any{"172.20.0.0/16"}, "spec", "network", "podCIDRs")
				}, where(s.proxy, advertiseOutside, proxyOnly)},
				{"no address beside an IPv6 first service range", true, nil, where(s.proxy, ipv6NeedsAnAddress, "")},
				{"an IPv6 address beside an IPv6 first service range", true, func(t *testing.T, o *unstructured.Unstructured) {
					set(t, o, "fd12:3456:789a::1", "spec", "apiServer", "advertiseAddress")
				}, where(s.proxy, "", proxyOnly)},
			} {
				t.Run(c.name, func(t *testing.T) {
					obj := s.controlPlane(t, "scope", c.ipv6)
					if c.mutate != nil {
						c.mutate(t, obj)
					}
					expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
				})
			}
		})
	}
}

func TestAdvertiseAddressTransitions(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	cases := []struct {
		name, from string
		mutate     func(*testing.T, *unstructured.Unstructured)
		want       string
	}{
		{"the default named", "", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "240.0.0.1", "spec", "apiServer", "advertiseAddress")
		}, advertiseImmutable},
		{"changed", "240.0.0.2", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "240.0.0.3", "spec", "apiServer", "advertiseAddress")
		}, advertiseImmutable},
		{"removed", "240.0.0.2", func(t *testing.T, o *unstructured.Unstructured) {
			unstructured.RemoveNestedField(o.Object, "spec", "apiServer", "advertiseAddress")
		}, advertiseImmutable},
		{"kept while something else changes", "240.0.0.2", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, true, "spec", "paused")
		}, ""},
	}
	for i, s := range proxyScopes {
		t.Run(s.name, func(t *testing.T) {
			if !s.proxy {
				update(t, s.controlPlane(t, fmt.Sprintf("advertised-%d", i), false), func(t *testing.T, o *unstructured.Unstructured) {
					set(t, o, "240.0.0.2", "spec", "apiServer", "advertiseAddress")
				}, proxyOnly)
				return
			}
			for j, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					obj := s.controlPlane(t, fmt.Sprintf("advertised-%d-%d", i, j), false)
					if c.from != "" {
						set(t, obj, c.from, "spec", "apiServer", "advertiseAddress")
					}
					update(t, obj, c.mutate, c.want)
				})
			}
		})
	}
}

func TestLoadBalancerHost(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const (
		form    = "must be a unicast IP address in canonical form, neither loopback nor link-local, or a lowercase DNS name"
		family  = "endpoint.loadBalancer.host, an IP address, must be of the family of the first service range"
		outside = "endpoint.loadBalancer.host, an IP address, must lie outside the service and pod ranges"
	)
	for _, c := range []struct {
		name, host string
		ipv6       bool
		want       string
	}{
		{"a public IPv4 address", "203.0.113.10", false, ""},
		{"a private IPv4 address", "172.20.0.10", false, ""},
		{"an IPv6 address", "2001:db8::10", true, ""},
		{"a unique local IPv6 address", "fd12:3456:789a::10", true, ""},
		{"a name", "api.dev.example.com", false, ""},
		{"a name of one label", "api", false, ""},
		{"a name of 253 characters", strings.Repeat(strings.Repeat("a", 63)+".", 3) + strings.Repeat("a", 61), false, ""},
		{"a name of 254 characters", strings.Repeat(strings.Repeat("a", 63)+".", 3) + strings.Repeat("a", 62), false, "host: Too long"},
		{"a label of 63 characters", strings.Repeat("a", 63) + ".example.com", false, ""},
		{"a label of 64 characters", strings.Repeat("a", 64) + ".example.com", false, form},
		{"empty", "", false, "host in body should be at least 1 chars long"},
		{"an uppercase name", "API.dev.example.com", false, form},
		{"a name with a trailing dot", "api.dev.example.com.", false, form},
		{"a URL", "https://api.dev.example.com", false, form},
		{"an address with a port", "203.0.113.10:443", false, form},
		{"1.2.3", "1.2.3", false, form},
		{"256.256.256.256", "256.256.256.256", false, form},
		{"an IPv4 address with a leading zero", "203.0.113.010", false, form},
		{"IPv4 loopback", "127.0.0.1", false, form},
		{"IPv4 link-local", "169.254.0.10", false, form},
		{"IPv4 unspecified", "0.0.0.0", false, form},
		{"IPv4 multicast", "239.1.1.1", false, form},
		{"IPv4 broadcast", "255.255.255.255", false, form},
		{"IPv6 loopback", "::1", true, form},
		{"IPv6 link-local", "fe80::10", true, form},
		{"IPv6 unspecified", "::", true, form},
		{"IPv6 multicast", "ff02::1", true, form},
		{"IPv6 in uppercase", "2001:DB8::10", true, form},
		{"IPv6 not compressed", "2001:db8:0:0:0:0:0:10", true, form},
		{"IPv6 in brackets", "[2001:db8::10]", true, form},
		{"IPv6 with a zone", "fe80::10%eth0", true, form},
		{"an IPv4-mapped IPv6 address", "::ffff:203.0.113.10", true, form},
		{"an IPv6 address beside an IPv4 first service range", "2001:db8::10", false, family},
		{"an IPv4 address beside an IPv6 first service range", "203.0.113.10", true, family},
		{"in the service range", "10.96.0.10", false, outside},
		{"in the pod range", "10.244.3.10", false, outside},
		{"in the IPv6 service range", "fd00:10:96::10", true, outside},
		{"in the IPv6 pod range", "fd00:10:244::10", true, outside},
	} {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-single.yaml", "host")
			if c.ipv6 {
				ipv6Ranges(t, obj)
				set(t, obj, "fd12:3456:789a::1", "spec", "apiServer", "advertiseAddress")
				if c.want == "" {
					unstructured.RemoveNestedField(obj.Object, "spec", "apiServer")
				}
			}
			set(t, obj, c.host, "spec", "endpoint", "loadBalancer", "host")
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
		})
	}
	t.Run("dual stack, IPv4 first, with an IPv4 address", func(t *testing.T) {
		obj := example(t, "controlplane-single.yaml", "host")
		set(t, obj, []any{"10.96.0.0/16", "fd00:10:96::/112"}, "spec", "network", "serviceCIDRs")
		set(t, obj, []any{"10.244.0.0/16", "fd00:10:244::/56"}, "spec", "network", "podCIDRs")
		set(t, obj, "203.0.113.10", "spec", "endpoint", "loadBalancer", "host")
		expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), "")
	})
}

func TestAnEndpointHostTakesTheAdvertiseAddressAndHostnameRules(t *testing.T) {
	manifest, err := os.ReadFile(filepath.Join("..", "..", "api", "crds", "lodia.io_controlplanes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	schema := crdSchema(t, manifest)
	advertise := rules(at(t, schema, "spec.apiServer.advertiseAddress"))
	hostname := rules(at(t, schema, "spec.network.clusterDomain"))
	host := rules(at(t, schema, "spec.endpoint.loadBalancer.host"))
	if len(advertise) != 1 || len(hostname) != 1 || !slices.Equal(host, []string{advertise[0] + " || " + hostname[0]}) {
		t.Errorf("endpoint.loadBalancer.host has the rules %q, want the advertise address's %q or the Hostname's %q", host, advertise, hostname)
	}
}

func TestEndpointTransitions(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	const immutable = "endpoint is immutable in v1alpha1 but for loadBalancer.annotations and loadBalancer.sourceRanges"
	loadBalancer := func(fields map[string]any) map[string]any {
		lb := map[string]any{
			"host":         "api.dev.example.com",
			"class":        "example.com/l4",
			"sourceRanges": []any{"203.0.113.0/24", "198.51.100.0/24"},
			"annotations":  map[string]any{"example.com/address": "a"},
		}
		for k, v := range fields {
			if v == nil {
				delete(lb, k)
			} else {
				lb[k] = v
			}
		}
		return map[string]any{"loadBalancer": lb, "certSANs": []any{"dev.example.com", "dev.corp.example.com"}}
	}
	gateway := example(t, "controlplane-ha.yaml", "gateway").Object["spec"].(map[string]any)["endpoint"].(map[string]any)
	withGateway := func(mutate func(map[string]any)) map[string]any {
		g := runtime.DeepCopyJSONValue(gateway).(map[string]any)
		mutate(g["gateway"].(map[string]any))
		return g
	}
	under := func(m map[string]any, path ...string) map[string]any {
		for _, p := range path {
			m = m[p].(map[string]any)
		}
		return m
	}
	nodePort := map[string]any{"nodePort": map[string]any{}}
	cases := []struct {
		field    string
		name     string
		from, to map[string]any
		want     string
	}{
		{"loadBalancer.annotations", "added", loadBalancer(map[string]any{"annotations": nil}), loadBalancer(nil), ""},
		{"loadBalancer.annotations", "changed", loadBalancer(nil), loadBalancer(map[string]any{"annotations": map[string]any{"example.com/address": "b"}}), ""},
		{"loadBalancer.annotations", "removed", loadBalancer(nil), loadBalancer(map[string]any{"annotations": nil}), ""},
		{"loadBalancer.annotations", "changed beside an IP host", loadBalancer(map[string]any{"host": "203.0.113.10"}), loadBalancer(map[string]any{"host": "203.0.113.10", "annotations": map[string]any{"example.com/address": "b"}}), ""},
		{"loadBalancer.annotations", "changed with the class", loadBalancer(nil), loadBalancer(map[string]any{"class": "example.com/l7", "annotations": map[string]any{"example.com/address": "b"}}), immutable},
		{"loadBalancer.sourceRanges", "added", loadBalancer(map[string]any{"sourceRanges": nil}), loadBalancer(nil), ""},
		{"loadBalancer.sourceRanges", "narrowed", loadBalancer(nil), loadBalancer(map[string]any{"sourceRanges": []any{"203.0.113.0/24"}}), ""},
		{"loadBalancer.sourceRanges", "reordered", loadBalancer(nil), loadBalancer(map[string]any{"sourceRanges": []any{"198.51.100.0/24", "203.0.113.0/24"}}), ""},
		{"loadBalancer.sourceRanges", "removed", loadBalancer(nil), loadBalancer(map[string]any{"sourceRanges": nil}), ""},
		{"loadBalancer.host", "an IP address added", loadBalancer(map[string]any{"host": nil}), loadBalancer(map[string]any{"host": "203.0.113.10"}), immutable},
		{"loadBalancer.host", "a name added", loadBalancer(map[string]any{"host": nil}), loadBalancer(nil), immutable},
		{"loadBalancer.host", "an IP address changed", loadBalancer(map[string]any{"host": "203.0.113.10"}), loadBalancer(map[string]any{"host": "203.0.113.11"}), immutable},
		{"loadBalancer.host", "a name changed", loadBalancer(nil), loadBalancer(map[string]any{"host": "api2.dev.example.com"}), immutable},
		{"loadBalancer.host", "an IP address for a name", loadBalancer(nil), loadBalancer(map[string]any{"host": "203.0.113.10"}), immutable},
		{"loadBalancer.host", "removed", loadBalancer(nil), loadBalancer(map[string]any{"host": nil}), immutable},
		{"loadBalancer.class", "added", loadBalancer(map[string]any{"class": nil}), loadBalancer(nil), immutable},
		{"loadBalancer.class", "changed", loadBalancer(nil), loadBalancer(map[string]any{"class": "example.com/l7"}), immutable},
		{"loadBalancer.class", "removed", loadBalancer(nil), loadBalancer(map[string]any{"class": nil}), immutable},
		{"certSANs", "added", map[string]any{"loadBalancer": map[string]any{}}, map[string]any{"loadBalancer": map[string]any{}, "certSANs": []any{"dev.example.com"}}, immutable},
		{"certSANs", "changed", loadBalancer(nil), func() map[string]any {
			e := loadBalancer(nil)
			e["certSANs"] = []any{"dev.example.com", "dev2.corp.example.com"}
			return e
		}(), immutable},
		{"certSANs", "reordered", loadBalancer(nil), func() map[string]any {
			e := loadBalancer(nil)
			e["certSANs"] = []any{"dev.corp.example.com", "dev.example.com"}
			return e
		}(), ""},
		{"certSANs", "removed", loadBalancer(nil), func() map[string]any {
			e := loadBalancer(nil)
			delete(e, "certSANs")
			return e
		}(), immutable},
		{"loadBalancer", "for a node port", loadBalancer(nil), nodePort, immutable},
		{"loadBalancer", "for a gateway", map[string]any{"loadBalancer": map[string]any{}}, gateway, immutable},
		{"nodePort", "for a load balancer", nodePort, map[string]any{"loadBalancer": map[string]any{}}, immutable},
		{"gateway", "for a load balancer", gateway, map[string]any{"loadBalancer": map[string]any{}}, immutable},
		{"gateway.parentRef.name", "changed", gateway, withGateway(func(g map[string]any) { under(g, "parentRef")["name"] = "other" }), immutable},
		{"gateway.parentRef.namespace", "changed", gateway, withGateway(func(g map[string]any) { under(g, "parentRef")["namespace"] = "other" }), immutable},
		{"gateway.parentRef.sectionName", "changed", gateway, withGateway(func(g map[string]any) { under(g, "parentRef")["sectionName"] = "other" }), immutable},
		{"gateway.hostnames.apiServer", "changed", gateway, withGateway(func(g map[string]any) { under(g, "hostnames")["apiServer"] = "api2.prod-eu.k8s.example.com" }), immutable},
		{"gateway.hostnames.konnectivity", "changed", gateway, withGateway(func(g map[string]any) {
			under(g, "hostnames")["konnectivity"] = "konnectivity2.prod-eu.k8s.example.com"
		}), immutable},
		{"gateway.hostnames.tunnel", "changed", gateway, withGateway(func(g map[string]any) { under(g, "hostnames")["tunnel"] = "tunnel2.prod-eu.k8s.example.com" }), immutable},
	}
	covered := map[string]bool{}
	for i, c := range cases {
		covered[c.field] = true
		t.Run(c.field+"/"+c.name, func(t *testing.T) {
			obj := example(t, "controlplane-single.yaml", fmt.Sprintf("endpoint-%d", i))
			set(t, obj, runtime.DeepCopyJSONValue(c.from), "spec", "endpoint")
			update(t, obj, func(t *testing.T, o *unstructured.Unstructured) {
				set(t, o, runtime.DeepCopyJSONValue(c.to), "spec", "endpoint")
			}, c.want)
		})
	}
	manifest, err := os.ReadFile(filepath.Join("..", "..", "api", "crds", "lodia.io_controlplanes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var leaves func(prefix string, schema apiextensionsv1.JSONSchemaProps) []string
	leaves = func(prefix string, schema apiextensionsv1.JSONSchemaProps) []string {
		if schema.Type != "object" || len(schema.Properties) == 0 {
			return []string{prefix}
		}
		var out []string
		for name, child := range schema.Properties {
			out = append(out, leaves(strings.TrimPrefix(prefix+"."+name, "."), child)...)
		}
		return out
	}
	for _, field := range leaves("", at(t, crdSchema(t, manifest), "spec.endpoint")) {
		if !covered[field] {
			t.Errorf("spec.endpoint.%s has no transition case", field)
		}
	}
}

func TestTheSchemaDefaultsTheAdvertiseAddressAsTheAPI(t *testing.T) {
	manifest, err := os.ReadFile(filepath.Join("..", "..", "api", "crds", "lodia.io_controlplanes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	quoted := "'" + lodiav1alpha1.DefaultAdvertiseAddress + "'"
	if !slices.ContainsFunc(rules(at(t, crdSchema(t, manifest), "spec")), func(rule string) bool { return strings.Contains(rule, "["+quoted+"]") }) {
		t.Errorf("no rule of spec takes %s for the advertise address where apiserver-proxy runs", quoted)
	}
}

func TestRestoreFromAtCreation(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	for _, c := range []struct {
		name, release, want, stored string
	}{
		{"pending by default", "", "", "Pending"},
		{"held", "Held", `supported values: "Pending", "Released"`, ""},
		{"released at once", "Released", "", "Released"},
		{"another release", "Paused", `supported values: "Pending", "Released"`, ""},
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
		{"removed", "Pending", func(t *testing.T, o *unstructured.Unstructured) {
			unstructured.RemoveNestedField(o.Object, "spec", "restoreFrom")
		}, "restoreFrom can be set only when the ControlPlane is created, and never removed"},
		{"another Backup", "Pending", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "weekly", "spec", "restoreFrom", "backupRef", "name")
		}, "restoreFrom.backupRef is immutable"},
		{"released", "Pending", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "Released", "spec", "restoreFrom", "release")
		}, ""},
		{"pending again", "Released", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "Pending", "spec", "restoreFrom", "release")
		}, "restoreFrom.release moves only from Pending to Released"},
		{"kept while something else changes", "Pending", func(t *testing.T, o *unstructured.Unstructured) {
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
		{"the rehearsal", map[string]any{"step": "Rehearsal", "startedAt": "2026-10-27T02:00:00Z"}, ""},
		{"another step", map[string]any{"step": "Nodes", "startedAt": "2026-10-27T02:00:00Z"}, `supported values: "Rehearsal", "Backup", "APIServer", "ControllerManager", "Scheduler", "Addons"`},
		{"no start", map[string]any{"step": "APIServer"}, "startedAt: Required value"},
	} {
		t.Run(c.name, func(t *testing.T) {
			written := obj.DeepCopy()
			set(t, written, map[string]any{"version": "v1.36.4", "upgrade": c.upgrade}, "status", "kubernetes")
			expect(t, k8s.Status().Update(context.Background(), written, client.DryRunAll), c.want)
		})
	}
}

func TestRehearsalStatus(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	obj := example(t, "controlplane-single.yaml", "rehearsal-status")
	if err := k8s.Create(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
	for _, c := range []struct {
		name      string
		rehearsal map[string]any
		want      string
	}{
		{"a version", map[string]any{"version": "v1.37.1"}, ""},
		{"a version of 16 characters", map[string]any{"version": "v1.37.1234567890"}, ""},
		{"a version of 17 characters", map[string]any{"version": "v1.37.12345678901"}, "status.rehearsal.version: Too long"},
		{"no version", map[string]any{}, "status.rehearsal.version: Required value"},
	} {
		t.Run(c.name, func(t *testing.T) {
			written := obj.DeepCopy()
			set(t, written, c.rehearsal, "status", "rehearsal")
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
			set(t, written, c.restore, "status", "restore")
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

func TestRemovedFieldsAreUnknown(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	for _, c := range []struct {
		file  string
		value any
		path  []string
	}{
		{"controlplane-single.yaml", "Split", []string{"spec", "layout"}},
		{"controlplane-single.yaml", map[string]any{"policyRef": map[string]any{"name": "audit"}}, []string{"spec", "apiServer", "audit"}},
		{"controlplane-single.yaml", map[string]any{"configRef": map[string]any{"name": "admission"}}, []string{"spec", "apiServer", "admission"}},
		{"controlplane-single.yaml", map[string]any{"name": "profiles"}, []string{"spec", "scheduler", "configRef"}},
		{"controlplane-single.yaml", true, []string{"spec", "serviceAccount", "publicDiscovery"}},
		{"controlplane-single.yaml", map[string]any{"name": "pool-a"}, []string{"spec", "datastore", "shared"}},
		{"controlplane-single.yaml", map[string]any{"className": "fast-ssd"}, []string{"spec", "datastore", "dedicated"}},
		{"datastoreclass.yaml", "720h", []string{"spec", "backup", "retention", "afterDelete"}},
	} {
		field := strings.Join(c.path, ".")
		t.Run(field, func(t *testing.T) {
			obj := example(t, c.file, "removed")
			set(t, obj, c.value, c.path...)
			err := k8s.Create(context.Background(), obj.DeepCopy(), client.DryRunAll, client.FieldValidation("Strict"))
			if err == nil || !strings.Contains(err.Error(), `unknown field "`+field+`"`) {
				t.Fatalf("want %s refused as an unknown field under strict field validation, got %v", field, err)
			}
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), "")
			if _, found, _ := unstructured.NestedFieldNoCopy(obj.Object, c.path...); found {
				t.Errorf("%s is kept, want it pruned", field)
			}
		})
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

func TestTheDatastoreKindIsGone(t *testing.T) {
	needsEnvtest(t)
	obj := decode(t, "{apiVersion: lodia.io/v1alpha1, kind: Datastore, metadata: {name: pool-a}, spec: {endpoints: [https://etcd-0.pool-a.etcd.svc:2379]}}")
	if err := k8s.Create(context.Background(), obj, client.DryRunAll); !meta.IsNoMatchError(err) {
		t.Fatalf("want no kind Datastore served, got %v", err)
	}
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
