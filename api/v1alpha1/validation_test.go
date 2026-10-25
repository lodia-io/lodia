// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"
)

var k8s client.Client

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("skipping CRD validation: KUBEBUILDER_ASSETS is not set; run make test")
		os.Exit(0)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
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
	ensureNamespaces(t, "team-a", "team-b")
	for _, obj := range examples(t) {
		t.Run(obj.GetKind()+"/"+obj.GetName(), func(t *testing.T) {
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), "")
		})
	}
}

func TestControlPlaneDefaults(t *testing.T) {
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
		{"a flag Brygga sets", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"etcd-servers": "https://elsewhere:2379"}, "spec", "apiServer", "extraArgs")
		}, "Brygga sets this flag"},
		{"a flag Brygga leaves alone", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"max-requests-inflight": "800"}, "spec", "apiServer", "extraArgs")
		}, ""},
		{"uppercase hostname", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{"API.example.com"}, "spec", "endpoint", "certSANs")
		}, "spec.endpoint.certSANs"},
		{"plain http issuer", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, "http://id.example.com", "spec", "serviceAccount", "issuer")
		}, "must be an https URL"},
		{"window at 25:00", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, map[string]any{"begin": "25:00", "end": "04:00"}, "spec", "maintenance", "window")
		}, "spec.maintenance.window.begin"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "controlplane-single.yaml", "validation")
			c.mutate(t, obj)
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
		})
	}
}

func TestControlPlaneTransitions(t *testing.T) {
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
		}, "restoreFrom can only be set when the ControlPlane is created"},
		{"paused", func(t *testing.T, o *unstructured.Unstructured) { set(t, o, true, "spec", "paused") }, ""},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			obj := example(t, "controlplane-single.yaml", fmt.Sprintf("transition-%d", i))
			if err := k8s.Create(ctx, obj); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
			c.mutate(t, obj)
			expect(t, k8s.Update(ctx, obj), c.want)
		})
	}
}

func TestRestoreFromAtCreation(t *testing.T) {
	ensureNamespaces(t, "team-a")
	obj := example(t, "controlplane-single.yaml", "restored")
	set(t, obj, map[string]any{"backupRef": map[string]any{"name": "nightly"}}, "spec", "restoreFrom")
	expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), "")
}

func TestEndpointStatus(t *testing.T) {
	ensureNamespaces(t, "team-a")
	obj := example(t, "controlplane-single.yaml", "endpoint-status")
	if err := k8s.Create(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
	address := func(host string, port int64) map[string]any { return map[string]any{"host": host, "port": port} }
	cases := []struct {
		name     string
		endpoint map[string]any
		want     string
	}{
		{"an IPv4 address", map[string]any{"apiServer": address("203.0.113.10", 443), "konnectivity": address("203.0.113.10", 8132)}, ""},
		{"an IPv6 address", map[string]any{"apiServer": address("2001:db8::1", 443), "konnectivity": address("2001:db8::1", 8132)}, ""},
		{"DNS names", map[string]any{"apiServer": address("api.example.test", 443), "konnectivity": address("konnectivity.example.test", 443)}, ""},
		{"an IPv6 address in brackets", map[string]any{"apiServer": address("[2001:db8::1]", 443), "konnectivity": address("2001:db8::1", 8132)}, "must be an IP address or a lowercase DNS name"},
		{"an uppercase name", map[string]any{"apiServer": address("API.example.test", 443), "konnectivity": address("api.example.test", 8132)}, "must be an IP address or a lowercase DNS name"},
		{"port 0", map[string]any{"apiServer": address("203.0.113.10", 0), "konnectivity": address("203.0.113.10", 8132)}, "greater than or equal to 1"},
		{"no konnectivity", map[string]any{"apiServer": address("203.0.113.10", 443)}, "konnectivity: Required value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			written := obj.DeepCopy()
			set(t, written, tc.endpoint, "status", "endpoint")
			expect(t, k8s.Status().Update(context.Background(), written, client.DryRunAll), tc.want)
		})
	}
}

func TestKubeconfigRequestValidation(t *testing.T) {
	ensureNamespaces(t, "team-a")
	cases := []struct {
		name   string
		mutate func(*testing.T, *unstructured.Unstructured)
		want   string
	}{
		{"system:masters", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, []any{"system:masters"}, "spec", "groups")
		}, "system:masters cannot be requested"},
		{"five minutes", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, int64(300), "spec", "expirationSeconds")
		}, "spec.expirationSeconds"},
		{"two days", func(t *testing.T, o *unstructured.Unstructured) {
			set(t, o, int64(172800), "spec", "expirationSeconds")
		}, "spec.expirationSeconds"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := example(t, "kubeconfigrequest.yaml", "validation")
			c.mutate(t, obj)
			expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
		})
	}
}

func TestRequestsAndBackupsCannotChange(t *testing.T) {
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
	obj := example(t, "datastore.yaml", "plain")
	set(t, obj, []any{"http://etcd-0.pool-a.etcd.svc:2379"}, "spec", "endpoints")
	expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), "must be an https URL")
}

func TestDatastoreClassValidation(t *testing.T) {
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
