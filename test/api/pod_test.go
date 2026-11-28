// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	labelName      = "a key must be a label's name"
	labelValue     = "must be a label's value"
	withoutKey     = "a toleration without a key has operator Exists"
	noExecute      = "tolerationSeconds is set only with effect NoExecute"
	existsNoValue  = "a toleration with operator Exists has no value"
	tolerationKey  = "must be a label's name"
	subdomain      = "must be a lowercase DNS subdomain"
	resourceNames  = "a control plane container takes cpu, memory and ephemeral-storage alone"
	negative       = "a quantity cannot be negative"
	aboveLimit     = "a request cannot exceed its limit"
	unsupportedOp  = `supported values: "Equal", "Exists"`
	unsupportedEff = `supported values: "NoSchedule", "PreferNoSchedule", "NoExecute"`
)

var placements = []struct {
	name      string
	placement map[string]any
	want      string
}{
	{"a node selector of a plain name", map[string]any{"nodeSelector": map[string]any{"pool": "control-planes"}}, ""},
	{"a node selector of a prefixed name", map[string]any{"nodeSelector": map[string]any{"node-role.kubernetes.io/control-plane": ""}}, ""},
	{"a node selector of an empty value", map[string]any{"nodeSelector": map[string]any{"pool": ""}}, ""},
	{"a node selector of a value of 63 characters", map[string]any{"nodeSelector": map[string]any{"pool": strings.Repeat("a", 63)}}, ""},
	{"a node selector of a value of 64 characters", map[string]any{"nodeSelector": map[string]any{"pool": strings.Repeat("a", 64)}}, "Too long"},
	{"a node selector of a value with underscores and dots", map[string]any{"nodeSelector": map[string]any{"pool": "a_b.c-d"}}, ""},
	{"a node selector of a value that starts with a dash", map[string]any{"nodeSelector": map[string]any{"pool": "-a"}}, labelValue},
	{"a node selector of a value with a space", map[string]any{"nodeSelector": map[string]any{"pool": "a b"}}, labelValue},
	{"a node selector of a name with two slashes", map[string]any{"nodeSelector": map[string]any{"a/b/c": "x"}}, labelName},
	{"a node selector of a name with a space", map[string]any{"nodeSelector": map[string]any{"a b": "x"}}, labelName},
	{"a node selector of an uppercase prefix", map[string]any{"nodeSelector": map[string]any{"Example.com/pool": "x"}}, labelName},
	{"a node selector of a name of 63 characters", map[string]any{"nodeSelector": map[string]any{strings.Repeat("n", 63): "x"}}, ""},
	{"a node selector of a name of 64 characters", map[string]any{"nodeSelector": map[string]any{strings.Repeat("n", 64): "x"}}, labelName},
	{"a node selector of a prefix of 253 characters", map[string]any{"nodeSelector": map[string]any{longPrefix(253) + "/pool": "x"}}, ""},
	{"a node selector of a prefix of 254 characters", map[string]any{"nodeSelector": map[string]any{longPrefix(254) + "/pool": "x"}}, labelName},
	{"a node selector of a prefixed name of 63 characters", map[string]any{"nodeSelector": map[string]any{"example.com/" + strings.Repeat("n", 63): "x"}}, ""},
	{"a node selector of a prefixed name of 64 characters", map[string]any{"nodeSelector": map[string]any{"example.com/" + strings.Repeat("n", 64): "x"}}, labelName},
	{"a node selector of an uppercase name and value", map[string]any{"nodeSelector": map[string]any{"topology.example.com/Zone": "EU-West"}}, ""},
	{"a toleration of every taint", map[string]any{"tolerations": []any{map[string]any{"operator": "Exists"}}}, ""},
	{"a toleration without a key, operator Equal by default", map[string]any{"tolerations": []any{map[string]any{"value": "x"}}}, withoutKey},
	{"a toleration of an empty key", map[string]any{"tolerations": []any{map[string]any{"key": "", "operator": "Exists"}}}, "spec.placement.tolerations[0].key in body should be at least 1 chars long"},
	{"a toleration of a prefixed key", map[string]any{"tolerations": []any{map[string]any{"key": "node-role.kubernetes.io/control-plane", "operator": "Exists", "effect": "NoSchedule"}}}, ""},
	{"a toleration of a key with a space", map[string]any{"tolerations": []any{map[string]any{"key": "a b", "operator": "Exists"}}}, tolerationKey},
	{"a toleration of a key of 63 characters", map[string]any{"tolerations": []any{map[string]any{"key": strings.Repeat("k", 63), "operator": "Exists"}}}, ""},
	{"a toleration of a key of 64 characters", map[string]any{"tolerations": []any{map[string]any{"key": strings.Repeat("k", 64), "operator": "Exists"}}}, tolerationKey},
	{"a toleration of a key with a prefix of 253 characters", map[string]any{"tolerations": []any{map[string]any{"key": longPrefix(253) + "/k", "operator": "Exists"}}}, ""},
	{"a toleration of a key with a prefix of 254 characters", map[string]any{"tolerations": []any{map[string]any{"key": longPrefix(254) + "/k", "operator": "Exists"}}}, tolerationKey},
	{"a toleration of a key and a value", map[string]any{"tolerations": []any{map[string]any{"key": "dedicated", "operator": "Equal", "value": "control-planes", "effect": "NoSchedule"}}}, ""},
	{"a toleration of an uppercase key", map[string]any{"tolerations": []any{map[string]any{"key": "example.com/GPU", "operator": "Exists"}}}, ""},
	{"a toleration of an uppercase value", map[string]any{"tolerations": []any{map[string]any{"key": "dedicated", "value": "Control-Planes"}}}, ""},
	{"a toleration of a key and no value", map[string]any{"tolerations": []any{map[string]any{"key": "dedicated"}}}, ""},
	{"a toleration of a key and an empty value", map[string]any{"tolerations": []any{map[string]any{"key": "dedicated", "value": ""}}}, "spec.placement.tolerations[0].value in body should be at least 1 chars long"},
	{"a toleration of a value with a space", map[string]any{"tolerations": []any{map[string]any{"key": "dedicated", "value": "a b"}}}, labelValue},
	{"a toleration of a value of 64 characters", map[string]any{"tolerations": []any{map[string]any{"key": "dedicated", "value": strings.Repeat("v", 64)}}}, "Too long"},
	{"a toleration Exists with a value", map[string]any{"tolerations": []any{map[string]any{"key": "a", "operator": "Exists", "value": "b"}}}, existsNoValue},
	{"a toleration of seconds with NoExecute", map[string]any{"tolerations": []any{map[string]any{"key": "a", "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": int64(300)}}}, ""},
	{"a toleration of seconds with NoSchedule", map[string]any{"tolerations": []any{map[string]any{"key": "a", "operator": "Exists", "effect": "NoSchedule", "tolerationSeconds": int64(5)}}}, noExecute},
	{"a toleration of seconds without an effect", map[string]any{"tolerations": []any{map[string]any{"key": "a", "operator": "Exists", "tolerationSeconds": int64(5)}}}, noExecute},
	{"a toleration of operator Lt", map[string]any{"tolerations": []any{map[string]any{"key": "a", "operator": "Lt", "value": "5"}}}, unsupportedOp},
	{"a toleration of operator Gt", map[string]any{"tolerations": []any{map[string]any{"key": "a", "operator": "Gt", "value": "5"}}}, unsupportedOp},
	{"a toleration of effect PreferNoSchedule", map[string]any{"tolerations": []any{map[string]any{"key": "a", "operator": "Exists", "effect": "PreferNoSchedule"}}}, ""},
	{"a toleration of an unknown effect", map[string]any{"tolerations": []any{map[string]any{"key": "a", "operator": "Exists", "effect": "Evict"}}}, unsupportedEff},
	{"a priority class", map[string]any{"priorityClassName": "tenant-control-plane"}, ""},
	{"a priority class of a subdomain", map[string]any{"priorityClassName": "control-planes.example.com"}, ""},
	{"an uppercase priority class", map[string]any{"priorityClassName": "High"}, subdomain},
	{"a priority class with two dots in a row", map[string]any{"priorityClassName": "a..b"}, subdomain},
}

var refusedOnPurpose = map[string]string{
	"a toleration of an empty key":             "an empty optional string would mean what leaving it out means",
	"a toleration of a key and an empty value": "an empty optional string would mean what leaving it out means",
	"a toleration of operator Lt":              "pod validation admits it only behind a feature gate",
	"a toleration of operator Gt":              "pod validation admits it only behind a feature gate",
	"a resource a component never needs":       "a control plane pod needs no extended resource",
	"a request of huge pages":                  "a control plane pod needs no huge pages",
}

var resourceRows = []struct {
	name      string
	resources map[string]any
	want      string
}{
	{"an integer cpu request under a string limit", map[string]any{"requests": map[string]any{"cpu": int64(1), "memory": "256Mi"}, "limits": map[string]any{"cpu": "2", "memory": "1Gi"}}, ""},
	{"an equal request and limit", map[string]any{"requests": map[string]any{"cpu": "500m"}, "limits": map[string]any{"cpu": "0.5"}}, ""},
	{"ephemeral storage", map[string]any{"requests": map[string]any{"ephemeral-storage": "1Gi"}, "limits": map[string]any{"ephemeral-storage": "2Gi"}}, ""},
	{"limits alone", map[string]any{"limits": map[string]any{"memory": "1Gi"}}, ""},
	{"a request above its limit", map[string]any{"requests": map[string]any{"memory": "2Gi"}, "limits": map[string]any{"memory": "1Gi"}}, aboveLimit},
	{"an integer request above a milli limit", map[string]any{"requests": map[string]any{"cpu": int64(2)}, "limits": map[string]any{"cpu": "1500m"}}, aboveLimit},
	{"a resource a component never needs", map[string]any{"limits": map[string]any{"nvidia.com/gpu": int64(1)}}, resourceNames},
	{"a request of huge pages", map[string]any{"requests": map[string]any{"memory": "1Gi", "hugepages-2Mi": "2Mi"}}, resourceNames},
	{"a negative request", map[string]any{"requests": map[string]any{"cpu": "-1"}}, negative},
	{"a negative limit", map[string]any{"limits": map[string]any{"memory": "-1Gi"}}, negative},
}

func longPrefix(n int) string {
	label := strings.Repeat("p", 62)
	prefix := label
	for len(prefix)+1+len(label) <= n {
		prefix += "." + label
	}
	return prefix + "." + strings.Repeat("q", n-len(prefix)-1)
}

var placementPaths = []struct{ file, name string }{
	{"controlplane-single.yaml", "spec.placement"},
	{"datastoreclass.yaml", "spec.placement"},
}

var resourcePaths = []struct{ file, name string }{
	{"controlplane-single.yaml", "spec.apiServer.resources"},
	{"controlplane-single.yaml", "spec.controllerManager.resources"},
	{"controlplane-single.yaml", "spec.scheduler.resources"},
	{"datastoreclass.yaml", "spec.etcd.resources"},
}

// TestPlacementAndResourcesTakeTheRulesOfPodValidation holds every placement and resources field to what pod validation admits.
func TestPlacementAndResourcesTakeTheRulesOfPodValidation(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	for _, p := range placementPaths {
		for _, c := range placements {
			t.Run(p.file+"/"+c.name, func(t *testing.T) {
				obj := example(t, p.file, "placement")
				set(t, obj, c.placement, strings.Split(p.name, ".")...)
				expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
			})
		}
	}
	for _, p := range resourcePaths {
		for _, c := range resourceRows {
			t.Run(p.name+"/"+c.name, func(t *testing.T) {
				obj := example(t, p.file, "resources")
				set(t, obj, c.resources, strings.Split(p.name, ".")...)
				expect(t, k8s.Create(context.Background(), obj, client.DryRunAll), c.want)
			})
		}
	}
}

// TestResourcesHaveNoClaims refuses resources.claims as an unknown field.
func TestResourcesHaveNoClaims(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	for _, p := range resourcePaths {
		t.Run(p.name, func(t *testing.T) {
			obj := example(t, p.file, "claims")
			set(t, obj, map[string]any{"claims": []any{map[string]any{"name": "gpu"}}}, strings.Split(p.name, ".")...)
			err := k8s.Create(context.Background(), obj, client.DryRunAll, client.FieldValidation("Strict"))
			if err == nil || !strings.Contains(err.Error(), `unknown field "`+p.name+`.claims"`) {
				t.Fatalf("want %s.claims refused as an unknown field, got %v", p.name, err)
			}
		})
	}
}

// TestEveryAcceptedPlacementAndResourcesPassPodValidation converts every accepted row as Workloads and the engine do, requires the pod it asks for, and creates a Deployment of it.
func TestEveryAcceptedPlacementAndResourcesPassPodValidation(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	for _, c := range placements {
		if c.want != "" {
			continue
		}
		for _, p := range placementPaths {
			t.Run(p.file+"/"+c.name, func(t *testing.T) {
				obj := example(t, p.file, "placement")
				set(t, obj, c.placement, strings.Split(p.name, ".")...)
				placement := admitted[lodiav1alpha1.Placement](t, obj, p.name)
				pod := podOf(p.file, corev1.PodSpec{NodeSelector: placement.PodNodeSelector(), Tolerations: placement.PodTolerations(), PriorityClassName: placement.PriorityClassName})
				if asked := podOf(p.file, decoded[corev1.PodSpec](t, c.placement)); !equality.Semantic.DeepEqual(pod, asked) {
					t.Fatalf("the conversion renders %+v, the row asks %+v", pod, asked)
				}
				expect(t, k8s.Create(context.Background(), deploymentOf(pod, corev1.ResourceRequirements{}), client.DryRunAll), "")
			})
		}
	}
	for _, c := range resourceRows {
		if c.want != "" {
			continue
		}
		for _, p := range resourcePaths {
			t.Run(p.name+"/"+c.name, func(t *testing.T) {
				obj := example(t, p.file, "resources")
				set(t, obj, c.resources, strings.Split(p.name, ".")...)
				resources := admitted[lodiav1alpha1.Resources](t, obj, p.name).Requirements()
				if asked := decoded[corev1.ResourceRequirements](t, c.resources); !equality.Semantic.DeepEqual(resources, asked) {
					t.Fatalf("the conversion renders %+v, the row asks %+v", resources, asked)
				}
				expect(t, k8s.Create(context.Background(), deploymentOf(corev1.PodSpec{}, resources), client.DryRunAll), "")
			})
		}
	}
}

// TestEveryRefusedPlacementAndResourcesFailPodValidation creates a Deployment of the pod every refused row asks for and requires the API server to refuse it, but for the rows refusedOnPurpose names.
func TestEveryRefusedPlacementAndResourcesFailPodValidation(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a")
	for _, c := range placements {
		if c.want == "" || refusedOnPurpose[c.name] != "" {
			continue
		}
		for _, p := range placementPaths {
			t.Run(p.file+"/"+c.name, func(t *testing.T) {
				err := k8s.Create(context.Background(), deploymentOf(podOf(p.file, decoded[corev1.PodSpec](t, c.placement)), corev1.ResourceRequirements{}), client.DryRunAll)
				if !apierrors.IsInvalid(err) {
					t.Fatalf("pod validation does not refuse a row the schema refuses: %v", err)
				}
			})
		}
	}
	for _, c := range resourceRows {
		if c.want == "" || refusedOnPurpose[c.name] != "" {
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			err := k8s.Create(context.Background(), deploymentOf(corev1.PodSpec{}, decoded[corev1.ResourceRequirements](t, c.resources)), client.DryRunAll)
			if !apierrors.IsInvalid(err) {
				t.Fatalf("pod validation does not refuse a row the schema refuses: %v", err)
			}
		})
	}
}

// podOf is pod with its node selector where the file's renderer puts it, a class's in the etcd's node affinity.
func podOf(file string, pod corev1.PodSpec) corev1.PodSpec {
	if file != "datastoreclass.yaml" || len(pod.NodeSelector) == 0 {
		return pod
	}
	var match []corev1.NodeSelectorRequirement
	for _, k := range slices.Sorted(maps.Keys(pod.NodeSelector)) {
		match = append(match, corev1.NodeSelectorRequirement{Key: k, Operator: corev1.NodeSelectorOpIn, Values: []string{pod.NodeSelector[k]}})
	}
	pod.NodeSelector = nil
	pod.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
		NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: match}},
	}}}
	return pod
}

func decoded[T any](t *testing.T, row map[string]any) T {
	t.Helper()
	var out T
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(row, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func admitted[T any](t *testing.T, obj *unstructured.Unstructured, path string) *T {
	t.Helper()
	if err := k8s.Create(context.Background(), obj, client.DryRunAll); err != nil {
		t.Fatalf("the schema refuses an accepted row: %v", err)
	}
	field, found, err := unstructured.NestedMap(obj.Object, strings.Split(path, ".")...)
	if err != nil || !found {
		t.Fatalf("%s is not in the admitted object: %v", path, err)
	}
	out := new(T)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(field, out); err != nil {
		t.Fatal(err)
	}
	return out
}

func deploymentOf(pod corev1.PodSpec, resources corev1.ResourceRequirements) *appsv1.Deployment {
	labels := map[string]string{"app": "pod-validation"}
	pod.Containers = []corev1.Container{{Name: "c", Image: "registry.k8s.io/pause:3.10", Resources: resources}}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-validation", Namespace: "team-a"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: pod},
		},
	}
}
