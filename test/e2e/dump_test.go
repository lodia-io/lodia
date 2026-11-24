// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"errors"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func object(apiVersion, kind, namespace, name string, fields map[string]any) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   map[string]any{"name": name, "managedFields": []any{map[string]any{"manager": "noise"}}},
	}}
	if namespace != "" {
		obj.SetNamespace(namespace)
	}
	maps.Copy(obj.Object, fields)
	return obj
}

func apiResource(name string, namespaced bool, verbs ...string) metav1.APIResource {
	return metav1.APIResource{Name: name, Namespaced: namespaced, Verbs: verbs}
}

func discoveryOf(t *testing.T) *discoveryfake.FakeDiscovery {
	t.Helper()
	disco := kubefake.NewSimpleClientset().Discovery().(*discoveryfake.FakeDiscovery)
	disco.Resources = []*metav1.APIResourceList{
		{GroupVersion: "v1", APIResources: []metav1.APIResource{
			apiResource("pods", true, "list", "get"),
			apiResource("pods/log", true, "get"),
			apiResource("secrets", true, "list"),
			apiResource("events", true, "list"),
			apiResource("configmaps", true, "list"),
			apiResource("bindings", true, "create"),
		}},
		{GroupVersion: "lodia.io/v1alpha1", APIResources: []metav1.APIResource{apiResource("controlplanes", true, "list")}},
		{GroupVersion: "lodia.io/v1beta1", APIResources: []metav1.APIResource{apiResource("controlplanes", true, "list")}},
		{GroupVersion: "apiextensions.k8s.io/v1", APIResources: []metav1.APIResource{apiResource("customresourcedefinitions", false, "list")}},
	}
	return disco
}

func dynamicOf(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	listKinds := map[schema.GroupVersionResource]string{}
	for gvr, kind := range map[string]string{"pods": "PodList", "secrets": "SecretList", "events": "EventList", "configmaps": "ConfigMapList"} {
		listKinds[schema.GroupVersionResource{Version: "v1", Resource: gvr}] = kind
	}
	for _, version := range []string{"v1alpha1", "v1beta1"} {
		listKinds[schema.GroupVersionResource{Group: "lodia.io", Version: version, Resource: "controlplanes"}] = "ControlPlaneList"
	}
	listKinds[schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}] = "CustomResourceDefinitionList"
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds, objs...)
}

func TestADumpHoldsTheObjectsAndEventsOfTheClusterAndNoSecret(t *testing.T) {
	dyn := dynamicOf(
		object("v1", "Pod", "dev", "web", nil),
		object("v1", "Secret", "dev", "keys", map[string]any{"data": map[string]any{"token": "c2VjcmV0LXRva2Vu"}}),
		object("v1", "Event", "dev", "web.1", map[string]any{"reason": "FailedScheduling"}),
		object("lodia.io/v1alpha1", "ControlPlane", "dev", "cp", nil),
		object("apiextensions.k8s.io/v1", "CustomResourceDefinition", "", "controlplanes.lodia.io", nil),
	)
	dir := t.TempDir()
	if err := dumpObjects(t.Context(), discoveryOf(t), dyn, dir); err != nil {
		t.Fatal(err)
	}
	var got []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			rel, _ := filepath.Rel(dir, path)
			got = append(got, rel)
			content, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			for _, leaked := range []string{"c2VjcmV0LXRva2Vu", "managedFields", "customresourcedefinitions"} {
				if strings.Contains(string(content), leaked) {
					t.Errorf("%s holds %q", rel, leaked)
				}
			}
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"objects/controlplanes.lodia.io.yaml", "objects/events.yaml", "objects/pods.yaml"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("wrote %v, want %v", got, want)
	}
	if event, err := os.ReadFile(filepath.Join(dir, "objects", "events.yaml")); err != nil || !strings.Contains(string(event), "FailedScheduling") {
		t.Fatalf("the events file is %q, %v", event, err)
	}
	listed := map[string]int{}
	for _, action := range dyn.Actions() {
		listed[action.GetResource().Resource]++
	}
	if listed["secrets"] != 0 || listed["customresourcedefinitions"] != 0 || listed["controlplanes"] != 1 {
		t.Errorf("the dump listed %v, want no Secret, no CustomResourceDefinition and the ControlPlanes once", listed)
	}
}

func TestADumpGoesOnPastAResourceItCannotList(t *testing.T) {
	dyn := dynamicOf(object("v1", "Pod", "dev", "web", nil), object("v1", "Event", "dev", "web.1", nil), object("lodia.io/v1alpha1", "ControlPlane", "dev", "cp", nil))
	dyn.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("the API server is down")
	})
	dir := t.TempDir()
	err := dumpObjects(t.Context(), discoveryOf(t), dyn, dir)
	if err == nil || !strings.Contains(err.Error(), "list pods: the API server is down") {
		t.Fatalf("got %v, want the failed list named", err)
	}
	for _, file := range []string{"events.yaml", "controlplanes.lodia.io.yaml"} {
		if _, err := os.Stat(filepath.Join(dir, "objects", file)); err != nil {
			t.Error(err)
		}
	}
}

func TestADumpHoldsEveryContainersLogAndThePreviousOfARestartedOne(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "dev", Name: "web"},
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{{Name: "init"}},
			ContainerStatuses:     []corev1.ContainerStatus{{Name: "app", RestartCount: 2}, {Name: "side"}},
		},
	}
	dir := t.TempDir()
	if err := dumpLogs(t.Context(), kubefake.NewSimpleClientset(pod), dir); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"init.log", "app.log", "app.previous.log", "side.log"} {
		if content, err := os.ReadFile(filepath.Join(dir, "logs", "dev", "web", want)); err != nil || len(content) == 0 {
			t.Errorf("%s is %q, %v", want, content, err)
		}
	}
	for _, absent := range []string{"init.previous.log", "side.previous.log"} {
		if _, err := os.Stat(filepath.Join(dir, "logs", "dev", "web", absent)); err == nil {
			t.Errorf("%s exists", absent)
		}
	}
}

func TestADumpHoldsTheConsoleOfEveryContainerTheSuiteStartedAndTheJournalOfThoseThatHaveOne(t *testing.T) {
	bin, calls := t.TempDir(), filepath.Join(t.TempDir(), "calls")
	script := `#!/bin/sh
echo "$*" >> ` + calls + `
case "$1" in
ps) printf 'e2e-tenant-node-1\ne2e-dns\n' ;;
logs) echo "console of $2" ;;
exec) if [ "$2" = e2e-dns ]; then echo "no journalctl" >&2; exit 126; fi; echo "journal of $2" ;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	dir := t.TempDir()
	if err := dumpContainers(t.Context(), "lodia-e2e-x", dir); err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(calls)
	if err != nil || !strings.HasPrefix(string(log), "ps --all --no-trunc --format {{.Names}} --filter label=io.lodia.e2e.cluster=lodia-e2e-x\n") {
		t.Fatalf("docker was called as %q, %v", log, err)
	}
	for path, want := range map[string]string{
		"containers/e2e-tenant-node-1/console.log": "console of e2e-tenant-node-1\n",
		"containers/e2e-tenant-node-1/journal.log": "journal of e2e-tenant-node-1\n",
		"containers/e2e-dns/console.log":           "console of e2e-dns\n",
	} {
		if got, err := os.ReadFile(filepath.Join(dir, path)); err != nil || string(got) != want {
			t.Errorf("%s is %q, %v, want %q", path, got, err, want)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "containers", "e2e-dns", "journal.log")); err == nil {
		t.Error("the DNS container has a journal")
	}
}

func TestADumpLeavesOutWhatDockerPrintsThatIsNoContainerName(t *testing.T) {
	calls := fakeDocker(t, `case "$1" in
ps) echo "WARNING: the daemon is slow" >&2; printf '%s\n' e2e-dns -v a/b ;;
logs) echo "console of $2" ;;
exec) exit 126 ;;
esac
`)
	dir := t.TempDir()
	if err := dumpContainers(t.Context(), "lodia-e2e-x", dir); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "containers"))
	if err != nil || len(entries) != 1 || entries[0].Name() != "e2e-dns" {
		t.Fatalf("the dump holds %v, %v, want e2e-dns alone", entries, err)
	}
	for _, call := range dockerCalls(t, calls) {
		if strings.Contains(call, "WARNING") || strings.Contains(call, "-v") || strings.Contains(call, "a/b") {
			t.Errorf("docker was called as %q", call)
		}
	}
}
