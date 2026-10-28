// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && conformance

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	conformanceTenant = "team-conformance"
	conformanceClass  = "e2e-conformance"
	conformanceNodes  = 2
	hydrophone        = "sigs.k8s.io/hydrophone@v0.8.0"
	idle              = 3 * time.Minute
	window            = 5 * time.Minute
	cpuUsage          = "container_cpu_usage_seconds_total"
	workingSet        = "container_memory_working_set_bytes"
)

func TestConformance(t *testing.T) {
	var (
		k8s     client.Client
		access  *tenantAccess
		version string
	)
	artifacts := t.ArtifactDir()
	key := client.ObjectKey{Namespace: conformanceTenant, Name: controlPlane}
	f := features.New("the Kubernetes conformance suite against a tenant with two nodes").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			createTenant(ctx, t, k8s, key, conformanceClass)
			version = kindNodeVersion(t)
			return ctx
		}).
		Assess("the ControlPlane is Ready, two nodes join and turn Ready, and CoreDNS is available", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 3*time.Minute, "Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{})
			})
			access = openTenant(ctx, t, k8s, key)
			joinNodes(ctx, t, cfg, k8s, key, access, conformanceNodes)
			until(ctx, t, 3*time.Minute, "CoreDNS is available", func(ctx context.Context) error {
				coreDNS := &appsv1.Deployment{}
				if err := access.client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: "coredns"}, coreDNS); err != nil {
					return err
				}
				if s, want := coreDNS.Status, ptr.Deref(coreDNS.Spec.Replicas, 1); s.ObservedGeneration != coreDNS.Generation || s.UpdatedReplicas != want || s.AvailableReplicas != want {
					return fmt.Errorf("%d of %d replicas available", s.AvailableReplicas, want)
				}
				return nil
			})
			return ctx
		}).
		Assess("the idle control plane's CPU and memory, per container", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			t.Logf("the tenant idles for %s", idle)
			time.Sleep(idle)
			list := &corev1.PodList{}
			if err := k8s.List(ctx, list, client.InNamespace(key.Namespace)); err != nil {
				t.Fatal(err)
			}
			pods := slices.DeleteFunc(list.Items, func(pod corev1.Pod) bool { return !pod.DeletionTimestamp.IsZero() || running(&pod) != nil })
			management := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
			first := containerUsage(ctx, t, management, pods)
			time.Sleep(window)
			last := containerUsage(ctx, t, management, pods)
			var table bytes.Buffer
			fmt.Fprintf(&table, "ControlPlane %s, Kubernetes %s, idle with %d nodes joined, at %s:\n", key, version, conformanceNodes, time.Now().UTC().Format(time.RFC3339))
			fmt.Fprintf(&table, "CPU is the rate of %s over the %s before, memory %s then,\n", cpuUsage, window, workingSet)
			fmt.Fprintf(&table, "from the resource metrics of the kubelet on %s.\n\n", strings.Join(kubelets(ctx, t, management, pods), "; "))
			var rows bytes.Buffer
			fmt.Fprintln(&rows, "container\tpod\tCPU (millicores)\tmemory (MiB)")
			var cpu, memory float64
			for _, name := range slices.Sorted(maps.Keys(last)) {
				before, after := first[name], last[name]
				if after.at <= before.at || after.cpu < before.cpu {
					t.Fatalf("%s: %s went from %g at %d to %g at %d, a restart in the window", name, cpuUsage, before.cpu, before.at, after.cpu, after.at)
				}
				rate := (after.cpu - before.cpu) / (float64(after.at-before.at) / 1000)
				pod, container, _ := strings.Cut(name, "/")
				fmt.Fprintf(&rows, "%s\t%s\t%.1f\t%.1f\n", container, pod, rate*1000, after.memory/(1<<20))
				cpu += rate
				memory += after.memory
			}
			fmt.Fprintf(&rows, "sum\t\t%.1f\t%.1f\n", cpu*1000, memory/(1<<20))
			w := tabwriter.NewWriter(&table, 0, 0, 2, ' ', 0)
			if _, err := w.Write(rows.Bytes()); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			t.Logf("the idle control plane:\n%s", table.String())
			if err := os.WriteFile(filepath.Join(artifacts, "idle-control-plane.txt"), table.Bytes(), 0o644); err != nil {
				t.Fatal(err)
			}
			return ctx
		}).
		Assess("hydrophone runs the conformance suite in the tenant, and it passes", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
			if err := clientcmd.WriteToFile(access.kubeconfig, kubeconfig); err != nil {
				t.Fatal(err)
			}
			args := []string{"run", hydrophone, "--kubeconfig", kubeconfig, "--conformance-image", "registry.k8s.io/conformance:" + version,
				"--output-dir", artifacts, "--parallel", "4", "--disable-progress-status"}
			if focus := os.Getenv("CONFORMANCE_FOCUS"); focus != "" {
				args = append(args, "--focus", focus)
			} else {
				args = append(args, "--conformance")
			}
			run := exec.CommandContext(ctx, "go", args...)
			run.Stdout, run.Stderr = os.Stdout, os.Stderr
			start := time.Now()
			err := run.Run()
			t.Logf("go %s ran for %s", strings.Join(args, " "), time.Since(start).Round(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			closeTenant(ctx, t, k8s, key, conformanceClass)
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

type usage struct {
	cpu    float64
	at     int64
	memory float64
}

func containerUsage(ctx context.Context, t *testing.T, c kubernetes.Interface, pods []corev1.Pod) map[string]usage {
	t.Helper()
	found := map[string]usage{}
	for _, node := range nodesOf(pods) {
		raw, err := c.CoreV1().RESTClient().Get().Resource("nodes").Name(node).SubResource("proxy").Suffix("metrics", "resource").DoRaw(ctx)
		if err != nil {
			t.Fatal(err)
		}
		parser := expfmt.NewTextParser(model.UTF8Validation)
		families, err := parser.TextToMetricFamilies(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		for _, family := range []string{cpuUsage, workingSet} {
			for _, m := range families[family].GetMetric() {
				labels := map[string]string{}
				for _, l := range m.GetLabel() {
					labels[l.GetName()] = l.GetValue()
				}
				if labels["namespace"] != pods[0].Namespace {
					continue
				}
				name := labels["pod"] + "/" + labels["container"]
				u := found[name]
				if family == cpuUsage {
					u.cpu, u.at = m.GetCounter().GetValue(), m.GetTimestampMs()
				} else {
					u.memory = m.GetGauge().GetValue()
				}
				found[name] = u
			}
		}
	}
	usages := map[string]usage{}
	for _, pod := range pods {
		containers := slices.Clone(pod.Spec.Containers)
		for _, sidecar := range pod.Spec.InitContainers {
			if ptr.Deref(sidecar.RestartPolicy, "") == corev1.ContainerRestartPolicyAlways {
				containers = append(containers, sidecar)
			}
		}
		for _, container := range containers {
			name := pod.Name + "/" + container.Name
			u, ok := found[name]
			if !ok || u.at == 0 || u.memory == 0 {
				t.Fatalf("the kubelet on %s reports no %s or %s for %s: %+v", pod.Spec.NodeName, cpuUsage, workingSet, name, u)
			}
			usages[name] = u
		}
	}
	return usages
}

func nodesOf(pods []corev1.Pod) []string {
	var nodes []string
	for _, pod := range pods {
		if !slices.Contains(nodes, pod.Spec.NodeName) {
			nodes = append(nodes, pod.Spec.NodeName)
		}
	}
	return nodes
}

func kubelets(ctx context.Context, t *testing.T, c kubernetes.Interface, pods []corev1.Pod) []string {
	t.Helper()
	var described []string
	for _, name := range nodesOf(pods) {
		node, err := c.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		info, capacity := node.Status.NodeInfo, node.Status.Capacity
		described = append(described, fmt.Sprintf("%s (kubelet %s, %s, %s/%s, kernel %s, %s CPUs, %.1f GiB)",
			name, info.KubeletVersion, info.ContainerRuntimeVersion, info.OperatingSystem, info.Architecture, info.KernelVersion,
			capacity.Cpu(), float64(capacity.Memory().Value())/(1<<30)))
	}
	return described
}
