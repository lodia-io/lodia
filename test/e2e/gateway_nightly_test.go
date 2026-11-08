// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && gateway

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/distribution/reference"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	g6Client        = "g6client"
	g6Bound         = 5.0
	g6ServiceSuffix = "-g6-direct"
)

type g6Path struct {
	Name       string  `json:"name"`
	P50Ms      float64 `json:"p50Ms"`
	P99Ms      float64 `json:"p99Ms"`
	DeltaP99Ms float64 `json:"deltaP99Ms"`
	CILowMs    float64 `json:"ciLowMs"`
	CIHighMs   float64 `json:"ciHighMs"`
	Errors     int64   `json:"errors"`
	Dials      int64   `json:"dials"`
}

type g6Mix struct {
	Name  string   `json:"name"`
	N     int      `json:"n"`
	Paths []g6Path `json:"paths"`
}

func TestGatewayOverhead(t *testing.T) {
	var (
		k8s    client.WithWatch
		access *tenantAccess
		node   string
		image  reference.NamedTagged
	)
	artifacts := t.ArtifactDir()
	key := client.ObjectKey{Namespace: "team-g6", Name: controlPlane}
	const class = "e2e-gateway-g6"
	gateway := tenantsGateway(key.Namespace)
	f := features.New("G6: the Gateway's and the tunnel's latency over a direct path to one API server replica").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			createTenant(ctx, t, k8s, key, class, behindTheGateway(key.Namespace))
			createGateway(ctx, t, k8s, gateway)
			built, err := build(ctx, cfg.KubeconfigFile(), "./test/"+g6Client)
			if err != nil {
				t.Fatal(err)
			}
			image = built
			return ctx
		}).
		Assess("the tenant is Ready with AddonsAvailable, one replica and one joined node", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 5*time.Minute, "Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{})
			})
			kubeconfig := adminKubeconfig(ctx, t, k8s, key)
			throughTheGateway(kubeconfig)
			access = connect(t, kubeconfig)
			nodeImages = []reference.NamedTagged{tunnelImage(ctx, t, access)}
			node = joinNodes(ctx, t, cfg, k8s, key, access, 1)[0]
			loadImage(ctx, t, cfg.KubeconfigFile(), node, image)
			until(ctx, t, 3*time.Minute, "CoreDNS is available", func(ctx context.Context) error {
				return coreDNSAvailable(ctx, access.client)
			})
			until(ctx, t, 3*time.Minute, "AddonsAvailable=True", func(ctx context.Context) error {
				_, err := conditionAt(ctx, k8s, key, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable)
				return err
			})
			create(ctx, t, k8s, g6DirectService(key))
			t.Log("the tenant idles for 30 seconds")
			time.Sleep(30 * time.Second)
			return ctx
		}).
		Assess("Δp99 is under 5 ms for the Gateway and for the tunnel, on reused and on new connections, with no error", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cp := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, key, cp); err != nil {
				t.Fatal(err)
			}
			host := cp.Status.Endpoint.APIServer.Host
			service := &corev1.Service{}
			if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: key.Name + g6ServiceSuffix}, service); err != nil {
				t.Fatal(err)
			}
			direct := fmt.Sprintf("https://%s:%d", gatewayAddress, service.Spec.Ports[0].NodePort)
			through := "https://" + gatewayAddress + ":443"
			kubernetesService := &corev1.Service{}
			if err := access.client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceDefault, Name: "kubernetes"}, kubernetesService); err != nil {
				t.Fatal(err)
			}
			tunnel := "https://" + net.JoinHostPort(kubernetesService.Spec.ClusterIP, "443")
			pod := g6Pod(image, host, direct, through, tunnel, node)
			create(ctx, t, access.client, pod)
			clientset := kubernetes.NewForConfigOrDie(access.cfg)
			until(ctx, t, 15*time.Minute, "pod "+g6Client+" completes", func(ctx context.Context) error {
				p, err := clientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				if p.Status.Phase != corev1.PodSucceeded {
					return fmt.Errorf("the pod is %s", p.Status.Phase)
				}
				return nil
			})
			raw, err := clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{}).DoRaw(ctx)
			if err != nil {
				t.Fatal(err)
			}
			result, ok := g6Result(string(raw))
			if !ok {
				t.Fatalf("the pod's log has no result:\n%s", raw)
			}
			table := g6Table(ctx, t, access.client, node, result)
			t.Logf("G6:\n%s", table)
			if err := os.WriteFile(filepath.Join(artifacts, "g6-results.txt"), table, 0o644); err != nil {
				t.Fatal(err)
			}
			for _, m := range result {
				for _, p := range m.Paths {
					if p.Errors != 0 {
						t.Errorf("%s, %s: %d requests failed", m.Name, p.Name, p.Errors)
					}
					if strings.HasPrefix(m.Name, "M1") && p.Dials != 1 {
						t.Errorf("%s, %s: %d dials on a reused connection, want one", m.Name, p.Name, p.Dials)
					}
					if p.Name != "direct" && p.DeltaP99Ms >= g6Bound {
						t.Errorf("%s, %s: Δp99 %.2f ms, want under %.0f ms", m.Name, p.Name, p.DeltaP99Ms, g6Bound)
					}
				}
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if k8s == nil {
				return ctx
			}
			if err := client.IgnoreNotFound(k8s.Delete(ctx, gateway)); err != nil {
				t.Error(err)
			}
			closeTenant(ctx, t, k8s, key, class)
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func g6DirectService(key client.ObjectKey) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name + g6ServiceSuffix},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: map[string]string{lodiav1alpha1.ControlPlaneLabel: key.Name, lodiav1alpha1.ComponentLabel: lodiav1alpha1.APIServerComponent},
			Ports:    []corev1.ServicePort{{Name: "https", Port: 6443, TargetPort: intstr.FromInt32(6443), Protocol: corev1.ProtocolTCP}},
		},
	}
}

func g6Pod(image reference.NamedTagged, host, direct, through, tunnel, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: g6Client},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			NodeName:      node,
			Containers: []corev1.Container{{
				Name:            g6Client,
				Image:           image.String(),
				ImagePullPolicy: corev1.PullNever,
				Args:            []string{"--direct=" + direct, "--gateway=" + through, "--tunnel=" + tunnel, "--server-name=" + host},
			}},
		},
	}
}

func g6Result(log string) ([]g6Mix, bool) {
	var result []g6Mix
	for line := range strings.Lines(log) {
		if body, ok := strings.CutPrefix(strings.TrimSpace(line), "g6result "); ok {
			if err := json.Unmarshal([]byte(body), &result); err != nil {
				return nil, false
			}
			return result, true
		}
	}
	return nil, false
}

func g6Table(ctx context.Context, t *testing.T, c client.Client, nodeName string, result []g6Mix) []byte {
	t.Helper()
	node := &corev1.Node{}
	if err := c.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		t.Fatal(err)
	}
	info, capacity := node.Status.NodeInfo, node.Status.Capacity
	var out bytes.Buffer
	fmt.Fprintf(&out, "G6 at %s, from a pod on %s (kubelet %s, %s, kernel %s, %s CPUs); one API server replica, one node\n", time.Now().UTC().Format(time.RFC3339), nodeName, info.KubeletVersion, info.ContainerRuntimeVersion, info.KernelVersion, capacity.Cpu())
	fmt.Fprintf(&out, "p99 by nearest rank; Δ against the direct path, with a 95%% bootstrap interval from 1000 resamples of the pairs, fixed seed\n\n")
	var rows bytes.Buffer
	fmt.Fprintln(&rows, "mix\tpath\tn\tp50 (ms)\tp99 (ms)\tΔp99 (ms)\t95% interval (ms)\terrors\tdials")
	for _, m := range result {
		for _, p := range m.Paths {
			delta, interval := "—", "—"
			if p.Name != "direct" {
				delta = strconv.FormatFloat(p.DeltaP99Ms, 'f', 2, 64)
				interval = fmt.Sprintf("[%.2f, %.2f]", p.CILowMs, p.CIHighMs)
			}
			fmt.Fprintf(&rows, "%s\t%s\t%d\t%.2f\t%.2f\t%s\t%s\t%d\t%d\n", m.Name, p.Name, m.N, p.P50Ms, p.P99Ms, delta, interval, p.Errors, p.Dials)
		}
	}
	w := tabwriter.NewWriter(&out, 0, 0, 2, ' ', 0)
	if _, err := w.Write(rows.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func TestGatewayConformance(t *testing.T) {
	var (
		k8s    client.WithWatch
		access *tenantAccess
	)
	artifacts := t.ArtifactDir()
	key := client.ObjectKey{Namespace: "team-gw-conformance", Name: controlPlane}
	const class = "e2e-gateway-conformance"
	gateway := tenantsGateway(key.Namespace)
	f := features.New("the Kubernetes conformance suite against a tenant behind the Gateway").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			createTenant(ctx, t, k8s, key, class, behindTheGateway(key.Namespace))
			createGateway(ctx, t, k8s, gateway)
			return ctx
		}).
		Assess("the ControlPlane is Ready behind the Gateway, two nodes join and turn Ready, and CoreDNS is available", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 5*time.Minute, "Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{})
			})
			kubeconfig := adminKubeconfig(ctx, t, k8s, key)
			throughTheGateway(kubeconfig)
			access = connect(t, kubeconfig)
			nodeImages = []reference.NamedTagged{tunnelImage(ctx, t, access)}
			joinNodes(ctx, t, cfg, k8s, key, access, 2)
			until(ctx, t, 3*time.Minute, "CoreDNS is available", func(ctx context.Context) error {
				return coreDNSAvailable(ctx, access.client)
			})
			return ctx
		}).
		Assess("hydrophone runs the conformance suite in the tenant, and it passes", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			args := []string{"--conformance-image", "registry.k8s.io/conformance:" + kindNodeVersion(t)}
			if focus := os.Getenv("CONFORMANCE_FOCUS"); focus != "" {
				args = append(args, "--focus", focus)
			} else {
				args = append(args, "--conformance")
			}
			runHydrophone(ctx, t, access, artifacts, args...)
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if k8s == nil {
				return ctx
			}
			if err := client.IgnoreNotFound(k8s.Delete(ctx, gateway)); err != nil {
				t.Error(err)
			}
			closeTenant(ctx, t, k8s, key, class)
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}
