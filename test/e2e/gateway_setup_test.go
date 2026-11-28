// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && (gateway || hold)

package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/third_party/helm"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	gatewayNamespace      = "gateway"
	gatewayName           = "tenants"
	gatewayListener       = "tls"
	gatewayClassName      = "eg"
	envoyGatewayNamespace = "envoy-gateway-system"
	envoyGatewayCRDs      = "oci://mirror.gcr.io/envoyproxy/gateway-crds-helm@sha256:a7e5267358e3d46e90b9e1a626f75a2ea206662c520de440cab2a7704e8cd6fd"
	envoyGatewayChart     = "oci://mirror.gcr.io/envoyproxy/gateway-helm@sha256:be1b8f163a338f94e7d6532336c08d6856fc70a0b790ac14a2826a54d23e0dd3"
	envoyGatewayImage     = "mirror.gcr.io/envoyproxy/gateway:v1.9.3@sha256:192fc9bdadb6d2462e8cf7bd702ce3dd9648f3310c6038787448a1f509b7cd9f"
	envoyImage            = "mirror.gcr.io/envoyproxy/envoy:distroless-v1.39.5@sha256:9d84a586119a27b50437dbb01eccd4722832741539b1ed53c8e9d6da6067a6e0"
	e2eFieldOwner         = "lodia-e2e"
)

var gatewayAddress string

func init() {
	beforeLodia = append(beforeLodia, installGateway)
}

func tenantsGateway(namespaces ...string) *gatewayv1.Gateway {
	return &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Namespace: gatewayNamespace, Name: gatewayName},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: gatewayClassName,
			Listeners: []gatewayv1.Listener{{
				Name:     gatewayListener,
				Port:     443,
				Protocol: gatewayv1.TLSProtocolType,
				Hostname: new(gatewayv1.Hostname("*." + gatewayZone)),
				TLS:      &gatewayv1.ListenerTLSConfig{Mode: new(gatewayv1.TLSModePassthrough)},
				AllowedRoutes: &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{
					From: new(gatewayv1.NamespacesFromSelector),
					Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
						Key: corev1.LabelMetadataName, Operator: metav1.LabelSelectorOpIn, Values: namespaces,
					}}},
				}},
			}},
		},
	}
}

func createGateway(ctx context.Context, t *testing.T, k8s client.Client, gateway *gatewayv1.Gateway) {
	t.Helper()
	gateway.Spec.Addresses = []gatewayv1.GatewaySpecAddress{{Type: new(gatewayv1.IPAddressType), Value: gatewayAddress}}
	create(ctx, t, k8s, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: gatewayNamespace}})
	create(ctx, t, k8s, gateway)
	until(ctx, t, 3*time.Minute, "Gateway "+gatewayNamespace+"/"+gatewayName+" is Programmed at "+gatewayAddress, func(ctx context.Context) error {
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(gateway), gateway); err != nil {
			return err
		}
		if c := meta.FindStatusCondition(gateway.Status.Conditions, string(gatewayv1.GatewayConditionProgrammed)); c == nil || c.Status != metav1.ConditionTrue || c.ObservedGeneration != gateway.Generation {
			return fmt.Errorf("Programmed is %+v", c)
		}
		return nil
	})
}

func hostnamesOf(namespace string) lodiav1alpha1.GatewayHostnames {
	return lodiav1alpha1.GatewayHostnames{
		APIServer:    lodiav1alpha1.Hostname("api." + namespace + "." + gatewayZone),
		Konnectivity: lodiav1alpha1.Hostname("konnectivity." + namespace + "." + gatewayZone),
		Tunnel:       lodiav1alpha1.Hostname("tunnel." + namespace + "." + gatewayZone),
	}
}

func behindTheGateway(namespace string) func(*lodiav1alpha1.ControlPlaneSpec) {
	return func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.Endpoint = lodiav1alpha1.Endpoint{Gateway: lodiav1alpha1.GatewayEndpoint{
			ParentRef: lodiav1alpha1.ParentReference{Name: gatewayName, Namespace: gatewayNamespace, SectionName: gatewayListener},
			Hostnames: hostnamesOf(namespace),
		}}
	}
}

func throughTheGateway(kubeconfig *clientcmdapi.Config) {
	for _, cluster := range kubeconfig.Clusters {
		server, err := url.Parse(cluster.Server)
		if err != nil {
			continue
		}
		cluster.TLSServerName = server.Hostname()
		cluster.Server = "https://" + net.JoinHostPort(gatewayAddress, server.Port())
	}
}

func operatorPods(ctx context.Context, t *testing.T, k8s client.Client) []string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := k8s.List(ctx, pods, client.InNamespace(operatorNamespace), client.MatchingLabels(operatorDeployment(ctx, t, k8s).Spec.Selector.MatchLabels)); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, pod := range pods.Items {
		var restarts int32
		for _, s := range pod.Status.ContainerStatuses {
			restarts += s.RestartCount
		}
		out = append(out, fmt.Sprintf("%s (UID %s) %d restarts", pod.Name, pod.UID, restarts))
	}
	slices.Sort(out)
	return out
}

func installGateway(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
	start := time.Now()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, gatewayv1.Install} {
		if err := add(scheme); err != nil {
			return ctx, err
		}
	}
	c, err := client.New(cfg.Client().RESTConfig(), client.Options{Scheme: scheme})
	if err != nil {
		return ctx, err
	}
	if gatewayAddress, err = nodeAddress(ctx, c); err != nil {
		return ctx, err
	}
	module, err := exec.CommandContext(ctx, "go", "list", "-m", "-f", "{{.Dir}}", "sigs.k8s.io/gateway-api").Output()
	if err != nil {
		return ctx, fmt.Errorf("find the Gateway API module: %w", err)
	}
	files, err := filepath.Glob(filepath.Join(strings.TrimSpace(string(module)), "config", "crd", "standard", "*.yaml"))
	if err != nil {
		return ctx, err
	}
	if len(files) == 0 {
		return ctx, errors.New("the Gateway API module has no standard CRDs")
	}
	for _, file := range files {
		manifest, err := os.ReadFile(file)
		if err != nil {
			return ctx, err
		}
		if err := applyManifests(ctx, c, manifest); err != nil {
			return ctx, fmt.Errorf("%s: %w", file, err)
		}
	}
	crds, err := exec.CommandContext(ctx, "helm", "template", "eg-crds", envoyGatewayCRDs, "--set", "crds.envoyGateway.enabled=true").Output()
	if err != nil {
		return ctx, fmt.Errorf("render Envoy Gateway's CRDs: %w", err)
	}
	if err := applyManifests(ctx, c, crds); err != nil {
		return ctx, err
	}
	cluster, err := kindCluster(cfg.KubeconfigFile())
	if err != nil {
		return ctx, err
	}
	if err := pull(ctx, cluster, envoyGatewayImage, envoyImage); err != nil {
		return ctx, err
	}
	err = helm.New(cfg.KubeconfigFile()).RunUpgrade(
		helm.WithName("eg"), helm.WithChart(envoyGatewayChart), helm.WithNamespace(envoyGatewayNamespace), helm.WithContext(ctx),
		helm.WithArgs("--install", "--create-namespace", "--set", "crds.enabled=false",
			"--set", "global.images.envoyGateway.image="+envoyGatewayImage, "--set", "global.images.envoyProxy.image="+envoyImage),
		helm.WithWait(), helm.WithTimeout("5m"))
	if err != nil {
		return ctx, fmt.Errorf("install Envoy Gateway: %w", err)
	}
	class := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: gatewayClassName},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: "gateway.envoyproxy.io/gatewayclass-controller"},
	}
	if err := c.Create(ctx, class); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctx, err
	}
	err = eventually(ctx, 2*time.Minute, "GatewayClass "+gatewayClassName+" is Accepted", func(ctx context.Context) error {
		if err := c.Get(ctx, client.ObjectKeyFromObject(class), class); err != nil {
			return err
		}
		if !meta.IsStatusConditionTrue(class.Status.Conditions, string(gatewayv1.GatewayClassConditionStatusAccepted)) {
			return fmt.Errorf("its conditions are %+v", class.Status.Conditions)
		}
		return nil
	})
	if err != nil {
		return ctx, err
	}
	if nodeDNS, err = startDNS(ctx, cluster); err != nil {
		return ctx, err
	}
	if err := forwardZone(ctx, c, nodeDNS); err != nil {
		return ctx, err
	}
	fmt.Printf("Envoy Gateway v1.9.3 runs on the Gateway API CRDs of %s, GatewayClass %s is Accepted, and %s answers *.%s with %s, after %s\n",
		strings.TrimSpace(string(module)), gatewayClassName, nodeDNS, gatewayZone, gatewayAddress, time.Since(start).Round(time.Second))
	return ctx, nil
}

func nodeAddress(ctx context.Context, c client.Client) (string, error) {
	nodes := &corev1.NodeList{}
	if err := c.List(ctx, nodes); err != nil {
		return "", err
	}
	for _, node := range nodes.Items {
		for _, address := range node.Status.Addresses {
			if ip := net.ParseIP(address.Address); address.Type == corev1.NodeInternalIP && ip.To4() != nil {
				return address.Address, nil
			}
		}
	}
	return "", errors.New("no management node has an IPv4 internal address")
}

func applyManifests(ctx context.Context, c client.Client, manifest []byte) error {
	decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)
	for {
		obj := &unstructured.Unstructured{}
		if err := decoder.Decode(&obj.Object); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if len(obj.Object) == 0 {
			continue
		}
		if err := c.Apply(ctx, client.ApplyConfigurationFromUnstructured(obj), client.FieldOwner(e2eFieldOwner), client.ForceOwnership); err != nil {
			return fmt.Errorf("apply %s %s: %w", obj.GetKind(), obj.GetName(), err)
		}
	}
}

func startDNS(ctx context.Context, cluster string) (string, error) {
	corefile := fmt.Sprintf("%s:53 {\n    template IN A {\n        answer \"{{ .Name }} 60 IN A %s\"\n    }\n    template IN ANY\n}\n.:53 {\n    forward . /etc/resolv.conf\n}\n", gatewayZone, gatewayAddress)
	return runDNS(ctx, cluster, map[string]string{"/Corefile": corefile}, "api.probe."+gatewayZone, gatewayAddress)
}
