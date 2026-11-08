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

	"github.com/distribution/reference"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/third_party/helm"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	kindcluster "sigs.k8s.io/kind/pkg/cluster"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	gatewayZone           = "lodia.test"
	gatewayNamespace      = "gateway"
	gatewayName           = "tenants"
	gatewayListener       = "tls"
	gatewayClassName      = "eg"
	envoyGatewayNamespace = "envoy-gateway-system"
	envoyGatewayCRDs      = "oci://mirror.gcr.io/envoyproxy/gateway-crds-helm@sha256:a7e5267358e3d46e90b9e1a626f75a2ea206662c520de440cab2a7704e8cd6fd"
	envoyGatewayChart     = "oci://mirror.gcr.io/envoyproxy/gateway-helm@sha256:be1b8f163a338f94e7d6532336c08d6856fc70a0b790ac14a2826a54d23e0dd3"
	envoyGatewayImage     = "mirror.gcr.io/envoyproxy/gateway:v1.9.3@sha256:192fc9bdadb6d2462e8cf7bd702ce3dd9648f3310c6038787448a1f509b7cd9f"
	envoyImage            = "mirror.gcr.io/envoyproxy/envoy:distroless-v1.39.5@sha256:9d84a586119a27b50437dbb01eccd4722832741539b1ed53c8e9d6da6067a6e0"
	gatewayDNSImage       = "registry.k8s.io/coredns/coredns:v1.13.1@sha256:9b9128672209474da07c91439bf15ed704ae05ad918dd6454e5b6ae14e35fee6"
	apiserverProxy        = "apiserver-proxy"
	e2eFieldOwner         = "lodia-e2e"
)

var gatewayAddress string

func init() {
	beforeLodia = append(beforeLodia, installGateway)
	afterNodes = append(afterNodes, removeDNS)
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
		spec.Endpoint = lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
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

func tunnelImage(ctx context.Context, t *testing.T, access *tenantAccess) reference.NamedTagged {
	t.Helper()
	ds := &appsv1.DaemonSet{}
	if err := access.client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: apiserverProxy}, ds); err != nil {
		t.Fatal(err)
	}
	named, err := reference.ParseNamed(ds.Spec.Template.Spec.Containers[0].Image)
	if err != nil {
		t.Fatal(err)
	}
	tagged, ok := named.(reference.NamedTagged)
	if !ok {
		t.Fatalf("apiserver-proxy's image %s has no tag", named)
	}
	return tagged
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

func pull(ctx context.Context, cluster string, images ...string) error {
	nodes, err := kindcluster.NewProvider().ListNodes(cluster)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		for _, image := range images {
			var last error
			for attempt := range 5 {
				if _, last = docker(ctx, "exec", node.String(), "crictl", "pull", image); last == nil {
					break
				}
				time.Sleep(time.Duration(attempt+1) * 5 * time.Second)
			}
			if last != nil {
				return fmt.Errorf("pull %s into %s: %w", image, node, last)
			}
		}
	}
	return nil
}

func dnsContainer(cluster string) string {
	return cluster + "-dns"
}

func startDNS(ctx context.Context, cluster string) (string, error) {
	name := dnsContainer(cluster)
	corefile := filepath.Join(os.TempDir(), name+".Corefile")
	if err := os.WriteFile(corefile, []byte(fmt.Sprintf("%s:53 {\n    template IN A {\n        answer \"{{ .Name }} 60 IN A %s\"\n    }\n    template IN ANY\n}\n.:53 {\n    forward . /etc/resolv.conf\n}\n", gatewayZone, gatewayAddress)), 0o644); err != nil {
		return "", err
	}
	defer func() { _ = os.Remove(corefile) }()
	for _, args := range [][]string{
		{"rm", "--force", "--volumes", name},
		{"create", "--name", name, "--network", "kind", "--restart=on-failure:3", gatewayDNSImage, "-conf", "/Corefile"},
		{"cp", corefile, name + ":/Corefile"},
		{"start", name},
	} {
		if _, err := docker(ctx, args...); err != nil {
			return "", err
		}
	}
	out, err := docker(ctx, "inspect", "--format", `{{(index .NetworkSettings.Networks "kind").IPAddress}}`, name)
	if err != nil {
		return "", err
	}
	address := strings.TrimSpace(out)
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(address, "53"))
	}}
	return address, eventually(ctx, time.Minute, "the DNS container "+name+" at "+address+" answers", func(ctx context.Context) error {
		zone, err := resolver.LookupHost(ctx, "api.probe."+gatewayZone)
		if err != nil {
			return err
		}
		if !slices.Equal(zone, []string{gatewayAddress}) {
			return fmt.Errorf("api.probe.%s is %v", gatewayZone, zone)
		}
		_, err = resolver.LookupHost(ctx, "registry.k8s.io")
		return err
	})
}

func forwardZone(ctx context.Context, c client.Client, dns string) error {
	coreDNS := &corev1.ConfigMap{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: "coredns"}, coreDNS); err != nil {
		return err
	}
	kind, _, _ := strings.Cut(coreDNS.Data["Corefile"], gatewayZone+":53 {")
	coreDNS.Data["Corefile"] = strings.TrimRight(kind, "\n") + fmt.Sprintf("\n%s:53 {\n    forward . %s\n}\n", gatewayZone, dns)
	if err := c.Update(ctx, coreDNS); err != nil {
		return err
	}
	deployment := &appsv1.Deployment{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(coreDNS), deployment); err != nil {
		return err
	}
	patch := client.MergeFrom(deployment.DeepCopy())
	if deployment.Spec.Template.Annotations == nil {
		deployment.Spec.Template.Annotations = map[string]string{}
	}
	deployment.Spec.Template.Annotations["lodia.io/e2e-zone"] = gatewayZone + " " + dns
	if err := c.Patch(ctx, deployment, patch); err != nil {
		return err
	}
	return eventually(ctx, 2*time.Minute, "the management cluster's CoreDNS forwards "+gatewayZone+" to "+dns, func(ctx context.Context) error {
		return coreDNSAvailable(ctx, c)
	})
}

func eventually(ctx context.Context, timeout time.Duration, what string, check func(context.Context) error) error {
	var last error
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		err := check(ctx)
		if ctx.Err() == nil {
			last = err
		}
		return err == nil, nil
	})
	if err != nil {
		return fmt.Errorf("%s: %w", what, errors.Join(err, last))
	}
	return nil
}

func removeDNS(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
	cluster, err := kindCluster(cfg.KubeconfigFile())
	if err != nil {
		return ctx, err
	}
	_, err = docker(ctx, "rm", "--force", "--volumes", dnsContainer(cluster))
	return ctx, err
}
