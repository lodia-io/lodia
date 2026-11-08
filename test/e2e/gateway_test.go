// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && gateway

package e2e

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/distribution/reference"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
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
	"sigs.k8s.io/e2e-framework/pkg/features"
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
	envoyAdminPort        = 19000
	ldsRejected           = "envoy_listener_manager_lds_update_rejected"
	ldsSuccess            = "envoy_listener_manager_lds_update_success"
	gatewayDNSImage       = "registry.k8s.io/coredns/coredns:v1.13.1@sha256:9b9128672209474da07c91439bf15ed704ae05ad918dd6454e5b6ae14e35fee6"
	apiserverProxy        = "apiserver-proxy"
	e2eFieldOwner         = "lodia-e2e"
)

var (
	gatewayAddress string
	ownRoutes      = []string{controlPlane + "-konnectivity", controlPlane + "-kube-apiserver", controlPlane + "-tunnel"}
)

func init() {
	beforeLodia = append(beforeLodia, installGateway)
	afterNodes = append(afterNodes, removeDNS)
}

func TestGatewayGate(t *testing.T) {
	var (
		k8s      client.WithWatch
		access   *tenantAccess
		node     string
		loop     reference.NamedTagged
		loops    []apiLoopRun
		operator []string
		envoy    envoyStats
	)
	a := client.ObjectKey{Namespace: "team-gw-a", Name: controlPlane}
	b := client.ObjectKey{Namespace: "team-gw-b", Name: controlPlane}
	c := client.ObjectKey{Namespace: "team-gw-c", Name: controlPlane}
	classes := map[client.ObjectKey]string{a: "e2e-gateway-a", b: "e2e-gateway-b", c: "e2e-gateway-c"}
	gateway := tenantsGateway(a.Namespace, b.Namespace, c.Namespace)
	f := features.New("the gateway gate on kind: tenants behind Envoy Gateway by SNI, a node joined by hostname, pods reaching their API through apiserver-proxy and the tunnel").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			operator = operatorPods(ctx, t, k8s)
			createTenant(ctx, t, k8s, a, classes[a], behindTheGateway(a.Namespace))
			image, err := build(ctx, cfg.KubeconfigFile(), "./test/"+apiLoop)
			if err != nil {
				t.Fatal(err)
			}
			loop = image
			return ctx
		}).
		Assess("without its Gateway a ControlPlane reports RoutesReady False, GatewayNotFound, and publishes no endpoint", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 2*time.Minute, "RoutesReady=False, GatewayNotFound, with APIServerReady waiting for the endpoint", func(ctx context.Context) error {
				cp, err := conditionAt(ctx, k8s, a, lodiav1alpha1.ConditionRoutesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonGatewayNotFound)
				if err != nil {
					return err
				}
				if cp.Status.Endpoint != nil {
					return fmt.Errorf("status.endpoint is %+v", cp.Status.Endpoint)
				}
				_, err = conditionAt(ctx, k8s, a, lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint)
				return err
			})
			return ctx
		}).
		Assess("the Gateway, once created, brings the ControlPlane to Ready with RoutesReady True, without an operator restart", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			createGateway(ctx, t, k8s, gateway)
			cp := &lodiav1alpha1.ControlPlane{}
			until(ctx, t, 5*time.Minute, "Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, a, cp)
			})
			if _, err := conditionAt(ctx, k8s, a, lodiav1alpha1.ConditionRoutesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted); err != nil {
				t.Error(err)
			}
			reachable := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReachable)
			for _, path := range []string{cp.Status.Endpoint.APIServer.Host, cp.Status.Endpoint.Tunnel.Host, cp.Status.Endpoint.Konnectivity.Host} {
				if reachable == nil || !strings.Contains(reachable.Message, path) {
					t.Errorf("Reachable does not name %s: %+v", path, reachable)
				}
			}
			if ready := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady); ready != nil {
				e := cp.Status.Endpoint
				t.Logf("Ready %s after the ControlPlane's creation, at %s:%d, %s:%d and %s:%d, in the cluster at %s:%d",
					ready.LastTransitionTime.Sub(cp.CreationTimestamp.Time), e.APIServer.Host, e.APIServer.Port, e.Konnectivity.Host, e.Konnectivity.Port, e.Tunnel.Host, e.Tunnel.Port, e.InCluster.Host, e.InCluster.Port)
			}
			if names := routesIn(ctx, t, k8s, a.Namespace); !slices.Equal(names, ownRoutes) {
				t.Errorf("namespace %s holds the TLSRoutes [%s], want [%s]", a.Namespace, strings.Join(names, ", "), strings.Join(ownRoutes, ", "))
			}
			if after := operatorPods(ctx, t, k8s); !slices.Equal(after, operator) {
				t.Errorf("the operator's pods were [%s] before the Gateway and are [%s]", strings.Join(operator, ", "), strings.Join(after, ", "))
			}
			return ctx
		}).
		Assess("a node joins by hostname, default/kubernetes is 240.0.0.1:6443, and the address sits on the node's lo under its label", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			kubeconfig := adminKubeconfig(ctx, t, k8s, a)
			throughTheGateway(kubeconfig)
			access = connect(t, kubeconfig)
			t.Logf("the tenant's API server is reached at %s with the server name %s", access.cfg.Host, access.cfg.ServerName)
			cp := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, a, cp); err != nil {
				t.Fatal(err)
			}
			if host := cp.Status.Endpoint.APIServer.Host; net.ParseIP(host) != nil {
				t.Fatalf("status.endpoint.apiServer is the address %s, not a hostname", host)
			}
			nodeImages = []reference.NamedTagged{tunnelImage(ctx, t, access)}
			node = joinNodes(ctx, t, cfg, k8s, a, access, 1)[0]
			slice := &discoveryv1.EndpointSlice{}
			if err := access.client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceDefault, Name: "kubernetes"}, slice); err != nil {
				t.Fatal(err)
			}
			want := net.JoinHostPort(lodiav1alpha1.DefaultAdvertiseAddress, "6443")
			if got := sliceAddresses(slice); !slices.Equal(got, []string{want}) {
				t.Errorf("the EndpointSlice default/kubernetes holds [%s], want [%s]", strings.Join(got, ", "), want)
			}
			until(ctx, t, 2*time.Minute, lodiav1alpha1.DefaultAdvertiseAddress+"/32 is on "+node+"'s lo, labelled lo:lodia", func(ctx context.Context) error {
				return onLoopback(ctx, node)
			})
			return ctx
		}).
		Assess("from a pod, kubernetes.default answers through apiserver-proxy, and logs, exec and port-forward reach the pod over konnectivity", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			throughKonnectivity(ctx, t, cfg, k8s, a, access, node)
			until(ctx, t, 3*time.Minute, "CoreDNS is available", func(ctx context.Context) error {
				return coreDNSAvailable(ctx, access.client)
			})
			loops = startLoops(ctx, t, cfg, k8s, access, a, node, loop)
			readLoops(ctx, t, "both loops answer on new and reused connections", loops)
			listening, err := docker(ctx, "exec", node, "ss", "-Hltnp", "src", net.JoinHostPort(lodiav1alpha1.DefaultAdvertiseAddress, "6443"))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(listening, `"lodia-tunnel"`) {
				t.Errorf("on node %s, %s:6443 is not lodia-tunnel's: %q", node, lodiav1alpha1.DefaultAdvertiseAddress, listening)
			}
			t.Logf("on node %s: %s", node, strings.TrimSpace(listening))
			return ctx
		}).
		Assess("G1: an API server rollout loses no request from outside through the Gateway or from inside through the tunnel", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			before, _ := readLoops(ctx, t, "both loops' counts are read before the rollout", loops)
			rollAPIServer(ctx, t, k8s, a)
			after, logs := readLoops(ctx, t, "both loops' counts are read after the rollout", loops)
			for i, l := range loops {
				checkLoop(t, l.where, before[i], after[i], logs[i])
			}
			return ctx
		}).
		Assess("a rollout of apiserver-proxy under the in-cluster loop: the surge pod schedules, the address stays, and the loop's clients reconnect", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			inside := loops[1:]
			ds := &appsv1.DaemonSet{}
			if err := access.client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: apiserverProxy}, ds); err != nil {
				t.Fatal(err)
			}
			old := proxyPods(ctx, t, access.client)
			if len(old) != 1 || running(&old[0]) != nil {
				t.Fatalf("%d apiserver-proxy pods before the rollout, want one running", len(old))
			}
			before, _ := readLoops(ctx, t, "the in-cluster loop's counts are read before the rollout", inside)
			patch := client.MergeFrom(ds.DeepCopy())
			ds.Spec.Template.Annotations = map[string]string{"lodia.io/e2e-rollout": time.Now().UTC().Format(time.RFC3339)}
			if err := access.client.Patch(ctx, ds, patch); err != nil {
				t.Fatal(err)
			}
			t.Logf("DaemonSet %s rolls out at generation %d, from pod %s", apiserverProxy, ds.Generation, old[0].Name)
			var surge string
			var samples int
			var lost []string
			err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, 3*time.Minute, true, func(ctx context.Context) (bool, error) {
				samples++
				if err := onLoopback(ctx, node); err != nil {
					lost = append(lost, time.Now().UTC().Format(time.RFC3339Nano)+": "+err.Error())
				}
				pods := proxyPods(ctx, t, access.client)
				oldThere := slices.ContainsFunc(pods, func(p corev1.Pod) bool { return p.UID == old[0].UID })
				for i := range pods {
					if p := &pods[i]; oldThere && p.UID != old[0].UID && p.Spec.NodeName == node && running(p) == nil {
						surge = p.Name
					}
				}
				if err := access.client.Get(ctx, client.ObjectKeyFromObject(ds), ds); err != nil {
					return false, err
				}
				s := ds.Status
				return !oldThere && s.ObservedGeneration == ds.Generation && s.UpdatedNumberScheduled == s.DesiredNumberScheduled && s.NumberAvailable == s.DesiredNumberScheduled, nil
			})
			if err != nil {
				t.Fatalf("the rollout of %s: %v", apiserverProxy, err)
			}
			gone, _ := readLoops(ctx, t, "the in-cluster loop's counts are read once pod "+old[0].Name+" is gone", inside)
			t.Logf("the rollout finished; the address was on lo in %d of %d samples", samples-len(lost), samples)
			if surge == "" {
				t.Error("no new pod ran on the node beside the old one")
			} else {
				t.Logf("the surge pod %s ran beside %s", surge, old[0].Name)
			}
			if len(lost) > 0 {
				t.Errorf("the address left lo during the rollout:\n%s", strings.Join(lost, "\n"))
			}
			time.Sleep(10 * time.Second)
			after, _ := readLoops(ctx, t, "the in-cluster loop's counts are read 10 seconds later", inside)
			t.Logf("over the rollout the in-cluster loop had %d of %d requests fail on new connections and %d of %d on reused ones",
				gone[0].Fresh.Failed-before[0].Fresh.Failed, gone[0].Fresh.OK+gone[0].Fresh.Failed-before[0].Fresh.OK-before[0].Fresh.Failed,
				gone[0].Reused.Failed-before[0].Reused.Failed, gone[0].Reused.OK+gone[0].Reused.Failed-before[0].Reused.OK-before[0].Reused.Failed)
			if after[0].Fresh.OK <= gone[0].Fresh.OK || after[0].Reused.OK <= gone[0].Reused.OK {
				t.Errorf("after the old pod went the in-cluster loop answered %d more on new connections and %d more on reused ones", after[0].Fresh.OK-gone[0].Fresh.OK, after[0].Reused.OK-gone[0].Reused.OK)
			}
			return ctx
		}).
		Assess("a client without the tunnel's client certificate is refused at the sidecar, while the proxy's path keeps working", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			inside := loops[1:]
			cp := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, a, cp); err != nil {
				t.Fatal(err)
			}
			tunnel := cp.Status.Endpoint.Tunnel
			cas := x509.NewCertPool()
			if !cas.AppendCertsFromPEM(access.cfg.CAData) {
				t.Fatal("the admin kubeconfig holds no CA")
			}
			before, _ := readLoops(ctx, t, "the in-cluster loop's counts are read before the stranger's session", inside)
			dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: &tls.Config{ServerName: tunnel.Host, RootCAs: cas, MinVersion: tls.VersionTLS13}}
			conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(gatewayAddress, strconv.Itoa(int(tunnel.Port))))
			if err == nil {
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				_, err = conn.Read(make([]byte, 1))
				_ = conn.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "certificate required") {
				t.Errorf("a session to %s through the Gateway without a client certificate: %v, want the sidecar's certificate required alert", tunnel.Host, err)
			}
			t.Logf("a session to %s through the Gateway, its server verified against the cluster CA, without a client certificate: %v", tunnel.Host, err)
			time.Sleep(5 * time.Second)
			after, logs := readLoops(ctx, t, "the in-cluster loop's counts are read after it", inside)
			checkLoop(t, inside[0].where, before[0], after[0], logs[0])
			stopLoops(ctx, t, loops)
			return ctx
		}).
		Assess("Health: the tunnel's and konnectivity's paths, failing alone, turn Reachable False and name themselves", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			node := managementNode(t, cfg)
			for _, path := range []struct {
				name string
				port string
			}{{"tunnel", "6444"}, {"konnectivity", "8132"}} {
				rule := []string{"FORWARD", "-p", "tcp", "--syn", "--dport", path.port, "-j", "REJECT", "--reject-with", "tcp-reset"}
				if _, err := docker(ctx, append([]string{"exec", node, "iptables", "-I"}, rule...)...); err != nil {
					t.Fatal(err)
				}
				t.Logf("new connections to port %s are refused on the management node", path.port)
				until(ctx, t, 3*time.Minute, "Reachable=False, naming the "+path.name+" path", func(ctx context.Context) error {
					cp, err := conditionAt(ctx, k8s, a, lodiav1alpha1.ConditionReachable, metav1.ConditionFalse, lodiav1alpha1.ReasonExternalProbeFailed)
					if err != nil {
						return err
					}
					if m := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReachable).Message; !strings.HasPrefix(m, path.name+": ") {
						return fmt.Errorf("the message does not start with %q: %s", path.name+": ", m)
					}
					return nil
				})
				if _, err := docker(ctx, append([]string{"exec", node, "iptables", "-D"}, rule...)...); err != nil {
					t.Fatal(err)
				}
				until(ctx, t, 3*time.Minute, "Reachable=True once port "+path.port+" takes connections again", func(ctx context.Context) error {
					_, err := conditionAt(ctx, k8s, a, lodiav1alpha1.ConditionReachable, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable)
					return err
				})
			}
			until(ctx, t, 3*time.Minute, "Ready=True again", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, a, &lodiav1alpha1.ControlPlane{})
			})
			return ctx
		}).
		Assess("B1: a second ControlPlane naming the first's hostnames gets HostnameConflict and no route", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			envoy = readEnvoy(ctx, t, cfg, k8s)
			owner := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, a, owner); err != nil {
				t.Fatal(err)
			}
			named := fmt.Sprintf("by ControlPlane %s/%s (UID %s)", a.Namespace, a.Name, owner.UID)
			createTenant(ctx, t, k8s, b, classes[b], behindTheGateway(a.Namespace))
			until(ctx, t, 2*time.Minute, b.Namespace+"'s RoutesReady=False, HostnameConflict, naming the ControlPlane "+a.Namespace+"/"+a.Name+" as the owner", func(ctx context.Context) error {
				cp, err := conditionAt(ctx, k8s, b, lodiav1alpha1.ConditionRoutesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonHostnameConflict)
				if err != nil {
					return err
				}
				if m := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionRoutesReady).Message; !strings.Contains(m, named) {
					return fmt.Errorf("the message does not say %q: %s", named, m)
				}
				return nil
			})
			if names := routesIn(ctx, t, k8s, b.Namespace); len(names) > 0 {
				t.Errorf("namespace %s holds the TLSRoutes [%s], want none", b.Namespace, strings.Join(names, ", "))
			}
			for _, hostname := range listed(hostnamesOf(a.Namespace)) {
				if holders := routesHolding(ctx, t, k8s, hostname); len(holders) != 1 {
					t.Errorf("hostname %s is held by the TLSRoutes [%s], want one", hostname, strings.Join(holders, ", "))
				}
			}
			return ctx
		}).
		Assess("B1: Envoy rejects no listener update and takes a third tenant's new routes", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			createTenant(ctx, t, k8s, c, classes[c], behindTheGateway(c.Namespace))
			until(ctx, t, 5*time.Minute, c.Namespace+" is Ready, reached by Health through the Gateway", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, c, &lodiav1alpha1.ControlPlane{})
			})
			now := readEnvoy(ctx, t, cfg, k8s)
			t.Logf("Envoy pod %s: lds.update_rejected %v and update_success %v before the third tenant, %v and %v after", now.pod, envoy.rejected, envoy.success, now.rejected, now.success)
			if now.pod != envoy.pod || now.rejected != 0 || envoy.rejected != 0 || now.success <= envoy.success {
				t.Errorf("Envoy %s then %s: lds.update_rejected went from %v to %v and update_success from %v to %v; want 0, 0 and a rise", envoy.pod, now.pod, envoy.rejected, now.rejected, envoy.success, now.success)
			}
			envoy = now
			return ctx
		}).
		Assess("B1: deleting the first ControlPlane hands the routes to the second", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if err := k8s.Delete(ctx, &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: a.Namespace, Name: a.Name}}); err != nil {
				t.Fatal(err)
			}
			until(ctx, t, 3*time.Minute, a.Namespace+" is gone", func(ctx context.Context) error {
				return gone(k8s.Get(ctx, a, &lodiav1alpha1.ControlPlane{}))
			})
			until(ctx, t, 3*time.Minute, b.Namespace+"'s RoutesReady=True with its three routes", func(ctx context.Context) error {
				if _, err := conditionAt(ctx, k8s, b, lodiav1alpha1.ConditionRoutesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonAccepted); err != nil {
					return err
				}
				if names := routesIn(ctx, t, k8s, b.Namespace); !slices.Equal(names, ownRoutes) {
					return fmt.Errorf("namespace %s holds the TLSRoutes [%s], want [%s]", b.Namespace, strings.Join(names, ", "), strings.Join(ownRoutes, ", "))
				}
				return nil
			})
			until(ctx, t, 3*time.Minute, b.Namespace+" is Ready, reached by Health through the Gateway", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, b, &lodiav1alpha1.ControlPlane{})
			})
			if now := readEnvoy(ctx, t, cfg, k8s); now.pod != envoy.pod || now.rejected != 0 {
				t.Errorf("Envoy %s then %s: lds.update_rejected is %v after the handover", envoy.pod, now.pod, now.rejected)
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
			for _, key := range []client.ObjectKey{a, b, c} {
				closeTenant(ctx, t, k8s, key, classes[key])
			}
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
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

func managementNode(t *testing.T, cfg *envconf.Config) string {
	t.Helper()
	cluster, err := kindCluster(cfg.KubeconfigFile())
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := kindcluster.NewProvider().ListNodes(cluster)
	if err != nil || len(nodes) == 0 {
		t.Fatalf("the nodes of kind cluster %s: %d, %v", cluster, len(nodes), err)
	}
	return nodes[0].String()
}

func hostnamesOf(namespace string) lodiav1alpha1.GatewayHostnames {
	return lodiav1alpha1.GatewayHostnames{
		APIServer:    lodiav1alpha1.Hostname("api." + namespace + "." + gatewayZone),
		Konnectivity: lodiav1alpha1.Hostname("konnectivity." + namespace + "." + gatewayZone),
		Tunnel:       lodiav1alpha1.Hostname("tunnel." + namespace + "." + gatewayZone),
	}
}

func listed(h lodiav1alpha1.GatewayHostnames) []string {
	return []string{string(h.APIServer), string(h.Konnectivity), string(h.Tunnel)}
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

func conditionAt(ctx context.Context, k8s client.Client, key client.ObjectKey, conditionType string, status metav1.ConditionStatus, reason string) (*lodiav1alpha1.ControlPlane, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		return nil, err
	}
	c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
	if c == nil || c.Status != status || c.Reason != reason || c.ObservedGeneration != cp.Generation {
		return nil, fmt.Errorf("%s is %+v at generation %d", conditionType, c, cp.Generation)
	}
	return cp, nil
}

func routesIn(ctx context.Context, t *testing.T, k8s client.Client, namespace string) []string {
	t.Helper()
	routes := &gatewayv1.TLSRouteList{}
	if err := k8s.List(ctx, routes, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, route := range routes.Items {
		if owner := metav1.GetControllerOf(&route); owner == nil || owner.Kind != "ControlPlane" || owner.Name != controlPlane {
			names = append(names, route.Name+" (not controlled by ControlPlane "+controlPlane+")")
			continue
		}
		names = append(names, route.Name)
	}
	slices.Sort(names)
	return names
}

func routesHolding(ctx context.Context, t *testing.T, k8s client.Client, hostname string) []string {
	t.Helper()
	routes := &gatewayv1.TLSRouteList{}
	if err := k8s.List(ctx, routes); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, route := range routes.Items {
		if slices.Contains(route.Spec.Hostnames, gatewayv1.Hostname(hostname)) {
			names = append(names, route.Namespace+"/"+route.Name)
		}
	}
	return names
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

func sliceAddresses(slice *discoveryv1.EndpointSlice) []string {
	var out []string
	for _, e := range slice.Endpoints {
		for _, address := range e.Addresses {
			for _, port := range slice.Ports {
				out = append(out, net.JoinHostPort(address, strconv.Itoa(int(*port.Port))))
			}
		}
	}
	return out
}

func onLoopback(ctx context.Context, node string) error {
	out, err := docker(ctx, "exec", node, "ip", "-4", "-o", "addr", "show", "dev", "lo", "label", "lo:lodia")
	if err != nil {
		return err
	}
	if !strings.Contains(out, " "+lodiav1alpha1.DefaultAdvertiseAddress+"/32 ") {
		return fmt.Errorf("lo holds under lo:lodia %q", strings.TrimSpace(out))
	}
	return nil
}

func proxyPods(ctx context.Context, t *testing.T, c client.Client) []corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(metav1.NamespaceSystem), client.MatchingLabels{"k8s-app": apiserverProxy}); err != nil {
		t.Fatal(err)
	}
	return pods.Items
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

type envoyStats struct {
	pod               string
	rejected, success float64
}

func readEnvoy(ctx context.Context, t *testing.T, cfg *envconf.Config, k8s client.Client) envoyStats {
	t.Helper()
	pods := &corev1.PodList{}
	if err := k8s.List(ctx, pods, client.InNamespace(envoyGatewayNamespace), client.MatchingLabels{
		"gateway.envoyproxy.io/owning-gateway-namespace": gatewayNamespace, "gateway.envoyproxy.io/owning-gateway-name": gatewayName,
	}); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 || running(&pods.Items[0]) != nil {
		t.Fatalf("%d Envoy pods of Gateway %s/%s, want one running", len(pods.Items), gatewayNamespace, gatewayName)
	}
	pod := &pods.Items[0]
	address, stop := forward(t, cfg.Client().RESTConfig(), pod, envoyAdminPort)
	defer stop()
	status, body := httpGet(ctx, t, http.DefaultClient, "http://"+address+"/stats/prometheus")
	if status != http.StatusOK {
		t.Fatalf("Envoy's stats answer %d:\n%s", status, body)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	value := func(name string) float64 {
		family, ok := families[name]
		if !ok {
			t.Fatalf("Envoy's stats have no %s", name)
		}
		var sum float64
		for _, m := range family.GetMetric() {
			sum += m.GetCounter().GetValue()
		}
		return sum
	}
	return envoyStats{pod: pod.Name + " (UID " + string(pod.UID) + ")", rejected: value(ldsRejected), success: value(ldsSuccess)}
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
