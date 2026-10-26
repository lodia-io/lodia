// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/distribution/reference"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"
	certutil "k8s.io/client-go/util/cert"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	bootstraputil "k8s.io/cluster-bootstrap/token/util"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/klient/decoder"
	"sigs.k8s.io/e2e-framework/klient/k8s/resources"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
	"sigs.k8s.io/kind/pkg/apis/config/defaults"
	kindcluster "sigs.k8s.io/kind/pkg/cluster"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	gateTenant              = "team-gate"
	gateClass               = "e2e-gate"
	gatePodCIDR             = "10.200.0.0/16"
	nodePatches             = "/etc/kubernetes/patches"
	agnhost                 = "registry.k8s.io/e2e-test-images/agnhost:2.66.1@sha256:6e2eef87d0dc77e070a7549bc5fdfa290c0fefe3359eb33c2ad1eda692fe66a4"
	konnectivityMetricsPort = 8134
	dialDuration            = "konnectivity_network_proxy_server_dial_duration_seconds"
	apiLoop                 = "apiloop"
	apiLoopRate             = 10
)

var booted = regexp.MustCompile("Reached target .*Multi-User System.*|detected cgroup v1")

var coreDNSObjects = map[string]client.Object{
	"ClusterRole system:coredns":         &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "system:coredns"}},
	"ClusterRoleBinding system:coredns":  &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "system:coredns"}},
	"ConfigMap kube-system/coredns":      &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceSystem, Name: "coredns"}},
	"Deployment kube-system/coredns":     &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceSystem, Name: "coredns"}},
	"Service kube-system/kube-dns":       &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceSystem, Name: "kube-dns"}},
	"ServiceAccount kube-system/coredns": &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceSystem, Name: "coredns"}},
}

var everyNamespaceHolds = []string{"ConfigMap kube-root-ca.crt", "ServiceAccount default"}

func TestPhase1Gate(t *testing.T) {
	var (
		k8s    client.Client
		access *tenantAccess
		node   string
		loop   reference.NamedTagged
	)
	key := client.ObjectKey{Namespace: gateTenant, Name: controlPlane}
	f := features.New("the phase 1 gate on kind").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			createTenant(ctx, t, k8s, key, gateClass)
			image, err := build(ctx, cfg.KubeconfigFile(), "./test/"+apiLoop)
			if err != nil {
				t.Fatal(err)
			}
			loop = image
			return ctx
		}).
		Assess("Ready in under 3 minutes from the ControlPlane's creation", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cp := &lodiav1alpha1.ControlPlane{}
			until(ctx, t, 3*time.Minute, "Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, key, cp)
			})
			ready := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady)
			took := ready.LastTransitionTime.Sub(cp.CreationTimestamp.Time)
			t.Logf("the ControlPlane turned Ready %s after its creation", took)
			if took >= 3*time.Minute {
				t.Errorf("Ready took %s; the gate wants under 3 minutes", took)
			}
			return ctx
		}).
		Assess("anonymous requests reach the tenant's health paths and cluster-info, nothing else", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			access = openTenant(ctx, t, k8s, cfg.Client().RESTConfig(), key)
			anonymous, err := rest.HTTPClientFor(rest.AnonymousClientConfig(access.cfg))
			if err != nil {
				t.Fatal(err)
			}
			for path, want := range map[string]int{
				"/healthz": http.StatusOK, "/livez": http.StatusOK, "/readyz": http.StatusOK,
				"/api/v1/namespaces/kube-public/configmaps/cluster-info": http.StatusOK,
				"/version": http.StatusUnauthorized, "/api/v1/namespaces/kube-public/configmaps": http.StatusUnauthorized,
			} {
				if status, _ := httpGet(ctx, t, anonymous, access.cfg.Host+path); status != want {
					t.Errorf("an anonymous GET %s answers %d, want %d", path, status, want)
				}
			}
			return ctx
		}).
		Assess("a container of kind's node image joins with kubeadm join --token and its node turns Ready", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			node = joinNodes(ctx, t, cfg, k8s, key, access, 1)[0]
			return ctx
		}).
		Assess("a pod runs on the node, and logs, exec and port-forward reach it through the konnectivity tunnel", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			leases := &coordinationv1.LeaseList{}
			if err := access.client.List(ctx, leases, client.InNamespace(metav1.NamespaceSystem), client.MatchingLabels{"k8s-app": "konnectivity-server"}); err != nil {
				t.Fatal(err)
			}
			if len(leases.Items) == 0 {
				t.Fatal("the tenant's kube-system holds no konnectivity server lease for the agents to count")
			}
			t.Logf("the agents count the server lease %s", leases.Items[0].Name)
			web := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "web"},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "web",
					Image: agnhost,
					Args:  []string{"netexec", "--http-port=8080"},
					Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
				}}},
			}
			create(ctx, t, access.client, web)
			until(ctx, t, 3*time.Minute, "pod web runs on node "+node, func(ctx context.Context) error {
				if err := access.client.Get(ctx, client.ObjectKeyFromObject(web), web); err != nil {
					return err
				}
				if web.Spec.NodeName != node {
					return fmt.Errorf("pod web is scheduled to %q", web.Spec.NodeName)
				}
				return running(web)
			})

			metrics, stopMetrics := forward(t, cfg.Client().RESTConfig(), apiServerPod(ctx, t, k8s, key), konnectivityMetricsPort)
			defer stopMetrics()
			count := dials(ctx, t, metrics)
			oneDial := func(call string) {
				t.Helper()
				after := dials(ctx, t, metrics)
				t.Logf("konnectivity-server's dials: %d before %s, %d after", count, call, after)
				if after != count+1 {
					t.Errorf("%s took konnectivity-server's dials from %d to %d; want one dial", call, count, after)
				}
				count = after
			}

			logs, err := kubernetes.NewForConfigOrDie(access.cfg).CoreV1().Pods(web.Namespace).GetLogs(web.Name, &corev1.PodLogOptions{}).DoRaw(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(logs), "Started HTTP server on port 8080") {
				t.Errorf("the logs of pod web lack agnhost's start line:\n%s", logs)
			}
			oneDial("logs")
			var stdout, stderr bytes.Buffer
			if err := access.resources.ExecInPod(ctx, web.Namespace, web.Name, "web", []string{"hostname"}, &stdout, &stderr); err != nil {
				t.Fatalf("exec hostname in pod web: %v\n%s", err, stderr.String())
			}
			if hostname := strings.TrimSpace(stdout.String()); hostname != web.Name {
				t.Errorf("exec hostname in pod web printed %q", hostname)
			}
			oneDial("exec")
			stdout.Reset()
			stderr.Reset()
			execURL := kubernetes.NewForConfigOrDie(access.cfg).CoreV1().RESTClient().Post().Resource("pods").Namespace(web.Namespace).Name(web.Name).SubResource("exec").
				VersionedParams(&corev1.PodExecOptions{Container: "web", Command: []string{"hostname"}, Stdout: true, Stderr: true}, scheme.ParameterCodec).URL()
			webSocket, err := remotecommand.NewWebSocketExecutor(access.cfg, http.MethodGet, execURL.String())
			if err != nil {
				t.Fatal(err)
			}
			if err := webSocket.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
				t.Fatalf("exec hostname in pod web over WebSocket: %v\n%s", err, stderr.String())
			}
			if hostname := strings.TrimSpace(stdout.String()); hostname != web.Name {
				t.Errorf("exec hostname in pod web over WebSocket printed %q", hostname)
			}
			oneDial("exec over WebSocket")
			local, stopWeb := forward(t, access.cfg, web, 8080)
			defer stopWeb()
			if status, echo := httpGet(ctx, t, http.DefaultClient, "http://"+local+"/echo?msg=through-the-tunnel"); status != http.StatusOK || echo != "through-the-tunnel" {
				t.Errorf("port-forward to pod web answered %d %q", status, echo)
			}
			oneDial("port-forward")
			return ctx
		}).
		Assess("an API server rollout loses no request from outside or inside the tenant, whose kubernetes endpoints never go empty", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 3*time.Minute, "CoreDNS is available", func(ctx context.Context) error {
				return coreDNSAvailable(ctx, access.client)
			})
			cp := &lodiav1alpha1.ControlPlane{}
			if err := k8s.Get(ctx, key, cp); err != nil {
				t.Fatal(err)
			}
			kubeconfig := &corev1.Secret{}
			if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: lodiav1alpha1.AdminKubeconfigSecret.Name(key.Name)}, kubeconfig); err != nil {
				t.Fatal(err)
			}
			create(ctx, t, access.client, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: apiLoop},
				Data:       map[string][]byte{lodiav1alpha1.KubeconfigKey: kubeconfig.Data[lodiav1alpha1.KubeconfigKey]},
			})
			loadImage(ctx, t, cfg.KubeconfigFile(), node, loop)
			endpoint := "https://" + net.JoinHostPort(cp.Status.Endpoint.APIServer.Host, strconv.Itoa(int(cp.Status.Endpoint.APIServer.Port)))
			outside := loopPod(key.Namespace, kubeconfig.Name, loop, endpoint)
			inside := loopPod(metav1.NamespaceDefault, apiLoop, loop, "https://kubernetes.default")
			inside.Spec.NodeName = node
			create(ctx, t, k8s, outside)
			create(ctx, t, access.client, inside)
			management := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig())
			var outsideBefore, insideBefore loopCounts
			until(ctx, t, 2*time.Minute, "both loops answer on new and reused connections", func(ctx context.Context) error {
				var err error
				if outsideBefore, _, err = readLoop(ctx, management, outside); err != nil {
					return err
				}
				insideBefore, _, err = readLoop(ctx, kubernetes.NewForConfigOrDie(access.cfg), inside)
				return err
			})

			old := apiServerPod(ctx, t, k8s, key)
			patch := client.MergeFrom(cp.DeepCopy())
			cp.Spec.APIServer = &lodiav1alpha1.APIServer{ExtraArgs: map[string]string{"event-ttl": "1h0m0s"}}
			if err := k8s.Patch(ctx, cp, patch); err != nil {
				t.Fatal(err)
			}
			t.Logf("apiServer.extraArgs takes --event-ttl at generation %d, and pod %s rolls", cp.Generation, old.Name)
			until(ctx, t, 4*time.Minute, "the API server rolled out and pod "+old.Name+" is gone", func(ctx context.Context) error {
				if err := gone(k8s.Get(ctx, client.ObjectKeyFromObject(old), &corev1.Pod{})); err != nil {
					return fmt.Errorf("pod %s: %w", old.Name, err)
				}
				fresh := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, key, fresh); err != nil {
					return err
				}
				if c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady); c == nil || c.Status != metav1.ConditionTrue || c.ObservedGeneration != fresh.Generation {
					return fmt.Errorf("APIServerReady is %+v at generation %d", c, fresh.Generation)
				}
				return nil
			})
			time.Sleep(5 * time.Second)
			access.stop()
			access = openTenant(ctx, t, k8s, cfg.Client().RESTConfig(), key)

			var outsideAfter, insideAfter loopCounts
			var outsideLog, insideLog string
			until(ctx, t, time.Minute, "both loops' counts are read after the rollout", func(ctx context.Context) error {
				var err error
				if outsideAfter, outsideLog, err = readLoop(ctx, management, outside); err != nil {
					return err
				}
				insideAfter, insideLog, err = readLoop(ctx, kubernetes.NewForConfigOrDie(access.cfg), inside)
				return err
			})
			checkLoop(t, "outside the tenant, to "+endpoint, outsideBefore, outsideAfter, outsideLog)
			checkLoop(t, "inside the tenant, to https://kubernetes.default", insideBefore, insideAfter, insideLog)
			if err := k8s.Delete(ctx, outside); err != nil {
				t.Error(err)
			}
			if err := access.client.Delete(ctx, inside); err != nil {
				t.Error(err)
			}
			return ctx
		}).
		Assess("disabling and re-enabling CoreDNS removes and restores its objects through the inventory", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, time.Minute, "CoreDNS's objects are in the tenant and its inventory", func(ctx context.Context) error {
				return coreDNSApplied(ctx, access.client, true)
			})
			setCoreDNS(ctx, t, k8s, key, false)
			until(ctx, t, 2*time.Minute, "CoreDNS's objects are gone from the tenant and its inventory", func(ctx context.Context) error {
				return coreDNSApplied(ctx, access.client, false)
			})
			setCoreDNS(ctx, t, k8s, key, true)
			until(ctx, t, 2*time.Minute, "CoreDNS's objects are back in the tenant and its inventory", func(ctx context.Context) error {
				return coreDNSApplied(ctx, access.client, true)
			})
			return ctx
		}).
		Assess("deleting the ControlPlane leaves nothing in the namespace but the claim the class retains", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if err := k8s.Delete(ctx, &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: controlPlane, Namespace: gateTenant}}); err != nil {
				t.Fatal(err)
			}
			until(ctx, t, 3*time.Minute, "the ControlPlane is gone", func(ctx context.Context) error {
				return gone(k8s.Get(ctx, key, &lodiav1alpha1.ControlPlane{}))
			})
			etcd := lodiav1alpha1.EtcdName(controlPlane)
			kept := []string{"PersistentVolumeClaim " + etcd + "-" + etcd + "-0"}
			served, err := discovery.NewDiscoveryClientForConfig(cfg.Client().RESTConfig())
			if err != nil {
				t.Fatal(err)
			}
			objects, err := metadata.NewForConfig(cfg.Client().RESTConfig())
			if err != nil {
				t.Fatal(err)
			}
			until(ctx, t, 3*time.Minute, "the namespace holds only what the class's reclaim policy keeps", func(ctx context.Context) error {
				left, err := leftovers(ctx, served, objects)
				if err != nil {
					return err
				}
				if !slices.Equal(left, kept) {
					return fmt.Errorf("the namespace holds [%s]; the class keeps [%s]", strings.Join(left, ", "), strings.Join(kept, ", "))
				}
				return nil
			})
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			closeTenant(ctx, t, k8s, access, key, gateClass)
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func createTenant(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey, class string) string {
	t.Helper()
	image, err := reference.ParseNormalizedNamed(defaults.Image)
	tagged, ok := image.(reference.Tagged)
	if err != nil || !ok {
		t.Fatalf("kind's node image %s has no version: %v", defaults.Image, err)
	}
	version := tagged.Tag()
	create(ctx, t, k8s, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: key.Namespace}})
	create(ctx, t, k8s, &lodiav1alpha1.DatastoreClass{
		ObjectMeta: metav1.ObjectMeta{Name: class},
		Spec:       lodiav1alpha1.DatastoreClassSpec{Storage: lodiav1alpha1.EtcdStorage{Size: resource.MustParse("1Gi"), ReclaimPolicy: "Retain"}},
	})
	create(ctx, t, k8s, &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   version,
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{ClassName: class}},
			Endpoint:  lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}},
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.32.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{gatePodCIDR}},
			Addons:    &lodiav1alpha1.Addons{KubeProxy: &lodiav1alpha1.KubeProxy{Conntrack: &lodiav1alpha1.KubeProxyConntrack{MaxPerCore: new(int32(0))}}},
		},
	})
	t.Logf("ControlPlane %s created at %s, the version of kind's node image %s", key, version, defaults.Image)
	return version
}

func closeTenant(ctx context.Context, t *testing.T, k8s client.Client, access *tenantAccess, key client.ObjectKey, class string) {
	t.Helper()
	if access != nil {
		access.stop()
	}
	if k8s == nil {
		return
	}
	for _, obj := range []client.Object{
		&lodiav1alpha1.DatastoreClass{ObjectMeta: metav1.ObjectMeta{Name: class}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: key.Namespace}},
	} {
		if err := client.IgnoreNotFound(k8s.Delete(ctx, obj)); err != nil {
			t.Error(err)
		}
	}
}

func controlPlaneReady(ctx context.Context, k8s client.Client, key client.ObjectKey, cp *lodiav1alpha1.ControlPlane) error {
	if err := k8s.Get(ctx, key, cp); err != nil {
		return err
	}
	if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Status != metav1.ConditionTrue || c.ObservedGeneration != cp.Generation {
		return fmt.Errorf("Ready is %+v at generation %d", c, cp.Generation)
	}
	return nil
}

type tenantAccess struct {
	cfg        *rest.Config
	kubeconfig clientcmdapi.Config
	client     client.Client
	resources  *resources.Resources
	stop       func()
}

func openTenant(ctx context.Context, t *testing.T, k8s client.Client, management *rest.Config, key client.ObjectKey) *tenantAccess {
	t.Helper()
	secret := &corev1.Secret{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: lodiav1alpha1.AdminKubeconfigSecret.Name(key.Name)}, secret); err != nil {
		t.Fatal(err)
	}
	kubeconfig, err := clientcmd.Load(secret.Data[lodiav1alpha1.KubeconfigKey])
	if err != nil {
		t.Fatal(err)
	}
	pod := apiServerPod(ctx, t, k8s, key)
	var port int32
	for _, container := range pod.Spec.Containers {
		for _, p := range container.Ports {
			if p.Name == "https" {
				port = p.ContainerPort
			}
		}
	}
	address, stop := forward(t, management, pod, port)
	for _, cluster := range kubeconfig.Clusters {
		cluster.Server = "https://" + address
	}
	cfg, err := clientcmd.NewDefaultClientConfig(*kubeconfig, nil).ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.New(cfg, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := resources.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("the tenant's API server %s is reached through a port-forward to pod %s, as %s", cfg.Host, pod.Name, secret.Name)
	return &tenantAccess{cfg: cfg, kubeconfig: *kubeconfig, client: c, resources: r, stop: stop}
}

func apiServerPod(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) *corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := k8s.List(ctx, pods, client.InNamespace(key.Namespace), client.MatchingLabels{
		lodiav1alpha1.ControlPlaneLabel: key.Name, lodiav1alpha1.ComponentLabel: lodiav1alpha1.APIServerComponent,
	}); err != nil {
		t.Fatal(err)
	}
	for i := range pods.Items {
		if pod := &pods.Items[i]; pod.DeletionTimestamp.IsZero() && running(pod) == nil {
			return pod
		}
	}
	t.Fatalf("no running API server pod among %d", len(pods.Items))
	return nil
}

// @todo: stands in for the tenant's endpoint, unreachable from macOS; remove with todo.md's port-forward item.
func forward(t *testing.T, cfg *rest.Config, pod *corev1.Pod, port int32) (string, func()) {
	t.Helper()
	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	url := clientset.CoreV1().RESTClient().Post().Resource("pods").Namespace(pod.Namespace).Name(pod.Name).SubResource("portforward").URL()
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				stop := make(chan struct{})
				defer close(stop)
				forwarder, err := portforward.NewOnAddresses(dialer, []string{"127.0.0.1"}, []string{fmt.Sprintf(":%d", port)}, stop, make(chan struct{}), io.Discard, os.Stderr)
				if err == nil {
					err = relay(conn, forwarder)
				}
				if err != nil {
					fmt.Fprintf(os.Stderr, "port-forward to %s/%s:%d: %v\n", pod.Namespace, pod.Name, port, err)
				}
			}()
		}
	}()
	return listener.Addr().String(), sync.OnceFunc(func() { _ = listener.Close() })
}

func relay(conn net.Conn, forwarder *portforward.PortForwarder) error {
	defer func() { _ = conn.Close() }()
	failed := make(chan error, 1)
	go func() { failed <- forwarder.ForwardPorts() }()
	select {
	case <-forwarder.Ready:
	case err := <-failed:
		return err
	}
	ports, err := forwarder.GetPorts()
	if err != nil {
		return err
	}
	remote, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(ports[0].Local))))
	if err != nil {
		return err
	}
	go func() {
		_, _ = io.Copy(remote, conn)
		_ = remote.Close()
	}()
	_, _ = io.Copy(conn, remote)
	return nil
}

func bootstrapToken(ctx context.Context, t *testing.T, c client.Client) string {
	t.Helper()
	token, err := bootstraputil.GenerateBootstrapToken()
	if err != nil {
		t.Fatal(err)
	}
	id, secret, _ := strings.Cut(token, ".")
	create(ctx, t, c, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceSystem, Name: bootstraputil.BootstrapTokenSecretName(id)},
		Type:       bootstrapapi.SecretTypeBootstrapToken,
		StringData: map[string]string{
			bootstrapapi.BootstrapTokenIDKey:               id,
			bootstrapapi.BootstrapTokenSecretKey:           secret,
			bootstrapapi.BootstrapTokenExpirationKey:       time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
			bootstrapapi.BootstrapTokenUsageAuthentication: "true",
			bootstrapapi.BootstrapTokenUsageSigningKey:     "true",
			bootstrapapi.BootstrapTokenExtraGroupsKey:      "system:bootstrappers:kubeadm:default-node-token",
		},
	})
	until(ctx, t, time.Minute, "the bootstrap signer signed cluster-info for token "+id, func(ctx context.Context) error {
		info := &corev1.ConfigMap{}
		if err := c.Get(ctx, client.ObjectKey{Namespace: metav1.NamespacePublic, Name: bootstrapapi.ConfigMapClusterInfo}, info); err != nil {
			return err
		}
		if _, signed := info.Data[bootstrapapi.JWSSignatureKeyPrefix+id]; !signed {
			return fmt.Errorf("cluster-info has no %s%s", bootstrapapi.JWSSignatureKeyPrefix, id)
		}
		return nil
	})
	return token
}

func joinNodes(ctx context.Context, t *testing.T, cfg *envconf.Config, k8s client.Client, key client.ObjectKey, access *tenantAccess, count int) []string {
	t.Helper()
	token := bootstrapToken(ctx, t, access.client)
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		t.Fatal(err)
	}
	endpoint := net.JoinHostPort(cp.Status.Endpoint.APIServer.Host, strconv.Itoa(int(cp.Status.Endpoint.APIServer.Port)))
	cas, err := certutil.ParseCertsPEM(access.cfg.CAData)
	if err != nil {
		t.Fatal(err)
	}
	nodes := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		node := startNode(ctx, t, cfg.KubeconfigFile(), i)
		join := []string{"exec", node, "kubeadm", "join", endpoint, "--token", token, "--patches", nodePatches, "--ignore-preflight-errors=SystemVerification"}
		for _, ca := range cas {
			hash := sha256.Sum256(ca.RawSubjectPublicKeyInfo)
			join = append(join, "--discovery-token-ca-cert-hash", "sha256:"+hex.EncodeToString(hash[:]))
		}
		out, err := docker(ctx, join...)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("kubeadm join %s:\n%s", endpoint, out)
		until(ctx, t, time.Minute, "node "+node+" is registered", func(ctx context.Context) error {
			return access.client.Get(ctx, client.ObjectKey{Name: node}, &corev1.Node{})
		})
		nodes = append(nodes, node)
	}
	installCNI(ctx, t, access, nodes[0])
	for _, node := range nodes {
		until(ctx, t, 3*time.Minute, "node "+node+" is Ready", func(ctx context.Context) error {
			joined := &corev1.Node{}
			if err := access.client.Get(ctx, client.ObjectKey{Name: node}, joined); err != nil {
				return err
			}
			for _, c := range joined.Status.Conditions {
				if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
					return nil
				}
			}
			return fmt.Errorf("node %s is not Ready: %+v", node, joined.Status.Conditions)
		})
	}
	until(ctx, t, 3*time.Minute, fmt.Sprintf("the konnectivity agents on the %d nodes are ready", count), func(ctx context.Context) error {
		agents := &appsv1.DaemonSet{}
		if err := access.client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: "konnectivity-agent"}, agents); err != nil {
			return err
		}
		if s := agents.Status; s.DesiredNumberScheduled != int32(count) || s.NumberReady != int32(count) {
			return fmt.Errorf("%d of %d agents ready", s.NumberReady, s.DesiredNumberScheduled)
		}
		return nil
	})
	return nodes
}

var startedNodes []string

func startNode(ctx context.Context, t *testing.T, kubeconfig string, i int) string {
	t.Helper()
	cluster, err := kindCluster(kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	name := cluster + "-tenant-node-" + strconv.Itoa(i)
	if !slices.Contains(startedNodes, name) {
		startedNodes = append(startedNodes, name)
	}
	if _, err := docker(ctx, "rm", "--force", "--volumes", name); err != nil {
		t.Fatal(err)
	}
	if _, err := docker(ctx, "run", "--detach", "--tty", "--name", name, "--hostname", name, "--network", "kind",
		"--privileged", "--security-opt", "seccomp=unconfined", "--security-opt", "apparmor=unconfined",
		"--cgroupns=private", "--tmpfs", "/tmp", "--tmpfs", "/run", "--volume", "/var",
		"--volume", "/lib/modules:/lib/modules:ro", "--restart=on-failure:1", "--init=false", defaults.Image); err != nil {
		t.Fatal(err)
	}
	until(ctx, t, time.Minute, "container "+name+" booted", func(ctx context.Context) error {
		logs, err := docker(ctx, "logs", name)
		if err != nil {
			return err
		}
		if !booted.MatchString(logs) {
			return errors.New("its entrypoint has not reported the boot")
		}
		return nil
	})
	if _, err := docker(ctx, "cp", filepath.Join("testdata", "kubeadm-patches"), name+":"+nodePatches); err != nil {
		t.Fatal(err)
	}
	return name
}

func removeNodes(ctx context.Context, _ *envconf.Config) (context.Context, error) {
	if len(startedNodes) == 0 {
		return ctx, nil
	}
	_, err := docker(ctx, append([]string{"rm", "--force", "--volumes"}, startedNodes...)...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "remove the node containers:", err)
	}
	return ctx, err
}

func docker(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker %s: %w\n%s", args[0], err, out)
	}
	return string(out), nil
}

func installCNI(ctx context.Context, t *testing.T, access *tenantAccess, node string) {
	t.Helper()
	manifest, err := docker(ctx, "exec", node, "cat", "/kind/manifests/default-cni.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cni, err := template.New("cni").Parse(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var rendered bytes.Buffer
	if err := cni.Execute(&rendered, struct{ PodSubnet string }{gatePodCIDR}); err != nil {
		t.Fatal(err)
	}
	if err := decoder.DecodeEach(ctx, &rendered, decoder.CreateHandler(access.resources)); err != nil {
		t.Fatal(err)
	}
}

func dials(ctx context.Context, t *testing.T, address string) uint64 {
	t.Helper()
	status, metrics := httpGet(ctx, t, http.DefaultClient, "http://"+address+"/metrics")
	if status != http.StatusOK {
		t.Fatalf("konnectivity-server's metrics answer %d:\n%s", status, metrics)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(metrics))
	if err != nil {
		t.Fatal(err)
	}
	var count uint64
	for _, m := range families[dialDuration].GetMetric() {
		count += m.GetHistogram().GetSampleCount()
	}
	return count
}

func httpGet(ctx context.Context, t *testing.T, c *http.Client, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

func setCoreDNS(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey, enabled bool) {
	t.Helper()
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(cp.DeepCopy())
	if cp.Spec.Addons.CoreDNS == nil {
		cp.Spec.Addons.CoreDNS = &lodiav1alpha1.CoreDNS{}
	}
	cp.Spec.Addons.CoreDNS.Enabled = new(enabled)
	if err := k8s.Patch(ctx, cp, patch); err != nil {
		t.Fatal(err)
	}
	t.Logf("addons.coreDNS.enabled is %t at generation %d", enabled, cp.Generation)
}

func coreDNSApplied(ctx context.Context, c client.Client, enabled bool) error {
	inventory := &corev1.ConfigMap{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: "lodia-inventory"}, inventory); err != nil {
		return err
	}
	var entries []struct{ Kind, Namespace, Name string }
	if err := json.Unmarshal([]byte(inventory.Data["objects"]), &entries); err != nil {
		return err
	}
	listed := make([]string, 0, len(entries))
	for _, e := range entries {
		listed = append(listed, e.Kind+" "+strings.TrimPrefix(e.Namespace+"/"+e.Name, "/"))
	}
	for entry, obj := range coreDNSObjects {
		err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj.DeepCopyObject().(client.Object))
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if exists, inInventory := err == nil, slices.Contains(listed, entry); exists != enabled || inInventory != enabled {
			return fmt.Errorf("%s: in the tenant %t, in the inventory %t", entry, exists, inInventory)
		}
	}
	return nil
}

func leftovers(ctx context.Context, served *discovery.DiscoveryClient, objects metadata.Interface) ([]string, error) {
	namespaced, err := served.ServerPreferredNamespacedResourcesWithContext(ctx)
	if err != nil {
		return nil, err
	}
	var left []string
	for _, resources := range discovery.FilteredBy(discovery.SupportsAllVerbs{Verbs: []string{"list"}}, namespaced) {
		gv, err := schema.ParseGroupVersion(resources.GroupVersion)
		if err != nil {
			return nil, err
		}
		for _, r := range resources.APIResources {
			if r.Name == "events" {
				continue
			}
			list, err := objects.Resource(gv.WithResource(r.Name)).Namespace(gateTenant).List(ctx, metav1.ListOptions{})
			if err != nil {
				return nil, err
			}
			for _, item := range list.Items {
				if entry := r.Kind + " " + item.Name; !slices.Contains(everyNamespaceHolds, entry) {
					left = append(left, entry)
				}
			}
		}
	}
	slices.Sort(left)
	return left, nil
}

func coreDNSAvailable(ctx context.Context, c client.Client) error {
	coreDNS := &appsv1.Deployment{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: "coredns"}, coreDNS); err != nil {
		return err
	}
	if s, want := coreDNS.Status, ptr.Deref(coreDNS.Spec.Replicas, 1); s.ObservedGeneration != coreDNS.Generation || s.UpdatedReplicas != want || s.AvailableReplicas != want {
		return fmt.Errorf("%d of %d replicas available", s.AvailableReplicas, want)
	}
	return nil
}

func loadImage(ctx context.Context, t *testing.T, kubeconfig, node string, image reference.NamedTagged) {
	t.Helper()
	cluster, err := kindCluster(kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := kindcluster.NewProvider().ListNodes(cluster)
	if err != nil || len(nodes) == 0 {
		t.Fatalf("the nodes of kind cluster %s: %d, %v", cluster, len(nodes), err)
	}
	export := exec.CommandContext(ctx, "docker", "exec", nodes[0].String(), "ctr", "--namespace=k8s.io", "images", "export", "-", image.String())
	load := exec.CommandContext(ctx, "docker", "exec", "--interactive", node, "ctr", "--namespace=k8s.io", "images", "import", "--digests", "-")
	var stderr bytes.Buffer
	export.Stderr, load.Stderr = &stderr, &stderr
	archive, err := export.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	load.Stdin = archive
	if err := load.Start(); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(export.Run(), load.Wait()); err != nil {
		t.Fatalf("copy %s from %s into %s: %v\n%s", image, nodes[0], node, err, stderr.String())
	}
}

func loopPod(namespace, kubeconfig string, image reference.NamedTagged, server string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: apiLoop},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:            apiLoop,
				Image:           image.String(),
				ImagePullPolicy: corev1.PullNever,
				Args:            []string{"--kubeconfig=/etc/apiloop/" + lodiav1alpha1.KubeconfigKey, "--server=" + server, "--rate=" + strconv.Itoa(apiLoopRate)},
				VolumeMounts:    []corev1.VolumeMount{{Name: "kubeconfig", MountPath: "/etc/apiloop", ReadOnly: true}},
			}},
			Volumes: []corev1.Volume{{Name: "kubeconfig", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: kubeconfig}}}},
		},
	}
}

type loopTally struct {
	OK     int64 `json:"ok"`
	Failed int64 `json:"failed"`
	Empty  int64 `json:"empty"`
}

type loopCounts struct {
	Time   time.Time `json:"time"`
	Fresh  loopTally `json:"fresh"`
	Reused loopTally `json:"reused"`
}

func readLoop(ctx context.Context, c kubernetes.Interface, pod *corev1.Pod) (loopCounts, string, error) {
	raw, err := c.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{}).DoRaw(ctx)
	if err != nil {
		return loopCounts{}, "", err
	}
	var last loopCounts
	for line := range strings.Lines(string(raw)) {
		if counts, ok := strings.CutPrefix(line, "counts "); ok {
			if err := json.Unmarshal([]byte(counts), &last); err != nil {
				return loopCounts{}, "", err
			}
		}
	}
	if last.Fresh.OK == 0 || last.Reused.OK == 0 {
		return loopCounts{}, "", fmt.Errorf("pod %s/%s has had no answer on new and on reused connections: %+v", pod.Namespace, pod.Name, last)
	}
	return last, string(raw), nil
}

func checkLoop(t *testing.T, where string, before, after loopCounts, log string) {
	t.Helper()
	seconds := after.Time.Sub(before.Time).Seconds()
	failed := false
	for _, mode := range []struct {
		name          string
		before, after loopTally
	}{{"new connections", before.Fresh, after.Fresh}, {"reused connections", before.Reused, after.Reused}} {
		ok, lost, empty := mode.after.OK-mode.before.OK, mode.after.Failed-mode.before.Failed, mode.after.Empty-mode.before.Empty
		expected := apiLoopRate * seconds
		t.Logf("%s on %s, %.0fs through the rollout: %d answered of %.0f sent at %d per second, %d failed, %d found the kubernetes EndpointSlice empty", where, mode.name, seconds, ok, expected, apiLoopRate, lost, empty)
		if lost != 0 || empty != 0 || float64(ok) < 0.95*expected {
			failed = true
			t.Errorf("%s on %s: %d failed and %d found the EndpointSlice empty; %d answered, want at least 95%% of %.0f", where, mode.name, lost, empty, ok, expected)
		}
	}
	if !failed {
		return
	}
	for line := range strings.Lines(log) {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "failed" && fields[0] != "empty" {
			continue
		}
		if at, err := time.Parse(time.RFC3339Nano, fields[1]); err == nil && !at.Before(before.Time) {
			t.Log(strings.TrimSpace(line))
		}
	}
}
