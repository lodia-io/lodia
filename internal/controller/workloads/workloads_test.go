// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/controller/workloads"
	"go.lodia.io/lodia/internal/operator"
	"go.lodia.io/lodia/internal/versions"
)

const (
	version     = "v1.37.1"
	signerImage = "registry.example/lodia-signer:test"
	lbAddress   = "203.0.113.10"
)

var restartOnlySecrets = []lodiav1alpha1.Secret{
	lodiav1alpha1.CABundleSecret, lodiav1alpha1.EtcdCASecret, lodiav1alpha1.FrontProxyClientSecret,
	lodiav1alpha1.KonnectivityKubeconfigSecret, lodiav1alpha1.KonnectivityServerSecret,
}

var k8s client.WithWatch

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("skipping the workloads controller tests: KUBEBUILDER_ASSETS is not set; run make test")
		os.Exit(0)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "api", "crds")},
		ErrorIfCRDPathMissing: true,
		UseExistingCluster:    new(false),
	}
	env.ControlPlane.GetAPIServer().Configure().Append("enable-admission-plugins", "OwnerReferencesPermissionEnforcement")
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		os.Exit(1)
	}
	stop := func(what string, err error) {
		fmt.Fprintln(os.Stderr, what+":", err)
		_ = env.Stop()
		os.Exit(1)
	}
	scheme := operator.Scheme()
	if k8s, err = client.NewWithWatch(cfg, client.Options{Scheme: scheme}); err != nil {
		stop("client", err)
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	controller, err := asWorkloadsController(cfg)
	if err != nil {
		stop("the controller's ClusterRole", err)
	}
	cache, err := operator.Cache()
	if err != nil {
		stop("cache", err)
	}
	cache.ReaderFailOnMissingInformer = true
	mgr, err := ctrl.NewManager(controller, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, Cache: cache})
	if err != nil {
		stop("manager", err)
	}
	reconciler := &workloads.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), SignerImage: signerImage}
	if err := reconciler.SetupWithManager(mgr, 1); err != nil {
		stop("controller", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() { started <- mgr.Start(ctx) }()
	code := m.Run()
	cancel()
	if err := <-started; err != nil {
		fmt.Fprintln(os.Stderr, "manager:", err)
		code = 1
	}
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop envtest:", err)
	}
	os.Exit(code)
}

func asWorkloadsController(cfg *rest.Config) (*rest.Config, error) {
	const user = "lodia-workloads"
	ctx := context.Background()
	manifest, err := os.ReadFile(filepath.Join("..", "..", "..", "charts", "lodia", "files", "rbac", "workloads.yaml"))
	if err != nil {
		return nil, err
	}
	role := &rbacv1.ClusterRole{}
	if err := yaml.UnmarshalStrict(manifest, role); err != nil {
		return nil, err
	}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: user},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}
	for _, obj := range []client.Object{role, binding} {
		if err := k8s.Create(ctx, obj); err != nil {
			return nil, err
		}
	}
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
			User:               user,
			ResourceAttributes: &authorizationv1.ResourceAttributes{Resource: "nodes", Verb: "list"},
		}}
		if err := k8s.Create(ctx, review); err != nil {
			return false, err
		}
		return review.Status.Allowed, nil
	})
	if err != nil {
		return nil, err
	}
	controller := rest.CopyConfig(cfg)
	controller.Impersonate = rest.ImpersonationConfig{UserName: user}
	return controller, nil
}

func TestTheEndpointIsPublishedBeforeAnythingWaits(t *testing.T) {
	cp := controlPlane(t, lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{
		Class:        "example.com/lb",
		SourceRanges: []lodiav1alpha1.CIDR{"198.51.100.0/24"},
		Annotations:  map[string]string{"example.com/tier": "control-plane"},
	}})
	eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint)
	})
	svc := getService(t, cp)
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer || *svc.Spec.LoadBalancerClass != "example.com/lb" ||
		!slices.Equal(svc.Spec.LoadBalancerSourceRanges, []string{"198.51.100.0/24"}) || svc.Annotations["example.com/tier"] != "control-plane" {
		t.Errorf("the Service is %s with class %v, ranges %v and annotations %v", svc.Spec.Type, svc.Spec.LoadBalancerClass, svc.Spec.LoadBalancerSourceRanges, svc.Annotations)
	}
	if e := get(t, cp).Status.Endpoint; e != nil {
		t.Errorf("status.endpoint is %+v before the load balancer has an address", e)
	}

	setIngress(t, cp, corev1.LoadBalancerIngress{Hostname: "lb.example.com"})
	eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonLoadBalancerHostnameUnsupported)
	})
	if e := get(t, cp).Status.Endpoint; e != nil {
		t.Errorf("status.endpoint is %+v while the load balancer has a host name alone", e)
	}

	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget) })
	want := lodiav1alpha1.EndpointStatus{
		APIServer:    lodiav1alpha1.HostPort{Host: lbAddress, Port: 443},
		Konnectivity: lodiav1alpha1.HostPort{Host: lbAddress, Port: 8132},
		InCluster:    lodiav1alpha1.HostPort{Host: lbAddress, Port: 6443},
	}
	if e := get(t, cp).Status.Endpoint; e == nil || *e != want {
		t.Errorf("status.endpoint is %+v, want %+v", e, want)
	}
}

func TestANodePortControlPlanePublishesItsNodePorts(t *testing.T) {
	createNode(t, "node-a", false, "192.0.2.10")
	createNode(t, "node-b", true, "192.0.2.11")
	createNode(t, "node-c", true, "192.0.2.12")
	cp := controlPlane(t, lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}})
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget) })
	svc := getService(t, cp)
	api, konnectivity := nodePort(svc, 6443), nodePort(svc, 8132)
	e := get(t, cp).Status.Endpoint
	want := lodiav1alpha1.EndpointStatus{
		APIServer:    lodiav1alpha1.HostPort{Host: "192.0.2.11", Port: api},
		Konnectivity: lodiav1alpha1.HostPort{Host: "192.0.2.11", Port: konnectivity},
		InCluster:    lodiav1alpha1.HostPort{Host: "192.0.2.11", Port: api},
	}
	if svc.Spec.Type != corev1.ServiceTypeNodePort || api == 0 || konnectivity == 0 || e == nil || *e != want {
		t.Fatalf("Service %s with node ports %d and %d, status.endpoint %+v", svc.Spec.Type, api, konnectivity, e)
	}

	inputsReady(t, cp)
	deployment := eventuallyDeployment(t, cp, "kube-apiserver")
	port := strconv.Itoa(int(api))
	expectFlags(t, deployment.Spec.Template.Spec.Containers[0].Command,
		"--advertise-address=192.0.2.11", "--secure-port=6443", "--service-account-issuer=https://192.0.2.11:"+port)
	if p := deployment.Spec.Template.Spec.Containers[0].Ports; len(p) != 1 || p[0].Name != "https" || p[0].ContainerPort != 6443 {
		t.Errorf("the API server listens on %+v, want 6443", p)
	}

	setReady(t, "node-b", false)
	reconcileNow(t, cp)
	if e := get(t, cp).Status.Endpoint; e == nil || *e != want {
		t.Errorf("status.endpoint is %+v once node-b is NotReady, want %+v", e, want)
	}
	createNode(t, "node-0", true, "192.0.2.9")
	reconcileNow(t, cp)
	if e := get(t, cp).Status.Endpoint; e == nil || *e != want {
		t.Errorf("status.endpoint is %+v once node-0 sorts first, want %+v", e, want)
	}
	if err := k8s.Delete(t.Context(), &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}}); err != nil {
		t.Fatal(err)
	}
	reconcileNow(t, cp)
	want.APIServer.Host, want.Konnectivity.Host, want.InCluster.Host = "192.0.2.9", "192.0.2.9", "192.0.2.9"
	if e := get(t, cp).Status.Endpoint; e == nil || *e != want {
		t.Errorf("status.endpoint is %+v once node-b is gone, want %+v", e, want)
	}
}

func TestAControlPlaneRollsOutOnceItsInputsAreReady(t *testing.T) {
	cp := controlPlane(t, loadBalancer())
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget) })

	setTarget(t, cp, version)
	eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForCertificates)
	})
	setTrue(t, cp, lodiav1alpha1.ConditionCertificatesReady, "Issued")
	eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore)
	})
	setTrue(t, cp, lodiav1alpha1.ConditionDatastoreReady, "Ready")
	eventually(t, func() error {
		c := condition(t, cp)
		if c == nil || c.Reason != lodiav1alpha1.ReasonWaitingForDatastore || !strings.Contains(c.Message, "status.datastore.endpoints") {
			return fmt.Errorf("APIServerReady is %+v, want status.datastore.endpoints awaited", c)
		}
		return nil
	})
	setDatastore(t, cp, lodiav1alpha1.HostPort{Host: "etcd-0.dev.example", Port: 2379}, lodiav1alpha1.HostPort{Host: "2001:db8::5", Port: 12379})
	unhashed := "dev-ca-bundle, dev-etcd-ca, dev-front-proxy-client, dev-konnectivity-kubeconfig, dev-konnectivity-server"
	eventually(t, func() error {
		c := condition(t, cp)
		if c == nil || c.Reason != lodiav1alpha1.ReasonWaitingForCertificates || !strings.HasSuffix(c.Message, ": "+unhashed) {
			return fmt.Errorf("APIServerReady is %+v, want the restart-only Secrets named", c)
		}
		return nil
	})
	createSecrets(t, cp, "")
	reconcileNow(t, cp)
	if c := condition(t, cp); c.Reason != lodiav1alpha1.ReasonWaitingForCertificates || !strings.HasSuffix(c.Message, ": "+unhashed) {
		t.Errorf("APIServerReady is %s %q while no Secret carries its data hash", c.Reason, c.Message)
	}
	if deployments(t, cp) != 0 {
		t.Fatal("a Deployment was created before PKI wrote the Secrets it reads once with their data hash")
	}

	createSecrets(t, cp, "one")
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRollingOut) })
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady) })
	if c := condition(t, cp); c.Message != "kube-apiserver, kube-controller-manager and kube-scheduler run "+version+", every replica updated and ready" {
		t.Errorf("APIServerReady says %q", c.Message)
	}

	images, _ := versions.For(version)
	api := deployment(t, cp, "kube-apiserver")
	pod := api.Spec.Template.Spec
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Error("the API server pod mounts a ServiceAccount token of the management cluster")
	}
	if len(pod.Containers) != 1 || pod.Containers[0].Image != images.APIServer || len(pod.InitContainers) != 2 || pod.InitContainers[1].Image != images.KonnectivityServer {
		t.Fatalf("the API server pod runs %+v and %+v", pod.InitContainers, pod.Containers)
	}
	expectFlags(t, pod.Containers[0].Command,
		"--advertise-address="+lbAddress,
		"--secure-port=6443",
		"--etcd-servers=https://etcd-0.dev.example:2379,https://[2001:db8::5]:12379",
		"--service-account-signing-endpoint=/run/signer/signer.sock",
		"--service-account-issuer=https://"+lbAddress,
		"--client-ca-file=/etc/lodia/client-ca/ca.crt",
		"--enable-bootstrap-token-auth=true",
		"--authentication-config=/etc/lodia/authentication/authentication-config.yaml",
		"--egress-selector-config-file=/etc/lodia/egress/egress-selector.yaml",
	)
	for _, flag := range pod.Containers[0].Command {
		if strings.HasPrefix(flag, "--service-account-key-file") || strings.HasPrefix(flag, "--anonymous-auth") {
			t.Errorf("the API server runs with %s, which the signer endpoint and the authentication configuration exclude", flag)
		}
	}
	expectFlags(t, pod.InitContainers[1].Command, "--kubeconfig=/etc/lodia/konnectivity-kubeconfig/kubeconfig", "--uds-name=/run/konnectivity/konnectivity-server.socket", "--agent-port=8132")
	if pod.InitContainers[0].Image != signerImage || pod.InitContainers[0].RestartPolicy == nil || *pod.InitContainers[0].RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Errorf("the signer is %+v, want a native sidecar of %s", pod.InitContainers[0], signerImage)
	}

	kcm := deployment(t, cp, "kube-controller-manager").Spec.Template.Spec.Containers[0]
	if kcm.Image != images.ControllerManager {
		t.Errorf("the controller manager runs %s", kcm.Image)
	}
	expectFlags(t, kcm.Command,
		"--cluster-signing-kubelet-client-cert-file=/etc/lodia/kubelet-signer/tls.crt",
		"--cluster-signing-duration=720h0m0s",
		"--controllers=*,bootstrap-signer-controller,token-cleaner-controller",
		"--root-ca-file=/etc/lodia/ca-bundle/ca.crt",
		"--cluster-cidr=10.244.0.0/16",
		"--service-cluster-ip-range=10.96.0.0/16",
	)
	expectFlags(t, deployment(t, cp, "kube-scheduler").Spec.Template.Spec.Containers[0].Command, "--kubeconfig=/etc/lodia/kubeconfig/kubeconfig", "--leader-elect=true")

	authn := &corev1.ConfigMap{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: "dev-authentication-config"}, authn); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(authn.Data["authentication-config.yaml"], "path: /api/v1/namespaces/kube-public/configmaps/cluster-info") {
		t.Errorf("the authentication configuration does not let kubeadm join read cluster-info:\n%s", authn.Data["authentication-config.yaml"])
	}

	policies := &networkingv1.NetworkPolicyList{}
	if err := k8s.List(t.Context(), policies, client.InNamespace(cp.Namespace)); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, p := range policies.Items {
		var rules []string
		for _, rule := range p.Spec.Ingress {
			var ports []string
			for _, port := range rule.Ports {
				ports = append(ports, port.Port.String())
			}
			rules = append(rules, fmt.Sprintf("from %d peers to %v", len(rule.From), ports))
		}
		selected := p.Spec.PodSelector.MatchLabels
		got[p.Name] = fmt.Sprintf("%s/%s %v %v", selected[lodiav1alpha1.ControlPlaneLabel], selected["app.kubernetes.io/name"], p.Spec.PolicyTypes, rules)
	}
	want := map[string]string{
		"dev-kube-apiserver":          "dev/kube-apiserver [Ingress] [from 0 peers to [https 8132]]",
		"dev-kube-controller-manager": "dev/kube-controller-manager [Ingress] []",
		"dev-kube-scheduler":          "dev/kube-scheduler [Ingress] []",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("the NetworkPolicies are %v, want %v", got, want)
	}
}

func TestOnlyARestartOnlyInputRollsItsComponent(t *testing.T) {
	cp := running(t)
	before := hashes(t, cp)
	if before["kube-apiserver"] == "" || before["kube-controller-manager"] == "" || before["kube-scheduler"] != "" {
		t.Fatalf("restart hashes %v", before)
	}
	generations := generationsOf(t, cp)

	setSecret(t, cp, lodiav1alpha1.APIServerServingSecret, "renewed")
	reconcileNow(t, cp)
	if after := generationsOf(t, cp); fmt.Sprint(after) != fmt.Sprint(generations) {
		t.Errorf("a renewal of the reloaded apiserver-serving alone rolled: generations %v, then %v", generations, after)
	}

	setSecret(t, cp, lodiav1alpha1.FrontProxyClientSecret, "renewed")
	eventually(t, func() error {
		if after := hashes(t, cp); after["kube-apiserver"] == before["kube-apiserver"] || after["kube-controller-manager"] != before["kube-controller-manager"] {
			return fmt.Errorf("restart hashes %v, then %v: want the API server's alone to change", before, after)
		}
		return nil
	})

	middle := hashes(t, cp)
	setSecret(t, cp, lodiav1alpha1.CABundleSecret, "renewed")
	eventually(t, func() error {
		if after := hashes(t, cp); after["kube-controller-manager"] == middle["kube-controller-manager"] || after["kube-apiserver"] != middle["kube-apiserver"] {
			return fmt.Errorf("restart hashes %v, then %v: want the controller manager's alone to change", middle, after)
		}
		return nil
	})
}

func TestTheSpecIsRendered(t *testing.T) {
	cp := controlPlane(t, loadBalancer(), func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.APIServer = &lodiav1alpha1.APIServer{
			ExtraArgs:    map[string]string{"audit-log-maxage": "7", "enable-admission-plugins": "PodSecurity,NodeRestriction"},
			FeatureGates: map[string]bool{"B": false, "A": true},
			Resources:    &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}},
		}
		spec.ControllerManager = &lodiav1alpha1.Component{ExtraArgs: map[string]string{"node-monitor-grace-period": "50s", "controllers": "-ttl-controller"}}
		spec.ServiceAccount = &lodiav1alpha1.ServiceAccount{Issuer: "https://issuer.example.com"}
		spec.Addons = &lodiav1alpha1.Addons{Konnectivity: &lodiav1alpha1.Konnectivity{Version: "v0.37.2"}}
		spec.Placement = &lodiav1alpha1.Placement{
			NodeSelector:      map[string]string{"node-role.example.com/control-plane": ""},
			Tolerations:       []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}},
			PriorityClassName: "system-cluster-critical",
		}
	})
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReady(t, cp)
	api := eventuallyDeployment(t, cp, "kube-apiserver")
	pod := api.Spec.Template.Spec
	expectFlags(t, pod.Containers[0].Command, "--audit-log-maxage=7", "--feature-gates=A=true,B=false", "--service-account-issuer=https://issuer.example.com",
		"--enable-admission-plugins=NodeRestriction,PodSecurity")
	if pod.Containers[0].Resources.Requests.Memory().String() != "1Gi" || !strings.HasSuffix(pod.InitContainers[1].Image, ":v0.37.2") {
		t.Errorf("the API server requests %v, konnectivity %s", pod.Containers[0].Resources.Requests, pod.InitContainers[1].Image)
	}
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		pod := eventuallyDeployment(t, cp, component).Spec.Template.Spec
		if pod.PriorityClassName != "system-cluster-critical" || len(pod.Tolerations) != 1 || pod.NodeSelector["node-role.example.com/control-plane"] != "" || len(pod.NodeSelector) != 1 {
			t.Errorf("%s is placed with %v, %v and %q", component, pod.NodeSelector, pod.Tolerations, pod.PriorityClassName)
		}
	}
	expectFlags(t, deployment(t, cp, "kube-controller-manager").Spec.Template.Spec.Containers[0].Command, "--node-monitor-grace-period=50s",
		"--controllers=*,bootstrap-signer-controller,token-cleaner-controller,-ttl-controller")
}

func TestWhatIsNotBuiltYetIsSaid(t *testing.T) {
	gateway := controlPlane(t, lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
		ParentRef: lodiav1alpha1.ParentReference{Name: "shared", Namespace: "gateways"},
		Hostnames: lodiav1alpha1.GatewayHostnames{APIServer: "api.dev.example.com", Konnectivity: "konnectivity.dev.example.com"},
	}})
	eventually(t, func() error { return expectCondition(gateway, metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported) })
	if c := condition(t, gateway); c.Message != "not built yet: endpoint.gateway" {
		t.Errorf("APIServerReady says %q", c.Message)
	}

	ha := controlPlane(t, loadBalancer(), func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
		spec.Scheduler = &lodiav1alpha1.Scheduler{ConfigRef: &lodiav1alpha1.LocalReference{Name: "profiles"}}
	})
	eventually(t, func() error { return expectCondition(ha, metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported) })
	if c := condition(t, ha); c.Message != "not built yet: highAvailability, scheduler.configRef" {
		t.Errorf("APIServerReady says %q", c.Message)
	}
	setIngress(t, ha, corev1.LoadBalancerIngress{IP: lbAddress})
	eventually(t, func() error {
		if e := get(t, ha).Status.Endpoint; e == nil || e.APIServer.Host != lbAddress {
			return fmt.Errorf("status.endpoint is %+v", e)
		}
		return expectCondition(ha, metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported)
	})
	if getServiceOrNil(t, gateway) != nil {
		t.Errorf("a Service was created in %s for a gateway endpoint", gateway.Namespace)
	}

	ignored := controlPlane(t, loadBalancer(), func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.APIServer = &lodiav1alpha1.APIServer{Replicas: new(int32(2)), AdvertiseAddress: "172.20.0.1"}
		spec.ServiceAccount = &lodiav1alpha1.ServiceAccount{PublicDiscovery: true}
	})
	eventually(t, func() error { return expectCondition(ignored, metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported) })
	if c := condition(t, ignored); c.Message != "not built yet: apiServer.replicas, apiServer.advertiseAddress, serviceAccount.publicDiscovery" {
		t.Errorf("APIServerReady says %q", c.Message)
	}
}

func TestAnUnsupportedFieldKeepsTheEndpoint(t *testing.T) {
	cp := running(t)
	published := get(t, cp).Status.Endpoint
	fresh := get(t, cp)
	fresh.Spec.ServiceAccount = &lodiav1alpha1.ServiceAccount{PublicDiscovery: true}
	if err := k8s.Update(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported) })
	if e := get(t, cp).Status.Endpoint; e == nil || *e != *published {
		t.Errorf("status.endpoint is %+v once the ControlPlane asks for a field not built yet, want %+v", e, published)
	}
}

func TestEveryFlagLodiaRendersIsRefusedOrMerged(t *testing.T) {
	cp := running(t)
	merged := map[string]bool{"enable-admission-plugins": true, "controllers": true}
	components := map[string]struct {
		alsoRefused []string
		set         func(*lodiav1alpha1.ControlPlaneSpec, map[string]string)
	}{
		"kube-apiserver": {[]string{"anonymous-auth", "feature-gates", "service-account-key-file", "service-account-signing-key-file", "shutdown-send-retry-after"},
			func(spec *lodiav1alpha1.ControlPlaneSpec, args map[string]string) {
				spec.APIServer = &lodiav1alpha1.APIServer{ExtraArgs: args}
			}},
		"kube-controller-manager": {[]string{"cluster-signing-cert-file", "cluster-signing-key-file", "feature-gates", "service-account-private-key-file"},
			func(spec *lodiav1alpha1.ControlPlaneSpec, args map[string]string) {
				spec.ControllerManager = &lodiav1alpha1.Component{ExtraArgs: args}
			}},
		"kube-scheduler": {[]string{"config", "feature-gates"},
			func(spec *lodiav1alpha1.ControlPlaneSpec, args map[string]string) {
				spec.Scheduler = &lodiav1alpha1.Scheduler{ExtraArgs: args}
			}},
	}
	for component, c := range components {
		flags := c.alsoRefused
		for _, arg := range deployment(t, cp, component).Spec.Template.Spec.Containers[0].Command[1:] {
			if name, _, _ := strings.Cut(strings.TrimPrefix(arg, "--"), "="); !merged[name] {
				flags = append(flags, name)
			}
		}
		for _, flag := range flags {
			spec := cp.Spec.DeepCopy()
			c.set(spec, map[string]string{flag: "set by the tenant"})
			tenant := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "flags", Namespace: cp.Namespace}, Spec: *spec}
			if err := k8s.Create(t.Context(), tenant, client.DryRunAll); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "Lodia sets this flag") {
				t.Errorf("the extra arguments of %s take --%s: %v", component, flag, err)
			}
		}
	}
}

func TestTheProbesAreKubeadms(t *testing.T) {
	cp := running(t)
	describe := func(p *corev1.Probe) string {
		if p == nil {
			return "none"
		}
		return fmt.Sprintf("%s %s %s %d/%d/%d/%d", p.HTTPGet.Scheme, p.HTTPGet.Path, p.HTTPGet.Port.String(), p.InitialDelaySeconds, p.TimeoutSeconds, p.FailureThreshold, p.PeriodSeconds)
	}
	for component, want := range map[string][3]string{
		"kube-apiserver":          {"HTTPS /livez https 10/15/8/10", "HTTPS /readyz https 0/15/3/1", "HTTPS /livez https 10/15/24/10"},
		"kube-controller-manager": {"HTTPS /healthz 10257 10/15/8/10", "none", "HTTPS /healthz 10257 10/15/24/10"},
		"kube-scheduler":          {"HTTPS /livez 10259 10/15/8/10", "HTTPS /readyz 10259 0/15/3/1", "HTTPS /livez 10259 10/15/24/10"},
	} {
		c := deployment(t, cp, component).Spec.Template.Spec.Containers[0]
		if got := [3]string{describe(c.LivenessProbe), describe(c.ReadinessProbe), describe(c.StartupProbe)}; got != want {
			t.Errorf("%s probes liveness, readiness and startup with %q, want kubeadm's %q", component, got, want)
		}
	}
}

func TestARolloutSurgesAndTheOldAPIServerDrains(t *testing.T) {
	cp := running(t)
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		d := deployment(t, cp, component)
		s := d.Spec.Strategy
		if s.Type != appsv1.RollingUpdateDeploymentStrategyType || s.RollingUpdate == nil || s.RollingUpdate.MaxSurge.String() != "100%" || s.RollingUpdate.MaxUnavailable.String() != "0" {
			t.Errorf("%s rolls with %+v, want a surge of 100%% and none unavailable", component, s)
		}
		if want := map[string]int32{"kube-apiserver": 10}[component]; d.Spec.MinReadySeconds != want {
			t.Errorf("%s counts a pod available after %ds ready, want %d", component, d.Spec.MinReadySeconds, want)
		}
	}
	api := deployment(t, cp, "kube-apiserver").Spec.Template.Spec
	expectFlags(t, api.Containers[0].Command, "--endpoint-reconciler-type=none", "--shutdown-delay-duration=15s", "--shutdown-watch-termination-grace-period=10s")
	if grace := api.TerminationGracePeriodSeconds; grace == nil || *grace != 90 {
		t.Errorf("the API server pod has a termination grace period of %v, want 90", grace)
	}
	var sidecars []string
	for _, c := range api.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			sidecars = append(sidecars, c.Name)
		}
	}
	if !slices.Equal(sidecars, []string{"lodia-signer", "konnectivity-server"}) || len(api.InitContainers) != 2 {
		t.Fatalf("the API server pod starts %+v, want the signer and then konnectivity-server as native sidecars", api.InitContainers)
	}
	konnectivity := api.InitContainers[1]
	expectFlags(t, konnectivity.Command, "--graceful-shutdown-timeout=10s", "--keepalive-time=30s", "--health-port=8134")
	if p := konnectivity.StartupProbe; p == nil || p.HTTPGet == nil || p.HTTPGet.Path != "/healthz" || p.HTTPGet.Port.IntValue() != 8134 {
		t.Errorf("konnectivity-server's startup probe is %+v, want /healthz on its health port", p)
	}
}

func TestOrphansUnderWorkloadsNamesAreAdopted(t *testing.T) {
	ns := namespace(t)
	labels := map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev"}
	scheduler := map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev", "app.kubernetes.io/name": "kube-scheduler"}
	orphans := []client.Object{
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "dev-kube-apiserver", Namespace: ns, Labels: labels},
			Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "https", Port: 443}}},
		},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "dev-egress-selector", Namespace: ns, Labels: labels}},
		&networkingv1.NetworkPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "dev-kube-scheduler", Namespace: ns, Labels: labels},
			Spec:       networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: scheduler}},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "dev-kube-scheduler", Namespace: ns, Labels: labels},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: scheduler},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: scheduler},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "kube-scheduler", Image: "registry.example/orphan"}}},
				},
			},
		},
	}
	for _, orphan := range orphans {
		if err := k8s.Create(t.Context(), orphan); err != nil {
			t.Fatal(err)
		}
	}
	cp := create(t, ns, loadBalancer())
	eventually(t, func() error {
		if svc := getService(t, cp); !metav1.IsControlledBy(svc, cp) || svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
			return fmt.Errorf("the Service is %s, controlled by %v; APIServerReady %+v", svc.Spec.Type, metav1.GetControllerOf(svc), condition(t, cp))
		}
		return nil
	})
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReady(t, cp)
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRollingOut) })
	for _, orphan := range orphans {
		fresh := orphan.DeepCopyObject().(client.Object)
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(orphan), fresh); err != nil {
			t.Fatal(err)
		}
		if !metav1.IsControlledBy(fresh, cp) {
			t.Errorf("%T %s is controlled by %v", orphan, orphan.GetName(), metav1.GetControllerOf(fresh))
		}
	}
}

func TestConditionsFromState(t *testing.T) {
	ns := namespace(t)
	events, err := k8s.Watch(t.Context(), &lodiav1alpha1.ControlPlaneList{}, client.InNamespace(ns))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Stop()
	cp := create(t, ns, loadBalancer())
	var first *metav1.Condition
	for first == nil {
		select {
		case event := <-events.ResultChan():
			if fresh, ok := event.Object.(*lodiav1alpha1.ControlPlane); ok && event.Type == watch.Modified {
				first = meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("no APIServerReady condition was written")
		}
	}
	if first.Status != metav1.ConditionUnknown || first.Reason != lodiav1alpha1.ReasonReconciling {
		t.Errorf("the first APIServerReady is %s/%s %q, want Unknown/%s", first.Status, first.Reason, first.Message, lodiav1alpha1.ReasonReconciling)
	}
	eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint)
	})

	before := get(t, cp).ResourceVersion
	injected := errors.New("injected: the API server did not answer")
	failing := interceptor.NewClient(k8s, interceptor.Funcs{
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			return injected
		},
	})
	reconciler := &workloads.Reconciler{Client: failing, Reader: failing, SignerImage: signerImage}
	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cp.Namespace, Name: cp.Name}}); !errors.Is(err, injected) {
		t.Fatalf("a pass whose Service apply fails returned %v, want the apply's error", err)
	}
	if after := get(t, cp).ResourceVersion; after != before {
		t.Errorf("a pass that failed wrote the ControlPlane: resourceVersion %s, then %s", before, after)
	}
}

func TestAPassThatChangesNothingWritesNothing(t *testing.T) {
	cp := running(t)
	versionsOf := func() map[string]string {
		out := map[string]string{"ControlPlane": get(t, cp).ResourceVersion}
		for _, list := range []client.ObjectList{&appsv1.DeploymentList{}, &corev1.ServiceList{}, &corev1.ConfigMapList{}, &networkingv1.NetworkPolicyList{}} {
			if err := k8s.List(t.Context(), list, client.InNamespace(cp.Namespace)); err != nil {
				t.Fatal(err)
			}
			items, err := meta.ExtractList(list)
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range items {
				o := item.(client.Object)
				out[fmt.Sprintf("%T/%s", o, o.GetName())] = o.GetResourceVersion()
			}
		}
		return out
	}
	before := versionsOf()
	reconcileNow(t, cp)
	if after := versionsOf(); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Errorf("a pass over an unchanged ControlPlane wrote:\n%v\nthen\n%v", before, after)
	}
}

func TestAPredecessorsServiceIsNotWrittenOver(t *testing.T) {
	ns := namespace(t)
	predecessor := types.UID("5d1e8a52-5f0e-4bd3-a1a8-0f3c0e6a8b11")
	held := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      lodiav1alpha1.APIServerServiceName("dev"),
			Namespace: ns,
			Labels:    map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "dev", UID: predecessor, Controller: new(true),
			}},
		},
		Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "https", Port: 443}}},
	}
	if err := k8s.Create(t.Context(), held); err != nil {
		t.Fatal(err)
	}
	cp := create(t, ns, loadBalancer())
	eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere)
	})
	if c := condition(t, cp); !strings.Contains(c.Message, "Service dev-kube-apiserver by ControlPlane dev (UID "+string(predecessor)+")") {
		t.Errorf("APIServerReady says %q", c.Message)
	}
	if svc := getService(t, cp); svc.ResourceVersion != held.ResourceVersion {
		t.Error("the predecessor's Service was written")
	}
	if e := get(t, cp).Status.Endpoint; e != nil {
		t.Errorf("status.endpoint is %+v, from the predecessor's Service", e)
	}
	if result := reconcileNow(t, cp); result.RequeueAfter != 10*time.Second {
		t.Errorf("requeue after %s, want 10s while the garbage collector deletes a predecessor's Service", result.RequeueAfter)
	}

	if err := k8s.Delete(t.Context(), held); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint)
	})
	if svc := getService(t, cp); !metav1.IsControlledBy(svc, cp) || svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
		t.Errorf("the Service is %s, controlled by %v", svc.Spec.Type, metav1.GetControllerOf(svc))
	}
}

func TestAnObjectAnotherControlPlaneKeepsHoldsEveryWorkload(t *testing.T) {
	ns := namespace(t)
	other := types.UID("8f0c6f2e-0d6e-4f4e-9e44-0d6a7c1b5a21")
	pod := map[string]string{"app": "held"}
	held := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dev-kube-scheduler",
			Namespace: ns,
			Labels:    map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "prod", UID: other, Controller: new(true),
			}},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: pod},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: pod},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "held", Image: "registry.example/held"}}},
			},
		},
	}
	if err := k8s.Create(t.Context(), held); err != nil {
		t.Fatal(err)
	}
	cp := create(t, ns, loadBalancer())
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReady(t, cp)
	eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere)
	})
	if c := condition(t, cp); !strings.Contains(c.Message, "Deployment dev-kube-scheduler by ControlPlane prod (UID "+string(other)+")") {
		t.Errorf("APIServerReady says %q", c.Message)
	}
	if result := reconcileNow(t, cp); result.RequeueAfter != time.Minute {
		t.Errorf("requeue after %s, want a minute while another ControlPlane keeps a Deployment", result.RequeueAfter)
	}
	written := 0
	for _, list := range []client.ObjectList{&appsv1.DeploymentList{}, &corev1.ConfigMapList{}, &networkingv1.NetworkPolicyList{}} {
		if err := k8s.List(t.Context(), list, client.InNamespace(ns)); err != nil {
			t.Fatal(err)
		}
		written += meta.LenList(list)
	}
	if fresh := deployment(t, cp, "kube-scheduler"); written != 1 || fresh.ResourceVersion != held.ResourceVersion {
		t.Errorf("%d objects in the namespace, the held Deployment at resourceVersion %s: want the held one alone, unchanged", written, fresh.ResourceVersion)
	}
	if e := get(t, cp).Status.Endpoint; e == nil || e.APIServer.Host != lbAddress {
		t.Errorf("status.endpoint is %+v, want it published from the ControlPlane's own Service", e)
	}

	if err := k8s.Delete(t.Context(), held); err != nil {
		t.Fatal(err)
	}
	reconcileNow(t, cp)
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRollingOut) })
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		if d := deployment(t, cp, component); !metav1.IsControlledBy(d, cp) {
			t.Errorf("%s is controlled by %v", d.Name, metav1.GetControllerOf(d))
		}
	}
}

func TestAQuotaThatRefusesTheServiceIsSaid(t *testing.T) {
	ns := namespace(t)
	none := corev1.ResourceList{corev1.ResourceServicesLoadBalancers: resource.MustParse("0")}
	quota := &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: "no-load-balancers", Namespace: ns}, Spec: corev1.ResourceQuotaSpec{Hard: none}}
	if err := k8s.Create(t.Context(), quota); err != nil {
		t.Fatal(err)
	}
	quota.Status = corev1.ResourceQuotaStatus{Hard: none, Used: none}
	if err := k8s.Status().Update(t.Context(), quota); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		probe := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: ns},
			Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, Ports: []corev1.ServicePort{{Port: 443}}},
		}
		if err := k8s.Create(t.Context(), probe, client.DryRunAll); !apistatus.Refused(err) || !strings.Contains(err.Error(), "exceeded quota") {
			return fmt.Errorf("the quota does not refuse a load balancer yet: %w", err)
		}
		return nil
	})

	cp := create(t, ns, loadBalancer())
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused) })
	if c := condition(t, cp); c.Message != "the API server refuses Service dev-kube-apiserver; the reason is in Lodia's log" {
		t.Errorf("APIServerReady says %q", c.Message)
	}
	reconciler := &workloads.Reconciler{Client: k8s, Reader: k8s, SignerImage: signerImage}
	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !apistatus.Refused(err) {
		t.Errorf("a pass whose Service is refused returned %v, want the refusal for the backoff", err)
	}

	if err := k8s.Delete(t.Context(), quota); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
			return err
		}
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint)
	})
}

func TestAPolicyThatRefusesADeploymentIsSaid(t *testing.T) {
	ns := namespace(t)
	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "refuse-schedulers-in-" + ns},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			FailurePolicy: new(admissionregistrationv1.Fail),
			MatchConstraints: &admissionregistrationv1.MatchResources{ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
				RuleWithOperations: admissionregistrationv1.RuleWithOperations{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
					Rule:       admissionregistrationv1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1"}, Resources: []string{"deployments"}},
				},
			}}},
			Validations: []admissionregistrationv1.Validation{{
				Expression: "object.metadata.namespace != '" + ns + "' || object.metadata.name != 'dev-kube-scheduler'",
				Message:    "no scheduler in " + ns,
			}},
		},
	}
	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: policy.Name},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        policy.Name,
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
		},
	}
	for _, obj := range []client.Object{policy, binding} {
		if err := k8s.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := client.IgnoreNotFound(k8s.Delete(context.Background(), obj)); err != nil {
				t.Error(err)
			}
		})
	}
	pod := map[string]string{"app": "probe"}
	eventually(t, func() error {
		probe := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "dev-kube-scheduler", Namespace: ns},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: pod},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: pod},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "probe", Image: "registry.example/probe"}}},
				},
			},
		}
		if err := k8s.Create(t.Context(), probe, client.DryRunAll); err == nil || !strings.Contains(err.Error(), "no scheduler in "+ns) {
			return fmt.Errorf("the policy does not refuse the scheduler yet: %w", err)
		}
		return nil
	})

	cp := create(t, ns, loadBalancer())
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReady(t, cp)
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused) })
	if c := condition(t, cp); c.Message != "the API server refuses Deployment dev-kube-scheduler; the reason is in Lodia's log" {
		t.Errorf("APIServerReady says %q", c.Message)
	}
	if e := get(t, cp).Status.Endpoint; e == nil || e.APIServer.Host != lbAddress {
		t.Errorf("status.endpoint is %+v while the scheduler is refused", e)
	}

	if err := k8s.Delete(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	reconciler := &workloads.Reconciler{Client: k8s, Reader: k8s, SignerImage: signerImage}
	eventually(t, func() error {
		if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
			return err
		}
		return expectCondition(cp, metav1.ConditionFalse, lodiav1alpha1.ReasonRollingOut)
	})
}

func loadBalancer() lodiav1alpha1.Endpoint {
	return lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}}
}

func namespace(t *testing.T) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "workloads-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	return ns.Name
}

func controlPlane(t *testing.T, endpoint lodiav1alpha1.Endpoint, change ...func(*lodiav1alpha1.ControlPlaneSpec)) *lodiav1alpha1.ControlPlane {
	t.Helper()
	return create(t, namespace(t), endpoint, change...)
}

func create(t *testing.T, ns string, endpoint lodiav1alpha1.Endpoint, change ...func(*lodiav1alpha1.ControlPlaneSpec)) *lodiav1alpha1.ControlPlane {
	t.Helper()
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   version,
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint:  endpoint,
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	for _, c := range change {
		c(&cp.Spec)
	}
	if err := k8s.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

// running brings a ControlPlane to APIServerReady=True.
func running(t *testing.T) *lodiav1alpha1.ControlPlane {
	t.Helper()
	cp := controlPlane(t, loadBalancer())
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReady(t, cp)
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReady) })
	return cp
}

func inputsReady(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	setTarget(t, cp, version)
	setTrue(t, cp, lodiav1alpha1.ConditionCertificatesReady, "Issued")
	setTrue(t, cp, lodiav1alpha1.ConditionDatastoreReady, "Ready")
	setDatastore(t, cp, lodiav1alpha1.HostPort{Host: "dev-etcd-client." + cp.Namespace + ".svc", Port: 2379})
	createSecrets(t, cp, "one")
	setSecret(t, cp, lodiav1alpha1.APIServerServingSecret, "one")
}

func reconcileNow(t *testing.T, cp *lodiav1alpha1.ControlPlane) ctrl.Result {
	t.Helper()
	reconciler := &workloads.Reconciler{Client: k8s, Reader: k8s, SignerImage: signerImage}
	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func get(t *testing.T, cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.ControlPlane {
	t.Helper()
	fresh := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		t.Fatal(err)
	}
	return fresh
}

func condition(t *testing.T, cp *lodiav1alpha1.ControlPlane) *metav1.Condition {
	t.Helper()
	return meta.FindStatusCondition(get(t, cp).Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
}

func expectCondition(cp *lodiav1alpha1.ControlPlane, status metav1.ConditionStatus, reason string) error {
	fresh := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		return err
	}
	c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
	if c == nil || c.Status != status || c.Reason != reason || c.ObservedGeneration != fresh.Generation {
		return fmt.Errorf("APIServerReady is %+v, want %s %s at generation %d", c, status, reason, fresh.Generation)
	}
	return nil
}

func applyStatus(t *testing.T, cp *lodiav1alpha1.ControlPlane, owner string, status map[string]any) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     status,
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(owner), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func setTarget(t *testing.T, cp *lodiav1alpha1.ControlPlane, version string) {
	t.Helper()
	applyStatus(t, cp, "test/target", map[string]any{"target": map[string]any{"version": version}})
}

func setDatastore(t *testing.T, cp *lodiav1alpha1.ControlPlane, endpoints ...lodiav1alpha1.HostPort) {
	t.Helper()
	var list []any
	for _, e := range endpoints {
		list = append(list, map[string]any{"host": e.Host, "port": int64(e.Port)})
	}
	applyStatus(t, cp, "test/datastore", map[string]any{"datastore": map[string]any{
		"endpoints": list,
		"members":   map[string]any{"total": int64(1), "ready": int64(1)},
	}})
}

func setTrue(t *testing.T, cp *lodiav1alpha1.ControlPlane, conditionType, reason string) {
	t.Helper()
	applyStatus(t, cp, "test/"+conditionType, map[string]any{"conditions": []any{map[string]any{
		"type":               conditionType,
		"status":             string(metav1.ConditionTrue),
		"reason":             reason,
		"message":            "set by the test",
		"observedGeneration": get(t, cp).Generation,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}}})
}

func createSecrets(t *testing.T, cp *lodiav1alpha1.ControlPlane, hash string) {
	t.Helper()
	for _, s := range restartOnlySecrets {
		setSecret(t, cp, s, hash)
	}
}

func setSecret(t *testing.T, cp *lodiav1alpha1.ControlPlane, s lodiav1alpha1.Secret, hash string) {
	t.Helper()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: s.Name(cp.Name), Namespace: cp.Namespace}}
	err := k8s.Get(t.Context(), client.ObjectKeyFromObject(secret), secret)
	if client.IgnoreNotFound(err) != nil {
		t.Fatal(err)
	}
	secret.Labels = map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name}
	secret.Annotations = map[string]string{lodiav1alpha1.DataHashAnnotation: hash}
	if hash == "" {
		secret.Annotations = nil
	}
	secret.Data = map[string][]byte{"content": []byte("the same data at every hash")}
	if err == nil {
		err = k8s.Update(t.Context(), secret)
	} else {
		err = k8s.Create(t.Context(), secret)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func setIngress(t *testing.T, cp *lodiav1alpha1.ControlPlane, ingress corev1.LoadBalancerIngress) {
	t.Helper()
	eventually(t, func() error {
		svc := getServiceOrNil(t, cp)
		if svc == nil {
			return errors.New("no Service yet")
		}
		svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{ingress}
		if ingress.IP != "" {
			svc.Status.LoadBalancer.Ingress[0].IPMode = new(corev1.LoadBalancerIPModeVIP)
		}
		return k8s.Status().Update(t.Context(), svc)
	})
}

func getService(t *testing.T, cp *lodiav1alpha1.ControlPlane) *corev1.Service {
	t.Helper()
	svc := getServiceOrNil(t, cp)
	if svc == nil {
		t.Fatal("no Service")
	}
	return svc
}

func getServiceOrNil(t *testing.T, cp *lodiav1alpha1.ControlPlane) *corev1.Service {
	t.Helper()
	svc := &corev1.Service{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: lodiav1alpha1.APIServerServiceName(cp.Name)}, svc); err != nil {
		if client.IgnoreNotFound(err) != nil {
			t.Fatal(err)
		}
		return nil
	}
	return svc
}

func nodePort(svc *corev1.Service, port int32) int32 {
	for _, p := range svc.Spec.Ports {
		if p.Port == port {
			return p.NodePort
		}
	}
	return 0
}

func setReady(t *testing.T, name string, ready bool) {
	t.Helper()
	node := &corev1.Node{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: name}, node); err != nil {
		t.Fatal(err)
	}
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: status}}
	if err := k8s.Status().Update(t.Context(), node); err != nil {
		t.Fatal(err)
	}
}

func createNode(t *testing.T, name string, ready bool, address string) {
	t.Helper()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := k8s.Create(t.Context(), node); err != nil {
		t.Fatal(err)
	}
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: status}}
	node.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeHostName, Address: name}, {Type: corev1.NodeInternalIP, Address: address}}
	if err := k8s.Status().Update(t.Context(), node); err != nil {
		t.Fatal(err)
	}
}

func deployment(t *testing.T, cp *lodiav1alpha1.ControlPlane, component string) *appsv1.Deployment {
	t.Helper()
	d := &appsv1.Deployment{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-" + component}, d); err != nil {
		t.Fatal(err)
	}
	return d
}

func eventuallyDeployment(t *testing.T, cp *lodiav1alpha1.ControlPlane, component string) *appsv1.Deployment {
	t.Helper()
	d := &appsv1.Deployment{}
	eventually(t, func() error {
		return k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-" + component}, d)
	})
	return d
}

func deployments(t *testing.T, cp *lodiav1alpha1.ControlPlane) int {
	t.Helper()
	list := &appsv1.DeploymentList{}
	if err := k8s.List(t.Context(), list, client.InNamespace(cp.Namespace)); err != nil {
		t.Fatal(err)
	}
	return len(list.Items)
}

func markAvailable(t *testing.T, d *appsv1.Deployment) {
	t.Helper()
	eventually(t, func() error {
		fresh := &appsv1.Deployment{}
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(d), fresh); err != nil {
			return err
		}
		replicas := *fresh.Spec.Replicas
		fresh.Status = appsv1.DeploymentStatus{ObservedGeneration: fresh.Generation, Replicas: replicas, UpdatedReplicas: replicas, ReadyReplicas: replicas, AvailableReplicas: replicas}
		return k8s.Status().Update(t.Context(), fresh)
	})
}

func generationsOf(t *testing.T, cp *lodiav1alpha1.ControlPlane) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		out[component] = deployment(t, cp, component).Generation
	}
	return out
}

func hashes(t *testing.T, cp *lodiav1alpha1.ControlPlane) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		out[component] = deployment(t, cp, component).Spec.Template.Annotations["lodia.io/restart-hash"]
	}
	return out
}

func expectFlags(t *testing.T, command []string, flags ...string) {
	t.Helper()
	for _, flag := range flags {
		if !slices.Contains(command, flag) {
			t.Errorf("%s is not in %v", flag, command)
		}
	}
}

func eventually(t *testing.T, check func() error) {
	t.Helper()
	var last error
	err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 30*time.Second, true, func(context.Context) (bool, error) {
		last = check()
		return last == nil, nil
	})
	if err != nil {
		t.Fatalf("%v: %v", err, last)
	}
}
