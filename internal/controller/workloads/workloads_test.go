// Copyright 2026 The Brygga Authors.
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

	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/controller/workloads"
	"github.com/segla-io/brygga/internal/versions"
)

const (
	version        = "v1.37.1"
	signerImage    = "registry.example/brygga-signer:test"
	druidNamespace = "etcd-druid"
	lbAddress      = "203.0.113.10"
)

var workloadsRules = []rbacv1.PolicyRule{
	{APIGroups: []string{bryggav1alpha1.GroupVersion.Group}, Resources: []string{"controlplanes"}, Verbs: []string{"list", "watch"}},
	{APIGroups: []string{bryggav1alpha1.GroupVersion.Group}, Resources: []string{"controlplanes/status"}, Verbs: []string{"patch"}},
	{APIGroups: []string{bryggav1alpha1.GroupVersion.Group}, Resources: []string{"controlplanes/finalizers"}, Verbs: []string{"update"}},
	{APIGroups: []string{"apps"}, Resources: []string{"deployments"}, Verbs: []string{"list", "watch", "create", "patch"}},
	{APIGroups: []string{""}, Resources: []string{"services", "configmaps"}, Verbs: []string{"list", "watch", "create", "patch"}},
	{APIGroups: []string{"networking.k8s.io"}, Resources: []string{"networkpolicies"}, Verbs: []string{"list", "watch", "create", "patch"}},
	{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get", "list", "watch"}},
	{APIGroups: []string{""}, Resources: []string{"nodes"}, Verbs: []string{"list"}},
}

var restartOnlySecrets = []string{"ca-bundle", "etcd-ca", "front-proxy-client", "konnectivity-kubeconfig", "konnectivity-server"}

var k8s client.WithWatch

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("skipping the workloads controller tests: KUBEBUILDER_ASSETS is not set; run make test")
		os.Exit(0)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
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
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, bryggav1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			stop("scheme", err)
		}
	}
	if k8s, err = client.NewWithWatch(cfg, client.Options{Scheme: scheme}); err != nil {
		stop("client", err)
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	controller, err := asWorkloadsController(cfg)
	if err != nil {
		stop("the controller's ClusterRole", err)
	}
	labelled, err := labels.NewRequirement(bryggav1alpha1.ControlPlaneLabel, selection.Exists, nil)
	if err != nil {
		stop("label selector", err)
	}
	everything := cache.ByObject{Label: labels.Everything()}
	mgr, err := ctrl.NewManager(controller, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			DefaultLabelSelector:        labels.NewSelector().Add(*labelled),
			DefaultTransform:            cache.TransformStripManagedFields(),
			ReaderFailOnMissingInformer: true,
			ByObject: map[client.Object]cache.ByObject{
				&bryggav1alpha1.ControlPlane{}:      everything,
				&bryggav1alpha1.DatastoreClass{}:    everything,
				&bryggav1alpha1.Datastore{}:         everything,
				&bryggav1alpha1.KubeconfigRequest{}: everything,
				&bryggav1alpha1.Backup{}:            everything,
			},
		},
	})
	if err != nil {
		stop("manager", err)
	}
	reconciler := &workloads.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), SignerImage: signerImage, DruidNamespace: druidNamespace}
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
	const user = "brygga-workloads"
	ctx := context.Background()
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: user}, Rules: workloadsRules}
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
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
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
	cp := controlPlane(t, bryggav1alpha1.Endpoint{LoadBalancer: &bryggav1alpha1.LoadBalancerEndpoint{
		Class:        "example.com/lb",
		SourceRanges: []bryggav1alpha1.CIDR{"198.51.100.0/24"},
		Annotations:  map[string]string{"example.com/tier": "control-plane"},
	}})
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, workloads.ReasonWaitingForEndpoint) })
	svc := getService(t, cp)
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer || *svc.Spec.LoadBalancerClass != "example.com/lb" ||
		!slices.Equal(svc.Spec.LoadBalancerSourceRanges, []string{"198.51.100.0/24"}) || svc.Annotations["example.com/tier"] != "control-plane" {
		t.Errorf("the Service is %s with class %v, ranges %v and annotations %v", svc.Spec.Type, svc.Spec.LoadBalancerClass, svc.Spec.LoadBalancerSourceRanges, svc.Annotations)
	}
	if e := get(t, cp).Status.Endpoint; e == nil || e.APIServer != nil || e.Konnectivity != nil ||
		*e.Internal != (bryggav1alpha1.HostPort{Host: "dev-kube-apiserver." + cp.Namespace + ".svc", Port: 443}) {
		t.Errorf("status.endpoint is %+v, want the in-namespace Service alone", e)
	}

	setIngress(t, cp, corev1.LoadBalancerIngress{Hostname: "lb.example.com"})
	eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, workloads.ReasonLoadBalancerHostnameUnsupported)
	})

	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, workloads.ReasonWaitingForTarget) })
	e := get(t, cp).Status.Endpoint
	if *e.APIServer != (bryggav1alpha1.HostPort{Host: lbAddress, Port: 443}) || *e.Konnectivity != (bryggav1alpha1.HostPort{Host: lbAddress, Port: 8132}) {
		t.Errorf("status.endpoint is %+v %+v, want %s:443 and %s:8132", *e.APIServer, *e.Konnectivity, lbAddress, lbAddress)
	}
}

func TestANodePortControlPlaneListensOnItsNodePort(t *testing.T) {
	createNode(t, "node-a", false, "192.0.2.10")
	createNode(t, "node-b", true, "192.0.2.11")
	createNode(t, "node-c", true, "192.0.2.12")
	cp := controlPlane(t, bryggav1alpha1.Endpoint{NodePort: &bryggav1alpha1.NodePortEndpoint{}})
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, workloads.ReasonWaitingForTarget) })
	svc := getService(t, cp)
	api, konnectivity := nodePort(svc, 6443), nodePort(svc, 8132)
	e := get(t, cp).Status.Endpoint
	if svc.Spec.Type != corev1.ServiceTypeNodePort || api == 0 || konnectivity == 0 ||
		*e.APIServer != (bryggav1alpha1.HostPort{Host: "192.0.2.11", Port: api}) || *e.Konnectivity != (bryggav1alpha1.HostPort{Host: "192.0.2.11", Port: konnectivity}) {
		t.Fatalf("Service %s with node ports %d and %d, status.endpoint %+v %+v", svc.Spec.Type, api, konnectivity, e.APIServer, e.Konnectivity)
	}

	inputsReady(t, cp)
	deployment := eventuallyDeployment(t, cp, "kube-apiserver")
	port := strconv.Itoa(int(api))
	expectFlags(t, deployment.Spec.Template.Spec.Containers[0].Command,
		"--advertise-address=192.0.2.11", "--secure-port="+port, "--service-account-issuer=https://192.0.2.11:"+port)
	if p := deployment.Spec.Template.Spec.Containers[0].Ports; len(p) != 1 || p[0].Name != "https" || p[0].ContainerPort != api {
		t.Errorf("the API server listens on %+v, want its node port %d", p, api)
	}
}

func TestAControlPlaneRollsOutOnceItsInputsAreReady(t *testing.T) {
	cp := controlPlane(t, loadBalancer())
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, workloads.ReasonWaitingForTarget) })

	setTarget(t, cp, version)
	eventually(t, func() error {
		return expectCondition(cp, metav1.ConditionFalse, workloads.ReasonWaitingForCertificates)
	})
	setTrue(t, cp, bryggav1alpha1.ConditionCertificatesReady, "Issued")
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, workloads.ReasonWaitingForDatastore) })
	setTrue(t, cp, bryggav1alpha1.ConditionDatastoreReady, "Ready")
	eventually(t, func() error {
		c := condition(t, cp)
		if c == nil || c.Reason != workloads.ReasonWaitingForCertificates || !strings.Contains(c.Message, "dev-ca-bundle, dev-etcd-ca") {
			return fmt.Errorf("APIServerReady is %+v, want the restart-only Secrets named", c)
		}
		return nil
	})
	if deployments(t, cp) != 0 {
		t.Fatal("a Deployment was created before the Secrets it mounts exist")
	}

	createSecrets(t, cp, "one")
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, workloads.ReasonRollingOut) })
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionTrue, workloads.ReasonReady) })
	if c := condition(t, cp); c.Message != "kube-apiserver, kube-controller-manager and kube-scheduler run "+version+", every replica updated and ready" {
		t.Errorf("APIServerReady says %q", c.Message)
	}

	images, _ := versions.For(version)
	api := deployment(t, cp, "kube-apiserver")
	pod := api.Spec.Template.Spec
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Error("the API server pod mounts a ServiceAccount token of the management cluster")
	}
	if len(pod.Containers) != 2 || pod.Containers[0].Image != images.APIServer || pod.Containers[1].Image != images.KonnectivityServer {
		t.Fatalf("the API server pod runs %+v", pod.Containers)
	}
	expectFlags(t, pod.Containers[0].Command,
		"--advertise-address="+lbAddress,
		"--secure-port=6443",
		"--etcd-servers=https://dev-etcd-client."+cp.Namespace+".svc:2379",
		"--service-account-signing-endpoint=/run/signer/signer.sock",
		"--service-account-issuer=https://"+lbAddress,
		"--client-ca-file=/etc/brygga/client-ca/ca.crt",
		"--enable-bootstrap-token-auth=true",
		"--authentication-config=/etc/brygga/authentication/authentication-config.yaml",
		"--egress-selector-config-file=/etc/brygga/egress/egress-selector.yaml",
	)
	for _, flag := range pod.Containers[0].Command {
		if strings.HasPrefix(flag, "--service-account-key-file") || strings.HasPrefix(flag, "--anonymous-auth") {
			t.Errorf("the API server runs with %s, which the signer endpoint and the authentication configuration exclude", flag)
		}
	}
	expectFlags(t, pod.Containers[1].Command, "--kubeconfig=/etc/brygga/konnectivity-kubeconfig/kubeconfig", "--uds-name=/run/konnectivity/konnectivity-server.socket", "--agent-port=8132")
	if len(pod.InitContainers) != 1 || pod.InitContainers[0].Image != signerImage || pod.InitContainers[0].RestartPolicy == nil || *pod.InitContainers[0].RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Errorf("the signer is %+v, want a native sidecar of %s", pod.InitContainers, signerImage)
	}

	kcm := deployment(t, cp, "kube-controller-manager").Spec.Template.Spec.Containers[0]
	if kcm.Image != images.ControllerManager {
		t.Errorf("the controller manager runs %s", kcm.Image)
	}
	expectFlags(t, kcm.Command,
		"--cluster-signing-kubelet-client-cert-file=/etc/brygga/kubelet-signer/tls.crt",
		"--cluster-signing-duration=720h0m0s",
		"--controllers=*,bootstrap-signer-controller,token-cleaner-controller",
		"--root-ca-file=/etc/brygga/ca-bundle/ca.crt",
		"--cluster-cidr=10.244.0.0/16",
		"--service-cluster-ip-range=10.96.0.0/16",
	)
	expectFlags(t, deployment(t, cp, "kube-scheduler").Spec.Template.Spec.Containers[0].Command, "--kubeconfig=/etc/brygga/kubeconfig/kubeconfig", "--leader-elect=true")

	authn := &corev1.ConfigMap{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: "dev-authentication-config"}, authn); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(authn.Data["authentication-config.yaml"], "path: /api/v1/namespaces/kube-public/configmaps/cluster-info") {
		t.Errorf("the authentication configuration does not let kubeadm join read cluster-info:\n%s", authn.Data["authentication-config.yaml"])
	}

	policy := &networkingv1.NetworkPolicy{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: "dev-etcd"}, policy); err != nil {
		t.Fatal(err)
	}
	from := map[string][]int32{}
	for _, rule := range policy.Spec.Ingress {
		var ports []int32
		for _, p := range rule.Ports {
			ports = append(ports, p.Port.IntVal)
		}
		switch peer := rule.From[0]; {
		case peer.NamespaceSelector != nil:
			from["namespace "+peer.NamespaceSelector.MatchLabels[corev1.LabelMetadataName]] = ports
		default:
			from[peer.PodSelector.MatchLabels["app.kubernetes.io/name"]+peer.PodSelector.MatchLabels["app.kubernetes.io/part-of"]] = ports
		}
	}
	want := map[string][]int32{"kube-apiserver": {2379}, "dev-etcd": {2379, 2380}, "namespace " + druidNamespace: {2379, 8080}}
	if fmt.Sprint(from) != fmt.Sprint(want) || policy.Spec.PodSelector.MatchLabels["app.kubernetes.io/part-of"] != "dev-etcd" {
		t.Errorf("the etcd NetworkPolicy selects %v and admits %v, want %v", policy.Spec.PodSelector.MatchLabels, from, want)
	}
}

func TestOnlyARestartOnlyInputRollsItsComponent(t *testing.T) {
	cp := running(t)
	before := hashes(t, cp)
	if before["kube-apiserver"] == "" || before["kube-controller-manager"] == "" || before["kube-scheduler"] != "" {
		t.Fatalf("restart hashes %v", before)
	}

	setSecret(t, cp, "apiserver-serving", "renewed")
	setSecret(t, cp, "front-proxy-client", "renewed")
	eventually(t, func() error {
		if after := hashes(t, cp); after["kube-apiserver"] == before["kube-apiserver"] || after["kube-controller-manager"] != before["kube-controller-manager"] {
			return fmt.Errorf("restart hashes %v, then %v: want the API server's alone to change", before, after)
		}
		return nil
	})

	middle := hashes(t, cp)
	setSecret(t, cp, "ca-bundle", "renewed")
	eventually(t, func() error {
		if after := hashes(t, cp); after["kube-controller-manager"] == middle["kube-controller-manager"] || after["kube-apiserver"] != middle["kube-apiserver"] {
			return fmt.Errorf("restart hashes %v, then %v: want the controller manager's alone to change", middle, after)
		}
		return nil
	})
}

func TestTheSpecIsRendered(t *testing.T) {
	cp := controlPlane(t, loadBalancer(), func(spec *bryggav1alpha1.ControlPlaneSpec) {
		spec.APIServer = &bryggav1alpha1.APIServer{
			Replicas:     new(int32(2)),
			ExtraArgs:    map[string]string{"audit-log-maxage": "7"},
			FeatureGates: map[string]bool{"B": false, "A": true},
			Resources:    &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}},
		}
		spec.ControllerManager = &bryggav1alpha1.Component{ExtraArgs: map[string]string{"node-monitor-grace-period": "50s"}}
		spec.ServiceAccount = &bryggav1alpha1.ServiceAccount{Issuer: "https://issuer.example.com"}
		spec.Addons = &bryggav1alpha1.Addons{Konnectivity: &bryggav1alpha1.Konnectivity{Version: "v0.37.2"}}
		spec.Placement = &bryggav1alpha1.Placement{
			NodeSelector:      map[string]string{"node-role.example.com/control-plane": ""},
			Tolerations:       []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}},
			PriorityClassName: "system-cluster-critical",
		}
	})
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReady(t, cp)
	api := eventuallyDeployment(t, cp, "kube-apiserver")
	pod := api.Spec.Template.Spec
	expectFlags(t, pod.Containers[0].Command, "--audit-log-maxage=7", "--feature-gates=A=true,B=false", "--service-account-issuer=https://issuer.example.com")
	if *api.Spec.Replicas != 2 || pod.Containers[0].Resources.Requests.Memory().String() != "1Gi" || !strings.HasSuffix(pod.Containers[1].Image, ":v0.37.2") {
		t.Errorf("the API server has %d replicas, requests %v, konnectivity %s", *api.Spec.Replicas, pod.Containers[0].Resources.Requests, pod.Containers[1].Image)
	}
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		pod := eventuallyDeployment(t, cp, component).Spec.Template.Spec
		if pod.PriorityClassName != "system-cluster-critical" || len(pod.Tolerations) != 1 || pod.NodeSelector["node-role.example.com/control-plane"] != "" || len(pod.NodeSelector) != 1 {
			t.Errorf("%s is placed with %v, %v and %q", component, pod.NodeSelector, pod.Tolerations, pod.PriorityClassName)
		}
	}
	expectFlags(t, deployment(t, cp, "kube-controller-manager").Spec.Template.Spec.Containers[0].Command, "--node-monitor-grace-period=50s")
}

func TestWhatIsNotBuiltYetIsSaid(t *testing.T) {
	gateway := controlPlane(t, bryggav1alpha1.Endpoint{Gateway: &bryggav1alpha1.GatewayEndpoint{
		ParentRef: bryggav1alpha1.ParentReference{Name: "shared", Namespace: "gateways"},
		Hostnames: bryggav1alpha1.GatewayHostnames{APIServer: "api.dev.example.com", Konnectivity: "konnectivity.dev.example.com"},
	}})
	eventually(t, func() error {
		return expectCondition(gateway, metav1.ConditionFalse, workloads.ReasonGatewayUnsupported)
	})

	ha := controlPlane(t, loadBalancer(), func(spec *bryggav1alpha1.ControlPlaneSpec) {
		spec.HighAvailability = &bryggav1alpha1.HighAvailability{FailureTolerance: "Node"}
		spec.Scheduler = &bryggav1alpha1.Scheduler{ConfigRef: &bryggav1alpha1.LocalReference{Name: "profiles"}}
	})
	eventually(t, func() error { return expectCondition(ha, metav1.ConditionFalse, workloads.ReasonUnsupported) })
	if c := condition(t, ha); c.Message != "not built yet: highAvailability, scheduler.configRef" {
		t.Errorf("APIServerReady says %q", c.Message)
	}
	if getServiceOrNil(t, ha) != nil {
		t.Error("a Service was created for a ControlPlane whose fields are not built yet")
	}
}

func TestConditionsFromState(t *testing.T) {
	ns := namespace(t)
	events, err := k8s.Watch(t.Context(), &bryggav1alpha1.ControlPlaneList{}, client.InNamespace(ns))
	if err != nil {
		t.Fatal(err)
	}
	defer events.Stop()
	cp := create(t, ns, loadBalancer())
	var first *metav1.Condition
	for first == nil {
		select {
		case event := <-events.ResultChan():
			if fresh, ok := event.Object.(*bryggav1alpha1.ControlPlane); ok && event.Type == watch.Modified {
				first = meta.FindStatusCondition(fresh.Status.Conditions, bryggav1alpha1.ConditionAPIServerReady)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("no APIServerReady condition was written")
		}
	}
	if first.Status != metav1.ConditionUnknown || first.Reason != workloads.ReasonReconciling {
		t.Errorf("the first APIServerReady is %s/%s %q, want Unknown/%s", first.Status, first.Reason, first.Message, workloads.ReasonReconciling)
	}
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionFalse, workloads.ReasonWaitingForEndpoint) })

	before := get(t, cp).ResourceVersion
	injected := errors.New("injected: the API server did not answer")
	failing := interceptor.NewClient(k8s, interceptor.Funcs{
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			return injected
		},
	})
	reconciler := &workloads.Reconciler{Client: failing, Reader: failing, SignerImage: signerImage, DruidNamespace: druidNamespace}
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
	reconciler := &workloads.Reconciler{Client: k8s, Reader: k8s, SignerImage: signerImage, DruidNamespace: druidNamespace}
	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: cp.Namespace, Name: cp.Name}}); err != nil {
		t.Fatal(err)
	}
	if after := versionsOf(); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Errorf("a pass over an unchanged ControlPlane wrote:\n%v\nthen\n%v", before, after)
	}
}

func loadBalancer() bryggav1alpha1.Endpoint {
	return bryggav1alpha1.Endpoint{LoadBalancer: &bryggav1alpha1.LoadBalancerEndpoint{}}
}

func namespace(t *testing.T) string {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "workloads-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	return ns.Name
}

func controlPlane(t *testing.T, endpoint bryggav1alpha1.Endpoint, change ...func(*bryggav1alpha1.ControlPlaneSpec)) *bryggav1alpha1.ControlPlane {
	t.Helper()
	return create(t, namespace(t), endpoint, change...)
}

func create(t *testing.T, ns string, endpoint bryggav1alpha1.Endpoint, change ...func(*bryggav1alpha1.ControlPlaneSpec)) *bryggav1alpha1.ControlPlane {
	t.Helper()
	cp := &bryggav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns},
		Spec: bryggav1alpha1.ControlPlaneSpec{
			Version:   version,
			Datastore: bryggav1alpha1.DatastoreChoice{Dedicated: &bryggav1alpha1.DedicatedDatastore{}},
			Endpoint:  endpoint,
			Network:   bryggav1alpha1.Network{ServiceCIDRs: []bryggav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []bryggav1alpha1.CIDR{"10.244.0.0/16"}},
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
func running(t *testing.T) *bryggav1alpha1.ControlPlane {
	t.Helper()
	cp := controlPlane(t, loadBalancer())
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReady(t, cp)
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	eventually(t, func() error { return expectCondition(cp, metav1.ConditionTrue, workloads.ReasonReady) })
	return cp
}

func inputsReady(t *testing.T, cp *bryggav1alpha1.ControlPlane) {
	t.Helper()
	setTarget(t, cp, version)
	setTrue(t, cp, bryggav1alpha1.ConditionCertificatesReady, "Issued")
	setTrue(t, cp, bryggav1alpha1.ConditionDatastoreReady, "Ready")
	createSecrets(t, cp, "one")
}

func get(t *testing.T, cp *bryggav1alpha1.ControlPlane) *bryggav1alpha1.ControlPlane {
	t.Helper()
	fresh := &bryggav1alpha1.ControlPlane{}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		t.Fatal(err)
	}
	return fresh
}

func condition(t *testing.T, cp *bryggav1alpha1.ControlPlane) *metav1.Condition {
	t.Helper()
	return meta.FindStatusCondition(get(t, cp).Status.Conditions, bryggav1alpha1.ConditionAPIServerReady)
}

func expectCondition(cp *bryggav1alpha1.ControlPlane, status metav1.ConditionStatus, reason string) error {
	fresh := &bryggav1alpha1.ControlPlane{}
	if err := k8s.Get(context.Background(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		return err
	}
	c := meta.FindStatusCondition(fresh.Status.Conditions, bryggav1alpha1.ConditionAPIServerReady)
	if c == nil || c.Status != status || c.Reason != reason || c.ObservedGeneration != fresh.Generation {
		return fmt.Errorf("APIServerReady is %+v, want %s %s at generation %d", c, status, reason, fresh.Generation)
	}
	return nil
}

func applyStatus(t *testing.T, cp *bryggav1alpha1.ControlPlane, owner string, status map[string]any) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": bryggav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     status,
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(owner), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func setTarget(t *testing.T, cp *bryggav1alpha1.ControlPlane, version string) {
	t.Helper()
	applyStatus(t, cp, "test/target", map[string]any{"target": map[string]any{"version": version}})
}

func setTrue(t *testing.T, cp *bryggav1alpha1.ControlPlane, conditionType, reason string) {
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

func createSecrets(t *testing.T, cp *bryggav1alpha1.ControlPlane, content string) {
	t.Helper()
	for _, suffix := range restartOnlySecrets {
		setSecret(t, cp, suffix, content)
	}
}

func setSecret(t *testing.T, cp *bryggav1alpha1.ControlPlane, suffix, content string) {
	t.Helper()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cp.Name + "-" + suffix, Namespace: cp.Namespace}}
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(secret), secret); err == nil {
		secret.Data = map[string][]byte{"content": []byte(content)}
		if err := k8s.Update(t.Context(), secret); err != nil {
			t.Fatal(err)
		}
		return
	}
	secret.Labels = map[string]string{bryggav1alpha1.ControlPlaneLabel: cp.Name}
	secret.Data = map[string][]byte{"content": []byte(content)}
	if err := k8s.Create(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
}

func setIngress(t *testing.T, cp *bryggav1alpha1.ControlPlane, ingress corev1.LoadBalancerIngress) {
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

func getService(t *testing.T, cp *bryggav1alpha1.ControlPlane) *corev1.Service {
	t.Helper()
	svc := getServiceOrNil(t, cp)
	if svc == nil {
		t.Fatal("no Service")
	}
	return svc
}

func getServiceOrNil(t *testing.T, cp *bryggav1alpha1.ControlPlane) *corev1.Service {
	t.Helper()
	svc := &corev1.Service{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-kube-apiserver"}, svc); err != nil {
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

func deployment(t *testing.T, cp *bryggav1alpha1.ControlPlane, component string) *appsv1.Deployment {
	t.Helper()
	d := &appsv1.Deployment{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-" + component}, d); err != nil {
		t.Fatal(err)
	}
	return d
}

func eventuallyDeployment(t *testing.T, cp *bryggav1alpha1.ControlPlane, component string) *appsv1.Deployment {
	t.Helper()
	d := &appsv1.Deployment{}
	eventually(t, func() error {
		return k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: cp.Name + "-" + component}, d)
	})
	return d
}

func deployments(t *testing.T, cp *bryggav1alpha1.ControlPlane) int {
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

func hashes(t *testing.T, cp *bryggav1alpha1.ControlPlane) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		out[component] = deployment(t, cp, component).Spec.Template.Annotations["brygga.segla.io/restart-hash"]
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
