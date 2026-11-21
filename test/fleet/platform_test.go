// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package fleet

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	druidapicommon "github.com/gardener/etcd-druid/api/common"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/operator"
	"go.lodia.io/lodia/internal/testenv"
)

const (
	controlPlaneName  = "tenant"
	hostSuffix        = "fleet.test"
	kubernetesVersion = "v1.37.2"
	gatewayNamespace  = "fleet-gateway"
	gatewayName       = "edge"
	gatewayListener   = "tls"
	druidNamespace    = "etcd-druid"
	druidDeployment   = "etcd-druid"
	druidVersion      = "v0.39.2"
	operatorNamespace = "lodia-system"
	operatorUser      = "system:serviceaccount:" + operatorNamespace + ":lodia"
	// boundedBody is a list a tenant at Health's bounds answers: just under
	// maxResponseBytes, 8 MiB.
	boundedBody = 8<<20 - 4<<10
)

// operatorGroups are the groups of the operator's ServiceAccount, by which
// the API server's priority and fairness classes its requests
// (https://kubernetes.io/docs/concepts/cluster-administration/flow-control/#defaults).
var operatorGroups = []string{"system:serviceaccounts", "system:serviceaccounts:" + operatorNamespace, "system:authenticated"}

func namespaceOf(i int) string { return fmt.Sprintf("fleet-%04d", i) }

func startManagement(t *testing.T) (*rest.Config, client.Client) {
	t.Helper()
	druid, err := testenv.DruidCRDs()
	if err != nil {
		t.Fatal(err)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "api", "crds")},
		ErrorIfCRDPathMissing: true,
		CRDs:                  append(druid, gatewayCRDs(t)...),
		UseExistingCluster:    new(false),
	}
	// envtest's own range, a /24, holds fewer Services than the fleet takes,
	// one for each tenant.
	env.ControlPlane.GetAPIServer().Configure().Set("service-cluster-ip-range", "10.0.0.0/16")
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	cfg.QPS = -1
	admin, err := client.New(cfg, client.Options{Scheme: operator.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	return cfg, admin
}

func gatewayCRDs(t *testing.T) []*apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	module, err := exec.CommandContext(t.Context(), "go", "list", "-m", "-f", "{{.Dir}}", "sigs.k8s.io/gateway-api").Output()
	if err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(strings.TrimSpace(string(module)), "config", "crd", "standard", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("the Gateway API module has no standard CRDs: %v", err)
	}
	var out []*apiextensionsv1.CustomResourceDefinition
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		reader := k8syaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
		for {
			doc, err := reader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			crd := &apiextensionsv1.CustomResourceDefinition{}
			if err := yaml.Unmarshal(doc, crd); err != nil {
				t.Fatal(err)
			}
			if crd.Kind == "CustomResourceDefinition" {
				out = append(out, crd)
			}
		}
	}
	return out
}

func grantOperator(t *testing.T, admin client.Client) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "charts", "lodia", "files", "rbac", "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no ClusterRoles in the chart: %v", err)
	}
	var roles []*rbacv1.ClusterRole
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		role := &rbacv1.ClusterRole{}
		if err := yaml.UnmarshalStrict(raw, role); err != nil {
			t.Fatal(err)
		}
		roles = append(roles, role)
	}
	roles = append(roles, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "lodia-fleet-etcd-druid"},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{appsv1.GroupName}, Resources: []string{"deployments"}, ResourceNames: []string{druidDeployment}, Verbs: []string{"get"}}},
	})
	subject := []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: operatorUser}}
	for _, role := range roles {
		binding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: role.Name},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
			Subjects:   subject,
		}
		for _, obj := range []client.Object{role, binding} {
			if err := admin.Create(t.Context(), obj); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func asOperator(cfg *rest.Config) *rest.Config {
	out := rest.CopyConfig(cfg)
	out.Impersonate = rest.ImpersonationConfig{UserName: operatorUser, Groups: operatorGroups}
	return out
}

type platform struct {
	mgr     ctrl.Manager
	paused  atomic.Bool
	refresh atomic.Int64
}

func startPlatform(t *testing.T, cfg *rest.Config, admin client.Client, logs io.Writer) *platform {
	t.Helper()
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     operator.Scheme(),
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: new(true)},
		Logger:     logger(logs),
	})
	if err != nil {
		t.Fatal(err)
	}
	p := &platform{mgr: mgr}
	c := mgr.GetClient()
	for _, setup := range []func() error{
		func() error {
			return ctrl.NewControllerManagedBy(mgr).Named("fleet-deployments").For(&appsv1.Deployment{}).WithOptions(workers(16)).Complete(deployments{c})
		},
		func() error {
			return ctrl.NewControllerManagedBy(mgr).Named("fleet-etcd-druid").For(&druidv1alpha1.Etcd{}).WithOptions(workers(16)).Complete(druid{c})
		},
		func() error {
			return ctrl.NewControllerManagedBy(mgr).Named("fleet-routes").For(&gatewayv1.TLSRoute{}).WithOptions(workers(16)).Complete(routes{c})
		},
	} {
		if err := setup(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := mgr.GetCache().GetInformer(t.Context(), &lodiav1alpha1.ControlPlane{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Error(err)
		}
	})
	if !mgr.GetCache().WaitForCacheSync(t.Context()) {
		t.Fatal("the platform's cache did not sync")
	}
	go p.refreshEtcds(ctx)
	createPlatformObjects(t, admin)
	return p
}

func workers(n int) controller.Options { return controller.Options{MaxConcurrentReconciles: n} }

func createPlatformObjects(t *testing.T, admin client.Client) {
	t.Helper()
	ctx := t.Context()
	for _, name := range []string{druidNamespace, gatewayNamespace, operatorNamespace} {
		if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
			t.Fatal(err)
		}
	}
	labels := map[string]string{"app.kubernetes.io/name": druidDeployment}
	objects := []client.Object{
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: druidDeployment, Namespace: druidNamespace},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: druidDeployment, Image: "europe-docker.pkg.dev/gardener-project/releases/gardener/etcd-druid:" + druidVersion}}},
				},
			},
		},
		&lodiav1alpha1.DatastoreClass{
			ObjectMeta: metav1.ObjectMeta{Name: "fleet", Annotations: map[string]string{lodiav1alpha1.DefaultClassAnnotation: "true"}},
			Spec:       lodiav1alpha1.DatastoreClassSpec{Storage: lodiav1alpha1.EtcdStorage{Size: resource.MustParse("8Gi"), ReclaimPolicy: "Delete"}},
		},
	}
	all := gatewayv1.NamespacesFromAll
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{Name: gatewayName, Namespace: gatewayNamespace},
		Spec: gatewayv1.GatewaySpec{GatewayClassName: "fleet", Listeners: []gatewayv1.Listener{{
			Name: gatewayListener, Port: 443, Protocol: gatewayv1.TLSProtocolType, Hostname: new(gatewayv1.Hostname("*." + hostSuffix)),
			TLS:           &gatewayv1.ListenerTLSConfig{Mode: new(gatewayv1.TLSModePassthrough)},
			AllowedRoutes: &gatewayv1.AllowedRoutes{Namespaces: &gatewayv1.RouteNamespaces{From: &all}},
		}}},
	}
	for _, obj := range append(objects, gateway) {
		if err := admin.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	now := metav1.Now()
	accepted := func(t string) metav1.Condition {
		return metav1.Condition{Type: t, Status: metav1.ConditionTrue, Reason: t, ObservedGeneration: gateway.Generation, LastTransitionTime: now}
	}
	gateway.Status = gatewayv1.GatewayStatus{
		Conditions: []metav1.Condition{accepted(string(gatewayv1.GatewayConditionAccepted)), accepted(string(gatewayv1.GatewayConditionProgrammed))},
		Listeners: []gatewayv1.ListenerStatus{{
			Name:           gatewayListener,
			SupportedKinds: []gatewayv1.RouteGroupKind{{Group: new(gatewayv1.Group(gatewayv1.GroupName)), Kind: "TLSRoute"}},
			Conditions: []metav1.Condition{
				accepted(string(gatewayv1.ListenerConditionAccepted)), accepted(string(gatewayv1.ListenerConditionProgrammed)), accepted(string(gatewayv1.ListenerConditionResolvedRefs)),
			},
		}},
	}
	if err := admin.Status().Update(ctx, gateway); err != nil {
		t.Fatal(err)
	}
}

func controlPlane(i int) *lodiav1alpha1.ControlPlane {
	ns := namespaceOf(i)
	return &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: controlPlaneName, Namespace: ns},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version: kubernetesVersion,
			Endpoint: lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
				ParentRef: lodiav1alpha1.ParentReference{Name: gatewayName, Namespace: gatewayNamespace, SectionName: gatewayListener},
				Hostnames: lodiav1alpha1.GatewayHostnames{
					APIServer:    lodiav1alpha1.Hostname(pathAPI + "." + ns + "." + hostSuffix),
					Konnectivity: lodiav1alpha1.Hostname(pathKonnectivity + "." + ns + "." + hostSuffix),
					Tunnel:       lodiav1alpha1.Hostname(pathTunnel + "." + ns + "." + hostSuffix),
				},
			}},
			Network: lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
}

type deployments struct{ client.Client }

func (d deployments) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	deployment := &appsv1.Deployment{}
	if err := d.Get(ctx, req.NamespacedName, deployment); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	replicas := ptr.Deref(deployment.Spec.Replicas, 1)
	want := appsv1.DeploymentStatus{
		ObservedGeneration: deployment.Generation, Replicas: replicas, UpdatedReplicas: replicas, ReadyReplicas: replicas, AvailableReplicas: replicas,
		Conditions: deployment.Status.Conditions,
	}
	if equality.Semantic.DeepEqual(deployment.Status, want) {
		return ctrl.Result{}, nil
	}
	deployment.Status = want
	return ctrl.Result{}, client.IgnoreNotFound(d.Status().Update(ctx, deployment))
}

type druid struct{ client.Client }

// Reconcile serves a new Etcd unasked and any other on request, as etcd-druid
// does with enableEtcdSpecAutoReconcile off: it syncs the StatefulSet, removes
// the request and writes a succeeded spec reconcile under a new run ID with
// every member ready.
func (d druid) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	etcd := &druidv1alpha1.Etcd{}
	if err := d.Get(ctx, req.NamespacedName, etcd); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	asked := druidv1alpha1.HasReconcileOperationAnnotation(etcd.ObjectMeta)
	if !etcd.DeletionTimestamp.IsZero() || etcd.Status.LastOperation != nil && !asked {
		return ctrl.Result{}, nil
	}
	if err := d.statefulSet(ctx, etcd); err != nil {
		return ctrl.Result{}, err
	}
	if asked {
		before := etcd.DeepCopy()
		delete(etcd.Annotations, druidv1alpha1.DruidOperationAnnotation)
		if err := d.Patch(ctx, etcd, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}
	before := etcd.DeepCopy()
	now := metav1.Now()
	operation := druidv1alpha1.LastOperationTypeReconcile
	if etcd.Status.LastOperation == nil {
		operation = druidv1alpha1.LastOperationTypeCreate
	}
	replicas := etcd.Spec.Replicas
	etcd.Status.ObservedGeneration = new(etcd.Generation)
	etcd.Status.Ready = new(true)
	etcd.Status.Replicas, etcd.Status.CurrentReplicas, etcd.Status.ReadyReplicas = replicas, replicas, replicas
	etcd.Status.Members = nil
	for i := range replicas {
		etcd.Status.Members = append(etcd.Status.Members, druidv1alpha1.EtcdMemberStatus{
			Name: fmt.Sprintf("%s-%d", etcd.Name, i), ID: new(fmt.Sprintf("%x", i+1)), Role: new(druidv1alpha1.EtcdRoleLeader),
			Status: druidv1alpha1.EtcdMemberStatusReady, Reason: "LeaseSucceeded", LastTransitionTime: now,
		})
	}
	etcd.Status.Conditions = []druidv1alpha1.Condition{
		{Type: druidv1alpha1.ConditionTypeReady, Status: druidv1alpha1.ConditionTrue, LastTransitionTime: now, LastUpdateTime: now, Reason: "QuorumReached", Message: "The majority of ETCD members is ready"},
		{Type: druidv1alpha1.ConditionTypeAllMembersReady, Status: druidv1alpha1.ConditionTrue, LastTransitionTime: now, LastUpdateTime: now, Reason: "AllMembersReady", Message: "All members are ready"},
	}
	etcd.Status.LastOperation = &druidapicommon.LastOperation{
		Type: operation, State: druidv1alpha1.LastOperationStateSucceeded, Description: "served by the fleet's stand-in", RunID: string(uuid.NewUUID()), LastUpdateTime: now,
	}
	return ctrl.Result{}, client.IgnoreNotFound(d.Status().Patch(ctx, etcd, client.MergeFrom(before)))
}

func (d druid) statefulSet(ctx context.Context, etcd *druidv1alpha1.Etcd) error {
	labels := map[string]string{"app.kubernetes.io/name": etcd.Name}
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: etcd.Name, Namespace: etcd.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, d.Client, sts, func() error {
		sts.Spec.Replicas = new(etcd.Spec.Replicas)
		sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		sts.Spec.Template.Labels = labels
		sts.Spec.Template.Annotations = map[string]string{"fleet.test/etcd-generation": fmt.Sprint(etcd.Generation)}
		sts.Spec.Template.Spec.Containers = []corev1.Container{{Name: "etcd", Image: "registry.example/etcd:fleet"}}
		return nil
	}); err != nil {
		return err
	}
	replicas := etcd.Spec.Replicas
	sts.Status = appsv1.StatefulSetStatus{
		ObservedGeneration: sts.Generation, Replicas: replicas, ReadyReplicas: replicas, CurrentReplicas: replicas, UpdatedReplicas: replicas, AvailableReplicas: replicas,
	}
	return d.Status().Update(ctx, sts)
}

// refreshEtcds writes new timestamps into every served Etcd's conditions
// every 15 s, spread over the period, as etcd-druid's status sync does.
func (p *platform) refreshEtcds(ctx context.Context) {
	const period = 15 * time.Second
	c := p.mgr.GetClient()
	for ctx.Err() == nil {
		list := &druidv1alpha1.EtcdList{}
		if err := c.List(ctx, list); err != nil || len(list.Items) == 0 || p.paused.Load() {
			sleep(ctx, period)
			continue
		}
		gap := period / time.Duration(len(list.Items))
		for i := range list.Items {
			etcd := &list.Items[i]
			if etcd.Status.LastOperation != nil && !p.paused.Load() {
				before := etcd.DeepCopy()
				now := metav1.Now()
				for j := range etcd.Status.Conditions {
					etcd.Status.Conditions[j].LastUpdateTime = now
				}
				if err := c.Status().Patch(ctx, etcd, client.MergeFrom(before)); err == nil {
					p.refresh.Add(1)
				}
			}
			sleep(ctx, gap)
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

type routes struct{ client.Client }

func (r routes) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	route := &gatewayv1.TLSRoute{}
	if err := r.Get(ctx, req.NamespacedName, route); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	now := metav1.Now()
	var parents []gatewayv1.RouteParentStatus
	for _, ref := range route.Spec.ParentRefs {
		status := gatewayv1.RouteParentStatus{ParentRef: ref, ControllerName: "fleet.test/gateway"}
		for _, t := range []gatewayv1.RouteConditionType{gatewayv1.RouteConditionAccepted, gatewayv1.RouteConditionResolvedRefs} {
			status.Conditions = append(status.Conditions, metav1.Condition{Type: string(t), Status: metav1.ConditionTrue, Reason: string(t), ObservedGeneration: route.Generation, LastTransitionTime: now})
		}
		parents = append(parents, status)
	}
	if acceptedAt(route) {
		return ctrl.Result{}, nil
	}
	route.Status.Parents = parents
	return ctrl.Result{}, client.IgnoreNotFound(r.Status().Update(ctx, route))
}

func acceptedAt(route *gatewayv1.TLSRoute) bool {
	if len(route.Status.Parents) != len(route.Spec.ParentRefs) {
		return false
	}
	for _, p := range route.Status.Parents {
		c := meta.FindStatusCondition(p.Conditions, string(gatewayv1.RouteConditionAccepted))
		if c == nil || c.ObservedGeneration != route.Generation {
			return false
		}
	}
	return true
}
