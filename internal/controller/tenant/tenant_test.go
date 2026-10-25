// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant_test

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/certs"
	"github.com/segla-io/brygga/internal/controller/pki"
	"github.com/segla-io/brygga/internal/controller/tenant"
)

const (
	version          = "v1.35.8"
	endpointHost     = "203.0.113.10"
	apiServerPort    = 443
	konnectivityPort = 8132
)

var tenantRules = []rbacv1.PolicyRule{
	{APIGroups: []string{bryggav1alpha1.GroupVersion.Group}, Resources: []string{"controlplanes"}, Verbs: []string{"list", "watch"}},
	{APIGroups: []string{bryggav1alpha1.GroupVersion.Group}, Resources: []string{"controlplanes/status"}, Verbs: []string{"patch"}},
	{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get"}},
}

type dialFunc = func(ctx context.Context, network, address string) (net.Conn, error)

var (
	k8s    client.Client
	scheme = runtime.NewScheme()
	dial   atomic.Pointer[dialFunc]

	unreachable dialFunc = func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("connection refused")
	}
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("skipping the tenant controller tests: KUBEBUILDER_ASSETS is not set; run make test")
		os.Exit(0)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		UseExistingCluster:    new(false),
	}
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
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, bryggav1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			stop("scheme", err)
		}
	}
	if k8s, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		stop("client", err)
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	controller, err := asTenantController(cfg)
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
	dial.Store(&unreachable)
	r := &tenant.Reconciler{
		Client:      mgr.GetClient(),
		Reader:      mgr.GetAPIReader(),
		ApplyPeriod: 2 * time.Second,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (*dial.Load())(ctx, network, address)
		},
	}
	if err := r.SetupWithManager(mgr, 1); err != nil {
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

func asTenantController(cfg *rest.Config) (*rest.Config, error) {
	const user = "brygga-tenant"
	ctx := context.Background()
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: user}, Rules: tenantRules}
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
			ResourceAttributes: &authorizationv1.ResourceAttributes{Group: bryggav1alpha1.GroupVersion.Group, Resource: "controlplanes", Verb: "watch"},
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

func TestTheFirstVisitWaitsForAccess(t *testing.T) {
	cp := controlPlane(t, "tenant-first-visit")
	eventually(t, 10*time.Second, func() error {
		if err := expectCondition(t, cp, bryggav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, tenant.ReasonWaitingForAPIServer); err != nil {
			return err
		}
		return expectCondition(t, cp, bryggav1alpha1.ConditionAddonsAvailable, metav1.ConditionUnknown, tenant.ReasonReconciling)
	})
}

func TestTheTenantGetsItsObjects(t *testing.T) {
	ctx := t.Context()
	cp := controlPlane(t, "tenant-objects")
	publish(t, cp)
	issuer := &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}
	reconcilePKI(t, issuer, cp)
	admin := startTenant(t, cp)
	setDial(t, admin.host)
	issuer.Dial = *dial.Load()
	setCondition(t, cp, bryggav1alpha1.ConditionAPIServerReady, "Ready")
	reconcilePKI(t, issuer, cp)
	expectNow(t, cp, bryggav1alpha1.ConditionAccessReady, metav1.ConditionTrue, pki.ReasonBound)

	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, bryggav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, tenant.ReasonApplied)
	})
	expectNow(t, cp, bryggav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, tenant.ReasonRolloutPending)

	info := get(t, admin, &corev1.ConfigMap{}, "kube-public", "cluster-info")
	kubeconfig, err := clientcmd.Load([]byte(info.Data["kubeconfig"]))
	if err != nil {
		t.Fatal(err)
	}
	bundle := &corev1.Secret{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: "dev-ca-bundle"}, bundle); err != nil {
		t.Fatal(err)
	}
	if cluster := kubeconfig.Clusters[""]; cluster == nil || cluster.Server != "https://203.0.113.10:443" || !bytes.Equal(cluster.CertificateAuthorityData, bundle.Data["ca.crt"]) {
		t.Errorf("cluster-info names %+v", kubeconfig.Clusters)
	}
	var clusterConfiguration map[string]any
	if err := yaml.Unmarshal([]byte(get(t, admin, &corev1.ConfigMap{}, "kube-system", "kubeadm-config").Data["ClusterConfiguration"]), &clusterConfiguration); err != nil {
		t.Fatal(err)
	}
	if clusterConfiguration["controlPlaneEndpoint"] != "203.0.113.10:443" || clusterConfiguration["kubernetesVersion"] != version {
		t.Errorf("kubeadm-config is %v", clusterConfiguration)
	}
	if !strings.Contains(get(t, admin, &corev1.ConfigMap{}, "kube-system", "kubelet-config").Data["kubelet"], `- "10.96.0.10"`) {
		t.Error("kubelet-config does not name CoreDNS at the tenth service address")
	}
	if dns := get(t, admin, &corev1.Service{}, "kube-system", "kube-dns"); dns.Spec.ClusterIP != "10.96.0.10" {
		t.Errorf("kube-dns has the ClusterIP %s", dns.Spec.ClusterIP)
	}
	if image := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns").Spec.Template.Spec.Containers[0].Image; image != "registry.k8s.io/coredns/coredns:v1.13.1" {
		t.Errorf("CoreDNS runs %s", image)
	}
	if image := get(t, admin, &appsv1.DaemonSet{}, "kube-system", "kube-proxy").Spec.Template.Spec.Containers[0].Image; image != "registry.k8s.io/kube-proxy:"+version {
		t.Errorf("kube-proxy runs %s", image)
	}
	agent := get(t, admin, &appsv1.DaemonSet{}, "kube-system", "konnectivity-agent").Spec.Template.Spec.Containers[0]
	for _, arg := range []string{"--proxy-server-host=203.0.113.10", "--proxy-server-port=8132", "--count-server-leases", "--agent-identifiers=ipv4=$(HOST_IP)"} {
		if !slices.Contains(agent.Command, arg) {
			t.Errorf("the konnectivity agent runs without %s: %v", arg, agent.Command)
		}
	}
	inventory := inventoryOf(t, admin)
	if !slices.Contains(inventory, "ConfigMap kube-public/cluster-info") || !slices.Contains(inventory, "Deployment kube-system/coredns") {
		t.Errorf("the inventory lists %v", inventory)
	}
	for _, entry := range inventory {
		obj := objectOf(t, admin, entry)
		if obj.GetLabels()["app.kubernetes.io/managed-by"] != "brygga" || !slices.ContainsFunc(obj.GetManagedFields(), func(f metav1.ManagedFieldsEntry) bool {
			return f.Manager == tenant.FieldManager && f.Operation == metav1.ManagedFieldsOperationApply
		}) {
			t.Errorf("%s is not applied as %s with the managed-by label", entry, tenant.FieldManager)
		}
	}

	setCoreDNS(t, cp, false)
	eventually(t, 30*time.Second, func() error {
		if inventory := inventoryOf(t, admin); slices.ContainsFunc(inventory, func(e string) bool { return strings.Contains(e, "coredns") || strings.Contains(e, "kube-dns") }) {
			return fmt.Errorf("the inventory still lists CoreDNS: %v", inventory)
		}
		return nil
	})
	for _, gone := range []string{"Deployment kube-system/coredns", "Service kube-system/kube-dns", "ConfigMap kube-system/coredns", "ServiceAccount kube-system/coredns", "ClusterRole system:coredns", "ClusterRoleBinding system:coredns"} {
		if exists(t, admin, gone) {
			t.Errorf("%s survived disabling CoreDNS", gone)
		}
	}
	setCoreDNS(t, cp, true)
	eventually(t, 30*time.Second, func() error {
		if !exists(t, admin, "Deployment kube-system/coredns") || !exists(t, admin, "Service kube-system/kube-dns") {
			return errors.New("CoreDNS is not back")
		}
		return nil
	})

	if err := admin.Delete(ctx, &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "kube-proxy"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		if !exists(t, admin, "DaemonSet kube-system/kube-proxy") {
			return errors.New("the deleted kube-proxy DaemonSet is not back")
		}
		return nil
	})

	for _, name := range []string{"kube-proxy", "konnectivity-agent"} {
		ds := get(t, admin, &appsv1.DaemonSet{}, "kube-system", name)
		ds.Status = appsv1.DaemonSetStatus{ObservedGeneration: ds.Generation}
		if err := admin.Status().Update(ctx, ds); err != nil {
			t.Fatal(err)
		}
	}
	coredns := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns")
	coredns.Status = appsv1.DeploymentStatus{ObservedGeneration: coredns.Generation, Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2}
	if err := admin.Status().Update(ctx, coredns); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, bryggav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, tenant.ReasonAvailable)
	})

	setInventory(t, admin, "not a list")
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, bryggav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, tenant.ReasonApplying); err != nil {
			return err
		}
		if c := condition(t, cp, bryggav1alpha1.ConditionAddonsApplied); !strings.Contains(c.Message, "ConfigMap kube-system/brygga-inventory cannot be read") {
			return fmt.Errorf("AddonsApplied says %q", c.Message)
		}
		return nil
	})
	if err := admin.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "brygga-inventory"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, bryggav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, tenant.ReasonApplied)
	})
}

func setInventory(t *testing.T, c client.Client, objects string) {
	t.Helper()
	inventory := get(t, c, &corev1.ConfigMap{}, "kube-system", "brygga-inventory")
	inventory.Data["objects"] = objects
	if err := c.Update(t.Context(), inventory); err != nil {
		t.Fatal(err)
	}
}

func TestAnObjectTheTenantRefusesIsReportedAndNeverLeaks(t *testing.T) {
	ctx := t.Context()
	cp := controlPlane(t, "tenant-refused")
	publish(t, cp)
	issuer := &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}
	reconcilePKI(t, issuer, cp)
	admin := startTenant(t, cp)
	setDial(t, admin.host)
	issuer.Dial = *dial.Load()
	setCondition(t, cp, bryggav1alpha1.ConditionAPIServerReady, "Ready")
	reconcilePKI(t, issuer, cp)
	setCoreDNS(t, cp, false)
	setCondition(t, cp, bryggav1alpha1.ConditionAccessReady, pki.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, bryggav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, tenant.ReasonApplied)
	})

	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "no-coredns"},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			MatchConstraints: &admissionregistrationv1.MatchResources{ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
				RuleWithOperations: admissionregistrationv1.RuleWithOperations{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
					Rule:       admissionregistrationv1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1"}, Resources: []string{"deployments"}},
				},
			}}},
			Validations: []admissionregistrationv1.Validation{{Expression: "object.metadata.name != 'coredns'", Message: "no CoreDNS here"}},
		},
	}
	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "no-coredns"},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        policy.Name,
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
		},
	}
	for _, obj := range []client.Object{policy, binding} {
		if err := admin.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 30*time.Second, func() error {
		err := admin.Create(ctx, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "coredns"},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "probe"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "probe"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "probe", Image: "probe"}}},
				},
			},
		}, client.DryRunAll)
		if err == nil {
			return errors.New("the policy does not refuse a coredns Deployment yet")
		}
		return nil
	})

	setCoreDNS(t, cp, true)
	setCondition(t, cp, bryggav1alpha1.ConditionAccessReady, pki.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, bryggav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, tenant.ReasonApplying); err != nil {
			return err
		}
		if c := condition(t, cp, bryggav1alpha1.ConditionAddonsApplied); !strings.Contains(c.Message, "Deployment kube-system/coredns is refused") {
			return fmt.Errorf("AddonsApplied says %q", c.Message)
		}
		return nil
	})
	if !exists(t, admin, "ConfigMap kube-system/coredns") || !slices.Contains(inventoryOf(t, admin), "ConfigMap kube-system/coredns") {
		t.Fatal("the objects before the refused one were not applied, or not recorded before they were")
	}

	setCoreDNS(t, cp, false)
	setCondition(t, cp, bryggav1alpha1.ConditionAccessReady, pki.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, bryggav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, tenant.ReasonApplied)
	})
	for _, gone := range []string{"ConfigMap kube-system/coredns", "ServiceAccount kube-system/coredns", "ClusterRole system:coredns"} {
		if exists(t, admin, gone) {
			t.Errorf("%s, applied by the pass the tenant refused, leaked", gone)
		}
	}
}

func TestATenantThatDoesNotAnswerKeepsATrueAddonsApplied(t *testing.T) {
	cp := controlPlane(t, "tenant-unanswered")
	publish(t, cp)
	issuer := &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}
	reconcilePKI(t, issuer, cp)
	admin := startTenant(t, cp)
	setDial(t, admin.host)
	issuer.Dial = *dial.Load()
	setCondition(t, cp, bryggav1alpha1.ConditionAPIServerReady, "Ready")
	reconcilePKI(t, issuer, cp)
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, bryggav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, tenant.ReasonApplied)
	})
	before := condition(t, cp, bryggav1alpha1.ConditionAddonsApplied)

	dial.Store(&unreachable)
	time.Sleep(5 * time.Second)
	if after := condition(t, cp, bryggav1alpha1.ConditionAddonsApplied); after.Status != before.Status || after.Reason != before.Reason || after.Message != before.Message || after.ObservedGeneration != before.ObservedGeneration {
		t.Fatalf("passes that could not reach the tenant changed AddonsApplied from %+v to %+v", before, after)
	}

	setDial(t, admin.host)
	setReplicas(t, cp, 3)
	setCondition(t, cp, bryggav1alpha1.ConditionAccessReady, pki.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, bryggav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, tenant.ReasonApplied); err != nil {
			return err
		}
		if replicas := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns").Spec.Replicas; replicas == nil || *replicas != 3 {
			return fmt.Errorf("CoreDNS has %v replicas", replicas)
		}
		return nil
	})
}

func TestATenantThatNeverAnsweredIsApplying(t *testing.T) {
	cp := controlPlane(t, "tenant-never-answered")
	publish(t, cp)
	reconcilePKI(t, &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour}, cp)
	dial.Store(&unreachable)
	setCondition(t, cp, bryggav1alpha1.ConditionAccessReady, pki.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, bryggav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, tenant.ReasonApplying); err != nil {
			return err
		}
		if c := condition(t, cp, bryggav1alpha1.ConditionAddonsApplied); !strings.Contains(c.Message, "does not answer") {
			return fmt.Errorf("AddonsApplied says %q", c.Message)
		}
		return nil
	})
}

type tenantClient struct {
	client.Client
	host string
}

func startTenant(t *testing.T, cp *bryggav1alpha1.ControlPlane) *tenantClient {
	t.Helper()
	secrets := map[string]*corev1.Secret{}
	var list corev1.SecretList
	if err := k8s.List(t.Context(), &list, client.InNamespace(cp.Namespace)); err != nil {
		t.Fatal(err)
	}
	for i := range list.Items {
		secrets[list.Items[i].Name] = &list.Items[i]
	}
	dir := t.TempDir()
	file := func(secret, key string) string {
		t.Helper()
		path := filepath.Join(dir, secret+"-"+key)
		if err := os.WriteFile(path, secrets[secret].Data[key], 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	clientCA, err := certs.Parse(secrets["dev-cakey-client"].Data["tls.crt"], secrets["dev-cakey-client"].Data["tls.key"])
	if err != nil {
		t.Fatal(err)
	}
	env := &envtest.Environment{UseExistingCluster: new(false)}
	api := env.ControlPlane.GetAPIServer()
	api.Authn = &tenantAuthn{api: api, clientCA: clientCA, clusterCA: secrets["dev-ca-bundle"].Data["ca.crt"], bundle: file("dev-client-ca", "ca.crt")}
	saKey := file("dev-sa-keys", "signing.key")
	api.Configure().
		Set("tls-cert-file", file("dev-apiserver-serving", "tls.crt")).
		Set("tls-private-key-file", file("dev-apiserver-serving", "tls.key")).
		Set("service-account-signing-key-file", saKey).
		Set("service-account-key-file", saKey).
		Set("service-cluster-ip-range", string(cp.Spec.Network.ServiceCIDRs[0]))
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return &tenantClient{Client: admin, host: cfg.Host}
}

type tenantAuthn struct {
	api       *envtest.APIServer
	clientCA  *certs.Certificate
	clusterCA []byte
	bundle    string
}

func (a *tenantAuthn) Configure(_ string, args *envtest.Arguments) error {
	args.Set("client-ca-file", a.bundle)
	return nil
}

func (a *tenantAuthn) Start() error {
	a.api.CA = a.clusterCA
	return nil
}

func (a *tenantAuthn) Stop() error {
	return nil
}

func (a *tenantAuthn) AddUser(user envtest.User, base *rest.Config) (*rest.Config, error) {
	c, err := a.clientCA.Issue(certs.Profile{CommonName: user.Name, Organizations: user.Groups, Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, time.Now(), time.Hour)
	if err != nil {
		return nil, err
	}
	out := rest.CopyConfig(base)
	if out.CertData, err = certs.CertPEM(c.Cert); err != nil {
		return nil, err
	}
	out.KeyData, err = certs.KeyPEM(c.Key)
	return out, err
}

func setDial(t *testing.T, host string) {
	t.Helper()
	u, err := url.Parse(host)
	if err != nil {
		t.Fatal(err)
	}
	to := func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, u.Host)
	}
	dial.Store(&to)
}

func controlPlane(t *testing.T, namespace string) *bryggav1alpha1.ControlPlane {
	t.Helper()
	if err := k8s.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil {
		t.Fatal(err)
	}
	cp := &bryggav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: namespace},
		Spec: bryggav1alpha1.ControlPlaneSpec{
			Version:   version,
			Datastore: bryggav1alpha1.DatastoreChoice{Dedicated: &bryggav1alpha1.DedicatedDatastore{}},
			Endpoint:  bryggav1alpha1.Endpoint{LoadBalancer: &bryggav1alpha1.LoadBalancerEndpoint{}},
			Network:   bryggav1alpha1.Network{ServiceCIDRs: []bryggav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []bryggav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	if err := k8s.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func reconcilePKI(t *testing.T, r *pki.Reconciler, cp *bryggav1alpha1.ControlPlane) {
	t.Helper()
	refresh(t, cp)
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
}

func applyStatus(t *testing.T, cp *bryggav1alpha1.ControlPlane, manager string, status map[string]any) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": bryggav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     status,
	}}
	if err := k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(manager), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func publish(t *testing.T, cp *bryggav1alpha1.ControlPlane) {
	t.Helper()
	applyStatus(t, cp, "test/target", map[string]any{"target": map[string]any{"version": version}})
	applyStatus(t, cp, "test/endpoint", map[string]any{"endpoint": map[string]any{
		"apiServer":    map[string]any{"host": endpointHost, "port": int64(apiServerPort)},
		"konnectivity": map[string]any{"host": endpointHost, "port": int64(konnectivityPort)},
	}})
}

func setCondition(t *testing.T, cp *bryggav1alpha1.ControlPlane, conditionType, reason string) {
	t.Helper()
	refresh(t, cp)
	applyStatus(t, cp, "test/"+conditionType, map[string]any{"conditions": []any{map[string]any{
		"type":               conditionType,
		"status":             string(metav1.ConditionTrue),
		"reason":             reason,
		"message":            "",
		"observedGeneration": cp.Generation,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}}})
}

func setCoreDNS(t *testing.T, cp *bryggav1alpha1.ControlPlane, enabled bool) {
	t.Helper()
	update(t, cp, func(cp *bryggav1alpha1.ControlPlane) {
		if cp.Spec.Addons == nil {
			cp.Spec.Addons = &bryggav1alpha1.Addons{}
		}
		if cp.Spec.Addons.CoreDNS == nil {
			cp.Spec.Addons.CoreDNS = &bryggav1alpha1.CoreDNS{}
		}
		cp.Spec.Addons.CoreDNS.Enabled = &enabled
	})
	setCondition(t, cp, bryggav1alpha1.ConditionAccessReady, pki.ReasonBound)
}

func setReplicas(t *testing.T, cp *bryggav1alpha1.ControlPlane, replicas int32) {
	t.Helper()
	update(t, cp, func(cp *bryggav1alpha1.ControlPlane) {
		if cp.Spec.Addons == nil {
			cp.Spec.Addons = &bryggav1alpha1.Addons{}
		}
		if cp.Spec.Addons.CoreDNS == nil {
			cp.Spec.Addons.CoreDNS = &bryggav1alpha1.CoreDNS{}
		}
		cp.Spec.Addons.CoreDNS.Replicas = &replicas
	})
}

func update(t *testing.T, cp *bryggav1alpha1.ControlPlane, change func(*bryggav1alpha1.ControlPlane)) {
	t.Helper()
	err := wait.PollUntilContextTimeout(t.Context(), 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		refresh(t, cp)
		change(cp)
		err := k8s.Update(ctx, cp)
		return err == nil, client.IgnoreNotFound(ignoreConflict(err))
	})
	if err != nil {
		t.Fatal(err)
	}
}

func ignoreConflict(err error) error {
	if apierrors.IsConflict(err) {
		return nil
	}
	return err
}

func refresh(t *testing.T, cp *bryggav1alpha1.ControlPlane) {
	t.Helper()
	if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(cp), cp); err != nil {
		t.Fatal(err)
	}
}

func condition(t *testing.T, cp *bryggav1alpha1.ControlPlane, conditionType string) *metav1.Condition {
	t.Helper()
	refresh(t, cp)
	c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
	if c == nil {
		return &metav1.Condition{Type: conditionType}
	}
	return c
}

func expectCondition(t *testing.T, cp *bryggav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason string) error {
	t.Helper()
	c := condition(t, cp, conditionType)
	if c.Status != status || c.Reason != reason || c.ObservedGeneration != cp.Generation {
		return fmt.Errorf("%s is %s %s at generation %d (%s), want %s %s at %d", conditionType, c.Status, c.Reason, c.ObservedGeneration, c.Message, status, reason, cp.Generation)
	}
	return nil
}

func expectNow(t *testing.T, cp *bryggav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	if err := expectCondition(t, cp, conditionType, status, reason); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, timeout time.Duration, check func() error) {
	t.Helper()
	var last error
	err := wait.PollUntilContextTimeout(t.Context(), 200*time.Millisecond, timeout, true, func(context.Context) (bool, error) {
		last = check()
		return last == nil, nil
	})
	if err != nil {
		t.Fatal(last)
	}
}

func get[T client.Object](t *testing.T, c client.Client, obj T, namespace, name string) T {
	t.Helper()
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
		t.Fatal(err)
	}
	return obj
}

func inventoryOf(t *testing.T, c client.Client) []string {
	t.Helper()
	var entries []struct{ Group, Version, Kind, Namespace, Name string }
	if err := json.Unmarshal([]byte(get(t, c, &corev1.ConfigMap{}, "kube-system", "brygga-inventory").Data["objects"]), &entries); err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Namespace == "" {
			out = append(out, e.Kind+" "+e.Name)
		} else {
			out = append(out, e.Kind+" "+e.Namespace+"/"+e.Name)
		}
	}
	return out
}

var kinds = map[string]string{
	"ConfigMap": "v1", "ServiceAccount": "v1", "Service": "v1",
	"Deployment": "apps/v1", "DaemonSet": "apps/v1",
	"ClusterRole": "rbac.authorization.k8s.io/v1", "ClusterRoleBinding": "rbac.authorization.k8s.io/v1",
	"Role": "rbac.authorization.k8s.io/v1", "RoleBinding": "rbac.authorization.k8s.io/v1",
}

func lookup(t *testing.T, c client.Client, entry string) (*unstructured.Unstructured, error) {
	t.Helper()
	kind, rest, _ := strings.Cut(entry, " ")
	namespace, name, namespaced := strings.Cut(rest, "/")
	if !namespaced {
		namespace, name = "", rest
	}
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(kinds[kind])
	obj.SetKind(kind)
	return obj, c.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: name}, obj)
}

func objectOf(t *testing.T, c client.Client, entry string) *unstructured.Unstructured {
	t.Helper()
	obj, err := lookup(t, c, entry)
	if err != nil {
		t.Fatal(err)
	}
	return obj
}

func exists(t *testing.T, c client.Client, entry string) bool {
	t.Helper()
	_, err := lookup(t, c, entry)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}
