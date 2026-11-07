// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/pki"
	"go.lodia.io/lodia/internal/controller/tenant"
)

const (
	tunnelImage  = "registry.example/lodia-tunnel:test"
	clientSecret = "apiserver-proxy-client"
)

func gatewayMode(cp *lodiav1alpha1.ControlPlane) {
	cp.Spec.Endpoint = lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{
		ParentRef: lodiav1alpha1.ParentReference{Name: "edge", Namespace: "gateways"},
		Hostnames: lodiav1alpha1.GatewayHostnames{APIServer: "api.dev.example.test", Konnectivity: "konnectivity.dev.example.test", Tunnel: "tunnel.dev.example.test"},
	}}
}

func publishGateway(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	applyStatus(t, cp, "test/target", map[string]any{"target": map[string]any{"version": version}})
	applyStatus(t, cp, "test/endpoint", map[string]any{"endpoint": map[string]any{
		"apiServer":    map[string]any{"host": "api.dev.example.test", "port": int64(443)},
		"konnectivity": map[string]any{"host": "konnectivity.dev.example.test", "port": int64(443)},
		"inCluster":    map[string]any{"host": lodiav1alpha1.DefaultAdvertiseAddress, "port": int64(6443)},
		"tunnel":       map[string]any{"host": "tunnel.dev.example.test", "port": int64(443)},
	}})
}

func gatewayIssuer() *pki.Reconciler {
	const year = 365 * 24 * time.Hour
	return &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: year, CALifetime: 5 * year, TrustAnchorLifetime: 10 * year, CAExpiryNotice: year, Clock: clock.RealClock{}}
}

func gatewayTenantWithoutAccess(t *testing.T, namespace string) (*lodiav1alpha1.ControlPlane, *tenantClient) {
	t.Helper()
	cp := controlPlane(t, namespace, gatewayMode)
	publishGateway(t, cp)
	reconcilePKI(t, gatewayIssuer(), cp)
	admin := startTenant(t, cp)
	setDial(t, admin.host)
	return cp, admin
}

func grantAccess(t *testing.T, cp *lodiav1alpha1.ControlPlane) *pki.Reconciler {
	t.Helper()
	issuer := gatewayIssuer()
	issuer.Dial = *dial.Load()
	setCondition(t, cp, lodiav1alpha1.ConditionAPIServerReady, lodiav1alpha1.ReasonReady)
	reconcilePKI(t, issuer, cp)
	return issuer
}

func gatewayTenant(t *testing.T, namespace string) (*lodiav1alpha1.ControlPlane, *tenantClient, *pki.Reconciler) {
	t.Helper()
	cp, admin := gatewayTenantWithoutAccess(t, namespace)
	return cp, admin, grantAccess(t, cp)
}

func TestAGatewayTenantGetsApiserverProxy(t *testing.T) {
	ctx := t.Context()
	cp, admin, _ := gatewayTenant(t, "tenant-gateway-objects")
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})

	ds := get(t, admin, &appsv1.DaemonSet{}, "kube-system", "apiserver-proxy")
	spec := ds.Spec.Template.Spec
	if len(spec.InitContainers) != 1 || len(spec.Containers) != 1 {
		t.Fatalf("the pod runs %v and %v", spec.InitContainers, spec.Containers)
	}
	addr, proxy := spec.InitContainers[0], spec.Containers[0]
	if addr.RestartPolicy == nil || *addr.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Errorf("addr has the restartPolicy %v", addr.RestartPolicy)
	}
	wantStartup := []string{"/ko-app/lodia-tunnel", "addr", "--address=" + lodiav1alpha1.DefaultAdvertiseAddress, "--check"}
	if p := addr.StartupProbe; p == nil || p.Exec == nil || !slices.Equal(p.Exec.Command, wantStartup) || p.PeriodSeconds != 1 || p.FailureThreshold != 30 {
		t.Errorf("addr's startup probe is %+v", p)
	}
	wantProbe := []string{"/ko-app/lodia-tunnel", "probe", "--socket=/run/lodia/proxy.sock"}
	for name, p := range map[string]*corev1.Probe{"readiness": proxy.ReadinessProbe, "liveness": proxy.LivenessProbe} {
		if p == nil || p.Exec == nil || !slices.Equal(p.Exec.Command, wantProbe) {
			t.Errorf("proxy's %s probe is %+v", name, p)
		}
	}
	if len(addr.Ports) != 0 || len(proxy.Ports) != 0 {
		t.Errorf("declared ports: addr %v, proxy %v", addr.Ports, proxy.Ports)
	}
	if !spec.HostNetwork || spec.DNSPolicy != corev1.DNSDefault || *spec.Volumes[0].Secret.DefaultMode != 0o440 {
		t.Errorf("the pod's spec came back as %+v", spec)
	}
	if u := ds.Spec.UpdateStrategy.RollingUpdate; u == nil || u.MaxSurge.IntValue() != 1 || u.MaxUnavailable.IntValue() != 0 {
		t.Errorf("the rolling update is %+v", u)
	}

	pair := get(t, k8s, &corev1.Secret{}, cp.Namespace, "dev-tunnel-client")
	delivered := get(t, admin, &corev1.Secret{}, "kube-system", clientSecret)
	if delivered.Type != corev1.SecretTypeTLS || !equality.Semantic.DeepEqual(delivered.Data, map[string][]byte{"tls.crt": pair.Data["tls.crt"], "tls.key": pair.Data["tls.key"]}) {
		t.Errorf("the tenant's Secret is of type %s, with %d keys, and not the pair", delivered.Type, len(delivered.Data))
	}
	agents := get(t, admin, &appsv1.DaemonSet{}, "kube-system", "konnectivity-agent").Spec.Template.Spec
	if agents.DNSPolicy != corev1.DNSDefault || !slices.Contains(agents.Containers[0].Command, "--kubeconfig=/etc/konnectivity-agent/kubeconfig") {
		t.Errorf("the agents resolve with %s and run %v", agents.DNSPolicy, agents.Containers[0].Command)
	}
	if kubeconfig := get(t, admin, &corev1.ConfigMap{}, "kube-system", "konnectivity-agent-kubeconfig").Data["kubeconfig"]; !strings.Contains(kubeconfig, `server: "https://api.dev.example.test:443"`) {
		t.Errorf("the agents' kubeconfig is %q", kubeconfig)
	}

	gatewayOnly := []string{"DaemonSet kube-system/apiserver-proxy", "Secret kube-system/" + clientSecret, "ConfigMap kube-system/konnectivity-agent-kubeconfig"}
	inventory := inventoryOf(t, admin)
	for _, entry := range gatewayOnly {
		if !slices.Contains(inventory, entry) {
			t.Errorf("the inventory does not list %s: %v", entry, inventory)
		}
		obj := objectOf(t, admin, entry)
		if obj.GetLabels()[lodiav1alpha1.ManagedByLabel] != lodiav1alpha1.ManagedBy || !slices.ContainsFunc(obj.GetManagedFields(), func(f metav1.ManagedFieldsEntry) bool {
			return f.Manager == tenant.FieldManager && f.Operation == metav1.ManagedFieldsOperationApply
		}) {
			t.Errorf("%s is not applied as %s with the managed-by label", entry, tenant.FieldManager)
		}
	}

	for _, name := range []string{"kube-proxy", "konnectivity-agent"} {
		other := get(t, admin, &appsv1.DaemonSet{}, "kube-system", name)
		other.Status = appsv1.DaemonSetStatus{ObservedGeneration: other.Generation}
		if err := admin.Status().Update(ctx, other); err != nil {
			t.Fatal(err)
		}
	}
	coredns := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns")
	coredns.Status = appsv1.DeploymentStatus{ObservedGeneration: coredns.Generation, Replicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2}
	if err := admin.Status().Update(ctx, coredns); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending); err != nil {
			return err
		}
		if c := condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable); c.Message != "apiserver-proxy 0 of 0 available" {
			return fmt.Errorf("AddonsAvailable says %q", c.Message)
		}
		return nil
	})
	ds = get(t, admin, &appsv1.DaemonSet{}, "kube-system", "apiserver-proxy")
	ds.Status = appsv1.DaemonSetStatus{ObservedGeneration: ds.Generation, DesiredNumberScheduled: 2, UpdatedNumberScheduled: 2, NumberAvailable: 1, NumberMisscheduled: 0}
	if err := admin.Status().Update(ctx, ds); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		if c := condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable); c.Message != "apiserver-proxy 1 of 2 available" {
			return fmt.Errorf("AddonsAvailable says %q", c.Message)
		}
		return nil
	})
	ds = get(t, admin, &appsv1.DaemonSet{}, "kube-system", "apiserver-proxy")
	ds.Status.NumberAvailable = 2
	if err := admin.Status().Update(ctx, ds); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable); err != nil {
			return err
		}
		if c := condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable); !strings.Contains(c.Message, "apiserver-proxy") {
			return fmt.Errorf("AddonsAvailable says %q", c.Message)
		}
		return nil
	})

	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	refresh(t, cp)
	left := cp.DeepCopy()
	left.Spec.Endpoint = lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}}
	left.Status.Endpoint.Tunnel = nil
	r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: left}, Reader: k8s, ApplyPeriod: time.Hour, Dial: *dial.Load()}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	inventory = inventoryOf(t, admin)
	for _, entry := range gatewayOnly {
		if slices.Contains(inventory, entry) {
			t.Errorf("the inventory still lists %s, which left the render", entry)
		}
		if exists(t, admin, entry) {
			t.Errorf("%s survived leaving the render", entry)
		}
	}
}

type failingSecret struct {
	client.Reader
	name  string
	reads atomic.Int32
}

func (r *failingSecret) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok && key.Name == r.name {
		r.reads.Add(1)
		return errReadFailed
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestOnlyGatewayModeReadsTheTunnelsClient(t *testing.T) {
	for _, c := range []struct {
		namespace string
		mode      func(*lodiav1alpha1.ControlPlane)
		published func(*testing.T, *lodiav1alpha1.ControlPlane)
		reads     bool
	}{
		{"tenant-reads-load-balancer", func(*lodiav1alpha1.ControlPlane) {}, publish, false},
		{"tenant-reads-node-port", func(cp *lodiav1alpha1.ControlPlane) {
			cp.Spec.Endpoint = lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}
		}, publish, false},
		{"tenant-reads-gateway", gatewayMode, publishGateway, true},
	} {
		cp := controlPlane(t, c.namespace, c.mode)
		c.published(t, cp)
		reconcilePKI(t, gatewayIssuer(), cp)
		update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
		setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
		refresh(t, cp)
		reader := &failingSecret{Reader: k8s, name: "dev-tunnel-client"}
		waiting := cp.DeepCopy()
		meta.RemoveStatusCondition(&waiting.Status.Conditions, lodiav1alpha1.ConditionAccessReady)
		r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: waiting}, Reader: reader, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: unreachable}
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
			t.Errorf("%s: a pass waiting for access returned %v", c.namespace, err)
		}
		r.Client = snapshot{Client: k8s, cp: cp.DeepCopy()}
		_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		if got := errors.Is(err, errReadFailed); got != c.reads {
			t.Errorf("%s: a pass whose read of the tunnel's client fails returned %v, want the read's error: %t", c.namespace, err, c.reads)
		}
		if got := reader.reads.Load() > 0; got != c.reads {
			t.Errorf("%s: %d reads of the tunnel's client, want some: %t", c.namespace, reader.reads.Load(), c.reads)
		}
	}
}

func TestATunnelClientAnotherOwnerKeepsIsNotUsed(t *testing.T) {
	cp := controlPlane(t, "tenant-kept-tunnel-client", gatewayMode)
	publishGateway(t, cp)
	reconcilePKI(t, gatewayIssuer(), cp)
	dial.Store(&unreachable)
	pair := get(t, k8s, &corev1.Secret{}, cp.Namespace, "dev-tunnel-client")
	own := pair.OwnerReferences
	pair.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "dev", UID: "predecessor", Controller: new(true),
	}}
	if err := k8s.Update(t.Context(), pair); err != nil {
		t.Fatal(err)
	}
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess); err != nil {
			return err
		}
		if c := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); !strings.Contains(c.Message, "Secret dev-tunnel-client is kept by ControlPlane dev with UID predecessor") {
			return fmt.Errorf("AddonsApplied says %q", c.Message)
		}
		return nil
	})
	pair.OwnerReferences = own
	if err := k8s.Update(t.Context(), pair); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying)
	})
}

func TestAGatewayEndpointWithoutItsTunnelWaitsForEndpoint(t *testing.T) {
	cp := controlPlane(t, "tenant-no-tunnel", gatewayMode)
	publish(t, cp)
	reconcilePKI(t, gatewayIssuer(), cp)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint); err != nil {
			return err
		}
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint)
	})
}

func renew(t *testing.T, cp *lodiav1alpha1.ControlPlane, pair string) {
	t.Helper()
	secret := get(t, k8s, &corev1.Secret{}, cp.Namespace, "dev-tunnel-client")
	secret.Data["tls.crt"], secret.Data["tls.key"] = []byte(pair+" certificate"), []byte(pair+" key")
	if err := k8s.Update(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
}

func holds(t *testing.T, admin *tenantClient, pair string) error {
	t.Helper()
	secret := get(t, admin, &corev1.Secret{}, "kube-system", clientSecret)
	if string(secret.Data["tls.crt"]) != pair+" certificate" || string(secret.Data["tls.key"]) != pair+" key" {
		return fmt.Errorf("the tenant's Secret holds %q, want %q", secret.Data["tls.crt"], pair)
	}
	return nil
}

func applied(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
}

func TestARenewalReachesTheTenantWhateverTheConditionsSay(t *testing.T) {
	cp, admin, issuer := gatewayTenant(t, "tenant-delivery")
	applied(t, cp)
	arrives := func(pair string) {
		t.Helper()
		renew(t, cp, pair)
		eventually(t, 30*time.Second, func() error { return holds(t, admin, pair) })
	}

	setReplicas(t, cp, 3)
	edited := get(t, admin, &corev1.Secret{}, "kube-system", clientSecret)
	edited.Data["tls.crt"] = []byte("another manager's edit")
	if err := admin.Update(t.Context(), edited); err != nil {
		t.Fatal(err)
	}
	arrives("while the generation is stuck")
	if access := condition(t, cp, lodiav1alpha1.ConditionAccessReady); access.ObservedGeneration == cp.Generation {
		t.Errorf("AccessReady describes generation %d, which is not stuck", access.ObservedGeneration)
	}
	if replicas := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns").Spec.Replicas; replicas == nil || *replicas == 3 {
		t.Errorf("the ordinary apply ran while the generation is stuck: CoreDNS has %v replicas", replicas)
	}

	refresh(t, cp)
	applyStatus(t, cp, "test/"+lodiav1alpha1.ConditionAPIServerReady, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAPIServerReady, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, cp.Generation),
	}})
	applyStatus(t, cp, "test/"+lodiav1alpha1.ConditionAccessReady, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAccessReady, metav1.ConditionTrue, lodiav1alpha1.ReasonBound, cp.Generation),
	}})
	reconcilePKI(t, issuer, cp)
	expectNow(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAPIServer)
	arrives("with APIServerReady False, Refused")
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	})

	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	rebindOperator(t, admin, []rbacv1.PolicyRule{
		{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"patch"}},
		{NonResourceURLs: []string{"*"}, Verbs: []string{"*"}},
	}, authorizationv1.ResourceAttributes{Resource: "configmaps", Verb: "patch", Namespace: "kube-system"})
	eventually(t, 30*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused)
	})
	arrives("while an earlier object is refused")
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused); err != nil {
		t.Error(err)
	}
}

func TestAnotherOwnersSecretUnderThatNameIsLeftAlone(t *testing.T) {
	cp, admin := gatewayTenantWithoutAccess(t, "tenant-delivery-foreign")
	theirs := map[string][]byte{"tls.crt": []byte("the owner's certificate"), "tls.key": []byte("the owner's key")}
	if err := admin.Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: clientSecret},
		Type:       corev1.SecretTypeTLS,
		Data:       theirs,
	}); err != nil {
		t.Fatal(err)
	}
	before := get(t, admin, &corev1.Secret{}, "kube-system", clientSecret).ResourceVersion
	grantAccess(t, cp)
	eventually(t, 30*time.Second, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying); err != nil {
			return err
		}
		if c := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); c.Message != "Secret kube-system/"+clientSecret+" exists and is not Lodia's; delete it or disable the addon" {
			return fmt.Errorf("AddonsApplied says %q", c.Message)
		}
		return nil
	})
	renew(t, cp, "renewed")
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	refresh(t, cp)
	r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: cp.DeepCopy()}, Reader: k8s, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: *dial.Load()}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	after := get(t, admin, &corev1.Secret{}, "kube-system", clientSecret)
	if after.ResourceVersion != before || !equality.Semantic.DeepEqual(after.Data, theirs) {
		t.Errorf("the owner's Secret went from resourceVersion %s to %s, holding %q", before, after.ResourceVersion, after.Data)
	}
	if slices.ContainsFunc(after.ManagedFields, func(f metav1.ManagedFieldsEntry) bool { return f.Manager == tenant.FieldManager }) {
		t.Errorf("Lodia wrote the owner's Secret: %+v", after.ManagedFields)
	}
	if inventory := inventoryOf(t, admin); slices.Contains(inventory, "Secret kube-system/"+clientSecret) {
		t.Errorf("the inventory lists the owner's Secret: %v", inventory)
	}
}

func TestTheFirstWriteIsTheInventorys(t *testing.T) {
	ctx := t.Context()
	cp, admin := gatewayTenantWithoutAccess(t, "tenant-delivery-first")
	if err := admin.Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "lodia:operator"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "cluster-admin"},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: "lodia:operator"}},
	}); err != nil {
		t.Fatal(err)
	}
	r := &tenant.Reconciler{Client: k8s, Reader: k8s, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: *dial.Load()}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if exists(t, admin, "Secret kube-system/"+clientSecret) {
		t.Error("delivery wrote a Secret that did not exist")
	}
	if exists(t, admin, "ConfigMap kube-system/lodia-inventory") {
		t.Error("a pass waiting for access wrote the inventory")
	}

	grantAccess(t, cp)
	applied(t, cp)
	if !slices.Contains(inventoryOf(t, admin), "Secret kube-system/"+clientSecret) {
		t.Error("the inventory does not list the Secret it wrote")
	}
	pair := get(t, k8s, &corev1.Secret{}, cp.Namespace, "dev-tunnel-client")
	if delivered := get(t, admin, &corev1.Secret{}, "kube-system", clientSecret); !equality.Semantic.DeepEqual(delivered.Data["tls.crt"], pair.Data["tls.crt"]) {
		t.Error("the Secret does not hold the pair")
	}
}

func TestAnUnchangedPairMovesNoResourceVersion(t *testing.T) {
	cp, admin, _ := gatewayTenant(t, "tenant-delivery-unchanged")
	applied(t, cp)
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	refresh(t, cp)
	access := cp.DeepCopy()
	waiting := cp.DeepCopy()
	meta.RemoveStatusCondition(&waiting.Status.Conditions, lodiav1alpha1.ConditionAccessReady)
	before := get(t, admin, &corev1.Secret{}, "kube-system", clientSecret)
	for name, at := range map[string]*lodiav1alpha1.ControlPlane{"a pass that applies": access, "a pass that waits": waiting} {
		r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: at}, Reader: k8s, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: *dial.Load()}
		for range 3 {
			if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		}
		after := get(t, admin, &corev1.Secret{}, "kube-system", clientSecret)
		if after.ResourceVersion != before.ResourceVersion || !equality.Semantic.DeepEqual(after.ManagedFields, before.ManagedFields) {
			t.Errorf("%s moved the Secret: resourceVersion %s, then %s; managed fields %+v, then %+v", name, before.ResourceVersion, after.ResourceVersion, before.ManagedFields, after.ManagedFields)
		}
	}
}

func TestABlockingDialCostsDeliveryItsTimeoutAndThePassReports(t *testing.T) {
	cp := controlPlane(t, "tenant-delivery-blocked", gatewayMode)
	publishGateway(t, cp)
	reconcilePKI(t, gatewayIssuer(), cp)
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable, cp.Generation),
	}})
	var dials atomic.Int32
	dropsPackets := func(ctx context.Context, _, _ string) (net.Conn, error) {
		dials.Add(1)
		timeout, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		<-timeout.Done()
		return nil, timeout.Err()
	}
	pass := func() (time.Duration, error) {
		t.Helper()
		refresh(t, cp)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: cp.DeepCopy()}, Reader: k8s, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: dropsPackets}
		start := time.Now()
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		return time.Since(start), err
	}

	elapsed, err := pass()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("a pass that waits for access took %s", elapsed)
	if elapsed < 9*time.Second || elapsed > 15*time.Second {
		t.Errorf("a pass that waits for access took %s, want the delivery's 10 s", elapsed)
	}
	if dials.Load() == 0 {
		t.Error("nothing dialled the tenant")
	}
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)

	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	elapsed, err = pass()
	if err == nil {
		t.Error("a pass that could not reach the tenant succeeded")
	}
	t.Logf("a pass that applies took %s", elapsed)
	if elapsed < 35*time.Second || elapsed >= time.Minute {
		t.Errorf("a pass that applies took %s, want the delivery's 10 s and the ordinary client's 30 s dial", elapsed)
	}
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying)
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying)
}

func TestWhatStopsDelivery(t *testing.T) {
	ctx := t.Context()
	cp := controlPlane(t, "tenant-delivery-stops", gatewayMode)
	publishGateway(t, cp)
	reconcilePKI(t, gatewayIssuer(), cp)
	var dials atomic.Int32
	counting := func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("connection refused")
	}
	r := &tenant.Reconciler{Client: k8s, Reader: k8s, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: counting}
	pass := func() ctrl.Result {
		t.Helper()
		dials.Store(0)
		result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}

	if result := pass(); dials.Load() == 0 || result.RequeueAfter != time.Hour {
		t.Fatalf("a pass waiting for access dialled %d times and requeues after %s, want a delivery and the apply period", dials.Load(), result.RequeueAfter)
	}

	for _, name := range []string{"dev-tunnel-client", "dev-lodia-kubeconfig"} {
		secret := get(t, k8s, &corev1.Secret{}, cp.Namespace, name)
		own := secret.OwnerReferences
		secret.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "dev", UID: "predecessor", Controller: new(true),
		}}
		if err := k8s.Update(ctx, secret); err != nil {
			t.Fatal(err)
		}
		if pass(); dials.Load() != 0 {
			t.Errorf("a pass dialled the tenant while another owner keeps %s", name)
		}
		secret.OwnerReferences = own
		if err := k8s.Update(ctx, secret); err != nil {
			t.Fatal(err)
		}

		if err := k8s.Delete(ctx, secret); err != nil {
			t.Fatal(err)
		}
		if pass(); dials.Load() != 0 {
			t.Errorf("a pass dialled the tenant without %s", name)
		}
		reconcilePKI(t, gatewayIssuer(), cp)
	}

	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	if result := pass(); dials.Load() != 0 || result.RequeueAfter != 0 {
		t.Errorf("a paused pass dialled %d times and requeues after %s", dials.Load(), result.RequeueAfter)
	}
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.Paused = false
		cp.Finalizers = append(cp.Finalizers, "test.lodia.io/hold")
	})
	if err := k8s.Delete(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if result := pass(); dials.Load() != 0 || result.RequeueAfter != 0 {
		t.Errorf("a pass over a deleted ControlPlane dialled %d times and requeues after %s", dials.Load(), result.RequeueAfter)
	}
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Finalizers = nil })

	other := controlPlane(t, "tenant-delivery-other-mode")
	publish(t, other)
	reconcilePKI(t, gatewayIssuer(), other)
	dials.Store(0)
	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(other)})
	if err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 0 || result.RequeueAfter != 0 {
		t.Errorf("a loadBalancer pass waiting for access dialled %d times and requeues after %s", dials.Load(), result.RequeueAfter)
	}
}

func TestAFailedDeliveryStopsNothing(t *testing.T) {
	ctx := t.Context()
	cp, admin, _ := gatewayTenant(t, "tenant-delivery-fails")
	applied(t, cp)
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	refresh(t, cp)
	renew(t, cp, "through the ordinary apply")
	reaches := *dial.Load()
	var dials atomic.Int32
	firstFails := func(ctx context.Context, network, address string) (net.Conn, error) {
		if dials.Add(1) == 1 {
			return nil, errors.New("connection refused")
		}
		return reaches(ctx, network, address)
	}
	r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: cp.DeepCopy()}, Reader: k8s, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: firstFails}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if err := holds(t, admin, "through the ordinary apply"); err != nil {
		t.Errorf("after a delivery that could not reach the tenant: %v", err)
	}

	waiting := cp.DeepCopy()
	meta.RemoveStatusCondition(&waiting.Status.Conditions, lodiav1alpha1.ConditionAccessReady)
	r = &tenant.Reconciler{Client: snapshot{Client: k8s, cp: waiting}, Reader: &failingSecret{Reader: k8s, name: "dev-tunnel-client"}, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: reaches}
	if result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil || result.RequeueAfter != time.Hour {
		t.Errorf("a pass whose delivery could not read the pair returned %+v, %v", result, err)
	}
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)

	refresh(t, cp)
	rebindOperator(t, admin, []rbacv1.PolicyRule{
		{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"get", "list", "watch"}},
		{NonResourceURLs: []string{"*"}, Verbs: []string{"*"}},
	}, authorizationv1.ResourceAttributes{Resource: "secrets", Verb: "patch", Namespace: "kube-system"})
	renew(t, cp, "refused")
	r = &tenant.Reconciler{Client: snapshot{Client: k8s, cp: cp.DeepCopy()}, Reader: k8s, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: reaches}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !apierrors.IsForbidden(err) {
		t.Errorf("a pass whose applies are refused returned %v, want the ordinary apply's refusal", err)
	}
	if err := holds(t, admin, "through the ordinary apply"); err != nil {
		t.Error(err)
	}
	expectNow(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused)
}

func inTenant(t *testing.T, admin *tenantClient) map[string]map[string]any {
	t.Helper()
	objects := map[string]map[string]any{}
	for _, entry := range append(inventoryOf(t, admin), "ConfigMap kube-system/lodia-inventory") {
		objects[entry] = objectOf(t, admin, entry).Object
	}
	return objects
}

func TestAMissingSecretIsReportedAndNothingIsWritten(t *testing.T) {
	for _, c := range []struct {
		namespace string
		tenant    func(*testing.T, string) (*lodiav1alpha1.ControlPlane, *tenantClient)
		secrets   []string
	}{
		{"tenant-missing-gateway", func(t *testing.T, namespace string) (*lodiav1alpha1.ControlPlane, *tenantClient) {
			t.Helper()
			cp, admin, _ := gatewayTenant(t, namespace)
			return cp, admin
		}, []string{"dev-tunnel-client", "dev-ca-bundle", "dev-lodia-kubeconfig"}},
		{"tenant-missing-load-balancer", servedTenant, []string{"dev-ca-bundle", "dev-lodia-kubeconfig"}},
	} {
		t.Run(c.namespace, func(t *testing.T) {
			ctx := t.Context()
			cp, admin := c.tenant(t, c.namespace)
			applied(t, cp)
			update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
			setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
			pass := func() ctrl.Result {
				t.Helper()
				refresh(t, cp)
				r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: cp.DeepCopy()}, Reader: k8s, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: *dial.Load()}
				result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			pass()
			for _, name := range c.secrets {
				secret := get(t, k8s, &corev1.Secret{}, cp.Namespace, name)
				before := inTenant(t, admin)
				if err := k8s.Delete(ctx, secret); err != nil {
					t.Fatal(err)
				}
				if result := pass(); result.RequeueAfter != time.Minute {
					t.Errorf("without %s the pass requeues after %s, want a minute", name, result.RequeueAfter)
				}
				for _, conditionType := range []string{lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ConditionAddonsAvailable} {
					expectNow(t, cp, conditionType, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
					if got := condition(t, cp, conditionType); got.Message != "Secret "+name+" does not exist" {
						t.Errorf("without %s, %s says %q", name, conditionType, got.Message)
					}
				}
				after := inTenant(t, admin)
				for entry, object := range before {
					if !equality.Semantic.DeepEqual(object, after[entry]) {
						t.Errorf("a pass without %s wrote %s", name, entry)
					}
				}
				secret.ObjectMeta = metav1.ObjectMeta{Namespace: cp.Namespace, Name: name, OwnerReferences: secret.OwnerReferences}
				if err := k8s.Create(ctx, secret); err != nil {
					t.Fatal(err)
				}
				pass()
				expectNow(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
			}
		})
	}
}

func TestDeliveryUsesOnlyWhatThisControlPlaneControls(t *testing.T) {
	ctx := t.Context()
	cp, admin, _ := gatewayTenant(t, "tenant-delivery-controlled")
	applied(t, cp)
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	plain := func(*lodiav1alpha1.ControlPlane) {}
	restored := func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.RestoreFrom = &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: "backup"}}
	}
	pass := func(shape func(*lodiav1alpha1.ControlPlane)) {
		t.Helper()
		refresh(t, cp)
		waiting := cp.DeepCopy()
		shape(waiting)
		meta.SetStatusCondition(&waiting.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionAccessReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForAPIServer})
		r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: waiting}, Reader: k8s, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: *dial.Load()}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
			t.Fatal(err)
		}
	}
	renew(t, cp, "this ControlPlane's")
	pass(plain)
	if err := holds(t, admin, "this ControlPlane's"); err != nil {
		t.Fatal(err)
	}

	renew(t, cp, "not delivered yet")
	predecessor := metav1.OwnerReference{APIVersion: lodiav1alpha1.GroupVersion.String(), Kind: "ControlPlane", Name: "dev", UID: "predecessor"}
	for _, name := range []string{"dev-tunnel-client", "dev-lodia-kubeconfig"} {
		secret := get(t, k8s, &corev1.Secret{}, cp.Namespace, name)
		own := secret.OwnerReferences
		for _, owners := range [][]metav1.OwnerReference{nil, append(slices.Clone(own), predecessor)} {
			secret.OwnerReferences = owners
			if err := k8s.Update(ctx, secret); err != nil {
				t.Fatal(err)
			}
			for shape, change := range map[string]func(*lodiav1alpha1.ControlPlane){"plain": plain, "restored": restored} {
				pass(change)
				if err := holds(t, admin, "this ControlPlane's"); err != nil {
					t.Errorf("%s with the owners %+v, on a %s ControlPlane: %v", name, owners, shape, err)
				}
			}
		}
		secret.OwnerReferences = own
		if err := k8s.Update(ctx, secret); err != nil {
			t.Fatal(err)
		}
	}
	pass(restored)
	if err := holds(t, admin, "not delivered yet"); err != nil {
		t.Error(err)
	}
}
