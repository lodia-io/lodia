// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
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

func gatewayTenant(t *testing.T, namespace string) (*lodiav1alpha1.ControlPlane, *tenantClient, *pki.Reconciler) {
	t.Helper()
	cp := controlPlane(t, namespace, gatewayMode)
	publishGateway(t, cp)
	issuer := gatewayIssuer()
	reconcilePKI(t, issuer, cp)
	admin := startTenant(t, cp)
	setDial(t, admin.host)
	issuer.Dial = *dial.Load()
	setCondition(t, cp, lodiav1alpha1.ConditionAPIServerReady, lodiav1alpha1.ReasonReady)
	reconcilePKI(t, issuer, cp)
	return cp, admin, issuer
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
	name string
}

func (r failingSecret) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok && key.Name == r.name {
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
		r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: cp.DeepCopy()}, Reader: failingSecret{Reader: k8s, name: "dev-tunnel-client"}, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: unreachable}
		_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		if got := errors.Is(err, errReadFailed); got != c.reads {
			t.Errorf("%s: a pass whose read of the tunnel's client fails returned %v, want the read's error: %t", c.namespace, err, c.reads)
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
