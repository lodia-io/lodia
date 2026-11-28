// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/tenant"
	"go.lodia.io/lodia/internal/testenv"
)

const nameHost = "api.dev.example.test"

func nameHostMode(cp *lodiav1alpha1.ControlPlane) {
	cp.Spec.Endpoint = lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{Host: nameHost}}
}

func noHostMode(cp *lodiav1alpha1.ControlPlane) {
	cp.Spec.Endpoint = lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}}
}

func publishNameHost(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	setTarget(t, cp, version, revision)
	applyStatus(t, cp, "test/endpoint", map[string]any{"endpoint": map[string]any{
		"apiServer":    map[string]any{"host": nameHost, "port": int64(apiServerPort)},
		"konnectivity": map[string]any{"host": nameHost, "port": int64(konnectivityPort)},
		"inCluster":    map[string]any{"host": lodiav1alpha1.DefaultAdvertiseAddress, "port": int64(6443)},
	}})
}

func TestATenantWhosePodsDoNotDialAnIPHostGetsApiserverProxyWithoutTheTunnel(t *testing.T) {
	for _, c := range []struct {
		namespace string
		mode      func(*lodiav1alpha1.ControlPlane)
	}{
		{"tenant-proxy-name-host", nameHostMode},
		{"tenant-proxy-no-host", noHostMode},
	} {
		t.Run(c.namespace, func(t *testing.T) {
			ctx := t.Context()
			cp := controlPlane(t, c.namespace, c.mode)
			publishNameHost(t, cp)
			reconcilePKI(t, gatewayIssuer(), cp)
			admin := startTenant(t, cp)
			setDial(t, admin.host)
			grantAccess(t, cp)
			testenv.Eventually(t, func() error {
				return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
			})

			spec := get(t, admin, &appsv1.DaemonSet{}, "kube-system", "apiserver-proxy").Spec.Template.Spec
			if len(spec.Containers) != 1 || !slices.Equal(spec.Containers[0].Args, []string{"proxy", "--listen=240.0.0.1:6443", "--upstream=" + nameHost + ":443", "--socket=/run/lodia/proxy.sock"}) {
				t.Errorf("the proxy runs %v", spec.Containers)
			}
			if len(spec.Volumes) != 1 || spec.Volumes[0].EmptyDir == nil || len(spec.Containers[0].VolumeMounts) != 1 || spec.SecurityContext.FSGroup != nil {
				t.Errorf("the proxy's pod mounts %v into %v with fsGroup %v, want only its socket's emptyDir", spec.Volumes, spec.Containers[0].VolumeMounts, spec.SecurityContext.FSGroup)
			}
			if !spec.HostNetwork || spec.DNSPolicy != corev1.DNSDefault {
				t.Errorf("the proxy's pod runs with hostNetwork %t and dnsPolicy %s", spec.HostNetwork, spec.DNSPolicy)
			}
			if exists(t, admin, "Secret kube-system/"+clientSecret) {
				t.Errorf("the tenant holds %s without the tunnel", clientSecret)
			}
			for _, name := range []string{"dev-cakey-tunnel", "dev-tunnel-ca", "dev-tunnel-serving", "dev-tunnel-client"} {
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, &corev1.Secret{}); !apierrors.IsNotFound(err) {
					t.Errorf("PKI wrote %s outside gateway mode: %v", name, err)
				}
			}

			agents := get(t, admin, &appsv1.DaemonSet{}, "kube-system", "konnectivity-agent").Spec.Template.Spec
			if agents.DNSPolicy != corev1.DNSDefault || !slices.Contains(agents.Containers[0].Command, "--kubeconfig=/etc/konnectivity-agent/kubeconfig") {
				t.Errorf("the agents resolve with %s and run %v", agents.DNSPolicy, agents.Containers[0].Command)
			}
			if kubeconfig := get(t, admin, &corev1.ConfigMap{}, "kube-system", "konnectivity-agent-kubeconfig").Data["kubeconfig"]; !strings.Contains(kubeconfig, `server: "https://`+nameHost+`:443"`) {
				t.Errorf("the agents' kubeconfig is %q", kubeconfig)
			}

			slice := get(t, admin, &discoveryv1.EndpointSlice{}, "default", "kubernetes")
			if len(slice.Endpoints) != 1 || !slices.Equal(slice.Endpoints[0].Addresses, []string{lodiav1alpha1.DefaultAdvertiseAddress}) || len(slice.Ports) != 1 || *slice.Ports[0].Port != 6443 {
				t.Errorf("the kubernetes EndpointSlice holds %+v %+v, want the advertise address and 6443", slice.Endpoints, slice.Ports)
			}

			inventory := inventoryOf(t, admin)
			for _, entry := range []string{"DaemonSet kube-system/apiserver-proxy", "ConfigMap kube-system/konnectivity-agent-kubeconfig"} {
				if !slices.Contains(inventory, entry) {
					t.Errorf("the inventory does not list %s: %v", entry, inventory)
				}
			}
			if slices.Contains(inventory, "Secret kube-system/"+clientSecret) {
				t.Errorf("the inventory lists %s without the tunnel", clientSecret)
			}

			for _, name := range []string{"kube-proxy", "konnectivity-agent", "apiserver-proxy"} {
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
			testenv.Eventually(t, func() error {
				if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable); err != nil {
					return err
				}
				if c := condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable); !strings.Contains(c.Message, "apiserver-proxy") {
					return fmt.Errorf("AddonsAvailable says %q", c.Message)
				}
				return nil
			})
		})
	}
}

func TestATunnelsProxyLeftInTheTenantBecomesThePlainProxy(t *testing.T) {
	ctx := t.Context()
	cp, admin, _ := gatewayTenant(t, "tenant-proxy-leftover")
	testenv.Eventually(t, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	if !exists(t, admin, "Secret kube-system/"+clientSecret) {
		t.Fatalf("the gateway render left no %s", clientSecret)
	}

	pause(t, cp)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	refresh(t, cp)
	restored := cp.DeepCopy()
	nameHostMode(restored)
	restored.Status.Endpoint = lodiav1alpha1.EndpointStatus{
		APIServer:    lodiav1alpha1.HostPort{Host: nameHost, Port: apiServerPort},
		Konnectivity: lodiav1alpha1.HostPort{Host: nameHost, Port: konnectivityPort},
		InCluster:    lodiav1alpha1.HostPort{Host: lodiav1alpha1.DefaultAdvertiseAddress, Port: 6443},
	}
	r := &tenant.Reconciler{Client: snapshot{Client: k8s, cp: restored}, Reader: k8s, Secrets: k8s, Revision: revision, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Dial: *dial.Load()}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}

	spec := get(t, admin, &appsv1.DaemonSet{}, "kube-system", "apiserver-proxy").Spec.Template.Spec
	if !slices.Equal(spec.Containers[0].Args, []string{"proxy", "--listen=240.0.0.1:6443", "--upstream=" + nameHost + ":443", "--socket=/run/lodia/proxy.sock"}) {
		t.Errorf("the proxy runs %v", spec.Containers[0].Args)
	}
	if len(spec.Volumes) != 1 || len(spec.Containers[0].VolumeMounts) != 1 || spec.SecurityContext.FSGroup != nil {
		t.Errorf("the proxy's pod keeps %v mounted at %v with fsGroup %v", spec.Volumes, spec.Containers[0].VolumeMounts, spec.SecurityContext.FSGroup)
	}
	if exists(t, admin, "Secret kube-system/"+clientSecret) || slices.Contains(inventoryOf(t, admin), "Secret kube-system/"+clientSecret) {
		t.Errorf("%s survived the tunnel's leaving the render", clientSecret)
	}
}
