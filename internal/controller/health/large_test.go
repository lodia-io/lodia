// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const (
	operatorsTimeout = 5 * time.Second
	seedWorkers      = 16
)

func kubeletNode(name string, at time.Time) *corev1.Node {
	images := make([]corev1.ContainerImage, 24)
	for i := range images {
		images[i] = corev1.ContainerImage{Names: []string{fmt.Sprintf("registry.example.com/team/image-%02d@sha256:%064d", i, i), fmt.Sprintf("registry.example.com/team/image-%02d:v1.%d.0", i, i)}, SizeBytes: 123456789}
	}
	condition := func(t corev1.NodeConditionType, status corev1.ConditionStatus, reason, message string) corev1.NodeCondition {
		return corev1.NodeCondition{Type: t, Status: status, Reason: reason, Message: message, LastHeartbeatTime: metav1.NewTime(at), LastTransitionTime: metav1.NewTime(at)}
	}
	resources := corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("32780676Ki"),
		corev1.ResourceEphemeralStorage: resource.MustParse("253196720Ki"), corev1.ResourcePods: resource.MustParse("110"),
		"hugepages-1Gi": resource.MustParse("0"), "hugepages-2Mi": resource.MustParse("0"),
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"beta.kubernetes.io/arch": "amd64", "beta.kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64",
				"kubernetes.io/hostname": name, "kubernetes.io/os": "linux", "topology.kubernetes.io/zone": "zone-a",
			},
			Annotations: map[string]string{
				"kubeadm.alpha.kubernetes.io/cri-socket":                 "unix:///run/containerd/containerd.sock",
				"node.alpha.kubernetes.io/ttl":                           "0",
				"volumes.kubernetes.io/controller-managed-attach-detach": "true",
			},
		},
		Spec: corev1.NodeSpec{PodCIDR: "10.244.1.0/24", PodCIDRs: []string{"10.244.1.0/24"}, ProviderID: "provider://" + name},
		Status: corev1.NodeStatus{
			Capacity: resources, Allocatable: resources,
			Conditions: []corev1.NodeCondition{
				condition(corev1.NodeMemoryPressure, corev1.ConditionFalse, "KubeletHasSufficientMemory", "kubelet has sufficient memory available"),
				condition(corev1.NodeDiskPressure, corev1.ConditionFalse, "KubeletHasNoDiskPressure", "kubelet has no disk pressure"),
				condition(corev1.NodePIDPressure, corev1.ConditionFalse, "KubeletHasSufficientPID", "kubelet has sufficient PID available"),
				condition(corev1.NodeReady, corev1.ConditionTrue, "KubeletReady", "kubelet is posting ready status"),
			},
			Addresses:       []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.1"}, {Type: corev1.NodeHostName, Address: name}},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: 10250}},
			NodeInfo: corev1.NodeSystemInfo{
				MachineID: "0123456789abcdef0123456789abcdef", SystemUUID: "01234567-89ab-cdef-0123-456789abcdef", BootID: "01234567-89ab-cdef-0123-456789abcdef",
				KernelVersion: "6.8.0-1000-generic", OSImage: "Ubuntu 24.04.1 LTS", ContainerRuntimeVersion: "containerd://2.0.0",
				KubeletVersion: "v1.35.0", OperatingSystem: "linux", Architecture: "amd64",
			},
			Images: images,
		},
	}
}

func seedKubeletNodes(t *testing.T, nodes, leases int) {
	t.Helper()
	cfg, err := clientcmd.RESTConfigFromKubeConfig(tenantKubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	cfg.QPS = -1
	seeding, err := client.New(cfg, client.Options{Scheme: k8s.Scheme()})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	names := make(chan int)
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		failure error
	)
	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		failure = err
	}
	for range seedWorkers {
		wg.Go(func() {
			for i := range names {
				name := fmt.Sprintf("node-%05d", i)
				if i < nodes {
					if err := seeding.Create(t.Context(), kubeletNode(name, now)); err != nil {
						fail(err)
					}
				}
				if i < leases {
					lease := &coordinationv1.Lease{
						ObjectMeta: metav1.ObjectMeta{Namespace: corev1.NamespaceNodeLease, Name: name},
						Spec:       coordinationv1.LeaseSpec{HolderIdentity: &name, LeaseDurationSeconds: new(int32(40)), RenewTime: &metav1.MicroTime{Time: now}},
					}
					if err := seeding.Create(t.Context(), lease); err != nil {
						fail(err)
					}
				}
			}
		})
	}
	for i := range max(nodes, leases) {
		names <- i
	}
	close(names)
	wg.Wait()
	if failure != nil {
		t.Fatal(failure)
	}
}

func longestContinue(t *testing.T, list func(token string) (next string, err error)) int {
	t.Helper()
	longest := 0
	for token := ""; ; {
		next, err := list(token)
		if err != nil {
			t.Fatal(err)
		}
		longest = max(longest, len(next))
		if next == "" {
			return longest
		}
		token = next
	}
}

func continueTokens(t *testing.T, c client.Client) (leases, nodes int) {
	t.Helper()
	leases = longestContinue(t, func(token string) (string, error) {
		var list coordinationv1.LeaseList
		err := c.List(t.Context(), &list, client.InNamespace(corev1.NamespaceNodeLease), client.Limit(500), client.Continue(token))
		return list.Continue, err
	})
	nodes = longestContinue(t, func(token string) (string, error) {
		var list corev1.NodeList
		err := c.List(t.Context(), &list, client.Limit(100), client.Continue(token))
		return list.Continue, err
	})
	return leases, nodes
}

func TestATenantOfMaxNodesNodesIsCountedWholeAtTheOperatorsTimeout(t *testing.T) {
	const maxNodes = 5000
	cp := tenantControlPlane(t)
	seedKubeletNodes(t, maxNodes, maxNodes)
	leaseToken, nodeToken := continueTokens(t, tenant)
	t.Logf("the longest continue token of %d Leases listed 500 to a page is %d bytes, of %d Nodes listed 100 to a page %d bytes", maxNodes, leaseToken, maxNodes, nodeToken)
	if max(leaseToken, nodeToken) > 1024 {
		t.Errorf("a kube-apiserver gave a continue token of %d bytes, want under a quarter of the 4,096 Health sends back", max(leaseToken, nodeToken))
	}
	now := clocktesting.NewFakePassiveClock(time.Now().Truncate(time.Second))
	pass := func() time.Duration {
		r := nodesReconciler(now)
		r.Timeout = operatorsTimeout
		begun := time.Now()
		sample(t, r, cp)
		return time.Since(begun)
	}

	took := pass()
	t.Logf("a first pass of %d Nodes and their Leases took %v", maxNodes, took)
	expectNotHeld(t, cp, silent(0, maxNodes))
	expectNodes(t, cp, nodesOf(maxNodes, maxNodes, 0, "v1.35.0"))

	create(t, tenant, kubeletNode(fmt.Sprintf("node-%05d", maxNodes), time.Now()))
	took = pass()
	t.Logf("a first pass of %d Nodes took %v", maxNodes+1, took)
	expectTooMany(t, cp, tooManyNodes(maxNodes))
	expectNodes(t, cp, truncatedNodes(maxNodes, maxNodes))
}

func TestAContinueTokenOfNamesOf253BytesIsUnderAQuarterOfTheBoundHealthSendsBack(t *testing.T) {
	_, inside := startTenant(t, func(*envtest.APIServer) {})
	name := func(i int) string { return fmt.Sprintf("%03d%s", i, strings.Repeat("a", 250)) }
	for i := range 501 {
		lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: corev1.NamespaceNodeLease, Name: name(i)}}
		create(t, inside, lease)
	}
	for i := range 101 {
		create(t, inside, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name(i)}})
	}
	leaseToken, nodeToken := continueTokens(t, inside)
	t.Logf("the longest continue token of Leases and Nodes named with 253 bytes is %d bytes for the Leases and %d for the Nodes", leaseToken, nodeToken)
	if leaseToken == 0 || nodeToken == 0 {
		t.Fatalf("the lists gave no continue token: %d and %d bytes", leaseToken, nodeToken)
	}
	if max(leaseToken, nodeToken) > 1024 {
		t.Errorf("a kube-apiserver gave a continue token of %d bytes, want under a quarter of the 4,096 Health sends back", max(leaseToken, nodeToken))
	}
}
