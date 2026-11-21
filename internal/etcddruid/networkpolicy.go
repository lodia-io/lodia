// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid

import (
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	networkingv1ac "k8s.io/client-go/applyconfigurations/networking/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/contract"
)

// etcd-druid's defaults for the ports Lodia leaves unset in the Etcd
// (etcd-druid@v0.39.2 internal/common/constants.go#L55-L60). The policy names
// them by number: the peer port and the sidecar's share the container port
// name "server" (internal/component/statefulset/constants.go#L12-L15).
const (
	etcdPeerPort          int32 = 2380
	etcdBackupRestorePort int32 = 8080
)

// NetworkPolicy is the etcd's NetworkPolicy of cp, whose operator runs in
// operatorNamespace.
func (e *Engine) NetworkPolicy(cp *lodiav1alpha1.ControlPlane, s *Store, operatorNamespace string) *networkingv1ac.NetworkPolicyApplyConfiguration {
	return networkPolicy(cp, s.etcd, e.Namespace, operatorNamespace)
}

// networkPolicy admits to the members of the Etcd what dials them, at any
// member count (docs/README.md, A tenant namespace). The operator's namespace
// gets a rule of its own only when etcd-druid's rule does not cover it.
func networkPolicy(cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd, druidNamespace, operatorNamespace string) *networkingv1ac.NetworkPolicyApplyConfiguration {
	members := metav1ac.LabelSelector().WithMatchLabels(memberLabels(etcd))
	component := func(name string) *metav1ac.LabelSelectorApplyConfiguration {
		return metav1ac.LabelSelector().WithMatchLabels(map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name, lodiav1alpha1.ComponentLabel: name})
	}
	tcp := func(port int32) *networkingv1ac.NetworkPolicyPortApplyConfiguration {
		return networkingv1ac.NetworkPolicyPort().WithProtocol(corev1.ProtocolTCP).WithPort(intstr.FromInt32(port))
	}
	clientPort := druidv1alpha1.GetClientPort(etcd)
	memberPorts := []*networkingv1ac.NetworkPolicyPortApplyConfiguration{tcp(clientPort), tcp(etcdPeerPort)}
	druidPorts := []*networkingv1ac.NetworkPolicyPortApplyConfiguration{tcp(clientPort)}
	if etcd.IsBackupStoreEnabled() {
		memberPorts = append(memberPorts, tcp(etcdBackupRestorePort))
		druidPorts = append(druidPorts, tcp(etcdBackupRestorePort))
	}
	namespace := func(name string) *metav1ac.LabelSelectorApplyConfiguration {
		return metav1ac.LabelSelector().WithMatchLabels(map[string]string{corev1.LabelMetadataName: name})
	}
	rules := []*networkingv1ac.NetworkPolicyIngressRuleApplyConfiguration{
		networkingv1ac.NetworkPolicyIngressRule().WithFrom(networkingv1ac.NetworkPolicyPeer().WithPodSelector(component(lodiav1alpha1.APIServerComponent))).WithPorts(tcp(clientPort)),
		networkingv1ac.NetworkPolicyIngressRule().WithFrom(networkingv1ac.NetworkPolicyPeer().WithPodSelector(component(lodiav1alpha1.RehearsalComponent))).WithPorts(tcp(clientPort)),
		networkingv1ac.NetworkPolicyIngressRule().WithFrom(networkingv1ac.NetworkPolicyPeer().WithPodSelector(members)).WithPorts(memberPorts...),
		networkingv1ac.NetworkPolicyIngressRule().WithFrom(networkingv1ac.NetworkPolicyPeer().WithNamespaceSelector(namespace(druidNamespace))).WithPorts(druidPorts...),
	}
	if operatorNamespace != druidNamespace {
		rules = append(rules, networkingv1ac.NetworkPolicyIngressRule().WithFrom(networkingv1ac.NetworkPolicyPeer().WithNamespaceSelector(namespace(operatorNamespace))).WithPorts(tcp(clientPort)))
	}
	return networkingv1ac.NetworkPolicy(contract.EtcdName(cp.Name), cp.Namespace).
		WithLabels(map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name}).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(lodiav1alpha1.GroupVersion.String()).
			WithKind("ControlPlane").
			WithName(cp.Name).
			WithUID(cp.UID).
			WithController(true).
			WithBlockOwnerDeletion(true)).
		WithSpec(networkingv1ac.NetworkPolicySpec().
			WithPodSelector(members).
			WithPolicyTypes(networkingv1.PolicyTypeIngress).
			WithIngress(rules...))
}
