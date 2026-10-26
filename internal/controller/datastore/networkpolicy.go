// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"fmt"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	networkingv1ac "k8s.io/client-go/applyconfigurations/networking/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/ownership"
)

// etcd-druid's defaults for the ports Lodia leaves unset in the Etcd
// (etcd-druid@v0.39.0 internal/common/constants.go#L55-L60). The policy names
// them by number: the peer port and the sidecar's share the container port
// name "server" (internal/component/statefulset/constants.go#L12-L15).
const (
	etcdPeerPort          int32 = 2380
	etcdBackupRestorePort int32 = 8080
)

// networkPolicy admits to the members of the Etcd what dials them
// (docs/README.md, A tenant namespace): the API server's pods to the client
// port; the members to the client port, which their sidecars dial through the
// client Service, and to the peer port; and, with a backup store, etcd-druid's
// pods to the sidecar's port, for the snapshots it asks for.
func networkPolicy(cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd, druidNamespace string) *networkingv1ac.NetworkPolicyApplyConfiguration {
	members := metav1ac.LabelSelector().
		WithMatchLabels(druidv1alpha1.GetDefaultLabels(etcd.ObjectMeta)).
		WithMatchLabels(map[string]string{druidv1alpha1.LabelAppNameKey: etcd.Name})
	apiServer := metav1ac.LabelSelector().WithMatchLabels(map[string]string{
		lodiav1alpha1.ControlPlaneLabel: cp.Name,
		lodiav1alpha1.ComponentLabel:    lodiav1alpha1.APIServerComponent,
	})
	tcp := func(port int32) *networkingv1ac.NetworkPolicyPortApplyConfiguration {
		return networkingv1ac.NetworkPolicyPort().WithProtocol(corev1.ProtocolTCP).WithPort(intstr.FromInt32(port))
	}
	clientPort := druidv1alpha1.GetClientPort(etcd)
	rules := []*networkingv1ac.NetworkPolicyIngressRuleApplyConfiguration{
		networkingv1ac.NetworkPolicyIngressRule().WithFrom(networkingv1ac.NetworkPolicyPeer().WithPodSelector(apiServer)).WithPorts(tcp(clientPort)),
		networkingv1ac.NetworkPolicyIngressRule().WithFrom(networkingv1ac.NetworkPolicyPeer().WithPodSelector(members)).WithPorts(tcp(clientPort), tcp(etcdPeerPort)),
	}
	if etcd.IsBackupStoreEnabled() {
		druid := metav1ac.LabelSelector().WithMatchLabels(map[string]string{corev1.LabelMetadataName: druidNamespace})
		rules = append(rules, networkingv1ac.NetworkPolicyIngressRule().WithFrom(networkingv1ac.NetworkPolicyPeer().WithNamespaceSelector(druid)).WithPorts(tcp(etcdBackupRestorePort)))
	}
	return networkingv1ac.NetworkPolicy(lodiav1alpha1.EtcdName(cp.Name), cp.Namespace).
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

// applyNetworkPolicy finds the policy by name, labelled or not, as the Etcd is
// found, and never writes over one another owner keeps. The first result says
// why the policy is not written, the second carries a refusal for the log.
func (r *Reconciler) applyNetworkPolicy(ctx context.Context, cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd) (*metav1.Condition, error) {
	key := client.ObjectKey{Namespace: cp.Namespace, Name: lodiav1alpha1.EtcdName(cp.Name)}
	current := &networkingv1.NetworkPolicy{}
	err := r.Get(ctx, key, current)
	if apierrors.IsNotFound(err) {
		err = r.Reader.Get(ctx, key, current)
	}
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return nil, err
	default:
		if ref := ownership.ControlledElsewhere(current, cp); ref != nil {
			held := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere,
				fmt.Sprintf("another owner keeps NetworkPolicy %s by %s %s (UID %s); the Datastore controller writes it once it is gone: the garbage collector deletes a previous ControlPlane's, any other owner's needs a person",
					key.Name, ref.Kind, ref.Name, ref.UID))
			return &held, nil
		}
	}
	if err := r.Apply(ctx, networkPolicy(cp, etcd, r.EtcdDruidNamespace), client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		err = fmt.Errorf("apply NetworkPolicy %s: %w", key.Name, err)
		if apistatus.Refused(err) {
			refused := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, "the API server refuses NetworkPolicy "+key.Name+"; the reason is in Lodia's log")
			return &refused, err
		}
		return nil, err
	}
	return nil, nil
}
