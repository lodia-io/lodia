// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"fmt"

	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/contract"
	"go.lodia.io/lodia/internal/etcddruid"
	"go.lodia.io/lodia/internal/ownership"
)

// applyNetworkPolicy finds the policy by name, labelled or not, as the Etcd is
// found, never writes over one another owner keeps, and while the pass is
// held writes it only when it is missing. The first result says why the
// policy is not written, the second carries a refusal for the log.
func (r *Reconciler) applyNetworkPolicy(ctx context.Context, cp *lodiav1alpha1.ControlPlane, etcd *etcddruid.Store) (*metav1.Condition, error) {
	key := client.ObjectKey{Namespace: cp.Namespace, Name: contract.EtcdName(cp.Name)}
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
			held := notReady(lodiav1alpha1.ReasonObjectsControlledElsewhere,
				fmt.Sprintf("another owner keeps NetworkPolicy %s by %s %s (UID %s); the Datastore controller writes it once it is gone: the garbage collector deletes a previous ControlPlane's, any other owner's needs a person",
					key.Name, ref.Kind, ref.Name, ref.UID))
			return &held, nil
		}
		if r.held(cp) {
			return nil, nil
		}
	}
	if err := r.Apply(ctx, r.Druid.NetworkPolicy(cp, etcd, r.OperatorNamespace), client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		err = fmt.Errorf("apply NetworkPolicy %s: %w", key.Name, err)
		if apistatus.Refused(err) {
			refused := notReady(lodiav1alpha1.ReasonRefused, "the API server refuses NetworkPolicy "+key.Name+"; the reason is in Lodia's log")
			return &refused, err
		}
		return nil, err
	}
	return nil, nil
}
