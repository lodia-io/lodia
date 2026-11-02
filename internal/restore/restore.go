// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package restore holds what more than one controller reads to restore a
// ControlPlane and to release it (docs/decisions/restore-replaces-the-source.md).
package restore

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// Source returns the ControlPlane in cp's namespace, being deleted or not,
// with the UID of the source cp's completed restore replaces; nil while the
// restore is not complete or once no such ControlPlane exists.
func Source(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane) (*lodiav1alpha1.ControlPlane, error) {
	if cp.Status.Datastore == nil || cp.Status.Datastore.Restore == nil {
		return nil, nil
	}
	list := &lodiav1alpha1.ControlPlaneList{}
	if err := reader.List(ctx, list, client.InNamespace(cp.Namespace)); err != nil {
		return nil, err
	}
	for i := range list.Items {
		if list.Items[i].UID == cp.Status.Datastore.Restore.Source.UID {
			return &list.Items[i], nil
		}
	}
	return nil, nil
}

// RestoresOf maps a Backup to the ControlPlanes of its namespace that name it
// in restoreFrom.
func RestoresOf(reader client.Reader) handler.MapFunc {
	return func(ctx context.Context, backup client.Object) []reconcile.Request {
		list := &lodiav1alpha1.ControlPlaneList{}
		if err := reader.List(ctx, list, client.InNamespace(backup.GetNamespace())); err != nil {
			logf.FromContext(ctx).Error(err, "list the ControlPlanes restored from a Backup", "backup", backup.GetName())
			return nil
		}
		var requests []reconcile.Request
		for i := range list.Items {
			if from := list.Items[i].Spec.RestoreFrom; from != nil && from.BackupRef.Name == backup.GetName() {
				requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&list.Items[i])})
			}
		}
		return requests
	}
}
