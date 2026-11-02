// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package restore holds what more than one controller reads to restore a
// ControlPlane and to release it (docs/decisions/restore-replaces-the-source.md).
package restore

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	if cp.Status.Restore == nil {
		return nil, nil
	}
	list := &lodiav1alpha1.ControlPlaneList{}
	if err := reader.List(ctx, list, client.InNamespace(cp.Namespace)); err != nil {
		return nil, err
	}
	for i := range list.Items {
		if list.Items[i].UID == cp.Status.Restore.Source.UID {
			return &list.Items[i], nil
		}
	}
	return nil, nil
}

// Final says whether a Backup's Ready says the backup reconciler is done with
// it: a ControlPlane gone once the Backup started counts, one that never
// existed is awaited.
func Final(backup *lodiav1alpha1.Backup, ready *metav1.Condition) bool {
	if ready == nil || ready.Status != metav1.ConditionFalse {
		return false
	}
	switch ready.Reason {
	case lodiav1alpha1.ReasonUnsupported, lodiav1alpha1.ReasonNoBackupStore, lodiav1alpha1.ReasonFailed:
		return true
	case lodiav1alpha1.ReasonControlPlaneNotFound:
		return backup.Status.StartedAt != nil
	}
	return false
}

// Failed says whether a Backup's Ready is one no restore waits out, so that
// Datastore and PKI both tell a person to restore another Backup: Final, a
// seal lost, or an object the Backup writes that another controls.
func Failed(backup *lodiav1alpha1.Backup, ready *metav1.Condition) bool {
	if Final(backup, ready) {
		return true
	}
	return ready != nil && ready.Status == metav1.ConditionFalse &&
		(ready.Reason == lodiav1alpha1.ReasonSealLost || ready.Reason == lodiav1alpha1.ReasonObjectsControlledElsewhere)
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
