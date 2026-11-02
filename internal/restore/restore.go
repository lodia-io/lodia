// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package restore holds what Workloads and Health both read to release a
// restored ControlPlane (docs/decisions/restore-replaces-the-source.md).
package restore

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

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
