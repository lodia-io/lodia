// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package live reads a writer's own conditions as the API server holds them,
// for the waits and keeps docs/decisions/independent-controllers.md names.
package live

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// Conditions replaces cp's conditions of the given types with the API
// server's, read through reader, which must not be a cache.
func Conditions(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane, conditionTypes ...string) error {
	current := &lodiav1alpha1.ControlPlane{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(cp), current); err != nil {
		return err
	}
	for _, conditionType := range conditionTypes {
		meta.RemoveStatusCondition(&cp.Status.Conditions, conditionType)
		if c := meta.FindStatusCondition(current.Status.Conditions, conditionType); c != nil {
			cp.Status.Conditions = append(cp.Status.Conditions, *c)
		}
	}
	return nil
}
