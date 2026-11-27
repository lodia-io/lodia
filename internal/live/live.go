// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package live reads a writer's own conditions, Datastore's status.datastore,
// status.restore and status.memberRestore with status.components, which it
// ends MemberRestored on, Workloads' status.components and status.endpoint
// with status.memberRestore, which it renders from, the
// rehearsal's status.rehearsal, Tenant's status.addons with status.components,
// which it renders from, PKI's
// status.credentials and Health's status.nodes and status.health,
// and for Lifecycle's decisions the whole object, as the API server holds
// them, for the waits and keeps docs/decisions/independent-controllers.md
// names. A read fails when the API
// server holds another object under the name, as when the cache still holds a
// deleted predecessor, so that the pass is retried once the cache has caught
// up.
package live

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// Object returns the whole of cp as the API server holds it, read through
// reader, which must not be a cache.
func Object(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane) (*lodiav1alpha1.ControlPlane, error) {
	return conditions(ctx, reader, cp, nil)
}

// Conditions replaces cp's conditions of the given types with the API
// server's, read through reader, which must not be a cache.
func Conditions(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane, conditionTypes ...string) error {
	_, err := conditions(ctx, reader, cp, conditionTypes)
	return err
}

// Datastore replaces cp's status.datastore, status.restore,
// status.memberRestore, status.components and its conditions of the given
// types with the API server's, read through reader, which must not be a
// cache, and returns the resourceVersion of that read, which it leaves out of
// cp: cp's own stays the cached copy's.
func Datastore(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane, conditionTypes ...string) (string, error) {
	current, err := conditions(ctx, reader, cp, conditionTypes)
	if err != nil {
		return "", err
	}
	cp.Status.Datastore, cp.Status.Restore, cp.Status.MemberRestore, cp.Status.Components = current.Status.Datastore, current.Status.Restore, current.Status.MemberRestore, current.Status.Components
	return current.ResourceVersion, nil
}

// Components replaces cp's status.components, status.endpoint,
// status.memberRestore and its conditions of the given types with the API
// server's, read through reader, which must not be a cache.
func Components(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane, conditionTypes ...string) error {
	current, err := conditions(ctx, reader, cp, conditionTypes)
	if err != nil {
		return err
	}
	cp.Status.Components, cp.Status.Endpoint, cp.Status.MemberRestore = current.Status.Components, current.Status.Endpoint, current.Status.MemberRestore
	return nil
}

// Rehearsal replaces cp's status.rehearsal and its conditions of the given
// types with the API server's, read through reader, which must not be a
// cache.
func Rehearsal(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane, conditionTypes ...string) error {
	current, err := conditions(ctx, reader, cp, conditionTypes)
	if err != nil {
		return err
	}
	cp.Status.Rehearsal = current.Status.Rehearsal
	return nil
}

// Addons replaces cp's status.addons, status.components and its conditions of
// the given types with the API server's, read through reader, which must not
// be a cache.
func Addons(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane, conditionTypes ...string) error {
	current, err := conditions(ctx, reader, cp, conditionTypes)
	if err != nil {
		return err
	}
	cp.Status.Addons, cp.Status.Components = current.Status.Addons, current.Status.Components
	return nil
}

// Credentials replaces cp's status.credentials and its conditions of the given
// types with the API server's, read through reader, which must not be a cache.
func Credentials(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane, conditionTypes ...string) error {
	current, err := conditions(ctx, reader, cp, conditionTypes)
	if err != nil {
		return err
	}
	cp.Status.Credentials = current.Status.Credentials
	return nil
}

// Nodes replaces cp's status.nodes, status.health and its conditions of the
// given types with the API server's, read through reader, which must not be a
// cache.
func Nodes(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane, conditionTypes ...string) error {
	current, err := conditions(ctx, reader, cp, conditionTypes)
	if err != nil {
		return err
	}
	cp.Status.Nodes, cp.Status.Health = current.Status.Nodes, current.Status.Health
	return nil
}

func conditions(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane, conditionTypes []string) (*lodiav1alpha1.ControlPlane, error) {
	current := &lodiav1alpha1.ControlPlane{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(cp), current); err != nil {
		return nil, err
	}
	if current.UID != cp.UID {
		return nil, fmt.Errorf("ControlPlane %s/%s has UID %s, not %s", cp.Namespace, cp.Name, current.UID, cp.UID)
	}
	for _, conditionType := range conditionTypes {
		meta.RemoveStatusCondition(&cp.Status.Conditions, conditionType)
		if c := meta.FindStatusCondition(current.Status.Conditions, conditionType); c != nil {
			cp.Status.Conditions = append(cp.Status.Conditions, *c)
		}
	}
	return current, nil
}
