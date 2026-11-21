// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package etcddruid runs a ControlPlane's etcd through etcd-druid, and is the
// only package of Lodia's production code that names etcd-druid
// (docs/decisions/etcd-druid-behind-a-seam.md). Its callers pass the reader,
// the client and the clock each call uses, and get Lodia's values back.
package etcddruid

import (
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// Engine is etcd-druid as the operator was given it, and what the operator
// saw of its reconciles.
type Engine struct {
	// Namespace is where etcd-druid runs; the etcd's NetworkPolicy admits its
	// pods to the backup-restore sidecar.
	Namespace string
	// Deployment is etcd-druid's Deployment in Namespace, which must run
	// Version in every replica before an Etcd is handed to it under a new
	// operator revision.
	Deployment string
	Version    string
	// StatusSync is etcd-druid's etcdStatusSyncPeriod, how often it syncs an
	// Etcd's status.
	StatusSync time.Duration
	// sightings is, per Etcd, when this operator first saw its run Processing.
	sightings map[client.ObjectKey]sighting
}

// AddToScheme adds the kinds the engine reads and writes.
func AddToScheme(scheme *runtime.Scheme) error {
	return druidv1alpha1.AddToScheme(scheme)
}

// StoreObject is the object of a ControlPlane's etcd, which a reconciler owns,
// and StoreChanged the predicate of the updates of it that reach the
// reconciler.
func (e *Engine) StoreObject() client.Object { return &druidv1alpha1.Etcd{} }

func (e *Engine) StoreChanged() predicate.Funcs { return etcdChanged() }

// SnapshotTaskObject and CopyTaskObject are the objects of the tasks a
// reconciler creates: a full snapshot, and a copy of snapshots.
func (e *Engine) SnapshotTaskObject() client.Object { return &druidv1alpha1.EtcdOpsTask{} }

func (e *Engine) CopyTaskObject() client.Object { return &druidv1alpha1.EtcdCopyBackupsTask{} }
