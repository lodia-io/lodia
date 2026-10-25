// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package operator assembles the manager cmd/brygga runs, and the tests run
// the same one: its scheme, the cache every controller reads through, and the
// controllers.
package operator

import (
	"fmt"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/controller/datastore"
	"github.com/segla-io/brygga/internal/controller/health"
	"github.com/segla-io/brygga/internal/controller/lifecycle"
	"github.com/segla-io/brygga/internal/controller/pki"
	"github.com/segla-io/brygga/internal/controller/workloads"
)

// workers is the default of controllers.<name>.workers until the operator
// configuration file is read (docs/README.md, Operator configuration).
const workers = 10

// Scheme holds the kinds the controllers read and write.
func Scheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(bryggav1alpha1.AddToScheme(scheme))
	utilruntime.Must(druidv1alpha1.AddToScheme(scheme))
	return scheme
}

// Cache holds Brygga's own kinds whole, and of every other kind only the
// objects labelled with their ControlPlane, without their managed fields.
func Cache() (cache.Options, error) {
	labelled, err := labels.NewRequirement(bryggav1alpha1.ControlPlaneLabel, selection.Exists, nil)
	if err != nil {
		return cache.Options{}, err
	}
	everything := cache.ByObject{Label: labels.Everything()}
	return cache.Options{
		DefaultLabelSelector: labels.NewSelector().Add(*labelled),
		DefaultTransform:     cache.TransformStripManagedFields(),
		ByObject: map[client.Object]cache.ByObject{
			&bryggav1alpha1.ControlPlane{}:      everything,
			&bryggav1alpha1.DatastoreClass{}:    everything,
			&bryggav1alpha1.Datastore{}:         everything,
			&bryggav1alpha1.KubeconfigRequest{}: everything,
			&bryggav1alpha1.Backup{}:            everything,
		},
	}, nil
}

// Setup registers every controller with the manager; signerImage is
// brygga-signer's, which runs beside each API server.
func Setup(mgr ctrl.Manager, signerImage string) error {
	if err := (&pki.Reconciler{
		Client:              mgr.GetClient(),
		Reader:              mgr.GetAPIReader(),
		LeafLifetime:        365 * 24 * time.Hour,
		CALifetime:          5 * 365 * 24 * time.Hour,
		TrustAnchorLifetime: 10 * 365 * 24 * time.Hour,
		CAExpiryNotice:      365 * 24 * time.Hour,
	}).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the pki controller: %w", err)
	}
	if err := (&datastore.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader()}).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the datastore controller: %w", err)
	}
	if err := (&lifecycle.Reconciler{Client: mgr.GetClient()}).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the lifecycle controller: %w", err)
	}
	if err := (&health.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Period: 10 * time.Second, Timeout: 5 * time.Second}).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the health controller: %w", err)
	}
	if err := (&workloads.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), SignerImage: signerImage}).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the workloads controller: %w", err)
	}
	return nil
}
