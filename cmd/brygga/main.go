// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Command brygga runs the operator: one manager, leader-elected, whose cache
// holds Brygga's own kinds and only the objects it labels.
package main

import (
	"flag"
	"os"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/controller/datastore"
)

// workers is the default of controllers.<name>.workers until the operator
// configuration file is read (docs/README.md, Operator configuration).
const workers = 10

func main() {
	var metricsAddr, probeAddr string
	var leaderElect bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8443", "Address of the metrics endpoint, served over HTTPS behind authentication and authorization.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address of the health and readiness probes.")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Run controllers only in the replica that holds the leader lease.")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(bryggav1alpha1.AddToScheme(scheme))
	utilruntime.Must(druidv1alpha1.AddToScheme(scheme))

	labelled, err := labels.NewRequirement(bryggav1alpha1.ControlPlaneLabel, selection.Exists, nil)
	if err != nil {
		log.Error(err, "build the cache's label selector")
		os.Exit(1)
	}
	everything := cache.ByObject{Label: labels.Everything()}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:    metricsAddr,
			SecureServing:  true,
			FilterProvider: filters.WithAuthenticationAndAuthorization,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "brygga.segla.io",
		Cache: cache.Options{
			DefaultLabelSelector: labels.NewSelector().Add(*labelled),
			DefaultTransform:     cache.TransformStripManagedFields(),
			ByObject: map[client.Object]cache.ByObject{
				&bryggav1alpha1.ControlPlane{}:      everything,
				&bryggav1alpha1.DatastoreClass{}:    everything,
				&bryggav1alpha1.Datastore{}:         everything,
				&bryggav1alpha1.KubeconfigRequest{}: everything,
				&bryggav1alpha1.Backup{}:            everything,
			},
		},
	})
	if err != nil {
		log.Error(err, "create the manager")
		os.Exit(1)
	}
	if err := (&datastore.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader()}).SetupWithManager(mgr, workers); err != nil {
		log.Error(err, "set up the datastore controller")
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "add the health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "add the readiness check")
		os.Exit(1)
	}

	log.Info("starting the manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "run the manager")
		os.Exit(1)
	}
}
