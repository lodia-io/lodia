// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Command brygga runs the operator: one manager, leader-elected, whose cache
// holds Brygga's own kinds and only the objects it labels.
package main

import (
	"flag"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/segla-io/brygga/internal/operator"
)

func main() {
	var metricsAddr, probeAddr, signerImage string
	var leaderElect bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8443", "Address of the metrics endpoint, served over HTTPS behind authentication and authorization.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address of the health and readiness probes.")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Run controllers only in the replica that holds the leader lease.")
	flag.StringVar(&signerImage, "signer-image", "", "Image of brygga-signer, the API server's ServiceAccount token signer; required.")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")
	if signerImage == "" {
		log.Error(nil, "--signer-image is required")
		os.Exit(1)
	}

	cache, err := operator.Cache()
	if err != nil {
		log.Error(err, "build the cache options")
		os.Exit(1)
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: operator.Scheme(),
		Metrics: metricsserver.Options{
			BindAddress:    metricsAddr,
			SecureServing:  true,
			FilterProvider: filters.WithAuthenticationAndAuthorization,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "brygga.segla.io",
		Cache:                  cache,
	})
	if err != nil {
		log.Error(err, "create the manager")
		os.Exit(1)
	}
	if err := operator.Setup(mgr, signerImage); err != nil {
		log.Error(err, "set up the controllers")
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
