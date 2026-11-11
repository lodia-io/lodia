// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Command lodia runs the operator: one manager, leader-elected, whose cache
// holds Lodia's own kinds and only the objects it labels.
package main

import (
	"flag"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"go.lodia.io/lodia/internal/operator"
)

// version is set by ko from .ko.yaml.
var version string

func main() {
	var metricsAddr, probeAddr, configPath, signerImage, tunnelImage, etcdDruidVersion, etcdDruidNamespace, etcdDruidDeployment, operatorNamespace string
	var leaderElect bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8443", "Address of the metrics endpoint, served over HTTPS behind authentication and authorization.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address of the health and readiness probes.")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Run controllers only in the replica that holds the leader lease.")
	flag.StringVar(&configPath, "config", "", "Path to the operator's configuration file, YAML; without it the defaults apply.")
	flag.StringVar(&signerImage, "signer-image", "", "Image of lodia-signer, the API server's ServiceAccount token signer; required.")
	flag.StringVar(&tunnelImage, "tunnel-image", "", "Image of lodia-tunnel, a gateway tenant's in-cluster path to its API server; required.")
	flag.StringVar(&etcdDruidVersion, "etcd-druid-version", "", "Version of etcd-druid, what follows the repository and its colon in its image, a digest included, which the operator revision covers; required.")
	flag.StringVar(&etcdDruidNamespace, "etcd-druid-namespace", "", "Namespace etcd-druid runs in, whose pods each etcd's NetworkPolicy admits to the backup-restore sidecar; required.")
	flag.StringVar(&etcdDruidDeployment, "etcd-druid-deployment", "", "Name of etcd-druid's Deployment in --etcd-druid-namespace, which must run --etcd-druid-version in every replica before an etcd is handed to it under a new operator revision; required.")
	flag.StringVar(&operatorNamespace, "operator-namespace", "", "Namespace the operator runs in, whose pods each etcd's NetworkPolicy admits to the client port; required.")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")
	if signerImage == "" {
		log.Error(nil, "--signer-image is required")
		os.Exit(1)
	}
	if tunnelImage == "" {
		log.Error(nil, "--tunnel-image is required")
		os.Exit(1)
	}
	if etcdDruidVersion == "" {
		log.Error(nil, "--etcd-druid-version is required")
		os.Exit(1)
	}
	if etcdDruidNamespace == "" {
		log.Error(nil, "--etcd-druid-namespace is required")
		os.Exit(1)
	}
	if etcdDruidDeployment == "" {
		log.Error(nil, "--etcd-druid-deployment is required")
		os.Exit(1)
	}
	if operatorNamespace == "" {
		log.Error(nil, "--operator-namespace is required")
		os.Exit(1)
	}
	config, err := operator.LoadConfig(configPath)
	if err != nil {
		log.Error(err, "load the configuration")
		os.Exit(1)
	}

	cfg := ctrl.GetConfigOrDie()
	gatewayAPI, err := operator.GatewayAPI(cfg)
	if err != nil {
		log.Error(err, "discover the Gateway API")
		os.Exit(1)
	}
	if gatewayAPI {
		log.Info("the management cluster serves TLSRoute and Gateway at gateway.networking.k8s.io/v1")
	} else {
		log.Info("the management cluster serves no TLSRoute or no Gateway at gateway.networking.k8s.io/v1; once it does, restart the operator")
	}
	cache, err := operator.Cache(gatewayAPI)
	if err != nil {
		log.Error(err, "build the cache options")
		os.Exit(1)
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: operator.Scheme(),
		Metrics: metricsserver.Options{
			BindAddress:    metricsAddr,
			SecureServing:  true,
			FilterProvider: filters.WithAuthenticationAndAuthorization,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "lodia.io",
		Cache:                  cache,
	})
	if err != nil {
		log.Error(err, "create the manager")
		os.Exit(1)
	}
	in := operator.Inputs{Config: config, Version: version, SignerImage: signerImage, TunnelImage: tunnelImage, EtcdDruidVersion: etcdDruidVersion, EtcdDruidNamespace: etcdDruidNamespace, EtcdDruidDeployment: etcdDruidDeployment, OperatorNamespace: operatorNamespace, GatewayAPI: gatewayAPI}
	sources, err := operator.Setup(mgr, in)
	if err != nil {
		log.Error(err, "set up the controllers")
		os.Exit(1)
	}
	collector, err := operator.Metrics(mgr.GetCache(), mgr.Elected(), in, sources)
	if err != nil {
		log.Error(err, "build Lodia's metrics")
		os.Exit(1)
	}
	if err := ctrlmetrics.Registry.Register(collector); err != nil {
		log.Error(err, "register Lodia's metrics")
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
