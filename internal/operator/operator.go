// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package operator assembles the manager cmd/lodia runs, and the tests run
// the same one: its scheme, the cache every controller reads through, and the
// controllers.
package operator

import (
	"context"
	"fmt"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/controller/health"
	"go.lodia.io/lodia/internal/controller/lifecycle"
	"go.lodia.io/lodia/internal/controller/pki"
	"go.lodia.io/lodia/internal/controller/tenant"
	"go.lodia.io/lodia/internal/controller/workloads"
)

// workers is the default of controllers.<name>.workers, a key the operator
// does not read yet (docs/README.md, Operator configuration).
const workers = 10

// Scheme holds the kinds the controllers read and write.
func Scheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(lodiav1alpha1.AddToScheme(scheme))
	utilruntime.Must(druidv1alpha1.AddToScheme(scheme))
	utilruntime.Must(gatewayv1.Install(scheme))
	return scheme
}

// GatewayAPI says whether the management cluster serves TLSRoute and Gateway
// at gateway.networking.k8s.io/v1. Only a kind that does not match means
// they are not served; any other error is returned, since a blip of the API
// server would otherwise leave the operator without them until it restarts.
func GatewayAPI(cfg *rest.Config) (bool, error) {
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return false, err
	}
	mapper, err := apiutil.NewDynamicRESTMapper(cfg, httpClient)
	if err != nil {
		return false, err
	}
	for _, kind := range []string{"TLSRoute", "Gateway"} {
		_, err := mapper.RESTMapping(schema.GroupKind{Group: gatewayv1.GroupName, Kind: kind}, gatewayv1.GroupVersion.Version)
		if meta.IsNoMatchError(err) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("discover %s: %w", kind, err)
		}
	}
	return true, nil
}

// Cache holds Lodia's own kinds whole, TLSRoutes and Gateways whole where
// gatewayAPI says they are served, and of every other kind only the objects
// labelled with their ControlPlane, without their managed fields. The cache
// maps every kind it holds when it is built, and fails on one not served.
func Cache(gatewayAPI bool) (cache.Options, error) {
	labelled, err := labels.NewRequirement(lodiav1alpha1.ControlPlaneLabel, selection.Exists, nil)
	if err != nil {
		return cache.Options{}, err
	}
	everything := cache.ByObject{Label: labels.Everything()}
	byObject := map[client.Object]cache.ByObject{
		&lodiav1alpha1.ControlPlane{}:      everything,
		&lodiav1alpha1.DatastoreClass{}:    everything,
		&lodiav1alpha1.Datastore{}:         everything,
		&lodiav1alpha1.KubeconfigRequest{}: everything,
		&lodiav1alpha1.Backup{}:            everything,
	}
	if gatewayAPI {
		byObject[&gatewayv1.TLSRoute{}] = everything
		byObject[&gatewayv1.Gateway{}] = everything
	}
	return cache.Options{
		DefaultLabelSelector: labels.NewSelector().Add(*labelled),
		DefaultTransform:     cache.TransformStripManagedFields(),
		ByObject:             byObject,
	}, nil
}

// Setup registers every controller with the manager; cfg is what LoadConfig
// returned, signerImage is lodia-signer's, which runs beside each API server,
// tunnelImage lodia-tunnel's, which runs on each node of a gateway tenant,
// etcdDruidNamespace etcd-druid's and operatorNamespace the operator's, which
// each etcd's NetworkPolicy admits, and gatewayAPI what GatewayAPI found.
func Setup(mgr ctrl.Manager, cfg Config, signerImage, tunnelImage, etcdDruidNamespace, operatorNamespace string, gatewayAPI bool) error {
	if err := (&pki.Reconciler{
		Client:              mgr.GetClient(),
		Reader:              mgr.GetAPIReader(),
		LeafLifetime:        365 * 24 * time.Hour,
		CALifetime:          5 * 365 * 24 * time.Hour,
		TrustAnchorLifetime: 10 * 365 * 24 * time.Hour,
		CAExpiryNotice:      365 * 24 * time.Hour,
		SignerPropagation:   5 * time.Minute,
		Clock:               clock.RealClock{},
	}).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the pki controller: %w", err)
	}
	if err := (&pki.KubeconfigRequestReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Clock: clock.RealClock{}, PolicyGrace: 10 * time.Second}).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the kubeconfig request reconciler: %w", err)
	}
	dialEtcd := func(ctx context.Context, etcd *druidv1alpha1.Etcd) (*clientv3.Client, error) {
		return datastore.DialEtcd(ctx, mgr.GetAPIReader(), etcd)
	}
	if err := (&datastore.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), EtcdDruidNamespace: etcdDruidNamespace, OperatorNamespace: operatorNamespace, EtcdClient: dialEtcd, Clock: clock.RealClock{}}).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the datastore controller: %w", err)
	}
	if err := (&datastore.BackupReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), EtcdClient: dialEtcd, Clock: clock.RealClock{}}).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the backup reconciler: %w", err)
	}
	if err := (&lifecycle.Reconciler{Client: mgr.GetClient()}).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the lifecycle controller: %w", err)
	}
	if err := (&tenant.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), ApplyPeriod: 10 * time.Minute, TunnelImage: tunnelImage}).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the tenant controller: %w", err)
	}
	if err := newHealth(mgr.GetClient(), mgr.GetAPIReader(), cfg.Health).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the health controller: %w", err)
	}
	if err := (&workloads.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), SignerImage: signerImage, TunnelImage: tunnelImage, GatewayAPI: gatewayAPI}).SetupWithManager(mgr, workers); err != nil {
		return fmt.Errorf("set up the workloads controller: %w", err)
	}
	return nil
}

func newHealth(c client.Client, reader client.Reader, cfg HealthConfig) *health.Reconciler {
	return &health.Reconciler{Client: c, Reader: reader, Period: cfg.Period.Duration, FailuresBeforeFalse: cfg.FailuresBeforeFalse, Timeout: 5 * time.Second, Clock: clock.RealClock{}}
}
