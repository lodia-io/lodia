// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package operator assembles the manager cmd/lodia runs, and the tests run
// the same one: its scheme, the cache every controller reads through, and the
// controllers.
package operator

import (
	"context"
	"fmt"
	"net"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/features"
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
	"go.lodia.io/lodia/internal/etcddruid"
	"go.lodia.io/lodia/internal/fence"
	"go.lodia.io/lodia/internal/metrics"
	"go.lodia.io/lodia/internal/secretcache"
)

const healthTimeout = 5 * time.Second

// Scheme holds the kinds the controllers read and write.
func Scheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(lodiav1alpha1.AddToScheme(scheme))
	utilruntime.Must(etcddruid.AddToScheme(scheme))
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
// gatewayAPI says they are served, the Pods labelled as rehearsals' copies alone,
// and of every other kind only the objects labelled with their ControlPlane,
// without their managed fields. The cache
// maps every kind it holds when it is built, and fails on one not served.
func Cache(gatewayAPI bool) (cache.Options, error) {
	labelled, err := labels.NewRequirement(lodiav1alpha1.ControlPlaneLabel, selection.Exists, nil)
	if err != nil {
		return cache.Options{}, err
	}
	rehearsals, err := labels.NewRequirement(lodiav1alpha1.ComponentLabel, selection.Equals, []string{lodiav1alpha1.RehearsalComponent})
	if err != nil {
		return cache.Options{}, err
	}
	everything := cache.ByObject{Label: labels.Everything()}
	byObject := map[client.Object]cache.ByObject{
		&corev1.Pod{}:                      {Label: labels.NewSelector().Add(*labelled, *rehearsals)},
		&lodiav1alpha1.ControlPlane{}:      everything,
		&lodiav1alpha1.DatastoreClass{}:    everything,
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

// Inputs are what Setup registers the controllers with: Config is what
// LoadConfig returned, Version the operator's build version, SignerImage
// lodia-signer's, which runs beside each API server, TunnelImage
// lodia-tunnel's, which runs on each node of a gateway tenant,
// EtcdDruidVersion, EtcdDruidNamespace, EtcdDruidDeployment and
// EtcdDruidStatusSync etcd-druid's,
// OperatorNamespace the operator's, which each etcd's NetworkPolicy admits,
// and GatewayAPI what GatewayAPI found. Dial, when set, opens every
// connection PKI, Tenant, Health and the rehearsal make to a tenant, whose
// names nothing outside a cluster resolves; it is nil in production.
type Inputs struct {
	Config              Config
	Version             string
	SignerImage         string
	TunnelImage         string
	EtcdDruidVersion    string
	EtcdDruidNamespace  string
	EtcdDruidDeployment string
	EtcdDruidStatusSync time.Duration
	OperatorNamespace   string
	GatewayAPI          bool
	Dial                func(ctx context.Context, network, address string) (net.Conn, error)
}

// Sources are the controllers Setup registers whose memory Lodia's metrics
// serve: Health, with its counter of failed probes, health-nodes, with its
// histogram of lag and its counts of the nodes, and the etcd sampler, with its
// samples, and the engine the collector reads the etcds through. No registry
// holds the counter or the histogram.
type Sources struct {
	Health *health.Reconciler
	Nodes  *health.NodesReconciler
	Etcd   *datastore.SampleReconciler
	Druid  *etcddruid.Engine
}

type controllers struct {
	pki       *pki.Reconciler
	requests  *pki.KubeconfigRequestReconciler
	datastore *datastore.Reconciler
	backup    *datastore.BackupReconciler
	sampler   *datastore.SampleReconciler
	lifecycle *lifecycle.Reconciler
	tenant    *tenant.Reconciler
	health    *health.Reconciler
	nodes     *health.NodesReconciler
	workloads *workloads.Reconciler
	rehearsal *workloads.RehearsalReconciler
}

func newControllers(mgr ctrl.Manager, in Inputs, revision string) *controllers {
	secrets := secretcache.New(mgr.GetCache(), mgr.GetAPIReader())
	druid := &etcddruid.Engine{Namespace: in.EtcdDruidNamespace, Deployment: in.EtcdDruidDeployment, Version: in.EtcdDruidVersion, StatusSync: in.EtcdDruidStatusSync}
	dialEtcd := func(ctx context.Context, etcd *etcddruid.Store) (*clientv3.Client, error) {
		return druid.Dial(ctx, secrets, etcd)
	}
	c := &controllers{
		pki: &pki.Reconciler{
			Client:              mgr.GetClient(),
			Reader:              mgr.GetAPIReader(),
			LeafLifetime:        365 * 24 * time.Hour,
			CALifetime:          5 * 365 * 24 * time.Hour,
			TrustAnchorLifetime: 10 * 365 * 24 * time.Hour,
			CAExpiryNotice:      365 * 24 * time.Hour,
			SignerPropagation:   5 * time.Minute,
			Clock:               clock.RealClock{},
			Dial:                in.Dial,
		},
		requests: &pki.KubeconfigRequestReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Clock: clock.RealClock{}, PolicyGrace: 10 * time.Second},
		datastore: &datastore.Reconciler{
			Client:            mgr.GetClient(),
			Reader:            mgr.GetAPIReader(),
			Druid:             druid,
			Revision:          revision,
			OperatorNamespace: in.OperatorNamespace,
			EtcdClient:        dialEtcd,
			Clock:             clock.RealClock{},
		},
		backup:    &datastore.BackupReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Druid: druid, EtcdClient: dialEtcd, Clock: clock.RealClock{}},
		sampler:   &datastore.SampleReconciler{Cache: mgr.GetCache(), Druid: druid, EtcdClient: dialEtcd, Clock: clock.RealClock{}},
		lifecycle: newLifecycle(mgr.GetClient(), mgr.GetAPIReader(), in.Config, in.EtcdDruidStatusSync, revision),
		tenant:    &tenant.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Secrets: secrets, ApplyPeriod: 10 * time.Minute, TunnelImage: in.TunnelImage, Revision: revision, Dial: in.Dial},
		health:    newHealth(mgr.GetClient(), mgr.GetAPIReader(), secrets, in.Config.Health),
		nodes:     newNodes(mgr.GetClient(), mgr.GetAPIReader(), secrets, in.Config.Health),
		workloads: &workloads.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), SignerImage: in.SignerImage, TunnelImage: in.TunnelImage, GatewayAPI: in.GatewayAPI, Revision: revision},
		rehearsal: newRehearsal(mgr.GetClient(), mgr.GetAPIReader(), secrets, in),
	}
	c.health.DialContext, c.nodes.DialContext = in.Dial, in.Dial
	return c
}

// Setup registers every controller with the manager and returns the Sources
// Metrics serves. It refuses while client-go's informers may apply a kind's
// changes out of order, which the fences of Workloads and Lifecycle rely on.
func Setup(mgr ctrl.Manager, in Inputs) (*Sources, error) {
	if err := fence.Ordered(features.FeatureGates()); err != nil {
		return nil, err
	}
	revision, err := in.Revision()
	if err != nil {
		return nil, err
	}
	workers := in.Config.Controllers
	c := newControllers(mgr, in, revision)
	if err := c.pki.SetupWithManager(mgr, workers.PKI.Workers); err != nil {
		return nil, fmt.Errorf("set up the pki controller: %w", err)
	}
	if err := c.requests.SetupWithManager(mgr, workers.KubeconfigRequest.Workers); err != nil {
		return nil, fmt.Errorf("set up the kubeconfig request reconciler: %w", err)
	}
	if err := c.datastore.SetupWithManager(mgr, workers.Datastore.Workers); err != nil {
		return nil, fmt.Errorf("set up the datastore controller: %w", err)
	}
	if err := c.backup.SetupWithManager(mgr, workers.Backup.Workers); err != nil {
		return nil, fmt.Errorf("set up the backup reconciler: %w", err)
	}
	if err := c.sampler.SetupWithManager(mgr, workers.DatastoreSample.Workers); err != nil {
		return nil, fmt.Errorf("set up the etcd sampler: %w", err)
	}
	if err := c.lifecycle.SetupWithManager(mgr, workers.Lifecycle.Workers); err != nil {
		return nil, fmt.Errorf("set up the lifecycle controller: %w", err)
	}
	if err := c.tenant.SetupWithManager(mgr, workers.Tenant.Workers); err != nil {
		return nil, fmt.Errorf("set up the tenant controller: %w", err)
	}
	if err := c.health.SetupWithManager(mgr, workers.Health.Workers); err != nil {
		return nil, fmt.Errorf("set up the health controller: %w", err)
	}
	if err := c.nodes.SetupWithManager(mgr, workers.HealthNodes.Workers); err != nil {
		return nil, fmt.Errorf("set up the health nodes reconciler: %w", err)
	}
	if err := c.workloads.SetupWithManager(mgr, workers.Workloads.Workers); err != nil {
		return nil, fmt.Errorf("set up the workloads controller: %w", err)
	}
	if err := c.rehearsal.SetupWithManager(mgr, workers.Rehearsal.Workers); err != nil {
		return nil, fmt.Errorf("set up the rehearsal reconciler: %w", err)
	}
	return &Sources{Health: c.health, Nodes: c.nodes, Etcd: c.sampler, Druid: c.sampler.Druid}, nil
}

// Metrics is Lodia's collector over a manager's cache and its Elected, and
// over the Sources Setup returned. Setup does not register it, since
// controller-runtime's registry is global and tests run Setup for many
// managers in one process: cmd/lodia does.
func Metrics(reader client.Reader, elected <-chan struct{}, in Inputs, sources *Sources) (*metrics.Collector, error) {
	revision, err := in.Revision()
	if err != nil {
		return nil, err
	}
	return &metrics.Collector{
		Reader: reader, Druid: sources.Druid, Elected: elected, Revision: revision, MaxConcurrentRollouts: in.Config.Fleet.MaxConcurrentRollouts, HoldFraction: sources.Nodes.HoldFraction,
		EtcdSample: sources.Etcd.Sample, NodeCounts: sources.Nodes.Counts, SampleLag: sources.Nodes.SampleLag, ProbeFailures: sources.Health.ProbeFailures,
	}, nil
}

// newLifecycle settles a change Lodia started for as long as Health takes to
// turn Reachable False and etcd-druid to sync a member's loss into
// DatastoreReady (docs/decisions/upgrades-and-maintenance-windows.md).
func newLifecycle(c client.Client, reader client.Reader, cfg Config, statusSync time.Duration, revision string) *lifecycle.Reconciler {
	return &lifecycle.Reconciler{
		Client: c, Reader: reader, Clock: clock.RealClock{}, Revision: revision,
		MaxConcurrentRollouts:   cfg.Fleet.MaxConcurrentRollouts,
		MaxConcurrentRehearsals: cfg.Fleet.MaxConcurrentRehearsals,
		Settling:                time.Duration(cfg.Health.FailuresBeforeFalse)*(cfg.Health.Period.Duration+healthTimeout) + statusSync,
	}
}

func newRehearsal(c client.Client, reader, secrets client.Reader, in Inputs) *workloads.RehearsalReconciler {
	return &workloads.RehearsalReconciler{
		Client: c, Reader: reader, Secrets: secrets, Clock: clock.RealClock{}, SignerImage: in.SignerImage,
		OperatorNamespace: in.OperatorNamespace, MaxConcurrent: in.Config.Fleet.MaxConcurrentRehearsals, Timeout: healthTimeout, Dial: in.Dial,
	}
}

func newHealth(c client.Client, reader, secrets client.Reader, cfg HealthConfig) *health.Reconciler {
	return &health.Reconciler{Client: c, Reader: reader, Secrets: secrets, Period: cfg.Period.Duration, FailuresBeforeFalse: cfg.FailuresBeforeFalse, Timeout: healthTimeout, Clock: clock.RealClock{}, ProbeFailures: metrics.NewProbeFailures()}
}

func newNodes(c client.Client, reader, secrets client.Reader, cfg HealthConfig) *health.NodesReconciler {
	return &health.NodesReconciler{
		Client: c, Reader: reader, Secrets: secrets, Period: cfg.Period.Duration, Timeout: healthTimeout,
		SilentAfter: cfg.SilentAfter.Duration, HoldFraction: int(cfg.HoldFraction), ReleaseAfter: cfg.ReleaseAfter.Duration, MaxNodes: cfg.MaxNodes, Clock: clock.RealClock{},
		SampleLag: metrics.NewSampleLag(),
	}
}
