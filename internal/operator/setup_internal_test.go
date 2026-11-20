// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package operator

import (
	"context"
	"crypto/x509"
	"sync/atomic"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
	"go.lodia.io/lodia/internal/secretcache"
)

func TestHealthRunsOnTheFilesValues(t *testing.T) {
	cfg := DefaultConfig().Health
	cfg.Period = metav1.Duration{Duration: 7 * time.Second}
	cfg.FailuresBeforeFalse = 4
	got := newHealth(nil, nil, nil, cfg)
	if got.Period != 7*time.Second {
		t.Errorf("Period = %s, want 7s", got.Period)
	}
	if got.FailuresBeforeFalse != 4 {
		t.Errorf("FailuresBeforeFalse = %d, want 4", got.FailuresBeforeFalse)
	}
}

func TestHealthNodesRunOnTheFilesValues(t *testing.T) {
	cfg := DefaultConfig().Health
	cfg.Period = metav1.Duration{Duration: 7 * time.Second}
	cfg.SilentAfter = metav1.Duration{Duration: 25 * time.Second}
	cfg.HoldFraction = 750
	cfg.MaxNodes = 120
	got := newNodes(nil, nil, nil, cfg)
	if got.Period != 7*time.Second {
		t.Errorf("Period = %s, want 7s", got.Period)
	}
	if got.SilentAfter != 25*time.Second {
		t.Errorf("SilentAfter = %s, want 25s", got.SilentAfter)
	}
	if got.HoldFraction != 750 {
		t.Errorf("HoldFraction = %d, want 750", got.HoldFraction)
	}
	if got.MaxNodes != 120 {
		t.Errorf("MaxNodes = %d, want 120", got.MaxNodes)
	}
	if want := newHealth(nil, nil, nil, cfg).Timeout; got.Timeout != want {
		t.Errorf("Timeout = %s, want Health's %s", got.Timeout, want)
	}
}

func TestLifecycleRunsOnTheFilesValues(t *testing.T) {
	if got := newLifecycle(nil, nil, DefaultConfig(), "v1.0.0+0"); got.Settling != 45*time.Second || got.MaxConcurrentRollouts != 5 {
		t.Errorf("at the defaults Settling = %s and MaxConcurrentRollouts = %d, want 45s and 5", got.Settling, got.MaxConcurrentRollouts)
	}
	cfg := DefaultConfig()
	cfg.Health.Period = metav1.Duration{Duration: 7 * time.Second}
	cfg.Health.FailuresBeforeFalse = 4
	cfg.Fleet.MaxConcurrentRollouts = 2
	cfg.Fleet.MaxConcurrentRehearsals = 0
	got := newLifecycle(nil, nil, cfg, "v1.0.0+0")
	if want := 4*(7*time.Second+newHealth(nil, nil, nil, cfg.Health).Timeout) + 15*time.Second; got.Settling != want {
		t.Errorf("Settling = %s, want %s", got.Settling, want)
	}
	if got.MaxConcurrentRollouts != 2 || got.MaxConcurrentRehearsals != 0 || got.Revision != "v1.0.0+0" {
		t.Errorf("MaxConcurrentRollouts = %d, MaxConcurrentRehearsals = %d and Revision = %q, want 2, 0 and v1.0.0+0", got.MaxConcurrentRollouts, got.MaxConcurrentRehearsals, got.Revision)
	}
	if got := newLifecycle(nil, nil, DefaultConfig(), "v1.0.0+0"); got.MaxConcurrentRehearsals != 5 {
		t.Errorf("at the defaults MaxConcurrentRehearsals = %d, want 5", got.MaxConcurrentRehearsals)
	}
}

func TestTheMetricsRunOnTheFilesLimitAndTheRevisionLifecycleGrants(t *testing.T) {
	in := Inputs{Config: DefaultConfig(), Version: "v1.0.0", SignerImage: "signer", TunnelImage: "tunnel", EtcdDruidVersion: "v0.39.0", EtcdDruidNamespace: "druid", OperatorNamespace: "lodia"}
	in.Config.Fleet.MaxConcurrentRollouts = 2
	revision, err := in.Revision()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Metrics(nil, nil, in, &Sources{Health: newHealth(nil, nil, nil, in.Config.Health), Nodes: newNodes(nil, nil, nil, in.Config.Health)})
	if err != nil {
		t.Fatal(err)
	}
	if got.EtcdSample == nil {
		t.Error("the metrics serve no sample of the etcd sampler")
	}
	if want := newLifecycle(nil, nil, in.Config, revision); got.Revision != want.Revision || got.MaxConcurrentRollouts != want.MaxConcurrentRollouts {
		t.Errorf("the metrics run on revision %q and limit %d, want Lifecycle's %q and %d", got.Revision, got.MaxConcurrentRollouts, want.Revision, want.MaxConcurrentRollouts)
	}
	if got.MaxConcurrentRollouts != 2 {
		t.Errorf("MaxConcurrentRollouts = %d, want the file's 2", got.MaxConcurrentRollouts)
	}
}

func TestTheMetricsServeTheHoldFractionOfTheSourcesHealthNodes(t *testing.T) {
	in := Inputs{Config: DefaultConfig(), Version: "v1.0.0"}
	in.Config.Health.HoldFraction = 750
	sources := &Sources{Health: newHealth(nil, nil, nil, in.Config.Health), Nodes: newNodes(nil, nil, nil, in.Config.Health)}
	got, err := Metrics(nil, nil, in, sources)
	if err != nil {
		t.Fatal(err)
	}
	if got.HoldFraction != sources.Nodes.HoldFraction || got.HoldFraction != 750 {
		t.Errorf("the metrics serve a hold fraction of %d thousandths, want health-nodes' %d and the file's 750", got.HoldFraction, sources.Nodes.HoldFraction)
	}
}

func TestTheMetricsServeTheCounterAndHistogramOfTheSources(t *testing.T) {
	in := Inputs{Config: DefaultConfig(), Version: "v1.0.0"}
	sources := &Sources{Health: newHealth(nil, nil, nil, in.Config.Health), Nodes: newNodes(nil, nil, nil, in.Config.Health)}
	got, err := Metrics(nil, nil, in, sources)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProbeFailures == nil || got.ProbeFailures != sources.Health.ProbeFailures {
		t.Error("the metrics serve another counter than the one Health's probes count into")
	}
	if got.SampleLag == nil || got.SampleLag != sources.Nodes.SampleLag {
		t.Error("the metrics serve another histogram than the one health-nodes observes")
	}
}

type managerOf struct {
	ctrl.Manager
	cache  cache.Cache
	reader client.Reader
}

func (m managerOf) GetCache() cache.Cache       { return m.cache }
func (m managerOf) GetAPIReader() client.Reader { return m.reader }
func (m managerOf) GetClient() client.Client    { return nil }

type secretsOf map[client.ObjectKey]*corev1.Secret

type cacheOf struct {
	cache.Cache
	secrets secretsOf
}

func (c cacheOf) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	secret, ok := c.secrets[key]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, key.Name)
	}
	held := obj.(*metav1.PartialObjectMetadata)
	held.UID, held.ResourceVersion = secret.UID, secret.ResourceVersion
	return nil
}

func (c cacheOf) GetInformer(context.Context, client.Object, ...cache.InformerGetOption) (cache.Informer, error) {
	return informerOf{}, nil
}

type informerOf struct{ cache.Informer }

func (informerOf) AddEventHandler(toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

type liveSecrets struct {
	secrets secretsOf
	reads   atomic.Int32
}

func (l *liveSecrets) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	l.reads.Add(1)
	secret, ok := l.secrets[key]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, key.Name)
	}
	secret.DeepCopyInto(obj.(*corev1.Secret))
	return nil
}

func (l *liveSecrets) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return nil
}

func TestEveryReconcilerThatReadsTheAccessSecretsReadsThemThroughTheOneReader(t *testing.T) {
	ca, err := certs.NewCA("etcd", time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue(certs.Profile{CommonName: "client", Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, time.Now(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	caPEM, err := certs.CertPEM(ca.Cert)
	if err != nil {
		t.Fatal(err)
	}
	certPEM, err := certs.CertPEM(leaf.Cert)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := certs.KeyPEM(leaf.Key)
	if err != nil {
		t.Fatal(err)
	}
	secrets := secretsOf{}
	for name, data := range map[string]map[string][]byte{
		"dev-etcd-client": {corev1.TLSCertKey: certPEM, corev1.TLSPrivateKeyKey: keyPEM},
		"dev-etcd-ca":     {lodiav1alpha1.CACertKey: caPEM},
	} {
		key := client.ObjectKey{Namespace: "team-a", Name: name}
		secrets[key] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: name, UID: types.UID("uid-" + name), ResourceVersion: "1"}, Data: data}
	}
	live := &liveSecrets{secrets: secrets}
	in := Inputs{Config: DefaultConfig(), Version: "v1.0.0", SignerImage: "signer", TunnelImage: "tunnel", EtcdDruidVersion: "v0.39.0", EtcdDruidNamespace: "druid", OperatorNamespace: "lodia"}
	c := newControllers(managerOf{cache: cacheOf{secrets: secrets}, reader: live}, in, "v1.0.0+0")

	if _, ok := c.health.Secrets.(*secretcache.Reader); !ok {
		t.Errorf("health reads Secrets through %T, want a *secretcache.Reader", c.health.Secrets)
	}
	for name, got := range map[string]client.Reader{"tenant": c.tenant.Secrets, "health-nodes": c.nodes.Secrets} {
		if got != c.health.Secrets {
			t.Errorf("%s reads Secrets through %T, want health's", name, got)
		}
	}
	etcd := &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a"}}
	etcd.Spec.Etcd.ClientUrlTLS = &druidv1alpha1.TLSConfig{
		TLSCASecretRef:     druidv1alpha1.SecretReference{SecretReference: corev1.SecretReference{Name: "dev-etcd-ca"}},
		ClientTLSSecretRef: corev1.SecretReference{Name: "dev-etcd-client"},
	}
	for name, dial := range map[string]func(context.Context, *druidv1alpha1.Etcd) (*clientv3.Client, error){
		"the datastore controller": c.datastore.EtcdClient, "the backup reconciler": c.backup.EtcdClient, "the etcd sampler": c.sampler.EtcdClient,
	} {
		dialled, err := dial(t.Context(), etcd)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		_ = dialled.Close()
	}
	if got := live.reads.Load(); got != 2 {
		t.Errorf("3 dials of the etcd sent %d reads of a Secret, want the 2 of the first, which the others did not repeat", got)
	}
	for key := range secrets {
		if err := c.health.Secrets.Get(t.Context(), key, &corev1.Secret{}); err != nil {
			t.Fatal(err)
		}
	}
	if got := live.reads.Load(); got != 2 {
		t.Errorf("health's reader sent %d reads of the Secrets the dials had read, want none: the dials read through another reader", got-2)
	}
}
