// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && upgrade

package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	clientv3 "go.etcd.io/etcd/client/v3"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/contract"
)

var copyStartWrites = []string{"/registry/clusterroles/", "/registry/clusterrolebindings/", "/registry/apiregistration.k8s.io/apiservices/"}

type apiServerState struct {
	generation int64
	template   string
	pods       []types.UID
	version    string
}

func apiServerNow(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) apiServerState {
	t.Helper()
	d := &appsv1.Deployment{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: key.Name + "-" + lodiav1alpha1.APIServerComponent}, d); err != nil {
		t.Fatal(err)
	}
	pods := &corev1.PodList{}
	if err := k8s.List(ctx, pods, client.InNamespace(key.Namespace), client.MatchingLabels{lodiav1alpha1.ControlPlaneLabel: key.Name, lodiav1alpha1.ComponentLabel: lodiav1alpha1.APIServerComponent}); err != nil {
		t.Fatal(err)
	}
	s := apiServerState{generation: d.Generation, template: strings.Join(d.Spec.Template.Spec.Containers[0].Command, " ") + " " + d.Spec.Template.Spec.Containers[0].Image}
	for _, p := range pods.Items {
		s.pods = append(s.pods, p.UID)
	}
	slices.Sort(s.pods)
	if c := getControlPlane(ctx, t, k8s, key).Status.Components; c != nil && c.APIServer != nil {
		s.version = c.APIServer.Version
	}
	return s
}

func expectRehearsed(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey, version string, status metav1.ConditionStatus, reason string) *metav1.Condition {
	t.Helper()
	var c *metav1.Condition
	until(ctx, t, 10*time.Minute, fmt.Sprintf("Rehearsed is %s %s for %s", status, reason, version), func(ctx context.Context) error {
		cp := &lodiav1alpha1.ControlPlane{}
		if err := k8s.Get(ctx, key, cp); err != nil {
			return err
		}
		c = meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionRehearsed)
		switch {
		case c == nil || c.ObservedGeneration != cp.Generation || cp.Status.Rehearsal == nil || cp.Status.Rehearsal.Version != version:
			return fmt.Errorf("Rehearsed is %+v, status.rehearsal %+v", c, cp.Status.Rehearsal)
		case c.Status != status || c.Reason != reason:
			return fmt.Errorf("Rehearsed is %s %s: %s", c.Status, c.Reason, c.Message)
		}
		return nil
	})
	t.Logf("Rehearsed %s: %s", c.Reason, c.Message)
	return c
}

func expectNoCopy(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) {
	t.Helper()
	until(ctx, t, time.Minute, "no copy is left", func(ctx context.Context) error {
		pods, err := copyPods(ctx, k8s, key)
		switch {
		case err != nil:
			return err
		case len(pods) > 0:
			return fmt.Errorf("Pod %s is there", pods[0].Name)
		}
		name := client.ObjectKey{Namespace: key.Namespace, Name: key.Name + "-rehearsal"}
		if err := k8s.Get(ctx, name, &networkingv1.NetworkPolicy{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("NetworkPolicy %s: %w", name.Name, err)
		}
		return nil
	})
}

func copyPods(ctx context.Context, k8s client.Client, key client.ObjectKey) ([]corev1.Pod, error) {
	list := &corev1.PodList{}
	err := k8s.List(ctx, list, client.InNamespace(key.Namespace), client.MatchingLabels{lodiav1alpha1.ControlPlaneLabel: key.Name, lodiav1alpha1.ComponentLabel: lodiav1alpha1.RehearsalComponent})
	return list.Items, err
}

type copyTimeline struct {
	mu                   sync.Mutex
	created, ready, gone time.Time
	stop                 context.CancelFunc
	done                 chan struct{}
}

func watchCopy(ctx context.Context, k8s client.Client, key client.ObjectKey) *copyTimeline {
	watching, cancel := context.WithCancel(context.WithoutCancel(ctx))
	tl := &copyTimeline{stop: cancel, done: make(chan struct{})}
	go func() {
		defer close(tl.done)
		for watching.Err() == nil {
			pods, err := copyPods(watching, k8s, key)
			now := time.Now()
			tl.mu.Lock()
			switch {
			case err == nil && len(pods) > 0 && tl.created.IsZero():
				tl.created = pods[0].CreationTimestamp.Time
			case err == nil && len(pods) == 0 && !tl.created.IsZero() && tl.gone.IsZero():
				tl.gone = now
			}
			if err == nil && len(pods) > 0 && tl.ready.IsZero() && slices.ContainsFunc(pods[0].Status.ContainerStatuses, func(c corev1.ContainerStatus) bool { return c.Name == lodiav1alpha1.APIServerComponent && c.Ready }) {
				tl.ready = now
			}
			tl.mu.Unlock()
			select {
			case <-watching.Done():
			case <-time.After(500 * time.Millisecond):
			}
		}
	}()
	return tl
}

func (tl *copyTimeline) log(t *testing.T, name string) (time.Time, time.Time) {
	t.Helper()
	tl.stop()
	<-tl.done
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if tl.gone.IsZero() {
		tl.gone = time.Now()
	}
	since := func(at time.Time) string {
		if at.IsZero() {
			return "never"
		}
		return at.Sub(tl.created).Round(100 * time.Millisecond).String()
	}
	t.Logf("the %s copy: created at %s, its API server ready after %s, gone after %s", name, tl.created.UTC().Format(time.RFC3339), since(tl.ready), since(tl.gone))
	return tl.created, tl.gone
}

type tenantEtcd struct {
	mu     sync.Mutex
	events []etcdEvent
	err    error
	stop   func()
}

type etcdEvent struct {
	at      time.Time
	key, op string
}

func watchTenantEtcd(ctx context.Context, t *testing.T, cfg *envconf.Config, k8s client.Client, key client.ObjectKey) *tenantEtcd {
	t.Helper()
	etcd := &druidv1alpha1.Etcd{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: contract.EtcdName(key.Name)}, etcd); err != nil {
		t.Fatal(err)
	}
	refs := etcd.Spec.Etcd.ClientUrlTLS
	read := func(name string) *corev1.Secret {
		secret := &corev1.Secret{}
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: name}, secret); err != nil {
			t.Fatal(err)
		}
		return secret
	}
	leaf := read(refs.ClientTLSSecretRef.Name)
	certificate, err := tls.X509KeyPair(leaf.Data[corev1.TLSCertKey], leaf.Data[corev1.TLSPrivateKeyKey])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(read(refs.TLSCASecretRef.Name).Data[ptr.Deref(refs.TLSCASecretRef.DataKey, lodiav1alpha1.CACertKey)]) {
		t.Fatal("no etcd CA certificate")
	}
	member := &corev1.Pod{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: etcd.Name + "-0"}, member); err != nil {
		t.Fatal(err)
	}
	local, stopForward := forward(t, cfg.Client().RESTConfig(), member, druidv1alpha1.GetClientPort(etcd))
	c, err := clientv3.New(clientv3.Config{
		Context: context.WithoutCancel(ctx), Endpoints: []string{"https://" + local}, DialTimeout: 10 * time.Second,
		TLS: &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: roots, MinVersion: tls.VersionTLS12},
	})
	if err != nil {
		stopForward()
		t.Fatal(err)
	}
	head, err := c.Get(ctx, "rehearsal", clientv3.WithCountOnly())
	if err != nil {
		t.Fatal(err)
	}
	watching, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w := &tenantEtcd{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for resp := range c.Watch(clientv3.WithRequireLeader(watching), "", clientv3.WithPrefix(), clientv3.WithRev(head.Header.GetRevision()+1)) {
			now := time.Now()
			w.mu.Lock()
			if err := resp.Err(); err != nil && watching.Err() == nil {
				w.err = err
			}
			for _, e := range resp.Events {
				op := "update"
				switch {
				case e.Type == clientv3.EventTypeDelete:
					op = "delete"
				case e.IsCreate():
					op = "create"
				}
				w.events = append(w.events, etcdEvent{at: now, key: string(e.Kv.GetKey()), op: op})
			}
			w.mu.Unlock()
		}
	}()
	w.stop = func() {
		cancel()
		<-done
		_ = c.Close()
		stopForward()
	}
	return w
}

func (w *tenantEtcd) check(t *testing.T, name string, from, to time.Time) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		t.Errorf("the watch of the tenant's etcd broke: %v", w.err)
	}
	written := map[string]int{}
	for _, e := range w.events {
		if e.at.Before(from) || e.at.After(to) {
			continue
		}
		parts := strings.SplitN(e.key, "/", 5)
		prefix := e.key
		if len(parts) >= 4 {
			prefix = strings.Join(parts[:4], "/")
		}
		written[e.op+" "+prefix]++
		if slices.ContainsFunc(copyStartWrites, func(p string) bool { return strings.HasPrefix(e.key, p) }) {
			t.Errorf("while the %s copy ran the tenant's etcd saw %s %s", name, e.op, e.key)
		}
	}
	for _, k := range slices.Sorted(maps.Keys(written)) {
		t.Logf("while the %s copy ran the tenant's etcd saw %d × %s", name, written[k], k)
	}
}

func noCopyIdentity(ctx context.Context, t *testing.T, access *tenantAccess, key client.ObjectKey) {
	t.Helper()
	leases := &coordinationv1.LeaseList{}
	if err := access.client.List(ctx, leases, client.InNamespace(metav1.NamespaceSystem), client.MatchingLabels{"apiserver.kubernetes.io/identity": "kube-apiserver"}); err != nil {
		t.Fatal(err)
	}
	var hosts []string
	for _, l := range leases.Items {
		hosts = append(hosts, l.Labels[corev1.LabelHostname])
		if strings.HasPrefix(l.Labels[corev1.LabelHostname], key.Name+"-rehearsal-") {
			t.Errorf("the tenant holds the copy's identity Lease %s", l.Name)
		}
	}
	if len(hosts) == 0 {
		t.Error("the tenant holds no identity Lease of kube-apiserver")
	}
	t.Logf("the tenant's kube-apiserver identities are of %v", hosts)
}

func setFeatureGates(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey, gates map[string]bool) {
	t.Helper()
	cp := getControlPlane(ctx, t, k8s, key)
	patch := client.MergeFrom(cp.DeepCopy())
	if cp.Spec.APIServer == nil {
		cp.Spec.APIServer = &lodiav1alpha1.APIServer{}
	}
	cp.Spec.APIServer.FeatureGates = gates
	if err := k8s.Patch(ctx, cp, patch); err != nil {
		t.Fatal(err)
	}
	t.Logf("spec.apiServer.featureGates is %v at generation %d", gates, cp.Generation)
}
