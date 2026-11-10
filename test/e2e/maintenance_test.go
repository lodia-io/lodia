// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && maintenance

package e2e

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/distribution/reference"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
	"sigs.k8s.io/e2e-framework/third_party/helm"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const signerTagB = "e2e-b"

func TestOperatorUpgradeInWindows(t *testing.T) {
	var (
		k8s       client.WithWatch
		b         secondBuild
		revisionA string
		revisionB string
		closedWas closedState
		fleet     *fleetWatch
		upgraded  time.Time
	)
	start := time.Now().UTC().Truncate(time.Minute)
	clock := func(d time.Duration) string { return start.Add(d).Format("15:04") }
	open := client.ObjectKey{Namespace: "team-window-open", Name: controlPlane}
	closed := client.ObjectKey{Namespace: "team-window-closed", Name: controlPlane}
	anytime := client.ObjectKey{Namespace: "team-window-anytime", Name: controlPlane}
	closedOpens := start.Add(4 * time.Hour)
	windows := map[client.ObjectKey]*lodiav1alpha1.MaintenanceWindow{
		open:    {Begin: clock(-2 * time.Hour), End: clock(2 * time.Hour), TimeZone: "UTC"},
		closed:  {Begin: clock(4 * time.Hour), End: clock(5 * time.Hour), TimeZone: "UTC"},
		anytime: nil,
	}
	f := features.New("gate G5 on kind: an operator upgrade re-renders a tenant only in its window, and at once on an edit of its spec").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			for _, key := range []client.ObjectKey{open, closed, anytime} {
				window := windows[key]
				createTenant(ctx, t, k8s, key, "e2e-"+key.Namespace, func(spec *lodiav1alpha1.ControlPlaneSpec) {
					if window != nil {
						spec.Maintenance = &lodiav1alpha1.Maintenance{Window: window}
					}
				})
				t.Logf("ControlPlane %s has the maintenance window %+v", key, window)
			}
			return ctx
		}).
		Assess("the three tenants are Ready, every part reporting revision A", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			version, err := buildVersion()
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []client.ObjectKey{open, closed, anytime} {
				revision := readyAtOneRevision(ctx, t, k8s, key)
				if !strings.HasPrefix(revision, version+"+") || revisionA != "" && revision != revisionA {
					t.Fatalf("%s runs revision %q, want %s and a digest, as the others", key, revision, version)
				}
				revisionA = revision
			}
			closedWas = recordClosed(ctx, t, k8s, closed)
			if closedWas.etcdRevision != revisionA {
				t.Fatalf("closed's Etcd is stamped %q, want %q", closedWas.etcdRevision, revisionA)
			}
			fleet = watchFleet(ctx, t, k8s, open, closed, anytime)
			return ctx
		}).
		Assess("the chart upgrades to build B, whose replica takes the leader lease", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			b = buildSecond(ctx, t, cfg)
			upgradeTo(ctx, t, cfg, b)
			awaitLeader(ctx, t, k8s, b.operator.String())
			upgraded = time.Now()
			return ctx
		}).
		Assess("within 3 minutes open and anytime run revision B with signer B, Maintaining then Idle, and stay Ready", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			for _, key := range []client.ObjectKey{open, anytime} {
				until(ctx, t, 3*time.Minute-time.Since(upgraded), key.Namespace+" runs revision B in every part and is Idle", func(ctx context.Context) error {
					cp := &lodiav1alpha1.ControlPlane{}
					if err := k8s.Get(ctx, key, cp); err != nil {
						return err
					}
					revision := cp.Status.Target.OperatorRevision
					switch {
					case revision == revisionA || !strings.HasPrefix(revision, b.version+"+"):
						return fmt.Errorf("target.operatorRevision is %q", revision)
					case revisionB != "" && revision != revisionB:
						return fmt.Errorf("target.operatorRevision is %q, and the other tenant's %q", revision, revisionB)
					}
					if err := reportsRevision(cp, revision); err != nil {
						return err
					}
					if err := signerIs(ctx, k8s, key, b.signer); err != nil {
						return err
					}
					if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionProgressing); c == nil || c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonIdle {
						return fmt.Errorf("Progressing is %+v", c)
					}
					revisionB = revision
					return nil
				})
				if !fleet.saw(key, func(o observation) bool {
					return o.revision == revisionB && o.progressing == lodiav1alpha1.ReasonMaintaining && o.progressingStatus == metav1.ConditionTrue
				}) {
					t.Errorf("%s was never seen Progressing True Maintaining at revision B", key.Namespace)
				}
			}
			return ctx
		}).
		Assess("closed waits for its window for 2 more minutes and changes nothing", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, time.Minute, "closed is WaitingForWindow", func(ctx context.Context) error {
				cp := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, closed, cp); err != nil {
					return err
				}
				return waitingForWindow(observe(cp, time.Now()), revisionA, closedOpens)
			})
			hold(ctx, t, 2*time.Minute, func(ctx context.Context) error {
				cp := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, closed, cp); err != nil {
					return err
				}
				return waitingForWindow(observe(cp, time.Now()), revisionA, closedOpens)
			})
			waited := false
			for _, o := range fleet.of(closed) {
				if waited = waited || o.progressing == lodiav1alpha1.ReasonWaitingForWindow; waited {
					if err := waitingForWindow(o, revisionA, closedOpens); err != nil {
						t.Errorf("once it waited for its window, closed at resourceVersion %s: %v", o.resourceVersion, err)
					}
				}
			}
			if now := recordClosed(ctx, t, k8s, closed); !closedWas.equal(now) {
				t.Errorf("closed changed while it waited for its window: %+v, then %+v", closedWas, now)
			}
			fleet.ready(t, open, closed, anytime)
			return ctx
		}).
		Assess("an edit of closed's spec renders it at once with revision B and signer B, never Maintaining", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cp := getTenant(ctx, t, k8s, closed)
			patch := client.MergeFrom(cp.DeepCopy())
			cp.Spec.Addons.CoreDNS = &lodiav1alpha1.CoreDNS{Replicas: new(int32(1))}
			if err := k8s.Patch(ctx, cp, patch); err != nil {
				t.Fatal(err)
			}
			until(ctx, t, 30*time.Second, "closed is granted revision B", func(ctx context.Context) error {
				fresh := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, closed, fresh); err != nil {
					return err
				}
				if got := fresh.Status.Target.OperatorRevision; got != revisionB {
					return fmt.Errorf("target.operatorRevision is %q", got)
				}
				return nil
			})
			until(ctx, t, 3*time.Minute, "closed's API server rolls to signer B", func(ctx context.Context) error {
				fresh := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, closed, fresh); err != nil {
					return err
				}
				if c := fresh.Status.Components; c == nil || c.APIServer == nil || c.APIServer.OperatorRevision != revisionB {
					return fmt.Errorf("status.components is %+v", c)
				}
				return signerIs(ctx, k8s, closed, b.signer)
			})
			if fleet.saw(closed, func(o observation) bool { return o.progressing == lodiav1alpha1.ReasonMaintaining }) {
				t.Error("closed was seen Maintaining: an edit of its spec took a slot of the fleet")
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if fleet != nil {
				fleet.stop()
				fleet.log(t)
			}
			for _, key := range []client.ObjectKey{open, closed, anytime} {
				closeTenant(ctx, t, k8s, key, "e2e-"+key.Namespace)
			}
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

type secondBuild struct {
	operator reference.NamedTagged
	signer   string
	version  string
}

func buildSecond(ctx context.Context, t *testing.T, cfg *envconf.Config) secondBuild {
	t.Helper()
	version, err := buildVersion()
	if err != nil {
		t.Fatal(err)
	}
	b := secondBuild{version: version + "-b"}
	if b.operator, err = buildAs(ctx, cfg.KubeconfigFile(), "./cmd/lodia", b.version); err != nil {
		t.Fatal(err)
	}
	signer, err := buildAs(ctx, cfg.KubeconfigFile(), "./cmd/lodia-signer", version, signerTagB)
	if err != nil {
		t.Fatal(err)
	}
	b.signer = signer.Name() + ":" + signerTagB
	t.Logf("build B: %s at %s, and %s", b.operator, b.version, b.signer)
	return b
}

func upgradeTo(ctx context.Context, t *testing.T, cfg *envconf.Config, b secondBuild, values ...string) {
	t.Helper()
	args := []string{"--reuse-values", "--set", "image.tag=" + b.operator.Tag(), "--set", "signerImage.tag=" + signerTagB}
	for _, value := range values {
		args = append(args, "--set", value)
	}
	err := helm.New(cfg.KubeconfigFile()).RunUpgrade(
		helm.WithName(release), helm.WithChart(chart), helm.WithNamespace(operatorNamespace), helm.WithContext(ctx),
		helm.WithArgs(args...), helm.WithWait(), helm.WithTimeout("5m"))
	if err != nil {
		t.Fatalf("upgrade the chart to build B: %v", err)
	}
	t.Logf("the chart runs %s and %s", b.operator, b.signer)
}

func awaitLeader(ctx context.Context, t *testing.T, k8s client.Client, image string) {
	t.Helper()
	deployment := operatorDeployment(ctx, t, k8s)
	until(ctx, t, 5*time.Minute, "a running replica of "+image+" holds the lease lodia.io", func(ctx context.Context) error {
		lease := &coordinationv1.Lease{}
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: operatorNamespace, Name: "lodia.io"}, lease); err != nil {
			return err
		}
		pods := &corev1.PodList{}
		if err := k8s.List(ctx, pods, client.InNamespace(operatorNamespace), client.MatchingLabels(deployment.Spec.Selector.MatchLabels)); err != nil {
			return err
		}
		identity := ptr.Deref(lease.Spec.HolderIdentity, "")
		for i := range pods.Items {
			pod := &pods.Items[i]
			if strings.HasPrefix(identity, pod.Name+"_") && running(pod) == nil && pod.Spec.Containers[0].Image == image {
				return nil
			}
		}
		return fmt.Errorf("the lease is held by %q", identity)
	})
}

func getTenant(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) *lodiav1alpha1.ControlPlane {
	t.Helper()
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func readyAtOneRevision(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) string {
	t.Helper()
	var revision string
	until(ctx, t, 8*time.Minute, key.Namespace+" is Ready and Idle, every part at one revision", func(ctx context.Context) error {
		cp := &lodiav1alpha1.ControlPlane{}
		if err := controlPlaneReady(ctx, k8s, key, cp); err != nil {
			return err
		}
		if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionProgressing); c == nil || c.Reason != lodiav1alpha1.ReasonIdle {
			return fmt.Errorf("Progressing is %+v", c)
		}
		revision = cp.Status.Target.OperatorRevision
		return reportsRevision(cp, revision)
	})
	return revision
}

func reportsRevision(cp *lodiav1alpha1.ControlPlane, revision string) error {
	return allAt(revisions(cp), revision)
}

func revisions(cp *lodiav1alpha1.ControlPlane) map[string]string {
	parts := map[string]string{}
	if c := cp.Status.Components; c != nil {
		for part, s := range map[string]*lodiav1alpha1.ComponentStatus{"kube-apiserver": c.APIServer, "kube-controller-manager": c.ControllerManager, "kube-scheduler": c.Scheduler} {
			if s != nil {
				parts[part] = s.OperatorRevision
			}
		}
	}
	if a := cp.Status.Addons; a != nil {
		parts["the addons"] = a.OperatorRevision
	}
	if d := cp.Status.Datastore; d != nil {
		parts["etcd"] = d.OperatorRevision
	}
	return parts
}

func allAt(parts map[string]string, revision string) error {
	for _, part := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler", "the addons", "etcd"} {
		got, ok := parts[part]
		switch {
		case !ok:
			return fmt.Errorf("%s reports no operator revision", part)
		case got != revision:
			return fmt.Errorf("%s reports operator revision %q, not %q", part, got, revision)
		}
	}
	return nil
}

func signerIs(ctx context.Context, k8s client.Client, key client.ObjectKey, image string) error {
	deployment := &appsv1.Deployment{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: lodiav1alpha1.APIServerServiceName(key.Name)}, deployment); err != nil {
		return err
	}
	for _, c := range deployment.Spec.Template.Spec.InitContainers {
		if c.Name == "lodia-signer" {
			if c.Image != image {
				return fmt.Errorf("the API server's template runs %s, not %s", c.Image, image)
			}
			return nil
		}
	}
	return fmt.Errorf("the API server's template has no lodia-signer")
}

type closedState struct {
	deployments  map[string]int64
	pods         map[string]types.UID
	etcd         int64
	etcdRevision string
	inventory    string
}

func (s closedState) equal(other closedState) bool {
	return maps.Equal(s.deployments, other.deployments) && maps.Equal(s.pods, other.pods) && s.etcd == other.etcd && s.etcdRevision == other.etcdRevision && s.inventory == other.inventory
}

func recordClosed(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) closedState {
	t.Helper()
	s := closedState{deployments: map[string]int64{}, pods: map[string]types.UID{}}
	deployments := &appsv1.DeploymentList{}
	if err := k8s.List(ctx, deployments, client.InNamespace(key.Namespace)); err != nil {
		t.Fatal(err)
	}
	for _, d := range deployments.Items {
		s.deployments[d.Name] = d.Generation
	}
	pods := &corev1.PodList{}
	if err := k8s.List(ctx, pods, client.InNamespace(key.Namespace)); err != nil {
		t.Fatal(err)
	}
	for _, p := range pods.Items {
		s.pods[p.Name] = p.UID
	}
	etcd := &druidv1alpha1.Etcd{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: lodiav1alpha1.EtcdName(key.Name)}, etcd); err != nil {
		t.Fatal(err)
	}
	s.etcd, s.etcdRevision = etcd.Generation, etcd.Annotations[lodiav1alpha1.OperatorRevisionAnnotation]
	inventory := &corev1.ConfigMap{}
	if err := openTenant(ctx, t, k8s, key).client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: "lodia-inventory"}, inventory); err != nil {
		t.Fatal(err)
	}
	s.inventory = inventory.ResourceVersion
	return s
}

func waitingForWindow(o observation, revision string, opens time.Time) error {
	switch {
	case o.progressing != lodiav1alpha1.ReasonWaitingForWindow || o.progressingStatus != metav1.ConditionFalse:
		return fmt.Errorf("Progressing is %s %s", o.progressingStatus, o.progressing)
	case o.scheduledFor == nil || !o.scheduledFor.Time.Equal(opens):
		return fmt.Errorf("target.scheduledFor is %v, want the window's next start %s", o.scheduledFor, opens.Format(time.RFC3339))
	case o.revision != revision:
		return fmt.Errorf("target.operatorRevision is %q, want %q", o.revision, revision)
	}
	return allAt(o.parts, revision)
}

func hold(ctx context.Context, t *testing.T, d time.Duration, check func(context.Context) error) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if err := check(ctx); err != nil {
			t.Fatalf("%s into the %s: %v", time.Until(end).Round(time.Second), d, err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Second):
		}
	}
	t.Logf("held for %s", d)
}

type observation struct {
	key               client.ObjectKey
	at                time.Time
	resourceVersion   string
	ready             metav1.ConditionStatus
	readyReason       string
	progressingStatus metav1.ConditionStatus
	progressing       string
	revision          string
	scheduledFor      *metav1.Time
	parts             map[string]string
	slot              bool
}

func observe(cp *lodiav1alpha1.ControlPlane, at time.Time) observation {
	o := observation{key: client.ObjectKeyFromObject(cp), at: at, resourceVersion: cp.ResourceVersion, ready: metav1.ConditionUnknown, progressingStatus: metav1.ConditionUnknown, parts: revisions(cp)}
	if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady); c != nil {
		o.ready, o.readyReason = c.Status, c.Reason
	}
	if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionProgressing); c != nil {
		o.progressingStatus, o.progressing = c.Status, c.Reason
	}
	if target := cp.Status.Target; target != nil {
		o.revision, o.scheduledFor = target.OperatorRevision, target.ScheduledFor
	}
	o.slot = cp.Status.Maintenance != nil && cp.Status.Maintenance.SlotSince != nil
	return o
}

type fleetWatch struct {
	mu     sync.Mutex
	keys   []client.ObjectKey
	seen   []observation
	broken []error
	stop   func()
}

func watchFleet(ctx context.Context, t *testing.T, k8s client.WithWatch, keys ...client.ObjectKey) *fleetWatch {
	t.Helper()
	w := &fleetWatch{keys: keys}
	list := &lodiav1alpha1.ControlPlaneList{}
	if err := k8s.List(ctx, list); err != nil {
		t.Fatal(err)
	}
	for i := range list.Items {
		w.observe(&list.Items[i])
	}
	lw := &cache.ListWatch{WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
		return k8s.Watch(ctx, &lodiav1alpha1.ControlPlaneList{}, &client.ListOptions{Raw: &options})
	}}
	watching, cancel := context.WithCancel(context.WithoutCancel(ctx))
	rw, err := watchtools.NewRetryWatcherWithContext(watching, list.ResourceVersion, lw)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range rw.ResultChan() {
			switch cp, ok := event.Object.(*lodiav1alpha1.ControlPlane); {
			case event.Type == watch.Error:
				w.mu.Lock()
				w.broken = append(w.broken, apierrors.FromObject(event.Object))
				w.mu.Unlock()
			case ok && (event.Type == watch.Added || event.Type == watch.Modified):
				w.observe(cp)
			}
		}
	}()
	w.stop = sync.OnceFunc(func() {
		cancel()
		rw.Stop()
		<-done
	})
	return w
}

func (w *fleetWatch) observe(cp *lodiav1alpha1.ControlPlane) {
	if !slices.Contains(w.keys, client.ObjectKeyFromObject(cp)) {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seen = append(w.seen, observe(cp, time.Now()))
}

func (w *fleetWatch) all() []observation {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.seen)
}

func (w *fleetWatch) of(key client.ObjectKey) []observation {
	return slices.DeleteFunc(w.all(), func(o observation) bool { return o.key != key })
}

func (w *fleetWatch) saw(key client.ObjectKey, match func(observation) bool) bool {
	return slices.ContainsFunc(w.of(key), match)
}

func (w *fleetWatch) whole() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.broken) > 0 {
		return fmt.Errorf("the watch of the ControlPlanes broke, so its record is incomplete: %w", errors.Join(w.broken...))
	}
	return nil
}

func (w *fleetWatch) ready(t *testing.T, keys ...client.ObjectKey) {
	t.Helper()
	if err := w.whole(); err != nil {
		t.Error(err)
	}
	for _, o := range w.all() {
		if slices.Contains(keys, o.key) && o.ready != metav1.ConditionTrue {
			t.Errorf("%s at resourceVersion %s, %s: Ready is %s %s", o.key, o.resourceVersion, o.at.Format(time.RFC3339), o.ready, o.readyReason)
		}
	}
}

func (w *fleetWatch) log(t *testing.T) {
	t.Helper()
	last := map[client.ObjectKey]observation{}
	for _, o := range w.all() {
		previous, seen := last[o.key]
		last[o.key] = o
		if seen && previous.ready == o.ready && previous.progressing == o.progressing && previous.revision == o.revision {
			continue
		}
		t.Logf("%s %s: Ready=%s %s, Progressing=%s %s, revision %s, scheduledFor %v", o.at.Format(time.RFC3339), o.key.Namespace, o.ready, o.readyReason, o.progressingStatus, o.progressing, o.revision, o.scheduledFor)
	}
}
