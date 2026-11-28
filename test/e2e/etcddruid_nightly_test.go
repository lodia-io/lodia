// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && maintenance

package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
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
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/contract"
)

const druidDeployment = "etcd-druid"

type druidRelease struct {
	tag     string
	wrapper string
	brctl   string
}

var (
	druidBefore = druidRelease{tag: "v0.39.1@sha256:979f0ae62d1e84a5b083132445bfe5621369727ba9787c7098e3b0ac206902f8", wrapper: "v0.8.3", brctl: "v0.45.2"}
	druidAfter  = druidRelease{wrapper: "v0.8.4", brctl: "v0.45.3"}
)

func TestEtcdDruidPatchUpgradeUnderAHeldTenant(t *testing.T) {
	var (
		k8s       client.WithWatch
		history   *druidHistory
		revisionA string
		revisionB string
		member    types.UID
		upgraded  time.Time
		opened    time.Time
		resumed   time.Time
	)
	key := client.ObjectKey{Namespace: "team-druid-patch", Name: controlPlane}
	etcdKey := client.ObjectKey{Namespace: key.Namespace, Name: contract.EtcdName(key.Name)}
	start := time.Now().UTC().Truncate(time.Minute)
	clock := func(at time.Time, d time.Duration) string { return at.Add(d).Format("15:04") }
	closedOpens := start.Add(4 * time.Hour)
	f := features.New("nightly on kind: etcd-druid upgraded across a patch under a tenant held outside its window").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			druidAfter.tag = chartDruidTag(t)
			if druidAfter.tag == druidBefore.tag {
				t.Fatalf("the chart pins etcd-druid %s, the release the upgrade starts from: name a release before it in druidBefore, with its sidecars", druidAfter.tag)
			}
			setEtcdDruid(ctx, t, cfg, druidBefore.tag, true)
			awaitOperatorWith(ctx, t, k8s, druidBefore.tag)
			until(ctx, t, 3*time.Minute, "etcd-druid's Deployment runs "+druidBefore.tag+" in every replica", func(ctx context.Context) error {
				return druidRunning(ctx, k8s, druidBefore.tag)
			})
			createTenant(ctx, t, k8s, key, "e2e-druid-patch", func(spec *lodiav1alpha1.ControlPlaneSpec) {
				spec.Maintenance = &lodiav1alpha1.Maintenance{Window: lodiav1alpha1.MaintenanceWindow{Begin: clock(start, 4*time.Hour), End: clock(start, 5*time.Hour), TimeZone: "UTC"}}
			})
			return ctx
		}).
		Assess("the tenant is Ready under etcd-druid v0.39.1, its member on that release's sidecars", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			version, err := buildVersion()
			if err != nil {
				t.Fatal(err)
			}
			revisionA = readyAtOneRevision(ctx, t, k8s, key)
			if !strings.HasPrefix(revisionA, version+"+") {
				t.Fatalf("%s runs revision %q, want %s and a digest", key, revisionA, version)
			}
			pod := memberPod(ctx, t, k8s, etcdKey)
			if err := runsSidecars(pod, druidBefore); err != nil {
				t.Fatal(err)
			}
			member = pod.UID
			history = recordDruid(ctx, t, k8s, key, etcdKey)
			return ctx
		}).
		Assess("with etcd-druid's Deployment paused, the chart upgrades to v0.39.2, and the tenant waits for its window for 2 minutes with nothing asked of etcd-druid", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			history.mark("etcd-druid's Deployment is paused")
			pauseDruid(ctx, t, k8s, true)
			history.mark("helm upgrade --reuse-values --set etcdDruid.image.tag=" + druidAfter.tag)
			setEtcdDruid(ctx, t, cfg, druidAfter.tag, false)
			awaitOperatorWith(ctx, t, k8s, druidAfter.tag)
			upgraded = time.Now()
			history.mark("every pod of the operator runs with --etcd-druid-version=" + druidAfter.tag)
			until(ctx, t, 3*time.Minute, "etcd-druid's Deployment asks for "+druidAfter.tag+" and still runs "+druidBefore.tag, func(ctx context.Context) error {
				return druidPaused(ctx, k8s, druidAfter.tag)
			})
			until(ctx, t, 3*time.Minute, "the tenant is WaitingForWindow", func(ctx context.Context) error {
				return heldAtRevision(ctx, k8s, key, revisionA, closedOpens)
			})
			hold(ctx, t, 2*time.Minute, func(ctx context.Context) error {
				return errors.Join(heldAtRevision(ctx, k8s, key, revisionA, closedOpens), druidUntouched(ctx, k8s, etcdKey, history, member))
			})
			return ctx
		}).
		Assess("its window opens with the Deployment still paused: the revision is granted, and for a minute etcd-druid is asked for nothing", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			opened = time.Now()
			history.mark("the tenant's window is edited to hold now")
			now := time.Now().UTC().Truncate(time.Minute)
			cp := getTenant(ctx, t, k8s, key)
			patch := client.MergeFrom(cp.DeepCopy())
			cp.Spec.Maintenance.Window = lodiav1alpha1.MaintenanceWindow{Begin: clock(now, -2*time.Hour), End: clock(now, 2*time.Hour), TimeZone: "UTC"}
			if err := k8s.Patch(ctx, cp, patch); err != nil {
				t.Fatal(err)
			}
			until(ctx, t, time.Minute, "the tenant is granted a revision other than "+revisionA, func(ctx context.Context) error {
				fresh := getTenant(ctx, t, k8s, key)
				if fresh.Status.Target == nil || fresh.Status.Target.OperatorRevision == revisionA {
					return fmt.Errorf("status.target is %+v", fresh.Status.Target)
				}
				revisionB = fresh.Status.Target.OperatorRevision
				return nil
			})
			history.mark("the tenant is granted " + short(revisionB))
			hold(ctx, t, time.Minute, func(ctx context.Context) error {
				return errors.Join(druidPaused(ctx, k8s, druidAfter.tag), druidUntouched(ctx, k8s, etcdKey, history, member), datastoreAt(ctx, k8s, key, revisionA))
			})
			return ctx
		}).
		Assess("once the Deployment runs v0.39.2 in every replica, etcd-druid is asked once under the new revision, serves it once, and the member runs the new sidecars", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			resumed = time.Now()
			history.mark("etcd-druid's Deployment is resumed")
			pauseDruid(ctx, t, k8s, false)
			until(ctx, t, 10*time.Minute, "the tenant reports etcd at "+short(revisionB)+", Ready and Idle at one revision", func(ctx context.Context) error {
				cp := &lodiav1alpha1.ControlPlane{}
				if err := controlPlaneReady(ctx, k8s, key, cp); err != nil {
					return err
				}
				if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionProgressing); c == nil || c.Reason != lodiav1alpha1.ReasonIdle {
					return fmt.Errorf("Progressing is %+v", c)
				}
				return reportsRevision(cp, revisionB)
			})
			hold(ctx, t, 30*time.Second, func(ctx context.Context) error {
				return datastoreAt(ctx, k8s, key, revisionB)
			})
			history.stop()
			history.verify(t, druidChange{
				revisionA: revisionA, revisionB: revisionB, member: member,
				upgraded: upgraded, opened: opened, resumed: resumed,
			})
			if err := runsSidecars(memberPod(ctx, t, k8s, etcdKey), druidAfter); err != nil {
				t.Error(err)
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if history != nil {
				history.stop()
				history.log(t)
			}
			if k8s != nil {
				pauseDruid(ctx, t, k8s, false)
			}
			closeTenant(ctx, t, k8s, key, "e2e-druid-patch")
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func chartDruidTag(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(chart, "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		EtcdDruid struct {
			Image struct {
				Tag string `json:"tag"`
			} `json:"image"`
		} `json:"etcdDruid"`
	}
	if err := yaml.Unmarshal(data, &values); err != nil {
		t.Fatal(err)
	}
	return values.EtcdDruid.Image.Tag
}

func setEtcdDruid(ctx context.Context, t *testing.T, cfg *envconf.Config, tag string, wait bool) {
	t.Helper()
	options := []helm.Option{
		helm.WithName(release), helm.WithChart(chart), helm.WithNamespace(operatorNamespace), helm.WithContext(ctx),
		helm.WithArgs("--reuse-values", "--set", "etcdDruid.image.tag="+tag),
	}
	if wait {
		options = append(options, helm.WithWait(), helm.WithTimeout("5m"))
	}
	if err := helm.New(cfg.KubeconfigFile()).RunUpgrade(options...); err != nil {
		t.Fatalf("upgrade the chart to etcd-druid %s: %v", tag, err)
	}
	t.Logf("the chart asks for etcd-druid %s", tag)
}

func awaitOperatorWith(ctx context.Context, t *testing.T, k8s client.Client, druidTag string) {
	t.Helper()
	deployment := operatorDeployment(ctx, t, k8s)
	arg := "--etcd-druid-version=" + druidTag
	until(ctx, t, 5*time.Minute, "every pod of the operator runs with "+arg+" and one of them holds the lease lodia.io", func(ctx context.Context) error {
		lease := &coordinationv1.Lease{}
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: operatorNamespace, Name: "lodia.io"}, lease); err != nil {
			return err
		}
		pods := &corev1.PodList{}
		if err := k8s.List(ctx, pods, client.InNamespace(operatorNamespace), client.MatchingLabels(deployment.Spec.Selector.MatchLabels)); err != nil {
			return err
		}
		identity, leading := ptr.Deref(lease.Spec.HolderIdentity, ""), false
		for i := range pods.Items {
			pod := &pods.Items[i]
			if !slices.Contains(pod.Spec.Containers[0].Args, arg) {
				return fmt.Errorf("pod %s runs with %v", pod.Name, pod.Spec.Containers[0].Args)
			}
			leading = leading || strings.HasPrefix(identity, pod.Name+"_") && running(pod) == nil
		}
		if !leading {
			return fmt.Errorf("the lease is held by %q", identity)
		}
		return nil
	})
}

func druidRunning(ctx context.Context, k8s client.Client, tag string) error {
	d := &appsv1.Deployment{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: operatorNamespace, Name: druidDeployment}, d); err != nil {
		return err
	}
	if !druidRolledOut(d, tag) {
		return fmt.Errorf("Deployment %s: paused %t, generation %d, status %+v", d.Name, d.Spec.Paused, d.Generation, d.Status)
	}
	return nil
}

func druidRolledOut(d *appsv1.Deployment, tag string) bool {
	s := d.Status
	asks := slices.ContainsFunc(d.Spec.Template.Spec.Containers, func(c corev1.Container) bool { return strings.HasSuffix(c.Image, ":"+tag) })
	replicas := ptr.Deref(d.Spec.Replicas, 1)
	return asks && !d.Spec.Paused && s.ObservedGeneration >= d.Generation && s.UpdatedReplicas == replicas && s.Replicas == replicas && s.AvailableReplicas == replicas && ptr.Deref(s.TerminatingReplicas, 0) == 0
}

func druidPaused(ctx context.Context, k8s client.Client, tag string) error {
	d := &appsv1.Deployment{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: operatorNamespace, Name: druidDeployment}, d); err != nil {
		return err
	}
	asks := slices.ContainsFunc(d.Spec.Template.Spec.Containers, func(c corev1.Container) bool { return strings.HasSuffix(c.Image, ":"+tag) })
	if !asks || !d.Spec.Paused || d.Status.UpdatedReplicas != 0 || d.Status.ObservedGeneration < d.Generation {
		return fmt.Errorf("Deployment %s: asks for %s %t, paused %t, generation %d, status %+v", d.Name, tag, asks, d.Spec.Paused, d.Generation, d.Status)
	}
	return nil
}

func pauseDruid(ctx context.Context, t *testing.T, k8s client.Client, paused bool) {
	t.Helper()
	d := &appsv1.Deployment{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: operatorNamespace, Name: druidDeployment}, d); err != nil {
		t.Error(err)
		return
	}
	patch := client.MergeFrom(d.DeepCopy())
	d.Spec.Paused = paused
	if err := k8s.Patch(ctx, d, patch); err != nil {
		t.Error(err)
	}
}

func memberPod(ctx context.Context, t *testing.T, k8s client.Client, etcd client.ObjectKey) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: etcd.Namespace, Name: etcd.Name + "-0"}, pod); err != nil {
		t.Fatal(err)
	}
	return pod
}

func runsSidecars(pod *corev1.Pod, release druidRelease) error {
	for container, image := range map[string]string{"etcd": "/etcd-wrapper:" + release.wrapper, "backup-restore": "/etcdbrctl:" + release.brctl} {
		i := slices.IndexFunc(pod.Spec.Containers, func(c corev1.Container) bool { return c.Name == container })
		if i < 0 || !strings.HasSuffix(pod.Spec.Containers[i].Image, image) {
			return fmt.Errorf("pod %s: container %s runs %v, want an image ending in %s", pod.Name, container, pod.Spec.Containers, image)
		}
	}
	return nil
}

func heldAtRevision(ctx context.Context, k8s client.Client, key client.ObjectKey, revision string, opens time.Time) error {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		return err
	}
	return waitingForWindow(observe(cp, time.Now()), revision, opens)
}

func datastoreAt(ctx context.Context, k8s client.Client, key client.ObjectKey, revision string) error {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		return err
	}
	if got := revisions(cp)["etcd"]; got != revision {
		return fmt.Errorf("status.datastore.operatorRevision is %q, want %q", got, revision)
	}
	return nil
}

func druidUntouched(ctx context.Context, k8s client.Client, etcdKey client.ObjectKey, history *druidHistory, member types.UID) error {
	etcd := &druidv1alpha1.Etcd{}
	if err := k8s.Get(ctx, etcdKey, etcd); err != nil {
		return err
	}
	if druidv1alpha1.HasReconcileOperationAnnotation(etcd.ObjectMeta) {
		return fmt.Errorf("Etcd %s carries %s", etcd.Name, druidv1alpha1.DruidOperationAnnotation)
	}
	request, updateRevision, generation := history.baseline()
	if requested := etcd.Annotations[lodiav1alpha1.EtcdReconcileRequestedAnnotation]; requested != request {
		return fmt.Errorf("Etcd %s's request is %q, was %q", etcd.Name, requested, request)
	}
	sts := &appsv1.StatefulSet{}
	if err := k8s.Get(ctx, etcdKey, sts); err != nil {
		return err
	}
	if sts.Status.UpdateRevision != updateRevision || sts.Generation != generation {
		return fmt.Errorf("StatefulSet %s is at generation %d and updateRevision %s, was %d and %s", sts.Name, sts.Generation, sts.Status.UpdateRevision, generation, updateRevision)
	}
	pod := &corev1.Pod{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: etcdKey.Namespace, Name: etcdKey.Name + "-0"}, pod); err != nil {
		return err
	}
	if pod.UID != member {
		return fmt.Errorf("the member pod is %s, was %s", pod.UID, member)
	}
	return nil
}

type druidChange struct {
	revisionA, revisionB      string
	member                    types.UID
	upgraded, opened, resumed time.Time
}

type etcdState struct {
	at         time.Time
	generation int64
	stamp      string
	requested  string
	operation  bool
	run        string
	lastOp     string
}

type statefulSetState struct {
	at             time.Time
	generation     int64
	updateRevision string
	ready          int32
}

type deploymentState struct {
	at        time.Time
	paused    bool
	image     string
	updated   int32
	rolledOut bool
}

type podState struct {
	at     time.Time
	uid    types.UID
	images string
	ready  bool
}

type tenantState struct {
	at          time.Time
	ready       metav1.ConditionStatus
	progressing string
	target      string
	datastore   string
}

type druidHistory struct {
	mu          sync.Mutex
	created     time.Time
	marks       []stage
	etcd        []etcdState
	statefulSet []statefulSetState
	deployment  []deploymentState
	pod         []podState
	tenant      []tenantState
	broken      []error
	stops       []func()
	stopped     sync.Once
}

func recordDruid(ctx context.Context, t *testing.T, k8s client.WithWatch, key, etcdKey client.ObjectKey) *druidHistory {
	t.Helper()
	h := &druidHistory{created: time.Now()}
	named := func(name string) []client.ListOption {
		return []client.ListOption{client.InNamespace(etcdKey.Namespace), client.MatchingFields{"metadata.name": name}}
	}
	h.watch(ctx, t, k8s, &druidv1alpha1.EtcdList{}, h.observeEtcd, named(etcdKey.Name))
	h.watch(ctx, t, k8s, &appsv1.StatefulSetList{}, h.observeStatefulSet, named(etcdKey.Name))
	h.watch(ctx, t, k8s, &corev1.PodList{}, h.observePod, named(etcdKey.Name+"-0"))
	h.watch(ctx, t, k8s, &appsv1.DeploymentList{}, h.observeDeployment, []client.ListOption{client.InNamespace(operatorNamespace), client.MatchingFields{"metadata.name": druidDeployment}})
	h.watch(ctx, t, k8s, &lodiav1alpha1.ControlPlaneList{}, h.observeTenant, named(key.Name))
	return h
}

func (h *druidHistory) watch(ctx context.Context, t *testing.T, k8s client.WithWatch, list client.ObjectList, handle func(client.Object, time.Time), options []client.ListOption) {
	t.Helper()
	if err := k8s.List(ctx, list, options...); err != nil {
		t.Fatal(err)
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		handle(item.(client.Object), time.Now())
	}
	lw := &cache.ListWatch{WatchFuncWithContext: func(ctx context.Context, raw metav1.ListOptions) (watch.Interface, error) {
		return k8s.Watch(ctx, list.DeepCopyObject().(client.ObjectList), append(slices.Clone(options), &client.ListOptions{Raw: &raw})...)
	}}
	watching, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w, err := watchtools.NewRetryWatcherWithContext(watching, list.GetResourceVersion(), lw)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range w.ResultChan() {
			switch obj, ok := event.Object.(client.Object); {
			case event.Type == watch.Error:
				h.mu.Lock()
				h.broken = append(h.broken, fmt.Errorf("%T: %v", list, event.Object))
				h.mu.Unlock()
			case ok && (event.Type == watch.Added || event.Type == watch.Modified):
				handle(obj, time.Now())
			}
		}
	}()
	h.stops = append(h.stops, func() {
		cancel()
		w.Stop()
		<-done
	})
}

func (h *druidHistory) stop() {
	h.stopped.Do(func() {
		for _, stop := range h.stops {
			stop()
		}
	})
}

func (h *druidHistory) mark(what string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.marks = append(h.marks, stage{at: time.Now(), what: "-- " + what})
}

func (h *druidHistory) observeEtcd(obj client.Object, at time.Time) {
	etcd := obj.(*druidv1alpha1.Etcd)
	s := etcdState{
		at: at, generation: etcd.Generation, stamp: etcd.Annotations[lodiav1alpha1.OperatorRevisionAnnotation],
		requested: etcd.Annotations[lodiav1alpha1.EtcdReconcileRequestedAnnotation], operation: druidv1alpha1.HasReconcileOperationAnnotation(etcd.ObjectMeta),
	}
	if last := etcd.Status.LastOperation; last != nil {
		s.run, s.lastOp = last.RunID, fmt.Sprintf("%s %s", last.Type, last.State)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.etcd = append(h.etcd, s)
}

func (h *druidHistory) observeStatefulSet(obj client.Object, at time.Time) {
	sts := obj.(*appsv1.StatefulSet)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.statefulSet = append(h.statefulSet, statefulSetState{at: at, generation: sts.Generation, updateRevision: sts.Status.UpdateRevision, ready: sts.Status.ReadyReplicas})
}

func (h *druidHistory) observeDeployment(obj client.Object, at time.Time) {
	d := obj.(*appsv1.Deployment)
	s := deploymentState{at: at, paused: d.Spec.Paused, updated: d.Status.UpdatedReplicas, rolledOut: druidRolledOut(d, druidAfter.tag)}
	if len(d.Spec.Template.Spec.Containers) > 0 {
		s.image = d.Spec.Template.Spec.Containers[0].Image[strings.LastIndex(d.Spec.Template.Spec.Containers[0].Image, ":")+1:]
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deployment = append(h.deployment, s)
}

func (h *druidHistory) observePod(obj client.Object, at time.Time) {
	pod := obj.(*corev1.Pod)
	var images []string
	for _, c := range pod.Spec.Containers {
		images = append(images, c.Name+"="+c.Image[strings.LastIndex(c.Image, ":")+1:])
	}
	ready := slices.ContainsFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool {
		return c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pod = append(h.pod, podState{at: at, uid: pod.UID, images: strings.Join(images, " "), ready: ready})
}

func (h *druidHistory) observeTenant(obj client.Object, at time.Time) {
	o := observe(obj.(*lodiav1alpha1.ControlPlane), at)
	s := tenantState{at: at, ready: o.ready, progressing: o.progressing, target: o.revision, datastore: o.parts["etcd"]}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tenant = append(h.tenant, s)
}

func (h *druidHistory) baseline() (request, updateRevision string, generation int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.etcd[0].requested, h.statefulSet[0].updateRevision, h.statefulSet[0].generation
}

func changes[T comparable](values []T) []T {
	return slices.Compact(slices.Clone(values))
}

func project[S any, T any](states []S, field func(S) T) []T {
	values := make([]T, len(states))
	for i, s := range states {
		values[i] = field(s)
	}
	return values
}

func edges(flags []bool, to bool) int {
	n := 0
	for i := 1; i < len(flags); i++ {
		if flags[i] == to && flags[i-1] != to {
			n++
		}
	}
	return n
}

func (h *druidHistory) verify(t *testing.T, c druidChange) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.broken) > 0 {
		t.Errorf("a watch broke, so the record is incomplete: %v", h.broken)
	}
	requests := changes(project(h.etcd, func(s etcdState) string { return s.requested }))
	if len(requests) != 2 || !strings.Contains(requests[1], "/"+c.revisionB+"/") {
		t.Errorf("the Etcd's request annotation took the values %q, want its first and then one for revision %s alone", requests, c.revisionB)
	}
	if n := edges(project(h.etcd, func(s etcdState) bool { return s.operation }), true); n != 1 {
		t.Errorf("%s was set %d times after the record began, want once", druidv1alpha1.DruidOperationAnnotation, n)
	}
	if n := edges(project(h.etcd, func(s etcdState) bool { return s.operation }), false); n != 1 {
		t.Errorf("%s was removed %d times after the record began, want once", druidv1alpha1.DruidOperationAnnotation, n)
	}
	var serving []string
	for _, s := range h.etcd {
		if s.operation && strings.HasSuffix(s.lastOp, string(druidv1alpha1.LastOperationStateProcessing)) && !slices.Contains(serving, s.run) {
			serving = append(serving, s.run)
		}
	}
	if len(serving) != 1 {
		t.Errorf("etcd-druid's spec reconciles that started while %s stood ran under the run IDs %q, want one", druidv1alpha1.DruidOperationAnnotation, serving)
	}
	t.Logf("spec reconciles of etcd-druid after the request: %d, of which %d started under the annotation", len(changes(project(h.etcd, func(s etcdState) string { return s.run })))-1, len(serving))
	last := h.etcd[len(h.etcd)-1]
	if !strings.HasPrefix(last.lastOp, string(druidv1alpha1.LastOperationTypeReconcile)) || !strings.HasSuffix(last.lastOp, string(druidv1alpha1.LastOperationStateSucceeded)) || last.operation {
		t.Errorf("the Etcd ends with lastOperation %q and %s %t, want a Reconcile Succeeded and no annotation", last.lastOp, druidv1alpha1.DruidOperationAnnotation, last.operation)
	}
	if revisions := changes(project(h.tenant, func(s tenantState) string { return s.datastore })); !slices.Equal(revisions, []string{c.revisionA, c.revisionB}) {
		t.Errorf("status.datastore.operatorRevision took the values %q, want %q and then %q once", revisions, short(c.revisionA), short(c.revisionB))
	}
	if updates := changes(project(h.statefulSet, func(s statefulSetState) string { return s.updateRevision })); len(updates) != 2 {
		t.Errorf("the StatefulSet's updateRevision took the values %q, want one move", updates)
	}
	if pods := changes(project(h.pod, func(s podState) types.UID { return s.uid })); len(pods) != 2 || pods[0] != c.member {
		t.Errorf("the member pod ran under the UIDs %q, want %s and then one", pods, c.member)
	}
	for _, s := range h.tenant {
		if s.at.After(c.upgraded) && !s.at.After(c.opened) && s.ready != metav1.ConditionTrue {
			t.Errorf("%s: Ready was %s between the operator's upgrade and the opening of the window", s.at.Format(time.RFC3339Nano), s.ready)
		}
	}
	var rolledOut time.Time
	for _, s := range h.deployment {
		if s.at.After(c.resumed) && s.rolledOut {
			rolledOut = s.at
			break
		}
	}
	var asked time.Time
	for i := 1; i < len(h.etcd); i++ {
		if h.etcd[i].requested != h.etcd[i-1].requested {
			asked = h.etcd[i].at
		}
	}
	switch {
	case rolledOut.IsZero():
		t.Error("etcd-druid's Deployment was never seen running v0.39.2 in every replica after it was resumed")
	case !asked.After(rolledOut):
		t.Errorf("etcd-druid was asked at %s, before its Deployment ran v0.39.2 in every replica at %s", asked.Format(time.RFC3339Nano), rolledOut.Format(time.RFC3339Nano))
	default:
		t.Logf("etcd-druid's Deployment ran v0.39.2 in every replica %s after it was resumed, and the request came %s after that", rolledOut.Sub(c.resumed).Round(time.Second), asked.Sub(rolledOut).Round(time.Millisecond))
	}
}

func short(revision string) string {
	version, digest, found := strings.Cut(revision, "+")
	if !found || len(digest) < 8 {
		return revision
	}
	return version + "+" + digest[:8]
}

func (h *druidHistory) log(t *testing.T) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	stages := slices.Clone(h.marks)
	for i, s := range h.etcd {
		if i == 0 || s.requested != h.etcd[i-1].requested || s.operation != h.etcd[i-1].operation || s.run != h.etcd[i-1].run || s.lastOp != h.etcd[i-1].lastOp || s.generation != h.etcd[i-1].generation || s.stamp != h.etcd[i-1].stamp {
			stages = append(stages, stage{at: s.at, what: fmt.Sprintf("Etcd generation %d stamped %s, request %q, %s %t, lastOperation %s under %s", s.generation, short(s.stamp), shortRequest(s.requested), druidv1alpha1.DruidOperationAnnotation, s.operation, s.lastOp, s.run)})
		}
	}
	for i, s := range h.statefulSet {
		if i == 0 || s.updateRevision != h.statefulSet[i-1].updateRevision || s.generation != h.statefulSet[i-1].generation || s.ready != h.statefulSet[i-1].ready {
			stages = append(stages, stage{at: s.at, what: fmt.Sprintf("StatefulSet generation %d, updateRevision %s, %d ready", s.generation, s.updateRevision, s.ready)})
		}
	}
	for i, s := range h.deployment {
		if i == 0 || s != h.deployment[i-1] && (s.paused != h.deployment[i-1].paused || s.image != h.deployment[i-1].image || s.updated != h.deployment[i-1].updated || s.rolledOut != h.deployment[i-1].rolledOut) {
			stages = append(stages, stage{at: s.at, what: fmt.Sprintf("Deployment etcd-druid asks for %s, paused %t, %d updated, rolled out to v0.39.2 %t", s.image, s.paused, s.updated, s.rolledOut)})
		}
	}
	for i, s := range h.pod {
		if i == 0 || s.uid != h.pod[i-1].uid || s.images != h.pod[i-1].images || s.ready != h.pod[i-1].ready {
			stages = append(stages, stage{at: s.at, what: fmt.Sprintf("member pod %s runs %s, ready %t", s.uid, s.images, s.ready)})
		}
	}
	for i, s := range h.tenant {
		if i == 0 || s.ready != h.tenant[i-1].ready || s.progressing != h.tenant[i-1].progressing || s.target != h.tenant[i-1].target || s.datastore != h.tenant[i-1].datastore {
			stages = append(stages, stage{at: s.at, what: fmt.Sprintf("tenant Ready=%s, Progressing %s, target %s, etcd at %s", s.ready, s.progressing, short(s.target), short(s.datastore))})
		}
	}
	for _, line := range timeline(h.created, stages) {
		t.Log(line)
	}
}

func shortRequest(request string) string {
	asked, run, found := strings.Cut(request, "/")
	if !found {
		return request
	}
	revision, runID, _ := strings.Cut(run, "/")
	return asked + "/" + short(revision) + "/" + runID
}
