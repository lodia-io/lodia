// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && upgrade

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/distribution/reference"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/versions"
)

const (
	upgradeTenant = "team-upgrade"
	upgradeClass  = "e2e-upgrade"
	upgradeFrom   = "v1.36.4"
	upgradeTo     = "v1.37.1"
	nodeImage136  = "kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed"
	readyBackends = "konnectivity_network_proxy_server_ready_backend_connections"
)

func TestMinorUpgradeGate(t *testing.T) {
	var (
		k8s     client.WithWatch
		access  *tenantAccess
		loop    reference.NamedTagged
		node    string
		web     *corev1.Pod
		loops   []apiLoopRun
		before  []loopCounts
		history *recorder
		upgrade *watchedUpgrade
		start   time.Time
	)
	key := client.ObjectKey{Namespace: upgradeTenant, Name: controlPlane}
	f := features.New("gate G1 on kind: a minor upgrade under two loops").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			startStore(ctx, t, k8s)
			createStoreTenant(ctx, t, k8s, key, upgradeClass, upgradeFrom)
			image, err := build(ctx, cfg.KubeconfigFile(), "./test/"+apiLoop)
			if err != nil {
				t.Fatal(err)
			}
			loop = image
			nodeImage = nodeImage136
			return ctx
		}).
		Assess("the tenant is Ready at the older minor, and a node of that minor joins and runs a pod", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 5*time.Minute, "Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{})
			})
			until(ctx, t, 2*time.Minute, "status.kubernetes.version is the older version", func(ctx context.Context) error {
				cp := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, key, cp); err != nil {
					return err
				}
				if k := cp.Status.Kubernetes; k == nil || k.Version != upgradeFrom || k.Upgrade != nil {
					return fmt.Errorf("status.kubernetes is %+v", k)
				}
				return nil
			})
			access = openTenant(ctx, t, k8s, key)
			node = joinNodes(ctx, t, cfg, k8s, key, access, 1)[0]
			until(ctx, t, 3*time.Minute, "CoreDNS is available", func(ctx context.Context) error {
				return coreDNSAvailable(ctx, access.client)
			})
			web = runWeb(ctx, t, access, node)
			return ctx
		}).
		Assess("the loops run from before the edit", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			loops = startLoops(ctx, t, cfg, k8s, access, key, node, loop)
			before, _ = readLoops(ctx, t, "both loops answer on new and reused connections", loops)
			history = record(ctx, t, k8s, key)
			upgrade = watchUpgrade(ctx, t, k8s, key)
			return ctx
		}).
		Assess("the edit takes the upgrade through its preflight and a Backup that completes before the target moves", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			start = time.Now()
			setVersionTo(ctx, t, k8s, key, upgradeTo)
			backup := expectBackupCompleted(ctx, t, k8s, key, "1.37", upgradeFrom, upgradeTo, upgrade)
			t.Logf("Backup %s: %s", backup.Name, meta.FindStatusCondition(backup.Status.Conditions, lodiav1alpha1.ConditionReady).Message)
			return ctx
		}).
		Assess("the target moves, and logs and exec reach the pod during the skew", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 10*time.Minute, "the API server reports the new minor while the addons still run the old one", func(ctx context.Context) error {
				cp := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, key, cp); err != nil {
					return err
				}
				if c := cp.Status.Components; c == nil || c.APIServer == nil || c.APIServer.Version != upgradeTo {
					return fmt.Errorf("status.components is %+v", cp.Status.Components)
				}
				if a := cp.Status.Addons; a == nil || a.Version == upgradeTo {
					return fmt.Errorf("status.addons is %+v, want the older version still", a)
				}
				return nil
			})
			metrics, stop := forward(t, cfg.Client().RESTConfig(), apiServerPod(ctx, t, k8s, key), konnectivityMetricsPort)
			defer stop()
			until(ctx, t, 2*time.Minute, "the new konnectivity-server has an agent", func(ctx context.Context) error {
				if n := readGauge(ctx, t, metrics, readyBackends); n == 0 {
					return fmt.Errorf("%s is 0", readyBackends)
				}
				return nil
			})
			expectLogsAndExec(ctx, t, access, web)
			t.Logf("after logs and exec the addons report %s", getControlPlane(ctx, t, k8s, key).Status.Addons.Version)
			return ctx
		}).
		Assess("the upgrade ends with every part at the new version, the older node kept, and neither loop lost a request", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 10*time.Minute, "the upgrade ends", func(ctx context.Context) error {
				return upgradeEnded(ctx, k8s, key, upgradeTo)
			})
			end := time.Now()
			history.log(t, start, end)
			after, logs := readLoopsPast(ctx, t, end, loops)
			for i, l := range loops {
				checkLoop(t, l.where, before[i], after[i], logs[i])
			}
			stopLoops(ctx, t, loops)
			upgrade.check(t)
			kept := &corev1.Node{}
			if err := access.client.Get(ctx, client.ObjectKey{Name: node}, kept); err != nil {
				t.Fatal(err)
			}
			if got := kept.Status.NodeInfo.KubeletVersion; got != upgradeFrom {
				t.Errorf("node %s runs kubelet %s, want it kept at %s", node, got, upgradeFrom)
			}
			if proxy := daemonSetImage(ctx, t, access, "kube-proxy"); !strings.HasSuffix(proxy, ":"+upgradeTo) {
				t.Errorf("the tenant's kube-proxy runs %s, want %s", proxy, upgradeTo)
			}
			until(ctx, t, time.Minute, "status.nodes counts the older kubelet as outdated", func(ctx context.Context) error {
				cp := &lodiav1alpha1.ControlPlane{}
				if err := k8s.Get(ctx, key, cp); err != nil {
					return err
				}
				if n := cp.Status.Nodes; n == nil || n.OutdatedKubelets != 1 || n.OldestKubelet != upgradeFrom {
					return fmt.Errorf("status.nodes is %+v", n)
				}
				return nil
			})
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if history != nil {
				history.stop()
			}
			if upgrade != nil {
				upgrade.stop()
			}
			closeStoreTenant(ctx, t, k8s, key, upgradeClass)
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func createStoreTenant(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey, class, kubernetes string, change ...func(*lodiav1alpha1.ControlPlaneSpec)) {
	t.Helper()
	create(ctx, t, k8s, &lodiav1alpha1.DatastoreClass{
		ObjectMeta: metav1.ObjectMeta{Name: class},
		Spec: lodiav1alpha1.DatastoreClassSpec{
			Storage: lodiav1alpha1.EtcdStorage{Size: resource.MustParse("8Gi"), ReclaimPolicy: "Retain"},
			Backup: &lodiav1alpha1.EtcdBackup{
				Store: lodiav1alpha1.BackupStore{
					Provider: "S3", Bucket: s3Bucket, Prefix: "e2e", Endpoint: s3Endpoint,
					SecretRef: lodiav1alpha1.SecretReference{Name: s3Secret, Namespace: operatorNamespace},
				},
				DeltaSnapshotPeriod: &metav1.Duration{Duration: 10 * time.Second},
			},
		},
	})
	create(ctx, t, k8s, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: key.Namespace}})
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   kubernetes,
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{ClassName: class}},
			Endpoint:  lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}},
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.32.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{gatePodCIDR}},
			Addons:    &lodiav1alpha1.Addons{KubeProxy: &lodiav1alpha1.KubeProxy{Conntrack: &lodiav1alpha1.KubeProxyConntrack{MaxPerCore: new(int32(0))}}},
		},
	}
	for _, c := range change {
		c(&cp.Spec)
	}
	create(ctx, t, k8s, cp)
}

func closeStoreTenant(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey, class string) {
	t.Helper()
	closeTenant(ctx, t, k8s, key, class)
	if k8s == nil {
		return
	}
	for _, obj := range []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: s3Namespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: s3Secret}},
	} {
		if err := client.IgnoreNotFound(k8s.Delete(ctx, obj)); err != nil {
			t.Error(err)
		}
	}
}

func setVersionTo(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey, to string) {
	t.Helper()
	cp := getControlPlane(ctx, t, k8s, key)
	patch := client.MergeFrom(cp.DeepCopy())
	cp.Spec.Version = to
	if err := k8s.Patch(ctx, cp, patch); err != nil {
		t.Fatal(err)
	}
	t.Logf("spec.version is %s at generation %d", to, cp.Generation)
}

func getControlPlane(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) *lodiav1alpha1.ControlPlane {
	t.Helper()
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func expectBackupCompleted(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey, minor, from, to string, upgrade *watchedUpgrade) *lodiav1alpha1.Backup {
	t.Helper()
	cp := getControlPlane(ctx, t, k8s, key)
	name := lodiav1alpha1.PreUpgradeBackupName(key.Name, minor, cp.UID)
	backup := &lodiav1alpha1.Backup{}
	until(ctx, t, 5*time.Minute, "Backup "+name+" is Completed", func(ctx context.Context) error {
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: name}, backup); err != nil {
			return err
		}
		if c := meta.FindStatusCondition(backup.Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Status != metav1.ConditionTrue || c.Reason != lodiav1alpha1.ReasonCompleted {
			return fmt.Errorf("Ready is %+v", c)
		}
		return nil
	})
	if len(backup.OwnerReferences) != 0 {
		t.Errorf("Backup %s has owners %+v", name, backup.OwnerReferences)
	}
	if backup.Status.KubernetesVersion != from {
		t.Errorf("Backup %s recorded version %q, want %s, what the source ran when it was sealed", name, backup.Status.KubernetesVersion, from)
	}
	until(ctx, t, time.Minute, "the target moves to "+to, func(ctx context.Context) error {
		cp := &lodiav1alpha1.ControlPlane{}
		if err := k8s.Get(ctx, key, cp); err != nil {
			return err
		}
		if cp.Status.Target == nil || cp.Status.Target.Version != to {
			return fmt.Errorf("status.target is %+v", cp.Status.Target)
		}
		return nil
	})
	if moved := upgrade.targetMoved(to); moved.IsZero() {
		t.Errorf("no status with target %s was observed", to)
	} else if backup.Status.CompletedAt == nil || moved.Before(backup.Status.CompletedAt.Add(-time.Second)) {
		t.Errorf("the target moved at %s, before the Backup completed at %v", moved, backup.Status.CompletedAt)
	}
	return backup
}

func runWeb(ctx context.Context, t *testing.T, access *tenantAccess, node string) *corev1.Pod {
	t.Helper()
	web := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "web"},
		Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{
			Name:  "web",
			Image: agnhost,
			Args:  []string{"netexec", "--http-port=8080"},
			Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
		}}},
	}
	create(ctx, t, access.client, web)
	until(ctx, t, 3*time.Minute, "pod web runs on node "+node, func(ctx context.Context) error {
		if err := access.client.Get(ctx, client.ObjectKeyFromObject(web), web); err != nil {
			return err
		}
		return running(web)
	})
	return web
}

func readLoopsPast(ctx context.Context, t *testing.T, end time.Time, loops []apiLoopRun) ([]loopCounts, []string) {
	t.Helper()
	counts, logs := make([]loopCounts, len(loops)), make([]string, len(loops))
	until(ctx, t, 2*time.Minute, "both loops report counts later than the end", func(ctx context.Context) error {
		for i, l := range loops {
			var err error
			if counts[i], logs[i], err = readLoop(ctx, l.api, l.pod); err != nil {
				return err
			}
			if !counts[i].Time.After(end) {
				return fmt.Errorf("pod %s/%s last reported its counts at %s, not later than the end at %s", l.pod.Namespace, l.pod.Name, counts[i].Time, end)
			}
		}
		return nil
	})
	return counts, logs
}

func expectLogsAndExec(ctx context.Context, t *testing.T, access *tenantAccess, web *corev1.Pod) {
	t.Helper()
	logs, err := kubernetes.NewForConfigOrDie(access.cfg).CoreV1().Pods(web.Namespace).GetLogs(web.Name, &corev1.PodLogOptions{}).DoRaw(ctx)
	if err != nil {
		t.Fatalf("logs of pod web during the skew: %v", err)
	}
	if !strings.Contains(string(logs), "Started HTTP server on port 8080") {
		t.Errorf("the logs of pod web lack agnhost's start line:\n%s", logs)
	}
	var stdout, stderr bytes.Buffer
	if err := access.resources.ExecInPod(ctx, web.Namespace, web.Name, "web", []string{"hostname"}, &stdout, &stderr); err != nil {
		t.Fatalf("exec in pod web during the skew: %v\n%s", err, stderr.String())
	}
	if hostname := strings.TrimSpace(stdout.String()); hostname != web.Name {
		t.Errorf("exec hostname in pod web printed %q", hostname)
	}
	t.Log("logs and exec reached pod web during the skew")
}

func readGauge(ctx context.Context, t *testing.T, address, name string) float64 {
	t.Helper()
	status, body := httpGet(ctx, t, http.DefaultClient, "http://"+address+"/metrics")
	if status != http.StatusOK {
		t.Fatalf("konnectivity-server's metrics answer %d:\n%s", status, body)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var gauge float64
	for _, m := range families[name].GetMetric() {
		gauge += m.GetGauge().GetValue()
	}
	return gauge
}

func daemonSetImage(ctx context.Context, t *testing.T, access *tenantAccess, name string) string {
	t.Helper()
	ds := &appsv1.DaemonSet{}
	if err := access.client.Get(ctx, client.ObjectKey{Namespace: metav1.NamespaceSystem, Name: name}, ds); err != nil {
		t.Fatal(err)
	}
	return ds.Spec.Template.Spec.Containers[0].Image
}

func upgradeEnded(ctx context.Context, k8s client.Client, key client.ObjectKey, version string) error {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		return err
	}
	if k := cp.Status.Kubernetes; k == nil || k.Version != version || k.Upgrade != nil {
		return fmt.Errorf("status.kubernetes is %+v", k)
	}
	if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionProgressing); c == nil || c.Status != metav1.ConditionFalse || c.Reason != lodiav1alpha1.ReasonIdle {
		return fmt.Errorf("Progressing is %+v", c)
	}
	if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady); c == nil || c.Status != metav1.ConditionTrue {
		return fmt.Errorf("Ready is %+v", c)
	}
	return nil
}

type watchedUpgrade struct {
	mu         sync.Mutex
	violations []string
	moved      map[string]time.Time
	images     map[string]string
	broken     []error
	stops      []func()
}

func watchUpgrade(ctx context.Context, t *testing.T, k8s client.WithWatch, key client.ObjectKey) *watchedUpgrade {
	t.Helper()
	u := &watchedUpgrade{moved: map[string]time.Time{}, images: map[string]string{}}
	u.stops = append(u.stops,
		watchList(ctx, t, k8s, &lodiav1alpha1.ControlPlaneList{}, key.Namespace, u.broke, func(_ context.Context, event watch.Event) {
			if cp, ok := event.Object.(*lodiav1alpha1.ControlPlane); ok && cp.Name == key.Name {
				u.observe(cp)
			}
		}),
		watchList(ctx, t, k8s, &appsv1.DeploymentList{}, key.Namespace, u.broke, func(ctx context.Context, event watch.Event) {
			if d, ok := event.Object.(*appsv1.Deployment); ok && d.Labels[lodiav1alpha1.ControlPlaneLabel] == key.Name {
				u.observeImage(ctx, k8s, key, d)
			}
		}),
	)
	return u
}

func (u *watchedUpgrade) broke(err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.broken = append(u.broken, err)
}

func (u *watchedUpgrade) observe(cp *lodiav1alpha1.ControlPlane) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if t := cp.Status.Target; t != nil {
		if _, seen := u.moved[t.Version]; !seen {
			u.moved[t.Version] = time.Now()
		}
	}
	c := cp.Status.Components
	if c == nil {
		return
	}
	reports := map[string]string{}
	for name, status := range map[string]*lodiav1alpha1.ComponentStatus{"kube-apiserver": c.APIServer, "kube-controller-manager": c.ControllerManager, "kube-scheduler": c.Scheduler} {
		if status != nil && status.Version != "" {
			reports[name] = status.Version
		}
	}
	if err := skewOK(reports); err != nil {
		u.violations = append(u.violations, fmt.Sprintf("status at resourceVersion %s: %v", cp.ResourceVersion, err))
	}
	if a := cp.Status.Addons; a != nil && a.Version != "" {
		var seen []string
		for _, v := range reports {
			seen = append(seen, v)
		}
		if oldest, ok := versions.Oldest(seen...); ok && newer(a.Version, oldest) {
			u.violations = append(u.violations, fmt.Sprintf("status at resourceVersion %s: the addons report %s, newer than the oldest component %s", cp.ResourceVersion, a.Version, oldest))
		}
	}
}

func (u *watchedUpgrade) observeImage(ctx context.Context, k8s client.Client, key client.ObjectKey, d *appsv1.Deployment) {
	component := d.Labels[lodiav1alpha1.ComponentLabel]
	i := slices.IndexFunc(d.Spec.Template.Spec.Containers, func(c corev1.Container) bool { return c.Name == component })
	if i < 0 {
		return
	}
	named, err := reference.ParseNormalizedNamed(d.Spec.Template.Spec.Containers[i].Image)
	if err != nil {
		return
	}
	tagged, ok := named.(reference.Tagged)
	if !ok {
		return
	}
	api := ""
	if component != lodiav1alpha1.APIServerComponent {
		cp := &lodiav1alpha1.ControlPlane{}
		if err := k8s.Get(ctx, key, cp); err != nil {
			u.broke(err)
			return
		}
		if c := cp.Status.Components; c != nil && c.APIServer != nil {
			api = c.APIServer.Version
		}
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.images[component] = tagged.Tag()
	if newer(tagged.Tag(), api) {
		u.violations = append(u.violations, fmt.Sprintf("Deployment %s at resourceVersion %s runs %s, newer than the API server's report %s", d.Name, d.ResourceVersion, tagged.Tag(), api))
	}
}

func (u *watchedUpgrade) targetMoved(to string) time.Time {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.moved[to]
}

func (u *watchedUpgrade) check(t *testing.T) {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.broken) > 0 {
		t.Errorf("a watch broke, so the record is incomplete: %v", u.broken)
	}
	if len(u.violations) > 0 {
		t.Errorf("the skew broke on %d observations: %s", len(u.violations), strings.Join(u.violations, "; "))
	}
	if len(u.moved) == 0 {
		t.Error("no status was observed")
	}
	for _, component := range []string{lodiav1alpha1.APIServerComponent, "kube-controller-manager", "kube-scheduler"} {
		if _, seen := u.images[component]; !seen {
			t.Errorf("no Deployment of %s was observed", component)
		}
	}
}

func (u *watchedUpgrade) stop() {
	for _, stop := range u.stops {
		stop()
	}
}

func skewOK(components map[string]string) error {
	api, ok := components["kube-apiserver"]
	if !ok {
		return nil
	}
	for _, name := range []string{"kube-controller-manager", "kube-scheduler"} {
		if v, ok := components[name]; ok && newer(v, api) {
			return fmt.Errorf("%s reports %s, newer than the API server's %s", name, v, api)
		}
	}
	return nil
}

func newer(a, b string) bool {
	va, errA := version.ParseSemantic(a)
	vb, errB := version.ParseSemantic(b)
	return errA == nil && errB == nil && vb.LessThan(va)
}

func watchList(ctx context.Context, t *testing.T, k8s client.WithWatch, list client.ObjectList, namespace string, broke func(error), on func(context.Context, watch.Event)) func() {
	t.Helper()
	initial := list.DeepCopyObject().(client.ObjectList)
	if err := k8s.List(ctx, initial, client.InNamespace(namespace)); err != nil {
		t.Fatal(err)
	}
	watching, cancel := context.WithCancel(context.WithoutCancel(ctx))
	if err := meta.EachListItem(initial, func(item runtime.Object) error {
		on(watching, watch.Event{Type: watch.Added, Object: item})
		return nil
	}); err != nil {
		cancel()
		t.Fatal(err)
	}
	lw := &cache.ListWatch{WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
		return k8s.Watch(ctx, list.DeepCopyObject().(client.ObjectList), &client.ListOptions{Namespace: namespace, Raw: &options})
	}}
	w, err := watchtools.NewRetryWatcherWithContext(watching, initial.GetResourceVersion(), lw)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range w.ResultChan() {
			if event.Type == watch.Error {
				broke(apierrors.FromObject(event.Object))
				continue
			}
			if event.Type == watch.Added || event.Type == watch.Modified {
				on(watching, event)
			}
		}
	}()
	return func() {
		cancel()
		w.Stop()
		<-done
	}
}
