// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
	"sigs.k8s.io/e2e-framework/third_party/helm"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
	"github.com/segla-io/brygga/internal/controller/datastore"
	"github.com/segla-io/brygga/internal/controller/datastore/datastoretest"
)

const (
	druidVersion   = "v0.39.0"
	druidNamespace = "etcd-druid"
	tenant         = "team-e2e"
	controlPlane   = "dev"
)

var druidValues = filepath.Join("..", "..", "hack", "etcd-spike", "druid-values.yaml")

func TestDatastoreOnKind(t *testing.T) {
	var (
		k8s     client.Client
		stop    context.CancelFunc
		stopped chan error
	)
	f := features.New("the Datastore controller with etcd-druid "+druidVersion).
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			installEtcdDruid(t, cfg.KubeconfigFile())
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, bryggav1alpha1.AddToScheme, druidv1alpha1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			if k8s, err = client.New(cfg.Client().RESTConfig(), client.Options{Scheme: scheme}); err != nil {
				t.Fatal(err)
			}
			mgr := manager(t, cfg.Client().RESTConfig(), scheme)
			var run context.Context
			run, stop = context.WithCancel(context.Background())
			stopped = make(chan error, 1)
			go func() { stopped <- mgr.Start(run) }()

			create(ctx, t, k8s, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: tenant}})
			create(ctx, t, k8s, &bryggav1alpha1.DatastoreClass{
				ObjectMeta: metav1.ObjectMeta{Name: "e2e"},
				Spec:       bryggav1alpha1.DatastoreClassSpec{Storage: bryggav1alpha1.EtcdStorage{Size: resource.MustParse("1Gi"), ReclaimPolicy: "Delete"}},
			})
			writeGeneration(ctx, t, k8s, 1)
			create(ctx, t, k8s, &bryggav1alpha1.ControlPlane{
				ObjectMeta: metav1.ObjectMeta{Name: controlPlane, Namespace: tenant},
				Spec: bryggav1alpha1.ControlPlaneSpec{
					Version:   "v1.37.1",
					Datastore: bryggav1alpha1.DatastoreChoice{Dedicated: &bryggav1alpha1.DedicatedDatastore{ClassName: "e2e"}},
					Endpoint:  bryggav1alpha1.Endpoint{LoadBalancer: &bryggav1alpha1.LoadBalancerEndpoint{}},
					Network:   bryggav1alpha1.Network{ServiceCIDRs: []bryggav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []bryggav1alpha1.CIDR{"10.244.0.0/16"}},
				},
			})
			return ctx
		}).
		Assess("DatastoreReady=True with the member running", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 5*time.Minute, "DatastoreReady=True for generation 1", func(ctx context.Context) error {
				return conditionIs(ctx, k8s, metav1.ConditionTrue, datastore.ReasonReady)
			})
			pod := &corev1.Pod{}
			if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd-0"}, pod); err != nil {
				t.Fatal(err)
			}
			if err := running(pod); err != nil {
				t.Fatal(err)
			}
			if err := mountsGeneration(pod, 1); err != nil {
				t.Fatal(err)
			}
			t.Logf("pod %s is %s and mounts %s", pod.Name, pod.Status.Phase, strings.Join(secretVolumes(pod), ", "))
			logEtcd(ctx, t, k8s)
			return ctx
		}).
		Assess("generation 2 rolls the member and DatastoreReady returns to True", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			writeGeneration(ctx, t, k8s, 2)
			until(ctx, t, 2*time.Minute, "the Etcd names generation 2 in clientUrlTls, peerUrlTls and backup.tls", func(ctx context.Context) error {
				etcd := &druidv1alpha1.Etcd{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd"}, etcd); err != nil {
					return err
				}
				for _, ref := range []string{
					etcd.Spec.Etcd.ClientUrlTLS.TLSCASecretRef.Name, etcd.Spec.Etcd.ClientUrlTLS.ServerTLSSecretRef.Name, etcd.Spec.Etcd.ClientUrlTLS.ClientTLSSecretRef.Name,
					etcd.Spec.Etcd.PeerUrlTLS.TLSCASecretRef.Name, etcd.Spec.Etcd.PeerUrlTLS.ServerTLSSecretRef.Name,
					etcd.Spec.Backup.TLS.TLSCASecretRef.Name, etcd.Spec.Backup.TLS.ServerTLSSecretRef.Name, etcd.Spec.Backup.TLS.ClientTLSSecretRef.Name,
				} {
					if !strings.HasSuffix(ref, "-2") {
						return fmt.Errorf("%s is not of generation 2", ref)
					}
				}
				return nil
			})
			until(ctx, t, 2*time.Minute, "DatastoreReady leaves True for the roll", func(ctx context.Context) error {
				return conditionIs(ctx, k8s, metav1.ConditionFalse, datastore.ReasonProvisioning)
			})
			until(ctx, t, 5*time.Minute, "the member runs with generation 2 mounted", func(ctx context.Context) error {
				pod := &corev1.Pod{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd-0"}, pod); err != nil {
					return err
				}
				if err := mountsGeneration(pod, 2); err != nil {
					return err
				}
				return running(pod)
			})
			until(ctx, t, 3*time.Minute, "DatastoreReady=True for generation 2", func(ctx context.Context) error {
				etcd := &druidv1alpha1.Etcd{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd"}, etcd); err != nil {
					return err
				}
				if ptr.Deref(etcd.Status.ObservedGeneration, 0) != etcd.Generation {
					return fmt.Errorf("etcd-druid observed generation %d of %d", ptr.Deref(etcd.Status.ObservedGeneration, 0), etcd.Generation)
				}
				return conditionIs(ctx, k8s, metav1.ConditionTrue, datastore.ReasonReady)
			})
			logEtcd(ctx, t, k8s)
			return ctx
		}).
		Assess("deleting the ControlPlane removes the Etcd and the member's claim", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			cp := &bryggav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: controlPlane, Namespace: tenant}}
			if err := k8s.Delete(ctx, cp); err != nil {
				t.Fatal(err)
			}
			until(ctx, t, 3*time.Minute, "the ControlPlane is gone", func(ctx context.Context) error {
				return gone(k8s.Get(ctx, client.ObjectKeyFromObject(cp), &bryggav1alpha1.ControlPlane{}))
			})
			if err := gone(k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd"}, &druidv1alpha1.Etcd{})); err != nil {
				t.Errorf("the Etcd: %v", err)
			}
			until(ctx, t, 2*time.Minute, "the member's claim is gone", func(ctx context.Context) error {
				return gone(k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd-" + controlPlane + "-etcd-0"}, &corev1.PersistentVolumeClaim{}))
			})
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if stop != nil {
				stop()
				if err := <-stopped; err != nil {
					t.Error(err)
				}
			}
			if k8s != nil {
				for _, obj := range []client.Object{
					&bryggav1alpha1.DatastoreClass{ObjectMeta: metav1.ObjectMeta{Name: "e2e"}},
					&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: tenant}},
				} {
					if err := client.IgnoreNotFound(k8s.Delete(ctx, obj)); err != nil {
						t.Error(err)
					}
				}
			}
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func installEtcdDruid(t *testing.T, kubeconfig string) {
	t.Helper()
	dir := t.TempDir()
	clone := exec.CommandContext(t.Context(), "git", "clone", "--quiet", "--depth", "1", "--branch", druidVersion, "https://github.com/gardener/etcd-druid", dir)
	if out, err := clone.CombinedOutput(); err != nil {
		t.Fatalf("clone etcd-druid %s: %v\n%s", druidVersion, err, out)
	}
	values, err := filepath.Abs(druidValues)
	if err != nil {
		t.Fatal(err)
	}
	err = helm.New(kubeconfig).RunUpgrade(
		helm.WithName("etcd-druid"), helm.WithChart(filepath.Join(dir, "charts")), helm.WithNamespace(druidNamespace),
		helm.WithArgs("--install", "--create-namespace", "--values", values), helm.WithWait(), helm.WithTimeout("5m"))
	if err != nil {
		t.Fatalf("install etcd-druid %s: %v", druidVersion, err)
	}
	t.Logf("etcd-druid %s installed from its chart into namespace %s", druidVersion, druidNamespace)
}

func manager(t *testing.T, cfg *rest.Config, scheme *runtime.Scheme) ctrl.Manager {
	t.Helper()
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	labelled, err := labels.NewRequirement(bryggav1alpha1.ControlPlaneLabel, selection.Exists, nil)
	if err != nil {
		t.Fatal(err)
	}
	everything := cache.ByObject{Label: labels.Everything()}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			DefaultLabelSelector:        labels.NewSelector().Add(*labelled),
			DefaultTransform:            cache.TransformStripManagedFields(),
			ReaderFailOnMissingInformer: true,
			ByObject: map[client.Object]cache.ByObject{
				&bryggav1alpha1.ControlPlane{}:   everything,
				&bryggav1alpha1.DatastoreClass{}: everything,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (&datastore.Reconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader()}).SetupWithManager(mgr, 1); err != nil {
		t.Fatal(err)
	}
	return mgr
}

func create(ctx context.Context, t *testing.T, k8s client.Client, obj client.Object) {
	t.Helper()
	if err := k8s.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
}

func writeGeneration(ctx context.Context, t *testing.T, k8s client.Client, generation int) {
	t.Helper()
	secrets, err := datastoretest.Generation(tenant, controlPlane, generation)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range secrets {
		create(ctx, t, k8s, secret)
	}
	t.Logf("generation %d of the etcd certificates written: %d Secrets labelled %s=%d", generation, len(secrets), datastore.CertificatesLabel, generation)
}

func until(ctx context.Context, t *testing.T, timeout time.Duration, what string, check func(context.Context) error) {
	t.Helper()
	start := time.Now()
	var last error
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		last = check(ctx)
		return last == nil, nil
	})
	if err != nil {
		t.Fatalf("%s: %v: %v", what, err, last)
	}
	t.Logf("%s, after %s", what, time.Since(start).Round(time.Second))
}

func conditionIs(ctx context.Context, k8s client.Client, status metav1.ConditionStatus, reason string) error {
	cp := &bryggav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane}, cp); err != nil {
		return err
	}
	condition := meta.FindStatusCondition(cp.Status.Conditions, datastore.ConditionType)
	if condition == nil {
		return fmt.Errorf("no %s condition yet", datastore.ConditionType)
	}
	if condition.Status != status || condition.Reason != reason || condition.ObservedGeneration != cp.Generation {
		return fmt.Errorf("%s=%s (%s, generation %d of %d): %s", condition.Type, condition.Status, condition.Reason, condition.ObservedGeneration, cp.Generation, condition.Message)
	}
	return nil
}

func running(pod *corev1.Pod) error {
	if pod.Status.Phase != corev1.PodRunning {
		return fmt.Errorf("pod %s is %s", pod.Name, pod.Status.Phase)
	}
	for _, container := range pod.Status.ContainerStatuses {
		if !container.Ready {
			return fmt.Errorf("container %s of pod %s is not ready", container.Name, pod.Name)
		}
	}
	return nil
}

func secretVolumes(pod *corev1.Pod) []string {
	var names []string
	for _, volume := range pod.Spec.Volumes {
		if volume.Secret != nil {
			names = append(names, volume.Secret.SecretName)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func mountsGeneration(pod *corev1.Pod, generation int) error {
	mounted := secretVolumes(pod)
	for _, certificate := range datastore.Certificates {
		if name := datastore.SecretName(controlPlane, certificate, generation); !slices.Contains(mounted, name) {
			return fmt.Errorf("pod %s does not mount %s; it mounts %s", pod.Name, name, strings.Join(mounted, ", "))
		}
	}
	return nil
}

func gone(err error) error {
	switch {
	case err == nil:
		return fmt.Errorf("still there")
	case apierrors.IsNotFound(err):
		return nil
	default:
		return err
	}
}

func logEtcd(ctx context.Context, t *testing.T, k8s client.Client) {
	t.Helper()
	etcd := &druidv1alpha1.Etcd{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: tenant, Name: controlPlane + "-etcd"}, etcd); err != nil {
		t.Fatal(err)
	}
	var members []string
	for _, member := range etcd.Status.Members {
		members = append(members, fmt.Sprintf("%s %s %s", member.Name, ptr.Deref(member.Role, ""), member.Status))
	}
	t.Logf("Etcd %s generation %d: observedGeneration %d, ready %t, %d of %d replicas ready, members [%s], server certificate %s",
		etcd.Name, etcd.Generation, ptr.Deref(etcd.Status.ObservedGeneration, 0), ptr.Deref(etcd.Status.Ready, false),
		etcd.Status.ReadyReplicas, etcd.Spec.Replicas, strings.Join(members, "; "), etcd.Spec.Etcd.ClientUrlTLS.ServerTLSSecretRef.Name)
}
