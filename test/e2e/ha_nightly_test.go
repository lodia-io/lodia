// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && ha

package e2e

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestHighAvailabilityRemovedAndRestored(t *testing.T) {
	var (
		k8s     client.WithWatch
		history *recorder
	)
	const class = "e2e-ha-toggle"
	key := client.ObjectKey{Namespace: "team-ha-toggle", Name: controlPlane}
	zone := &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"}
	f := features.New("nightly on kind: highAvailability removed from a formed tenant and added back").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			createTenant(ctx, t, k8s, key, class, func(spec *lodiav1alpha1.ControlPlaneSpec) { spec.HighAvailability = zone })
			return ctx
		}).
		Assess("the tenant turns Ready with three etcd members and two replicas of each component", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 8*time.Minute, "three members and two replicas each, Ready", func(ctx context.Context) error {
				return shaped(ctx, k8s, key, 3, 2)
			})
			history = record(ctx, t, k8s, key)
			return ctx
		}).
		Assess("removing highAvailability scales etcd in to one member and each component to one replica, and Ready is back True", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			start := time.Now()
			setHighAvailability(ctx, t, k8s, key, nil)
			until(ctx, t, 10*time.Minute, "one member and one replica each, Ready", func(ctx context.Context) error {
				return shaped(ctx, k8s, key, 1, 1)
			})
			explainEtcd(ctx, t, k8s, key, start)
			history.log(t, start, time.Now())
			return ctx
		}).
		Assess("adding it back scales etcd out to three members and each component to two replicas, availability never leaving True", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			start := time.Now()
			stop := watchPods(ctx, t, k8s, kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig()), key)
			setHighAvailability(ctx, t, k8s, key, zone)
			until(ctx, t, 10*time.Minute, "three members and two replicas each, Ready", func(ctx context.Context) error {
				return shaped(ctx, k8s, key, 3, 2)
			})
			stop()
			explainEtcd(ctx, t, k8s, key, start)
			history.log(t, start, time.Now())
			history.waitsOnly(t, start, time.Now(), gateBound, lodiav1alpha1.ConditionReady, lodiav1alpha1.ConditionAPIServerReady, lodiav1alpha1.ConditionDatastoreReady)
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if history != nil {
				history.stop()
			}
			closeTenant(ctx, t, k8s, key, class)
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func setHighAvailability(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey, ha *lodiav1alpha1.HighAvailability) {
	t.Helper()
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(cp.DeepCopy())
	cp.Spec.HighAvailability = ha
	if err := k8s.Patch(ctx, cp, patch); err != nil {
		t.Fatal(err)
	}
	t.Logf("highAvailability is %+v at generation %d", ha, cp.Generation)
}

func shaped(ctx context.Context, k8s client.Client, key client.ObjectKey, members, replicas int32) error {
	if err := controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{}); err != nil {
		return err
	}
	etcd := &druidv1alpha1.Etcd{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: lodiav1alpha1.EtcdName(key.Name)}, etcd); err != nil {
		return err
	}
	if etcd.Spec.Replicas != members || ptr.Deref(etcd.Status.ObservedGeneration, 0) != etcd.Generation || etcd.Status.ReadyReplicas != members || len(etcd.Status.Members) != int(members) || !druidv1alpha1.HasScaleOperationCompleted(etcd) {
		return fmt.Errorf("Etcd %s asks for %d members, generation %d observed %d, %d ready, members %+v, conditions %+v", etcd.Name, etcd.Spec.Replicas, etcd.Generation, ptr.Deref(etcd.Status.ObservedGeneration, 0), etcd.Status.ReadyReplicas, etcd.Status.Members, etcd.Status.Conditions)
	}
	sts := &appsv1.StatefulSet{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: etcd.Name}, sts); err != nil {
		return err
	}
	if s := sts.Status; ptr.Deref(sts.Spec.Replicas, 0) != members || s.ObservedGeneration != sts.Generation || s.CurrentRevision != s.UpdateRevision || s.UpdatedReplicas != members || s.ReadyReplicas != members {
		return fmt.Errorf("StatefulSet %s asks for %d, generation %d observed %d, revision %s of %s, %d updated, %d ready", sts.Name, ptr.Deref(sts.Spec.Replicas, 0), sts.Generation, s.ObservedGeneration, s.CurrentRevision, s.UpdateRevision, s.UpdatedReplicas, s.ReadyReplicas)
	}
	for _, component := range []string{lodiav1alpha1.APIServerComponent, "kube-controller-manager", "kube-scheduler"} {
		name := key.Name + "-" + component
		d := &appsv1.Deployment{}
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: name}, d); err != nil {
			return err
		}
		if s := d.Status; ptr.Deref(d.Spec.Replicas, 1) != replicas || s.Replicas != replicas || s.AvailableReplicas != replicas {
			return fmt.Errorf("Deployment %s asks for %d replicas, runs %d, %d available", name, ptr.Deref(d.Spec.Replicas, 1), s.Replicas, s.AvailableReplicas)
		}
		err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: name}, &policyv1.PodDisruptionBudget{})
		switch {
		case replicas > 1 && err != nil:
			return fmt.Errorf("PodDisruptionBudget %s: %w", name, err)
		case replicas == 1 && !apierrors.IsNotFound(err):
			return fmt.Errorf("PodDisruptionBudget %s: %w", name, gone(err))
		}
	}
	return nil
}

func explainEtcd(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey, since time.Time) {
	t.Helper()
	name := lodiav1alpha1.EtcdName(key.Name)
	sts := &appsv1.StatefulSet{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: name}, sts); err != nil {
		t.Log(err)
	} else {
		t.Logf("StatefulSet %s: current revision %s, update revision %s, %d of %d ready", name, sts.Status.CurrentRevision, sts.Status.UpdateRevision, sts.Status.ReadyReplicas, ptr.Deref(sts.Spec.Replicas, 0))
	}
	pods := &corev1.PodList{}
	if err := k8s.List(ctx, pods, client.InNamespace(key.Namespace), client.MatchingLabels{lodiav1alpha1.ControlPlaneLabel: key.Name, lodiav1alpha1.ComponentLabel: name}); err != nil {
		t.Log(err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		var restarts int32
		for _, c := range pod.Status.ContainerStatuses {
			restarts += c.RestartCount
		}
		t.Logf("pod %s: UID %s, created %s, revision %s, %d restarts, ready: %v", pod.Name, pod.UID, pod.CreationTimestamp.UTC().Format(time.RFC3339), pod.Labels[appsv1.StatefulSetRevisionLabel], restarts, running(pod))
	}
	events := &corev1.EventList{}
	if err := k8s.List(ctx, events, client.InNamespace(key.Namespace)); err != nil {
		t.Log(err)
	}
	at := func(e corev1.Event) time.Time {
		return slices.MaxFunc([]time.Time{e.LastTimestamp.Time, e.EventTime.Time, e.FirstTimestamp.Time}, func(a, b time.Time) int { return a.Compare(b) })
	}
	slices.SortFunc(events.Items, func(a, b corev1.Event) int { return at(a).Compare(at(b)) })
	for _, e := range events.Items {
		if at(e).Before(since) || !strings.HasPrefix(e.InvolvedObject.Name, key.Name+"-") {
			continue
		}
		t.Logf("event %s %s %s: %s %s", at(e).UTC().Format(time.RFC3339), e.InvolvedObject.Kind, e.InvolvedObject.Name, e.Reason, e.Message)
	}
}

func watchPods(ctx context.Context, t *testing.T, k8s client.WithWatch, logs kubernetes.Interface, key client.ObjectKey) func() {
	t.Helper()
	pods := &corev1.PodList{}
	selector := client.MatchingLabels{lodiav1alpha1.ControlPlaneLabel: key.Name}
	if err := k8s.List(ctx, pods, client.InNamespace(key.Namespace), selector); err != nil {
		t.Fatal(err)
	}
	lw := &cache.ListWatch{WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
		return k8s.Watch(ctx, &corev1.PodList{}, client.InNamespace(key.Namespace), selector, &client.ListOptions{Raw: &options})
	}}
	watching, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w, err := watchtools.NewRetryWatcherWithContext(watching, pods.ResourceVersion, lw)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	seen := map[string]int32{}
	for _, pod := range pods.Items {
		for _, c := range pod.Status.ContainerStatuses {
			seen[pod.Name+"/"+c.Name] = c.RestartCount
		}
	}
	var mu sync.Mutex
	var lines []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range w.ResultChan() {
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			var restarts int32
			var states, previous []string
			for _, c := range pod.Status.ContainerStatuses {
				restarts += c.RestartCount
				if container := pod.Name + "/" + c.Name; c.RestartCount > seen[container] {
					seen[container] = c.RestartCount
					previous = append(previous, previousLog(watching, logs, pod, c)...)
				}
				switch {
				case c.State.Waiting != nil:
					states = append(states, c.Name+" waiting "+c.State.Waiting.Reason)
				case c.State.Terminated != nil:
					states = append(states, fmt.Sprintf("%s terminated %s %d", c.Name, c.State.Terminated.Reason, c.State.Terminated.ExitCode))
				}
				if c.LastTerminationState.Terminated != nil {
					states = append(states, fmt.Sprintf("%s last terminated %s %d", c.Name, c.LastTerminationState.Terminated.Reason, c.LastTerminationState.Terminated.ExitCode))
				}
			}
			ready := slices.ContainsFunc(pod.Status.Conditions, func(c corev1.PodCondition) bool {
				return c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue
			})
			mu.Lock()
			lines = append(lines, fmt.Sprintf("%s pod %s %s on %s: %s, ready %t, deleting %t, %d restarts %v",
				time.Now().UTC().Format(time.RFC3339Nano), event.Type, pod.Name, pod.Spec.NodeName, pod.Status.Phase, ready, !pod.DeletionTimestamp.IsZero(), restarts, states))
			lines = append(lines, previous...)
			mu.Unlock()
		}
	}()
	return func() {
		cancel()
		w.Stop()
		<-done
		mu.Lock()
		defer mu.Unlock()
		for _, line := range lines {
			t.Log(line)
		}
	}
}

func previousLog(ctx context.Context, logs kubernetes.Interface, pod *corev1.Pod, c corev1.ContainerStatus) []string {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	raw, err := logs.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: c.Name, Previous: true, TailLines: new(int64(20))}).DoRaw(ctx)
	if err != nil {
		return []string{fmt.Sprintf("pod %s container %s, the log before restart %d: %v", pod.Name, c.Name, c.RestartCount, err)}
	}
	var out []string
	for line := range strings.Lines(string(raw)) {
		out = append(out, fmt.Sprintf("pod %s container %s, before restart %d: %s", pod.Name, c.Name, c.RestartCount, strings.TrimRight(line, "\n")))
	}
	return out
}
