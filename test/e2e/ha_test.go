// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && ha

package e2e

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/distribution/reference"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	haTenant     = "team-ha"
	haClass      = "e2e-ha"
	healthPeriod = 5 * time.Second
	settle       = 30 * time.Second
)

var readyAndInputs = []string{
	lodiav1alpha1.ConditionReady,
	lodiav1alpha1.ConditionCertificatesReady,
	lodiav1alpha1.ConditionDatastoreReady,
	lodiav1alpha1.ConditionAPIServerReady,
	lodiav1alpha1.ConditionAccessReady,
	lodiav1alpha1.ConditionAddonsApplied,
	lodiav1alpha1.ConditionReachable,
	lodiav1alpha1.ConditionControllersLeading,
}

func TestHighAvailabilityGate(t *testing.T) {
	var (
		k8s     client.WithWatch
		access  *tenantAccess
		loop    reference.NamedTagged
		loops   []apiLoopRun
		history *recorder
	)
	key := client.ObjectKey{Namespace: haTenant, Name: controlPlane}
	f := features.New("gate G2 on kind: an API server pod and the etcd leader's pod of a highly available control plane go away").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			createTenant(ctx, t, k8s, key, haClass, func(spec *lodiav1alpha1.ControlPlaneSpec) {
				spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"}
			})
			image, err := build(ctx, cfg.KubeconfigFile(), "./test/"+apiLoop)
			if err != nil {
				t.Fatal(err)
			}
			loop = image
			return ctx
		}).
		Assess("Ready, every component and etcd spread over the three zones, and a PodDisruptionBudget for each", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 5*time.Minute, "Ready=True", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{})
			})
			replicas := map[string]int{lodiav1alpha1.APIServerComponent: 2, "kube-controller-manager": 2, "kube-scheduler": 2, lodiav1alpha1.EtcdName(key.Name): 3}
			zones := map[string][]string{}
			until(ctx, t, 3*time.Minute, "every replica and every etcd member runs", func(ctx context.Context) error {
				for name, want := range replicas {
					var err error
					if zones[name], err = zonesOf(ctx, k8s, key, name); err != nil {
						return err
					}
					if len(zones[name]) != want {
						return fmt.Errorf("%d of the %d pods of %s run", len(zones[name]), want, name)
					}
				}
				return nil
			})
			for name, want := range replicas {
				t.Logf("the pods of %s run in the zones %v", name, zones[name])
				if distinct := slices.Compact(slices.Sorted(slices.Values(zones[name]))); len(distinct) != want || slices.Contains(distinct, "") {
					t.Errorf("the pods of %s run in the zones %v; want %d zones", name, zones[name], want)
				}
			}
			for _, component := range []string{lodiav1alpha1.APIServerComponent, "kube-controller-manager", "kube-scheduler"} {
				budget := &policyv1.PodDisruptionBudget{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: key.Name + "-" + component}, budget); err != nil {
					t.Fatal(err)
				}
				if s := budget.Spec; s.MaxUnavailable == nil || s.MaxUnavailable.String() != "1" || ptr.Deref(s.UnhealthyPodEvictionPolicy, "") != policyv1.AlwaysAllow {
					t.Errorf("the PodDisruptionBudget of %s is %+v", component, s)
				}
			}
			etcd := &policyv1.PodDisruptionBudget{}
			if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: lodiav1alpha1.EtcdName(key.Name)}, etcd); err != nil {
				t.Fatal(err)
			}
			if s := etcd.Spec; s.MinAvailable == nil || s.MinAvailable.String() != "2" || ptr.Deref(s.UnhealthyPodEvictionPolicy, "") != policyv1.AlwaysAllow {
				t.Errorf("etcd-druid's PodDisruptionBudget is %+v", s)
			}
			return ctx
		}).
		Assess("a node joins, and the loops outside and inside the tenant answer", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			access = openTenant(ctx, t, k8s, key)
			node := joinNodes(ctx, t, cfg, k8s, key, access, 1)[0]
			until(ctx, t, 3*time.Minute, "CoreDNS is available", func(ctx context.Context) error {
				return coreDNSAvailable(ctx, access.client)
			})
			loops = startLoops(ctx, t, cfg, k8s, access, key, node, loop)
			readLoops(ctx, t, "both loops answer on new and reused connections", loops)
			history = record(ctx, t, k8s, key)
			return ctx
		}).
		Assess("an API server pod deleted, Ready stays True and neither loop loses a request", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			before, _ := readLoops(ctx, t, "both loops' counts are read before the step", loops)
			pod := apiServerPod(ctx, t, k8s, key)
			start := time.Now()
			if err := k8s.Delete(ctx, pod); err != nil {
				t.Fatal(err)
			}
			t.Logf("pod %s deleted at %s", pod.Name, start.Format(time.RFC3339Nano))
			until(ctx, t, 4*time.Minute, "pod "+pod.Name+" is gone and two API server replicas are ready", func(ctx context.Context) error {
				if err := gone(k8s.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})); err != nil {
					return fmt.Errorf("pod %s: %w", pod.Name, err)
				}
				d := &appsv1.Deployment{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: key.Name + "-" + lodiav1alpha1.APIServerComponent}, d); err != nil {
					return err
				}
				if d.Status.ReadyReplicas != 2 || d.Status.AvailableReplicas != 2 {
					return fmt.Errorf("%d replicas ready, %d available", d.Status.ReadyReplicas, d.Status.AvailableReplicas)
				}
				return nil
			})
			time.Sleep(settle)
			history.check(t, start, time.Now())
			after, logs := readLoops(ctx, t, "both loops' counts are read after the step", loops)
			for i, l := range loops {
				checkLoop(t, l.where, before[i], after[i], logs[i])
			}
			return ctx
		}).
		Assess("the etcd leader's pod deleted, Ready stays True and neither loop loses a request", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			before, _ := readLoops(ctx, t, "both loops' counts are read before the step", loops)
			leader := etcdLeader(ctx, t, k8s, key)
			start := time.Now()
			if err := k8s.Delete(ctx, leader); err != nil {
				t.Fatal(err)
			}
			t.Logf("pod %s, the etcd leader, deleted at %s", leader.Name, start.Format(time.RFC3339Nano))
			until(ctx, t, 5*time.Minute, "pod "+leader.Name+" is replaced and runs", func(ctx context.Context) error {
				pod := &corev1.Pod{}
				if err := k8s.Get(ctx, client.ObjectKeyFromObject(leader), pod); err != nil {
					return err
				}
				if pod.UID == leader.UID {
					return fmt.Errorf("pod %s is the one deleted", pod.Name)
				}
				return running(pod)
			})
			t.Logf("pod %s is the etcd leader now", etcdLeader(ctx, t, k8s, key).Name)
			time.Sleep(settle)
			history.check(t, start, time.Now())
			after, logs := readLoops(ctx, t, "both loops' counts are read after the step", loops)
			for i, l := range loops {
				checkLoop(t, l.where, before[i], after[i], logs[i])
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if history != nil {
				history.stop()
			}
			closeTenant(ctx, t, k8s, key, haClass)
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func zonesOf(ctx context.Context, k8s client.Client, key client.ObjectKey, name string) ([]string, error) {
	pods := &corev1.PodList{}
	if err := k8s.List(ctx, pods, client.InNamespace(key.Namespace), client.MatchingLabels{lodiav1alpha1.ControlPlaneLabel: key.Name, lodiav1alpha1.ComponentLabel: name}); err != nil {
		return nil, err
	}
	var zones []string
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !pod.DeletionTimestamp.IsZero() || running(pod) != nil {
			continue
		}
		node := &corev1.Node{}
		if err := k8s.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node); err != nil {
			return nil, err
		}
		zones = append(zones, node.Labels[corev1.LabelTopologyZone])
	}
	return zones, nil
}

func etcdLeader(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) *corev1.Pod {
	t.Helper()
	var leader string
	until(ctx, t, 2*time.Minute, "etcd-druid reports three members ready, one of them the leader", func(ctx context.Context) error {
		etcd := &druidv1alpha1.Etcd{}
		if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: lodiav1alpha1.EtcdName(key.Name)}, etcd); err != nil {
			return err
		}
		var leaders []string
		ready := 0
		for _, m := range etcd.Status.Members {
			if m.Status == druidv1alpha1.EtcdMemberStatusReady {
				ready++
			}
			if ptr.Deref(m.Role, "") == druidv1alpha1.EtcdRoleLeader {
				leaders = append(leaders, m.Name)
			}
		}
		if len(leaders) != 1 || ready != 3 || etcd.Status.ReadyReplicas != 3 {
			return fmt.Errorf("%d replicas ready, members %+v", etcd.Status.ReadyReplicas, etcd.Status.Members)
		}
		leader = leaders[0]
		return nil
	})
	pod := &corev1.Pod{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: leader}, pod); err != nil {
		t.Fatal(err)
	}
	return pod
}

type transition struct {
	at              time.Time
	condition       string
	status          metav1.ConditionStatus
	reason, message string
}

type recorder struct {
	mu          sync.Mutex
	transitions []transition
	broken      error
	stop        func()
}

func record(ctx context.Context, t *testing.T, k8s client.WithWatch, key client.ObjectKey) *recorder {
	t.Helper()
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		t.Fatal(err)
	}
	r := &recorder{}
	r.observe(cp, time.Now())
	lw := &cache.ListWatch{WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
		return k8s.Watch(ctx, &lodiav1alpha1.ControlPlaneList{}, &client.ListOptions{
			Namespace: key.Namespace, FieldSelector: fields.OneTermEqualSelector("metadata.name", key.Name), Raw: &options,
		})
	}}
	watching, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w, err := watchtools.NewRetryWatcherWithContext(watching, cp.ResourceVersion, lw)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for event := range w.ResultChan() {
			switch fresh, ok := event.Object.(*lodiav1alpha1.ControlPlane); {
			case event.Type == watch.Error:
				r.mu.Lock()
				r.broken = apierrors.FromObject(event.Object)
				r.mu.Unlock()
			case ok && (event.Type == watch.Added || event.Type == watch.Modified):
				r.observe(fresh, time.Now())
			}
		}
	}()
	r.stop = func() {
		cancel()
		w.Stop()
		<-done
	}
	return r
}

func (r *recorder) observe(cp *lodiav1alpha1.ControlPlane, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, conditionType := range readyAndInputs {
		next := transition{at: at, condition: conditionType, status: metav1.ConditionUnknown, reason: "Absent"}
		if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c != nil {
			next.status, next.reason, next.message = c.Status, c.Reason, c.Message
		}
		if last, ok := r.last(conditionType); ok && last.status == next.status && last.reason == next.reason && last.message == next.message {
			continue
		}
		r.transitions = append(r.transitions, next)
	}
}

func (r *recorder) last(conditionType string) (transition, bool) {
	for _, tr := range slices.Backward(r.transitions) {
		if tr.condition == conditionType {
			return tr, true
		}
	}
	return transition{}, false
}

func (r *recorder) check(t *testing.T, from, to time.Time) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.broken != nil {
		t.Errorf("the watch of the ControlPlane broke, so its record is incomplete: %v", r.broken)
	}
	for _, tr := range r.transitions {
		if tr.at.After(from) && !tr.at.After(to) {
			t.Logf("%s %s=%s %s: %s", tr.at.Format(time.RFC3339Nano), tr.condition, tr.status, tr.reason, tr.message)
		}
	}
	for _, conditionType := range readyAndInputs {
		longest := r.longestNotTrue(conditionType, from, to)
		t.Logf("%s was not True for at most %s in the %s of the step", conditionType, longest, to.Sub(from).Round(time.Second))
		if conditionType == lodiav1alpha1.ConditionReady && longest > healthPeriod {
			t.Errorf("Ready was not True for %s, longer than Health's period, %s", longest, healthPeriod)
		}
	}
}

func (r *recorder) longestNotTrue(conditionType string, from, to time.Time) time.Duration {
	status, since := metav1.ConditionUnknown, from
	var longest time.Duration
	for _, tr := range r.transitions {
		if tr.condition != conditionType || tr.at.After(to) || tr.status == status {
			continue
		}
		at := tr.at
		if at.Before(from) {
			at = from
		}
		if status != metav1.ConditionTrue {
			longest = max(longest, at.Sub(since))
		}
		status, since = tr.status, at
	}
	if status != metav1.ConditionTrue {
		longest = max(longest, to.Sub(since))
	}
	return longest
}
