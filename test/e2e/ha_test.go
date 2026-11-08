// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && ha

package e2e

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/distribution/reference"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	haTenant  = "team-ha"
	haClass   = "e2e-ha"
	gateBound = 5 * time.Second
	settle    = 30 * time.Second
)

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
			history.check(t, start, time.Now(), gateBound)
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
			history.check(t, start, time.Now(), gateBound)
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

func (r *recorder) check(t *testing.T, from, to time.Time, bound time.Duration) {
	t.Helper()
	r.log(t, from, to)
	if longest := r.longestNotTrue(lodiav1alpha1.ConditionReady, from, to); longest > bound {
		t.Errorf("Ready was not True for %s, longer than %s", longest, bound)
	}
}
