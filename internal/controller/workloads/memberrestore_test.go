// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/testenv"
)

var (
	firstRestore  = metav1.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)
	secondRestore = metav1.Date(2026, 11, 26, 11, 0, 0, 0, time.UTC)
)

func recordRestore(t *testing.T, cp *lodiav1alpha1.ControlPlane, detectedAt metav1.Time) {
	t.Helper()
	applyStatus(t, cp, "test/memberRestore", map[string]any{"memberRestore": map[string]any{
		"member": "dev-etcd-0", "detectedAt": detectedAt.UTC().Format(time.RFC3339), "from": string(lodiav1alpha1.MemberRestoreFromSnapshots),
	}})
}

func rollsOf(t *testing.T, cp *lodiav1alpha1.ControlPlane) string {
	t.Helper()
	var got []string
	for _, component := range componentNames {
		value, ok := deployment(t, cp, component).Spec.Template.Annotations[lodiav1alpha1.MemberRestoreDetectedAtAnnotation]
		if !ok {
			value = "none"
		}
		got = append(got, value)
	}
	return strings.Join(got, " ")
}

func rolledFor(c *lodiav1alpha1.ComponentsStatus) string {
	var got []string
	for _, s := range []*lodiav1alpha1.ComponentStatus{c.APIServer, c.ControllerManager, c.Scheduler} {
		if s == nil || s.MemberRestoreDetectedAt == nil {
			got = append(got, "none")
		} else {
			got = append(got, s.MemberRestoreDetectedAt.UTC().Format(time.RFC3339))
		}
	}
	return strings.Join(got, " ")
}

func eventuallyRolls(t *testing.T, cp *lodiav1alpha1.ControlPlane, want string) {
	t.Helper()
	testenv.Eventually(t, func() error {
		if got := rollsOf(t, cp); got != want {
			return fmt.Errorf("the templates carry %s, want %s", got, want)
		}
		return nil
	})
}

func eventuallyRolled(t *testing.T, cp *lodiav1alpha1.ControlPlane, want string) {
	t.Helper()
	testenv.Eventually(t, func() error {
		if got := rolledFor(get(t, cp).Status.Components); got != want {
			return fmt.Errorf("status.components reports the rolls for %s, want %s", got, want)
		}
		return nil
	})
}

func TestARestoredMemberRollsTheAPIServerThenTheControllerManagerAndTheSchedulerOnce(t *testing.T) {
	cp := running(t)
	if got := rollsOf(t, cp); got != "none none none" {
		t.Fatalf("a tenant without a record has templates carrying %s, want none", got)
	}
	at := firstRestore.UTC().Format(time.RFC3339)
	before := generationsOf(t, cp)
	recordRestore(t, cp, firstRestore)
	eventuallyRolls(t, cp, at+" none none")
	reconcileNow(t, cp)
	if got := rollsOf(t, cp); got != at+" none none" {
		t.Fatalf("before the API server has rolled the templates carry %s, want the API server's alone", got)
	}
	if after := generationsOf(t, cp); after["kube-controller-manager"] != before["kube-controller-manager"] || after["kube-scheduler"] != before["kube-scheduler"] {
		t.Fatalf("the controller manager and the scheduler moved from %v to %v before the API server rolled", before, after)
	}
	markAvailable(t, deployment(t, cp, "kube-apiserver"))
	eventuallyRolled(t, cp, at+" none none")
	eventuallyRolls(t, cp, at+" "+at+" "+at)
	markAvailable(t, deployment(t, cp, "kube-controller-manager"))
	markAvailable(t, deployment(t, cp, "kube-scheduler"))
	eventuallyRolled(t, cp, at+" "+at+" "+at)

	rolled := generationsOf(t, cp)
	reconcileNow(t, cp)
	if after := generationsOf(t, cp); !maps.Equal(after, rolled) {
		t.Errorf("a restarted reconciler rolled %v to %v", rolled, after)
	}
	setSecret(t, cp, lodiav1alpha1.EtcdCASecret, "two")
	testenv.Eventually(t, func() error {
		if g := deployment(t, cp, "kube-apiserver").Generation; g == rolled["kube-apiserver"] {
			return fmt.Errorf("the API server is at generation %d still", g)
		}
		return nil
	})
	markAvailable(t, deployment(t, cp, "kube-apiserver"))
	reconcileNow(t, cp)
	if after := generationsOf(t, cp); after["kube-controller-manager"] != rolled["kube-controller-manager"] || after["kube-scheduler"] != rolled["kube-scheduler"] {
		t.Errorf("a later rollout of the API server rolled the controller manager and the scheduler: %v, then %v", rolled, after)
	}
	if got := rolledFor(get(t, cp).Status.Components); got != at+" "+at+" "+at {
		t.Errorf("after a later rollout of the API server the reports are %s", got)
	}
}

func TestAHeldTenantRollsForARestoredMemberByAPatchOfTheAnnotationAlone(t *testing.T) {
	cp := running(t)
	grant(t, cp, previous)
	for _, component := range componentNames {
		d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: cp.Name + "-" + component}}
		byHand(t, d, func() {
			d.Annotations[lodiav1alpha1.OperatorRevisionAnnotation] = previous
			d.Spec.Template.Spec.Containers[0].Image = "registry.example/edited:" + version
		})
	}
	settle(t, cp)
	stored := templatesOf(t, cp)
	at := firstRestore.UTC().Format(time.RFC3339)
	recordRestore(t, cp, firstRestore)
	eventuallyRolls(t, cp, at+" none none")
	markAvailable(t, deployment(t, cp, "kube-apiserver"))
	eventuallyRolls(t, cp, at+" "+at+" "+at)
	markAvailable(t, deployment(t, cp, "kube-controller-manager"))
	markAvailable(t, deployment(t, cp, "kube-scheduler"))
	eventuallyRolled(t, cp, at+" "+at+" "+at)
	for component, template := range templatesOf(t, cp) {
		delete(template.Annotations, lodiav1alpha1.MemberRestoreDetectedAtAnnotation)
		if !equality.Semantic.DeepEqual(template, stored[component]) {
			t.Errorf("the held pass changed the pod template of %s beyond %s", component, lodiav1alpha1.MemberRestoreDetectedAtAnnotation)
		}
	}
	expectStamps(t, cp, previous)
	pause(t, cp)
	var patched []string
	held := interceptor.NewClient(unpaused(), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if d, ok := obj.(*appsv1.Deployment); ok {
				patched = append(patched, d.Name)
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	passOf(t, operatorAt(held, revision), cp)
	if len(patched) > 0 {
		t.Errorf("a held pass after the repair landed patched the Deployments %v, want none", patched)
	}
}

func TestAHeldTenantWhoseAPIServerReportsAMinorTheRevisionDoesNotRunRollsTheAPIServerAlone(t *testing.T) {
	cp := running(t)
	pause(t, cp)
	const report = "v1.33.2"
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: cp.Namespace, Name: cp.Name + "-kube-apiserver"}}
	byHand(t, d, func() { d.Spec.Template.Spec.Containers[0].Image = "registry.k8s.io/kube-apiserver:" + report })
	reader := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := cl.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if live, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				live.Status.Components.APIServer.Version = report
			}
			return nil
		},
	})
	setTargetAt(t, cp, version, previous)
	recordRestore(t, cp, firstRestore)
	r := operatorAt(unpaused(), revision)
	r.Reader = reader
	at := firstRestore.UTC().Format(time.RFC3339)
	passOf(t, r, cp)
	if got := rollsOf(t, cp); got != at+" none none" {
		t.Fatalf("the held pass left the templates carrying %s, want the API server's alone", got)
	}
	markAvailable(t, deployment(t, cp, "kube-apiserver"))
	passOf(t, r, cp)
	passOf(t, r, cp)
	if got := rollsOf(t, cp); got != at+" none none" {
		t.Errorf("once the API server rolled the templates carry %s, want the API server's alone", got)
	}
}

func TestACacheThatLacksTheRecordRollsNothing(t *testing.T) {
	cp := running(t)
	at := firstRestore.UTC().Format(time.RFC3339)
	recordRestore(t, cp, firstRestore)
	eventuallyRolls(t, cp, at+" none none")
	markAvailable(t, deployment(t, cp, "kube-apiserver"))
	eventuallyRolls(t, cp, at+" "+at+" "+at)
	markAvailable(t, deployment(t, cp, "kube-controller-manager"))
	markAvailable(t, deployment(t, cp, "kube-scheduler"))
	eventuallyRolled(t, cp, at+" "+at+" "+at)
	pause(t, cp)
	before := objectsOf(t, cp)
	reconcileThrough(t, cp, unpaused(func(_ client.ObjectKey, obj client.Object) {
		if cached, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
			cached.Status.MemberRestore, cached.Status.Components = nil, nil
		}
	}))
	expectNoWrites(t, cp, before, "a pass whose cache lacks the record")
}

func TestADeploymentCacheBehindAFinishedRollMovesNoReportBack(t *testing.T) {
	cp := running(t)
	first, second := firstRestore.UTC().Format(time.RFC3339), secondRestore.UTC().Format(time.RFC3339)
	recordRestore(t, cp, firstRestore)
	eventuallyRolls(t, cp, first+" none none")
	markAvailable(t, deployment(t, cp, "kube-apiserver"))
	eventuallyRolls(t, cp, first+" "+first+" "+first)
	for _, component := range componentNames {
		markAvailable(t, deployment(t, cp, component))
	}
	eventuallyRolled(t, cp, first+" "+first+" "+first)
	stale := map[string]*appsv1.Deployment{}
	for _, component := range []string{"kube-apiserver", "kube-controller-manager"} {
		stale[cp.Name+"-"+component] = deployment(t, cp, component)
	}
	recordRestore(t, cp, secondRestore)
	eventuallyRolls(t, cp, second+" "+first+" "+first)
	markAvailable(t, deployment(t, cp, "kube-apiserver"))
	eventuallyRolls(t, cp, second+" "+second+" "+second)
	for _, component := range componentNames {
		markAvailable(t, deployment(t, cp, component))
	}
	eventuallyRolled(t, cp, second+" "+second+" "+second)
	pause(t, cp)
	before := generationsOf(t, cp)
	reconcileThrough(t, cp, unpaused(func(key client.ObjectKey, obj client.Object) {
		if d, ok := obj.(*appsv1.Deployment); ok && stale[key.Name] != nil {
			stale[key.Name].DeepCopyInto(d)
		}
	}))
	if got := rolledFor(get(t, cp).Status.Components); got != second+" "+second+" "+second {
		t.Errorf("a pass on a cache behind the finished roll reports %s, want %s for each", got, second)
	}
	if after := generationsOf(t, cp); !maps.Equal(after, before) {
		t.Errorf("a pass on a cache behind the finished roll rolled %v to %v", before, after)
	}
}
