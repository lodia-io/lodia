// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/lifecycle"
	"go.lodia.io/lodia/internal/testenv"
)

var (
	startedAt = time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	later     = startedAt.Add(time.Hour)
)

func quietControlPlane(t *testing.T) *lodiav1alpha1.ControlPlane {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "lifecycle-"}}
	if err := quiet.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: ns.Name},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   "v1.37.1",
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint:  lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
		},
	}
	if err := quiet.Create(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func applyQuiet(t *testing.T, cp *lodiav1alpha1.ControlPlane, manager string, status map[string]any) {
	t.Helper()
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     status,
	}}
	if err := quiet.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(manager), client.ForceOwnership); err != nil {
		t.Fatal(err)
	}
}

func lifecycleStatus(target string, step lodiav1alpha1.UpgradeStep, since time.Time) map[string]any {
	status := map[string]any{"target": map[string]any{"version": target}}
	if step != "" {
		status["kubernetes"] = map[string]any{"upgrade": map[string]any{"step": string(step), "startedAt": since.Format(time.RFC3339)}}
	}
	return status
}

func quietConditions(cp *lodiav1alpha1.ControlPlane, overrides ...metav1.Condition) []any {
	list := []any{}
	generation := cp.Generation
	seen := map[string]bool{}
	entry := func(c metav1.Condition) map[string]any {
		return map[string]any{"type": c.Type, "status": string(c.Status), "reason": c.Reason, "message": c.Message, "observedGeneration": generation, "lastTransitionTime": startedAt.Format(time.RFC3339)}
	}
	for _, c := range overrides {
		seen[c.Type] = true
		list = append(list, entry(c))
	}
	for _, conditionType := range readiness {
		if !seen[conditionType] {
			list = append(list, entry(metav1.Condition{Type: conditionType, Status: metav1.ConditionTrue, Reason: "Fine"}))
		}
	}
	return list
}

func seedQuiet(t *testing.T, cp *lodiav1alpha1.ControlPlane, target string, step lodiav1alpha1.UpgradeStep, reports [4]string, oldestKubelet string) {
	t.Helper()
	applyQuiet(t, cp, lifecycle.FieldManager, lifecycleStatus(target, step, startedAt))
	components := map[string]any{}
	for i, key := range []string{"apiServer", "controllerManager", "scheduler"} {
		components[key] = map[string]any{"total": int64(1), "ready": int64(1), "updated": int64(1), "version": reports[i]}
	}
	status := map[string]any{
		"components": components,
		"addons":     map[string]any{"version": reports[3]},
		"conditions": quietConditions(cp),
	}
	status["nodes"] = map[string]any{"total": int64(0), "ready": int64(0)}
	if oldestKubelet != "" {
		status["nodes"] = map[string]any{"total": int64(1), "ready": int64(1), "oldestKubelet": oldestKubelet}
	}
	applyQuiet(t, cp, "test/seed", status)
}

func all(version string) [4]string { return [4]string{version, version, version, version} }

func reconcileQuiet(t *testing.T, c client.Client, reader client.Reader, now time.Time, cp *lodiav1alpha1.ControlPlane) error {
	t.Helper()
	return reconcileAt(t.Context(), c, reader, now, cp)
}

func reconcileAt(ctx context.Context, c client.Client, reader client.Reader, now time.Time, cp *lodiav1alpha1.ControlPlane) error {
	return reconcileAs(ctx, c, reader, runningRevision, now, cp)
}

func reconcileAs(ctx context.Context, c client.Client, reader client.Reader, revision string, now time.Time, cp *lodiav1alpha1.ControlPlane) error {
	r := &lifecycle.Reconciler{Client: c, Reader: reader, Clock: clocktesting.NewFakePassiveClock(now), Revision: revision}
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	return err
}

func quietPass(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	if err := reconcileQuiet(t, quiet, quiet, later, cp); err != nil {
		t.Fatal(err)
	}
}

func getQuiet(t *testing.T, cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.ControlPlane {
	t.Helper()
	fresh := &lodiav1alpha1.ControlPlane{}
	if err := quiet.Get(t.Context(), client.ObjectKeyFromObject(cp), fresh); err != nil {
		t.Fatal(err)
	}
	return fresh
}

func createBackup(t *testing.T, cp *lodiav1alpha1.ControlPlane, name string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	backup := &lodiav1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cp.Namespace},
		Spec:       lodiav1alpha1.BackupSpec{ControlPlaneRef: lodiav1alpha1.LocalReference{Name: cp.Name}},
	}
	if err := quiet.Create(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	backup.Status.Conditions = []metav1.Condition{{Type: lodiav1alpha1.ConditionReady, Status: status, Reason: reason, Message: reason + " says so", LastTransitionTime: metav1.NewTime(startedAt)}}
	if err := quiet.Status().Update(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
}

func preUpgrade(cp *lodiav1alpha1.ControlPlane) string {
	return lodiav1alpha1.PreUpgradeBackupName(cp.Name, "1.37", cp.UID)
}

func staleClient(behind *lodiav1alpha1.ControlPlane, applies *int) client.WithWatch {
	return interceptor.NewClient(quiet, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				behind.DeepCopyInto(cp)
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
		SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
			*applies++
			return c.SubResource(subResource).Apply(ctx, obj, opts...)
		},
	})
}

func TestAStaleCacheCannotUndoLifecyclesOwnWrite(t *testing.T) {
	for _, c := range []struct {
		name    string
		seed    func(cp *lodiav1alpha1.ControlPlane)
		behind  func(cp *lodiav1alpha1.ControlPlane)
		backups int
	}{
		{
			name:    "the target and step already moved by a Backup that completed",
			backups: 1,
			seed: func(cp *lodiav1alpha1.ControlPlane) {
				seedQuiet(t, cp, "v1.37.1", lodiav1alpha1.UpgradeStepAPIServer, all("v1.36.6"), "")
				createBackup(t, cp, preUpgrade(cp), metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted)
			},
			behind: func(cp *lodiav1alpha1.ControlPlane) {
				cp.Status.Target.Version = "v1.36.6"
				cp.Status.Kubernetes.Upgrade.Step = lodiav1alpha1.UpgradeStepBackup
			},
		},
		{
			name:    "an upgrade the cache has not seen keeps its startedAt",
			backups: 1,
			seed: func(cp *lodiav1alpha1.ControlPlane) {
				seedQuiet(t, cp, "v1.36.6", lodiav1alpha1.UpgradeStepBackup, all("v1.36.6"), "")
				createBackup(t, cp, preUpgrade(cp), metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying)
			},
			behind: func(cp *lodiav1alpha1.ControlPlane) { cp.Status.Kubernetes.Upgrade = nil },
		},
		{
			name: "an older target restored by the cache",
			seed: func(cp *lodiav1alpha1.ControlPlane) {
				seedQuiet(t, cp, "v1.37.1", lodiav1alpha1.UpgradeStepAPIServer, [4]string{"v1.36.5", "v1.36.5", "v1.36.5", "v1.36.5"}, "")
			},
			behind: func(cp *lodiav1alpha1.ControlPlane) {
				cp.Status.Target.Version = "v1.36.5"
				cp.Status.Kubernetes.Upgrade = nil
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := quietControlPlane(t)
			c.seed(cp)
			quietPass(t, cp)
			settled := getQuiet(t, cp)
			behind := settled.DeepCopy()
			c.behind(behind)
			applies := 0
			if err := reconcileQuiet(t, staleClient(behind, &applies), quiet, later.Add(time.Hour), cp); err != nil {
				t.Fatal(err)
			}
			after := getQuiet(t, cp)
			if applies != 0 || after.ResourceVersion != settled.ResourceVersion {
				t.Errorf("a pass on a stale cache applied %d times, resourceVersion %s then %s", applies, settled.ResourceVersion, after.ResourceVersion)
			}
			list := &lodiav1alpha1.BackupList{}
			if err := quiet.List(t.Context(), list, client.InNamespace(cp.Namespace)); err != nil {
				t.Fatal(err)
			}
			if len(list.Items) != c.backups {
				t.Errorf("%d Backups after a pass on a stale cache, want %d", len(list.Items), c.backups)
			}
		})
	}
}

func TestTheLiveDecisionReadsTheBackupOfItsOwnMinor(t *testing.T) {
	cp := quietControlPlane(t)
	seedQuiet(t, cp, "v1.36.6", lodiav1alpha1.UpgradeStepBackup, all("v1.36.6"), "")
	createBackup(t, cp, lodiav1alpha1.PreUpgradeBackupName(cp.Name, "1.36", cp.UID), metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted)
	behind := getQuiet(t, cp)
	behind.Status.Target.Version = "v1.35.10"
	applies := 0
	if err := reconcileQuiet(t, staleClient(behind, &applies), quiet, later, cp); err != nil {
		t.Fatal(err)
	}
	fresh := getQuiet(t, cp)
	if fresh.Status.Target.Version != "v1.36.6" || fresh.Status.Kubernetes.Upgrade.Step != lodiav1alpha1.UpgradeStepBackup {
		t.Errorf("target %s, upgrade %+v: the Backup of the cached minor moved the live one", fresh.Status.Target.Version, fresh.Status.Kubernetes.Upgrade)
	}
	if err := quiet.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: preUpgrade(cp)}, &lodiav1alpha1.Backup{}); err != nil {
		t.Errorf("the Backup of the live minor: %v", err)
	}
}

func countingReader(reads *int) client.WithWatch {
	return interceptor.NewClient(quiet, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			*reads++
			return c.Get(ctx, key, obj, opts...)
		},
	})
}

func TestAPassThatChangesNothingMakesNoLiveReadAndNoWrite(t *testing.T) {
	for _, c := range []struct {
		name string
		seed func(cp *lodiav1alpha1.ControlPlane)
	}{
		{"an idle tenant", func(cp *lodiav1alpha1.ControlPlane) { seedQuiet(t, cp, "v1.37.1", "", all("v1.37.1"), "") }},
		{"an upgrade at step Backup with a Backup copying", func(cp *lodiav1alpha1.ControlPlane) {
			seedQuiet(t, cp, "v1.36.6", lodiav1alpha1.UpgradeStepBackup, all("v1.36.6"), "")
			createBackup(t, cp, preUpgrade(cp), metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying)
		}},
		{"a rollout under way", func(cp *lodiav1alpha1.ControlPlane) {
			seedQuiet(t, cp, "v1.37.1", lodiav1alpha1.UpgradeStepAPIServer, all("v1.36.6"), "")
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := quietControlPlane(t)
			c.seed(cp)
			quietPass(t, cp)
			settled := getQuiet(t, cp)
			reads, applies := 0, 0
			written := staleClient(settled, &applies)
			if err := reconcileQuiet(t, written, countingReader(&reads), later, cp); err != nil {
				t.Fatal(err)
			}
			if reads != 0 || applies != 0 {
				t.Errorf("a pass that changes nothing read live %d times and applied %d times", reads, applies)
			}
			if after := getQuiet(t, cp); after.ResourceVersion != settled.ResourceVersion {
				t.Errorf("resourceVersion moved from %s to %s", settled.ResourceVersion, after.ResourceVersion)
			}
		})
	}
}

func TestStepAndStartedAtSurviveARestartAndThePreflightIsNotRerun(t *testing.T) {
	cp := quietControlPlane(t)
	seedQuiet(t, cp, "v1.36.6", lodiav1alpha1.UpgradeStepBackup, all("v1.36.6"), "")
	quietPass(t, cp)

	held := metav1.Condition{Type: lodiav1alpha1.ConditionControllerManagerHeld, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonNodesSilent}
	stopped := metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonProvisioning}
	applyQuiet(t, cp, "test/seed", map[string]any{"conditions": quietConditions(cp, held, stopped), "nodes": map[string]any{"total": int64(1), "ready": int64(1), "oldestKubelet": "v1.30.0"}})

	if err := reconcileAt(t.Context(), quiet, quiet, later.Add(24*time.Hour), cp); err != nil {
		t.Fatal(err)
	}
	fresh := getQuiet(t, cp)
	u := fresh.Status.Kubernetes.Upgrade
	if u == nil || u.Step != lodiav1alpha1.UpgradeStepBackup || !u.StartedAt.Time.Equal(startedAt) {
		t.Errorf("after a restart upgrade is %+v, want step Backup at %s", u, startedAt)
	}
	if fresh.Status.Target.Version != "v1.36.6" {
		t.Errorf("target is %s", fresh.Status.Target.Version)
	}
	c := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionProgressing)
	if c == nil || c.Reason != lodiav1alpha1.ReasonUpgrading {
		t.Errorf("Progressing is %+v, want Upgrading", c)
	}
	backup := &lodiav1alpha1.Backup{}
	if err := quiet.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: preUpgrade(cp)}, backup); err != nil {
		t.Fatalf("the Backup a hold and a Datastore's fall did not stop: %v", err)
	}
	backup.Status.Conditions = []metav1.Condition{{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonCompleted, LastTransitionTime: metav1.NewTime(startedAt)}}
	if err := quiet.Status().Update(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	quietPass(t, cp)
	fresh = getQuiet(t, cp)
	if fresh.Status.Target.Version != "v1.37.1" || fresh.Status.Kubernetes.Upgrade.Step != lodiav1alpha1.UpgradeStepAPIServer || !fresh.Status.Kubernetes.Upgrade.StartedAt.Time.Equal(startedAt) {
		t.Errorf("a Completed Backup under a hold and a Datastore's fall: target %s, upgrade %+v", fresh.Status.Target.Version, fresh.Status.Kubernetes.Upgrade)
	}
}

func TestTheBackupIsCreatedOnceWithTheMinorsNameAndNoOwner(t *testing.T) {
	cp := quietControlPlane(t)
	seedQuiet(t, cp, "v1.36.6", "", all("v1.36.6"), "v1.36.6")
	quietPass(t, cp)
	fresh := getQuiet(t, cp)
	if u := fresh.Status.Kubernetes.Upgrade; u == nil || u.Step != lodiav1alpha1.UpgradeStepBackup {
		t.Fatalf("upgrade is %+v, want step Backup", u)
	}
	want := preUpgrade(cp)
	backup := &lodiav1alpha1.Backup{}
	if err := quiet.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: want}, backup); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(want, "-pre-upgrade-v1.37-"+string(cp.UID)) || len(backup.OwnerReferences) != 0 || backup.Spec.ControlPlaneRef.Name != cp.Name {
		t.Errorf("Backup %s has owners %+v and refers to %+v", backup.Name, backup.OwnerReferences, backup.Spec.ControlPlaneRef)
	}

	gone := interceptor.NewClient(quiet, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*lodiav1alpha1.Backup); ok {
				return apierrors.NewNotFound(lodiav1alpha1.GroupVersion.WithResource("backups").GroupResource(), key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	if err := reconcileQuiet(t, gone, quiet, later, cp); err != nil {
		t.Fatalf("a repeat of the create, on a cache without the Backup: %v", err)
	}
	again := &lodiav1alpha1.Backup{}
	if err := quiet.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: want}, again); err != nil {
		t.Fatal(err)
	}
	if again.UID != backup.UID || again.ResourceVersion != backup.ResourceVersion {
		t.Errorf("the Backup changed: %s/%s, then %s/%s", backup.UID, backup.ResourceVersion, again.UID, again.ResourceVersion)
	}
	list := &lodiav1alpha1.BackupList{}
	if err := quiet.List(t.Context(), list, client.InNamespace(cp.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Errorf("%d Backups in the namespace, want 1", len(list.Items))
	}
}

func TestAPredecessorsBackupOfTheSameNameDoesNotStandIn(t *testing.T) {
	cp := quietControlPlane(t)
	seedQuiet(t, cp, "v1.36.6", lodiav1alpha1.UpgradeStepBackup, all("v1.36.6"), "")
	createBackup(t, cp, lodiav1alpha1.PreUpgradeBackupName(cp.Name, "1.37", types.UID("predecessor")), metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted)
	quietPass(t, cp)
	fresh := getQuiet(t, cp)
	if fresh.Status.Target.Version != "v1.36.6" || fresh.Status.Kubernetes.Upgrade.Step != lodiav1alpha1.UpgradeStepBackup {
		t.Errorf("target %s, upgrade %+v", fresh.Status.Target.Version, fresh.Status.Kubernetes.Upgrade)
	}
	if err := quiet.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: preUpgrade(cp)}, &lodiav1alpha1.Backup{}); err != nil {
		t.Errorf("no Backup of this ControlPlane: %v", err)
	}
}

func TestAFailureMidPassLosesNothing(t *testing.T) {
	injected := errors.New("injected")
	withApply := func(apply error, create error) client.WithWatch {
		return interceptor.NewClient(quiet, interceptor.Funcs{
			SubResourceApply: func(ctx context.Context, c client.Client, subResource string, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
				if apply != nil {
					return apply
				}
				return c.SubResource(subResource).Apply(ctx, obj, opts...)
			},
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if create != nil {
					return create
				}
				return c.Create(ctx, obj, opts...)
			},
		})
	}
	backupExists := func(cp *lodiav1alpha1.ControlPlane) bool {
		err := quiet.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: preUpgrade(cp)}, &lodiav1alpha1.Backup{})
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
		return err == nil
	}

	t.Run("a status write that fails creates no Backup", func(t *testing.T) {
		cp := quietControlPlane(t)
		seedQuiet(t, cp, "v1.36.6", "", all("v1.36.6"), "")
		if err := reconcileQuiet(t, withApply(injected, nil), quiet, later, cp); !errors.Is(err, injected) {
			t.Fatalf("the pass returned %v", err)
		}
		if backupExists(cp) {
			t.Error("a Backup was created though the upgrade was not recorded")
		}
		if u := getQuiet(t, cp).Status.Kubernetes; u != nil && u.Upgrade != nil {
			t.Errorf("upgrade is %+v", u.Upgrade)
		}
		quietPass(t, cp)
		if !backupExists(cp) || getQuiet(t, cp).Status.Kubernetes.Upgrade.Step != lodiav1alpha1.UpgradeStepBackup {
			t.Error("the next pass did not start the step")
		}
	})

	t.Run("a create that fails after the status write is repaired by the next pass", func(t *testing.T) {
		cp := quietControlPlane(t)
		seedQuiet(t, cp, "v1.36.6", "", all("v1.36.6"), "")
		if err := reconcileQuiet(t, withApply(nil, injected), quiet, later, cp); !errors.Is(err, injected) {
			t.Fatalf("the pass returned %v", err)
		}
		startedAtWritten := getQuiet(t, cp).Status.Kubernetes.Upgrade.StartedAt
		if backupExists(cp) {
			t.Fatal("the failed create left a Backup")
		}
		if err := reconcileQuiet(t, quiet, quiet, later.Add(time.Hour), cp); err != nil {
			t.Fatal(err)
		}
		fresh := getQuiet(t, cp)
		if !backupExists(cp) || !fresh.Status.Kubernetes.Upgrade.StartedAt.Equal(&startedAtWritten) {
			t.Errorf("the next pass: Backup %t, startedAt %s then %s", backupExists(cp), startedAtWritten, fresh.Status.Kubernetes.Upgrade.StartedAt)
		}
	})

	t.Run("a Backup read that fails writes nothing", func(t *testing.T) {
		cp := quietControlPlane(t)
		seedQuiet(t, cp, "v1.36.6", "", all("v1.36.6"), "")
		before := getQuiet(t, cp)
		failing := interceptor.NewClient(quiet, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*lodiav1alpha1.Backup); ok {
					return injected
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
		if err := reconcileQuiet(t, failing, quiet, later, cp); !errors.Is(err, injected) {
			t.Fatalf("the pass returned %v", err)
		}
		if after := getQuiet(t, cp); after.ResourceVersion != before.ResourceVersion {
			t.Error("a pass that could not read the Backup wrote the status")
		}
	})

	t.Run("a pass whose deadline is spent writes the status and creates on the next", func(t *testing.T) {
		cp := quietControlPlane(t)
		seedQuiet(t, cp, "v1.36.6", "", all("v1.36.6"), "")
		expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()
		cached := interceptor.NewClient(quiet, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return c.Get(context.Background(), key, obj, opts...)
			},
		})
		if err := reconcileAt(expired, cached, quiet, later, cp); err == nil {
			t.Fatal("the create on a spent deadline returned no error")
		}
		if u := getQuiet(t, cp).Status.Kubernetes.Upgrade; u == nil || u.Step != lodiav1alpha1.UpgradeStepBackup {
			t.Fatalf("upgrade is %+v after a pass past its deadline", u)
		}
		quietPass(t, cp)
		if !backupExists(cp) {
			t.Error("the next pass did not create the Backup")
		}
	})
}

func TestAMinorStepFromEditToEndThroughTheManager(t *testing.T) {
	cp := controlPlane(t, "v1.36.6")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	reportAll(t, cp, "v1.36.6")
	applyStatus(t, cp, "test/nodes", map[string]any{"nodes": map[string]any{"total": int64(0), "ready": int64(0)}})
	testenv.Eventually(t, func() error { return expect(t, cp, "v1.36.6", lodiav1alpha1.ReasonIdle) })

	setVersion(t, cp, "v1.37.1")
	for _, conditionType := range readiness {
		setCondition(t, cp, conditionType, metav1.ConditionTrue, "Fine", "")
	}
	created := get(t, cp)
	name := lodiav1alpha1.PreUpgradeBackupName(created.Name, "1.37", created.UID)
	var upgradeStarted metav1.Time
	testenv.Eventually(t, func() error {
		fresh := get(t, cp)
		if fresh.Status.Target.Version != "v1.36.6" {
			return fmt.Errorf("target is %s before the Backup", fresh.Status.Target.Version)
		}
		k := fresh.Status.Kubernetes
		if k == nil || k.Upgrade == nil || k.Upgrade.Step != lodiav1alpha1.UpgradeStepBackup {
			return fmt.Errorf("status.kubernetes is %+v", k)
		}
		upgradeStarted = k.Upgrade.StartedAt
		backup := &lodiav1alpha1.Backup{}
		return k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: name}, backup)
	})
	backup := &lodiav1alpha1.Backup{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: name}, backup); err != nil {
		t.Fatal(err)
	}
	if len(backup.OwnerReferences) != 0 {
		t.Errorf("Backup %s has owners %+v", backup.Name, backup.OwnerReferences)
	}
	backup.Status.Conditions = []metav1.Condition{{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionUnknown, Reason: lodiav1alpha1.ReasonCopying, Message: "copying", LastTransitionTime: metav1.Now()}}
	if err := k8s.Status().Update(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		c := condition(t, cp, lodiav1alpha1.ConditionProgressing)
		if c == nil || c.Reason != lodiav1alpha1.ReasonUpgrading || !strings.Contains(c.Message, "Copying: copying") {
			return fmt.Errorf("Progressing is %+v", c)
		}
		return nil
	})
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: name}, backup); err != nil {
		t.Fatal(err)
	}
	backup.Status.Conditions = []metav1.Condition{{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonFailed, Message: "the copy failed", LastTransitionTime: metav1.Now()}}
	if err := k8s.Status().Update(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		c := condition(t, cp, lodiav1alpha1.ConditionProgressing)
		if c == nil || c.Reason != lodiav1alpha1.ReasonFailed || c.Status != metav1.ConditionFalse {
			return fmt.Errorf("Progressing is %+v", c)
		}
		return nil
	})
	if err := k8s.Delete(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		replaced := &lodiav1alpha1.Backup{}
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: name}, replaced); err != nil {
			return err
		}
		if replaced.UID == backup.UID {
			return fmt.Errorf("Backup %s is still the failed one", name)
		}
		return nil
	})
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: cp.Namespace, Name: name}, backup); err != nil {
		t.Fatal(err)
	}
	backup.Status.Conditions = []metav1.Condition{{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonCompleted, Message: "copied", LastTransitionTime: metav1.Now()}}
	if err := k8s.Status().Update(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		fresh := get(t, cp)
		if fresh.Status.Target.Version != "v1.37.1" {
			return fmt.Errorf("target is %s", fresh.Status.Target.Version)
		}
		k := fresh.Status.Kubernetes
		if k == nil || k.Upgrade == nil || k.Upgrade.Step != lodiav1alpha1.UpgradeStepAPIServer || !k.Upgrade.StartedAt.Equal(&upgradeStarted) {
			return fmt.Errorf("status.kubernetes is %+v, want step APIServer started at %s", k, upgradeStarted)
		}
		return nil
	})
	reportAll(t, cp, "v1.37.1")
	testenv.Eventually(t, func() error {
		if k := get(t, cp).Status.Kubernetes; k == nil || k.Upgrade != nil || k.Version != "v1.37.1" {
			return fmt.Errorf("status.kubernetes is %+v", k)
		}
		return expect(t, cp, "v1.37.1", lodiav1alpha1.ReasonIdle)
	})
}
