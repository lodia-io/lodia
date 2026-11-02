// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/workloads"
)

const heldUntil = "kube-controller-manager is held at 0 replicas until the restore is released: "

var gone = lodiav1alpha1.UIDReference{Name: "prod", UID: "0b6c2f4e-8d1a-4c3e-9f57-2a6e1d0c7b93"}

func TestAHeldRestoreRunsNoControllerManagerAndCountsItAvailable(t *testing.T) {
	ns := namespace(t)
	prod := source(t, ns)
	cp := restored(t, ns, lodiav1alpha1.RestoreHeld)
	for _, step := range []struct {
		name, unmet string
		change      func()
	}{
		{"nothing", "spec.restoreFrom.release is Held; the restore is not complete; AddonsApplied is not True for generation 1", func() {}},
		{"the restore completed", "spec.restoreFrom.release is Held; ControlPlane prod (UID " + string(prod.UID) + "), the Backup's source, exists; AddonsApplied is not True for generation 1",
			func() { setRestore(t, cp, lodiav1alpha1.UIDReference{Name: prod.Name, UID: prod.UID}) }},
		{"AddonsApplied", "spec.restoreFrom.release is Held; ControlPlane prod (UID " + string(prod.UID) + "), the Backup's source, exists",
			func() { setTrue(t, cp, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ReasonApplied) }},
		{"the source's deletion", "spec.restoreFrom.release is Held", func() {
			if err := k8s.Delete(t.Context(), prod); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		step.change()
		eventually(t, func() error {
			return expectAt(cp, 1, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, heldUntil+step.unmet)
		})
		if n := controllerManagers(t, cp); n != 0 {
			t.Fatalf("after %s the controller manager runs %d replicas, want 0", step.name, n)
		}
	}
	expectComponents(t, cp, "1/1/1 0/0/0 1/1/1")
}

func TestARestoreNotCompleteKeepsItsControllerManagerHeld(t *testing.T) {
	cp := restored(t, namespace(t), lodiav1alpha1.RestoreReleased)
	setTrue(t, cp, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ReasonApplied)
	reconcileNow(t, cp)
	if err := expectAt(cp, 1, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, heldUntil+"the restore is not complete"); err != nil {
		t.Error(err)
	}
	if n := controllerManagers(t, cp); n != 0 {
		t.Errorf("the controller manager of a restore that is not complete runs %d replicas", n)
	}
}

func TestTheSourcesDeletionAloneReleasesItsReplacement(t *testing.T) {
	ns := namespace(t)
	prod := source(t, ns, "test.lodia.io/keep")
	cp := restored(t, ns, lodiav1alpha1.RestoreReleased)
	setRestore(t, cp, lodiav1alpha1.UIDReference{Name: prod.Name, UID: prod.UID})
	setTrue(t, cp, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ReasonApplied)
	sourceExists := heldUntil + "ControlPlane prod (UID " + string(prod.UID) + "), the Backup's source, exists"
	eventually(t, func() error {
		return expectAt(cp, 1, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, sourceExists)
	})
	if n := controllerManagers(t, cp); n != 0 {
		t.Fatalf("the controller manager runs %d replicas beside its source", n)
	}

	if err := k8s.Delete(t.Context(), prod); err != nil {
		t.Fatal(err)
	}
	reconcileNow(t, cp)
	if err := expectAt(cp, 1, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, sourceExists); err != nil {
		t.Fatal(err)
	}
	if n := controllerManagers(t, cp); n != 0 {
		t.Fatalf("the controller manager runs %d replicas while its source is being deleted", n)
	}

	eventually(t, func() error {
		fresh := &lodiav1alpha1.ControlPlane{}
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(prod), fresh); err != nil {
			return err
		}
		fresh.Finalizers = nil
		return k8s.Update(t.Context(), fresh)
	})
	eventually(t, func() error {
		if n := controllerManagers(t, cp); n != 1 {
			return fmt.Errorf("the controller manager runs %d replicas once its source is gone", n)
		}
		return nil
	})
}

func TestTheReleaseWaitsForAddonsAppliedAtItsGenerationAndHoldsForGood(t *testing.T) {
	ns := namespace(t)
	prod := source(t, ns)
	cp := restored(t, ns, lodiav1alpha1.RestoreHeld)
	setRestore(t, cp, lodiav1alpha1.UIDReference{Name: prod.Name, UID: prod.UID})
	setTrue(t, cp, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ReasonApplied)
	if err := k8s.Delete(t.Context(), prod); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		return expectAt(cp, 1, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, heldUntil+"spec.restoreFrom.release is Held")
	})

	update(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) { spec.RestoreFrom.Release = lodiav1alpha1.RestoreReleased })
	reconcileNow(t, cp)
	if err := expectAt(cp, 2, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, heldUntil+"AddonsApplied is not True for generation 2"); err != nil {
		t.Fatal(err)
	}
	if n := controllerManagers(t, cp); n != 0 {
		t.Fatalf("the controller manager runs %d replicas on an AddonsApplied of the generation before the release", n)
	}

	setTrue(t, cp, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ReasonApplied)
	eventually(t, func() error {
		if n := controllerManagers(t, cp); n != 1 {
			return fmt.Errorf("the released controller manager runs %d replicas", n)
		}
		return nil
	})
	markAvailable(t, deployment(t, cp, "kube-controller-manager"))
	eventually(t, func() error {
		return expectAt(cp, 2, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "every replica of kube-apiserver, kube-controller-manager and kube-scheduler is ready")
	})

	if err := k8s.Delete(t.Context(), deployment(t, cp, "kube-controller-manager")); err != nil {
		t.Fatal(err)
	}
	reconcileNow(t, cp)
	if n := controllerManagers(t, cp); n != 1 {
		t.Fatalf("a released controller manager's Deployment comes back with %d replicas while AddonsApplied is True", n)
	}

	setFalse(t, cp, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ReasonApplying)
	reconcileNow(t, cp)
	if n := controllerManagers(t, cp); n != 1 {
		t.Errorf("a released controller manager runs %d replicas once AddonsApplied turns False", n)
	}
}

func TestAControllerManagerScaledByHandIsReleased(t *testing.T) {
	cp := restored(t, namespace(t), lodiav1alpha1.RestoreHeld)
	update(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) { spec.Paused = true })
	r := &workloads.Reconciler{Client: unpaused(), Reader: k8s, SignerImage: signerImage}
	eventually(t, func() error {
		if d := deployment(t, cp, "kube-controller-manager"); *d.Spec.Replicas == 0 {
			d.Spec.Replicas = new(int32(1))
			if err := k8s.Update(t.Context(), d); err != nil {
				return err
			}
		}
		if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
			return err
		}
		if n := controllerManagers(t, cp); n != 1 {
			return fmt.Errorf("a held controller manager scaled by hand runs %d replicas after a pass", n)
		}
		return nil
	})
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if n := controllerManagers(t, cp); n != 1 {
		t.Errorf("a controller manager scaled by hand runs %d replicas after a second pass", n)
	}
	if err := expectAt(cp, 2, metav1.ConditionFalse, lodiav1alpha1.ReasonUnavailable, "kube-controller-manager has 0 of 1 replicas ready"); err != nil {
		t.Error(err)
	}
}

func TestHighAvailabilityRunsTwoControllerManagersOnceReleased(t *testing.T) {
	cp := restored(t, namespace(t), lodiav1alpha1.RestoreReleased, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Zone"}
	})
	setRestore(t, cp, gone)
	eventually(t, func() error {
		return expectAt(cp, 1, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, heldUntil+"AddonsApplied is not True for generation 1")
	})
	held := deployment(t, cp, "kube-controller-manager")
	if *held.Spec.Replicas != 0 {
		t.Fatalf("the held controller manager runs %d replicas", *held.Spec.Replicas)
	}
	expectSpread(t, held, map[string]int32{"kubernetes.io/hostname": 2, "topology.kubernetes.io/zone": 2})

	setTrue(t, cp, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ReasonApplied)
	eventually(t, func() error {
		if n := controllerManagers(t, cp); n != 2 {
			return fmt.Errorf("the released controller manager runs %d replicas", n)
		}
		return nil
	})
	if released := deployment(t, cp, "kube-controller-manager"); !equality.Semantic.DeepEqual(released.Spec.Template, held.Spec.Template) {
		t.Errorf("the release changed the pod template from %+v to %+v", held.Spec.Template, released.Spec.Template)
	}
}

func TestAnOrphanedControllerManagerReleasesNothing(t *testing.T) {
	running := create(t, namespace(t), loadBalancer())
	setIngress(t, running, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReady(t, running)
	kcm := eventuallyDeployment(t, running, "kube-controller-manager")
	if *kcm.Spec.Replicas != 1 {
		t.Fatalf("the controller manager of a ControlPlane that restores nothing runs %d replicas", *kcm.Spec.Replicas)
	}
	ns := namespace(t)
	orphan := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: kcm.Name, Namespace: ns, Labels: kcm.Labels}, Spec: *kcm.Spec.DeepCopy()}
	if err := k8s.Create(t.Context(), orphan); err != nil {
		t.Fatal(err)
	}

	cp := restored(t, ns, lodiav1alpha1.RestoreHeld)
	eventually(t, func() error {
		d := deployment(t, cp, "kube-controller-manager")
		if !metav1.IsControlledBy(d, cp) || *d.Spec.Replicas != 0 {
			return fmt.Errorf("the orphaned controller manager runs %d replicas, owned by %+v", *d.Spec.Replicas, d.OwnerReferences)
		}
		return nil
	})
}

func TestAMetReleaseInAWaitCountsTheControllerManagerAtZeroAvailable(t *testing.T) {
	cp := restored(t, namespace(t), lodiav1alpha1.RestoreReleased)
	setRestore(t, cp, gone)
	eventually(t, func() error {
		return expectAt(cp, 1, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, heldUntil+"AddonsApplied is not True for generation 1")
	})
	setFalse(t, cp, lodiav1alpha1.ConditionCertificatesReady, "Issuing")
	setTrue(t, cp, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ReasonApplied)
	reconcileNow(t, cp)
	if err := expectAt(cp, 1, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "kube-controller-manager has 0 of 0 replicas ready"); err != nil {
		t.Error(err)
	}
	if n := controllerManagers(t, cp); n != 0 {
		t.Errorf("the controller manager runs %d replicas while Workloads waits for CertificatesReady", n)
	}
}

func TestAWaitKeepsTheHeldMessage(t *testing.T) {
	cp := restored(t, namespace(t), lodiav1alpha1.RestoreHeld)
	unmet := "spec.restoreFrom.release is Held; the restore is not complete; AddonsApplied is not True for generation "
	eventually(t, func() error {
		return expectAt(cp, 1, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, heldUntil+unmet+"1")
	})
	changeSpec(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.APIServer = &lodiav1alpha1.APIServer{ExtraArgs: map[string]string{"audit-log-maxage": "7"}}
	})
	reconcileNow(t, cp)
	if err := expectAt(cp, 1, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, heldUntil+unmet+"2"); err != nil {
		t.Error(err)
	}
}

func TestAStaleCacheNeverStopsAReleasedControllerManager(t *testing.T) {
	cp := restored(t, namespace(t), lodiav1alpha1.RestoreReleased, func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
	})
	setRestore(t, cp, gone)
	setTrue(t, cp, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ReasonApplied)
	eventually(t, func() error {
		if n := controllerManagers(t, cp); n != 2 {
			return fmt.Errorf("the released controller manager runs %d replicas", n)
		}
		return nil
	})
	update(t, cp, func(spec *lodiav1alpha1.ControlPlaneSpec) { spec.Paused = true })
	kcm := types.NamespacedName{Namespace: cp.Namespace, Name: "dev-kube-controller-manager"}
	injected := errors.New("injected: the API server did not answer")
	r := &workloads.Reconciler{Client: unpaused(func(key client.ObjectKey, obj client.Object) {
		if d, ok := obj.(*appsv1.Deployment); ok && key == kcm {
			d.Spec.Replicas = new(int32(0))
		}
	}), Reader: interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return injected
		},
	}), SignerImage: signerImage}
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !errors.Is(err, injected) {
		t.Fatalf("a pass that cannot read the controller manager from the server returned %v, want the read's error", err)
	}
	if n := controllerManagers(t, cp); n != 2 {
		t.Fatalf("a pass that cannot read the controller manager from the server left it at %d replicas, want 2", n)
	}
	r.Reader = apiReader
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); err != nil {
		t.Fatal(err)
	}
	if c := condition(t, cp); c.ObservedGeneration != get(t, cp).Generation {
		t.Fatalf("the pass on a stale cache did not render generation %d: APIServerReady is %+v", get(t, cp).Generation, c)
	}
	if n := controllerManagers(t, cp); n != 2 {
		t.Errorf("a pass whose cache holds the controller manager at 0 left it at %d replicas, want 2", n)
	}
}

func unpaused(stale ...func(client.ObjectKey, client.Object)) client.WithWatch {
	return interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				cp.Spec.Paused = false
			}
			for _, change := range stale {
				change(key, obj)
			}
			return nil
		},
	})
}

func restored(t *testing.T, ns string, release lodiav1alpha1.RestoreRelease, change ...func(*lodiav1alpha1.ControlPlaneSpec)) *lodiav1alpha1.ControlPlane {
	t.Helper()
	restoring := func(spec *lodiav1alpha1.ControlPlaneSpec) {
		spec.RestoreFrom = &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: "prod-backup"}, Release: release}
	}
	cp := create(t, ns, loadBalancer(), append([]func(*lodiav1alpha1.ControlPlaneSpec){restoring}, change...)...)
	setIngress(t, cp, corev1.LoadBalancerIngress{IP: lbAddress})
	inputsReady(t, cp)
	for _, component := range []string{"kube-apiserver", "kube-controller-manager", "kube-scheduler"} {
		markAvailable(t, eventuallyDeployment(t, cp, component))
	}
	return cp
}

func source(t *testing.T, ns string, finalizers ...string) *lodiav1alpha1.ControlPlane {
	t.Helper()
	prod := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: ns, Finalizers: finalizers},
		Spec: lodiav1alpha1.ControlPlaneSpec{
			Version:   version,
			Datastore: lodiav1alpha1.DatastoreChoice{Dedicated: &lodiav1alpha1.DedicatedDatastore{}},
			Endpoint:  loadBalancer(),
			Network:   lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}},
			Paused:    true,
		},
	}
	if err := k8s.Create(t.Context(), prod); err != nil {
		t.Fatal(err)
	}
	return prod
}

func setRestore(t *testing.T, cp *lodiav1alpha1.ControlPlane, of lodiav1alpha1.UIDReference) {
	t.Helper()
	applyStatus(t, cp, "test/datastore", map[string]any{"datastore": map[string]any{
		"endpoints": []any{map[string]any{"host": "dev-etcd-client." + cp.Namespace + ".svc", "port": int64(2379)}},
		"members":   map[string]any{"total": int64(1), "ready": int64(1)},
		"restore": map[string]any{
			"backup":      map[string]any{"name": "prod-backup", "uid": "5f3a9c1e-2b7d-4e8f-a6c0-d4b1e9f27a35"},
			"source":      map[string]any{"name": of.Name, "uid": string(of.UID)},
			"completedAt": time.Now().UTC().Format(time.RFC3339),
		},
	}})
}

func setFalse(t *testing.T, cp *lodiav1alpha1.ControlPlane, conditionType, reason string) {
	t.Helper()
	applyStatus(t, cp, "test/"+conditionType, map[string]any{"conditions": []any{map[string]any{
		"type":               conditionType,
		"status":             string(metav1.ConditionFalse),
		"reason":             reason,
		"message":            "set by the test",
		"observedGeneration": get(t, cp).Generation,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}}})
}

func controllerManagers(t *testing.T, cp *lodiav1alpha1.ControlPlane) int32 {
	t.Helper()
	return *deployment(t, cp, "kube-controller-manager").Spec.Replicas
}
