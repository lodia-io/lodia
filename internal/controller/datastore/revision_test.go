// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	druidapicommon "github.com/gardener/etcd-druid/api/common"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/testenv"
)

func unpaused(c client.WithWatch) client.WithWatch {
	return interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				cp.Spec.Paused = false
			}
			return nil
		},
	})
}

func operatorAt(c client.WithWatch, operatorRevision string) *datastore.Reconciler {
	return &datastore.Reconciler{Client: c, Reader: k8s, EtcdDruidNamespace: druidNamespace, EtcdDruidDeployment: druidDeployment, EtcdDruidVersion: druidVersion, Revision: operatorRevision, OperatorNamespace: operatorNamespace}
}

func passOf(t *testing.T, r *datastore.Reconciler, ns, name string) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
}

func mustPass(t *testing.T, r *datastore.Reconciler, ns, name string) ctrl.Result {
	t.Helper()
	result, err := passOf(t, r, ns, name)
	if err != nil {
		t.Fatalf("the pass of %s returned %v", name, err)
	}
	return result
}

func druidRuns(t *testing.T, ns, cp, run string, state druidapicommon.LastOperationState) {
	t.Helper()
	last, err := json.Marshal(druidapicommon.LastOperation{RunID: run, Type: druidv1alpha1.LastOperationTypeReconcile, State: state, Description: "etcd-druid's stand-in", LastUpdateTime: metav1.Now()})
	if err != nil {
		t.Fatal(err)
	}
	etcd := &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Name: cp + "-etcd", Namespace: ns}}
	if err := k8s.Status().Patch(t.Context(), etcd, client.RawPatch(types.MergePatchType, []byte(`{"status":{"lastOperation":`+string(last)+`}}`)), client.FieldOwner("etcd-druid")); err != nil {
		t.Fatal(err)
	}
}

func removeDruidAnnotation(t *testing.T, ns, cp string) {
	t.Helper()
	etcd := &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Name: cp + "-etcd", Namespace: ns}}
	patch := client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"annotations":{"`+druidv1alpha1.DruidOperationAnnotation+`":null}}}`))
	if err := k8s.Patch(t.Context(), etcd, patch, client.FieldOwner("etcd-druid")); err != nil {
		t.Fatal(err)
	}
}

func answer(t *testing.T, ns, cp string) {
	t.Helper()
	druidRuns(t, ns, cp, string(uuid.NewUUID()), druidv1alpha1.LastOperationStateSucceeded)
	removeDruidAnnotation(t, ns, cp)
}

func reconciled(t *testing.T, ns, cp string) *druidv1alpha1.Etcd {
	t.Helper()
	return waitEtcd(t, ns, cp, func(etcd *druidv1alpha1.Etcd) error {
		if druidv1alpha1.HasReconcileOperationAnnotation(etcd.ObjectMeta) {
			answer(t, ns, cp)
			return errors.New("etcd-druid had a request to answer")
		}
		return requestedAs(fmt.Sprintf("%d/%s", etcd.Generation, etcd.Annotations[lodiav1alpha1.OperatorRevisionAnnotation]), false)(etcd)
	})
}

func requestedAs(request string, pending bool) func(*druidv1alpha1.Etcd) error {
	return func(etcd *druidv1alpha1.Etcd) error {
		record := etcd.Annotations[lodiav1alpha1.EtcdReconcileRequestedAnnotation]
		if got := record[:max(0, strings.LastIndex(record, "/"))]; got != request {
			return fmt.Errorf("the request recorded is %q, want %q and a run ID", record, request)
		}
		if got := druidv1alpha1.HasReconcileOperationAnnotation(etcd.ObjectMeta); got != pending {
			return fmt.Errorf("etcd-druid's annotation is there: %t, want %t", got, pending)
		}
		return nil
	}
}

func stamped(etcd *druidv1alpha1.Etcd) string {
	return etcd.Annotations[lodiav1alpha1.OperatorRevisionAnnotation]
}

func heldTenant(t *testing.T, ns, name, class string, edit func(*lodiav1alpha1.ControlPlane)) *druidv1alpha1.Etcd {
	t.Helper()
	cp := controlPlane(ns, name, class)
	cp.Spec.Paused = true
	if edit != nil {
		edit(cp)
	}
	create(t, cp)
	grant(t, cp, previousRevision)
	writeGeneration(t, ns, name, 1)
	previous := operatorAt(unpaused(k8s), previousRevision)
	mustPass(t, previous, ns, name)
	answer(t, ns, name)
	runStatefulSet(t, ns, name, 1)
	setEtcdStatus(t, ns, name, nil)
	mustPass(t, previous, ns, name)
	etcd := getEtcd(t, ns, name)
	if err := requestedAs("1/"+previousRevision, false)(etcd); err != nil || stamped(etcd) != previousRevision || etcd.Annotations[lodiav1alpha1.EtcdFormedAnnotation] != string(etcd.UID) {
		t.Fatalf("the Etcd of the previous operator is annotated %v: %v", etcd.Annotations, err)
	}
	return etcd
}

func managedBy(t *testing.T, obj client.Object, operation metav1.ManagedFieldsOperationType) []string {
	t.Helper()
	var paths []string
	var walk func(prefix string, fields map[string]any)
	walk = func(prefix string, fields map[string]any) {
		for key, value := range fields {
			if key == "." {
				continue
			}
			child, _ := value.(map[string]any)
			if len(child) == 0 || (len(child) == 1 && child["."] != nil) {
				paths = append(paths, prefix+key)
			}
			walk(prefix+key+" ", child)
		}
	}
	for _, entry := range obj.GetManagedFields() {
		if entry.Manager != datastore.FieldManager || entry.Operation != operation || entry.FieldsV1 == nil {
			continue
		}
		fields := map[string]any{}
		if err := json.Unmarshal(entry.FieldsV1.GetRawBytes(), &fields); err != nil {
			t.Fatal(err)
		}
		walk("", fields)
	}
	slices.Sort(paths)
	return paths
}

func appliedAnnotations(t *testing.T, etcd *druidv1alpha1.Etcd) []string {
	t.Helper()
	var keys []string
	for _, path := range managedBy(t, etcd, metav1.ManagedFieldsOperationApply) {
		if key, ok := strings.CutPrefix(path, "f:metadata f:annotations f:"); ok {
			keys = append(keys, key)
		}
	}
	return keys
}

func noRequestApplied(t *testing.T, etcd *druidv1alpha1.Etcd) {
	t.Helper()
	for _, key := range appliedAnnotations(t, etcd) {
		if key == druidv1alpha1.DruidOperationAnnotation || key == lodiav1alpha1.EtcdReconcileRequestedAnnotation {
			t.Errorf("the apply of Etcd %s holds the annotation %s: %v", etcd.Name, key, appliedAnnotations(t, etcd))
		}
	}
}

func TestAHeldPassChangesNothingButTheCertificateReferences(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	const cp = "held-copy"
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass("held-copy", credentialsNamespace))
	before := heldTenant(t, ns, cp, "held-copy", func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
	})
	if _, ok := before.Annotations["resources.druid.gardener.cloud/allow-unhealthy-pod-eviction"]; !ok {
		t.Fatalf("the Etcd of a highly available ControlPlane is annotated %v", before.Annotations)
	}

	person := client.RawPatch(types.MergePatchType, []byte(`{"spec":{"etcd":{"metrics":"extensive"}}}`))
	if err := k8s.Patch(t.Context(), before.DeepCopy(), person, client.FieldOwner("kubectl-edit")); err != nil {
		t.Fatal(err)
	}
	mustPass(t, operatorAt(unpaused(k8s), previousRevision), ns, cp)
	answer(t, ns, cp)
	editClass(t, "held-copy", func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Etcd.QuotaBackendBytes = new(resource.MustParse("3Gi"))
		class.Spec.Placement.PriorityClassName = "another"
	})
	setHighAvailability(t, ns, cp, nil)
	before = getEtcd(t, ns, cp)
	if err := requestedAs(fmt.Sprintf("%d/%s", before.Generation, previousRevision), false)(before); err != nil {
		t.Fatalf("a person's edit of the Etcd: %v", err)
	}
	ownedBefore := managedBy(t, before, metav1.ManagedFieldsOperationApply)

	held := operatorAt(unpaused(k8s), revision)
	mustPass(t, held, ns, cp)
	after := getEtcd(t, ns, cp)
	if !reflect.DeepEqual(after.Spec, before.Spec) {
		t.Errorf("a held pass changed the Etcd's spec from\n%+v\nto\n%+v", before.Spec, after.Spec)
	}
	if !reflect.DeepEqual(after.Annotations, before.Annotations) {
		t.Errorf("a held pass changed the Etcd's annotations from %v to %v", before.Annotations, after.Annotations)
	}
	var gained []string
	for _, path := range managedBy(t, after, metav1.ManagedFieldsOperationApply) {
		if !slices.Contains(ownedBefore, path) {
			gained = append(gained, path)
		}
	}
	t.Logf("the copy made %s's apply own %v", datastore.FieldManager, gained)
	if !slices.Equal(gained, []string{"f:spec f:etcd f:metrics"}) {
		t.Errorf("the copy made %s's apply own %v, want the person's spec.etcd.metrics alone", datastore.FieldManager, gained)
	}
	noRequestApplied(t, after)
	settled := after.ResourceVersion
	mustPass(t, held, ns, cp)
	if again := getEtcd(t, ns, cp); again.ResourceVersion != settled {
		t.Errorf("a held pass with no new certificate generation moved the Etcd from resourceVersion %s to %s", settled, again.ResourceVersion)
	}

	stale := getEtcd(t, ns, cp)
	writeGeneration(t, ns, cp, 2)
	missing := operatorAt(unpaused(k8s), revision)
	missing.EtcdDruidDeployment = "absent"
	mustPass(t, missing, ns, cp)
	rolled := getEtcd(t, ns, cp)
	if err := namesGeneration(2)(rolled); err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(withoutTLS(rolled), withoutTLS(before)) {
		t.Errorf("a held pass on a new certificate generation changed more of the spec than the references:\n%+v\nwas\n%+v", withoutTLS(rolled), withoutTLS(before))
	}
	for key, value := range before.Annotations {
		if key != lodiav1alpha1.EtcdReconcileRequestedAnnotation && rolled.Annotations[key] != value {
			t.Errorf("a held pass on a new certificate generation changed the annotation %s from %q to %q", key, value, rolled.Annotations[key])
		}
	}
	if err := requestedAs(fmt.Sprintf("%d/%s", rolled.Generation, previousRevision), true)(rolled); err != nil {
		t.Errorf("a new certificate generation under a hold: %v", err)
	}
	noRequestApplied(t, rolled)

	answer(t, ns, cp)
	current := getEtcd(t, ns, cp).ResourceVersion
	staleCache := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if etcd, ok := obj.(*druidv1alpha1.Etcd); ok {
				stale.DeepCopyInto(etcd)
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	if _, err := passOf(t, operatorAt(staleCache, revision), ns, cp); !apierrors.IsConflict(err) {
		t.Errorf("a held pass on a stale Etcd returned %v, want a conflict", err)
	}
	if got := getEtcd(t, ns, cp).ResourceVersion; got != current {
		t.Errorf("a held pass on a stale Etcd moved it from resourceVersion %s to %s", current, got)
	}

	grant(t, getControlPlane(t, ns, cp), revision)
	mustPass(t, held, ns, cp)
	granted := getEtcd(t, ns, cp)
	if quota := quantity(granted.Spec.Etcd.Quota); quota != "3Gi" || granted.Spec.Etcd.Metrics == nil || *granted.Spec.Etcd.Metrics != druidv1alpha1.Extensive || stamped(granted) != revision {
		t.Errorf("the grant left quota %s, metrics %v and stamp %s, want the class's 3Gi, the person's extensive kept and %s", quota, granted.Spec.Etcd.Metrics, stamped(granted), revision)
	}
}

func TestAHeldPassCreatesAMissingEtcdFromTheFullRender(t *testing.T) {
	ns := namespace(t)
	const cp = "held-create"
	createClass(t, newClass("held-create", "Retain"))
	plane := controlPlane(ns, cp, "held-create")
	plane.Spec.Paused = true
	create(t, plane)
	grant(t, plane, previousRevision)
	writeGeneration(t, ns, cp, 1)
	mustPass(t, operatorAt(unpaused(k8s), revision), ns, cp)
	etcd := getEtcd(t, ns, cp)
	if stamped(etcd) != revision || etcd.Annotations[lodiav1alpha1.DatastoreClassAnnotation] != "held-create" || quantity(etcd.Spec.Etcd.Quota) != "4Gi" || quantity(etcd.Spec.StorageCapacity) != "8Gi" {
		t.Errorf("a held pass created an Etcd stamped %q, of class %q, quota %s, capacity %s; want the full render of class held-create, stamped %s",
			stamped(etcd), etcd.Annotations[lodiav1alpha1.DatastoreClassAnnotation], quantity(etcd.Spec.Etcd.Quota), quantity(etcd.Spec.StorageCapacity), revision)
	}
	if err := requestedAs("1/"+revision, true)(etcd); err != nil {
		t.Errorf("an Etcd created under a hold: %v", err)
	}
	noRequestApplied(t, etcd)
	sameSpec(t, getPolicy(t, ns, cp).Spec, policySpec(cp, false))

	runStatefulSet(t, ns, cp, 1)
	setEtcdStatus(t, ns, cp, nil)
	mustPass(t, operatorAt(unpaused(k8s), revision), ns, cp)
	if etcd := getEtcd(t, ns, cp); etcd.Annotations[lodiav1alpha1.EtcdFormedAnnotation] != string(etcd.UID) {
		t.Errorf("a held pass over an Etcd with a ready member left it annotated %v", etcd.Annotations)
	}
}

func TestAHeldPassWritesTheNetworkPolicyOnlyWhenItIsMissing(t *testing.T) {
	ns := namespace(t)
	const cp = "held-policy"
	createClass(t, newClass("held-policy", "Retain"))
	heldTenant(t, ns, cp, "held-policy", nil)
	held := operatorAt(unpaused(k8s), revision)
	held.EtcdDruidNamespace = "another-druid-system"

	if err := k8s.Delete(t.Context(), getPolicy(t, ns, cp)); err != nil {
		t.Fatal(err)
	}
	mustPass(t, held, ns, cp)
	created := getPolicy(t, ns, cp)
	if !metav1.IsControlledBy(created, getControlPlane(t, ns, cp)) {
		t.Errorf("the NetworkPolicy a held pass created is owned by %+v", created.OwnerReferences)
	}

	edited := created.DeepCopy()
	edited.Spec.Ingress = edited.Spec.Ingress[:1]
	if err := k8s.Update(t.Context(), edited); err != nil {
		t.Fatal(err)
	}
	mustPass(t, held, ns, cp)
	if got := getPolicy(t, ns, cp); got.ResourceVersion != edited.ResourceVersion || len(got.Spec.Ingress) != 1 {
		t.Errorf("a held pass wrote the NetworkPolicy that exists: resourceVersion %s, was %s, %d ingress rules", got.ResourceVersion, edited.ResourceVersion, len(got.Spec.Ingress))
	}

	if err := k8s.Delete(t.Context(), getPolicy(t, ns, cp)); err != nil {
		t.Fatal(err)
	}
	foreign := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: cp + "-etcd", Namespace: ns, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "someone", UID: "5e1f7c3a-0000-4000-8000-0000000000aa", Controller: new(true)}}},
		Spec:       networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{}},
	}
	create(t, foreign)
	mustPass(t, held, ns, cp)
	ready := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
	if ready.Reason != lodiav1alpha1.ReasonObjectsControlledElsewhere || !strings.Contains(ready.Message, "another owner keeps NetworkPolicy "+cp+"-etcd by Deployment someone") {
		t.Errorf("a held pass over another owner's NetworkPolicy reports %s %q", ready.Reason, ready.Message)
	}
	if got := getPolicy(t, ns, cp); got.ResourceVersion != foreign.ResourceVersion {
		t.Errorf("a held pass wrote another owner's NetworkPolicy")
	}
}

func TestTheRequestFollowsEveryWriteOfTheEtcd(t *testing.T) {
	ns := namespace(t)
	const cp = "requested"
	createClass(t, newClass("requested", "Retain"))
	create(t, controlPlane(ns, cp, "requested"))
	writeGeneration(t, ns, cp, 1)
	created := waitEtcd(t, ns, cp, requestedAs("1/"+revision, true))
	noRequestApplied(t, created)
	if updated := managedBy(t, created, metav1.ManagedFieldsOperationUpdate); !slices.Equal(updated, []string{
		"f:metadata f:annotations f:" + druidv1alpha1.DruidOperationAnnotation,
		"f:metadata f:annotations f:" + lodiav1alpha1.EtcdReconcileRequestedAnnotation,
	}) {
		t.Errorf("the request owns %v, want the two annotations", updated)
	}

	writeGeneration(t, ns, cp, 2)
	waitEtcd(t, ns, cp, namesGeneration(2))
	consistently(t, func() error { return requestedAs("1/"+revision, true)(getEtcd(t, ns, cp)) })

	answer(t, ns, cp)
	testenv.EventuallyWithin(t, 5*time.Second, func() error { return requestedAs("2/"+revision, true)(getEtcd(t, ns, cp)) })
	noRequestApplied(t, getEtcd(t, ns, cp))

	answer(t, ns, cp)
	waitEtcd(t, ns, cp, requestedAs("2/"+revision, false))
	consistently(t, func() error { return requestedAs("2/"+revision, false)(getEtcd(t, ns, cp)) })

	setHighAvailability(t, ns, cp, &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"})
	runStatefulSet(t, ns, cp, 2)
	setEtcdStatus(t, ns, cp, nil)
	third := waitEtcd(t, ns, cp, requestedAs("3/"+revision, true))
	if third.Spec.Replicas != 3 {
		t.Errorf("the Etcd asks for %d members", third.Spec.Replicas)
	}
	noRequestApplied(t, third)
}

func TestARequestThatConflictsIsMadeByTheNextPass(t *testing.T) {
	ns := namespace(t)
	const cp = "request-conflict"
	createClass(t, newClass("request-conflict", "Retain"))
	plane := controlPlane(ns, cp, "request-conflict")
	plane.Spec.Paused = true
	create(t, plane)
	writeGeneration(t, ns, cp, 1)
	r := operatorAt(unpaused(k8s), revision)
	mustPass(t, r, ns, cp)
	answer(t, ns, cp)
	runStatefulSet(t, ns, cp, 1)
	setEtcdStatus(t, ns, cp, nil)
	mustPass(t, r, ns, cp)

	writeGeneration(t, ns, cp, 2)
	racing := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*druidv1alpha1.Etcd); ok {
				setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) {
					s.ObservedGeneration, s.Conditions[0].LastUpdateTime = new(int64(1)), metav1.NewTime(time.Now().Add(time.Hour))
				})
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	if result, err := passOf(t, operatorAt(racing, revision), ns, cp); err != nil || result.RequeueAfter != 15*time.Second {
		t.Errorf("a pass whose request conflicts returned %v and a requeue after %s, want none and 15s", err, result.RequeueAfter)
	}
	etcd := getEtcd(t, ns, cp)
	if err := namesGeneration(2)(etcd); err != nil {
		t.Fatal(err)
	}
	if err := requestedAs("1/"+revision, false)(etcd); err != nil {
		t.Errorf("a request that conflicts: %v", err)
	}
	mustPass(t, r, ns, cp)
	if err := requestedAs(fmt.Sprintf("%d/%s", etcd.Generation, revision), true)(getEtcd(t, ns, cp)); err != nil {
		t.Errorf("the pass after a conflict: %v", err)
	}

	answer(t, ns, cp)
	writeGeneration(t, ns, cp, 3)
	injected := errors.New("injected: the operator stopped between the apply and the request")
	stopping := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*druidv1alpha1.Etcd); ok {
				return injected
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	if _, err := passOf(t, operatorAt(stopping, revision), ns, cp); !errors.Is(err, injected) {
		t.Fatalf("a pass that stops before its request returned %v", err)
	}
	applied := getEtcd(t, ns, cp)
	if err := namesGeneration(3)(applied); err != nil {
		t.Fatal(err)
	}
	if applied.Annotations[lodiav1alpha1.EtcdReconcileRequestedAnnotation] == fmt.Sprintf("%d/%s", applied.Generation, revision) {
		t.Fatal("the request was made")
	}
	mustPass(t, operatorAt(unpaused(k8s), revision), ns, cp)
	if err := requestedAs(fmt.Sprintf("%d/%s", applied.Generation, revision), true)(getEtcd(t, ns, cp)); err != nil {
		t.Errorf("the first pass of a restarted operator: %v", err)
	}
}

func TestARequestTheAPIServerRefusesIsRefused(t *testing.T) {
	ns := namespace(t)
	const cp = "refused-request"
	createClass(t, newClass(cp, "Retain"))
	heldTenant(t, ns, cp, cp, nil)
	grant(t, getControlPlane(t, ns, cp), revision)
	refused := apierrors.NewForbidden(schema.GroupResource{Group: druidv1alpha1.SchemeGroupVersion.Group, Resource: "etcds"}, cp+"-etcd", errors.New("denied by a policy on "+druidv1alpha1.DruidOperationAnnotation))
	refusing := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*druidv1alpha1.Etcd); ok {
				return refused
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	if _, err := passOf(t, operatorAt(refusing, revision), ns, cp); !errors.Is(err, refused) {
		t.Fatalf("a pass whose request the API server refuses returned %v, want the refusal", err)
	}
	plane := getControlPlane(t, ns, cp)
	ready := meta.FindStatusCondition(plane.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
	want := "the API server refuses the request that etcd-druid reconcile Etcd " + cp + "-etcd, the annotations " + druidv1alpha1.DruidOperationAnnotation + " and " + lodiav1alpha1.EtcdReconcileRequestedAnnotation + "; the reason is in Lodia's log"
	if ready.Status != metav1.ConditionFalse || ready.Reason != lodiav1alpha1.ReasonRefused || ready.Message != want {
		t.Errorf("a refused request leaves DatastoreReady %s %s %q, want False %s %q", ready.Status, ready.Reason, ready.Message, lodiav1alpha1.ReasonRefused, want)
	}
	if datastore := plane.Status.Datastore; datastore == nil || len(datastore.Endpoints) != 1 || datastore.OperatorRevision != previousRevision || datastore.Members.Updated == nil {
		t.Errorf("a refused request leaves status.datastore %+v, want the applied Etcd's, members updated, and %s kept", datastore, previousRevision)
	}
	if backup := meta.FindStatusCondition(plane.Status.Conditions, lodiav1alpha1.ConditionBackupReady); backup.Reason != lodiav1alpha1.ReasonNoBackupStore {
		t.Errorf("a refused request leaves BackupReady %s, want the applied Etcd's %s", backup.Reason, lodiav1alpha1.ReasonNoBackupStore)
	}
	if etcd := getEtcd(t, ns, cp); stamped(etcd) != revision {
		t.Errorf("the pass whose request is refused applied the Etcd stamped %s, want %s", stamped(etcd), revision)
	}

	mustPass(t, operatorAt(unpaused(k8s), revision), ns, cp)
	if err := requestedAs("1/"+revision, true)(getEtcd(t, ns, cp)); err != nil {
		t.Errorf("the pass after the refusal: %v", err)
	}
	if ready := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); ready.Reason != lodiav1alpha1.ReasonReady {
		t.Errorf("the pass after the refusal leaves DatastoreReady %s %q", ready.Reason, ready.Message)
	}
}

func druidAt(t *testing.T, name, version string, status appsv1.DeploymentStatus) {
	t.Helper()
	labels := map[string]string{"app.kubernetes.io/name": name}
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: druidNamespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: new(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "etcd-druid", Image: "europe-docker.pkg.dev/gardener-project/releases/gardener/etcd-druid:" + version}}},
			},
		},
	}
	create(t, d)
	t.Cleanup(func() {
		if err := client.IgnoreNotFound(k8s.Delete(context.Background(), d)); err != nil {
			t.Error(err)
		}
	})
	status.ObservedGeneration = d.Generation
	d.Status = status
	if err := k8s.Status().Update(t.Context(), d); err != nil {
		t.Fatal(err)
	}
}

func TestAGrantAsksEtcdDruidOnceItRunsTheGivenVersion(t *testing.T) {
	ns := namespace(t)
	const cp = "druid-gate"
	createClass(t, newClass("druid-gate", "Retain"))
	heldTenant(t, ns, cp, "druid-gate", nil)
	editClass(t, "druid-gate", func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Etcd = &lodiav1alpha1.EtcdSettings{QuotaBackendBytes: new(resource.MustParse("3Gi"))}
	})
	mustPass(t, operatorAt(unpaused(k8s), revision), ns, cp)
	if quota := quantity(getEtcd(t, ns, cp).Spec.Etcd.Quota); quota != "4Gi" {
		t.Fatalf("a held pass applied the class edit: quota %s", quota)
	}
	grant(t, getControlPlane(t, ns, cp), revision)

	rolled := appsv1.DeploymentStatus{Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	druidAt(t, "druid-rolling", druidVersion, appsv1.DeploymentStatus{Replicas: 2, UpdatedReplicas: 1, ReadyReplicas: 2, AvailableReplicas: 2})
	druidAt(t, "druid-older", "v0.38.0", rolled)
	terminating := rolled
	terminating.TerminatingReplicas = new(int32(1))
	druidAt(t, "druid-terminating", druidVersion, terminating)
	for _, c := range []struct{ deployment, waits string }{
		{"druid-absent", "once Deployment " + druidNamespace + "/druid-absent exists"},
		{"druid-rolling", "once Deployment " + druidNamespace + "/druid-rolling has rolled out etcd-druid " + druidVersion + " to every replica"},
		{"druid-older", "once Deployment " + druidNamespace + "/druid-older runs etcd-druid " + druidVersion},
		{"druid-terminating", "once no pod of Deployment " + druidNamespace + "/druid-terminating terminates"},
	} {
		t.Run(c.deployment, func(t *testing.T) {
			r := operatorAt(unpaused(k8s), revision)
			r.EtcdDruidDeployment = c.deployment
			if result := mustPass(t, r, ns, cp); result.RequeueAfter != 15*time.Second {
				t.Errorf("a request that waits for etcd-druid requeues after %s, want 15s", result.RequeueAfter)
			}
			etcd := getEtcd(t, ns, cp)
			if quota := quantity(etcd.Spec.Etcd.Quota); quota != "3Gi" || stamped(etcd) != revision {
				t.Errorf("the pass that sees the grant left quota %s and stamp %s, want the class's 3Gi and %s", quota, stamped(etcd), revision)
			}
			if err := requestedAs("1/"+previousRevision, false)(etcd); err != nil {
				t.Errorf("a request for a new revision while etcd-druid's Deployment does not run its version: %v", err)
			}
			ready := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
			if want := "etcd-druid is asked to reconcile Etcd " + cp + "-etcd under operator revision " + revision + " " + c.waits; ready.Status != metav1.ConditionTrue || !strings.HasSuffix(ready.Message, want) {
				t.Errorf("DatastoreReady is %s %q, want True ending %q", ready.Status, ready.Message, want)
			}
		})
	}

	mustPass(t, operatorAt(unpaused(k8s), revision), ns, cp)
	etcd := getEtcd(t, ns, cp)
	if err := requestedAs(fmt.Sprintf("%d/%s", etcd.Generation, revision), true)(etcd); err != nil {
		t.Errorf("a request once etcd-druid runs its version in every replica: %v", err)
	}
	if ready := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); strings.Contains(ready.Message, "etcd-druid is asked") {
		t.Errorf("DatastoreReady still notes the wait: %q", ready.Message)
	}

	answer(t, ns, cp)
	writeGeneration(t, ns, cp, 2)
	absent := operatorAt(unpaused(k8s), revision)
	absent.EtcdDruidDeployment = "druid-absent"
	mustPass(t, absent, ns, cp)
	etcd = getEtcd(t, ns, cp)
	if err := requestedAs(fmt.Sprintf("%d/%s", etcd.Generation, revision), true)(etcd); err != nil {
		t.Errorf("a spec change under the same revision, while etcd-druid's Deployment is absent: %v", err)
	}
}

func reportedRevision(t *testing.T, ns, cp string) string {
	t.Helper()
	datastore := getControlPlane(t, ns, cp).Status.Datastore
	if datastore == nil {
		t.Fatal("no status.datastore")
	}
	return datastore.OperatorRevision
}

func TestTheOperatorRevisionIsReportedOnceEtcdDruidRolledItOut(t *testing.T) {
	ns := namespace(t)
	const cp = "reported-revision"
	createClass(t, newClass("reported-revision", "Retain"))
	heldTenant(t, ns, cp, "reported-revision", nil)
	if got := reportedRevision(t, ns, cp); got != previousRevision {
		t.Fatalf("status.datastore.operatorRevision is %q once etcd-druid reconciled the Etcd and the member runs it, want %s", got, previousRevision)
	}
	grant(t, getControlPlane(t, ns, cp), revision)
	r := operatorAt(unpaused(k8s), revision)
	keeps := func(t *testing.T) {
		t.Helper()
		mustPass(t, r, ns, cp)
		if got := reportedRevision(t, ns, cp); got != previousRevision {
			t.Errorf("status.datastore.operatorRevision is %q, want %s kept", got, previousRevision)
		}
	}

	t.Run("the record differs", func(t *testing.T) {
		waiting := operatorAt(unpaused(k8s), revision)
		waiting.EtcdDruidDeployment = "absent"
		if result := mustPass(t, waiting, ns, cp); result.RequeueAfter != 15*time.Second {
			t.Errorf("a pass whose record differs from a settled Etcd requeues after %s, want 15s", result.RequeueAfter)
		}
		if got := reportedRevision(t, ns, cp); got != previousRevision {
			t.Errorf("status.datastore.operatorRevision is %q, want %s kept", got, previousRevision)
		}
	})
	t.Run("the cache holds an older report", func(t *testing.T) {
		older := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if plane, ok := obj.(*lodiav1alpha1.ControlPlane); ok && plane.Status.Datastore != nil {
					plane.Status.Datastore.OperatorRevision = "v0.0.0-test+older"
				}
				return nil
			},
		})
		waiting := operatorAt(older, revision)
		waiting.EtcdDruidDeployment = "absent"
		setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) { s.AvailableReplicas = 0 })
		mustPass(t, waiting, ns, cp)
		if got := reportedRevision(t, ns, cp); got != previousRevision {
			t.Errorf("status.datastore.operatorRevision is %q, want %s, as the API server holds it", got, previousRevision)
		}
		setStatefulSetStatus(t, ns, cp, nil)
	})
	t.Run("etcd-druid's annotation is there", func(t *testing.T) {
		keeps(t)
		if err := requestedAs("1/"+revision, true)(getEtcd(t, ns, cp)); err != nil {
			t.Fatal(err)
		}
		keeps(t)
	})
	answer(t, ns, cp)
	t.Run("etcd-druid has not observed the generation", func(t *testing.T) {
		setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) { *s.ObservedGeneration-- })
		keeps(t)
		setEtcdStatus(t, ns, cp, nil)
	})
	t.Run("the StatefulSet has not observed its generation", func(t *testing.T) {
		setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) { s.ObservedGeneration-- })
		keeps(t)
	})
	t.Run("a member is not updated", func(t *testing.T) {
		setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) { s.UpdatedReplicas = 0 })
		keeps(t)
	})
	setStatefulSetStatus(t, ns, cp, nil)
	mustPass(t, r, ns, cp)
	if got := reportedRevision(t, ns, cp); got != revision {
		t.Errorf("status.datastore.operatorRevision is %q once every part holds, want %s", got, revision)
	}
}

func TestTheRevisionIsReportedFromTheStatefulSetReadAfterTheRequest(t *testing.T) {
	for i, row := range []struct {
		name   string
		status bool
	}{
		{"etcd-druid removes its annotation", false},
		{"etcd-druid writes its status and removes its annotation", true},
	} {
		t.Run(row.name, func(t *testing.T) {
			ns := namespace(t)
			cp := fmt.Sprintf("sts-after-request-%d", i)
			createClass(t, newClass(cp, "Retain"))
			heldTenant(t, ns, cp, cp, nil)
			grant(t, getControlPlane(t, ns, cp), revision)
			r := operatorAt(unpaused(k8s), revision)
			mustPass(t, r, ns, cp)
			if err := requestedAs("1/"+revision, true)(getEtcd(t, ns, cp)); err != nil {
				t.Fatal(err)
			}
			done := false
			racing := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
				Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
					if !done {
						done = true
						sts := &appsv1.StatefulSet{}
						if err := k8s.Get(ctx, client.ObjectKey{Namespace: ns, Name: cp + "-etcd"}, sts); err != nil {
							return err
						}
						sts.Spec.Template.Annotations = map[string]string{"etcd-druid": "the render of " + druidVersion}
						if err := k8s.Update(ctx, sts); err != nil {
							return err
						}
						if row.status {
							setEtcdStatus(t, ns, cp, nil)
						}
						answer(t, ns, cp)
					}
					return c.Apply(ctx, obj, opts...)
				},
			})
			mustPass(t, operatorAt(racing, revision), ns, cp)
			datastore := getControlPlane(t, ns, cp).Status.Datastore
			if datastore.OperatorRevision != previousRevision || datastore.Members.Updated != nil {
				t.Errorf("a pass that read the StatefulSet before etcd-druid rolled it reports revision %q and %v members updated, want %s kept and none counted", datastore.OperatorRevision, datastore.Members.Updated, previousRevision)
			}
			mustPass(t, r, ns, cp)
			if got := reportedRevision(t, ns, cp); got != previousRevision {
				t.Errorf("the next pass reports %q while the StatefulSet has not observed etcd-druid's roll, want %s kept", got, previousRevision)
			}
			setStatefulSetStatus(t, ns, cp, nil)
			mustPass(t, r, ns, cp)
			if got := reportedRevision(t, ns, cp); got != revision {
				t.Errorf("status.datastore.operatorRevision is %q once the StatefulSet has rolled, want %s", got, revision)
			}
		})
	}
}

func recordedRun(t *testing.T, ns, cp string) string {
	t.Helper()
	record := getEtcd(t, ns, cp).Annotations[lodiav1alpha1.EtcdReconcileRequestedAnnotation]
	return record[strings.LastIndex(record, "/")+1:]
}

func druidEnds(t *testing.T, ns, cp, run string) {
	t.Helper()
	druidRuns(t, ns, cp, run, druidv1alpha1.LastOperationStateSucceeded)
	setEtcdStatus(t, ns, cp, nil)
	removeDruidAnnotation(t, ns, cp)
}

func asksAgain(t *testing.T, r *datastore.Reconciler, ns, cp, request, run string) {
	t.Helper()
	if result := mustPass(t, r, ns, cp); result.RequeueAfter != 15*time.Second {
		t.Errorf("a pass over a request that is not served requeues after %s, want 15s", result.RequeueAfter)
	}
	if err := requestedAs(request, true)(getEtcd(t, ns, cp)); err != nil {
		t.Errorf("a request etcd-druid's annotation is gone from, not served: %v", err)
	}
	if got := recordedRun(t, ns, cp); got != run {
		t.Errorf("the request asked again records run ID %q, want %q", got, run)
	}
}

func TestARequestConsumedByAReconcileThatStartedBeforeItIsAskedAgain(t *testing.T) {
	t.Run("etcd-druid's first reconcile of a new Etcd", func(t *testing.T) {
		ns := namespace(t)
		const cp = "new-etcd-request"
		createClass(t, newClass(cp, "Retain"))
		plane := controlPlane(ns, cp, cp)
		plane.Spec.Paused = true
		create(t, plane)
		writeGeneration(t, ns, cp, 1)
		first := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*druidv1alpha1.Etcd); ok {
					druidRuns(t, ns, cp, "first", druidv1alpha1.LastOperationStateProcessing)
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		})
		mustPass(t, operatorAt(first, revision), ns, cp)
		if err := requestedAs("", false)(getEtcd(t, ns, cp)); err != nil {
			t.Fatalf("a request that conflicts with etcd-druid's first status write: %v", err)
		}
		writeGeneration(t, ns, cp, 2)
		r := operatorAt(unpaused(k8s), revision)
		mustPass(t, r, ns, cp)
		request := fmt.Sprintf("%d/%s", getEtcd(t, ns, cp).Generation, revision)
		if err := requestedAs(request, true)(getEtcd(t, ns, cp)); err != nil || recordedRun(t, ns, cp) != "first" {
			t.Fatalf("the request made while etcd-druid's first reconcile runs records run ID %q: %v", recordedRun(t, ns, cp), err)
		}
		runStatefulSet(t, ns, cp, 1)
		druidEnds(t, ns, cp, "first")
		asksAgain(t, r, ns, cp, request, "first")
		answer(t, ns, cp)
		mustPass(t, r, ns, cp)
		if err := requestedAs(request, false)(getEtcd(t, ns, cp)); err != nil {
			t.Errorf("a request etcd-druid served: %v", err)
		}
	})

	ns := namespace(t)
	const cp = "served-request"
	createClass(t, newClass(cp, "Retain"))
	heldTenant(t, ns, cp, cp, nil)
	r := operatorAt(unpaused(k8s), revision)
	t.Run("a reconcile etcd-druid started on a stale cache", func(t *testing.T) {
		druidRuns(t, ns, cp, "stale", druidv1alpha1.LastOperationStateProcessing)
		grant(t, getControlPlane(t, ns, cp), revision)
		mustPass(t, r, ns, cp)
		request := fmt.Sprintf("%d/%s", getEtcd(t, ns, cp).Generation, revision)
		if err := requestedAs(request, true)(getEtcd(t, ns, cp)); err != nil || recordedRun(t, ns, cp) != "stale" {
			t.Fatalf("the grant's request records run ID %q: %v", recordedRun(t, ns, cp), err)
		}
		druidEnds(t, ns, cp, "stale")
		asksAgain(t, r, ns, cp, request, "stale")
		if got := reportedRevision(t, ns, cp); got != previousRevision {
			t.Errorf("status.datastore.operatorRevision is %q while the request is not served, want %s kept", got, previousRevision)
		}

		answer(t, ns, cp)
		if result := mustPass(t, r, ns, cp); result.RequeueAfter != 0 {
			t.Errorf("a pass over a served request on a settled etcd requeues after %s", result.RequeueAfter)
		}
		if err := requestedAs(request, false)(getEtcd(t, ns, cp)); err != nil {
			t.Errorf("a request etcd-druid served: %v", err)
		}
		if got := reportedRevision(t, ns, cp); got != revision {
			t.Errorf("status.datastore.operatorRevision is %q once the request is served, want %s", got, revision)
		}
	})

	for i, c := range []struct {
		name   string
		druid  func(t *testing.T)
		served bool
		run    string
	}{
		{"an annotation removed without a reconcile", func(*testing.T) {}, false, ""},
		{"a reconcile under another run ID that failed", func(t *testing.T) { druidRuns(t, ns, cp, "failed", druidv1alpha1.LastOperationStateError) }, false, "failed"},
		{"a create under another run ID that succeeded", func(t *testing.T) {
			last := `{"status":{"lastOperation":{"runID":"created","type":"Create","state":"Succeeded","description":"etcd-druid's stand-in","lastUpdateTime":"2026-11-10T00:00:00Z"}}}`
			if err := k8s.Status().Patch(t.Context(), getEtcd(t, ns, cp), client.RawPatch(types.MergePatchType, []byte(last))); err != nil {
				t.Fatal(err)
			}
		}, true, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			writeGeneration(t, ns, cp, i+2)
			mustPass(t, r, ns, cp)
			request, run := fmt.Sprintf("%d/%s", getEtcd(t, ns, cp).Generation, revision), recordedRun(t, ns, cp)
			if err := requestedAs(request, true)(getEtcd(t, ns, cp)); err != nil {
				t.Fatal(err)
			}
			c.druid(t)
			removeDruidAnnotation(t, ns, cp)
			if !c.served {
				asksAgain(t, r, ns, cp, request, cmp.Or(c.run, run))
				answer(t, ns, cp)
				return
			}
			mustPass(t, r, ns, cp)
			if err := requestedAs(request, false)(getEtcd(t, ns, cp)); err != nil {
				t.Errorf("a request served by a create: %v", err)
			}
		})
	}
}

func TestAHeldPassAtANewGenerationReportsTheQuorumAtTheGenerationItDescribes(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	const cp = "held-generation"
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass("held-generation", credentialsNamespace))
	heldTenant(t, ns, cp, "held-generation", nil)
	setEtcdStatus(t, ns, cp, druidBackupReady(druidv1alpha1.ConditionTrue, "BackupSucceeded", "Snapshot backup succeeded"))
	r := operatorAt(unpaused(k8s), revision)
	mustPass(t, r, ns, cp)
	setHighAvailability(t, ns, cp, &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"})
	generation := getControlPlane(t, ns, cp).Generation

	check := func(conditionType string, status metav1.ConditionStatus, reason, message string) {
		t.Helper()
		c := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, conditionType)
		if c == nil || c.Status != status || c.Reason != reason || c.Message != message || c.ObservedGeneration != 1 {
			t.Errorf("%s is %+v, want %s %s %q at generation 1 while the ControlPlane is at %d", conditionType, c, status, reason, message, generation)
		}
	}
	setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) {
		s.Ready, s.ReadyReplicas = new(false), 0
		druidBackupReady(druidv1alpha1.ConditionFalse, "BackupFailed", "Stale snapshot leases")(s)
	})
	mustPass(t, r, ns, cp)
	check(lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost, "Etcd "+cp+"-etcd has 0 of 1 members ready")
	check(lodiav1alpha1.ConditionBackupReady, metav1.ConditionFalse, lodiav1alpha1.ReasonBackupFailed, "etcd-druid reports of Etcd "+cp+"-etcd: Stale snapshot leases")
	if etcd := getEtcd(t, ns, cp); etcd.Spec.Replicas != 1 {
		t.Errorf("a held pass gave the Etcd %d members", etcd.Spec.Replicas)
	}

	setEtcdStatus(t, ns, cp, druidBackupReady(druidv1alpha1.ConditionTrue, "BackupSucceeded", "Snapshot backup succeeded"))
	mustPass(t, r, ns, cp)
	check(lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd "+cp+"-etcd has quorum, 1 of 1 members ready")
	check(lodiav1alpha1.ConditionBackupReady, metav1.ConditionTrue, lodiav1alpha1.ReasonBackupSucceeded, "etcd-druid reports of Etcd "+cp+"-etcd: Snapshot backup succeeded")

	grant(t, getControlPlane(t, ns, cp), revision)
	mustPass(t, r, ns, cp)
	atGeneration := func(after string) {
		t.Helper()
		for _, conditionType := range []string{lodiav1alpha1.ConditionDatastoreReady, lodiav1alpha1.ConditionBackupReady} {
			if c := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, conditionType); c.ObservedGeneration != generation {
				t.Errorf("%s %s is at generation %d, want %d", after, conditionType, c.ObservedGeneration, generation)
			}
		}
	}
	atGeneration("after the grant")

	stale := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if plane, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				for i := range plane.Status.Conditions {
					plane.Status.Conditions[i].ObservedGeneration = 1
				}
			}
			return nil
		},
	})
	setEtcdStatus(t, ns, cp, func(s *druidv1alpha1.EtcdStatus) { s.Ready, s.ReadyReplicas = new(false), 0 })
	mustPass(t, operatorAt(stale, "v0.0.0-test+next"), ns, cp)
	if ready := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); ready.Reason != lodiav1alpha1.ReasonQuorumLost {
		t.Errorf("a held pass on a cache that lags the last write left DatastoreReady %s %q", ready.Reason, ready.Message)
	}
	atGeneration("after a held pass on a cache that lags the last write,")
}

func TestAStaleCacheWithAnOlderRevisionHoldsThePass(t *testing.T) {
	ns := namespace(t)
	const cp = "stale-revision"
	createClass(t, newClass("stale-revision", "Retain"))
	heldTenant(t, ns, cp, "stale-revision", nil)
	grant(t, getControlPlane(t, ns, cp), revision)
	editClass(t, "stale-revision", func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Etcd = &lodiav1alpha1.EtcdSettings{QuotaBackendBytes: new(resource.MustParse("3Gi"))}
	})
	before := getEtcd(t, ns, cp)
	stale := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if plane, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				plane.Status.Target.OperatorRevision = previousRevision
			}
			return nil
		},
	})
	mustPass(t, operatorAt(stale, revision), ns, cp)
	if etcd := getEtcd(t, ns, cp); etcd.ResourceVersion != before.ResourceVersion {
		t.Errorf("a pass on a cache that holds the previous grant wrote the Etcd: quota %s, stamp %s", quantity(etcd.Spec.Etcd.Quota), stamped(etcd))
	}
	mustPass(t, operatorAt(unpaused(k8s), revision), ns, cp)
	if etcd := getEtcd(t, ns, cp); quantity(etcd.Spec.Etcd.Quota) != "3Gi" || stamped(etcd) != revision {
		t.Errorf("the pass that reads the grant left quota %s and stamp %s", quantity(etcd.Spec.Etcd.Quota), stamped(etcd))
	}
}

func TestWithoutAGrantedRevisionDatastoreWaitsForTheTarget(t *testing.T) {
	ns := namespace(t)
	const cp = "no-target"
	createClass(t, newClass("no-target", "Delete"))
	if err := k8s.Create(t.Context(), controlPlane(ns, cp, "no-target")); err != nil {
		t.Fatal(err)
	}
	writeGeneration(t, ns, cp, 1)
	waitCondition(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, "waiting for Lifecycle to set status.target.operatorRevision")
	waitBackupReady(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, "waiting for Lifecycle to set status.target.operatorRevision")
	if !controllerutil.ContainsFinalizer(getControlPlane(t, ns, cp), lodiav1alpha1.DatastoreFinalizer) {
		t.Error("the finalizer waits for the target")
	}
	consistently(t, func() error {
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: cp + "-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
			return fmt.Errorf("want no Etcd before a revision is granted, got %w", err)
		}
		return nil
	})

	setHighAvailability(t, ns, cp, &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"})
	consistently(t, func() error {
		for _, c := range getControlPlane(t, ns, cp).Status.Conditions {
			if (c.Type == lodiav1alpha1.ConditionDatastoreReady || c.Type == lodiav1alpha1.ConditionBackupReady) && c.ObservedGeneration != 1 {
				return fmt.Errorf("%s moved to generation %d while it waits", c.Type, c.ObservedGeneration)
			}
		}
		return nil
	})

	grant(t, getControlPlane(t, ns, cp), revision)
	waitEtcd(t, ns, cp, requestedAs("1/"+revision, true))

	deleted := controlPlane(ns, "deleted-without-target", "no-target")
	if err := k8s.Create(t.Context(), deleted); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		if !controllerutil.ContainsFinalizer(getControlPlane(t, ns, deleted.Name), lodiav1alpha1.DatastoreFinalizer) {
			return errors.New("no finalizer yet")
		}
		return nil
	})
	if err := k8s.Delete(t.Context(), deleted); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		return gone(k8s.Get(t.Context(), client.ObjectKeyFromObject(deleted), &lodiav1alpha1.ControlPlane{}))
	})
}

func TestTheRestoreAsksEtcdDruidOnEveryEtcdItWrites(t *testing.T) {
	r := newRestore(t, restoreSetup{controlPlane: func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
	}})
	etcd := r.copied(t)
	if etcd.Spec.Replicas != 1 {
		t.Fatalf("the restoring Etcd asks for %d members", etcd.Spec.Replicas)
	}
	waitEtcd(t, r.ns, restoredCP, requestedAs("1/"+revision, true))
	answer(t, r.ns, restoredCP)
	r.complete(t, r.cleanedUp(t))
	raised := waitEtcd(t, r.ns, restoredCP, func(etcd *druidv1alpha1.Etcd) error {
		if etcd.Spec.Replicas != 3 {
			return fmt.Errorf("the Etcd asks for %d members", etcd.Spec.Replicas)
		}
		return nil
	})
	waitEtcd(t, r.ns, restoredCP, requestedAs(fmt.Sprintf("%d/%s", raised.Generation, revision), true))
}
