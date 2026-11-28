// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/etcddruid/druidtest"
)

func dryRun(opts []client.ApplyOption) bool {
	return len((&client.ApplyOptions{}).ApplyOptions(opts).DryRun) > 0
}

func counting(c client.WithWatch, dryRuns *atomic.Int32) client.WithWatch {
	return interceptor.NewClient(c, interceptor.Funcs{Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
		if dryRun(opts) {
			dryRuns.Add(1)
		}
		return c.Apply(ctx, obj, opts...)
	}})
}

// A held pass over a refused render whose message the emptied fields explain tries the render by dry run and reports what its refusal says, and once the other manager releases the field the next held pass reports what serves.
func TestAHeldPassTriesARenderTheEmptiedFieldsRefuse(t *testing.T) {
	t.Parallel()
	const cp = "held-empty"
	const quorum = "Etcd held-empty-etcd has quorum, 1 of 1 members ready"
	ns, credentialsNamespace := namespace(t), namespace(t)
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass(cp, credentialsNamespace))
	classTenant(t, ns, cp, cp)
	granted := operatorAt(withoutManagedFields(unpaused(k8s)), revision)
	apply := func(spec map[string]any) {
		t.Helper()
		applied := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": druidv1alpha1.SchemeGroupVersion.String(),
			"kind":       "Etcd",
			"metadata":   map[string]any{"name": cp + "-etcd", "namespace": ns},
		}}
		if spec != nil {
			applied.Object["spec"] = spec
		}
		if err := k8s.Apply(t.Context(), client.ApplyConfigurationFromUnstructured(applied), client.FieldOwner("other-tool")); err != nil {
			t.Fatal(err)
		}
	}
	apply(map[string]any{"backup": map[string]any{"store": map[string]any{}}})
	editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) { class.Spec.Backup = lodiav1alpha1.EtcdBackup{} })
	grantClass(t, ns, cp, cp)
	if _, err := passOf(t, granted, ns, cp); !apierrors.IsInvalid(err) {
		t.Fatalf("the pass that drops a store another manager holds returned %v, want the API server's refusal", err)
	}
	described := getControlPlane(t, ns, cp).Generation
	const emptied = ": the apply empties spec.backup.store, which the apply of field manager other-tool holds until other-tool applies without it; the API server leaves an object emptied while another manager holds it null, or empty without the fields it requires, and the Etcd's schema refuses either"
	expectReady(t, ns, cp, lodiav1alpha1.ReasonRefused, quorum+"; the API server refuses Etcd held-empty-etcd on certificate generation 1 with the fields of DatastoreClass held-empty generation 2"+emptied, described)
	held := operatorAt(withoutManagedFields(unpaused(k8s)), previousRevision)
	for range 2 {
		if _, err := passOf(t, held, ns, cp); !apierrors.IsInvalid(err) {
			t.Fatalf("a held pass whose dry run is refused returned %v, want the refusal", err)
		}
		expectReady(t, ns, cp, lodiav1alpha1.ReasonRefused, quorum+"; the grant's write of Etcd held-empty-etcd on certificate generation 1 with the fields of DatastoreClass held-empty generation 2 would be refused: the API server refuses the dry run"+emptied, described)
	}
	apply(nil)
	mustPass(t, held, ns, cp)
	expectReady(t, ns, cp, lodiav1alpha1.ReasonReady, quorum, described)
}

// A held pass whose dry run gets no answer stops there and writes no status: no answer is no observation.
func TestAHeldDryRunWithoutAnAnswerChangesNothing(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	const name = "unanswered-held"
	createClass(t, newClass("unanswering-held", "Retain"))
	heldTenant(t, ns, name, "unanswering-held", nil)
	waitPolicy(t, ns, name, nil)
	refuse(t, ns, admissionregistrationv1.Rule{APIGroups: []string{networkingv1.GroupName}, APIVersions: []string{"v1"}, Resources: []string{"networkpolicies"}}, func() client.Object {
		return &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: ns}}
	})
	if _, err := passOf(t, operatorAt(unpaused(k8s), previousRevision), ns, name); !apistatus.Refused(err) {
		t.Fatalf("a granted pass over the refused policy returned %v", err)
	}
	before := getControlPlane(t, ns, name)
	unanswered := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
		if dryRun(opts) {
			return apierrors.NewServiceUnavailable("the API server is starting")
		}
		return c.Apply(ctx, obj, opts...)
	}})
	if _, err := passOf(t, operatorAt(unanswered, revision), ns, name); err == nil || apistatus.Refused(err) {
		t.Fatalf("a held pass whose dry run gets no answer returned %v", err)
	}
	if after := getControlPlane(t, ns, name); after.ResourceVersion != before.ResourceVersion {
		t.Errorf("a held pass whose dry run got no answer wrote the ControlPlane: resourceVersion %s, then %s", before.ResourceVersion, after.ResourceVersion)
	}
}

// A held pass sends no dry run while DatastoreReady holds no refusal, nor a pass at the granted revision under one, and a held pass under one sends the two writes of the grant's first pass, the Etcd's render and its NetworkPolicy.
func TestAHeldPassTriesTheGrantOnlyUnderARefusal(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	const name = "counted-held"
	createClass(t, newClass("counting-held", "Retain"))
	heldTenant(t, ns, name, "counting-held", nil)
	waitPolicy(t, ns, name, nil)
	var dryRuns atomic.Int32
	held := operatorAt(counting(unpaused(k8s), &dryRuns), revision)
	mustPass(t, held, ns, name)
	if n := dryRuns.Load(); n != 0 {
		t.Errorf("a held pass without a refusal sent %d dry runs", n)
	}
	refuse(t, ns, admissionregistrationv1.Rule{APIGroups: []string{networkingv1.GroupName}, APIVersions: []string{"v1"}, Resources: []string{"networkpolicies"}}, func() client.Object {
		return &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: ns}}
	})
	for range 2 {
		if _, err := passOf(t, operatorAt(counting(unpaused(k8s), &dryRuns), previousRevision), ns, name); !apistatus.Refused(err) {
			t.Fatalf("a granted pass over the refused policy returned %v", err)
		}
	}
	if n := dryRuns.Load(); n != 0 {
		t.Errorf("a pass at the granted revision sent %d dry runs", n)
	}
	if _, err := passOf(t, held, ns, name); !apistatus.Refused(err) {
		t.Fatalf("a held pass under the refusal returned %v", err)
	}
	if n := dryRuns.Load(); n != 2 {
		t.Errorf("a held pass under a refusal sent %d dry runs, want 2", n)
	}
	if c := meta.FindStatusCondition(getControlPlane(t, ns, name).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); c.Reason != lodiav1alpha1.ReasonRefused || !strings.Contains(c.Message, "the grant's write of NetworkPolicy counted-held-etcd would be refused") {
		t.Errorf("DatastoreReady is %+v, want the refusal of the dry run", c)
	}
}

// A held pass under a refusal tries neither the credentials copy nor the fields of a class that waits for its own grant, which the grant of the revision leaves as they are.
func TestAHeldPassTriesNothingOfAClassThatWaitsForItsGrant(t *testing.T) {
	t.Parallel()
	ns, credentialsNamespace := namespace(t), namespace(t)
	const cp, schedule = "waiting-copy", "0 4 * * *"
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass(cp, credentialsNamespace))
	heldTenant(t, ns, cp, cp, nil)
	waitPolicy(t, ns, cp, nil)
	create(t, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-other", Namespace: credentialsNamespace}, Data: map[string][]byte{"accessKeyID": []byte("other-access")}})
	editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Backup.Store.Bucket, class.Spec.Backup.Store.SecretRef.Name = "other-bucket", "s3-other"
		class.Spec.Etcd = &lodiav1alpha1.EtcdSettings{Defragmentation: lodiav1alpha1.Defragmentation{Schedule: schedule}}
	})
	refuse(t, ns, admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"secrets"}}, func() client.Object {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: ns}}
	})
	refuseUnless(t, ns, "refuse-the-waiting-schedule-in-"+ns, admissionregistrationv1.Rule{APIGroups: []string{druidv1alpha1.SchemeGroupVersion.Group}, APIVersions: []string{"v1alpha1"}, Resources: []string{"etcds"}},
		"object.metadata.namespace != '"+ns+"' || !has(object.spec.etcd.defragmentationSchedule) || object.spec.etcd.defragmentationSchedule != '"+schedule+"'", func() client.Object {
			return &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: ns}, Spec: druidv1alpha1.EtcdSpec{Replicas: 1, Labels: map[string]string{"app": "etcd"}, Etcd: druidv1alpha1.EtcdConfig{DefragmentationSchedule: new(schedule)}}}
		})
	refuse(t, ns, admissionregistrationv1.Rule{APIGroups: []string{networkingv1.GroupName}, APIVersions: []string{"v1"}, Resources: []string{"networkpolicies"}}, func() client.Object {
		return &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: ns}}
	})
	if _, err := passOf(t, operatorAt(unpaused(k8s), previousRevision), ns, cp); !apistatus.Refused(err) {
		t.Fatalf("a granted pass over the refused policy returned %v", err)
	}
	if _, err := passOf(t, operatorAt(unpaused(k8s), revision), ns, cp); !apistatus.Refused(err) {
		t.Fatalf("a held pass under the refusal returned %v", err)
	}
	c := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
	if !strings.Contains(c.Message, "the grant's write of NetworkPolicy waiting-copy-etcd would be refused") || strings.Contains(c.Message, "Secret") || strings.Contains(c.Message, "the grant's write of Etcd") {
		t.Errorf("DatastoreReady is %+v, want the policy's dry run refused, and neither the copy nor the class's fields tried", c)
	}
}

func expectDatastoreReady(t *testing.T, ns, name string, status metav1.ConditionStatus, reason, message string, generation int64) {
	t.Helper()
	c := meta.FindStatusCondition(getControlPlane(t, ns, name).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
	if c == nil || c.Status != status || c.Reason != reason || c.Message != message || c.ObservedGeneration != generation {
		t.Fatalf("DatastoreReady is %+v, want %s %s %q at generation %d", c, status, reason, message, generation)
	}
}

// A held pass that writes a lost quorum over a refusal its dry run found tries the grant again on the next held pass, so the refusal stands once the member is back.
func TestAHeldPassOverALostQuorumTriesTheGrantAgain(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	const name = "quorum-held"
	const tried = "; the grant's write of NetworkPolicy quorum-held-etcd would be refused: the API server refuses the dry run; the reason is in Lodia's log"
	createClass(t, newClass("quorum-held", "Retain"))
	heldTenant(t, ns, name, "quorum-held", nil)
	waitPolicy(t, ns, name, nil)
	refuse(t, ns, admissionregistrationv1.Rule{APIGroups: []string{networkingv1.GroupName}, APIVersions: []string{"v1"}, Resources: []string{"networkpolicies"}}, func() client.Object {
		return &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: ns}}
	})
	granted, held := operatorAt(unpaused(k8s), previousRevision), operatorAt(unpaused(k8s), revision)
	if _, err := passOf(t, granted, ns, name); !apistatus.Refused(err) {
		t.Fatalf("a granted pass over a refused update of the policy returned %v", err)
	}
	described := getControlPlane(t, ns, name).Generation
	heldRefused(t, held, ns, name, "Etcd quorum-held-etcd has quorum, 1 of 1 members ready"+tried, described)
	setEtcdStatus(t, ns, name, druidtest.Members(1, 0))
	if _, err := passOf(t, held, ns, name); !apistatus.Refused(err) {
		t.Fatalf("a held pass over the lost quorum returned %v, want the refusal of its dry run", err)
	}
	expectDatastoreReady(t, ns, name, metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost, "Etcd quorum-held-etcd has 0 of 1 members ready"+tried, described)
	setEtcdStatus(t, ns, name)
	heldRefused(t, held, ns, name, "Etcd quorum-held-etcd has quorum, 1 of 1 members ready"+tried, described)
}

// A held pass tries the grant's writes with the grant's force, so fields another manager took over by hand in the Etcd, its NetworkPolicy and the credentials copy, which the grant takes back, do not stop the trial.
func TestAHeldPassTriesTheGrantWithItsForce(t *testing.T) {
	t.Parallel()
	ns, credentialsNamespace := namespace(t), namespace(t)
	const cp = "forced-held"
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass(cp, credentialsNamespace))
	heldTenant(t, ns, cp, cp, nil)
	waitPolicy(t, ns, cp, nil)
	create(t, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-other", Namespace: credentialsNamespace}, Data: map[string][]byte{"accessKeyID": []byte("other-access")}})
	editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Backup.Store.Bucket, class.Spec.Backup.Store.SecretRef.Name = "other-bucket", "s3-other"
	})
	grantClass(t, ns, cp, cp)
	etcd := getEtcd(t, ns, cp)
	etcd.Spec.Etcd.Quota = new(resource.MustParse("1Gi"))
	policy := waitPolicy(t, ns, cp, nil)
	policy.Spec.PodSelector.MatchLabels[druidv1alpha1.LabelAppNameKey] = "edited-by-hand"
	copied := getCopy(t, ns, cp)
	copied.Data["accessKeyID"] = []byte("edited-by-hand")
	for _, obj := range []client.Object{etcd, policy, copied} {
		if err := k8s.Update(t.Context(), obj, client.FieldOwner("kubectl-edit")); err != nil {
			t.Fatal(err)
		}
	}
	setEtcdStatus(t, ns, cp, druidtest.Members(1, 0))
	mustPass(t, operatorAt(unpaused(k8s), revision), ns, cp)
	var dryRuns atomic.Int32
	mustPass(t, operatorAt(counting(unpaused(k8s), &dryRuns), revision), ns, cp)
	if n := dryRuns.Load(); n != 3 {
		t.Errorf("a held pass over the lost quorum sent %d dry runs, want 3: the Etcd, its NetworkPolicy and the credentials copy", n)
	}
	expectDatastoreReady(t, ns, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost, "Etcd forced-held-etcd has 0 of 1 members ready", getControlPlane(t, ns, cp).Generation)
}

// A held pass tries the Etcd's render stamped with the running revision, as the grant's first pass sends it.
func TestAHeldPassTriesTheEtcdStampedWithTheRunningRevision(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	const name = "stamped-held"
	createClass(t, newClass("stamping-held", "Retain"))
	heldTenant(t, ns, name, "stamping-held", nil)
	rule := admissionregistrationv1.Rule{APIGroups: []string{druidv1alpha1.SchemeGroupVersion.Group}, APIVersions: []string{"v1alpha1"}, Resources: []string{"etcds"}}
	refuseUnless(t, ns, "refuse-the-running-stamp-in-"+ns, rule, "object.metadata.namespace != '"+ns+"' || !has(object.metadata.annotations) || !('"+lodiav1alpha1.OperatorRevisionAnnotation+"' in object.metadata.annotations) || object.metadata.annotations['"+lodiav1alpha1.OperatorRevisionAnnotation+"'] != '"+revision+"'", func() client.Object {
		return &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: ns, Annotations: map[string]string{lodiav1alpha1.OperatorRevisionAnnotation: revision}}, Spec: druidv1alpha1.EtcdSpec{Replicas: 1, Labels: map[string]string{"app": "etcd"}}}
	})
	setEtcdStatus(t, ns, name, druidtest.Members(1, 0))
	held := operatorAt(unpaused(k8s), revision)
	mustPass(t, held, ns, name)
	if _, err := passOf(t, held, ns, name); !apistatus.Refused(err) {
		t.Fatalf("a held pass whose dry run of the stamped render is refused returned %v", err)
	}
	expectDatastoreReady(t, ns, name, metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost, "Etcd stamped-held-etcd has 0 of 1 members ready; the grant's write of Etcd stamped-held-etcd on certificate generation 1 with the fields of DatastoreClass stamping-held generation 1 would be refused: the API server refuses the dry run; the reason is in Lodia's log", getControlPlane(t, ns, name).Generation)
}

// A held pass sends the request that etcd-druid reconcile the Etcd for real while one is due and meets its refusal, which reads as gone while the request waits for etcd-druid's Deployment.
func TestAHeldPassMeetsARefusedRequestOnceItIsSent(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	const cp, next = "requested-held", "v0.0.1-test+next"
	const quorum = "Etcd requested-held-etcd has quorum, 1 of 1 members ready"
	createClass(t, newClass(cp, "Retain"))
	heldTenant(t, ns, cp, cp, nil)
	grant(t, getControlPlane(t, ns, cp), revision)
	refusing, refused := refusingRequests(cp)
	if _, err := passOf(t, operatorAt(refusing, revision), ns, cp); !errors.Is(err, refused) {
		t.Fatalf("a granted pass whose request is refused returned %v, want the refusal", err)
	}
	described := getControlPlane(t, ns, cp).Generation
	want := quorum + "; the API server refuses the request that etcd-druid reconcile Etcd " + cp + "-etcd, the annotations " + druidv1alpha1.DruidOperationAnnotation + " and " + lodiav1alpha1.EtcdReconcileRequestedAnnotation + "; the reason is in Lodia's log"
	expectReady(t, ns, cp, lodiav1alpha1.ReasonRefused, want, described)
	waiting := operatorAt(refusing, next)
	waiting.Druid.Deployment = "druid-absent"
	mustPass(t, waiting, ns, cp)
	expectReady(t, ns, cp, lodiav1alpha1.ReasonReady, quorum+"; etcd-druid is asked to reconcile Etcd "+cp+"-etcd under operator revision "+revision+" once Deployment "+druidNamespace+"/druid-absent exists", described)
	if _, err := passOf(t, operatorAt(refusing, next), ns, cp); !errors.Is(err, refused) {
		t.Fatalf("a held pass whose request is refused returned %v, want the refusal", err)
	}
	expectReady(t, ns, cp, lodiav1alpha1.ReasonRefused, want, described)
}
