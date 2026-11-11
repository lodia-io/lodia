// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
)

func classTenant(t *testing.T, ns, name, class string) *datastore.Reconciler {
	t.Helper()
	cp := controlPlane(ns, name, class)
	cp.Spec.Paused = true
	create(t, cp)
	writeGeneration(t, ns, name, 1)
	r := operatorAt(unpaused(k8s), revision)
	mustPass(t, r, ns, name)
	answer(t, ns, name)
	runStatefulSet(t, ns, name, 1)
	setEtcdStatus(t, ns, name, nil)
	mustPass(t, r, ns, name)
	return r
}

func runsClass(t *testing.T, ns, cp, quota, generation string) {
	t.Helper()
	etcd := getEtcd(t, ns, cp)
	if got, recorded := quantity(etcd.Spec.Etcd.Quota), etcd.Annotations[lodiav1alpha1.DatastoreClassGenerationAnnotation]; got != quota || recorded != generation {
		t.Errorf("the Etcd runs quota %s of class generation %s, want %s of generation %s", got, recorded, quota, generation)
	}
	g, err := strconv.ParseInt(generation, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	want := lodiav1alpha1.DatastoreClassStatus{
		Name:                     etcd.Annotations[lodiav1alpha1.DatastoreClassAnnotation],
		DatastoreClassGeneration: lodiav1alpha1.DatastoreClassGeneration{UID: types.UID(etcd.Annotations[lodiav1alpha1.DatastoreClassUIDAnnotation]), Generation: g},
	}
	if status := getControlPlane(t, ns, cp).Status.Datastore; status == nil || status.Class == nil || *status.Class != want || want.UID == "" {
		t.Errorf("status.datastore is %+v, want class %+v", status, want)
	}
}

func rollOut(t *testing.T, r *datastore.Reconciler, ns, cp string) {
	t.Helper()
	answer(t, ns, cp)
	mustPass(t, r, ns, cp)
	answer(t, ns, cp)
	setEtcdStatus(t, ns, cp, nil)
	mustPass(t, r, ns, cp)
}

func classAt(t *testing.T, name string, g int64) *lodiav1alpha1.DatastoreClassGeneration {
	t.Helper()
	class := &lodiav1alpha1.DatastoreClass{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: name}, class); err != nil {
		t.Fatal(err)
	}
	return &lodiav1alpha1.DatastoreClassGeneration{UID: class.UID, Generation: g}
}

func quota(q string) func(*lodiav1alpha1.DatastoreClass) {
	return func(class *lodiav1alpha1.DatastoreClass) {
		if class.Spec.Etcd == nil {
			class.Spec.Etcd = &lodiav1alpha1.EtcdSettings{}
		}
		class.Spec.Etcd.QuotaBackendBytes = new(resource.MustParse(q))
	}
}

func readyMessage(t *testing.T, ns, cp string) string {
	t.Helper()
	return trueWith(t, ns, cp, lodiav1alpha1.ReasonReady)
}

func trueWith(t *testing.T, ns, cp, reason string) string {
	t.Helper()
	c := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != reason {
		t.Fatalf("DatastoreReady is %+v, want True, %s", c, reason)
	}
	return c.Message
}

func TestAClassEditWaitsUntilItsGenerationIsGranted(t *testing.T) {
	for _, c := range []struct {
		name, quota string
		class       func(name, credentialsNamespace string) *lodiav1alpha1.DatastoreClass
	}{
		{"with a backup store", "2Gi", fullClass},
		{"without a backup store", "4Gi", func(name, _ string) *lodiav1alpha1.DatastoreClass { return newClass(name, "Retain") }},
	} {
		t.Run(c.name, func(t *testing.T) {
			ns, credentialsNamespace := namespace(t), namespace(t)
			cp := "waits-" + strings.Fields(c.name)[0]
			create(t, credentials(credentialsNamespace))
			createClass(t, c.class(cp, credentialsNamespace))
			r := classTenant(t, ns, cp, cp)
			runsClass(t, ns, cp, c.quota, "1")
			waits := func(generation string) string {
				return "DatastoreClass " + cp + " generation " + generation + " waits for status.target.datastoreClass"
			}
			if message := readyMessage(t, ns, cp); strings.Contains(message, "waits for") {
				t.Errorf("a new Etcd notes %q", message)
			}

			editClass(t, cp, quota("3Gi"))
			mustPass(t, r, ns, cp)
			runsClass(t, ns, cp, c.quota, "1")
			if message := readyMessage(t, ns, cp); !strings.HasSuffix(message, "; "+waits("2")) {
				t.Errorf("DatastoreReady says %q, want it to end with %q", message, waits("2"))
			}

			writeGeneration(t, ns, cp, 2)
			mustPass(t, r, ns, cp)
			waitEtcd(t, ns, cp, namesGeneration(2))
			runsClass(t, ns, cp, c.quota, "1")

			applyTarget(t, getControlPlane(t, ns, cp), "v1.37.1", revision, classAt(t, cp, 1))
			mustPass(t, r, ns, cp)
			runsClass(t, ns, cp, c.quota, "1")

			editClass(t, cp, quota("1Gi"))
			mustPass(t, r, ns, cp)
			runsClass(t, ns, cp, c.quota, "1")
			if message := readyMessage(t, ns, cp); !strings.HasSuffix(message, "; "+waits("3")) {
				t.Errorf("DatastoreReady says %q, want it to end with %q", message, waits("3"))
			}

			grantClass(t, ns, cp, cp)
			mustPass(t, r, ns, cp)
			rollOut(t, r, ns, cp)
			runsClass(t, ns, cp, "1Gi", "3")
			if message := readyMessage(t, ns, cp); strings.Contains(message, "waits for") {
				t.Errorf("a granted class still notes %q", message)
			}
		})
	}
}

func classOf(name, quotaBytes string) *lodiav1alpha1.DatastoreClass {
	class := newClass(name, "Retain")
	quota(quotaBytes)(class)
	return class
}

func deleteClass(t *testing.T, name string) {
	t.Helper()
	if err := k8s.Delete(t.Context(), newClass(name, "Retain")); err != nil {
		t.Fatal(err)
	}
}

func recordsUID(t *testing.T, ns, cp string, class *lodiav1alpha1.DatastoreClass) {
	t.Helper()
	if got := getEtcd(t, ns, cp).Annotations[lodiav1alpha1.DatastoreClassUIDAnnotation]; got != string(class.UID) {
		t.Errorf("the Etcd records class UID %s, want %s", got, class.UID)
	}
}

func TestAClassCreatedAgainUnderItsNameWaitsForItsGrant(t *testing.T) {
	t.Run("at the generation granted to the class it replaces", func(t *testing.T) {
		ns := namespace(t)
		const cp = "again-same"
		replaced := classOf(cp, "2Gi")
		createClass(t, replaced)
		r := classTenant(t, ns, cp, cp)
		grantClass(t, ns, cp, cp)
		deleteClass(t, cp)
		mustPass(t, r, ns, cp)
		runsClass(t, ns, cp, "2Gi", "1")

		again := classOf(cp, "3Gi")
		createClass(t, again)
		mustPass(t, r, ns, cp)
		runsClass(t, ns, cp, "2Gi", "1")
		recordsUID(t, ns, cp, replaced)
		if want := "; DatastoreClass " + cp + " generation 1 of UID " + string(again.UID) + " waits for status.target.datastoreClass"; !strings.HasSuffix(readyMessage(t, ns, cp), want) {
			t.Errorf("DatastoreReady says %q, want it to end with %q", readyMessage(t, ns, cp), want)
		}

		grantClass(t, ns, cp, cp)
		mustPass(t, r, ns, cp)
		recordsUID(t, ns, cp, again)
		if got := getControlPlane(t, ns, cp).Status.Datastore.Class; got == nil || got.UID != replaced.UID {
			t.Errorf("while the class created again rolls status.datastore.class is %+v, want UID %s kept", got, replaced.UID)
		}
		rollOut(t, r, ns, cp)
		runsClass(t, ns, cp, "3Gi", "1")
		recordsUID(t, ns, cp, again)
	})
	t.Run("reaching the generation granted to the class it replaces", func(t *testing.T) {
		ns := namespace(t)
		const cp = "again-reaches"
		replaced := classOf(cp, "2Gi")
		createClass(t, replaced)
		r := classTenant(t, ns, cp, cp)
		editClass(t, cp, quota("1Gi"))
		grantClass(t, ns, cp, cp)
		mustPass(t, r, ns, cp)
		rollOut(t, r, ns, cp)
		runsClass(t, ns, cp, "1Gi", "2")

		deleteClass(t, cp)
		createClass(t, classOf(cp, "3Gi"))
		mustPass(t, r, ns, cp)
		runsClass(t, ns, cp, "1Gi", "2")

		editClass(t, cp, quota("4Gi"))
		mustPass(t, r, ns, cp)
		runsClass(t, ns, cp, "1Gi", "2")
		recordsUID(t, ns, cp, replaced)
	})
}

func TestANewEtcdTakesTheClassAsItIs(t *testing.T) {
	ns := namespace(t)
	const cp = "as-it-is"
	createClass(t, newClass(cp, "Retain"))
	editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Etcd = &lodiav1alpha1.EtcdSettings{QuotaBackendBytes: new(resource.MustParse("3Gi"))}
	})
	r := classTenant(t, ns, cp, cp)
	runsClass(t, ns, cp, "3Gi", "2")

	editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Etcd = &lodiav1alpha1.EtcdSettings{QuotaBackendBytes: new(resource.MustParse("1Gi"))}
	})
	mustPass(t, r, ns, cp)
	runsClass(t, ns, cp, "3Gi", "2")

	if err := k8s.Delete(t.Context(), getEtcd(t, ns, cp)); err != nil {
		t.Fatal(err)
	}
	mustPass(t, r, ns, cp)
	rollOut(t, r, ns, cp)
	runsClass(t, ns, cp, "1Gi", "3")
}

func TestAStoreMoveThatWaitsForItsGenerationKeepsTheCopyOfTheStoreTheEtcdNames(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	const cp = "waits-store"
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass(cp, credentialsNamespace))
	r := classTenant(t, ns, cp, cp)
	copied := func(t *testing.T, bucket, accessKeyID, region string) {
		t.Helper()
		if got := getEtcd(t, ns, cp).Spec.Backup.Store.Container; *got != bucket {
			t.Errorf("the Etcd is on bucket %s, want %s", *got, bucket)
		}
		if secret := getCopy(t, ns, cp); string(secret.Data["accessKeyID"]) != accessKeyID || string(secret.Data["region"]) != region {
			t.Errorf("%s holds accessKeyID %s and region %s, want %s and %s", secret.Name, secret.Data["accessKeyID"], secret.Data["region"], accessKeyID, region)
		}
	}

	create(t, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-rotated", Namespace: credentialsNamespace}, Data: map[string][]byte{"accessKeyID": []byte("rotated-access")}})
	editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Backup.Store.SecretRef.Name, class.Spec.Backup.Store.Region = "s3-rotated", "eu-west-1"
	})
	mustPass(t, r, ns, cp)
	copied(t, "lodia-backups", "rotated-access", "eu-west-1")

	create(t, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-other", Namespace: credentialsNamespace}, Data: map[string][]byte{"accessKeyID": []byte("other-access")}})
	editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Backup.Store.Bucket, class.Spec.Backup.Store.SecretRef.Name = "other-bucket", "s3-other"
	})
	mustPass(t, r, ns, cp)
	copied(t, "lodia-backups", "rotated-access", "eu-west-1")

	grantClass(t, ns, cp, cp)
	mustPass(t, r, ns, cp)
	copied(t, "other-bucket", "other-access", "eu-west-1")
}

func TestAClassHeldForItsCredentialsAlsoNotesItsWait(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	const cp = "waits-held"
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass(cp, credentialsNamespace))
	r := classTenant(t, ns, cp, cp)
	if err := k8s.Delete(t.Context(), credentials(credentialsNamespace)); err != nil {
		t.Fatal(err)
	}
	editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Etcd.QuotaBackendBytes = new(resource.MustParse("3Gi"))
	})
	mustPass(t, r, ns, cp)
	want := "; DatastoreClass " + cp + " generation 2 waits for status.target.datastoreClass; DatastoreClass " + cp + " is held: its credentials Secret " + credentialsNamespace + "/s3 is missing"
	if message := trueWith(t, ns, cp, lodiav1alpha1.ReasonSecretNotFound); !strings.HasSuffix(message, want) {
		t.Errorf("DatastoreReady says %q, want it to end with %q", message, want)
	}
	grantClass(t, ns, cp, cp)
	mustPass(t, r, ns, cp)
	runsClass(t, ns, cp, "2Gi", "1")
}

func TestAHeldPassGivesTheCredentialsHoldItsReason(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	const cp = "held-reason"
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass(cp, credentialsNamespace))
	heldTenant(t, ns, cp, cp, nil)
	if err := k8s.Delete(t.Context(), credentials(credentialsNamespace)); err != nil {
		t.Fatal(err)
	}
	mustPass(t, operatorAt(unpaused(k8s), revision), ns, cp)
	want := "; DatastoreClass " + cp + " is held: its credentials Secret " + credentialsNamespace + "/s3 is missing"
	if message := trueWith(t, ns, cp, lodiav1alpha1.ReasonSecretNotFound); !strings.HasSuffix(message, want) {
		t.Errorf("under the operator hold DatastoreReady says %q, want it to end with %q", message, want)
	}
}

func TestTheClassIsReportedOnceEtcdDruidRolledItOut(t *testing.T) {
	ns := namespace(t)
	const cp = "reported-class"
	createClass(t, newClass(cp, "Retain"))
	r := classTenant(t, ns, cp, cp)
	editClass(t, cp, quota("3Gi"))
	grantClass(t, ns, cp, cp)
	keeps := func(t *testing.T, r *datastore.Reconciler) {
		t.Helper()
		mustPass(t, r, ns, cp)
		if got := getControlPlane(t, ns, cp).Status.Datastore.Class; got == nil || got.Generation != 1 {
			t.Errorf("status.datastore.class is %+v, want generation 1 kept", got)
		}
	}

	t.Run("etcd-druid's annotation is there", func(t *testing.T) {
		keeps(t, r)
		etcd := getEtcd(t, ns, cp)
		if err := requestedAs(fmt.Sprintf("%d/%s", etcd.Generation, revision), true)(etcd); err != nil || etcd.Annotations[lodiav1alpha1.DatastoreClassGenerationAnnotation] != "2" {
			t.Fatalf("the grant's pass left the Etcd at class generation %s: %v", etcd.Annotations[lodiav1alpha1.DatastoreClassGenerationAnnotation], err)
		}
		keeps(t, r)
	})
	t.Run("the cache holds another report", func(t *testing.T) {
		other := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if plane, ok := obj.(*lodiav1alpha1.ControlPlane); ok && plane.Status.Datastore != nil {
					plane.Status.Datastore.Class = nil
				}
				return nil
			},
		})
		keeps(t, operatorAt(other, revision))
	})
	answer(t, ns, cp)
	t.Run("etcd-druid has not observed the generation", func(t *testing.T) {
		keeps(t, r)
	})
	setEtcdStatus(t, ns, cp, nil)
	t.Run("a member is not updated", func(t *testing.T) {
		setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) { s.UpdatedReplicas = 0 })
		keeps(t, r)
	})
	setStatefulSetStatus(t, ns, cp, nil)
	mustPass(t, r, ns, cp)
	runsClass(t, ns, cp, "3Gi", "2")
}

func unrolledTenant(t *testing.T, ns, name string) *datastore.Reconciler {
	t.Helper()
	cp := controlPlane(ns, name, name)
	cp.Spec.Paused = true
	create(t, cp)
	writeGeneration(t, ns, name, 1)
	r := operatorAt(unpaused(k8s), revision)
	mustPass(t, r, ns, name)
	answer(t, ns, name)
	runStatefulSet(t, ns, name, 1)
	setStatefulSetStatus(t, ns, name, func(s *appsv1.StatefulSetStatus) { s.AvailableReplicas, s.ReadyReplicas = 0, 0 })
	setEtcdStatus(t, ns, name, nil)
	mustPass(t, r, ns, name)
	return r
}

func TestAnEtcdThatNeverRolledOutReportsTheClassItWasAppliedWith(t *testing.T) {
	ns := namespace(t)
	const cp = "never-rolled"
	createClass(t, newClass(cp, "Retain"))
	r := unrolledTenant(t, ns, cp)
	runsClass(t, ns, cp, "4Gi", "1")
	if got := reportedRevision(t, ns, cp); got != "" {
		t.Errorf("an Etcd that never rolled out reports operator revision %q", got)
	}

	editClass(t, cp, quota("3Gi"))
	mustPass(t, r, ns, cp)
	runsClass(t, ns, cp, "4Gi", "1")
	if want := "; DatastoreClass " + cp + " generation 2 waits for status.target.datastoreClass"; !strings.HasSuffix(readyMessage(t, ns, cp), want) {
		t.Errorf("DatastoreReady says %q, want it to end with %q", readyMessage(t, ns, cp), want)
	}

	grantClass(t, ns, cp, cp)
	mustPass(t, r, ns, cp)
	runsClass(t, ns, cp, "3Gi", "2")
}

func TestARefusedRequestReportsTheClassOfAnEtcdThatNeverRolledOut(t *testing.T) {
	ns := namespace(t)
	const cp = "refused-unrolled"
	createClass(t, newClass(cp, "Retain"))
	plane := controlPlane(ns, cp, cp)
	plane.Spec.Paused = true
	create(t, plane)
	writeGeneration(t, ns, cp, 1)
	refusing, refused := refusingRequests(cp)
	if _, err := passOf(t, operatorAt(refusing, revision), ns, cp); !errors.Is(err, refused) {
		t.Fatalf("a pass whose request the API server refuses returned %v, want the refusal", err)
	}
	runsClass(t, ns, cp, "4Gi", "1")
}

func reportedClass(t *testing.T, ns, cp string) int64 {
	t.Helper()
	status := getControlPlane(t, ns, cp).Status.Datastore
	if status == nil || status.Class == nil {
		t.Fatalf("status.datastore is %+v, want a class", status)
	}
	return status.Class.Generation
}

func TestAClassReportNeverMovesBackwardsAcrossARoll(t *testing.T) {
	ns := namespace(t)
	const cp = "report-forward"
	createClass(t, newClass(cp, "Retain"))
	r := unrolledTenant(t, ns, cp)
	before := getEtcd(t, ns, cp)
	editClass(t, cp, quota("3Gi"))
	grantClass(t, ns, cp, cp)
	mustPass(t, r, ns, cp)
	runsClass(t, ns, cp, "3Gi", "2")

	refused := apierrors.NewInvalid(schema.GroupKind{Group: druidv1alpha1.SchemeGroupVersion.Group, Kind: "Etcd"}, cp+"-etcd", field.ErrorList{field.Forbidden(field.NewPath("spec"), "refused by a policy")})
	stale := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if etcd, ok := obj.(*druidv1alpha1.Etcd); ok {
				before.DeepCopyInto(etcd)
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			if u, ok := obj.(interface{ GetKind() string }); ok && u.GetKind() == "Etcd" {
				return refused
			}
			return c.Apply(ctx, obj, opts...)
		},
	})
	if _, err := passOf(t, operatorAt(stale, revision), ns, cp); !errors.Is(err, refused) {
		t.Fatalf("a pass whose apply the API server refuses returned %v, want the refusal", err)
	}
	if got := reportedClass(t, ns, cp); got != 2 {
		t.Errorf("a pass on a cache that holds the Etcd before the grant reports class generation %d, want 2 kept", got)
	}

	answer(t, ns, cp)
	setEtcdStatus(t, ns, cp, nil)
	setStatefulSetStatus(t, ns, cp, nil)
	mustPass(t, r, ns, cp)
	runsClass(t, ns, cp, "3Gi", "2")
	if got := reportedRevision(t, ns, cp); got != revision {
		t.Fatalf("status.datastore.operatorRevision is %q once the Etcd rolled out, want %s", got, revision)
	}

	editClass(t, cp, quota("1Gi"))
	grantClass(t, ns, cp, cp)
	mustPass(t, r, ns, cp)
	if etcd := getEtcd(t, ns, cp); etcd.Annotations[lodiav1alpha1.DatastoreClassGenerationAnnotation] != "3" {
		t.Fatalf("the grant's pass left the Etcd at class generation %s", etcd.Annotations[lodiav1alpha1.DatastoreClassGenerationAnnotation])
	}
	if got := reportedClass(t, ns, cp); got != 2 {
		t.Errorf("while generation 3 rolls status.datastore.class is at generation %d, want 2", got)
	}
	setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) { s.UpdatedReplicas = 0 })
	rollOut(t, r, ns, cp)
	if got := reportedClass(t, ns, cp); got != 2 {
		t.Errorf("while a member is not updated status.datastore.class is at generation %d, want 2", got)
	}
	setStatefulSetStatus(t, ns, cp, nil)
	mustPass(t, r, ns, cp)
	runsClass(t, ns, cp, "1Gi", "3")
}

func TestADeletingPassKeepsTheClassReported(t *testing.T) {
	ns := namespace(t)
	const cp = "deleting-unrolled"
	createClass(t, newClass(cp, "Retain"))
	r := unrolledTenant(t, ns, cp)
	if err := k8s.Patch(t.Context(), getEtcd(t, ns, cp), client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"annotations":{"`+lodiav1alpha1.DatastoreClassGenerationAnnotation+`":"7"}}}`)), client.FieldOwner("kubectl-annotate")); err != nil {
		t.Fatal(err)
	}
	if err := k8s.Delete(t.Context(), getControlPlane(t, ns, cp)); err != nil {
		t.Fatal(err)
	}
	mustPass(t, r, ns, cp)
	if got := reportedClass(t, ns, cp); got != 1 {
		t.Errorf("a deleting pass reports class generation %d, the Etcd's annotation, want 1 kept", got)
	}
}

func defaultClass(name string) *lodiav1alpha1.DatastoreClass {
	class := newClass(name, "Retain")
	class.Annotations = map[string]string{lodiav1alpha1.DefaultClassAnnotation: "true"}
	return class
}

func recreateEtcd(t *testing.T, r *datastore.Reconciler, ns, cp string) *druidv1alpha1.Etcd {
	t.Helper()
	predecessor := getEtcd(t, ns, cp)
	if err := k8s.Delete(t.Context(), predecessor); err != nil {
		t.Fatal(err)
	}
	setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) { s.AvailableReplicas, s.ReadyReplicas = 0, 0 })
	mustPass(t, r, ns, cp)
	return predecessor
}

func TestAnEtcdCreatedAgainFromAnotherClassReportsItsOwnClass(t *testing.T) {
	ns := namespace(t)
	const cp = "recreated-other"
	first, second := defaultClass(cp+"-a"), defaultClass(cp+"-b")
	createClass(t, first)
	r := classTenant(t, ns, cp, "")
	grantClass(t, ns, cp, first.Name)
	mustPass(t, r, ns, cp)
	runsClass(t, ns, cp, "4Gi", "1")

	editClass(t, first.Name, func(class *lodiav1alpha1.DatastoreClass) {
		delete(class.Annotations, lodiav1alpha1.DefaultClassAnnotation)
	})
	createClass(t, second)
	predecessor := recreateEtcd(t, r, ns, cp)
	if got := getEtcd(t, ns, cp).Annotations[lodiav1alpha1.DatastoreClassAnnotation]; got != second.Name {
		t.Fatalf("the Etcd created again records class %s, want the default %s", got, second.Name)
	}
	runsClass(t, ns, cp, "4Gi", "1")
	if got := reportedRevision(t, ns, cp); got != revision {
		t.Errorf("the Etcd created again reports operator revision %q, want its predecessor's %s", got, revision)
	}

	refused := apierrors.NewInvalid(schema.GroupKind{Group: druidv1alpha1.SchemeGroupVersion.Group, Kind: "Etcd"}, cp+"-etcd", field.ErrorList{field.Forbidden(field.NewPath("spec"), "refused by a policy")})
	stale := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if etcd, ok := obj.(*druidv1alpha1.Etcd); ok {
				predecessor.DeepCopyInto(etcd)
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			if u, ok := obj.(interface{ GetKind() string }); ok && u.GetKind() == "Etcd" {
				return refused
			}
			return c.Apply(ctx, obj, opts...)
		},
	})
	if _, err := passOf(t, operatorAt(stale, revision), ns, cp); !errors.Is(err, refused) {
		t.Fatalf("a pass whose apply the API server refuses returned %v, want the refusal", err)
	}
	if got := getControlPlane(t, ns, cp).Status.Datastore.Class; got == nil || got.Name != second.Name {
		t.Errorf("a refused pass on a cache that holds the Etcd's predecessor reports class %+v, want %s kept", got, second.Name)
	}

	answer(t, ns, cp)
	mustPass(t, r, ns, cp)
	editClass(t, second.Name, quota("3Gi"))
	mustPass(t, r, ns, cp)
	runsClass(t, ns, cp, "4Gi", "1")
	if c, want := meta.FindStatusCondition(getControlPlane(t, ns, cp).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady), "; DatastoreClass "+second.Name+" generation 2 waits for status.target.datastoreClass"; c == nil || !strings.HasSuffix(c.Message, want) {
		t.Errorf("DatastoreReady is %+v, want its message to end with %q", c, want)
	}

	grantClass(t, ns, cp, second.Name)
	mustPass(t, r, ns, cp)
	if got := reportedClass(t, ns, cp); got != 1 {
		t.Errorf("while generation 2 of %s rolls status.datastore.class is at generation %d, want 1", second.Name, got)
	}
	answer(t, ns, cp)
	setEtcdStatus(t, ns, cp, nil)
	setStatefulSetStatus(t, ns, cp, nil)
	mustPass(t, r, ns, cp)
	runsClass(t, ns, cp, "3Gi", "2")
}

func TestAnEtcdCreatedAgainOverAReportOfNoClassReportsItsClass(t *testing.T) {
	ns := namespace(t)
	const cp = "recreated-unreported"
	createClass(t, newClass(cp, "Retain"))
	r := classTenant(t, ns, cp, cp)
	if err := k8s.Patch(t.Context(), getEtcd(t, ns, cp), client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"annotations":{"`+lodiav1alpha1.DatastoreClassUIDAnnotation+`":null}}}`)), client.FieldOwner("kubectl-annotate")); err != nil {
		t.Fatal(err)
	}
	mustPass(t, r, ns, cp)
	if status := getControlPlane(t, ns, cp).Status.Datastore; status == nil || status.Class != nil || status.OperatorRevision != revision {
		t.Fatalf("an Etcd that rolled out without its class UID reports status.datastore %+v, want no class and operator revision %s", status, revision)
	}

	recreateEtcd(t, r, ns, cp)
	runsClass(t, ns, cp, "4Gi", "1")
	if got := reportedRevision(t, ns, cp); got != revision {
		t.Errorf("the Etcd created again reports operator revision %q, want its predecessor's %s", got, revision)
	}
}

func TestAPassThatKeepsTheClassFromAStaleCacheWritesNoOlderClassBack(t *testing.T) {
	for i, row := range []struct {
		name string
		keep func(t *testing.T, class, credentialsNamespace string)
	}{
		{"its edit waits for the grant", func(t *testing.T, class, _ string) { editClass(t, class, quota("1Gi")) }},
		{"it is held for its credentials", func(t *testing.T, _, credentialsNamespace string) {
			if err := k8s.Delete(t.Context(), credentials(credentialsNamespace)); err != nil {
				t.Fatal(err)
			}
		}},
		{"it is gone", func(t *testing.T, class, _ string) { deleteClass(t, class) }},
	} {
		t.Run(row.name, func(t *testing.T) {
			ns, credentialsNamespace := namespace(t), namespace(t)
			cp := fmt.Sprintf("stale-keep-%d", i)
			create(t, credentials(credentialsNamespace))
			createClass(t, fullClass(cp, credentialsNamespace))
			r := unrolledTenant(t, ns, cp)
			before := getEtcd(t, ns, cp)
			editClass(t, cp, quota("3Gi"))
			grantClass(t, ns, cp, cp)
			mustPass(t, r, ns, cp)
			runsClass(t, ns, cp, "3Gi", "2")

			row.keep(t, cp, credentialsNamespace)
			current := getEtcd(t, ns, cp).ResourceVersion
			stale := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if etcd, ok := obj.(*druidv1alpha1.Etcd); ok {
						before.DeepCopyInto(etcd)
						return nil
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			if _, err := passOf(t, operatorAt(stale, revision), ns, cp); !apierrors.IsConflict(err) {
				t.Errorf("a pass on a cache that holds the Etcd before the grant returned %v, want a conflict", err)
			}
			if got := getEtcd(t, ns, cp).ResourceVersion; got != current {
				t.Errorf("a pass on a cache that holds the Etcd before the grant moved it from resourceVersion %s to %s", current, got)
			}
			runsClass(t, ns, cp, "3Gi", "2")

			mustPass(t, r, ns, cp)
			runsClass(t, ns, cp, "3Gi", "2")
		})
	}
}

func TestAPassThatKeepsTheClassFromAStaleCacheKeepsTheCopyOfTheStoreTheEtcdNames(t *testing.T) {
	const next = "v0.0.0-test+next"
	for i, row := range []struct {
		name          string
		operator      string
		hold, release func(t *testing.T, ns, cp string)
	}{
		{"its edit waits for the grant", revision,
			func(*testing.T, string, string) {},
			func(t *testing.T, ns, cp string) { grantClass(t, ns, cp, cp) }},
		{"the operator revision is held", next,
			func(t *testing.T, ns, cp string) { grantClass(t, ns, cp, cp) },
			func(t *testing.T, ns, cp string) { grant(t, getControlPlane(t, ns, cp), next) }},
	} {
		t.Run(row.name, func(t *testing.T) {
			ns, credentialsNamespace := namespace(t), namespace(t)
			cp := fmt.Sprintf("stale-copy-%d", i)
			create(t, credentials(credentialsNamespace))
			create(t, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "s3-other", Namespace: credentialsNamespace}, Data: map[string][]byte{"accessKeyID": []byte("other-access")}})
			createClass(t, fullClass(cp, credentialsNamespace))
			r := classTenant(t, ns, cp, cp)
			before := getEtcd(t, ns, cp)
			copied := func(t *testing.T, bucket, accessKeyID string) {
				t.Helper()
				if got := *getEtcd(t, ns, cp).Spec.Backup.Store.Container; got != bucket {
					t.Errorf("the Etcd is on bucket %s, want %s", got, bucket)
				}
				if got := string(getCopy(t, ns, cp).Data["accessKeyID"]); got != accessKeyID {
					t.Errorf("%s-etcd-backup holds accessKeyID %s, want %s", cp, got, accessKeyID)
				}
			}
			editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) {
				class.Spec.Backup.Store.Bucket, class.Spec.Backup.Store.SecretRef.Name = "other-bucket", "s3-other"
			})
			grantClass(t, ns, cp, cp)
			mustPass(t, r, ns, cp)
			copied(t, "other-bucket", "other-access")

			editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) {
				class.Spec.Backup.Store.Bucket, class.Spec.Backup.Store.SecretRef.Name = "lodia-backups", "s3"
			})
			row.hold(t, ns, cp)
			stale := interceptor.NewClient(unpaused(k8s), interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if etcd, ok := obj.(*druidv1alpha1.Etcd); ok {
						before.DeepCopyInto(etcd)
						return nil
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			if _, err := passOf(t, operatorAt(stale, row.operator), ns, cp); !apierrors.IsConflict(err) {
				t.Errorf("a pass on a cache that holds the Etcd before the store move returned %v, want a conflict", err)
			}
			copied(t, "other-bucket", "other-access")

			injected := errors.New("injected: the API server did not answer")
			failing := operatorAt(unpaused(k8s), row.operator)
			failing.Reader = interceptor.NewClient(k8s, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*druidv1alpha1.Etcd); ok {
						return injected
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			if _, err := passOf(t, failing, ns, cp); !errors.Is(err, injected) {
				t.Errorf("a pass whose read of the Etcd failed returned %v, want %v", err, injected)
			}
			copied(t, "other-bucket", "other-access")

			mustPass(t, operatorAt(unpaused(k8s), row.operator), ns, cp)
			copied(t, "other-bucket", "other-access")

			row.release(t, ns, cp)
			mustPass(t, operatorAt(unpaused(k8s), row.operator), ns, cp)
			copied(t, "lodia-backups", "spike-access")
		})
	}
}

func TestTheCapNoteStaysWhileAClassEditWaits(t *testing.T) {
	ns := namespace(t)
	const cp = "cap-waits"
	createClass(t, newClass(cp, "Retain"))
	r := classTenant(t, ns, cp, cp)
	editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Storage.Size = resource.MustParse("40Gi")
		class.Spec.Etcd = &lodiav1alpha1.EtcdSettings{QuotaBackendBytes: new(resource.MustParse("6Gi"))}
	})
	grantClass(t, ns, cp, cp)
	mustPass(t, r, ns, cp)
	const capped = "DatastoreClass " + cp + "'s quota 6Gi is capped at 4Gi: it needs 12Gi of volume and Etcd " + cp + "-etcd's is 8Gi"
	if message := readyMessage(t, ns, cp); !strings.HasSuffix(message, "; "+capped) {
		t.Fatalf("a granted class whose quota is capped notes %q", message)
	}

	editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Etcd.Defragmentation = &lodiav1alpha1.Defragmentation{Schedule: "0 4 * * *"}
	})
	mustPass(t, r, ns, cp)
	want := "; DatastoreClass " + cp + " generation 3 waits for status.target.datastoreClass; " + capped
	if message := readyMessage(t, ns, cp); !strings.HasSuffix(message, want) {
		t.Errorf("while an edit waits DatastoreReady says %q, want it to end with %q", message, want)
	}
	mustPass(t, operatorAt(unpaused(k8s), "v0.0.0-test+next"), ns, cp)
	if message := readyMessage(t, ns, cp); !strings.HasSuffix(message, want) {
		t.Errorf("while an edit waits under the operator hold DatastoreReady says %q, want it to end with %q", message, want)
	}
}

func TestAnEtcdWhoseClassAnnotationsAPersonEditedReportsNoClass(t *testing.T) {
	tenants := []struct {
		name   string
		tenant func(t *testing.T, ns, cp string) *datastore.Reconciler
	}{
		{"rolled out", func(t *testing.T, ns, cp string) *datastore.Reconciler { return classTenant(t, ns, cp, cp) }},
		{"never rolled out", unrolledTenant},
	}
	for i, edit := range []string{
		`{"metadata":{"annotations":{"` + lodiav1alpha1.DatastoreClassAnnotation + `":null}}}`,
		`{"metadata":{"annotations":{"` + lodiav1alpha1.DatastoreClassUIDAnnotation + `":null}}}`,
		`{"metadata":{"annotations":{"` + lodiav1alpha1.DatastoreClassGenerationAnnotation + `":"x"}}}`,
		`{"metadata":{"annotations":{"` + lodiav1alpha1.DatastoreClassGenerationAnnotation + `":"0"}}}`,
		`{"metadata":{"annotations":{"` + lodiav1alpha1.DatastoreClassGenerationAnnotation + `":"-1"}}}`,
		`{"metadata":{"annotations":{"` + lodiav1alpha1.DatastoreClassGenerationAnnotation + `":"9223372036854775808"}}}`,
		`{"metadata":{"annotations":{"` + lodiav1alpha1.DatastoreClassUIDAnnotation + `":"` + strings.Repeat("u", 37) + `"}}}`,
		`{"metadata":{"annotations":{"` + lodiav1alpha1.DatastoreClassAnnotation + `":"` + strings.Repeat("n", 254) + `"}}}`,
	} {
		for j, tenant := range tenants {
			t.Run(tenant.name+" "+edit, func(t *testing.T) {
				ns := namespace(t)
				cp := fmt.Sprintf("annotated-%d-%d", i, j)
				createClass(t, newClass(cp, "Retain"))
				r := tenant.tenant(t, ns, cp)
				runsClass(t, ns, cp, "4Gi", "1")
				if err := k8s.Patch(t.Context(), getEtcd(t, ns, cp), client.RawPatch(types.MergePatchType, []byte(edit)), client.FieldOwner("kubectl-annotate")); err != nil {
					t.Fatal(err)
				}
				mustPass(t, r, ns, cp)
				if status := getControlPlane(t, ns, cp).Status.Datastore; status == nil || status.Class != nil {
					t.Errorf("an Etcd annotated %s reports status.datastore %+v, want no class", edit, status)
				}
			})
		}
	}
}

func TestAnEtcdAPersonAnnotatedWhileItRollsKeepsTheClassReportedUntilItRolledOut(t *testing.T) {
	ns := namespace(t)
	const cp = "annotated-rolling"
	createClass(t, newClass(cp, "Retain"))
	r := classTenant(t, ns, cp, cp)
	setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) { s.UpdatedReplicas = 0 })
	if err := k8s.Patch(t.Context(), getEtcd(t, ns, cp), client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"annotations":{"`+lodiav1alpha1.DatastoreClassGenerationAnnotation+`":"x"}}}`)), client.FieldOwner("kubectl-annotate")); err != nil {
		t.Fatal(err)
	}
	mustPass(t, r, ns, cp)
	if got := reportedClass(t, ns, cp); got != 1 {
		t.Errorf("while a member is not updated an Etcd annotated by hand reports class generation %d, want 1 kept", got)
	}
	setStatefulSetStatus(t, ns, cp, nil)
	mustPass(t, r, ns, cp)
	if status := getControlPlane(t, ns, cp).Status.Datastore; status == nil || status.Class != nil {
		t.Errorf("once it rolled out an Etcd annotated by hand reports status.datastore %+v, want no class", status)
	}
}

func TestAClassNameAPersonEditsIsReportedAtOnce(t *testing.T) {
	ns := namespace(t)
	const cp = "renamed"
	first, second := newClass(cp+"-a", "Retain"), classOf(cp+"-b", "3Gi")
	createClass(t, first)
	createClass(t, second)
	r := classTenant(t, ns, cp, first.Name)
	grantClass(t, ns, cp, first.Name)
	mustPass(t, r, ns, cp)
	runsClass(t, ns, cp, "4Gi", "1")

	setStatefulSetStatus(t, ns, cp, func(s *appsv1.StatefulSetStatus) { s.UpdatedReplicas = 0 })
	if err := k8s.Patch(t.Context(), getEtcd(t, ns, cp), client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"annotations":{"`+lodiav1alpha1.DatastoreClassAnnotation+`":"`+second.Name+`"}}}`)), client.FieldOwner("kubectl-annotate")); err != nil {
		t.Fatal(err)
	}
	mustPass(t, r, ns, cp)
	want := lodiav1alpha1.DatastoreClassStatus{Name: second.Name, DatastoreClassGeneration: lodiav1alpha1.DatastoreClassGeneration{UID: first.UID, Generation: 1}}
	if got := getControlPlane(t, ns, cp).Status.Datastore.Class; got == nil || *got != want {
		t.Errorf("while a member is not updated an Etcd whose class name a person edited reports class %+v, want %+v", got, want)
	}

	grantClass(t, ns, cp, second.Name)
	mustPass(t, r, ns, cp)
	setStatefulSetStatus(t, ns, cp, nil)
	rollOut(t, r, ns, cp)
	runsClass(t, ns, cp, "3Gi", "1")
	recordsUID(t, ns, cp, second)
}

func TestAClassAnnotationTheSchemaTakesIsReportedAsItIs(t *testing.T) {
	for i, row := range []struct{ annotation, value string }{
		{lodiav1alpha1.DatastoreClassAnnotation, strings.Repeat("ü", 253)},
		{lodiav1alpha1.DatastoreClassUIDAnnotation, strings.Repeat("ü", 36)},
	} {
		t.Run(row.annotation, func(t *testing.T) {
			ns := namespace(t)
			cp := fmt.Sprintf("annotated-wide-%d", i)
			createClass(t, newClass(cp, "Retain"))
			r := classTenant(t, ns, cp, cp)
			etcd := getEtcd(t, ns, cp)
			edit := client.MergeFrom(etcd.DeepCopy())
			metav1.SetMetaDataAnnotation(&etcd.ObjectMeta, row.annotation, row.value)
			if err := k8s.Patch(t.Context(), etcd, edit, client.FieldOwner("kubectl-annotate")); err != nil {
				t.Fatal(err)
			}
			mustPass(t, r, ns, cp)
			want := lodiav1alpha1.DatastoreClassStatus{
				Name:                     etcd.Annotations[lodiav1alpha1.DatastoreClassAnnotation],
				DatastoreClassGeneration: lodiav1alpha1.DatastoreClassGeneration{UID: types.UID(etcd.Annotations[lodiav1alpha1.DatastoreClassUIDAnnotation]), Generation: 1},
			}
			if status := getControlPlane(t, ns, cp).Status.Datastore; status == nil || status.Class == nil || *status.Class != want {
				t.Errorf("an Etcd whose %s holds %d characters of two bytes reports status.datastore %+v, want class %+v", row.annotation, len([]rune(row.value)), status, want)
			}
		})
	}
}

func TestAnEtcdAPersonTookTheQuotaFromNotesNoCap(t *testing.T) {
	ns := namespace(t)
	const cp = "unquoted"
	createClass(t, newClass(cp, "Retain"))
	classTenant(t, ns, cp, cp)
	editClass(t, cp, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Storage.Size = resource.MustParse("40Gi")
		class.Spec.Etcd = &lodiav1alpha1.EtcdSettings{QuotaBackendBytes: new(resource.MustParse("6Gi"))}
	})
	if err := k8s.Patch(t.Context(), getEtcd(t, ns, cp), client.RawPatch(types.MergePatchType, []byte(`{"spec":{"etcd":{"quota":null}}}`)), client.FieldOwner("kubectl-edit")); err != nil {
		t.Fatal(err)
	}
	for _, operator := range []string{revision, "v0.0.0-test+next"} {
		mustPass(t, operatorAt(unpaused(k8s), operator), ns, cp)
		if message := readyMessage(t, ns, cp); strings.Contains(message, "capped") {
			t.Errorf("an Etcd without a quota under operator %s notes %q", operator, message)
		}
	}
}
