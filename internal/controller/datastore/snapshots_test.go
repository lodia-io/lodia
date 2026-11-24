// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/etcddruid/druidtest"
)

var storeEdits = []struct{ name, patch, why string }{
	{"an unknown provider", `{"spec":{"backup":{"store":{"provider":"Local"}}}}`, "provider"},
	{"no container", `{"spec":{"backup":{"store":{"container":null}}}}`, "bucket"},
}

func editStore(t *testing.T, ns, cp, patch string) {
	t.Helper()
	if err := k8s.Patch(t.Context(), getEtcd(t, ns, cp), client.RawPatch(types.MergePatchType, []byte(patch)), client.FieldOwner("kubectl-edit")); err != nil {
		t.Fatal(err)
	}
}

func TestAStoreTheStatusCannotHoldIsLeftOutAndSaid(t *testing.T) {
	t.Parallel()
	for i, edit := range storeEdits {
		t.Run(edit.name, func(t *testing.T) {
			ns, credentialsNamespace := namespace(t), namespace(t)
			cp := fmt.Sprintf("odd-store-%d", i)
			create(t, credentials(credentialsNamespace))
			createClass(t, fullClass(cp, credentialsNamespace))
			heldTenant(t, ns, cp, cp, nil)
			editStore(t, ns, cp, edit.patch)
			setEtcdStatus(t, ns, cp, druidtest.Members(1, 0))

			for _, pass := range []struct{ name, ready string }{{"a pass", lodiav1alpha1.ReasonQuorumLost}, {"a pass of the deletion", lodiav1alpha1.ReasonDeleting}} {
				if pass.ready == lodiav1alpha1.ReasonDeleting {
					if err := k8s.Delete(t.Context(), getControlPlane(t, ns, cp)); err != nil {
						t.Fatal(err)
					}
				}
				mustPass(t, operatorAt(unpaused(k8s), revision), ns, cp)
				status := getControlPlane(t, ns, cp).Status
				if status.Datastore == nil || status.Datastore.Snapshots != nil {
					t.Errorf("%s: status.datastore is %+v, want it without snapshots", pass.name, status.Datastore)
				}
				if ready := meta.FindStatusCondition(status.Conditions, lodiav1alpha1.ConditionDatastoreReady); ready == nil || ready.Reason != pass.ready {
					t.Errorf("%s: DatastoreReady is %+v, want %s", pass.name, ready, pass.ready)
				}
				backup := meta.FindStatusCondition(status.Conditions, lodiav1alpha1.ConditionBackupReady)
				if backup == nil || backup.Status != metav1.ConditionFalse || backup.Reason != lodiav1alpha1.ReasonStoreUnsupported || !strings.Contains(backup.Message, edit.why) {
					t.Errorf("%s: BackupReady is %+v, want False, %s, naming the %s", pass.name, backup, lodiav1alpha1.ReasonStoreUnsupported, edit.why)
				}
			}
		})
	}
}

func TestABackupWaitsWhileItsSourceNamesAStoreItsStatusCannotHold(t *testing.T) {
	t.Parallel()
	prefix := func(prefix string) string { return `{"spec":{"backup":{"store":{"prefix":"` + prefix + `"}}}}` }
	for _, c := range []struct{ name, patch, message string }{
		{"an unknown provider", storeEdits[0].patch, "Etcd src-etcd names a backup store that status.datastore.snapshots cannot hold: its provider is none of"},
		{"no prefix", prefix(""), "Etcd src-etcd names a backup store that status.datastore.snapshots cannot hold: its prefix is not 1 to 512 characters long"},
		{"a prefix of 513 characters", prefix(strings.Repeat("p", 513)), "Etcd src-etcd names a backup store that status.datastore.snapshots cannot hold: its prefix is not 1 to 512 characters long"},
		{"a prefix of 504 characters whose Backup's is longer", prefix(strings.Repeat("p", 500) + "/a/b"), "the location of the Backup's copy in the store of Etcd src-etcd is one status.snapshots cannot hold: its prefix is not 1 to 512 characters long"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			s := newSource(t, nil, nil)
			editStore(t, s.ns, sourceName, c.patch)
			createBackup(t, s.ns, "b", nil)
			r := s.reconciler(k8s)
			passes(t, r, s.ns, "b", readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, c.message))
			if result, err := reconcileBackup(r, s.ns, "b"); err != nil || result.RequeueAfter != 10*time.Second {
				t.Errorf("the waiting pass returned %+v, %v, want a retry in 10 seconds", result, err)
			}
			if backup := getBackup(t, s.ns, "b"); backup.Status.StartedAt != nil || backup.Status.Snapshots != nil {
				t.Errorf("the Backup started on a store its status cannot hold: %+v", backup.Status)
			}
		})
	}
}

func TestTheStatusTakesAStoreExactlyWhenTheCheckSaysItCan(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	cp := controlPlane(ns, "bounds", "")
	cp.Spec.Paused = true
	backup := &lodiav1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "bounds", Namespace: ns}, Spec: lodiav1alpha1.BackupSpec{ControlPlaneRef: lodiav1alpha1.LocalReference{Name: "bounds"}}}
	for _, obj := range []client.Object{cp, backup} {
		if err := k8s.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	apply := func(kind string, status any) error {
		content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(status)
		if err != nil {
			t.Fatal(err)
		}
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": lodiav1alpha1.GroupVersion.String(),
			"kind":       kind,
			"metadata":   map[string]any{"name": "bounds", "namespace": ns},
			"status":     content,
		}}
		return k8s.Status().Apply(t.Context(), client.ApplyConfigurationFromUnstructured(u), client.FieldOwner("test/bounds"), client.ForceOwnership)
	}
	base := lodiav1alpha1.SnapshotLocation{Provider: "S3", Bucket: "b", Prefix: "p"}
	var locations []lodiav1alpha1.SnapshotLocation
	for _, provider := range []lodiav1alpha1.StoreProvider{"S3", "GCS", "ABS", "Swift", "s3", "Local", ""} {
		location := base
		location.Provider = provider
		locations = append(locations, location)
	}
	for _, character := range []string{"b", "ü"} {
		for _, n := range []int{0, 1, 63, 64} {
			location := base
			location.Bucket = strings.Repeat(character, n)
			locations = append(locations, location)
		}
		for _, n := range []int{0, 1, 512, 513} {
			location := base
			location.Prefix = strings.Repeat(character, n)
			locations = append(locations, location)
		}
		for _, n := range []int{0, 1, 512, 513} {
			location := base
			location.Endpoint = strings.Repeat(character, n)
			locations = append(locations, location)
		}
	}
	for _, location := range locations {
		why := datastore.Unrecordable(location)
		described := fmt.Sprintf("provider %q, a bucket of %d characters, a prefix of %d and an endpoint of %d", location.Provider,
			utf8.RuneCountInString(location.Bucket), utf8.RuneCountInString(location.Prefix), utf8.RuneCountInString(location.Endpoint))
		for kind, status := range map[string]any{
			"ControlPlane": &lodiav1alpha1.ControlPlaneStatus{Datastore: &lodiav1alpha1.DatastoreStatus{
				Endpoints: []lodiav1alpha1.HostPort{{Host: "bounds-etcd-client." + ns + ".svc", Port: 2379}},
				Snapshots: &location,
			}},
			"Backup": &lodiav1alpha1.BackupStatus{Snapshots: &lodiav1alpha1.BackupSnapshotLocation{SnapshotLocation: location}},
		} {
			if err := apply(kind, status); (err == nil) != (why == "") {
				t.Errorf("%s's status with %s: the check says %q, the API server %v", kind, described, why, err)
			}
		}
	}
}
