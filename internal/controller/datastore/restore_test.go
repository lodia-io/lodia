// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	clientv3 "go.etcd.io/etcd/client/v3"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
	"go.lodia.io/lodia/internal/controller/datastore"
)

type restoredEtcd struct {
	*clientv3.Client
	unreachable                                atomic.Bool
	refuseCounts, refuseCleanups, loseCleanups atomic.Int32
}

var restoredEtcds sync.Map

var errUnanswered = errors.New("injected: the etcd did not answer")

func dialRestored(_ context.Context, etcd *druidv1alpha1.Etcd) (*clientv3.Client, error) {
	value, ok := restoredEtcds.Load(etcd.Namespace + "/" + etcd.Name)
	if !ok {
		return nil, fmt.Errorf("no etcd stands in for Etcd %s/%s", etcd.Namespace, etcd.Name)
	}
	e := value.(*restoredEtcd)
	if e.unreachable.Load() {
		return nil, errUnanswered
	}
	c, err := clientv3.New(clientv3.Config{Endpoints: e.Endpoints(), DialTimeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	c.KV = faultyKV{KV: c.KV, e: e}
	return c, nil
}

type faultyKV struct {
	clientv3.KV
	e *restoredEtcd
}

func (kv faultyKV) Txn(ctx context.Context) clientv3.Txn {
	return faultyTxn{Txn: kv.KV.Txn(ctx), e: kv.e}
}

type faultyTxn struct {
	clientv3.Txn
	e           *restoredEtcd
	conditional bool
}

func (t faultyTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	return faultyTxn{Txn: t.Txn.If(cs...), e: t.e, conditional: true}
}

func (t faultyTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	return faultyTxn{Txn: t.Txn.Then(ops...), e: t.e, conditional: t.conditional}
}

func (t faultyTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	return faultyTxn{Txn: t.Txn.Else(ops...), e: t.e, conditional: t.conditional}
}

func (t faultyTxn) Commit() (*clientv3.TxnResponse, error) {
	if !t.conditional && take(&t.e.refuseCounts) || t.conditional && take(&t.e.refuseCleanups) {
		return nil, errUnanswered
	}
	response, err := t.Txn.Commit()
	if err == nil && t.conditional && take(&t.e.loseCleanups) {
		return nil, errUnanswered
	}
	return response, err
}

func take(n *atomic.Int32) bool {
	for {
		v := n.Load()
		if v <= 0 {
			return false
		}
		if n.CompareAndSwap(v, v-1) {
			return true
		}
	}
}

var (
	cleanedKeys = []string{
		"/registry/minions/source-node",
		"/registry/leases/kube-system/kube-controller-manager",
		"/registry/leases/kube-node-lease/source-node",
		"/registry/certificatesigningrequests/csr-source-node",
		"/registry/podcertificaterequests/default/pcr",
		"/registry/secrets/kube-system/bootstrap-token-abcdef",
		"/registry/configmaps/kube-system/extension-apiserver-authentication",
	}
	keptKeys = []string{
		"/registry/minionsx/n",
		"/registry/leases/kube-system-x/l",
		"/registry/leases/kube-node-lease-x/l",
		"/registry/certificatesigningrequestsx/c",
		"/registry/podcertificaterequestsx/p",
		"/registry/secrets/kube-system/bootstrap-tokenx",
		"/registry/secrets/kube-system/other",
		"/registry/configmaps/kube-system/extension-apiserver-authentication-x",
		"/registry/volumeattachments/v",
		"/registry/secrets/restore-a/data",
	}
)

func (e *restoredEtcd) put(t *testing.T, keys ...string) {
	t.Helper()
	for batch := range slices.Chunk(keys, 100) {
		var ops []clientv3.Op
		for _, key := range batch {
			ops = append(ops, clientv3.OpPut(key, "value of "+key))
		}
		if _, err := e.Txn(t.Context()).Then(ops...).Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

func (e *restoredEtcd) get(t *testing.T, key string) *clientv3.GetResponse {
	t.Helper()
	response, err := e.Get(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func (e *restoredEtcd) count(t *testing.T, prefix string) int64 {
	t.Helper()
	response, err := e.Get(t.Context(), prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		t.Fatal(err)
	}
	return response.Count
}

func (e *restoredEtcd) present(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if len(e.get(t, key).Kvs) == 0 {
			t.Errorf("%s is gone from the restored etcd", key)
		}
	}
}

func (e *restoredEtcd) absent(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if len(e.get(t, key).Kvs) != 0 {
			t.Errorf("%s is still in the restored etcd", key)
		}
	}
}

const (
	restoredCP = "b"
	backupName = "a-1"
)

type restore struct {
	ns, credentials string
	backup          *lodiav1alpha1.Backup
	etcd            *restoredEtcd
	source          lodiav1alpha1.UIDReference
}

type restoreSetup struct {
	class        func(*lodiav1alpha1.DatastoreClass)
	backup       func(*lodiav1alpha1.Backup)
	noBackup     bool
	preload      func(*testing.T, *restoredEtcd)
	before       func(*restore)
	controlPlane func(*lodiav1alpha1.ControlPlane)
}

func newRestore(t *testing.T, setup restoreSetup) *restore {
	t.Helper()
	r := &restore{ns: namespace(t), credentials: namespace(t), source: lodiav1alpha1.UIDReference{Name: "src", UID: "5e1f7c3a-0000-4000-8000-000000000001"}}
	create(t, credentials(r.credentials))
	class := fullClass(r.ns, r.credentials)
	if setup.class != nil {
		setup.class(class)
	}
	createClass(t, class)
	r.etcd = &restoredEtcd{Client: testserver.RunEtcd(t, nil).Client}
	key := r.ns + "/" + restoredCP + "-etcd"
	restoredEtcds.Store(key, r.etcd)
	t.Cleanup(func() { restoredEtcds.Delete(key) })
	if !setup.noBackup {
		r.backup = sealedBackup(t, r.ns, backupName, r.source, setup.backup)
		if _, err := r.etcd.Put(t.Context(), lodiav1alpha1.BackupSealKey(r.backup.UID), string(r.backup.UID)); err != nil {
			t.Fatal(err)
		}
	}
	if setup.preload != nil {
		setup.preload(t, r.etcd)
	} else {
		r.etcd.put(t, append(slices.Clone(cleanedKeys), keptKeys...)...)
	}
	if setup.before != nil {
		setup.before(r)
	}
	cp := controlPlane(r.ns, restoredCP, r.ns)
	cp.Spec.RestoreFrom = &lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: backupName}}
	if setup.controlPlane != nil {
		setup.controlPlane(cp)
	}
	create(t, cp)
	writeGeneration(t, r.ns, restoredCP, 1)
	return r
}

func sealedBackup(t *testing.T, ns, name string, source lodiav1alpha1.UIDReference, edit func(*lodiav1alpha1.Backup)) *lodiav1alpha1.Backup {
	t.Helper()
	backup := &lodiav1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       lodiav1alpha1.BackupSpec{ControlPlaneRef: lodiav1alpha1.LocalReference{Name: source.Name}},
	}
	create(t, backup)
	now := metav1.Now()
	prefix := "prod/" + string(namespaceUID(t, ns)) + "/backups/" + string(backup.UID)
	status := lodiav1alpha1.BackupStatus{
		Conditions: []metav1.Condition{{
			Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonCompleted,
			Message: "sealed at revision 5 and copied to lodia-backups/" + prefix, LastTransitionTime: now, ObservedGeneration: backup.Generation,
		}},
		Source:            &source,
		KubernetesVersion: "v1.37.1",
		Network:           &lodiav1alpha1.Network{ServiceCIDRs: []lodiav1alpha1.CIDR{"10.96.0.0/16"}, PodCIDRs: []lodiav1alpha1.CIDR{"10.244.0.0/16"}, ClusterDomain: "cluster.local"},
		Snapshots: &lodiav1alpha1.BackupSnapshotLocation{
			SnapshotLocation: lodiav1alpha1.SnapshotLocation{Provider: "S3", Endpoint: "https://s3.example.test", Bucket: "lodia-backups", Prefix: prefix},
			FromSecret:       &lodiav1alpha1.StoreSecretFields{Region: new("eu-central-1")},
		},
		Seal:        &lodiav1alpha1.BackupSeal{Revision: 5, QuotaBackendBytes: new(resource.MustParse("2Gi"))},
		StartedAt:   &now,
		CompletedAt: &now,
	}
	backup.Annotations = map[string]string{lodiav1alpha1.SealedUIDAnnotation: string(backup.UID)}
	backup.Status = status
	if edit != nil {
		edit(backup)
	}
	status = backup.Status
	if err := k8s.Update(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	backup.Status = status
	if err := k8s.Status().Update(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	return backup
}

func readyAs(status metav1.ConditionStatus, reason, message string) func(*lodiav1alpha1.Backup) {
	return func(b *lodiav1alpha1.Backup) {
		b.Status.Conditions[0].Status, b.Status.Conditions[0].Reason, b.Status.Conditions[0].Message = status, reason, message
	}
}

func (r *restore) controlPlane(t *testing.T) *lodiav1alpha1.ControlPlane {
	t.Helper()
	return getControlPlane(t, r.ns, restoredCP)
}

func (r *restore) taskName(t *testing.T) string {
	t.Helper()
	return "restore-" + string(r.controlPlane(t).UID)
}

func (r *restore) copyTask(t *testing.T) *druidv1alpha1.EtcdCopyBackupsTask {
	t.Helper()
	task := &druidv1alpha1.EtcdCopyBackupsTask{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: r.ns, Name: r.taskName(t)}, task); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		t.Fatal(err)
	}
	return task
}

func (r *restore) opsTask(t *testing.T) *druidv1alpha1.EtcdOpsTask {
	t.Helper()
	task := &druidv1alpha1.EtcdOpsTask{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: r.ns, Name: r.taskName(t)}, task); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		t.Fatal(err)
	}
	return task
}

func (r *restore) noEtcd(t *testing.T) {
	t.Helper()
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: r.ns, Name: restoredCP + "-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
		t.Errorf("want no Etcd, got %v", err)
	}
}

func (r *restore) nothingCreated(t *testing.T) {
	t.Helper()
	r.noEtcd(t)
	if task := r.copyTask(t); task != nil {
		t.Errorf("EtcdCopyBackupsTask %s exists", task.Name)
	}
	if task := r.opsTask(t); task != nil {
		t.Errorf("EtcdOpsTask %s exists", task.Name)
	}
}

func (r *restore) waitCopyTask(t *testing.T) *druidv1alpha1.EtcdCopyBackupsTask {
	t.Helper()
	var task *druidv1alpha1.EtcdCopyBackupsTask
	eventually(t, func() error {
		if task = r.copyTask(t); task == nil {
			return errors.New("no EtcdCopyBackupsTask yet")
		}
		return nil
	})
	return task
}

func (r *restore) copied(t *testing.T) *druidv1alpha1.Etcd {
	t.Helper()
	finishCopy(t, r.ns, r.waitCopyTask(t).Name, druidv1alpha1.EtcdCopyBackupsTaskSucceeded, "")
	etcd := waitEtcd(t, r.ns, restoredCP, nil)
	runStatefulSet(t, r.ns, restoredCP, 1)
	setEtcdStatus(t, r.ns, restoredCP, nil)
	return etcd
}

func (r *restore) marker(t *testing.T) int64 {
	t.Helper()
	response := r.etcd.get(t, lodiav1alpha1.RestoreMarkerKey(r.controlPlane(t).UID))
	if len(response.Kvs) == 0 {
		return 0
	}
	if kv := response.Kvs[0]; kv.GetCreateRevision() != kv.GetModRevision() {
		t.Errorf("the marker was created at %d and written again at %d", kv.GetCreateRevision(), kv.GetModRevision())
	}
	return response.Kvs[0].GetCreateRevision()
}

func (r *restore) cleanedUp(t *testing.T) int64 {
	t.Helper()
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "cleaned up; waiting for a full snapshot of revision")
	return r.marker(t)
}

func (r *restore) complete(t *testing.T, marker int64) *lodiav1alpha1.ControlPlane {
	t.Helper()
	lease := restoredCP + "-etcd-full-snap"
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: r.ns, Name: lease}, &coordinationv1.Lease{}); apierrors.IsNotFound(err) {
		createLease(t, r.ns, lease, strconv.FormatInt(marker, 10))
	} else {
		setLease(t, r.ns, lease, marker)
	}
	poke(t, r.ns)
	waitCondition(t, r.ns, restoredCP, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum")
	cp := r.controlPlane(t)
	if cp.Status.Datastore == nil || cp.Status.Datastore.Restore == nil {
		t.Fatalf("DatastoreReady is True without status.datastore.restore: %+v", cp.Status.Datastore)
	}
	return cp
}

func poke(t *testing.T, ns string) {
	t.Helper()
	cp := getControlPlane(t, ns, restoredCP)
	patch := client.MergeFrom(cp.DeepCopy())
	metav1.SetMetaDataAnnotation(&cp.ObjectMeta, "test.lodia.io/poked", strconv.FormatInt(time.Now().UnixNano(), 10))
	if err := k8s.Patch(t.Context(), cp, patch); err != nil {
		t.Fatal(err)
	}
}

func TestARestoreCopiesChecksCleansUpAndSnapshots(t *testing.T) {
	r := newRestore(t, restoreSetup{controlPlane: func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
	}})
	cp := r.controlPlane(t)
	task := r.waitCopyTask(t)
	name := "restore-" + string(cp.UID)
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "copying Backup a-1 into this ControlPlane's prefix: EtcdCopyBackupsTask "+name)
	target := "prod/" + string(namespaceUID(t, r.ns)) + "/controlplanes/" + string(cp.UID)
	want := druidv1alpha1.EtcdCopyBackupsTaskSpec{
		SourceStore: druidv1alpha1.StoreSpec{
			Provider: new(druidv1alpha1.StorageProvider("S3")), Container: new("lodia-backups"), EndpointOverride: new("https://s3.example.test"),
			Prefix: r.backup.Status.Snapshots.Prefix, SecretRef: &corev1.SecretReference{Name: "b-etcd-backup"},
		},
		TargetStore: druidv1alpha1.StoreSpec{
			Provider: new(druidv1alpha1.StorageProvider("S3")), Container: new("lodia-backups"), EndpointOverride: new("https://s3.example.test"),
			Prefix: target, SecretRef: &corev1.SecretReference{Name: "b-etcd-backup"},
		},
		MaxBackups: new(uint32(1)),
	}
	if !reflect.DeepEqual(task.Spec, want) {
		t.Errorf("the copy task's spec is\n%+v\nwant\n%+v", task.Spec, want)
	}
	owned(t, task, cp)
	r.noEtcd(t)

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		task := r.copyTask(t)
		controllerutil.AddFinalizer(task, "druid.gardener.cloud/etcd-druid")
		return k8s.Update(t.Context(), task)
	}); err != nil {
		t.Fatal(err)
	}
	finishCopy(t, r.ns, name, druidv1alpha1.EtcdCopyBackupsTaskFailed, "the bucket is gone")
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "the copy of Backup a-1 failed: the bucket is gone; copying again")
	r.noEtcd(t)
	failed := r.copyTask(t)
	if failed.DeletionTimestamp.IsZero() {
		t.Fatal("the failed copy task is not being deleted")
	}
	controllerutil.RemoveFinalizer(failed, "druid.gardener.cloud/etcd-druid")
	if err := k8s.Update(t.Context(), failed); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		if again := r.copyTask(t); again == nil || again.UID == failed.UID {
			return errors.New("no new copy task yet")
		}
		return nil
	})
	if again := r.copyTask(t); !reflect.DeepEqual(again.Spec, want) {
		t.Errorf("the second copy task's spec is %+v, want %+v", again.Spec, want)
	}
	r.noEtcd(t)

	etcd := r.copied(t)
	if etcd.Spec.Replicas != 1 || etcd.Spec.Backup.Store == nil || etcd.Spec.Backup.Store.Prefix != target {
		t.Fatalf("the restoring Etcd asks for %d members with store %+v, want 1 with prefix %s", etcd.Spec.Replicas, etcd.Spec.Backup.Store, target)
	}
	marker := r.cleanedUp(t)
	r.etcd.absent(t, cleanedKeys...)
	r.etcd.present(t, keptKeys...)
	value := restoreMarkerValue(t, r)
	if want := (restoreMarkerJSON{Backup: lodiav1alpha1.UIDReference{Name: backupName, UID: r.backup.UID}, Source: r.source}); value != want {
		t.Errorf("the marker holds %+v, want %+v", value, want)
	}
	ops := r.opsTask(t)
	if ops == nil {
		t.Fatal("no EtcdOpsTask")
	}
	if want := (druidv1alpha1.EtcdOpsTaskSpec{
		Config:                  druidv1alpha1.EtcdOpsTaskConfig{OnDemandSnapshot: &druidv1alpha1.OnDemandSnapshotConfig{Type: druidv1alpha1.OnDemandSnapshotTypeFull, TimeoutSecondsDelta: new(int32(60)), TimeoutSecondsFull: new(int32(900))}},
		TTLSecondsAfterFinished: new(int32(3600)),
		EtcdName:                new("b-etcd"),
	}); !reflect.DeepEqual(ops.Spec, want) {
		t.Errorf("the EtcdOpsTask's spec is %+v, want %+v", ops.Spec, want)
	}
	owned(t, ops, cp)
	rewritten := []string{"/registry/minions/b-node", "/registry/configmaps/kube-system/extension-apiserver-authentication"}
	r.etcd.put(t, rewritten...)

	createLease(t, r.ns, "b-etcd-full-snap", strconv.FormatInt(marker-1, 10))
	createLease(t, r.ns, "b-etcd-delta-snap", strconv.FormatInt(marker, 10))
	poke(t, r.ns)
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring,
		fmt.Sprintf("cleaned up; waiting for a full snapshot of revision %d: the store holds %d; EtcdOpsTask %s is ", marker, marker-1, name))
	finishFlush(t, r.ns, name, druidv1alpha1.TaskStateSucceeded, time.Now())
	poke(t, r.ns)
	eventually(t, func() error {
		if again := r.opsTask(t); again == nil || again.UID == ops.UID {
			return errors.New("the finished EtcdOpsTask is not replaced yet")
		}
		return nil
	})
	if got := getEtcd(t, r.ns, restoredCP); got.Spec.Replicas != 1 {
		t.Fatalf("the Etcd asks for %d members before the restore is complete", got.Spec.Replicas)
	}

	restored := r.complete(t, marker)
	if got := restored.Status.Datastore.Restore; got.Backup != (lodiav1alpha1.UIDReference{Name: backupName, UID: r.backup.UID}) || got.Source != r.source || got.CompletedAt.IsZero() {
		t.Errorf("status.datastore.restore is %+v", got)
	}
	waitEtcd(t, r.ns, restoredCP, func(etcd *druidv1alpha1.Etcd) error {
		if etcd.Spec.Replicas != 3 {
			return fmt.Errorf("the Etcd asks for %d members once the restore is complete", etcd.Spec.Replicas)
		}
		return nil
	})
	eventually(t, func() error {
		if r.copyTask(t) != nil || r.opsTask(t) != nil {
			return errors.New("the restore's tasks are still there")
		}
		return nil
	})
	completedAt := restored.Status.Datastore.Restore.CompletedAt
	poke(t, r.ns)
	consistently(t, func() error {
		cp := r.controlPlane(t)
		if cp.Status.Datastore == nil || cp.Status.Datastore.Restore == nil || !cp.Status.Datastore.Restore.CompletedAt.Equal(&completedAt) {
			return fmt.Errorf("status.datastore is %+v, want the restore completed at %v", cp.Status.Datastore, completedAt)
		}
		return nil
	})
	r.etcd.present(t, rewritten...)
	if got := r.marker(t); got != marker {
		t.Errorf("the marker moved from revision %d to %d", marker, got)
	}

	r.etcd.unreachable.Store(true)
	poke(t, r.ns)
	consistently(t, func() error {
		if etcd := getEtcd(t, r.ns, restoredCP); etcd.Spec.Replicas != 3 {
			return fmt.Errorf("a pass whose check could not run set %d members", etcd.Spec.Replicas)
		}
		ready := meta.FindStatusCondition(r.controlPlane(t).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
		if ready.Status != metav1.ConditionTrue {
			return fmt.Errorf("a complete restore whose etcd does not answer is DatastoreReady %s/%s %q", ready.Status, ready.Reason, ready.Message)
		}
		return nil
	})
}

type restoreMarkerJSON struct {
	Backup lodiav1alpha1.UIDReference `json:"backup"`
	Source lodiav1alpha1.UIDReference `json:"source"`
}

func restoreMarkerValue(t *testing.T, r *restore) restoreMarkerJSON {
	t.Helper()
	response := r.etcd.get(t, lodiav1alpha1.RestoreMarkerKey(r.controlPlane(t).UID))
	if len(response.Kvs) == 0 {
		t.Fatal("no marker")
	}
	var value restoreMarkerJSON
	if err := json.Unmarshal(response.Kvs[0].GetValue(), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func owned(t *testing.T, obj client.Object, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	want := []metav1.OwnerReference{{APIVersion: "lodia.io/v1alpha1", Kind: "ControlPlane", Name: cp.Name, UID: cp.UID, Controller: new(true), BlockOwnerDeletion: new(true)}}
	if !reflect.DeepEqual(obj.GetOwnerReferences(), want) {
		t.Errorf("%s has ownerReferences %+v, want %+v", obj.GetName(), obj.GetOwnerReferences(), want)
	}
	if want := map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name}; !reflect.DeepEqual(obj.GetLabels(), want) {
		t.Errorf("%s has labels %v, want %v", obj.GetName(), obj.GetLabels(), want)
	}
}

func TestARestoreStartsOnlyFromASealedCompatibleBackupInTheClasssStore(t *testing.T) {
	for _, c := range []struct {
		name            string
		setup           restoreSetup
		reason, message string
	}{
		{"absent", restoreSetup{noBackup: true}, lodiav1alpha1.ReasonBackupNotFound, "no Backup a-1"},
		{"no Ready", restoreSetup{backup: func(b *lodiav1alpha1.Backup) { b.Status.Conditions = nil }}, lodiav1alpha1.ReasonWaitingForBackup, "Backup a-1 has no Ready yet"},
		{"in progress", restoreSetup{backup: readyAs(metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying, "EtcdCopyBackupsTask backup-x copies")},
			lodiav1alpha1.ReasonWaitingForBackup, "Backup a-1 is Copying: EtcdCopyBackupsTask backup-x copies"},
		{"waiting for its source", restoreSetup{backup: readyAs(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "DatastoreReady of src is False")},
			lodiav1alpha1.ReasonWaitingForBackup, "Backup a-1 is WaitingForDatastore: DatastoreReady of src is False"},
		{"its source not created yet", restoreSetup{backup: func(b *lodiav1alpha1.Backup) {
			readyAs(metav1.ConditionFalse, lodiav1alpha1.ReasonControlPlaneNotFound, "no ControlPlane src")(b)
			b.Status.StartedAt = nil
		}}, lodiav1alpha1.ReasonWaitingForBackup, "Backup a-1 is ControlPlaneNotFound: no ControlPlane src"},
		{"its source gone", restoreSetup{backup: readyAs(metav1.ConditionFalse, lodiav1alpha1.ReasonControlPlaneNotFound, "no ControlPlane src")},
			lodiav1alpha1.ReasonRestoreFailed, "Backup a-1 is ControlPlaneNotFound: no ControlPlane src; restore another Backup"},
		{"failed", restoreSetup{backup: readyAs(metav1.ConditionFalse, lodiav1alpha1.ReasonFailed, "the copy failed: denied; create another Backup")},
			lodiav1alpha1.ReasonRestoreFailed, "Backup a-1 is Failed: the copy failed: denied; create another Backup; restore another Backup"},
		{"no store", restoreSetup{backup: readyAs(metav1.ConditionFalse, lodiav1alpha1.ReasonNoBackupStore, "no store")},
			lodiav1alpha1.ReasonRestoreFailed, "Backup a-1 is NoBackupStore: no store; restore another Backup"},
		{"unsupported", restoreSetup{backup: readyAs(metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported, "shared")},
			lodiav1alpha1.ReasonRestoreFailed, "Backup a-1 is Unsupported: shared; restore another Backup"},
		{"seal lost", restoreSetup{backup: readyAs(metav1.ConditionFalse, lodiav1alpha1.ReasonSealLost, "sealed as x")},
			lodiav1alpha1.ReasonRestoreFailed, "Backup a-1 is SealLost: sealed as x; restore another Backup"},
		{"keys controlled elsewhere", restoreSetup{backup: readyAs(metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere, "Secret a-1.encryption-config exists")},
			lodiav1alpha1.ReasonRestoreFailed, "Backup a-1 is ObjectsControlledElsewhere: Secret a-1.encryption-config exists; restore another Backup"},
		{"completed, not marked", restoreSetup{backup: func(b *lodiav1alpha1.Backup) { b.Annotations = nil }},
			lodiav1alpha1.ReasonWaitingForBackup, "Backup a-1 is Completed, and its lodia.io/sealed-uid does not name its UID yet"},
		{"completed, marked with another UID", restoreSetup{backup: func(b *lodiav1alpha1.Backup) {
			b.Annotations[lodiav1alpha1.SealedUIDAnnotation] = "5e1f7c3a-0000-4000-8000-0000000000ff"
		}}, lodiav1alpha1.ReasonWaitingForBackup, "Backup a-1 is Completed, and its lodia.io/sealed-uid does not name its UID yet"},
		{"another minor", restoreSetup{controlPlane: func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Version = "v1.36.4" }},
			lodiav1alpha1.ReasonBackupIncompatible, `Backup a-1 was taken at Kubernetes "v1.37.1", and spec.version v1.36.4 is of another minor: use the Backup's`},
		{"service ranges", restoreSetup{controlPlane: func(cp *lodiav1alpha1.ControlPlane) {
			cp.Spec.Network.ServiceCIDRs = []lodiav1alpha1.CIDR{"10.97.0.0/16"}
		}},
			lodiav1alpha1.ReasonBackupIncompatible, "spec.network.serviceCIDRs is [10.97.0.0/16], and Backup a-1 was taken of a network with [10.96.0.0/16]: use that"},
		{"pod ranges", restoreSetup{controlPlane: func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Network.PodCIDRs = []lodiav1alpha1.CIDR{"10.245.0.0/16"} }},
			lodiav1alpha1.ReasonBackupIncompatible, "spec.network.podCIDRs is [10.245.0.0/16], and Backup a-1 was taken of a network with [10.244.0.0/16]: use that"},
		{"cluster domain", restoreSetup{controlPlane: func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Network.ClusterDomain = "tenant.local" }},
			lodiav1alpha1.ReasonBackupIncompatible, "spec.network.clusterDomain is tenant.local, and Backup a-1 was taken of a network with cluster.local: use that"},
		{"a smaller quota", restoreSetup{backup: func(b *lodiav1alpha1.Backup) { b.Status.Seal.QuotaBackendBytes = new(resource.MustParse("4Gi")) }},
			lodiav1alpha1.ReasonBackupIncompatible, "Backup a-1 comes from an etcd with quota 4Gi; this ControlPlane's class gives 2Gi: use a class with at least 4Gi"},
		{"another provider", restoreSetup{class: func(c *lodiav1alpha1.DatastoreClass) { c.Spec.Backup.Store.Provider = "GCS" }},
			lodiav1alpha1.ReasonRestoreFailed, "names GCS https://s3.example.test lodia-backups/prod/"},
		{"another endpoint", restoreSetup{class: func(c *lodiav1alpha1.DatastoreClass) { c.Spec.Backup.Store.Endpoint = "https://s3.other.test" }},
			lodiav1alpha1.ReasonRestoreFailed, "names S3 https://s3.other.test lodia-backups/prod/"},
		{"another bucket", restoreSetup{class: func(c *lodiav1alpha1.DatastoreClass) { c.Spec.Backup.Store.Bucket = "other-bucket" }},
			lodiav1alpha1.ReasonRestoreFailed, "names S3 https://s3.example.test other-bucket/prod/"},
		{"another prefix", restoreSetup{class: func(c *lodiav1alpha1.DatastoreClass) { c.Spec.Backup.Store.Prefix = "staging" }},
			lodiav1alpha1.ReasonRestoreFailed, "names S3 https://s3.example.test lodia-backups/staging/"},
		{"another region in the credentials", restoreSetup{class: func(c *lodiav1alpha1.DatastoreClass) { c.Spec.Backup.Store.Region = "us-east-1" }},
			lodiav1alpha1.ReasonRestoreFailed, `Secret b-etcd-backup's region is "us-east-1", while Backup a-1 was sealed with "eu-central-1": point DatastoreClass `},
		{"no store in the class", restoreSetup{class: func(c *lodiav1alpha1.DatastoreClass) { c.Spec.Backup = nil }},
			lodiav1alpha1.ReasonRestoreFailed, "has no backup store, while Backup a-1 was sealed in S3 https://s3.example.test lodia-backups/prod/"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRestore(t, c.setup)
			waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, c.reason, c.message)
			r.nothingCreated(t)
		})
	}

	t.Run("a Backup without a quota", func(t *testing.T) {
		r := newRestore(t, restoreSetup{backup: func(b *lodiav1alpha1.Backup) { b.Status.Seal.QuotaBackendBytes = nil }})
		r.waitCopyTask(t)
	})
	t.Run("the store moved back", func(t *testing.T) {
		r := newRestore(t, restoreSetup{class: func(c *lodiav1alpha1.DatastoreClass) { c.Spec.Backup.Store.Bucket = "other-bucket" }})
		waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoreFailed, "point the class back at that store")
		editClass(t, r.ns, func(c *lodiav1alpha1.DatastoreClass) { c.Spec.Backup.Store.Bucket = "lodia-backups" })
		r.waitCopyTask(t)
	})
	t.Run("sealed later", func(t *testing.T) {
		r := newRestore(t, restoreSetup{backup: readyAs(metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying, "copying")})
		waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForBackup, "Backup a-1 is Copying: copying")
		b := getBackup(t, r.ns, backupName)
		readyAs(metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted, "sealed")(b)
		if err := k8s.Status().Update(t.Context(), b); err != nil {
			t.Fatal(err)
		}
		r.waitCopyTask(t)
	})
}

func TestABackupBeingDeletedRestoresAndGoesOnceTheRestoreIsComplete(t *testing.T) {
	backups := &datastore.BackupReconciler{Client: cached, Reader: apiReader, Clock: clock.RealClock{}}
	pass := func(t *testing.T, ns string) *lodiav1alpha1.Backup {
		t.Helper()
		if _, err := reconcileBackup(backups, ns, backupName); err != nil {
			t.Fatal(err)
		}
		return getBackup(t, ns, backupName)
	}
	r := newRestore(t, restoreSetup{before: func(r *restore) {
		b := pass(t, r.ns)
		controllerutil.AddFinalizer(b, "test.lodia.io/hold")
		if err := k8s.Update(t.Context(), b); err != nil {
			t.Fatal(err)
		}
		if err := k8s.Delete(t.Context(), b); err != nil {
			t.Fatal(err)
		}
		if b := getBackup(t, r.ns, backupName); b.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(b, lodiav1alpha1.RestoreSourceFinalizer) {
			t.Fatalf("Backup %s is not held while it is deleted: %v, %v", backupName, b.DeletionTimestamp, b.Finalizers)
		}
	}})
	cp := r.controlPlane(t)
	create(t, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: restoredCP + "-encryption-config", Namespace: r.ns, Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: restoredCP},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cp, lodiav1alpha1.GroupVersion.WithKind("ControlPlane"))},
		},
		Data: map[string][]byte{lodiav1alpha1.EncryptionConfigKey: []byte("keys")},
	})
	r.copied(t)
	marker := r.cleanedUp(t)
	if b := pass(t, r.ns); !controllerutil.ContainsFinalizer(b, lodiav1alpha1.RestoreSourceFinalizer) {
		t.Fatalf("the finalizer let Backup %s go before its restore completed", backupName)
	}
	r.complete(t, marker)
	if b := pass(t, r.ns); controllerutil.ContainsFinalizer(b, lodiav1alpha1.RestoreSourceFinalizer) {
		t.Fatalf("the finalizer keeps Backup %s after its restore completed: %v", backupName, b.Finalizers)
	}
}

func TestTheCopyTargetIsTheEtcdsPrefix(t *testing.T) {
	r := newRestore(t, restoreSetup{class: func(c *lodiav1alpha1.DatastoreClass) { c.Spec.Backup.Store.Prefix = "prod/" }})
	target := r.waitCopyTask(t).Spec.TargetStore.Prefix
	etcd := r.copied(t)
	if etcd.Spec.Backup.Store.Prefix != target {
		t.Errorf("the copy went to %s, and the Etcd restores from %s", target, etcd.Spec.Backup.Store.Prefix)
	}
}

func TestARestoreFromACopyWithoutItsSealCleansUpNothing(t *testing.T) {
	for _, c := range []struct {
		name string
		seal func(t *testing.T, r *restore)
	}{
		{"absent", func(t *testing.T, r *restore) {
			if _, err := r.etcd.Delete(t.Context(), lodiav1alpha1.BackupSealKey(r.backup.UID)); err != nil {
				t.Fatal(err)
			}
		}},
		{"of another Backup", func(t *testing.T, r *restore) {
			if _, err := r.etcd.Put(t.Context(), lodiav1alpha1.BackupSealKey(r.backup.UID), "5e1f7c3a-0000-4000-8000-0000000000ff"); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRestore(t, restoreSetup{before: func(r *restore) { c.seal(t, r) }})
			r.copied(t)
			waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoreFailed, fmt.Sprintf(
				"the restored etcd lacks /lodia.io/backups/%s, the seal of Backup a-1: the copy is not that Backup's; nothing was cleaned up and no API server starts; delete this ControlPlane and restore another Backup",
				r.backup.UID))
			poke(t, r.ns)
			consistently(t, func() error {
				if r.marker(t) != 0 || r.opsTask(t) != nil {
					return errors.New("a restore without its seal went on")
				}
				return nil
			})
			r.etcd.present(t, append(slices.Clone(cleanedKeys), keptKeys...)...)
			waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoreFailed, "the restored etcd lacks")
		})
	}
}

func TestTheCleanupFitsOneTransactionOfADeltaSnapshot(t *testing.T) {
	nodes := func(n int) func(*testing.T, *restoredEtcd) {
		return func(t *testing.T, e *restoredEtcd) {
			keys := make([]string, n)
			for i := range keys {
				keys[i] = fmt.Sprintf("/registry/minions/node-%05d", i)
			}
			e.put(t, keys...)
		}
	}
	t.Run("10240", func(t *testing.T) {
		r := newRestore(t, restoreSetup{preload: nodes(10240)})
		r.copied(t)
		waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoreFailed,
			"the cleanup would delete 10240 keys; etcd-backup-restore replays a revision as one transaction of at most 10240 operations (pkg/types/restorer.go:27)")
		if n := r.etcd.count(t, "/registry/minions/"); n != 10240 || r.marker(t) != 0 {
			t.Errorf("a refused cleanup left %d of 10240 Nodes and the marker at %d", n, r.marker(t))
		}
	})
	t.Run("10239", func(t *testing.T) {
		r := newRestore(t, restoreSetup{preload: nodes(10239)})
		r.copied(t)
		r.cleanedUp(t)
		if n := r.etcd.count(t, "/registry/minions/"); n != 0 {
			t.Errorf("the cleanup left %d Nodes", n)
		}
	})
}

func TestTheCleanupFunction(t *testing.T) {
	e := &restoredEtcd{Client: testserver.RunEtcd(t, nil).Client}
	e.put(t, append(slices.Clone(cleanedKeys), keptKeys...)...)
	uid := types.UID("5e1f7c3a-0000-4000-8000-0000000000aa")
	deleted, revision, err := datastore.Cleanup(t.Context(), e, uid, []byte("first"))
	if err != nil || deleted != int64(len(cleanedKeys)) || revision == 0 {
		t.Fatalf("the cleanup returned %d keys at revision %d, %v; want %d keys", deleted, revision, err, len(cleanedKeys))
	}
	response, err := e.Get(t.Context(), "/registry/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		t.Fatal(err)
	}
	var left []string
	for _, kv := range response.Kvs {
		left = append(left, string(kv.GetKey()))
	}
	if want := slices.Sorted(slices.Values(keptKeys)); !slices.Equal(left, want) {
		t.Errorf("the cleanup left\n%v\nwant\n%v", left, want)
	}
	e.put(t, cleanedKeys[0])
	again, at, err := datastore.Cleanup(t.Context(), e, uid, []byte("second"))
	if err != nil || at != revision {
		t.Fatalf("a second cleanup returned revision %d, %v; want the marker's %d", at, err, revision)
	}
	if again != 1 || len(e.get(t, cleanedKeys[0]).Kvs) == 0 {
		t.Errorf("a second cleanup deleted what was written after the first")
	}
	marker := e.get(t, lodiav1alpha1.RestoreMarkerKey(uid))
	if string(marker.Kvs[0].GetValue()) != "first" || marker.Kvs[0].GetModRevision() != revision {
		t.Errorf("the marker holds %q at revision %d, want the first cleanup's at %d", marker.Kvs[0].GetValue(), marker.Kvs[0].GetModRevision(), revision)
	}
}

func TestARestoreCheckThatCannotRunHoldsTheRestore(t *testing.T) {
	r := newRestore(t, restoreSetup{})
	finishCopy(t, r.ns, r.waitCopyTask(t).Name, druidv1alpha1.EtcdCopyBackupsTaskSucceeded, "")
	waitEtcd(t, r.ns, restoredCP, nil)
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "etcd-druid has not observed generation 1 of Etcd b-etcd")

	r.etcd.unreachable.Store(true)
	runStatefulSet(t, r.ns, restoredCP, 1)
	setEtcdStatus(t, r.ns, restoredCP, nil)
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "waiting for etcd of b to answer")
	if r.marker(t) != 0 {
		t.Fatal("cleaned up through an etcd that does not answer")
	}
	r.etcd.unreachable.Store(false)
	r.complete(t, r.cleanedUp(t))
}

func TestARestoreSurvivesAFailureAtEachStep(t *testing.T) {
	survives := func(t *testing.T, r *restore, marker int64) {
		t.Helper()
		r.etcd.put(t, "/registry/minions/b-node")
		r.complete(t, marker)
		poke(t, r.ns)
		consistently(t, func() error {
			if r.marker(t) != marker {
				return fmt.Errorf("the marker moved from %d to %d", marker, r.marker(t))
			}
			return nil
		})
		r.etcd.present(t, "/registry/minions/b-node")
	}
	t.Run("the copy task refused", func(t *testing.T) {
		var binding *admissionregistrationv1.ValidatingAdmissionPolicyBinding
		r := newRestore(t, restoreSetup{before: func(r *restore) {
			binding = refuse(t, r.ns, admissionregistrationv1.Rule{APIGroups: []string{druidv1alpha1.SchemeGroupVersion.Group}, APIVersions: []string{"v1alpha1"}, Resources: []string{"etcdcopybackupstasks"}}, func() client.Object {
				return &druidv1alpha1.EtcdCopyBackupsTask{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: r.ns}, Spec: druidv1alpha1.EtcdCopyBackupsTaskSpec{SourceStore: druidv1alpha1.StoreSpec{Prefix: "p"}, TargetStore: druidv1alpha1.StoreSpec{Prefix: "p"}}}
			})
		}})
		consistently(t, func() error {
			if r.copyTask(t) != nil {
				return errors.New("a refused copy task exists")
			}
			return nil
		})
		if err := k8s.Delete(t.Context(), binding); err != nil {
			t.Fatal(err)
		}
		r.copied(t)
		survives(t, r, r.cleanedUp(t))
	})
	t.Run("the Etcd refused after the copy", func(t *testing.T) {
		r := newRestore(t, restoreSetup{})
		task := r.waitCopyTask(t)
		binding := refuseEtcds(t, r.ns)
		finishCopy(t, r.ns, task.Name, druidv1alpha1.EtcdCopyBackupsTaskSucceeded, "")
		waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, "the API server refuses Etcd b-etcd")
		if err := k8s.Delete(t.Context(), binding); err != nil {
			t.Fatal(err)
		}
		r.copied(t)
		if again := r.copyTask(t); again.UID != task.UID {
			t.Errorf("the copy ran again: task %s, then %s", task.UID, again.UID)
		}
		survives(t, r, r.cleanedUp(t))
	})
	t.Run("the copy task gone once the Etcd exists", func(t *testing.T) {
		r := newRestore(t, restoreSetup{})
		r.etcd.unreachable.Store(true)
		r.copied(t)
		if err := k8s.Delete(t.Context(), r.copyTask(t)); err != nil {
			t.Fatal(err)
		}
		poke(t, r.ns)
		consistently(t, func() error {
			if r.copyTask(t) != nil {
				return errors.New("a copy task came back over an existing Etcd")
			}
			return nil
		})
		r.etcd.unreachable.Store(false)
		poke(t, r.ns)
		survives(t, r, r.cleanedUp(t))
	})
	for _, c := range []struct {
		name    string
		refused func(*restoredEtcd) *atomic.Int32
	}{
		{"the count refused", func(e *restoredEtcd) *atomic.Int32 { return &e.refuseCounts }},
		{"the cleanup refused", func(e *restoredEtcd) *atomic.Int32 { return &e.refuseCleanups }},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRestore(t, restoreSetup{})
			c.refused(r.etcd).Store(math.MaxInt32)
			r.copied(t)
			waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "waiting for etcd of b to answer")
			if r.marker(t) != 0 {
				t.Fatal("a refused cleanup reached the etcd")
			}
			r.etcd.present(t, cleanedKeys...)
			c.refused(r.etcd).Store(0)
			poke(t, r.ns)
			marker := r.cleanedUp(t)
			r.etcd.absent(t, cleanedKeys...)
			survives(t, r, marker)
		})
	}
	t.Run("the cleanup's answer lost", func(t *testing.T) {
		r := newRestore(t, restoreSetup{})
		r.etcd.loseCleanups.Store(1)
		r.copied(t)
		eventually(t, func() error {
			if r.marker(t) == 0 {
				return errors.New("not cleaned up yet")
			}
			return nil
		})
		r.etcd.put(t, "/registry/minions/written-after-the-cleanup")
		poke(t, r.ns)
		marker := r.cleanedUp(t)
		r.etcd.present(t, "/registry/minions/written-after-the-cleanup")
		survives(t, r, marker)
	})
	t.Run("the EtcdOpsTask refused", func(t *testing.T) {
		r := newRestore(t, restoreSetup{})
		binding := refuse(t, r.ns, admissionregistrationv1.Rule{APIGroups: []string{druidv1alpha1.SchemeGroupVersion.Group}, APIVersions: []string{"v1alpha1"}, Resources: []string{"etcdopstasks"}}, func() client.Object {
			return &druidv1alpha1.EtcdOpsTask{ObjectMeta: metav1.ObjectMeta{Name: "probe", Namespace: r.ns}, Spec: druidv1alpha1.EtcdOpsTaskSpec{
				Config: druidv1alpha1.EtcdOpsTaskConfig{OnDemandSnapshot: &druidv1alpha1.OnDemandSnapshotConfig{Type: druidv1alpha1.OnDemandSnapshotTypeFull}}, EtcdName: new("b-etcd"),
			}}
		})
		r.copied(t)
		eventually(t, func() error {
			if r.marker(t) == 0 {
				return errors.New("not cleaned up yet")
			}
			return nil
		})
		marker := r.marker(t)
		consistently(t, func() error {
			if r.opsTask(t) != nil {
				return errors.New("a refused EtcdOpsTask exists")
			}
			return nil
		})
		if err := k8s.Delete(t.Context(), binding); err != nil {
			t.Fatal(err)
		}
		poke(t, r.ns)
		if got := r.cleanedUp(t); got != marker {
			t.Fatalf("the marker moved from %d to %d", marker, got)
		}
		ops := r.opsTask(t)
		poke(t, r.ns)
		consistently(t, func() error {
			if again := r.opsTask(t); again == nil || again.UID != ops.UID {
				return errors.New("the EtcdOpsTask was not found by its name")
			}
			return nil
		})
		survives(t, r, marker)
	})
	t.Run("the scale-up refused", func(t *testing.T) {
		r := newRestore(t, restoreSetup{controlPlane: func(cp *lodiav1alpha1.ControlPlane) {
			cp.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
		}})
		r.copied(t)
		marker := r.cleanedUp(t)
		binding := refuseEtcds(t, r.ns)
		createLease(t, r.ns, restoredCP+"-etcd-full-snap", strconv.FormatInt(marker, 10))
		poke(t, r.ns)
		waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, "the API server refuses Etcd b-etcd")
		eventually(t, func() error {
			if cp := r.controlPlane(t); cp.Status.Datastore == nil || cp.Status.Datastore.Restore == nil {
				return errors.New("a complete restore whose scale-up is refused has no status")
			}
			return nil
		})
		if err := k8s.Delete(t.Context(), binding); err != nil {
			t.Fatal(err)
		}
		poke(t, r.ns)
		waitEtcd(t, r.ns, restoredCP, func(etcd *druidv1alpha1.Etcd) error {
			if etcd.Spec.Replicas != 3 {
				return fmt.Errorf("%d members", etcd.Spec.Replicas)
			}
			return nil
		})
	})
}

func TestARestoreStatusRemovedComesBackFromTheMarker(t *testing.T) {
	r := newRestore(t, restoreSetup{})
	r.copied(t)
	marker := r.cleanedUp(t)
	r.complete(t, marker)
	r.etcd.put(t, "/registry/minions/b-node")
	remove := []byte(`[{"op":"remove","path":"/status/datastore/restore"}]`)
	if err := k8s.Status().Patch(t.Context(), r.controlPlane(t), client.RawPatch(types.JSONPatchType, remove), client.FieldOwner("someone-else")); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		cp := r.controlPlane(t)
		if cp.Status.Datastore == nil || cp.Status.Datastore.Restore == nil {
			return errors.New("status.datastore.restore is not back")
		}
		if got := cp.Status.Datastore.Restore; got.Backup.UID != r.backup.UID || got.Source != r.source {
			return fmt.Errorf("status.datastore.restore came back as %+v", got)
		}
		return nil
	})
	if got := r.marker(t); got != marker {
		t.Errorf("the marker moved from %d to %d", marker, got)
	}
	r.etcd.present(t, "/registry/minions/b-node")
}

func TestALeftoverFullSnapshotLeaseCompletesNoRestore(t *testing.T) {
	r := newRestore(t, restoreSetup{before: func(r *restore) {
		createLease(t, r.ns, restoredCP+"-etcd-full-snap", strconv.FormatInt(math.MaxInt64, 10))
		time.Sleep(1100 * time.Millisecond)
	}})
	r.copied(t)
	r.cleanedUp(t)
	poke(t, r.ns)
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "the store holds 0; EtcdOpsTask")
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "ignored Lease b-etcd-full-snap, created before Etcd b-etcd")
}

func caBundle(t *testing.T, ns, cp string, notBefore time.Time) *corev1.Secret {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "cluster-ca@" + ns},
		NotBefore: notBefore, NotAfter: notBefore.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: cp + "-ca-bundle", Namespace: ns, Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: cp}},
		Data:       map[string][]byte{lodiav1alpha1.CACertKey: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})},
	}
}

func TestACAOlderThanItsControlPlaneStartsNoEmptyEtcd(t *testing.T) {
	adopted := func(t *testing.T, issuedBefore time.Duration) (string, *corev1.Secret) {
		t.Helper()
		ns := namespace(t)
		createClass(t, newClass(ns, "Retain"))
		create(t, controlPlane(ns, "old", ns))
		cp := getControlPlane(t, ns, "old")
		bundle := caBundle(t, ns, "old", cp.CreationTimestamp.Add(-certs.Backdate-issuedBefore))
		create(t, bundle)
		return ns, bundle
	}
	t.Run("issued before", func(t *testing.T) {
		ns, bundle := adopted(t, time.Second)
		cp := getControlPlane(t, ns, "old")
		writeGeneration(t, ns, "old", 1)
		waitCondition(t, ns, "old", metav1.ConditionFalse, lodiav1alpha1.ReasonCAPredatesControlPlane, fmt.Sprintf(
			"the cluster CA CN=cluster-ca@%s in old-ca-bundle was issued at %s, before this ControlPlane was created at %s: its Secrets came back and its data did not, and nodes that trust it would join an empty cluster. Restore the management cluster from a backup that keeps UIDs, or delete old-ca-bundle and old-cakey-cluster for a new identity",
			ns, cp.CreationTimestamp.Add(-time.Second).UTC().Format(time.RFC3339), cp.CreationTimestamp.UTC().Format(time.RFC3339)))
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "old-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
			t.Fatalf("want no Etcd under an older CA, got %v", err)
		}
		if err := k8s.Delete(t.Context(), bundle); err != nil {
			t.Fatal(err)
		}
		waitEtcd(t, ns, "old", nil)
	})
	t.Run("issued in the same second", func(t *testing.T) {
		ns, _ := adopted(t, 0)
		writeGeneration(t, ns, "old", 1)
		waitEtcd(t, ns, "old", nil)
	})
	t.Run("over the ControlPlane's claims", func(t *testing.T) {
		ns, _ := adopted(t, time.Second)
		cp := getControlPlane(t, ns, "old")
		create(t, claim(ns, "old-etcd-old-etcd-0", map[string]string{lodiav1alpha1.ControlPlaneUIDLabel: string(cp.UID)}))
		writeGeneration(t, ns, "old", 1)
		waitEtcd(t, ns, "old", nil)
	})
	t.Run("restoreFrom", func(t *testing.T) {
		r := newRestore(t, restoreSetup{before: func(r *restore) {
			create(t, caBundle(t, r.ns, restoredCP, time.Now().Add(-time.Hour)))
		}})
		r.waitCopyTask(t)
	})
}
