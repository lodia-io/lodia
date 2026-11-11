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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	clientv3 "go.etcd.io/etcd/client/v3"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/testenv"
)

type restoredEtcd struct {
	*clientv3.Client
	unreachable                                atomic.Bool
	refuseCounts, refuseCleanups, loseCleanups atomic.Int32
	refusedGet                                 atomic.Value
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

func (kv faultyKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if refused, _ := kv.e.refusedGet.Load().(string); refused == key {
		return nil, errUnanswered
	}
	return kv.KV.Get(ctx, key, opts...)
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
	quota        int64
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
	if setup.quota == 0 {
		r.etcd = &restoredEtcd{Client: testserver.RunEtcd(t, nil).Client}
	} else {
		config := testserver.NewTestConfig(t)
		config.QuotaBackendBytes = setup.quota
		r.etcd = &restoredEtcd{Client: testserver.RunEtcd(t, config).Client}
	}
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
	testenv.Eventually(t, func() error {
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
	if cp.Status.Restore == nil {
		t.Fatal("DatastoreReady is True without status.restore")
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
	t.Parallel()
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
	testenv.Eventually(t, func() error {
		return cached.Get(t.Context(), client.ObjectKeyFromObject(task), &druidv1alpha1.EtcdCopyBackupsTask{})
	})
	poke(t, r.ns)
	consistently(t, func() error {
		err := k8s.Get(t.Context(), client.ObjectKey{Namespace: r.ns, Name: restoredCP + "-etcd"}, &druidv1alpha1.Etcd{})
		if err == nil {
			return errors.New("an Etcd exists while the copy runs")
		}
		return client.IgnoreNotFound(err)
	})

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		task := r.copyTask(t)
		controllerutil.AddFinalizer(task, "druid.gardener.cloud/etcd-druid")
		return k8s.Update(t.Context(), task)
	}); err != nil {
		t.Fatal(err)
	}
	failCopy(t, r.ns, name, "the bucket is gone", time.Now().Add(-time.Minute))
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
	testenv.Eventually(t, func() error {
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
	finishFlush(t, r.ns, name, druidv1alpha1.TaskStateSucceeded, etcd.CreationTimestamp.Time)
	poke(t, r.ns)
	testenv.Eventually(t, func() error {
		if again := r.opsTask(t); again == nil || again.UID == ops.UID {
			return errors.New("the finished EtcdOpsTask is not replaced yet")
		}
		return nil
	})
	if got := getEtcd(t, r.ns, restoredCP); got.Spec.Replicas != 1 {
		t.Fatalf("the Etcd asks for %d members before the restore is complete", got.Spec.Replicas)
	}

	restored := r.complete(t, marker)
	if got := restored.Status.Restore; got.Backup != (lodiav1alpha1.UIDReference{Name: backupName, UID: r.backup.UID}) || got.Source != r.source || got.CompletedAt.IsZero() {
		t.Errorf("status.restore is %+v", got)
	}
	waitEtcd(t, r.ns, restoredCP, func(etcd *druidv1alpha1.Etcd) error {
		if etcd.Spec.Replicas != 3 {
			return fmt.Errorf("the Etcd asks for %d members once the restore is complete", etcd.Spec.Replicas)
		}
		return nil
	})
	testenv.Eventually(t, func() error {
		if r.copyTask(t) != nil || r.opsTask(t) != nil {
			return errors.New("the restore's tasks are still there")
		}
		return nil
	})
	completedAt := restored.Status.Restore.CompletedAt
	time.Sleep(time.Until(completedAt.Add(1100 * time.Millisecond)))
	poke(t, r.ns)
	consistently(t, func() error {
		cp := r.controlPlane(t)
		if cp.Status.Restore == nil || !cp.Status.Restore.CompletedAt.Equal(&completedAt) {
			return fmt.Errorf("status.restore is %+v, want the restore completed at %v", cp.Status.Restore, completedAt)
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

func failCopy(t *testing.T, ns, name, message string, at time.Time) {
	t.Helper()
	task := &druidv1alpha1.EtcdCopyBackupsTask{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, task); err != nil {
		t.Fatal(err)
	}
	task.Status.Conditions = []druidv1alpha1.Condition{{
		Type: druidv1alpha1.EtcdCopyBackupsTaskFailed, Status: druidv1alpha1.ConditionTrue, Reason: "JobFailed", Message: message,
		LastTransitionTime: metav1.NewTime(at), LastUpdateTime: metav1.NewTime(at),
	}}
	if err := k8s.Status().Update(t.Context(), task); err != nil {
		t.Fatal(err)
	}
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
	t.Parallel()
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
	t.Run("another patch of the minor", func(t *testing.T) {
		r := newRestore(t, restoreSetup{controlPlane: func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Version = "v1.37.2" }})
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
		consistently(t, func() error {
			if r.copyTask(t) != nil {
				return errors.New("a copy of a Backup not sealed yet")
			}
			return nil
		})
		b := getBackup(t, r.ns, backupName)
		readyAs(metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted, "sealed")(b)
		if err := k8s.Status().Update(t.Context(), b); err != nil {
			t.Fatal(err)
		}
		r.waitCopyTask(t)
	})
}

func TestABackupBeingDeletedRestoresAndGoesOnceTheRestoreIsComplete(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	r := newRestore(t, restoreSetup{class: func(c *lodiav1alpha1.DatastoreClass) { c.Spec.Backup.Store.Prefix = "prod/" }})
	target := r.waitCopyTask(t).Spec.TargetStore.Prefix
	etcd := r.copied(t)
	if etcd.Spec.Backup.Store.Prefix != target {
		t.Errorf("the copy went to %s, and the Etcd restores from %s", target, etcd.Spec.Backup.Store.Prefix)
	}
}

func TestARestoreFromACopyWithoutItsSealCleansUpNothing(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

func TestARestoreCheckThatCannotRunKeepsTheStatusTheCacheLacks(t *testing.T) {
	t.Parallel()
	r := newRestore(t, restoreSetup{})
	r.copied(t)
	marker := r.cleanedUp(t)
	before := r.controlPlane(t)
	completed := r.complete(t, marker).Status.Restore
	stale := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				before.DeepCopyInto(cp)
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	reconciler := &datastore.Reconciler{Client: stale, Reader: apiReader, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace, Revision: revision, EtcdDruidDeployment: druidDeployment, EtcdDruidVersion: druidVersion, EtcdClient: dialRestored, Clock: clock.RealClock{}}
	for _, c := range []struct {
		name            string
		check           func()
		status          metav1.ConditionStatus
		reason, message string
	}{
		{"the etcd does not answer", func() { r.etcd.unreachable.Store(true) }, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "Etcd b-etcd has quorum, 1 of 1 members ready"},
		{"no member ready", func() {
			setEtcdStatus(t, r.ns, restoredCP, func(s *druidv1alpha1.EtcdStatus) { s.Ready, s.ReadyReplicas = new(false), 0 })
			waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost, "")
		}, metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost, "Etcd b-etcd has 0 of 1 members ready"},
	} {
		t.Run(c.name, func(t *testing.T) {
			c.check()
			if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: r.ns, Name: restoredCP}}); err != nil {
				t.Fatal(err)
			}
			cp := r.controlPlane(t)
			if !reflect.DeepEqual(cp.Status.Restore, completed) {
				t.Errorf("a pass from a cache without status.restore left %+v, want %+v", cp.Status.Restore, completed)
			}
			ready := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
			if ready.Status != c.status || ready.Reason != c.reason || ready.Message != c.message {
				t.Errorf("a pass from a cache without status.restore left DatastoreReady %s/%s %q, want %s/%s %q", ready.Status, ready.Reason, ready.Message, c.status, c.reason, c.message)
			}
		})
	}
	t.Run("the live read fails", func(t *testing.T) {
		injected := errors.New("injected: the API server did not answer")
		failing := interceptor.NewClient(k8s, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
					return injected
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
		reconciler := &datastore.Reconciler{Client: noControlPlaneWrite(t, stale), Reader: failing, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace, Revision: revision, EtcdDruidDeployment: druidDeployment, EtcdDruidVersion: druidVersion, EtcdClient: dialRestored, Clock: clock.RealClock{}}
		if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: r.ns, Name: restoredCP}}); !errors.Is(err, injected) {
			t.Fatalf("the pass returned %v, want %v", err, injected)
		}
	})
}

func TestAFullSnapshotThatEndsShortOfTheMarkerIsSentAgain(t *testing.T) {
	t.Parallel()
	r := newRestore(t, restoreSetup{})
	r.waitCopyTask(t)
	time.Sleep(1100 * time.Millisecond)
	r.etcd.unreachable.Store(true)
	created := r.copied(t).CreationTimestamp.Time
	readyAt := created.Add(time.Second)
	time.Sleep(time.Until(created.Add(2 * time.Second)))
	quorate := func(at time.Time) druidv1alpha1.Condition {
		return druidv1alpha1.Condition{Status: druidv1alpha1.ConditionTrue, Reason: "Quorate", Message: "The majority of ETCD members is ready", LastTransitionTime: metav1.NewTime(at)}
	}
	lost := func(at time.Time) druidv1alpha1.Condition {
		return druidv1alpha1.Condition{Status: druidv1alpha1.ConditionFalse, Reason: "QuorumLost", Message: "The majority of ETCD members is not ready", LastTransitionTime: metav1.NewTime(at)}
	}
	now := clocktesting.NewFakePassiveClock(time.Now())
	setReady := func(condition druidv1alpha1.Condition) {
		t.Helper()
		condition.Type, condition.LastUpdateTime = druidv1alpha1.ConditionTypeReady, metav1.NewTime(now.Now()).Rfc3339Copy()
		setEtcdStatus(t, r.ns, restoredCP, func(s *druidv1alpha1.EtcdStatus) { s.Conditions = append(s.Conditions, condition) })
		poke(t, r.ns)
		testenv.Eventually(t, func() error {
			if ready := managerReads.readyOf(client.ObjectKey{Namespace: r.ns, Name: restoredCP + "-etcd"}); ready == nil || !equality.Semantic.DeepEqual(*ready, condition) {
				return fmt.Errorf("the suite's manager has read Ready %+v, want %+v", ready, condition)
			}
			return nil
		})
	}
	setReady(quorate(readyAt))
	r.etcd.unreachable.Store(false)
	poke(t, r.ns)
	marker := r.cleanedUp(t)
	reconciler := &datastore.Reconciler{Client: cached, Reader: apiReader, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace, Revision: revision, EtcdDruidDeployment: druidDeployment, EtcdDruidVersion: druidVersion, EtcdClient: dialRestored, Clock: now}
	pass := func() (time.Duration, error) {
		result, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: r.ns, Name: restoredCP}})
		return result.RequeueAfter, err
	}
	refused := errors.New("injected: the API server refused the delete")
	refusing := &datastore.Reconciler{Client: interceptor.NewClient(k8s, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*druidv1alpha1.EtcdOpsTask); ok {
				return refused
			}
			return c.Delete(ctx, obj, opts...)
		},
	}), Reader: apiReader, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace, Revision: revision, EtcdDruidDeployment: druidDeployment, EtcdDruidVersion: druidVersion, EtcdClient: dialRestored, Clock: now}
	ops := r.opsTask(t)
	if !ops.CreationTimestamp.After(readyAt) {
		t.Fatalf("the first EtcdOpsTask was created at %s, not after the Etcd was ready at %s", ops.CreationTimestamp.UTC(), readyAt.UTC())
	}
	for _, round := range []struct {
		state              druidv1alpha1.TaskState
		ready              druidv1alpha1.Condition
		after              []druidv1alpha1.Condition
		started, start     time.Time
		finished, interval time.Duration
	}{
		{druidv1alpha1.TaskStateSucceeded, quorate(readyAt), nil, time.Time{}, readyAt, 10 * time.Minute, 10 * time.Minute},
		{druidv1alpha1.TaskStateSucceeded, quorate(readyAt), nil, time.Time{}, readyAt, 3 * time.Hour, time.Hour},
		{druidv1alpha1.TaskStateSucceeded, lost(readyAt.Add(5 * time.Minute)), nil, time.Time{}, created, 20 * time.Minute, 20 * time.Minute},
		{druidv1alpha1.TaskStateFailed, lost(created.Add(51 * time.Minute)), []druidv1alpha1.Condition{quorate(created.Add(52 * time.Minute))}, created.Add(time.Minute), created, 50 * time.Minute, 50 * time.Minute},
		{druidv1alpha1.TaskStateFailed, quorate(created.Add(90 * time.Minute)), nil, created.Add(90*time.Minute - time.Second), created, 2 * time.Hour, time.Hour},
		{druidv1alpha1.TaskStateRejected, lost(readyAt), nil, time.Time{}, created, 30 * time.Minute, 30 * time.Minute},
		{druidv1alpha1.TaskStateFailed, quorate(created.Add(4 * time.Hour)), nil, created.Add(4*time.Hour + time.Second), created.Add(4 * time.Hour), 10 * time.Minute, 10 * time.Minute},
		{druidv1alpha1.TaskStateFailed, quorate(created.Add(5 * time.Hour)), nil, created.Add(5 * time.Hour), created.Add(5 * time.Hour), 20 * time.Minute, 20 * time.Minute},
	} {
		finished := round.start.Add(round.finished)
		next := finished.Add(round.interval)
		now.SetTime(next.Add(-time.Second))
		want := fmt.Sprintf("cleaned up; waiting for a full snapshot of revision %d: the store holds 0; EtcdOpsTask %s is %s at %s; the next goes at %s",
			marker, ops.Name, round.state, finished.UTC().Format(time.RFC3339), next.UTC().Format(time.RFC3339))
		settled := func() {
			t.Helper()
			testenv.Eventually(t, func() error {
				if _, err := pass(); err != nil {
					return err
				}
				if ready := meta.FindStatusCondition(r.controlPlane(t).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); ready.Reason != lodiav1alpha1.ReasonRestoring || ready.Message != want {
					return fmt.Errorf("DatastoreReady is %s %q, want Restoring %q", ready.Reason, ready.Message, want)
				}
				return nil
			})
		}
		setReady(round.ready)
		if !round.started.IsZero() {
			ops.Status.State, ops.Status.StartedAt = new(druidv1alpha1.TaskStateInProgress), new(metav1.NewTime(round.started))
			if err := k8s.Status().Update(t.Context(), ops); err != nil {
				t.Fatal(err)
			}
		}
		finishFlush(t, r.ns, ops.Name, round.state, finished)
		settled()
		for _, condition := range round.after {
			setReady(condition)
			settled()
		}
		for range 3 {
			requeueAfter, err := pass()
			if err != nil || requeueAfter != time.Second {
				t.Fatalf("a pass a second before the next full snapshot returned %v, %v; want a requeue in 1s", requeueAfter, err)
			}
			if again := r.opsTask(t); again == nil || again.UID != ops.UID {
				t.Fatalf("the EtcdOpsTask finished at %s was replaced before %s", finished, next)
			}
		}
		now.SetTime(next)
		if _, err := refusing.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: r.ns, Name: restoredCP}}); !errors.Is(err, refused) {
			t.Fatalf("a pass whose delete of the finished EtcdOpsTask was refused returned %v, want %v", err, refused)
		}
		if again := r.opsTask(t); again == nil || again.UID != ops.UID || !again.DeletionTimestamp.IsZero() {
			t.Fatal("the finished EtcdOpsTask went though its delete was refused")
		}
		testenv.Eventually(t, func() error {
			if _, err := pass(); err != nil {
				return err
			}
			if again := r.opsTask(t); again == nil || again.UID == ops.UID {
				return fmt.Errorf("no EtcdOpsTask at %s after the one finished at %s", next, finished)
			}
			return nil
		})
		ops = r.opsTask(t)
	}
	r.complete(t, marker)
}

func TestAFullSnapshotRejectedBeforeTheEtcdIsReadyGoesOnceItIs(t *testing.T) {
	t.Parallel()
	r := newRestore(t, restoreSetup{})
	created := r.copied(t).CreationTimestamp.Time
	marker := r.cleanedUp(t)
	now := clocktesting.NewFakePassiveClock(time.Now())
	reconciler := &datastore.Reconciler{Client: cached, Reader: apiReader, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace, Revision: revision, EtcdDruidDeployment: druidDeployment, EtcdDruidVersion: druidVersion, EtcdClient: dialRestored, Clock: now}
	pass := func() error {
		_, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: r.ns, Name: restoredCP}})
		return err
	}
	ops := r.opsTask(t)
	rejected := created.Add(20 * time.Minute)
	finishFlush(t, r.ns, ops.Name, druidv1alpha1.TaskStateRejected, rejected)
	now.SetTime(rejected.Add(5 * time.Second))
	want := fmt.Sprintf("cleaned up; waiting for a full snapshot of revision %d: the store holds 0; EtcdOpsTask %s is Rejected at %s; the next goes at %s",
		marker, ops.Name, rejected.UTC().Format(time.RFC3339), rejected.Add(20*time.Minute).UTC().Format(time.RFC3339))
	testenv.Eventually(t, func() error {
		if err := pass(); err != nil {
			return err
		}
		if ready := meta.FindStatusCondition(r.controlPlane(t).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); ready.Reason != lodiav1alpha1.ReasonRestoring || ready.Message != want {
			return fmt.Errorf("DatastoreReady is %s %q, want Restoring %q", ready.Reason, ready.Message, want)
		}
		return nil
	})
	now.SetTime(rejected.Add(40 * time.Second))
	setEtcdStatus(t, r.ns, restoredCP, func(s *druidv1alpha1.EtcdStatus) {
		s.Conditions = append(s.Conditions, druidv1alpha1.Condition{
			Type: druidv1alpha1.ConditionTypeReady, Status: druidv1alpha1.ConditionTrue, Reason: "Quorate", Message: "The majority of ETCD members is ready",
			LastTransitionTime: metav1.NewTime(rejected.Add(30 * time.Second)), LastUpdateTime: metav1.NewTime(now.Now()),
		})
	})
	testenv.Eventually(t, func() error {
		if err := pass(); err != nil {
			return err
		}
		if again := r.opsTask(t); again == nil || again.UID == ops.UID {
			return fmt.Errorf("the EtcdOpsTask rejected at %s is not sent again once Ready turned True at %s", rejected.UTC(), rejected.Add(30*time.Second).UTC())
		}
		return nil
	})
}

func TestALeaseThatReachesTheMarkerWhileTheNextFullSnapshotWaitsCompletesTheRestore(t *testing.T) {
	t.Parallel()
	r := newRestore(t, restoreSetup{})
	created := r.copied(t).CreationTimestamp.Time
	marker := r.cleanedUp(t)
	lease := restoredCP + "-etcd-full-snap"
	createLease(t, r.ns, lease, strconv.FormatInt(marker-1, 10))
	finished := created.Add(30 * time.Minute)
	finishFlush(t, r.ns, r.taskName(t), druidv1alpha1.TaskStateSucceeded, finished)
	settled := fmt.Sprintf("the store holds %d; EtcdOpsTask %s is Succeeded at %s; the next goes at %s",
		marker-1, r.taskName(t), finished.UTC().Format(time.RFC3339), finished.Add(30*time.Minute).UTC().Format(time.RFC3339))
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, settled)
	consistently(t, func() error {
		if ready := meta.FindStatusCondition(r.controlPlane(t).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); !strings.Contains(ready.Message, settled) {
			return fmt.Errorf("DatastoreReady moved to %s %q", ready.Reason, ready.Message)
		}
		return nil
	})
	setLease(t, r.ns, lease, marker)
	waitCondition(t, r.ns, restoredCP, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum")
}

func TestARestoreSurvivesAFailureAtEachStep(t *testing.T) {
	t.Parallel()
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
		testenv.Eventually(t, func() error {
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
		testenv.Eventually(t, func() error {
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
		testenv.Eventually(t, func() error {
			if cp := r.controlPlane(t); cp.Status.Restore == nil {
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
	t.Parallel()
	r := newRestore(t, restoreSetup{})
	r.copied(t)
	marker := r.cleanedUp(t)
	r.complete(t, marker)
	r.etcd.put(t, "/registry/minions/b-node")
	remove := []byte(`[{"op":"remove","path":"/status/restore"}]`)
	if err := k8s.Status().Patch(t.Context(), r.controlPlane(t), client.RawPatch(types.JSONPatchType, remove), client.FieldOwner("someone-else")); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		cp := r.controlPlane(t)
		if cp.Status.Restore == nil {
			return errors.New("status.restore is not back")
		}
		if got := cp.Status.Restore; got.Backup.UID != r.backup.UID || got.Source != r.source {
			return fmt.Errorf("status.restore came back as %+v", got)
		}
		return nil
	})
	if got := r.marker(t); got != marker {
		t.Errorf("the marker moved from %d to %d", marker, got)
	}
	r.etcd.present(t, "/registry/minions/b-node")
}

func TestDataFromBeforeTheCleanupIsCleanedUpAgain(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name string
		gone bool
	}{{"the Backup kept", false}, {"the Backup gone", true}} {
		t.Run(c.name, func(t *testing.T) {
			r := newRestore(t, restoreSetup{})
			r.copied(t)
			first := r.cleanedUp(t)
			r.complete(t, first)
			if c.gone {
				if err := k8s.Delete(t.Context(), r.backup); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := r.etcd.Delete(t.Context(), lodiav1alpha1.RestoreMarkerKey(r.controlPlane(t).UID)); err != nil {
				t.Fatal(err)
			}
			r.etcd.put(t, cleanedKeys[0])
			poke(t, r.ns)
			waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "cleaned up; waiting for a full snapshot of revision")
			r.etcd.absent(t, cleanedKeys[0])
			second := r.marker(t)
			if second <= first {
				t.Fatalf("the marker came back at revision %d, the first at %d", second, first)
			}
			if value := restoreMarkerValue(t, r); value != (restoreMarkerJSON{Backup: lodiav1alpha1.UIDReference{Name: backupName, UID: r.backup.UID}, Source: r.source}) {
				t.Errorf("the marker holds %+v", value)
			}
			r.complete(t, second)
		})
	}
}

func TestALeftoverFullSnapshotLeaseCompletesNoRestore(t *testing.T) {
	t.Parallel()
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

func TestACompletedRestoreNeedsItsBackupNoMore(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name string
		gone bool
	}{{"the Backup gone", true}, {"the Backup kept", false}} {
		t.Run(c.name, func(t *testing.T) {
			r := newRestore(t, restoreSetup{controlPlane: func(cp *lodiav1alpha1.ControlPlane) {
				cp.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
			}})
			r.copied(t)
			marker := r.cleanedUp(t)
			completed := r.complete(t, marker).Status.Restore
			if c.gone {
				if err := k8s.Delete(t.Context(), r.backup); err != nil {
					t.Fatal(err)
				}
			}
			first := getEtcd(t, r.ns, restoredCP)
			time.Sleep(1100 * time.Millisecond)
			if err := k8s.Delete(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			kept := func() error {
				cp := r.controlPlane(t)
				if !reflect.DeepEqual(cp.Status.Restore, completed) {
					return fmt.Errorf("status.restore is %+v, want the restore %+v", cp.Status.Restore, completed)
				}
				if task := r.copyTask(t); task != nil {
					return fmt.Errorf("EtcdCopyBackupsTask %s copies the Backup again", task.Name)
				}
				return nil
			}
			etcd := waitEtcd(t, r.ns, restoredCP, func(etcd *druidv1alpha1.Etcd) error {
				if etcd.UID == first.UID {
					return errors.New("the deleted Etcd is still there")
				}
				return nil
			})
			if etcd.Spec.Replicas != 1 || !reflect.DeepEqual(etcd.Spec.Backup.Store, first.Spec.Backup.Store) {
				t.Errorf("the Etcd created again asks for %d members with store %+v, want 1 with %+v", etcd.Spec.Replicas, etcd.Spec.Backup.Store, first.Spec.Backup.Store)
			}
			consistently(t, kept)
			setEtcdStatus(t, r.ns, restoredCP, nil)
			waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "ignored Lease b-etcd-full-snap, created before Etcd b-etcd")
			if err := kept(); err != nil {
				t.Fatal(err)
			}
			if err := k8s.Delete(t.Context(), &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: r.ns, Name: restoredCP + "-etcd-full-snap"}}); err != nil {
				t.Fatal(err)
			}
			if again := r.complete(t, marker).Status.Restore; !reflect.DeepEqual(again, completed) {
				t.Errorf("the restore completed again as %+v, want %+v", again, completed)
			}
			if got := r.marker(t); got != marker {
				t.Errorf("the marker moved from revision %d to %d", marker, got)
			}
		})
	}
}

func TestACompletedRestoreKeepsItsRecordWithoutAnEtcd(t *testing.T) {
	t.Parallel()
	completed := func(t *testing.T) (*restore, *lodiav1alpha1.RestoreStatus) {
		t.Helper()
		r := newRestore(t, restoreSetup{})
		r.copied(t)
		return r, r.complete(t, r.cleanedUp(t)).Status.Restore
	}
	kept := func(t *testing.T, r *restore, want *lodiav1alpha1.RestoreStatus, reason string) func() error {
		return func() error {
			cp := r.controlPlane(t)
			if !reflect.DeepEqual(cp.Status.Restore, want) {
				return fmt.Errorf("status.restore is %+v, want %+v", cp.Status.Restore, want)
			}
			if ready := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); ready.Reason != reason {
				return fmt.Errorf("DatastoreReady is %s/%s %q, want %s", ready.Status, ready.Reason, ready.Message, reason)
			}
			return nil
		}
	}
	hold := func(t *testing.T, obj client.Object) {
		t.Helper()
		patch := client.MergeFrom(obj.DeepCopyObject().(client.Object))
		controllerutil.AddFinalizer(obj, "test.lodia.io/hold")
		if err := k8s.Patch(t.Context(), obj, patch); err != nil {
			t.Fatal(err)
		}
	}
	release := func(t *testing.T, obj client.Object) {
		t.Helper()
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(obj), obj); err != nil {
				return client.IgnoreNotFound(err)
			}
			controllerutil.RemoveFinalizer(obj, "test.lodia.io/hold")
			return k8s.Update(t.Context(), obj)
		}); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("the Etcd refused", func(t *testing.T) {
		r, record := completed(t)
		if err := k8s.Delete(t.Context(), r.backup); err != nil {
			t.Fatal(err)
		}
		binding := refuseEtcds(t, r.ns)
		first := getEtcd(t, r.ns, restoredCP)
		if err := k8s.Delete(t.Context(), first); err != nil {
			t.Fatal(err)
		}
		waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, "the API server refuses Etcd b-etcd")
		poke(t, r.ns)
		consistently(t, kept(t, r, record, lodiav1alpha1.ReasonRefused))
		if err := k8s.Delete(t.Context(), binding); err != nil {
			t.Fatal(err)
		}
		poke(t, r.ns)
		etcd := waitEtcd(t, r.ns, restoredCP, func(etcd *druidv1alpha1.Etcd) error {
			if etcd.UID == first.UID {
				return errors.New("the deleted Etcd is still there")
			}
			return nil
		})
		if etcd.Spec.Replicas != 1 || !reflect.DeepEqual(etcd.Spec.Backup.Store, first.Spec.Backup.Store) {
			t.Errorf("the Etcd created again asks for %d members with store %+v, want 1 with %+v", etcd.Spec.Replicas, etcd.Spec.Backup.Store, first.Spec.Backup.Store)
		}
		waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, "")
		consistently(t, func() error {
			if task := r.copyTask(t); task != nil {
				return fmt.Errorf("EtcdCopyBackupsTask %s copies the Backup again", task.Name)
			}
			return kept(t, r, record, lodiav1alpha1.ReasonProvisioning)()
		})
	})

	t.Run("a foreign Etcd", func(t *testing.T) {
		r, record := completed(t)
		pause(t, r.ns, restoredCP)
		if err := k8s.Delete(t.Context(), getEtcd(t, r.ns, restoredCP)); err != nil {
			t.Fatal(err)
		}
		testenv.Eventually(t, func() error {
			return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: r.ns, Name: restoredCP + "-etcd"}, &druidv1alpha1.Etcd{}))
		})
		create(t, &druidv1alpha1.Etcd{
			ObjectMeta: metav1.ObjectMeta{Name: restoredCP + "-etcd", Namespace: r.ns},
			Spec:       druidv1alpha1.EtcdSpec{Replicas: 1, Labels: map[string]string{"app": "etcd"}},
		})
		setPaused(t, r.ns, restoredCP, false)
		waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonVolumesExist, "Etcd b-etcd is not this ControlPlane's, it has no controller")
		poke(t, r.ns)
		consistently(t, kept(t, r, record, lodiav1alpha1.ReasonVolumesExist))
	})

	t.Run("the deletion", func(t *testing.T) {
		r, record := completed(t)
		done := r.controlPlane(t)
		etcd := getEtcd(t, r.ns, restoredCP)
		hold(t, etcd)
		hold(t, done)
		if err := k8s.Delete(t.Context(), done); err != nil {
			t.Fatal(err)
		}
		waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonDeleting, "the ControlPlane is being deleted")
		if err := kept(t, r, record, lodiav1alpha1.ReasonDeleting)(); err != nil {
			t.Fatal(err)
		}

		before := r.controlPlane(t)
		before.Status = *done.Status.DeepCopy()
		before.Status.Restore = nil
		stale := interceptor.NewClient(k8s, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
					before.DeepCopyInto(cp)
					return nil
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
		reconciler := &datastore.Reconciler{Client: stale, Reader: apiReader, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace, Revision: revision, EtcdDruidDeployment: druidDeployment, EtcdDruidVersion: druidVersion, EtcdClient: dialRestored, Clock: clock.RealClock{}}
		if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: r.ns, Name: restoredCP}}); err != nil {
			t.Fatal(err)
		}
		if err := kept(t, r, record, lodiav1alpha1.ReasonDeleting)(); err != nil {
			t.Errorf("a deletion pass from a cache without status.restore: %v", err)
		}

		release(t, etcd)
		testenv.Eventually(t, func() error {
			if cp := r.controlPlane(t); controllerutil.ContainsFinalizer(cp, lodiav1alpha1.DatastoreFinalizer) {
				return errors.New("the Datastore finalizer is still there")
			}
			return nil
		})
		if err := kept(t, r, record, lodiav1alpha1.ReasonDeleting)(); err != nil {
			t.Error(err)
		}
		if cp := r.controlPlane(t); cp.Status.Datastore != nil {
			t.Errorf("status.datastore is %+v once the Etcd is gone", cp.Status.Datastore)
		}
		release(t, done)
	})

	t.Run("DatastoreReady removed", func(t *testing.T) {
		r, record := completed(t)
		updates, err := k8s.Watch(t.Context(), &lodiav1alpha1.ControlPlaneList{}, client.InNamespace(r.ns))
		if err != nil {
			t.Fatal(err)
		}
		defer updates.Stop()
		cp := r.controlPlane(t)
		i := slices.IndexFunc(cp.Status.Conditions, func(c metav1.Condition) bool { return c.Type == lodiav1alpha1.ConditionDatastoreReady })
		remove := fmt.Appendf(nil, `[{"op":"test","path":"/status/conditions/%d/type","value":%q},{"op":"remove","path":"/status/conditions/%d"}]`, i, lodiav1alpha1.ConditionDatastoreReady, i)
		if err := k8s.Status().Patch(t.Context(), cp, client.RawPatch(types.JSONPatchType, remove), client.FieldOwner("someone-else")); err != nil {
			t.Fatal(err)
		}
		deadline := time.After(30 * time.Second)
		for first := false; ; {
			select {
			case event := <-updates.ResultChan():
				fresh, ok := event.Object.(*lodiav1alpha1.ControlPlane)
				if !ok {
					t.Fatalf("the watch sent %s %T", event.Type, event.Object)
				}
				if !reflect.DeepEqual(fresh.Status.Restore, record) {
					t.Fatalf("a write left status.restore %+v, want %+v", fresh.Status.Restore, record)
				}
				ready := meta.FindStatusCondition(fresh.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
				switch {
				case ready == nil:
				case ready.Reason == lodiav1alpha1.ReasonReconciling:
					first = true
				case first && ready.Status == metav1.ConditionTrue:
					return
				}
			case <-deadline:
				t.Fatal("DatastoreReady did not come back through the first pass's Reconciling")
			}
		}
	})
}

func TestAnEtcdCreatedAgainFromAStaleCacheNeedsNoBackup(t *testing.T) {
	t.Parallel()
	r := newRestore(t, restoreSetup{})
	r.copied(t)
	marker := r.cleanedUp(t)
	before := r.controlPlane(t)
	completed := r.complete(t, marker).Status.Restore
	if err := k8s.Delete(t.Context(), r.backup); err != nil {
		t.Fatal(err)
	}
	pause(t, r.ns, restoredCP)
	if err := k8s.Delete(t.Context(), getEtcd(t, r.ns, restoredCP)); err != nil {
		t.Fatal(err)
	}
	stale := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
				before.DeepCopyInto(cp)
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	reconciler := &datastore.Reconciler{Client: stale, Reader: apiReader, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace, Revision: revision, EtcdDruidDeployment: druidDeployment, EtcdDruidVersion: druidVersion, EtcdClient: dialRestored, Clock: clock.RealClock{}}
	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: r.ns, Name: restoredCP}}); err != nil {
		t.Fatal(err)
	}
	getEtcd(t, r.ns, restoredCP)
	if cp := r.controlPlane(t); !reflect.DeepEqual(cp.Status.Restore, completed) {
		t.Errorf("a pass from a cache without status.restore left %+v, want the restore %+v", cp.Status.Restore, completed)
	}
	if task := r.copyTask(t); task != nil {
		t.Errorf("EtcdCopyBackupsTask %s copies the Backup again", task.Name)
	}
}

func TestAnEtcdDeletedMidRestoreComesBackOverItsClaims(t *testing.T) {
	t.Parallel()
	r := newRestore(t, restoreSetup{controlPlane: func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
	}})
	r.etcd.unreachable.Store(true)
	first := r.copied(t)
	if err := k8s.Delete(t.Context(), r.copyTask(t)); err != nil {
		t.Fatal(err)
	}
	pvc := claim(r.ns, "b-etcd-b-etcd-0", map[string]string{lodiav1alpha1.ControlPlaneUIDLabel: string(r.controlPlane(t).UID)})
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")
	create(t, pvc)
	if err := k8s.Delete(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	again := waitEtcd(t, r.ns, restoredCP, func(etcd *druidv1alpha1.Etcd) error {
		if etcd.UID == first.UID {
			return errors.New("the deleted Etcd is still there")
		}
		return nil
	})
	if again.Spec.Replicas != 1 {
		t.Errorf("the Etcd created again over the claim of its one member asks for %d members, want 1", again.Spec.Replicas)
	}
	consistently(t, func() error {
		if task := r.copyTask(t); task != nil {
			return fmt.Errorf("EtcdCopyBackupsTask %s copies the Backup again over the claims", task.Name)
		}
		return nil
	})
}

func TestACompletedRestoreComesBackOverItsClaimsWithTheirMembers(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name     string
		ordinals []int
		removed  bool
	}{
		{"three claims", []int{0, 1, 2}, false},
		{"claim 0 missing", []int{1, 2}, false},
		{"a claim beyond highAvailability's members", []int{0, 1, 2, 3}, false},
		{"status.restore removed", []int{0, 1, 2}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRestore(t, restoreSetup{controlPlane: func(cp *lodiav1alpha1.ControlPlane) {
				cp.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
			}})
			r.copied(t)
			r.complete(t, r.cleanedUp(t))
			first := waitEtcd(t, r.ns, restoredCP, func(etcd *druidv1alpha1.Etcd) error {
				if etcd.Spec.Replicas != 3 {
					return fmt.Errorf("the completed restore's Etcd asks for %d members", etcd.Spec.Replicas)
				}
				return nil
			})
			uid := string(r.controlPlane(t).UID)
			for _, ordinal := range c.ordinals {
				create(t, claim(r.ns, fmt.Sprintf("b-etcd-b-etcd-%d", ordinal), map[string]string{lodiav1alpha1.ControlPlaneUIDLabel: uid}))
			}
			if c.removed {
				pause(t, r.ns, restoredCP)
				remove := []byte(`[{"op":"remove","path":"/status/restore"}]`)
				if err := k8s.Status().Patch(t.Context(), r.controlPlane(t), client.RawPatch(types.JSONPatchType, remove), client.FieldOwner("someone-else")); err != nil {
					t.Fatal(err)
				}
			}
			if err := k8s.Delete(t.Context(), first); err != nil {
				t.Fatal(err)
			}
			if c.removed {
				testenv.Eventually(t, func() error {
					return gone(cached.Get(t.Context(), client.ObjectKey{Namespace: r.ns, Name: restoredCP + "-etcd"}, &druidv1alpha1.Etcd{}))
				})
				if restore := r.controlPlane(t).Status.Restore; restore != nil {
					t.Fatalf("status.restore came back as %+v while the ControlPlane is paused", restore)
				}
				setPaused(t, r.ns, restoredCP, false)
			}
			again := waitEtcd(t, r.ns, restoredCP, func(etcd *druidv1alpha1.Etcd) error {
				if etcd.UID == first.UID {
					return errors.New("the deleted Etcd is still there")
				}
				return nil
			})
			if again.Spec.Replicas != 3 {
				t.Fatalf("the Etcd created again over the claims of members %v asks for %d members, want 3", c.ordinals, again.Spec.Replicas)
			}
			poke(t, r.ns)
			consistently(t, func() error {
				if n := getEtcd(t, r.ns, restoredCP).Spec.Replicas; n != 3 {
					return fmt.Errorf("the Etcd over the claims of members %v asks for %d members, want 3", c.ordinals, n)
				}
				return nil
			})
		})
	}
}

func TestAStalePassWhoseCheckCannotRunKeepsTheMembers(t *testing.T) {
	t.Parallel()
	r := newRestore(t, restoreSetup{controlPlane: func(cp *lodiav1alpha1.ControlPlane) {
		cp.Spec.HighAvailability = &lodiav1alpha1.HighAvailability{FailureTolerance: "Node"}
	}})
	r.copied(t)
	marker := r.cleanedUp(t)
	before := getEtcd(t, r.ns, restoredCP)
	r.complete(t, marker)
	waitEtcd(t, r.ns, restoredCP, func(etcd *druidv1alpha1.Etcd) error {
		if etcd.Spec.Replicas != 3 {
			return fmt.Errorf("%d members", etcd.Spec.Replicas)
		}
		return nil
	})
	r.etcd.unreachable.Store(true)
	grantClass(t, r.ns, restoredCP, r.ns)
	stale := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if etcd, ok := obj.(*druidv1alpha1.Etcd); ok {
				before.DeepCopyInto(etcd)
				return nil
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	reconciler := &datastore.Reconciler{Client: stale, Reader: k8s, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace, Revision: revision, EtcdDruidDeployment: druidDeployment, EtcdDruidVersion: druidVersion, EtcdClient: dialRestored, Clock: clock.RealClock{}}
	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: r.ns, Name: restoredCP}}); err != nil {
		t.Fatal(err)
	}
	if n := getEtcd(t, r.ns, restoredCP).Spec.Replicas; n != 3 {
		t.Errorf("a pass from a cache that holds the Etcd before its scale-up left %d members, want 3", n)
	}
	injected := errors.New("injected: the API server did not answer")
	failing := interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*druidv1alpha1.Etcd); ok {
				return injected
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	reconciler.Reader = failing
	if _, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: r.ns, Name: restoredCP}}); !errors.Is(err, injected) {
		t.Fatalf("a pass whose read of the Etcd failed returned %v, want %v", err, injected)
	}
	if n := getEtcd(t, r.ns, restoredCP).Spec.Replicas; n != 3 {
		t.Errorf("a pass whose read of the Etcd failed left %d members, want 3", n)
	}
}

func TestAFailedCopyStaysAMinute(t *testing.T) {
	t.Parallel()
	r := newRestore(t, restoreSetup{})
	task := r.waitCopyTask(t)
	failed := time.Now().Truncate(time.Second)
	failCopy(t, r.ns, task.Name, "the bucket is gone", failed)
	now := clocktesting.NewFakePassiveClock(failed.Add(time.Minute - time.Second))
	reconciler := &datastore.Reconciler{Client: cached, Reader: apiReader, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace, Revision: revision, EtcdDruidDeployment: druidDeployment, EtcdDruidVersion: druidVersion, EtcdClient: dialRestored, Clock: now}
	pass := func() (time.Duration, error) {
		result, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: r.ns, Name: restoredCP}})
		return result.RequeueAfter, err
	}
	testenv.Eventually(t, func() error {
		requeueAfter, err := pass()
		if err == nil && requeueAfter != time.Second {
			return fmt.Errorf("a pass a second before the minute returned a requeue in %v, want 1s", requeueAfter)
		}
		return err
	})
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "the copy of Backup a-1 failed: the bucket is gone; copying again")
	if again := r.copyTask(t); again == nil || again.UID != task.UID || !again.DeletionTimestamp.IsZero() {
		t.Fatal("the failed copy task went before its minute")
	}
	now.SetTime(failed.Add(time.Minute))
	refused := errors.New("injected: the API server refused the delete")
	refusing := interceptor.NewClient(k8s, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*druidv1alpha1.EtcdCopyBackupsTask); ok {
				return refused
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	if _, err := (&datastore.Reconciler{Client: refusing, Reader: apiReader, EtcdDruidNamespace: druidNamespace, OperatorNamespace: operatorNamespace, Revision: revision, EtcdDruidDeployment: druidDeployment, EtcdDruidVersion: druidVersion, EtcdClient: dialRestored, Clock: now}).Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: r.ns, Name: restoredCP}}); !errors.Is(err, refused) {
		t.Fatalf("a pass whose delete was refused returned %v, want %v", err, refused)
	}
	if again := r.copyTask(t); again == nil || again.UID != task.UID || !again.DeletionTimestamp.IsZero() {
		t.Fatal("the failed copy task went though its delete was refused")
	}
	if _, err := pass(); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		if again := r.copyTask(t); again == nil || again.UID == task.UID {
			return errors.New("no new copy task yet")
		}
		return nil
	})
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "copying Backup a-1 into this ControlPlane's prefix")
}

func TestACleanupTheEtcdRefusesAtItsQuota(t *testing.T) {
	t.Parallel()
	r := newRestore(t, restoreSetup{quota: 1 << 20, preload: func(t *testing.T, e *restoredEtcd) {
		e.put(t, append(slices.Clone(cleanedKeys), keptKeys...)...)
		for i := 0; ; i++ {
			if _, err := e.Put(t.Context(), fmt.Sprintf("/filler/%d", i), strings.Repeat("x", 256<<10)); err != nil {
				break
			}
		}
	}})
	r.copied(t)
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring,
		`etcd refuses the cleanup: etcdserver: mvcc: database space exceeded; see "etcd space quota" in docs/operations.md`)
	if r.marker(t) != 0 {
		t.Fatal("a cleanup the etcd refused left the marker")
	}
	r.etcd.present(t, append(slices.Clone(cleanedKeys), keptKeys...)...)
}

func TestARestoreCompletesOnItsFullSnapshotWithoutAnEvent(t *testing.T) {
	t.Parallel()
	r := newRestore(t, restoreSetup{})
	r.copied(t)
	marker := r.cleanedUp(t)
	settled := "EtcdOpsTask " + r.taskName(t) + " is not admitted yet"
	waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, settled)
	consistently(t, func() error {
		if ready := meta.FindStatusCondition(r.controlPlane(t).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady); !strings.Contains(ready.Message, settled) {
			return fmt.Errorf("DatastoreReady moved to %s %q", ready.Reason, ready.Message)
		}
		return nil
	})
	createLease(t, r.ns, restoredCP+"-etcd-full-snap", strconv.FormatInt(marker, 10))
	waitCondition(t, r.ns, restoredCP, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "has quorum")
}

func TestARestoreReadThatFailsIsNoAnswer(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name string
		key  func(*testing.T, *restore) string
	}{
		{"the marker", func(t *testing.T, r *restore) string { return lodiav1alpha1.RestoreMarkerKey(r.controlPlane(t).UID) }},
		{"the seal", func(_ *testing.T, r *restore) string { return lodiav1alpha1.BackupSealKey(r.backup.UID) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRestore(t, restoreSetup{})
			r.etcd.refusedGet.Store(c.key(t, r))
			r.copied(t)
			waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "waiting for etcd of b to answer")
			poke(t, r.ns)
			consistently(t, func() error {
				if r.marker(t) != 0 {
					return errors.New("cleaned up while a read failed")
				}
				return nil
			})
			r.etcd.present(t, cleanedKeys...)
			waitCondition(t, r.ns, restoredCP, metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, "waiting for etcd of b to answer")
			r.etcd.refusedGet.Store("")
			poke(t, r.ns)
			r.complete(t, r.cleanedUp(t))
		})
	}
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
	t.Parallel()
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
		consistently(t, func() error {
			if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: "old-etcd"}, &druidv1alpha1.Etcd{}); !apierrors.IsNotFound(err) {
				return fmt.Errorf("want no Etcd under an older CA, got %w", err)
			}
			return nil
		})
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
