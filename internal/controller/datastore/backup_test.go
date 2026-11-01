// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.etcd.io/etcd/server/v3/embed"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/datastore"
	"go.lodia.io/lodia/internal/operator"
)

const sourceName = "src"

type source struct {
	ns, credentials string
	etcd            *kubernetes.Client
	keys            []byte
}

func newSource(t *testing.T, etcd *embed.Config, before func(*source)) *source {
	t.Helper()
	s := &source{ns: namespace(t), credentials: namespace(t), etcd: testserver.RunEtcd(t, etcd)}
	create(t, credentials(s.credentials))
	createClass(t, fullClass(s.ns, s.credentials))
	if before != nil {
		before(s)
	}
	create(t, controlPlane(s.ns, sourceName, s.ns))
	writeGeneration(t, s.ns, sourceName, 1)
	waitEtcd(t, s.ns, sourceName, namesGeneration(1))
	for _, lease := range []string{fullLease, deltaLease} {
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: s.ns, Name: lease}, &coordinationv1.Lease{}); apierrors.IsNotFound(err) {
			createLease(t, s.ns, lease, "0")
		}
	}
	target := []byte(`{"status":{"target":{"version":"v1.37.1"}}}`)
	if err := k8s.Status().Patch(t.Context(), getControlPlane(t, s.ns, sourceName), client.RawPatch(types.MergePatchType, target)); err != nil {
		t.Fatal(err)
	}
	s.keys = []byte("apiVersion: apiserver.config.k8s.io/v1\nkind: EncryptionConfiguration\nresources:\n- resources: [secrets]\n  providers:\n  - aescbc: {keys: [{name: key-2, secret: c2Vjb25k}]}\n  - aescbc: {keys: [{name: key-1, secret: Zmlyc3Q=}]}\n")
	create(t, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: sourceName + "-encryption-config", Namespace: s.ns, Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: sourceName}},
		Data:       map[string][]byte{lodiav1alpha1.EncryptionConfigKey: s.keys},
	})
	readyOn(t, s.ns, sourceName, 1)
	return s
}

const (
	fullLease  = sourceName + "-etcd-full-snap"
	deltaLease = sourceName + "-etcd-delta-snap"
)

func createLease(t *testing.T, ns, name, holder string) {
	t.Helper()
	create(t, &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}, Spec: coordinationv1.LeaseSpec{HolderIdentity: new(holder)}})
}

func setLease(t *testing.T, ns, name string, holder int64) {
	t.Helper()
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		lease := &coordinationv1.Lease{}
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, lease); err != nil {
			return err
		}
		lease.Spec.HolderIdentity = new(strconv.FormatInt(holder, 10))
		return k8s.Update(t.Context(), lease)
	}); err != nil {
		t.Fatal(err)
	}
}

func (s *source) reconciler(c client.Client) *datastore.BackupReconciler {
	return &datastore.BackupReconciler{Client: c, Reader: apiReader, EtcdClient: s.dial}
}

func (s *source) dial(context.Context, *druidv1alpha1.Etcd) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{Endpoints: s.etcd.Endpoints(), DialTimeout: 5 * time.Second})
}

func (s *source) revision(t *testing.T) int64 {
	t.Helper()
	response, err := s.etcd.Client.Get(t.Context(), "/lodia.io/probe")
	if err != nil {
		t.Fatal(err)
	}
	return response.Header.GetRevision()
}

func (s *source) sealKey(t *testing.T, uid types.UID) *mvccpb.KeyValue {
	t.Helper()
	response, err := s.etcd.Client.Get(t.Context(), lodiav1alpha1.BackupSealKey(uid))
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Kvs) == 0 {
		return nil
	}
	return response.Kvs[0]
}

func (s *source) sealKeys(t *testing.T) int64 {
	t.Helper()
	response, err := s.etcd.Client.Get(t.Context(), "/lodia.io/backups/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		t.Fatal(err)
	}
	return response.Count
}

func (s *source) actAsDruid(t *testing.T) {
	t.Helper()
	tasks := &druidv1alpha1.EtcdOpsTaskList{}
	if err := k8s.List(t.Context(), tasks, client.InNamespace(s.ns)); err != nil {
		t.Fatal(err)
	}
	for i := range tasks.Items {
		if tasks.Items[i].Status.State == nil {
			setLease(t, s.ns, deltaLease, s.revision(t))
			finishFlush(t, s.ns, tasks.Items[i].Name, druidv1alpha1.TaskStateSucceeded)
		}
	}
	copies := &druidv1alpha1.EtcdCopyBackupsTaskList{}
	if err := k8s.List(t.Context(), copies, client.InNamespace(s.ns)); err != nil {
		t.Fatal(err)
	}
	for i := range copies.Items {
		if len(copies.Items[i].Status.Conditions) == 0 {
			finishCopy(t, s.ns, copies.Items[i].Name, druidv1alpha1.EtcdCopyBackupsTaskSucceeded, "")
		}
	}
}

func finishFlush(t *testing.T, ns, name string, state druidv1alpha1.TaskState) {
	t.Helper()
	task := &druidv1alpha1.EtcdOpsTask{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, task); err != nil {
		t.Fatal(err)
	}
	task.Status.State = new(state)
	task.Status.LastTransitionTime = new(metav1.Now())
	if err := k8s.Status().Update(t.Context(), task); err != nil {
		t.Fatal(err)
	}
}

func finishCopy(t *testing.T, ns, name string, conditionType druidv1alpha1.ConditionType, message string) {
	t.Helper()
	task := &druidv1alpha1.EtcdCopyBackupsTask{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, task); err != nil {
		t.Fatal(err)
	}
	task.Status.Conditions = []druidv1alpha1.Condition{{
		Type: conditionType, Status: druidv1alpha1.ConditionTrue, Reason: "Job" + string(conditionType), Message: message,
		LastTransitionTime: metav1.Now(), LastUpdateTime: metav1.Now(),
	}}
	if err := k8s.Status().Update(t.Context(), task); err != nil {
		t.Fatal(err)
	}
}

func createBackup(t *testing.T, ns, name string, annotations map[string]string) *lodiav1alpha1.Backup {
	t.Helper()
	backup := &lodiav1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Annotations: annotations},
		Spec:       lodiav1alpha1.BackupSpec{ControlPlaneRef: lodiav1alpha1.LocalReference{Name: sourceName}},
	}
	create(t, backup)
	return backup
}

func getBackup(t *testing.T, ns, name string) *lodiav1alpha1.Backup {
	t.Helper()
	backup := &lodiav1alpha1.Backup{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: name}, backup); err != nil {
		t.Fatal(err)
	}
	return backup
}

func reconcileBackup(r *datastore.BackupReconciler, ns, name string) (reconcile.Result, error) {
	return r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
}

func passes(t *testing.T, r *datastore.BackupReconciler, ns, name string, check func(*lodiav1alpha1.Backup) error) *lodiav1alpha1.Backup {
	t.Helper()
	var backup *lodiav1alpha1.Backup
	eventually(t, func() error {
		if _, err := reconcileBackup(r, ns, name); err != nil {
			return err
		}
		backup = getBackup(t, ns, name)
		return check(backup)
	})
	return backup
}

func readyIs(status metav1.ConditionStatus, reason, message string) func(*lodiav1alpha1.Backup) error {
	return func(backup *lodiav1alpha1.Backup) error {
		ready := meta.FindStatusCondition(backup.Status.Conditions, lodiav1alpha1.ConditionReady)
		switch {
		case ready == nil:
			return errors.New("no Ready")
		case ready.Status != status || ready.Reason != reason || !strings.Contains(ready.Message, message):
			return fmt.Errorf("want Ready %s/%s with %q, got %s/%s %q", status, reason, message, ready.Status, ready.Reason, ready.Message)
		case ready.ObservedGeneration != backup.Generation:
			return fmt.Errorf("Ready describes generation %d of %d", ready.ObservedGeneration, backup.Generation)
		}
		return nil
	}
}

func task(uid types.UID) string { return "backup-" + string(uid) }

func getFlush(t *testing.T, ns string, uid types.UID) *druidv1alpha1.EtcdOpsTask {
	t.Helper()
	flush := &druidv1alpha1.EtcdOpsTask{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: task(uid)}, flush); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		t.Fatal(err)
	}
	return flush
}

func getCopyTask(t *testing.T, ns string, uid types.UID) *druidv1alpha1.EtcdCopyBackupsTask {
	t.Helper()
	copyTask := &druidv1alpha1.EtcdCopyBackupsTask{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: ns, Name: task(uid)}, copyTask); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		t.Fatal(err)
	}
	return copyTask
}

func ownedByBackup(t *testing.T, obj client.Object, backup *lodiav1alpha1.Backup) {
	t.Helper()
	want := []metav1.OwnerReference{{APIVersion: "lodia.io/v1alpha1", Kind: "Backup", Name: backup.Name, UID: backup.UID, Controller: new(true), BlockOwnerDeletion: new(true)}}
	if !reflect.DeepEqual(obj.GetOwnerReferences(), want) {
		t.Errorf("%s has ownerReferences %+v, want %+v", obj.GetName(), obj.GetOwnerReferences(), want)
	}
	if want := map[string]string{lodiav1alpha1.ControlPlaneLabel: sourceName}; !reflect.DeepEqual(obj.GetLabels(), want) {
		t.Errorf("%s has labels %v, want %v", obj.GetName(), obj.GetLabels(), want)
	}
}

func namespaceUID(t *testing.T, name string) types.UID {
	t.Helper()
	ns := &corev1.Namespace{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: name}, ns); err != nil {
		t.Fatal(err)
	}
	return ns.UID
}

func sealed(backup *lodiav1alpha1.Backup) error {
	if backup.Status.Seal == nil {
		return errors.New("no seal yet")
	}
	return nil
}

func (s *source) toCopy(t *testing.T, r *datastore.BackupReconciler) *lodiav1alpha1.Backup {
	t.Helper()
	createBackup(t, s.ns, "b", nil)
	backup := passes(t, r, s.ns, "b", sealed)
	setLease(t, s.ns, deltaLease, backup.Status.Seal.Revision)
	return passes(t, r, s.ns, "b", readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying, ""))
}

func TestABackupIsSealedAndCopied(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	created := createBackup(t, src.ns, "before-upgrade", nil)
	cp := getControlPlane(t, src.ns, sourceName)
	etcd := getEtcd(t, src.ns, sourceName)

	started := passes(t, r, src.ns, created.Name, readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotting, "sealing"))
	prefix := "prod/" + string(namespaceUID(t, src.ns)) + "/backups/" + string(created.UID)
	if want := (lodiav1alpha1.BackupStatus{
		Conditions:        started.Status.Conditions,
		Source:            &lodiav1alpha1.UIDReference{Name: sourceName, UID: cp.UID},
		KubernetesVersion: "v1.37.1",
		Network:           &cp.Spec.Network,
		Snapshots:         &lodiav1alpha1.SnapshotLocation{Provider: "S3", Endpoint: "https://s3.example.test", Bucket: "lodia-backups", Prefix: prefix, FromSecret: &lodiav1alpha1.StoreSecretFields{Region: new("eu-central-1")}},
		StartedAt:         started.Status.StartedAt,
	}); started.Status.StartedAt == nil || !reflect.DeepEqual(started.Status, want) {
		t.Fatalf("the started status is\n%+v\nwant\n%+v", started.Status, want)
	}
	if key := src.sealKey(t, created.UID); key != nil || getFlush(t, src.ns, created.UID) != nil {
		t.Fatalf("the first pass sealed or flushed: key %v", key)
	}

	backup := passes(t, r, src.ns, created.Name, sealed)
	key := src.sealKey(t, created.UID)
	switch {
	case key == nil:
		t.Fatal("no seal key in the source's etcd")
	case string(key.GetValue()) != string(created.UID) || key.GetCreateRevision() != backup.Status.Seal.Revision:
		t.Fatalf("the seal key holds %q at revision %d, status.seal.revision %d", key.GetValue(), key.GetCreateRevision(), backup.Status.Seal.Revision)
	case backup.Status.Seal.QuotaBackendBytes == nil || backup.Status.Seal.QuotaBackendBytes.Cmp(*etcd.Spec.Etcd.Quota) != 0:
		t.Fatalf("status.seal.quotaBackendBytes %v, want the Etcd's %v", backup.Status.Seal.QuotaBackendBytes, etcd.Spec.Etcd.Quota)
	}
	revision := backup.Status.Seal.Revision

	setLease(t, src.ns, deltaLease, revision-1)
	passes(t, r, src.ns, created.Name, readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotting,
		fmt.Sprintf("the seal is at revision %d in src's etcd; the store holds %d (Leases src-etcd-full-snap, src-etcd-delta-snap); EtcdOpsTask %s is ", revision, revision-1, task(created.UID))))
	flush := getFlush(t, src.ns, created.UID)
	if flush == nil {
		t.Fatal("no EtcdOpsTask")
	}
	if want := (druidv1alpha1.EtcdOpsTaskSpec{
		Config:                  druidv1alpha1.EtcdOpsTaskConfig{OnDemandSnapshot: &druidv1alpha1.OnDemandSnapshotConfig{Type: druidv1alpha1.OnDemandSnapshotTypeDelta, TimeoutSecondsDelta: new(int32(60)), TimeoutSecondsFull: new(int32(900))}},
		TTLSecondsAfterFinished: new(int32(3600)),
		EtcdName:                new("src-etcd"),
	}); !reflect.DeepEqual(flush.Spec, want) {
		t.Errorf("the EtcdOpsTask's spec is %+v, want %+v", flush.Spec, want)
	}
	ownedByBackup(t, flush, created)
	for range 3 {
		if _, err := reconcileBackup(r, src.ns, created.Name); err != nil {
			t.Fatal(err)
		}
		if err := readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotting, "the store holds "+strconv.FormatInt(revision-1, 10))(getBackup(t, src.ns, created.Name)); err != nil {
			t.Fatal(err)
		}
	}
	if getCopyTask(t, src.ns, created.UID) != nil {
		t.Fatal("a Lease below the seal let the copy start")
	}

	setLease(t, src.ns, deltaLease, revision)
	passes(t, r, src.ns, created.Name, readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying,
		fmt.Sprintf("EtcdCopyBackupsTask %s copies the newest snapshots of src to lodia-backups/%s", task(created.UID), prefix)))
	copyTask := getCopyTask(t, src.ns, created.UID)
	store := etcd.Spec.Backup.Store
	if want := "prod/" + string(namespaceUID(t, src.ns)) + "/controlplanes/" + string(cp.UID); store.Prefix != want || store.SecretRef.Name != "src-etcd-backup" {
		t.Fatalf("the source's store is %+v, want the prefix %s and the Secret src-etcd-backup", store, want)
	}
	target := store.DeepCopy()
	target.Prefix = prefix
	if want := (druidv1alpha1.EtcdCopyBackupsTaskSpec{SourceStore: *store, TargetStore: *target, MaxBackups: new(uint32(1))}); !reflect.DeepEqual(copyTask.Spec, want) {
		t.Errorf("the EtcdCopyBackupsTask's spec is %+v, want %+v", copyTask.Spec, want)
	}
	ownedByBackup(t, copyTask, created)
	if _, err := reconcileBackup(r, src.ns, created.Name); err != nil {
		t.Fatal(err)
	}
	if err := readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying, "")(getBackup(t, src.ns, created.Name)); err != nil {
		t.Fatal(err)
	}

	finishCopy(t, src.ns, task(created.UID), druidv1alpha1.EtcdCopyBackupsTaskSucceeded, "")
	completed := passes(t, r, src.ns, created.Name, readyIs(metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted,
		fmt.Sprintf("sealed at revision %d and copied to lodia-backups/%s", revision, prefix)))
	if completed.Status.CompletedAt == nil || completed.Status.Seal.Revision != revision || completed.Status.StartedAt.Time != started.Status.StartedAt.Time {
		t.Errorf("the completed status lost a recorded field: %+v", completed.Status)
	}
	keys := &corev1.Secret{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: src.ns, Name: "before-upgrade.encryption-config"}, keys); err != nil {
		t.Fatal(err)
	}
	if keys.Type != corev1.SecretTypeOpaque || keys.Immutable == nil || !*keys.Immutable || !reflect.DeepEqual(keys.Data, map[string][]byte{"encryption-config.yaml": src.keys}) {
		t.Errorf("the Backup's keys are type %s, immutable %v, data %q; want an immutable Opaque Secret with the source's configuration", keys.Type, keys.Immutable, keys.Data)
	}
	ownedByBackup(t, keys, created)

	done := passes(t, r, src.ns, created.Name, func(b *lodiav1alpha1.Backup) error {
		if b.Annotations[lodiav1alpha1.SealedUIDAnnotation] != string(b.UID) {
			return fmt.Errorf("annotations %v", b.Annotations)
		}
		if getFlush(t, src.ns, b.UID) != nil || getCopyTask(t, src.ns, b.UID) != nil {
			return errors.New("a task of the Backup is left")
		}
		return nil
	})
	consistently(t, func() error {
		if _, err := reconcileBackup(r, src.ns, created.Name); err != nil {
			return err
		}
		if again := getBackup(t, src.ns, created.Name); again.ResourceVersion != done.ResourceVersion {
			return fmt.Errorf("a pass of a sealed Backup moved it from resourceVersion %s to %s", done.ResourceVersion, again.ResourceVersion)
		}
		return nil
	})
}

func TestEitherSnapshotLeaseShowsTheSeal(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	for _, lease := range []string{deltaLease, fullLease} {
		t.Run(lease, func(t *testing.T) {
			setLease(t, src.ns, fullLease, 0)
			setLease(t, src.ns, deltaLease, 0)
			created := createBackup(t, src.ns, lease, nil)
			backup := passes(t, r, src.ns, lease, sealed)
			setLease(t, src.ns, lease, backup.Status.Seal.Revision)
			passes(t, r, src.ns, lease, readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying, ""))
			if getCopyTask(t, src.ns, created.UID) == nil {
				t.Fatal("no EtcdCopyBackupsTask")
			}
		})
	}
}

func TestAFlushThatEndsShortOfTheSealIsSentAgain(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	for _, state := range []druidv1alpha1.TaskState{druidv1alpha1.TaskStateSucceeded, druidv1alpha1.TaskStateFailed, druidv1alpha1.TaskStateRejected} {
		t.Run(string(state), func(t *testing.T) {
			name := strings.ToLower(string(state))
			created := createBackup(t, src.ns, name, nil)
			passes(t, r, src.ns, name, func(*lodiav1alpha1.Backup) error {
				if getFlush(t, src.ns, created.UID) == nil {
					return errors.New("no EtcdOpsTask yet")
				}
				return nil
			})
			first := getFlush(t, src.ns, created.UID)
			finishFlush(t, src.ns, first.Name, state)
			passes(t, r, src.ns, name, func(*lodiav1alpha1.Backup) error {
				if again := getFlush(t, src.ns, created.UID); again == nil || again.UID == first.UID {
					return fmt.Errorf("no EtcdOpsTask after the %s one", state)
				}
				return nil
			})
			if getCopyTask(t, src.ns, created.UID) != nil {
				t.Fatal("a flush short of the seal let the copy start")
			}
		})
	}
}

func TestALeaseOlderThanTheEtcdIsIgnored(t *testing.T) {
	src := newSource(t, nil, func(s *source) {
		createLease(t, s.ns, deltaLease, "1000000000")
		createLease(t, s.ns, fullLease, "1000000000")
		time.Sleep(1100 * time.Millisecond)
	})
	r := src.reconciler(cached)
	created := createBackup(t, src.ns, "leftover", nil)
	backup := passes(t, r, src.ns, created.Name, sealed)
	passes(t, r, src.ns, created.Name, readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotting,
		"the store holds 0 (Leases src-etcd-full-snap, src-etcd-delta-snap); EtcdOpsTask "+task(created.UID)))
	ready := meta.FindStatusCondition(getBackup(t, src.ns, created.Name).Status.Conditions, lodiav1alpha1.ConditionReady)
	for _, lease := range []string{fullLease, deltaLease} {
		if want := "ignored Lease " + lease + ", created before Etcd src-etcd"; !strings.Contains(ready.Message, want) {
			t.Errorf("Ready says %q, want %q", ready.Message, want)
		}
	}
	if backup.Status.Seal.Revision >= 1000000000 || getCopyTask(t, src.ns, created.UID) != nil {
		t.Fatal("a predecessor's Lease let the copy start")
	}
}

type refusingNth struct {
	client.Client
	writes *int
	nth    int
}

var errRefused = errors.New("injected: the API server refused the write")

func (c refusingNth) refused() bool {
	*c.writes++
	return *c.writes == c.nth
}

func (c refusingNth) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if c.refused() {
		return errRefused
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c refusingNth) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if c.refused() {
		return errRefused
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func (c refusingNth) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if c.refused() {
		return errRefused
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c refusingNth) Status() client.SubResourceWriter {
	return refusingStatus{SubResourceWriter: c.Client.Status(), c: c}
}

type refusingStatus struct {
	client.SubResourceWriter
	c refusingNth
}

func (s refusingStatus) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	if s.c.refused() {
		return errRefused
	}
	return s.SubResourceWriter.Apply(ctx, obj, opts...)
}

func TestAWriteRefusedAtAnyStep(t *testing.T) {
	src := newSource(t, nil, nil)
	writes := seal(t, src, "refused-none", 0)
	if writes < 10 {
		t.Fatalf("a Backup took %d writes", writes)
	}
	for nth := 1; nth <= writes; nth++ {
		t.Run(strconv.Itoa(nth), func(t *testing.T) {
			seal(t, src, "refused-"+strconv.Itoa(nth), nth)
		})
	}
}

func seal(t *testing.T, src *source, name string, nth int) int {
	t.Helper()
	writes := 0
	r := src.reconciler(refusingNth{Client: cached, writes: &writes, nth: nth})
	created := createBackup(t, src.ns, name, nil)
	var revision int64
	copies := map[types.UID]bool{}
	for pass := 1; ; pass++ {
		if pass > 100 {
			t.Fatal("the Backup was not sealed in 100 passes")
		}
		src.actAsDruid(t)
		previous := writes
		_, err := reconcileBackup(r, src.ns, name)
		if err != nil && (nth <= previous || nth > writes) {
			t.Fatalf("pass %d failed though it refused no write: %v", pass, err)
		}
		if _, err := src.etcd.Put(t.Context(), "/registry/written-meanwhile", strconv.Itoa(pass)); err != nil {
			t.Fatal(err)
		}
		backup := getBackup(t, src.ns, name)
		if key := src.sealKey(t, created.UID); key != nil {
			if revision == 0 {
				revision = key.GetCreateRevision()
			}
			if key.GetCreateRevision() != revision || key.GetModRevision() != revision {
				t.Fatalf("after pass %d the seal key was created at %d and written at %d, first created at %d", pass, key.GetCreateRevision(), key.GetModRevision(), revision)
			}
		}
		if backup.Status.Seal != nil && backup.Status.Seal.Revision != revision {
			t.Fatalf("after pass %d status.seal.revision is %d, the key's %d", pass, backup.Status.Seal.Revision, revision)
		}
		if copyTask := getCopyTask(t, src.ns, created.UID); copyTask != nil {
			copies[copyTask.UID] = true
		}
		if len(copies) > 1 {
			t.Fatalf("after pass %d the Backup had %d copy tasks", pass, len(copies))
		}
		if readyIs(metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted, "")(backup) == nil && backup.Annotations[lodiav1alpha1.SealedUIDAnnotation] == string(created.UID) &&
			getFlush(t, src.ns, created.UID) == nil && getCopyTask(t, src.ns, created.UID) == nil {
			if len(copies) != 1 || backup.Status.CompletedAt == nil {
				t.Fatalf("sealed with %d copy tasks: %+v", len(copies), backup.Status)
			}
			return writes
		}
		if err == nil {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func TestTheMarkIsWrittenAfterTheSourceIsGone(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	created := createBackup(t, src.ns, "marked-late", nil)
	passes(t, r, src.ns, created.Name, func(b *lodiav1alpha1.Backup) error {
		src.actAsDruid(t)
		return readyIs(metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted, "")(b)
	})
	writes := 0
	refusing := src.reconciler(refusingNth{Client: cached, writes: &writes, nth: 1})
	if _, err := reconcileBackup(refusing, src.ns, created.Name); !errors.Is(err, errRefused) {
		t.Fatalf("the pass with the mark refused returned %v", err)
	}
	if b := getBackup(t, src.ns, created.Name); b.Annotations[lodiav1alpha1.SealedUIDAnnotation] != "" {
		t.Fatalf("the refused mark is there: %v", b.Annotations)
	}
	if getFlush(t, src.ns, created.UID) == nil || getCopyTask(t, src.ns, created.UID) == nil {
		t.Fatal("the tasks were deleted before the mark")
	}

	if err := k8s.Delete(t.Context(), getControlPlane(t, src.ns, sourceName)); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error {
		return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: src.ns, Name: sourceName}, &lodiav1alpha1.ControlPlane{}))
	})
	passes(t, r, src.ns, created.Name, func(b *lodiav1alpha1.Backup) error {
		if err := readyIs(metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted, "")(b); err != nil {
			return err
		}
		if b.Annotations[lodiav1alpha1.SealedUIDAnnotation] != string(b.UID) {
			return fmt.Errorf("annotations %v", b.Annotations)
		}
		return nil
	})
}

type staleBackup struct {
	client.Reader
	backup *lodiav1alpha1.Backup
}

func (s staleBackup) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if backup, ok := obj.(*lodiav1alpha1.Backup); ok {
		s.backup.DeepCopyInto(backup)
		return nil
	}
	return s.Reader.Get(ctx, key, obj, opts...)
}

func TestAPassFromAStaleBackupDropsNoRecordedField(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	created := createBackup(t, src.ns, "stale", nil)
	before := getBackup(t, src.ns, created.Name)
	backup := passes(t, r, src.ns, created.Name, sealed)

	r.Reader = staleBackup{Reader: apiReader, backup: before}
	result, err := reconcileBackup(r, src.ns, created.Name)
	if err != nil || result.RequeueAfter != time.Second {
		t.Fatalf("a pass from a stale Backup returned %+v, %v; want a requeue after a second", result, err)
	}
	after := getBackup(t, src.ns, created.Name)
	if !reflect.DeepEqual(after.Status, backup.Status) {
		t.Fatalf("a pass from a stale Backup changed its status from\n%+v\nto\n%+v", backup.Status, after.Status)
	}
}

func TestABackupThatCannotBeTaken(t *testing.T) {
	t.Run("NoBackupStore", func(t *testing.T) {
		ns := namespace(t)
		createClass(t, newClass(ns, "Retain"))
		create(t, controlPlane(ns, sourceName, ns))
		writeGeneration(t, ns, sourceName, 1)
		waitEtcd(t, ns, sourceName, namesGeneration(1))
		readyOn(t, ns, sourceName, 1)
		finalBackup(t, ns, (&source{ns: ns}).reconciler(cached), readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonNoBackupStore, "the ControlPlane's class "+ns+" has no backup store"))
	})
	t.Run("Unsupported", func(t *testing.T) {
		ns := namespace(t)
		shared := controlPlane(ns, sourceName, "")
		shared.Spec.Datastore = lodiav1alpha1.DatastoreChoice{Shared: &lodiav1alpha1.SharedDatastore{Name: "pool"}}
		create(t, shared)
		finalBackup(t, ns, (&source{ns: ns}).reconciler(cached), readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported, "ControlPlane src has spec.datastore.shared"))
	})
	t.Run("ControlPlaneNotFound, absent", func(t *testing.T) {
		ns := namespace(t)
		r := (&source{ns: ns}).reconciler(cached)
		createBackup(t, ns, "b", nil)
		passes(t, r, ns, "b", readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonControlPlaneNotFound, "no ControlPlane src"))
		if getBackup(t, ns, "b").Status.StartedAt != nil {
			t.Fatal("a Backup of no ControlPlane started")
		}
	})
	t.Run("ControlPlaneNotFound, replaced", func(t *testing.T) {
		src := newSource(t, nil, nil)
		r := src.reconciler(cached)
		createBackup(t, src.ns, "b", nil)
		started := passes(t, r, src.ns, "b", sealed)
		if err := k8s.Delete(t.Context(), getControlPlane(t, src.ns, sourceName)); err != nil {
			t.Fatal(err)
		}
		eventually(t, func() error {
			return gone(k8s.Get(t.Context(), client.ObjectKey{Namespace: src.ns, Name: sourceName}, &lodiav1alpha1.ControlPlane{}))
		})
		create(t, controlPlane(src.ns, sourceName, src.ns))
		replacement := getControlPlane(t, src.ns, sourceName)
		passes(t, r, src.ns, "b", readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonControlPlaneNotFound,
			fmt.Sprintf("ControlPlane src has UID %s, not %s, which this Backup started from", replacement.UID, started.Status.Source.UID)))
		unmoved(t, r, src.ns, "b")
	})
	t.Run("WaitingForDatastore", func(t *testing.T) {
		ns, credentialsNamespace := namespace(t), namespace(t)
		create(t, credentials(credentialsNamespace))
		createClass(t, fullClass(ns, credentialsNamespace))
		create(t, controlPlane(ns, sourceName, ns))
		r := (&source{ns: ns}).reconciler(cached)
		createBackup(t, ns, "b", nil)
		passes(t, r, ns, "b", readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "DatastoreReady of src is False (WaitingForCertificates)"))
		if getBackup(t, ns, "b").Status.StartedAt != nil {
			t.Fatal("a Backup of a ControlPlane without a ready etcd started")
		}
	})
	t.Run("WaitingForDatastore, an older generation", func(t *testing.T) {
		src := newSource(t, nil, nil)
		r := src.reconciler(cached)
		setPaused(t, src.ns, sourceName, true)
		cp := getControlPlane(t, src.ns, sourceName)
		ready := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
		if ready.Status != metav1.ConditionTrue || ready.ObservedGeneration == cp.Generation {
			t.Fatalf("DatastoreReady is %s for generation %d of %d", ready.Status, ready.ObservedGeneration, cp.Generation)
		}
		created := createBackup(t, src.ns, "b", nil)
		passes(t, r, src.ns, "b", readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, fmt.Sprintf("DatastoreReady of src is True (Ready) for generation %d", ready.ObservedGeneration)))
		if getBackup(t, src.ns, "b").Status.StartedAt != nil || src.sealKey(t, created.UID) != nil {
			t.Fatal("a Backup of a ControlPlane whose DatastoreReady describes an older generation started")
		}
	})
	t.Run("WaitingForDatastore, a foreign Etcd", func(t *testing.T) {
		src := &source{ns: namespace(t), etcd: testserver.RunEtcd(t, nil)}
		create(t, &druidv1alpha1.Etcd{
			ObjectMeta: metav1.ObjectMeta{Name: "src-etcd", Namespace: src.ns, Labels: map[string]string{lodiav1alpha1.ControlPlaneLabel: sourceName}},
			Spec: druidv1alpha1.EtcdSpec{Replicas: 1, Labels: map[string]string{"app": "etcd"}, Backup: druidv1alpha1.BackupSpec{
				Store: &druidv1alpha1.StoreSpec{Provider: new(druidv1alpha1.StorageProvider("S3")), Container: new("lodia-backups"), Prefix: "prod/another/controlplanes/another"},
			}},
		})
		paused := controlPlane(src.ns, sourceName, src.ns)
		paused.Spec.Paused = true
		create(t, paused)
		meta.SetStatusCondition(&paused.Status.Conditions, metav1.Condition{
			Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, Message: "written by the test", ObservedGeneration: paused.Generation,
		})
		if err := k8s.Status().Update(t.Context(), paused); err != nil {
			t.Fatal(err)
		}
		r := src.reconciler(cached)
		created := createBackup(t, src.ns, "b", nil)
		passes(t, r, src.ns, "b", readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "ControlPlane src controls no Etcd src-etcd"))
		if getBackup(t, src.ns, "b").Status.StartedAt != nil || src.sealKeys(t) != 0 || getFlush(t, src.ns, created.UID) != nil {
			t.Fatal("a Backup started on an Etcd its source does not control")
		}
	})
	t.Run("Failed, the copy", func(t *testing.T) {
		src := newSource(t, nil, nil)
		r := src.reconciler(cached)
		backup := src.toCopy(t, r)
		finishCopy(t, src.ns, task(backup.UID), druidv1alpha1.EtcdCopyBackupsTaskFailed, "BackoffLimitExceeded: the Job has reached the specified backoff limit")
		passes(t, r, src.ns, "b", readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonFailed, "the copy failed: BackoffLimitExceeded: the Job has reached the specified backoff limit; create another Backup"))
		unmoved(t, r, src.ns, "b")
		if getCopyTask(t, src.ns, backup.UID) == nil {
			t.Fatal("the failed copy task is gone")
		}
	})
	t.Run("Failed, the source's keys", func(t *testing.T) {
		src := newSource(t, nil, nil)
		r := src.reconciler(cached)
		backup := src.toCopy(t, r)
		if err := k8s.Delete(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: src.ns, Name: "src-encryption-config"}}); err != nil {
			t.Fatal(err)
		}
		finishCopy(t, src.ns, task(backup.UID), druidv1alpha1.EtcdCopyBackupsTaskSucceeded, "")
		passes(t, r, src.ns, "b", readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonFailed, "the source's encryption keys could not be read after the copy"))
		unmoved(t, r, src.ns, "b")
	})
}

func finalBackup(t *testing.T, ns string, r *datastore.BackupReconciler, check func(*lodiav1alpha1.Backup) error) {
	t.Helper()
	createBackup(t, ns, "b", nil)
	if backup := passes(t, r, ns, "b", check); backup.Status.StartedAt != nil {
		t.Fatalf("the Backup started: %+v", backup.Status)
	}
	unmoved(t, r, ns, "b")
}

func unmoved(t *testing.T, r *datastore.BackupReconciler, ns, name string) {
	t.Helper()
	before := getBackup(t, ns, name)
	for range 3 {
		if _, err := reconcileBackup(r, ns, name); err != nil {
			t.Fatal(err)
		}
	}
	if after := getBackup(t, ns, name); after.ResourceVersion != before.ResourceVersion {
		t.Fatalf("a final Backup moved: %+v", after.Status)
	}
}

func TestASealThatDidNotComeBack(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	other := createBackup(t, src.ns, "another-uid", map[string]string{lodiav1alpha1.SealedUIDAnnotation: "0b6c3a51-7c8e-4f3c-9d55-2e2f1f3c4a10"})
	own := createBackup(t, src.ns, "own-uid", nil)
	patch := client.MergeFrom(own.DeepCopy())
	metav1.SetMetaDataAnnotation(&own.ObjectMeta, lodiav1alpha1.SealedUIDAnnotation, string(own.UID))
	if err := k8s.Patch(t.Context(), own, patch); err != nil {
		t.Fatal(err)
	}
	for _, backup := range []*lodiav1alpha1.Backup{other, own} {
		passes(t, r, src.ns, backup.Name, readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonSealLost,
			"the Backup was sealed as "+backup.Annotations[lodiav1alpha1.SealedUIDAnnotation]+"; its status, which says where the copy is, did not come back with it, so it is never sealed again"))
		unmoved(t, r, src.ns, backup.Name)
		if got := getBackup(t, src.ns, backup.Name).Status; got.StartedAt != nil || got.Seal != nil {
			t.Errorf("%s started: %+v", backup.Name, got)
		}
		if getFlush(t, src.ns, backup.UID) != nil || getCopyTask(t, src.ns, backup.UID) != nil {
			t.Errorf("%s has a task", backup.Name)
		}
	}
	if n := src.sealKeys(t); n != 0 {
		t.Fatalf("the source's etcd holds %d seal keys", n)
	}
}

func TestACompletedBackupMarkedWithAnotherUIDIsSealLost(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	created := createBackup(t, src.ns, "restored", nil)
	backup := passes(t, r, src.ns, created.Name, func(b *lodiav1alpha1.Backup) error {
		src.actAsDruid(t)
		if err := readyIs(metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted, "")(b); err != nil {
			return err
		}
		if b.Annotations[lodiav1alpha1.SealedUIDAnnotation] != string(b.UID) || getFlush(t, src.ns, b.UID) != nil || getCopyTask(t, src.ns, b.UID) != nil {
			return fmt.Errorf("not tidied: annotations %v", b.Annotations)
		}
		return nil
	})
	key := src.sealKey(t, created.UID)
	const mark = "0b6c3a51-7c8e-4f3c-9d55-2e2f1f3c4a10"
	patch := client.MergeFrom(backup.DeepCopy())
	metav1.SetMetaDataAnnotation(&backup.ObjectMeta, lodiav1alpha1.SealedUIDAnnotation, mark)
	if err := k8s.Patch(t.Context(), backup, patch); err != nil {
		t.Fatal(err)
	}
	passes(t, r, src.ns, created.Name, readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonSealLost, "the Backup was sealed as "+mark))
	unmoved(t, r, src.ns, created.Name)
	if getFlush(t, src.ns, created.UID) != nil || getCopyTask(t, src.ns, created.UID) != nil {
		t.Error("a task of the Backup is back")
	}
	if again := src.sealKey(t, created.UID); src.sealKeys(t) != 1 || again.GetModRevision() != key.GetModRevision() {
		t.Errorf("the source's etcd holds %d seal keys, this one written at %d, first at %d", src.sealKeys(t), again.GetModRevision(), key.GetModRevision())
	}
}

func TestAnOwnerlessKeySecretIsNotAdopted(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	ownerless := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "b.encryption-config", Namespace: src.ns},
		Data:       map[string][]byte{lodiav1alpha1.EncryptionConfigKey: []byte("not the source's")},
	}
	create(t, ownerless)
	backup := src.toCopy(t, r)
	finishCopy(t, src.ns, task(backup.UID), druidv1alpha1.EtcdCopyBackupsTaskSucceeded, "")
	passes(t, r, src.ns, "b", readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere,
		"Secret b.encryption-config exists and it has no controller; the backup reconciler writes it only where none exists: delete it"))
	if result, err := reconcileBackup(r, src.ns, "b"); err != nil || result.RequeueAfter != time.Minute {
		t.Fatalf("a pass held by the Secret returned %+v, %v; want a requeue after a minute", result, err)
	}
	if after := (&corev1.Secret{}); k8s.Get(t.Context(), client.ObjectKeyFromObject(ownerless), after) != nil || after.ResourceVersion != ownerless.ResourceVersion {
		t.Fatalf("the ownerless Secret was written: %+v", after.ObjectMeta)
	}

	if err := k8s.Delete(t.Context(), ownerless); err != nil {
		t.Fatal(err)
	}
	passes(t, r, src.ns, "b", readyIs(metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted, ""))
}

func TestASealTheEtcdRefuses(t *testing.T) {
	cfg := testserver.NewTestConfig(t)
	cfg.QuotaBackendBytes = 1 << 20
	src := newSource(t, cfg, nil)
	for i := 0; ; i++ {
		_, err := src.etcd.Put(t.Context(), fmt.Sprintf("/filler/%d", i), strings.Repeat("x", 256<<10))
		if err != nil {
			break
		}
	}
	r := src.reconciler(cached)
	created := createBackup(t, src.ns, "refused", nil)
	passes(t, r, src.ns, created.Name, readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotting,
		`etcd of src refuses the seal: etcdserver: mvcc: database space exceeded; see "etcd space quota" in docs/operations.md`))
	if result, err := reconcileBackup(r, src.ns, created.Name); err != nil || result.RequeueAfter != time.Minute {
		t.Fatalf("a refused seal returned %+v, %v; want a requeue after a minute", result, err)
	}
	if getBackup(t, src.ns, created.Name).Status.Seal != nil {
		t.Fatal("a refused seal was recorded")
	}
}

func waitBackupReady(t *testing.T, ns, name string, status metav1.ConditionStatus, reason, message string) {
	t.Helper()
	eventually(t, func() error {
		cp := getControlPlane(t, ns, name)
		condition := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionBackupReady)
		switch {
		case condition == nil:
			return errors.New("no BackupReady")
		case condition.Status != status || condition.Reason != reason || !strings.Contains(condition.Message, message):
			return fmt.Errorf("want BackupReady %s/%s with %q, got %s/%s %q", status, reason, message, condition.Status, condition.Reason, condition.Message)
		case condition.ObservedGeneration != cp.Generation:
			return fmt.Errorf("observedGeneration %d, generation %d", condition.ObservedGeneration, cp.Generation)
		}
		return nil
	})
}

func druidBackupReady(status druidv1alpha1.ConditionStatus, reason, message string) func(*druidv1alpha1.EtcdStatus) {
	return func(s *druidv1alpha1.EtcdStatus) {
		s.Conditions = append(s.Conditions, druidv1alpha1.Condition{
			Type: druidv1alpha1.ConditionTypeBackupReady, Status: status, Reason: reason, Message: message, LastTransitionTime: metav1.Now(), LastUpdateTime: metav1.Now(),
		})
	}
}

func TestBackupReadyFollowsEtcdDruid(t *testing.T) {
	ns, credentialsNamespace := namespace(t), namespace(t)
	create(t, credentials(credentialsNamespace))
	createClass(t, fullClass(ns, credentialsNamespace))
	createClass(t, newClass(ns+"-plain", "Retain"))
	create(t, controlPlane(ns, "backed", ns))
	create(t, controlPlane(ns, "plain", ns+"-plain"))
	shared := controlPlane(ns, "shared", "")
	shared.Spec.Datastore = lodiav1alpha1.DatastoreChoice{Shared: &lodiav1alpha1.SharedDatastore{Name: "pool"}}
	create(t, shared)

	waitBackupReady(t, ns, "shared", metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported, "spec.datastore.shared is not supported yet")
	waitBackupReady(t, ns, "backed", metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "this ControlPlane has no Etcd yet")
	writeGeneration(t, ns, "plain", 1)
	waitBackupReady(t, ns, "plain", metav1.ConditionFalse, lodiav1alpha1.ReasonNoBackupStore, "Etcd plain-etcd has no backup store: its class has none")

	writeGeneration(t, ns, "backed", 1)
	waitEtcd(t, ns, "backed", namesGeneration(1))
	readyOn(t, ns, "backed", 1)
	waitBackupReady(t, ns, "backed", metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotsPending, "etcd-druid reports no BackupReady of Etcd backed-etcd yet")
	for _, row := range []struct {
		druid   druidv1alpha1.ConditionStatus
		status  metav1.ConditionStatus
		reason  string
		message string
	}{
		{druidv1alpha1.ConditionUnknown, metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotsPending, "etcd-druid reports BackupReady Unknown (NotChecked) of Etcd backed-etcd: "},
		{druidv1alpha1.ConditionTrue, metav1.ConditionTrue, lodiav1alpha1.ReasonBackupSucceeded, "etcd-druid reports of Etcd backed-etcd: "},
		{druidv1alpha1.ConditionFalse, metav1.ConditionFalse, lodiav1alpha1.ReasonBackupFailed, "etcd-druid reports of Etcd backed-etcd: "},
	} {
		message := "snapshots are " + string(row.druid)
		setEtcdStatus(t, ns, "backed", druidBackupReady(row.druid, map[bool]string{true: "NotChecked", false: "Backup" + string(row.druid)}[row.druid == druidv1alpha1.ConditionUnknown], message))
		waitBackupReady(t, ns, "backed", row.status, row.reason, row.message+message)
	}

	setEtcdStatus(t, ns, "backed", druidBackupReady(druidv1alpha1.ConditionTrue, "BackupSucceeded", "Snapshot backup succeeded"))
	waitBackupReady(t, ns, "backed", metav1.ConditionTrue, lodiav1alpha1.ReasonBackupSucceeded, "Snapshot backup succeeded")
	setEtcdStatus(t, ns, "backed", func(s *druidv1alpha1.EtcdStatus) {
		druidBackupReady(druidv1alpha1.ConditionTrue, "BackupSucceeded", "Snapshot backup succeeded")(s)
		s.Ready, s.ReadyReplicas, s.Members[0].Status = new(false), 0, druidv1alpha1.EtcdMemberStatusNotReady
	})
	waitCondition(t, ns, "backed", metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost, "has 0 of 1 members ready")
	waitBackupReady(t, ns, "backed", metav1.ConditionTrue, lodiav1alpha1.ReasonBackupSucceeded, "Snapshot backup succeeded")
	var managers []string
	for _, entry := range getControlPlane(t, ns, "backed").ManagedFields {
		if entry.Subresource == "status" && strings.Contains(string(entry.FieldsV1.GetRawBytes()), `"k:{\"type\":\"BackupReady\"}"`) {
			managers = append(managers, entry.Manager)
		}
	}
	if !reflect.DeepEqual(managers, []string{datastore.FieldManager}) {
		t.Errorf("BackupReady is written by %v, want %s alone", managers, datastore.FieldManager)
	}
}

func TestTheCopyFollowsTheSourcesEtcdNotItsClass(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	etcd := getEtcd(t, src.ns, sourceName)
	if err := k8s.Delete(t.Context(), credentials(src.credentials)); err != nil {
		t.Fatal(err)
	}
	settle(t, src.ns, sourceName)
	waitCondition(t, src.ns, sourceName, metav1.ConditionTrue, lodiav1alpha1.ReasonReady, "DatastoreClass "+src.ns+" is held")
	class := &lodiav1alpha1.DatastoreClass{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: src.ns}, class); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(class.DeepCopy())
	class.Spec.Backup.Store.Prefix, class.Spec.Backup.Store.Bucket = "moved", "other-bucket"
	if err := k8s.Patch(t.Context(), class, patch); err != nil {
		t.Fatal(err)
	}
	backup := src.toCopy(t, r)
	prefix := "prod/" + string(namespaceUID(t, src.ns)) + "/backups/" + string(backup.UID)
	if got := backup.Status.Snapshots; got.Bucket != "lodia-backups" || got.Prefix != prefix {
		t.Errorf("status.snapshots is %+v, want the Etcd's bucket lodia-backups and the prefix %s", got, prefix)
	}
	copyTask := getCopyTask(t, src.ns, backup.UID)
	if got := copyTask.Spec.SourceStore; got.Prefix != etcd.Spec.Backup.Store.Prefix || *got.Container != "lodia-backups" {
		t.Errorf("the copy's source is %s/%s, want the Etcd's lodia-backups/%s", *got.Container, got.Prefix, etcd.Spec.Backup.Store.Prefix)
	}
	if got := copyTask.Spec.TargetStore; got.Prefix != prefix || *got.Container != "lodia-backups" {
		t.Errorf("the copy's target is %s/%s, want lodia-backups/%s", *got.Container, got.Prefix, prefix)
	}
}

func runBackupReconciler(t *testing.T, s *source) {
	t.Helper()
	options, err := operator.Cache()
	if err != nil {
		t.Fatal(err)
	}
	options.DefaultNamespaces = map[string]cache.Config{s.ns: {}}
	mgr, err := ctrl.NewManager(asDatastore, ctrl.Options{
		Scheme:     k8s.Scheme(),
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Cache:      options,
		Controller: config.Controller{SkipNameValidation: new(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := (&datastore.BackupReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), EtcdClient: s.dial}).SetupWithManager(mgr, 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Error(err)
		}
	})
}

func awaitBackup(t *testing.T, ns, name string, check func(*lodiav1alpha1.Backup) error) *lodiav1alpha1.Backup {
	t.Helper()
	var backup *lodiav1alpha1.Backup
	eventually(t, func() error {
		backup = getBackup(t, ns, name)
		return check(backup)
	})
	return backup
}

func TestTheWatchesBringTheNextPass(t *testing.T) {
	src := newSource(t, nil, func(s *source) {
		runBackupReconciler(t, s)
		createBackup(t, s.ns, "early", nil)
		awaitBackup(t, s.ns, "early", readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonControlPlaneNotFound, "no ControlPlane src"))
	})
	early := awaitBackup(t, src.ns, "early", func(b *lodiav1alpha1.Backup) error {
		if getFlush(t, src.ns, b.UID) == nil {
			return errors.New("no EtcdOpsTask yet")
		}
		return nil
	})
	setLease(t, src.ns, deltaLease, early.Status.Seal.Revision)
	finishFlush(t, src.ns, task(early.UID), druidv1alpha1.TaskStateSucceeded)
	awaitBackup(t, src.ns, "early", readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying, ""))
	finishCopy(t, src.ns, task(early.UID), druidv1alpha1.EtcdCopyBackupsTaskSucceeded, "")
	awaitBackup(t, src.ns, "early", readyIs(metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted, ""))

	setLease(t, src.ns, deltaLease, math.MaxInt64)
	late := createBackup(t, src.ns, "late", nil)
	awaitBackup(t, src.ns, "late", readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying, ""))
	finishCopy(t, src.ns, task(late.UID), druidv1alpha1.EtcdCopyBackupsTaskSucceeded, "")
	awaitBackup(t, src.ns, "late", readyIs(metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted, ""))

	ready := meta.FindStatusCondition(getControlPlane(t, src.ns, sourceName).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
	setPaused(t, src.ns, sourceName, true)
	createBackup(t, src.ns, "held", nil)
	awaitBackup(t, src.ns, "held", readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, fmt.Sprintf("DatastoreReady of src is True (Ready) for generation %d", ready.ObservedGeneration)))
	setPaused(t, src.ns, sourceName, false)
	awaitBackup(t, src.ns, "held", func(b *lodiav1alpha1.Backup) error {
		if b.Status.StartedAt == nil {
			return errors.New("not started")
		}
		return nil
	})
}

func editClass(t *testing.T, name string, edit func(*lodiav1alpha1.DatastoreClass)) {
	t.Helper()
	class := &lodiav1alpha1.DatastoreClass{}
	if err := k8s.Get(t.Context(), client.ObjectKey{Name: name}, class); err != nil {
		t.Fatal(err)
	}
	patch := client.MergeFrom(class.DeepCopy())
	edit(class)
	if err := k8s.Patch(t.Context(), class, patch); err != nil {
		t.Fatal(err)
	}
}

func TestAStoreMovedBeforeTheCopyFailsTheBackup(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	created := createBackup(t, src.ns, "moved", nil)
	backup := passes(t, r, src.ns, created.Name, sealed)
	setLease(t, src.ns, deltaLease, backup.Status.Seal.Revision)
	editClass(t, src.ns, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Backup.Store.Prefix, class.Spec.Backup.Store.Bucket = "moved", "other-bucket"
	})
	waitEtcd(t, src.ns, sourceName, func(etcd *druidv1alpha1.Etcd) error {
		if store := etcd.Spec.Backup.Store; *store.Container != "other-bucket" || !strings.HasPrefix(store.Prefix, "moved/") {
			return fmt.Errorf("the Etcd's store is %s/%s", *store.Container, store.Prefix)
		}
		return nil
	})
	ns := string(namespaceUID(t, src.ns))
	passes(t, r, src.ns, created.Name, readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonFailed, fmt.Sprintf(
		"the source's backup store moved from S3 https://s3.example.test lodia-backups/prod/%s to S3 https://s3.example.test other-bucket/moved/%s while the Backup was taken, and the snapshots that hold the seal are in the first; create another Backup", ns, ns)))
	unmoved(t, r, src.ns, created.Name)
	if getCopyTask(t, src.ns, created.UID) != nil {
		t.Fatal("a copy task from the moved store")
	}
}

func TestTheFlushIsFullWithoutDeltas(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	for _, row := range []struct {
		period   time.Duration
		snapshot druidv1alpha1.OnDemandSnapshotType
	}{
		{0, druidv1alpha1.OnDemandSnapshotTypeFull},
		{999 * time.Millisecond, druidv1alpha1.OnDemandSnapshotTypeFull},
		{time.Second, druidv1alpha1.OnDemandSnapshotTypeDelta},
	} {
		t.Run(row.period.String(), func(t *testing.T) {
			editClass(t, src.ns, func(class *lodiav1alpha1.DatastoreClass) {
				class.Spec.Backup.DeltaSnapshotPeriod = &metav1.Duration{Duration: row.period}
			})
			waitEtcd(t, src.ns, sourceName, func(etcd *druidv1alpha1.Etcd) error {
				if period := etcd.Spec.Backup.DeltaSnapshotPeriod; period == nil || period.Duration != row.period {
					return fmt.Errorf("the Etcd's deltaSnapshotPeriod is %v", period)
				}
				return nil
			})
			name := "period-" + strconv.FormatInt(row.period.Milliseconds(), 10)
			created := createBackup(t, src.ns, name, nil)
			passes(t, r, src.ns, name, func(*lodiav1alpha1.Backup) error {
				if getFlush(t, src.ns, created.UID) == nil {
					return errors.New("no EtcdOpsTask yet")
				}
				return nil
			})
			if got := getFlush(t, src.ns, created.UID).Spec.Config.OnDemandSnapshot.Type; got != row.snapshot {
				t.Errorf("the EtcdOpsTask asks for a %s snapshot, want %s", got, row.snapshot)
			}
		})
	}
}

func TestAStoreMovedAwayAndBackFailsTheBackup(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	created := createBackup(t, src.ns, "away", nil)
	backup := passes(t, r, src.ns, created.Name, sealed)
	moveStore(t, src.ns, "other-bucket", "moved")
	ns := string(namespaceUID(t, src.ns))
	failed := readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonFailed, fmt.Sprintf(
		"the source's backup store moved from S3 https://s3.example.test lodia-backups/prod/%s to S3 https://s3.example.test other-bucket/moved/%s while the Backup was taken", ns, ns))
	passes(t, r, src.ns, created.Name, failed)
	setLease(t, src.ns, deltaLease, backup.Status.Seal.Revision)
	moveStore(t, src.ns, "lodia-backups", "prod")
	unmoved(t, r, src.ns, created.Name)
	if err := failed(getBackup(t, src.ns, created.Name)); err != nil {
		t.Fatal(err)
	}
	if getCopyTask(t, src.ns, created.UID) != nil {
		t.Fatal("a copy task from a store moved away and back")
	}
}

func moveStore(t *testing.T, ns, bucket, prefix string) {
	t.Helper()
	editClass(t, ns, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Backup.Store.Bucket, class.Spec.Backup.Store.Prefix = bucket, prefix
	})
	waitEtcd(t, ns, sourceName, func(etcd *druidv1alpha1.Etcd) error {
		if store := etcd.Spec.Backup.Store; *store.Container != bucket || !strings.HasPrefix(store.Prefix, prefix+"/") {
			return fmt.Errorf("the Etcd's store is %s/%s", *store.Container, store.Prefix)
		}
		return nil
	})
}

type storeKeyMove struct {
	key, was, is string
	data         map[string]string
}

func TestAStoreMovedThroughTheCredentialsFailsTheBackup(t *testing.T) {
	for _, c := range []struct {
		name     string
		provider lodiav1alpha1.StoreProvider
		data     map[string]string
		want     lodiav1alpha1.StoreSecretFields
		moves    []storeKeyMove
		rotated  map[string]string
	}{{
		name:     "S3",
		provider: "S3",
		data:     map[string]string{"accessKeyID": "key-1", "secretAccessKey": "secret-1", "endpoint": "https://s3.a.test", "region": "eu-central-1"},
		want:     lodiav1alpha1.StoreSecretFields{Endpoint: new("https://s3.a.test"), Region: new("eu-central-1")},
		moves: []storeKeyMove{
			{"endpoint", `"https://s3.a.test"`, `"https://s3.b.test"`, map[string]string{"accessKeyID": "key-1", "secretAccessKey": "secret-1", "endpoint": "https://s3.b.test", "region": "eu-central-1"}},
			{"region", `"eu-central-1"`, `"cn-north-1"`, map[string]string{"accessKeyID": "key-1", "secretAccessKey": "secret-1", "endpoint": "https://s3.a.test", "region": "cn-north-1"}},
		},
		rotated: map[string]string{"accessKeyID": "key-2", "secretAccessKey": "secret-2", "endpoint": "https://s3.a.test", "region": "eu-central-1"},
	}, {
		name:     "S3 from a JSON key",
		provider: "S3",
		data:     map[string]string{"aws.json": `{"accessKeyID":"key-1","secretAccessKey":"secret-1","region":"eu-central-1"}`, "endpoint": "https://s3.ignored.test", "region": "us-east-1"},
		want:     lodiav1alpha1.StoreSecretFields{Region: new("eu-central-1")},
		moves: []storeKeyMove{
			{"endpoint", "unset", `"https://s3.b.test"`, map[string]string{"aws.json": `{"accessKeyID":"key-1","secretAccessKey":"secret-1","region":"eu-central-1","endpoint":"https://s3.b.test"}`, "endpoint": "https://s3.ignored.test", "region": "us-east-1"}},
		},
		rotated: map[string]string{"aws.json": `{"accessKeyID":"key-2","secretAccessKey":"secret-2","region":"eu-central-1"}`, "endpoint": "https://s3.ignored.test", "region": "us-east-1"},
	}, {
		name:     "GCS",
		provider: "GCS",
		data:     map[string]string{"serviceaccount.json": `{"type":"service_account","project_id":"lodia-a","private_key":"key-1","storageAPIEndpoint":"https://ignored.test"}`, "storageAPIEndpoint": "https://storage.a.test/storage/v1/"},
		want:     lodiav1alpha1.StoreSecretFields{StorageAPIEndpoint: new("https://storage.a.test/storage/v1/")},
		moves: []storeKeyMove{
			{"storageAPIEndpoint", `"https://storage.a.test/storage/v1/"`, `"https://storage.b.test/storage/v1/"`, map[string]string{"serviceaccount.json": `{"type":"service_account","project_id":"lodia-a","private_key":"key-1","storageAPIEndpoint":"https://ignored.test"}`, "storageAPIEndpoint": "https://storage.b.test/storage/v1/"}},
		},
		rotated: map[string]string{"serviceaccount.json": `{"type":"service_account","project_id":"lodia-a","private_key":"key-2"}`, "storageAPIEndpoint": "https://storage.a.test/storage/v1/"},
	}, {
		name:     "ABS",
		provider: "ABS",
		data:     map[string]string{"storageAccount": "account-a", "storageKey": "key-1"},
		want:     lodiav1alpha1.StoreSecretFields{StorageAccount: new("account-a")},
		moves: []storeKeyMove{
			{"storageAccount", `"account-a"`, `"account-b"`, map[string]string{"storageAccount": "account-b", "storageKey": "key-1"}},
			{"domain", "unset", `"blob.core.chinacloudapi.cn"`, map[string]string{"storageAccount": "account-a", "storageKey": "key-1", "domain": "blob.core.chinacloudapi.cn"}},
		},
		rotated: map[string]string{"storageAccount": "account-a", "storageKey": "key-2"},
	}, {
		name:     "Swift",
		provider: "Swift",
		data:     map[string]string{"authURL": "https://keystone.a.test/v3", "tenantName": "tenant-a", "domainName": "domain-a", "username": "lodia", "password": "password-1", "region": "RegionOne"},
		want:     lodiav1alpha1.StoreSecretFields{AuthURL: new("https://keystone.a.test/v3"), TenantName: new("tenant-a"), DomainName: new("domain-a")},
		moves: []storeKeyMove{
			{"authURL", `"https://keystone.a.test/v3"`, `"https://keystone.b.test/v3"`, map[string]string{"authURL": "https://keystone.b.test/v3", "tenantName": "tenant-a", "domainName": "domain-a", "username": "lodia", "password": "password-1", "region": "RegionOne"}},
			{"tenantName", `"tenant-a"`, `"tenant-b"`, map[string]string{"authURL": "https://keystone.a.test/v3", "tenantName": "tenant-b", "domainName": "domain-a", "username": "lodia", "password": "password-1", "region": "RegionOne"}},
			{"domainName", `"domain-a"`, `"domain-b"`, map[string]string{"authURL": "https://keystone.a.test/v3", "tenantName": "tenant-a", "domainName": "domain-b", "username": "lodia", "password": "password-1", "region": "RegionOne"}},
		},
		rotated: map[string]string{"authURL": "https://keystone.a.test/v3", "tenantName": "tenant-a", "domainName": "domain-a", "username": "lodia", "password": "password-2", "region": "RegionTwo"},
	}} {
		t.Run(c.name, func(t *testing.T) {
			src := newSource(t, nil, func(s *source) {
				create(t, credentialsSecret(s, "first", c.data))
				editClass(t, s.ns, func(class *lodiav1alpha1.DatastoreClass) {
					class.Spec.Backup.Store.Provider, class.Spec.Backup.Store.Region, class.Spec.Backup.Store.SecretRef.Name = c.provider, "", "first"
				})
			})
			r := src.reconciler(cached)
			start := func(name string) *lodiav1alpha1.Backup {
				createBackup(t, src.ns, name, nil)
				started := passes(t, r, src.ns, name, readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotting, "sealing"))
				if got := started.Status.Snapshots; got.Provider != c.provider || !reflect.DeepEqual(got.FromSecret, &c.want) {
					t.Fatalf("status.snapshots is %s with fromSecret %+v, want %s with %+v", got.Provider, got.FromSecret, c.provider, c.want)
				}
				return started
			}
			for _, move := range c.moves {
				t.Run(move.key, func(t *testing.T) {
					created := start(strings.ToLower(move.key))
					useCredentials(t, src, move.key, move.data)
					passes(t, r, src.ns, created.Name, readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonFailed, fmt.Sprintf(
						"the source's backup store moved from Secret src-etcd-backup's %s %s to %s while the Backup was taken, and the snapshots that hold the seal are in the first; create another Backup", move.key, move.was, move.is)))
					unmoved(t, r, src.ns, created.Name)
					useCredentials(t, src, "first", c.data)
				})
			}
			t.Run("keys rotated", func(t *testing.T) {
				created := start("rotated")
				useCredentials(t, src, "rotated", c.rotated)
				backup := passes(t, r, src.ns, created.Name, sealed)
				setLease(t, src.ns, deltaLease, backup.Status.Seal.Revision)
				passes(t, r, src.ns, created.Name, readyIs(metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying, ""))
				finishCopy(t, src.ns, task(created.UID), druidv1alpha1.EtcdCopyBackupsTaskSucceeded, "")
				passes(t, r, src.ns, created.Name, readyIs(metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted, ""))
			})
		})
	}
}

func credentialsSecret(s *source, name string, data map[string]string) *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: strings.ToLower(name), Namespace: s.credentials}, StringData: data}
}

func useCredentials(t *testing.T, s *source, name string, data map[string]string) {
	t.Helper()
	secret := credentialsSecret(s, name, data)
	if err := k8s.Create(t.Context(), secret); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	editClass(t, s.ns, func(class *lodiav1alpha1.DatastoreClass) {
		class.Spec.Backup.Store.SecretRef.Name = secret.Name
	})
	eventually(t, func() error {
		copied := &corev1.Secret{}
		if err := k8s.Get(t.Context(), client.ObjectKey{Namespace: s.ns, Name: sourceName + "-etcd-backup"}, copied); err != nil {
			return err
		}
		if len(copied.Data) != len(data) {
			return fmt.Errorf("%s-etcd-backup holds %d keys, want %d", sourceName, len(copied.Data), len(data))
		}
		for key, value := range data {
			if string(copied.Data[key]) != value {
				return fmt.Errorf("%s-etcd-backup holds %s %q, want %q", sourceName, key, copied.Data[key], value)
			}
		}
		return nil
	})
}

type hiding struct {
	client.Reader
	name string
}

func (h hiding) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, secret := obj.(*corev1.Secret); secret && key.Name == h.name {
		return apierrors.NewNotFound(corev1.Resource("secrets"), key.Name)
	}
	return h.Reader.Get(ctx, key, obj, opts...)
}

func TestABackupWaitsForTheCredentialsCopy(t *testing.T) {
	src := newSource(t, nil, nil)
	r := src.reconciler(cached)
	hidden := &datastore.BackupReconciler{Client: cached, Reader: hiding{apiReader, "src-etcd-backup"}, EtcdClient: src.dial}
	created := createBackup(t, src.ns, "early", nil)
	waiting := readyIs(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "no Secret src-etcd-backup, whose keys name part of the store")
	for _, sealedYet := range []bool{false, true} {
		passes(t, hidden, src.ns, created.Name, waiting)
		result, err := reconcileBackup(hidden, src.ns, created.Name)
		if err != nil || result.RequeueAfter != 10*time.Second {
			t.Fatalf("a pass without the copy returned %+v, %v; want a requeue in 10s", result, err)
		}
		if backup := getBackup(t, src.ns, created.Name); (backup.Status.StartedAt != nil) != sealedYet || (backup.Status.Seal != nil) != sealedYet {
			t.Fatalf("a pass without the copy left startedAt %v and seal %+v", backup.Status.StartedAt, backup.Status.Seal)
		}
		passes(t, r, src.ns, created.Name, sealed)
	}
}
