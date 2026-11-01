// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	BackupName         = "backup"
	BackupFieldManager = "lodia/" + BackupName

	// backupTaskPrefix names a Backup's EtcdOpsTask and EtcdCopyBackupsTask,
	// "backup-<Backup UID>": a UID is 36 characters, and the copy Job's pods
	// carry the task's name and "-worker" as a label value of at most 63
	// (etcd-druid@v0.39.0 internal/controller/etcdcopybackupstask/reconciler.go#L389-L396).
	backupTaskPrefix = "backup-"
)

// BackupReconciler seals a Backup in its source's etcd and copies the source's
// snapshots under the Backup's own prefix
// (docs/decisions/restore-replaces-the-source.md).
type BackupReconciler struct {
	client.Client
	// Reader reads the API server: the Backup in every pass, the snapshot
	// Leases and the Secrets.
	Reader client.Reader
	// EtcdClient connects to the etcd of an Etcd; the caller closes it.
	EtcdClient func(ctx context.Context, etcd *druidv1alpha1.Etcd) (*clientv3.Client, error)
}

func (r *BackupReconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(BackupName).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.Backup{}).
		Owns(&druidv1alpha1.EtcdOpsTask{}).
		Owns(&druidv1alpha1.EtcdCopyBackupsTask{}).
		Watches(&lodiav1alpha1.ControlPlane{}, handler.EnqueueRequestsFromMapFunc(r.backupsOf)).
		Complete(r)
}

// A Backup that is Ready waits for nothing from its source.
func (r *BackupReconciler) backupsOf(ctx context.Context, cp client.Object) []reconcile.Request {
	backups := &lodiav1alpha1.BackupList{}
	if err := r.List(ctx, backups, client.InNamespace(cp.GetNamespace())); err != nil {
		logf.FromContext(ctx).Error(err, "list the Backups of a ControlPlane", "controlPlane", cp.GetName())
		return nil
	}
	var requests []reconcile.Request
	for i := range backups.Items {
		backup := &backups.Items[i]
		if backup.Spec.ControlPlaneRef.Name == cp.GetName() && !meta.IsStatusConditionTrue(backup.Status.Conditions, lodiav1alpha1.ConditionReady) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(backup)})
		}
	}
	return requests
}

// +kubebuilder:rbac:groups=lodia.io,resources=backups,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=lodia.io,resources=backups/status,verbs=patch
// +kubebuilder:rbac:groups=lodia.io,resources=backups/finalizers,verbs=update
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get
// +kubebuilder:rbac:groups=druid.gardener.cloud,resources=etcdopstasks;etcdcopybackupstasks,verbs=get;list;watch;create;delete

// Reconcile takes the steps of a Backup in order, and each pass writes at
// most one status, with the resourceVersion of the Backup it read, so that a
// write computed from a stale copy is refused and drops no recorded field.
func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	backup := &lodiav1alpha1.Backup{}
	if err := r.Reader.Get(ctx, req.NamespacedName, backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !backup.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	status := backup.Status.DeepCopy()
	report := func(conditionStatus metav1.ConditionStatus, reason, message string, requeueAfter time.Duration) (ctrl.Result, error) {
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type: lodiav1alpha1.ConditionReady, Status: conditionStatus, Reason: reason, Message: message, ObservedGeneration: backup.Generation,
		})
		conflict, err := r.applyStatus(ctx, backup, status)
		switch {
		case err != nil:
			return ctrl.Result{}, err
		case conflict:
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}
	ready := meta.FindStatusCondition(backup.Status.Conditions, lodiav1alpha1.ConditionReady)
	completed := ready != nil && ready.Status == metav1.ConditionTrue && ready.Reason == lodiav1alpha1.ReasonCompleted
	mark, marked := backup.Annotations[lodiav1alpha1.SealedUIDAnnotation]
	switch {
	case completed && (!marked || mark == string(backup.UID)):
		return r.tidy(ctx, backup, marked)
	case marked:
		return report(metav1.ConditionFalse, lodiav1alpha1.ReasonSealLost, fmt.Sprintf("the Backup was sealed as %s; its status, which says where the copy is, did not come back with it, so it is never sealed again: restore the management cluster from a backup that keeps UIDs and status", mark), 0)
	case final(backup, ready):
		return ctrl.Result{}, nil
	}

	name := backup.Spec.ControlPlaneRef.Name
	source := &lodiav1alpha1.ControlPlane{}
	err := r.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: name}, source)
	switch {
	case apierrors.IsNotFound(err):
		return report(metav1.ConditionFalse, lodiav1alpha1.ReasonControlPlaneNotFound, "no ControlPlane "+name, 0)
	case err != nil:
		return ctrl.Result{}, err
	case status.Source != nil && status.Source.UID != source.UID:
		return report(metav1.ConditionFalse, lodiav1alpha1.ReasonControlPlaneNotFound, fmt.Sprintf("ControlPlane %s has UID %s, not %s, which this Backup started from", name, source.UID, status.Source.UID), 0)
	case source.Spec.Datastore.Dedicated == nil:
		return report(metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported, "ControlPlane "+name+" has spec.datastore.shared, which a Backup does not support yet", 0)
	}
	etcd, err := r.sourceEtcd(ctx, source)
	if err != nil {
		return ctrl.Result{}, err
	}
	if etcd != nil && etcd.Spec.Backup.Store == nil {
		return report(metav1.ConditionFalse, lodiav1alpha1.ReasonNoBackupStore, fmt.Sprintf("the ControlPlane's class %s has no backup store", etcd.Annotations[lodiav1alpha1.DatastoreClassAnnotation]), 0)
	}
	if waiting := sealable(source, etcd); waiting != "" {
		return report(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, waiting, 0)
	}
	store := etcd.Spec.Backup.Store

	if status.StartedAt == nil {
		status.Source = &lodiav1alpha1.UIDReference{Name: source.Name, UID: source.UID}
		if source.Status.Kubernetes != nil && source.Status.Kubernetes.Version != "" {
			status.KubernetesVersion = source.Status.Kubernetes.Version
		} else if source.Status.Target != nil {
			status.KubernetesVersion = source.Status.Target.Version
		}
		status.Network = source.Spec.Network.DeepCopy()
		status.Snapshots = &lodiav1alpha1.SnapshotLocation{
			Provider: lodiav1alpha1.StoreProvider(ptr.Deref(store.Provider, "")),
			Endpoint: ptr.Deref(store.EndpointOverride, ""),
			Bucket:   ptr.Deref(store.Container, ""),
			Prefix:   backupPrefix(store.Prefix, backup.UID),
		}
		status.StartedAt = new(metav1.Now())
		return report(metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotting, "sealing", 0)
	}

	if status.Seal == nil {
		revision, err := r.seal(ctx, etcd, backup.UID)
		if errors.Is(err, rpctypes.ErrNoSpace) {
			return report(metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotting, fmt.Sprintf(`etcd of %s refuses the seal: %v; see "etcd space quota" in docs/operations.md`, name, err), time.Minute)
		}
		if err != nil {
			return ctrl.Result{}, err
		}
		status.Seal = &lodiav1alpha1.BackupSeal{Revision: revision}
		if quota := etcd.Spec.Etcd.Quota; quota != nil {
			status.Seal.QuotaBackendBytes = new(quota.DeepCopy())
		}
		return report(metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotting, fmt.Sprintf("the seal is at revision %d in %s's etcd", revision, name), 0)
	}

	task := backupTaskPrefix + string(backup.UID)
	if message, err := r.flush(ctx, backup, source, etcd, status.Seal.Revision); message != "" || err != nil {
		if err != nil {
			return ctrl.Result{}, err
		}
		return report(metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotting, message, 10*time.Second)
	}

	copyTask := &druidv1alpha1.EtcdCopyBackupsTask{}
	err = r.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: task}, copyTask)
	destination := status.Snapshots.Bucket + "/" + status.Snapshots.Prefix
	copying := fmt.Sprintf("EtcdCopyBackupsTask %s copies the newest snapshots of %s to %s", task, name, destination)
	switch {
	case apierrors.IsNotFound(err):
		// The copy goes where the status says it is, which a restore reads,
		// even if the source's store moved since the Backup started.
		target := store.DeepCopy()
		target.Provider = new(druidv1alpha1.StorageProvider(status.Snapshots.Provider))
		target.Container = new(status.Snapshots.Bucket)
		target.EndpointOverride = nil
		if status.Snapshots.Endpoint != "" {
			target.EndpointOverride = new(status.Snapshots.Endpoint)
		}
		target.Prefix = status.Snapshots.Prefix
		copyTask = &druidv1alpha1.EtcdCopyBackupsTask{
			ObjectMeta: taskMeta(backup, source, task),
			Spec:       druidv1alpha1.EtcdCopyBackupsTaskSpec{SourceStore: *store.DeepCopy(), TargetStore: *target, MaxBackups: new(uint32(1))},
		}
		if err := r.Create(ctx, copyTask, client.FieldOwner(BackupFieldManager)); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, fmt.Errorf("create EtcdCopyBackupsTask %s: %w", task, err)
		}
		return report(metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying, copying, 0)
	case err != nil:
		return ctrl.Result{}, err
	}
	if failed := copyCondition(copyTask, druidv1alpha1.EtcdCopyBackupsTaskFailed); failed != nil {
		why := failed.Message
		if why == "" {
			why = ptr.Deref(copyTask.Status.LastError, failed.Reason)
		}
		return report(metav1.ConditionFalse, lodiav1alpha1.ReasonFailed, "the copy failed: "+why+"; create another Backup", 0)
	}
	if copyCondition(copyTask, druidv1alpha1.EtcdCopyBackupsTaskSucceeded) == nil {
		return report(metav1.ConditionUnknown, lodiav1alpha1.ReasonCopying, copying, 0)
	}

	if reason, message, err := r.writeKeys(ctx, backup, source); reason != "" || err != nil {
		if err != nil {
			return ctrl.Result{}, err
		}
		var requeueAfter time.Duration
		if reason == lodiav1alpha1.ReasonObjectsControlledElsewhere {
			requeueAfter = time.Minute
		}
		return report(metav1.ConditionFalse, reason, message, requeueAfter)
	}
	status.CompletedAt = new(metav1.Now())
	return report(metav1.ConditionTrue, lodiav1alpha1.ReasonCompleted, fmt.Sprintf("sealed at revision %d and copied to %s", status.Seal.Revision, destination), 0)
}

// final says whether the Backup's Ready says it is over: a ControlPlane gone
// once the Backup started counts, one that never existed is awaited.
func final(backup *lodiav1alpha1.Backup, ready *metav1.Condition) bool {
	if ready == nil || ready.Status != metav1.ConditionFalse {
		return false
	}
	switch ready.Reason {
	case lodiav1alpha1.ReasonUnsupported, lodiav1alpha1.ReasonNoBackupStore, lodiav1alpha1.ReasonFailed:
		return true
	case lodiav1alpha1.ReasonControlPlaneNotFound:
		return backup.Status.StartedAt != nil
	}
	return false
}

func (r *BackupReconciler) sourceEtcd(ctx context.Context, source *lodiav1alpha1.ControlPlane) (*druidv1alpha1.Etcd, error) {
	etcd := &druidv1alpha1.Etcd{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: source.Namespace, Name: lodiav1alpha1.EtcdName(source.Name)}, etcd); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(etcd, source) {
		return nil, nil
	}
	return etcd, nil
}

func sealable(source *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd) string {
	ready := meta.FindStatusCondition(source.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
	switch {
	case !source.DeletionTimestamp.IsZero():
		return "ControlPlane " + source.Name + " is being deleted"
	case ready == nil:
		return "DatastoreReady of " + source.Name + " is not written yet"
	case ready.Status != metav1.ConditionTrue || ready.ObservedGeneration != source.Generation:
		return fmt.Sprintf("DatastoreReady of %s is %s (%s) for generation %d", source.Name, ready.Status, ready.Reason, ready.ObservedGeneration)
	case etcd == nil:
		return fmt.Sprintf("ControlPlane %s controls no Etcd %s", source.Name, lodiav1alpha1.EtcdName(source.Name))
	}
	return ""
}

// backupPrefix puts the Backup beside the ControlPlanes of the namespace in
// the store: <class prefix>/<namespace UID>/backups/<Backup UID> for the
// source's <class prefix>/<namespace UID>/controlplanes/<ControlPlane UID>.
func backupPrefix(source string, backup types.UID) string {
	return path.Join(path.Dir(path.Dir(source)), "backups", string(backup))
}

// seal puts the Backup's key into the source's etcd once and returns the
// revision that created it: a retry finds the key and reads that revision
// instead of moving it on.
func (r *BackupReconciler) seal(ctx context.Context, etcd *druidv1alpha1.Etcd, uid types.UID) (int64, error) {
	c, err := r.EtcdClient(ctx, etcd)
	if err != nil {
		return 0, fmt.Errorf("connect to the etcd of Etcd %s: %w", etcd.Name, err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(ctx, etcdRequestTimeout)
	defer cancel()
	key := lodiav1alpha1.BackupSealKey(uid)
	response, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(uid))).
		Else(clientv3.OpGet(key)).
		Commit()
	switch {
	case err != nil:
		return 0, err
	case response.Succeeded:
		return response.Header.GetRevision(), nil
	}
	kvs := response.Responses[0].GetResponseRange().GetKvs()
	if len(kvs) == 0 {
		return 0, fmt.Errorf("the seal %s is neither absent nor present in the etcd of Etcd %s", key, etcd.Name)
	}
	return kvs[0].GetCreateRevision(), nil
}

// flush asks etcd-druid for a snapshot until a snapshot Lease shows the seal
// in the store, and says what it waits for until then. A finished
// EtcdOpsTask proves nothing, since a delta the sidecar skipped succeeds too
// (etcd-druid@v0.39.0 internal/controller/etcdopstask/handler/ondemandsnapshot/ondemandsnapshot.go#L135-L153),
// so one that finished short of the seal is deleted for the next.
func (r *BackupReconciler) flush(ctx context.Context, backup *lodiav1alpha1.Backup, source *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd, revision int64) (string, error) {
	leases := []string{druidv1alpha1.GetFullSnapshotLeaseName(etcd.ObjectMeta), druidv1alpha1.GetDeltaSnapshotLeaseName(etcd.ObjectMeta)}
	held, ignored, err := revisionInStore(ctx, r.Reader, etcd, leases...)
	if err != nil || held >= revision {
		return "", err
	}
	name := backupTaskPrefix + string(backup.UID)
	task := &druidv1alpha1.EtcdOpsTask{}
	var state string
	err = r.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: name}, task)
	switch {
	case apierrors.IsNotFound(err):
		snapshot := druidv1alpha1.OnDemandSnapshotTypeDelta
		if period := etcd.Spec.Backup.DeltaSnapshotPeriod; period != nil && period.Duration < time.Second {
			snapshot = druidv1alpha1.OnDemandSnapshotTypeFull
		}
		task = &druidv1alpha1.EtcdOpsTask{
			ObjectMeta: taskMeta(backup, source, name),
			Spec: druidv1alpha1.EtcdOpsTaskSpec{
				Config:   druidv1alpha1.EtcdOpsTaskConfig{OnDemandSnapshot: &druidv1alpha1.OnDemandSnapshotConfig{Type: snapshot}},
				EtcdName: new(etcd.Name),
			},
		}
		if err := r.Create(ctx, task, client.FieldOwner(BackupFieldManager)); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("create EtcdOpsTask %s: %w", name, err)
		}
		state = "created"
	case err != nil:
		return "", err
	case task.IsCompleted():
		if err := client.IgnoreNotFound(r.Delete(ctx, task, client.Preconditions{UID: &task.UID})); err != nil {
			return "", fmt.Errorf("delete EtcdOpsTask %s: %w", name, err)
		}
		state = string(*task.Status.State) + ", deleted for another"
	default:
		state = string(ptr.Deref(task.Status.State, "not admitted yet"))
	}
	message := []string{fmt.Sprintf("the seal is at revision %d in %s's etcd; the store holds %d (Leases %s); EtcdOpsTask %s is %s",
		revision, source.Name, held, strings.Join(leases, ", "), name, state)}
	for _, lease := range ignored {
		message = append(message, fmt.Sprintf("ignored Lease %s, created before Etcd %s", lease, etcd.Name))
	}
	if backupReady := meta.FindStatusCondition(source.Status.Conditions, lodiav1alpha1.ConditionBackupReady); backupReady != nil && backupReady.Status == metav1.ConditionFalse {
		message = append(message, fmt.Sprintf("BackupReady of %s is False: %s", source.Name, backupReady.Message))
	}
	return strings.Join(message, "; "), nil
}

// revisionInStore is the newest revision the snapshot Leases of the Etcd
// show in its store. The sidecar sets a Lease's holder to the last revision
// of a snapshot it uploaded and never lowers it; the full snapshot covers
// every revision up to its holder, and the deltas after it are contiguous
// (etcd-backup-restore@v0.45.1 pkg/health/heartbeat/heartbeat.go#L200-L289).
// etcd-druid adopts a Lease of a predecessor of the same name with its holder,
// so a Lease created before the Etcd counts for nothing; the second result
// names those.
func revisionInStore(ctx context.Context, reader client.Reader, etcd *druidv1alpha1.Etcd, names ...string) (int64, []string, error) {
	var held int64
	var ignored []string
	for _, name := range names {
		lease := &coordinationv1.Lease{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: etcd.Namespace, Name: name}, lease); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return 0, nil, fmt.Errorf("read Lease %s: %w", name, err)
		}
		if lease.CreationTimestamp.Before(&etcd.CreationTimestamp) {
			ignored = append(ignored, name)
			continue
		}
		if revision, err := strconv.ParseInt(ptr.Deref(lease.Spec.HolderIdentity, ""), 10, 64); err == nil {
			held = max(held, revision)
		}
	}
	return held, ignored, nil
}

func taskMeta(backup *lodiav1alpha1.Backup, source *lodiav1alpha1.ControlPlane, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            name,
		Namespace:       backup.Namespace,
		Labels:          map[string]string{lodiav1alpha1.ControlPlaneLabel: source.Name},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(backup, lodiav1alpha1.GroupVersion.WithKind("Backup"))},
	}
}

func copyCondition(task *druidv1alpha1.EtcdCopyBackupsTask, conditionType druidv1alpha1.ConditionType) *druidv1alpha1.Condition {
	for i := range task.Status.Conditions {
		if c := &task.Status.Conditions[i]; c.Type == conditionType && c.Status == druidv1alpha1.ConditionTrue {
			return c
		}
	}
	return nil
}

// writeKeys writes <backup>.encryption-config from the source's encryption
// configuration as it is after the copy: PKI keeps every provider it ever
// wrote there, so it holds every key that encrypted anything in the copy. It
// is created, never applied, so that it adopts nothing; one that exists is
// either the Backup's, immutable, or another owner's. A reason and its
// message say why the keys are not written.
func (r *BackupReconciler) writeKeys(ctx context.Context, backup *lodiav1alpha1.Backup, source *lodiav1alpha1.ControlPlane) (string, string, error) {
	name := lodiav1alpha1.BackupEncryptionConfigSecretName(backup.Name)
	existing := &metav1.PartialObjectMetadata{}
	existing.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	err := r.Reader.Get(ctx, client.ObjectKey{Namespace: backup.Namespace, Name: name}, existing)
	switch {
	case err == nil && metav1.IsControlledBy(existing, backup):
		return "", "", nil
	case err == nil:
		owner := "it has no controller"
		if ref := metav1.GetControllerOfNoCopy(existing); ref != nil {
			owner = fmt.Sprintf("its controller is %s %s with UID %s", ref.Kind, ref.Name, ref.UID)
		}
		return lodiav1alpha1.ReasonObjectsControlledElsewhere, fmt.Sprintf("Secret %s exists and %s; the backup reconciler writes it only where none exists: delete it", name, owner), nil
	case !apierrors.IsNotFound(err):
		return "", "", err
	}
	keys := &corev1.Secret{}
	from := lodiav1alpha1.EncryptionConfigSecret.Name(source.Name)
	err = r.Reader.Get(ctx, client.ObjectKey{Namespace: source.Namespace, Name: from}, keys)
	if apierrors.IsNotFound(err) || err == nil && len(keys.Data[lodiav1alpha1.EncryptionConfigKey]) == 0 {
		return lodiav1alpha1.ReasonFailed, "the source's encryption keys could not be read after the copy: Secret " + from + " holds no " + lodiav1alpha1.EncryptionConfigKey + "; create another Backup", nil
	}
	if err != nil {
		return "", "", err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       backup.Namespace,
			Labels:          map[string]string{lodiav1alpha1.ControlPlaneLabel: source.Name},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(backup, lodiav1alpha1.GroupVersion.WithKind("Backup"))},
		},
		Type:      corev1.SecretTypeOpaque,
		Immutable: new(true),
		Data:      map[string][]byte{lodiav1alpha1.EncryptionConfigKey: keys.Data[lodiav1alpha1.EncryptionConfigKey]},
	}
	if err := r.Create(ctx, secret, client.FieldOwner(BackupFieldManager)); err != nil {
		return "", "", fmt.Errorf("create Secret %s: %w", name, err)
	}
	return "", "", nil
}

// tidy marks a Completed Backup with its UID, then deletes its tasks: a
// status lost afterwards can no longer pass for a Backup never sealed.
func (r *BackupReconciler) tidy(ctx context.Context, backup *lodiav1alpha1.Backup, marked bool) (ctrl.Result, error) {
	if !marked {
		patch := client.MergeFromWithOptions(backup.DeepCopy(), client.MergeFromWithOptimisticLock{})
		metav1.SetMetaDataAnnotation(&backup.ObjectMeta, lodiav1alpha1.SealedUIDAnnotation, string(backup.UID))
		err := r.Patch(ctx, backup, patch, client.FieldOwner(BackupFieldManager))
		switch {
		case apierrors.IsConflict(err):
			return ctrl.Result{RequeueAfter: time.Second}, nil
		case err != nil:
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
	}
	key := client.ObjectKey{Namespace: backup.Namespace, Name: backupTaskPrefix + string(backup.UID)}
	for _, task := range []client.Object{&druidv1alpha1.EtcdOpsTask{}, &druidv1alpha1.EtcdCopyBackupsTask{}} {
		if err := r.Get(ctx, key, task); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return ctrl.Result{}, err
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, task, client.Preconditions{UID: new(task.GetUID())})); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// applyStatus applies the whole status, the recorded fields carried from the
// read, under the read's resourceVersion; the first result says the API
// server refused it as computed from a stale copy.
func (r *BackupReconciler) applyStatus(ctx context.Context, backup *lodiav1alpha1.Backup, status *lodiav1alpha1.BackupStatus) (bool, error) {
	if equality.Semantic.DeepEqual(&backup.Status, status) {
		return false, nil
	}
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(status)
	if err != nil {
		return false, err
	}
	ready, err := runtime.DefaultUnstructuredConverter.ToUnstructured(meta.FindStatusCondition(status.Conditions, lodiav1alpha1.ConditionReady))
	if err != nil {
		return false, err
	}
	content["conditions"] = []any{ready}
	applied := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": lodiav1alpha1.GroupVersion.String(),
		"kind":       "Backup",
		"metadata":   map[string]any{"name": backup.Name, "namespace": backup.Namespace, "resourceVersion": backup.ResourceVersion},
		"status":     content,
	}}
	err = r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(applied), client.FieldOwner(BackupFieldManager), client.ForceOwnership)
	if apierrors.IsConflict(err) {
		return true, nil
	}
	return false, err
}
