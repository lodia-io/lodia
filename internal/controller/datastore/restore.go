// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/certs"
	"go.lodia.io/lodia/internal/versions"
)

// restoreTaskPrefix names a restore's EtcdCopyBackupsTask and EtcdOpsTask,
// "restore-<ControlPlane UID>", within the length backupTaskPrefix explains.
const restoreTaskPrefix = "restore-"

// cleanupBound is the operations etcd-backup-restore replays a revision of a
// delta snapshot in, one transaction
// (etcd-backup-restore@v0.45.1 pkg/types/restorer.go#L27): the cleanup's
// deletions and the marker's Put must fit.
const cleanupBound = 10240

// The keys a restore's cleanup deletes, under the API server's default
// prefix, which Workloads keeps (docs/decisions/restore-replaces-the-source.md):
// each of cleanupRanges with every key it starts, and cleanupKey.
var cleanupRanges = []string{
	"/registry/minions/",
	"/registry/leases/kube-system/",
	"/registry/leases/kube-node-lease/",
	"/registry/certificatesigningrequests/",
	"/registry/podcertificaterequests/",
	"/registry/secrets/kube-system/bootstrap-token-",
}

const cleanupKey = "/registry/configmaps/kube-system/extension-apiserver-authentication"

// Cleanup unbinds a restored etcd's data from its source in one transaction
// that also puts RestoreMarkerKey(controlPlane) with value, and does nothing
// once that key exists. It returns how many keys it deletes and the revision
// the key was created at; it sends nothing, and returns revision 0, when the
// keys are cleanupBound or more.
func Cleanup(ctx context.Context, kv clientv3.KV, controlPlane types.UID, value []byte) (int64, int64, error) {
	counts := make([]clientv3.Op, 0, len(cleanupRanges)+1)
	deletes := make([]clientv3.Op, 0, len(cleanupRanges)+2)
	for _, prefix := range cleanupRanges {
		counts = append(counts, clientv3.OpGet(prefix, clientv3.WithPrefix(), clientv3.WithCountOnly()))
		deletes = append(deletes, clientv3.OpDelete(prefix, clientv3.WithPrefix()))
	}
	counts = append(counts, clientv3.OpGet(cleanupKey, clientv3.WithCountOnly()))
	counted, err := kv.Txn(ctx).Then(counts...).Commit()
	if err != nil {
		return 0, 0, err
	}
	var keys int64
	for _, response := range counted.Responses {
		keys += response.GetResponseRange().GetCount()
	}
	if keys >= cleanupBound {
		return keys, 0, nil
	}
	marker := lodiav1alpha1.RestoreMarkerKey(controlPlane)
	deletes = append(deletes, clientv3.OpDelete(cleanupKey), clientv3.OpPut(marker, string(value)))
	response, err := kv.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(marker), "=", 0)).
		Then(deletes...).
		Else(clientv3.OpGet(marker)).
		Commit()
	switch {
	case err != nil:
		return 0, 0, err
	case response.Succeeded:
		return keys, response.Header.GetRevision(), nil
	}
	kvs := response.Responses[0].GetResponseRange().GetKvs()
	if len(kvs) == 0 {
		return 0, 0, fmt.Errorf("the marker %s is neither absent nor present", marker)
	}
	return keys, kvs[0].GetCreateRevision(), nil
}

// restoreMarker is the marker's value, from which status.datastore.restore
// is rebuilt.
type restoreMarker struct {
	Backup lodiav1alpha1.UIDReference `json:"backup"`
	Source lodiav1alpha1.UIDReference `json:"source"`
}

func restoring(message string) *metav1.Condition {
	condition := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonRestoring, message)
	return &condition
}

func restoreTaskMeta(cp *lodiav1alpha1.ControlPlane) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            restoreTaskPrefix + string(cp.UID),
		Namespace:       cp.Namespace,
		Labels:          map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cp, lodiav1alpha1.GroupVersion.WithKind("ControlPlane"))},
	}
}

// restoreFirst decides whether the first Etcd of a ControlPlane with
// restoreFrom may be created from rendered: the Backup is sealed, fits, and
// is in the class's store, and its copy into the ControlPlane's prefix has
// succeeded; an Etcd started on an empty prefix would start empty. Nil means
// it may. A sealed Backup being deleted restores too: its finalizer keeps it
// for the restores that name it.
func (r *Reconciler) restoreFirst(ctx context.Context, cp *lodiav1alpha1.ControlPlane, class *lodiav1alpha1.DatastoreClass, rendered *druidv1alpha1.Etcd) (*metav1.Condition, time.Duration, error) {
	name := cp.Spec.RestoreFrom.BackupRef.Name
	backup := &lodiav1alpha1.Backup{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, backup); err != nil {
		if apierrors.IsNotFound(err) {
			condition := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonBackupNotFound, "no Backup "+name)
			return &condition, 0, nil
		}
		return nil, 0, fmt.Errorf("read Backup %s: %w", name, err)
	}
	if reason, message := unsealed(backup); reason != "" {
		condition := newCondition(metav1.ConditionFalse, reason, message)
		return &condition, 0, nil
	}
	if message := incompatible(cp, backup, rendered); message != "" {
		condition := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonBackupIncompatible, message)
		return &condition, 0, nil
	}
	recorded := ptr.Deref(backup.Status.Snapshots, lodiav1alpha1.BackupSnapshotLocation{})
	if message, err := r.storeDiffers(ctx, cp, class, backup, recorded, rendered); message != "" || err != nil {
		if err != nil {
			return nil, 0, err
		}
		condition := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonRestoreFailed, message)
		return &condition, 0, nil
	}

	task := &druidv1alpha1.EtcdCopyBackupsTask{}
	key := client.ObjectKey{Namespace: cp.Namespace, Name: restoreTaskPrefix + string(cp.UID)}
	err := r.Get(ctx, key, task)
	copying := fmt.Sprintf("copying Backup %s into this ControlPlane's prefix: EtcdCopyBackupsTask %s", name, key.Name)
	switch {
	case apierrors.IsNotFound(err):
		source := druidv1alpha1.StoreSpec{
			Provider:  new(druidv1alpha1.StorageProvider(recorded.Provider)),
			Container: new(recorded.Bucket),
			Prefix:    recorded.Prefix,
			SecretRef: &corev1.SecretReference{Name: lodiav1alpha1.EtcdBackupSecret.Name(cp.Name)},
		}
		if recorded.Endpoint != "" {
			source.EndpointOverride = new(recorded.Endpoint)
		}
		task = &druidv1alpha1.EtcdCopyBackupsTask{
			ObjectMeta: restoreTaskMeta(cp),
			Spec:       druidv1alpha1.EtcdCopyBackupsTaskSpec{SourceStore: source, TargetStore: *rendered.Spec.Backup.Store.DeepCopy(), MaxBackups: new(uint32(1))},
		}
		if err := r.Create(ctx, task, client.FieldOwner(FieldManager)); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, 0, fmt.Errorf("create EtcdCopyBackupsTask %s: %w", key.Name, err)
		}
		return restoring(copying), 0, nil
	case err != nil:
		return nil, 0, err
	}
	// The copier copies only what the target lacks
	// (etcd-backup-restore@v0.45.1 pkg/snapshot/copier/copier.go#L127-L151),
	// so another task completes what a failed one left. The failed one stays a
	// minute, so that its Job and status.lastError can be read.
	if failed := copyCondition(task, druidv1alpha1.EtcdCopyBackupsTaskFailed); failed != nil {
		why := failed.Message
		if why == "" {
			why = ptr.Deref(task.Status.LastError, failed.Reason)
		}
		condition := restoring(fmt.Sprintf("the copy of Backup %s failed: %s; copying again", name, why))
		if wait := failed.LastTransitionTime.Add(time.Minute).Sub(r.Clock.Now()); wait > 0 {
			return condition, wait, nil
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, task, client.Preconditions{UID: &task.UID})); err != nil {
			return nil, 0, fmt.Errorf("delete EtcdCopyBackupsTask %s: %w", key.Name, err)
		}
		return condition, 0, nil
	}
	if copyCondition(task, druidv1alpha1.EtcdCopyBackupsTaskSucceeded) == nil {
		return restoring(copying), 0, nil
	}
	return nil, 0, nil
}

// unsealed says why a Backup cannot be restored yet, or ever, as a reason
// and its message; empty once it is sealed: Completed, and marked with its own
// UID, which the backup reconciler writes in the pass after.
func unsealed(backup *lodiav1alpha1.Backup) (string, string) {
	ready := meta.FindStatusCondition(backup.Status.Conditions, lodiav1alpha1.ConditionReady)
	switch {
	case ready == nil:
		return lodiav1alpha1.ReasonWaitingForBackup, "Backup " + backup.Name + " has no Ready yet"
	case ready.Status == metav1.ConditionTrue && ready.Reason == lodiav1alpha1.ReasonCompleted:
		if backup.Annotations[lodiav1alpha1.SealedUIDAnnotation] == string(backup.UID) {
			return "", ""
		}
		return lodiav1alpha1.ReasonWaitingForBackup, fmt.Sprintf("Backup %s is Completed, and its %s does not name its UID yet", backup.Name, lodiav1alpha1.SealedUIDAnnotation)
	case final(backup, ready) || ready.Status == metav1.ConditionFalse && (ready.Reason == lodiav1alpha1.ReasonSealLost || ready.Reason == lodiav1alpha1.ReasonObjectsControlledElsewhere):
		return lodiav1alpha1.ReasonRestoreFailed, fmt.Sprintf("Backup %s is %s: %s; restore another Backup", backup.Name, ready.Reason, ready.Message)
	}
	return lodiav1alpha1.ReasonWaitingForBackup, fmt.Sprintf("Backup %s is %s: %s", backup.Name, ready.Reason, ready.Message)
}

// incompatible names the first of the Backup's records that this
// ControlPlane does not match: its minor, its network, and the quota the
// source's etcd had, which the first Etcd's must reach.
func incompatible(cp *lodiav1alpha1.ControlPlane, backup *lodiav1alpha1.Backup, rendered *druidv1alpha1.Etcd) string {
	recorded, _ := versions.Minor(backup.Status.KubernetesVersion)
	minor, _ := versions.Minor(cp.Spec.Version)
	network := ptr.Deref(backup.Status.Network, lodiav1alpha1.Network{})
	differs := func(field string, spec, recorded any) string {
		return fmt.Sprintf("spec.network.%s is %v, and Backup %s was taken of a network with %v: use that", field, spec, backup.Name, recorded)
	}
	seal := ptr.Deref(backup.Status.Seal, lodiav1alpha1.BackupSeal{})
	switch {
	case minor != recorded:
		return fmt.Sprintf("Backup %s was taken at Kubernetes %q, and spec.version %s is of another minor: use the Backup's", backup.Name, backup.Status.KubernetesVersion, cp.Spec.Version)
	case !slices.Equal(cp.Spec.Network.ServiceCIDRs, network.ServiceCIDRs):
		return differs("serviceCIDRs", cp.Spec.Network.ServiceCIDRs, network.ServiceCIDRs)
	case !slices.Equal(cp.Spec.Network.PodCIDRs, network.PodCIDRs):
		return differs("podCIDRs", cp.Spec.Network.PodCIDRs, network.PodCIDRs)
	case cp.Spec.Network.ClusterDomain != network.ClusterDomain:
		return differs("clusterDomain", cp.Spec.Network.ClusterDomain, network.ClusterDomain)
	case seal.QuotaBackendBytes != nil && rendered.Spec.Etcd.Quota.Cmp(*seal.QuotaBackendBytes) < 0:
		return fmt.Sprintf("Backup %s comes from an etcd with quota %s; this ControlPlane's class gives %s: use a class with at least %s",
			backup.Name, seal.QuotaBackendBytes.String(), rendered.Spec.Etcd.Quota.String(), seal.QuotaBackendBytes.String())
	}
	return ""
}

// storeDiffers names how the store the first Etcd would use differs from the
// one the Backup was sealed in, the keys of the credentials copy that name a
// store included, as the backup reconciler compares them.
func (r *Reconciler) storeDiffers(ctx context.Context, cp *lodiav1alpha1.ControlPlane, class *lodiav1alpha1.DatastoreClass, backup *lodiav1alpha1.Backup, recorded lodiav1alpha1.BackupSnapshotLocation, rendered *druidv1alpha1.Etcd) (string, error) {
	store := rendered.Spec.Backup.Store
	if store == nil {
		return fmt.Sprintf("DatastoreClass %s has no backup store, while Backup %s was sealed in %s: point the class back at that store, or restore another Backup", class.Name, backup.Name, storeOf(recorded.SnapshotLocation)), nil
	}
	credentials := lodiav1alpha1.EtcdBackupSecret.Name(cp.Name)
	fromSecret, err := storeSecretFields(ctx, r.Reader, rendered, credentials)
	if err != nil {
		return "", fmt.Errorf("read Secret %s: %w", credentials, err)
	}
	key, was, is := storeMoved(recorded, snapshotLocation(store, fromSecret, backup.UID))
	switch {
	case was == "":
		return "", nil
	case key == "":
		return fmt.Sprintf("DatastoreClass %s names %s, while Backup %s was sealed in %s: point the class back at that store, or restore another Backup", class.Name, is, backup.Name, was), nil
	}
	return fmt.Sprintf("Secret %s's %s is %s, while Backup %s was sealed with %s: point DatastoreClass %s back at that store, or restore another Backup", credentials, key, is, backup.Name, was, class.Name), nil
}

// restoreCheck is what a pass found of a restore once its Etcd exists.
type restoreCheck struct {
	// restored is the restore's status once it is complete.
	restored *lodiav1alpha1.RestoreStatus
	// condition is DatastoreReady while the restore is not complete.
	condition    *metav1.Condition
	unanswered   bool
	requeueAfter time.Duration
}

// checkRestore reads, in the etcd of a restoring ControlPlane, whether its
// restore is complete: the marker in the etcd and a full snapshot of it in
// the store, never a status. It takes the steps left: the seal check, the
// cleanup and the full snapshot. A check that cannot run, on an etcd with no
// ready member or one that does not answer, changes nothing.
func (r *Reconciler) checkRestore(ctx context.Context, cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd) (restoreCheck, error) {
	if min(etcd.Status.ReadyReplicas, etcd.Spec.Replicas) == 0 {
		return restoreCheck{}, nil
	}
	c, err := r.EtcdClient(ctx, etcd)
	if err != nil {
		return unanswered(ctx, etcd, fmt.Errorf("connect to the etcd of Etcd %s: %w", etcd.Name, err)), nil
	}
	defer func() { _ = c.Close() }()
	var marker *clientv3.GetResponse
	if err := request(ctx, func(ctx context.Context) (err error) {
		marker, err = c.Get(ctx, lodiav1alpha1.RestoreMarkerKey(cp.UID))
		return err
	}); err != nil {
		return unanswered(ctx, etcd, err), nil
	}
	var value restoreMarker
	var revision int64
	if len(marker.Kvs) > 0 {
		if err := json.Unmarshal(marker.Kvs[0].GetValue(), &value); err != nil {
			return restoreCheck{}, fmt.Errorf("read the marker %s in the etcd of Etcd %s: %w", lodiav1alpha1.RestoreMarkerKey(cp.UID), etcd.Name, err)
		}
		revision = marker.Kvs[0].GetCreateRevision()
	} else {
		var stop *restoreCheck
		if value, revision, stop, err = r.cleanUp(ctx, cp, etcd, c); stop != nil || err != nil {
			return ptr.Deref(stop, restoreCheck{}), err
		}
	}

	held, ignored, err := fullSnapshotRevision(ctx, r.Reader, etcd)
	if err != nil {
		return restoreCheck{}, err
	}
	if held >= revision {
		return restoreCheck{restored: restored(cp, value)}, r.tidyRestore(ctx, cp)
	}
	state, wait, err := r.snapshotRestore(ctx, cp, etcd)
	if err != nil {
		return restoreCheck{}, err
	}
	message := fmt.Sprintf("cleaned up; waiting for a full snapshot of revision %d: the store holds %d; EtcdOpsTask %s is %s",
		revision, held, restoreTaskPrefix+string(cp.UID), state)
	if ignored {
		message += fmt.Sprintf("; ignored Lease %s, created before Etcd %s", druidv1alpha1.GetFullSnapshotLeaseName(etcd.ObjectMeta), etcd.Name)
	}
	return restoreCheck{condition: restoring(message), requeueAfter: sooner(10*time.Second, wait)}, nil
}

func request(ctx context.Context, call func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, etcdRequestTimeout)
	defer cancel()
	return call(ctx)
}

func unanswered(ctx context.Context, etcd *druidv1alpha1.Etcd, err error) restoreCheck {
	logf.FromContext(ctx).Error(err, "check the restore in the etcd", "etcd", etcd.Name)
	return restoreCheck{unanswered: true, requeueAfter: 10 * time.Second}
}

// cleanUp checks the seal of the Backup in an etcd without the marker, then
// cleans it up, and returns the marker's value and revision, or the check
// that stops the restore here. Once the restore is recorded, the status names
// the Backup, which may be gone.
func (r *Reconciler) cleanUp(ctx context.Context, cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd, c *clientv3.Client) (restoreMarker, int64, *restoreCheck, error) {
	var value restoreMarker
	if recorded := restoreOf(cp); recorded != nil {
		value = restoreMarker{Backup: recorded.Backup, Source: recorded.Source}
	} else {
		name := cp.Spec.RestoreFrom.BackupRef.Name
		backup := &lodiav1alpha1.Backup{}
		if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, backup); err != nil {
			if apierrors.IsNotFound(err) {
				condition := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonBackupNotFound, "no Backup "+name)
				return restoreMarker{}, 0, &restoreCheck{condition: &condition}, nil
			}
			return restoreMarker{}, 0, nil, fmt.Errorf("read Backup %s: %w", name, err)
		}
		value = restoreMarker{Backup: lodiav1alpha1.UIDReference{Name: backup.Name, UID: backup.UID}, Source: ptr.Deref(backup.Status.Source, lodiav1alpha1.UIDReference{})}
	}
	var seal *clientv3.GetResponse
	if err := request(ctx, func(ctx context.Context) (err error) {
		seal, err = c.Get(ctx, lodiav1alpha1.BackupSealKey(value.Backup.UID))
		return err
	}); err != nil {
		return restoreMarker{}, 0, new(unanswered(ctx, etcd, err)), nil
	}
	if len(seal.Kvs) == 0 || string(seal.Kvs[0].GetValue()) != string(value.Backup.UID) {
		condition := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonRestoreFailed, fmt.Sprintf(
			"the restored etcd lacks %s, the seal of Backup %s: the copy is not that Backup's; nothing was cleaned up and no API server starts; delete this ControlPlane and restore another Backup",
			lodiav1alpha1.BackupSealKey(value.Backup.UID), value.Backup.Name))
		return restoreMarker{}, 0, &restoreCheck{condition: &condition}, nil
	}
	content, err := json.Marshal(value)
	if err != nil {
		return restoreMarker{}, 0, nil, err
	}
	var keys, revision int64
	err = request(ctx, func(ctx context.Context) (err error) {
		keys, revision, err = Cleanup(ctx, c, cp.UID, content)
		return err
	})
	switch {
	case errors.Is(err, rpctypes.ErrNoSpace):
		return restoreMarker{}, 0, &restoreCheck{condition: restoring(fmt.Sprintf(`etcd refuses the cleanup: %v; see "etcd space quota" in docs/operations.md`, err)), requeueAfter: time.Minute}, nil
	case err != nil:
		return restoreMarker{}, 0, new(unanswered(ctx, etcd, err)), nil
	case revision == 0:
		condition := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonRestoreFailed, fmt.Sprintf(
			"the cleanup would delete %d keys; etcd-backup-restore replays a revision as one transaction of at most %d operations (pkg/types/restorer.go:27)", keys, cleanupBound))
		return restoreMarker{}, 0, &restoreCheck{condition: &condition}, nil
	}
	return value, revision, nil, nil
}

// snapshotRestore asks etcd-druid for a full snapshot of the restored etcd
// until the full snapshot Lease shows the marker, as the backup reconciler
// does for a seal, with resend timed from the Etcd's creation, and says the
// task's state and how long it waits.
func (r *Reconciler) snapshotRestore(ctx context.Context, cp *lodiav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd) (string, time.Duration, error) {
	task := &druidv1alpha1.EtcdOpsTask{}
	key := client.ObjectKey{Namespace: cp.Namespace, Name: restoreTaskPrefix + string(cp.UID)}
	err := r.Get(ctx, key, task)
	switch {
	case apierrors.IsNotFound(err):
		task = &druidv1alpha1.EtcdOpsTask{
			ObjectMeta: restoreTaskMeta(cp),
			Spec: druidv1alpha1.EtcdOpsTaskSpec{
				Config:   druidv1alpha1.EtcdOpsTaskConfig{OnDemandSnapshot: &druidv1alpha1.OnDemandSnapshotConfig{Type: druidv1alpha1.OnDemandSnapshotTypeFull}},
				EtcdName: new(etcd.Name),
			},
		}
		if err := r.Create(ctx, task, client.FieldOwner(FieldManager)); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", 0, fmt.Errorf("create EtcdOpsTask %s: %w", key.Name, err)
		}
		return "created", 0, nil
	case err != nil:
		return "", 0, err
	case task.IsCompleted():
		return resend(ctx, r.Client, r.Clock.Now(), task, etcd.CreationTimestamp.Time)
	}
	return string(ptr.Deref(task.Status.State, "not admitted yet")), 0, nil
}

// restored is the status of a complete restore, from the marker's value; it
// keeps the time it was first written for the same Backup.
func restored(cp *lodiav1alpha1.ControlPlane, value restoreMarker) *lodiav1alpha1.RestoreStatus {
	status := &lodiav1alpha1.RestoreStatus{Backup: value.Backup, Source: value.Source, CompletedAt: metav1.Now()}
	if recorded := restoreOf(cp); recorded != nil && recorded.Backup.UID == value.Backup.UID {
		status.CompletedAt = recorded.CompletedAt
	}
	return status
}

func restoreOf(cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.RestoreStatus {
	if cp.Status.Datastore == nil {
		return nil
	}
	return cp.Status.Datastore.Restore
}

func (r *Reconciler) tidyRestore(ctx context.Context, cp *lodiav1alpha1.ControlPlane) error {
	key := client.ObjectKey{Namespace: cp.Namespace, Name: restoreTaskPrefix + string(cp.UID)}
	var errs []error
	for _, task := range []client.Object{&druidv1alpha1.EtcdCopyBackupsTask{}, &druidv1alpha1.EtcdOpsTask{}} {
		if err := r.Get(ctx, key, task); err != nil {
			errs = append(errs, client.IgnoreNotFound(err))
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, task, client.Preconditions{UID: new(task.GetUID())})); err != nil {
			errs = append(errs, fmt.Errorf("delete %T %s: %w", task, key.Name, err))
		}
	}
	return errors.Join(errs...)
}

// caPredates refuses the first Etcd of a ControlPlane whose cluster CA was
// issued before the ControlPlane was created: PKI adopted CA Secrets that came
// back without their data, and nodes that trust the CA would join an empty
// cluster (docs/decisions/restore-replaces-the-source.md). PKI backdates
// every certificate by certs.Backdate and x509 keeps seconds, so NotBefore
// plus that is the second PKI issued it in, never before the second of the
// ControlPlane it was issued for.
func (r *Reconciler) caPredates(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*metav1.Condition, error) {
	name := lodiav1alpha1.CABundleSecret.Name(cp.Name)
	bundle := &corev1.Secret{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, bundle); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if len(bundle.Data[lodiav1alpha1.CACertKey]) == 0 {
		return nil, nil
	}
	cas, err := certutil.ParseCertsPEM(bundle.Data[lodiav1alpha1.CACertKey])
	if err != nil {
		return nil, fmt.Errorf("read the CAs in Secret %s: %w", name, err)
	}
	for _, ca := range cas {
		if issued := ca.NotBefore.Add(certs.Backdate); issued.Before(cp.CreationTimestamp.Time) {
			condition := newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonCAPredatesControlPlane, fmt.Sprintf(
				"the cluster CA %s in %s was issued at %s, before this ControlPlane was created at %s: its Secrets came back and its data did not, and nodes that trust it would join an empty cluster. Restore the management cluster from a backup that keeps UIDs, or delete %s and %s for a new identity",
				ca.Subject.String(), name, issued.UTC().Format(time.RFC3339), cp.CreationTimestamp.UTC().Format(time.RFC3339), name, cp.Name+"-cakey-cluster"))
			return &condition, nil
		}
	}
	return nil, nil
}
