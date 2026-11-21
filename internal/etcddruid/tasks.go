// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid

import (
	"context"
	"fmt"
	"strconv"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// FullSnapshotLease is the name of the Etcd's full snapshot Lease.
func (s *Store) FullSnapshotLease() string {
	return druidv1alpha1.GetFullSnapshotLeaseName(s.etcd.ObjectMeta)
}

// FullSnapshotRevision is the last revision of the newest full snapshot the
// Etcd's full snapshot Lease shows in its store: the sidecar sets its holder
// after each full snapshot it uploaded and never lowers it
// (etcd-backup-restore@v0.45.3 pkg/health/heartbeat/heartbeat.go#L200-L222).
// etcd-druid adopts a Lease of a predecessor of the same name with its holder,
// so a Lease created before the Etcd counts for nothing, which the second
// result says.
func (e *Engine) FullSnapshotRevision(ctx context.Context, reader client.Reader, s *Store) (int64, bool, error) {
	etcd := s.etcd
	name := druidv1alpha1.GetFullSnapshotLeaseName(etcd.ObjectMeta)
	lease := &coordinationv1.Lease{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: etcd.Namespace, Name: name}, lease); err != nil {
		if apierrors.IsNotFound(err) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read Lease %s: %w", name, err)
	}
	if lease.CreationTimestamp.Before(&etcd.CreationTimestamp) {
		return 0, true, nil
	}
	if revision, err := strconv.ParseInt(ptr.Deref(lease.Spec.HolderIdentity, ""), 10, 64); err == nil {
		return revision, false, nil
	}
	return 0, false, nil
}

// FullSnapshot asks etcd-druid for a full snapshot of s with the task meta
// names, read and created through c as manager, and says the task's state and
// how long it waits: a finished task is sent again as resend says, from start.
// A task's name is at most 56 characters: the copy Job's pods carry it and
// "-worker" as a label value of at most 63
// (etcd-druid@v0.39.2 internal/controller/etcdcopybackupstask/reconciler.go#L389-L396).
func (e *Engine) FullSnapshot(ctx context.Context, c client.Client, clk clock.PassiveClock, manager string, meta metav1.ObjectMeta, s *Store, start time.Time) (string, time.Duration, error) {
	return fullSnapshot(ctx, c, clk, manager, meta, s.etcd, func(*druidv1alpha1.EtcdOpsTask) time.Time { return start })
}

// RestoredSnapshot is FullSnapshot of a restored etcd. resend is timed from
// when the Etcd's Ready condition turned True, without which etcd-druid takes
// no on-demand snapshot
// (etcd-druid@v0.39.2 internal/controller/etcdopstask/handler/ondemandsnapshot/ondemandsnapshot.go#L181-L188),
// and from the Etcd's creation while it is not True or, for a task etcd-druid
// ran rather than rejected at admission
// (etcd-druid@v0.39.2 internal/controller/etcdopstask/reconcile_task.go#L83-L121),
// when it turned True after etcd-druid admitted the task (its startedAt,
// etcd-druid@v0.39.2 internal/controller/etcdopstask/utils.go#L91-L94), so
// that a member back from an outage does not shorten the wait after a full
// snapshot.
func (e *Engine) RestoredSnapshot(ctx context.Context, c client.Client, clk clock.PassiveClock, manager string, meta metav1.ObjectMeta, s *Store) (string, time.Duration, error) {
	etcd := s.etcd
	return fullSnapshot(ctx, c, clk, manager, meta, etcd, func(task *druidv1alpha1.EtcdOpsTask) time.Time {
		start := etcd.CreationTimestamp.Time
		if ready := druidv1alpha1.GetCondition(etcd, druidv1alpha1.ConditionTypeReady); ready != nil && ready.Status == druidv1alpha1.ConditionTrue &&
			(*task.Status.State == druidv1alpha1.TaskStateRejected || !ready.LastTransitionTime.After(ptr.Deref(task.Status.StartedAt, task.CreationTimestamp).Time)) {
			start = ready.LastTransitionTime.Time
		}
		return start
	})
}

func fullSnapshot(ctx context.Context, c client.Client, clk clock.PassiveClock, manager string, meta metav1.ObjectMeta, etcd *druidv1alpha1.Etcd, start func(*druidv1alpha1.EtcdOpsTask) time.Time) (string, time.Duration, error) {
	task := &druidv1alpha1.EtcdOpsTask{}
	err := c.Get(ctx, client.ObjectKey{Namespace: meta.Namespace, Name: meta.Name}, task)
	switch {
	case apierrors.IsNotFound(err):
		task = &druidv1alpha1.EtcdOpsTask{
			ObjectMeta: meta,
			Spec: druidv1alpha1.EtcdOpsTaskSpec{
				Config:   druidv1alpha1.EtcdOpsTaskConfig{OnDemandSnapshot: &druidv1alpha1.OnDemandSnapshotConfig{Type: druidv1alpha1.OnDemandSnapshotTypeFull}},
				EtcdName: new(etcd.Name),
			},
		}
		if err := c.Create(ctx, task, client.FieldOwner(manager)); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", 0, fmt.Errorf("create EtcdOpsTask %s: %w", meta.Name, err)
		}
		return "created", 0, nil
	case err != nil:
		return "", 0, err
	case task.IsCompleted():
		return resend(ctx, c, clk.Now(), task, start(task))
	}
	return string(ptr.Deref(task.Status.State, "not admitted yet")), 0, nil
}

// resend deletes a finished EtcdOpsTask for the next once the time since
// start, at most an hour, has passed since it finished (its
// lastTransitionTime, etcd-druid@v0.39.2
// internal/controller/etcdopstask/utils.go#L95-L96), and says the task's state
// and how long it waits, 0 once it deleted it. A finished task names no
// revision (etcd-druid@v0.39.2 api/core/v1alpha1/etcdopstask.go#L137-L161),
// so one short of the revision it was sent for is sent again, and a Lease
// that never reaches that revision costs a full snapshot each time.
func resend(ctx context.Context, c client.Client, now time.Time, task *druidv1alpha1.EtcdOpsTask, start time.Time) (string, time.Duration, error) {
	finished := ptr.Deref(task.Status.LastTransitionTime, metav1.Time{}).Time
	next := finished.Add(min(time.Hour, finished.Sub(start)))
	if wait := next.Sub(now); wait > 0 {
		return fmt.Sprintf("%s at %s; the next goes at %s", *task.Status.State, finished.UTC().Format(time.RFC3339), next.UTC().Format(time.RFC3339)), wait, nil
	}
	if err := client.IgnoreNotFound(c.Delete(ctx, task, client.Preconditions{UID: &task.UID})); err != nil {
		return "", 0, fmt.Errorf("delete EtcdOpsTask %s: %w", task.Name, err)
	}
	return string(*task.Status.State) + ", deleted for another", 0, nil
}

// Copy is a copy of snapshots from one store to another, an
// EtcdCopyBackupsTask, as read.
type Copy struct {
	task *druidv1alpha1.EtcdCopyBackupsTask
}

// CopyOf reads the copy key names from reader.
func (e *Engine) CopyOf(ctx context.Context, reader client.Reader, key client.ObjectKey) (*Copy, error) {
	task := &druidv1alpha1.EtcdCopyBackupsTask{}
	if err := reader.Get(ctx, key, task); err != nil {
		return nil, err
	}
	return &Copy{task: task}, nil
}

// Failed says why the copy failed and when, false while it has not.
func (c *Copy) Failed() (string, time.Time, bool) {
	failed := copyCondition(c.task, druidv1alpha1.EtcdCopyBackupsTaskFailed)
	if failed == nil {
		return "", time.Time{}, false
	}
	why := failed.Message
	if why == "" {
		why = ptr.Deref(c.task.Status.LastError, failed.Reason)
	}
	return why, failed.LastTransitionTime.Time, true
}

func (c *Copy) Succeeded() bool {
	return copyCondition(c.task, druidv1alpha1.EtcdCopyBackupsTaskSucceeded) != nil
}

func copyCondition(task *druidv1alpha1.EtcdCopyBackupsTask, conditionType druidv1alpha1.ConditionType) *druidv1alpha1.Condition {
	for i := range task.Status.Conditions {
		if c := &task.Status.Conditions[i]; c.Type == conditionType && c.Status == druidv1alpha1.ConditionTrue {
			return c
		}
	}
	return nil
}

// CopyTo asks etcd-druid, with the task meta names, created through c as
// manager, to copy the newest snapshots of from's store, with its credentials,
// to the location to; a task's name is as long as FullSnapshot says.
func (e *Engine) CopyTo(ctx context.Context, c client.Client, manager string, meta metav1.ObjectMeta, from *Store, to lodiav1alpha1.SnapshotLocation) error {
	store := from.etcd.Spec.Backup.Store
	target := store.DeepCopy()
	target.Provider = new(druidv1alpha1.StorageProvider(to.Provider))
	target.Container = new(to.Bucket)
	target.EndpointOverride = nil
	if to.Endpoint != "" {
		target.EndpointOverride = new(to.Endpoint)
	}
	target.Prefix = to.Prefix
	return createCopy(ctx, c, manager, &druidv1alpha1.EtcdCopyBackupsTask{
		ObjectMeta: meta,
		Spec:       druidv1alpha1.EtcdCopyBackupsTaskSpec{SourceStore: *store.DeepCopy(), TargetStore: *target, MaxBackups: new(uint32(1))},
	})
}

// CopyFrom asks etcd-druid, as CopyTo does, to copy the newest snapshots at
// the location from, with the credentials of the Secret secret, into the store
// of the rendered to.
func (e *Engine) CopyFrom(ctx context.Context, c client.Client, manager string, meta metav1.ObjectMeta, from lodiav1alpha1.SnapshotLocation, secret string, to *Store) error {
	source := druidv1alpha1.StoreSpec{
		Provider:  new(druidv1alpha1.StorageProvider(from.Provider)),
		Container: new(from.Bucket),
		Prefix:    from.Prefix,
		SecretRef: &corev1.SecretReference{Name: secret},
	}
	if from.Endpoint != "" {
		source.EndpointOverride = new(from.Endpoint)
	}
	return createCopy(ctx, c, manager, &druidv1alpha1.EtcdCopyBackupsTask{
		ObjectMeta: meta,
		Spec:       druidv1alpha1.EtcdCopyBackupsTaskSpec{SourceStore: source, TargetStore: *to.etcd.Spec.Backup.Store.DeepCopy(), MaxBackups: new(uint32(1))},
	})
}

func createCopy(ctx context.Context, c client.Client, manager string, task *druidv1alpha1.EtcdCopyBackupsTask) error {
	if err := c.Create(ctx, task, client.FieldOwner(manager)); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create EtcdCopyBackupsTask %s: %w", task.Name, err)
	}
	return nil
}

// DeleteCopy deletes the copy under its UID.
func (e *Engine) DeleteCopy(ctx context.Context, c client.Client, copied *Copy) error {
	return c.Delete(ctx, copied.task, client.Preconditions{UID: &copied.task.UID})
}
