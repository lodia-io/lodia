// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && restore

package e2e

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/contract"
)

const deltasClass = "e2e-deltas"

const (
	restoreStarts = "data directory found to be invalid, restoring it from backup-bucket"
	restoreEnds   = "Successfully restored the etcd data directory."
	restoresBase  = "Restoring from base snapshot"
)

var attemptsDeltas = regexp.MustCompile(`Attempting to apply (\d+) delta snapshots for restoration`)

func TestOneMemberRestoredThroughDeltas(t *testing.T) {
	var (
		k8s      client.Client
		access   *tenantAccess
		event    *corev1.Event
		marker   *corev1.ConfigMap
		full     int64
		delta    int64
		next     int64
		member   types.UID
		claim    types.UID
		deletion time.Time
	)
	key := client.ObjectKey{Namespace: "team-deltas", Name: controlPlane}
	etcdKey := client.ObjectKey{Namespace: key.Namespace, Name: contract.EtcdName(key.Name)}
	claimKey := client.ObjectKey{Namespace: key.Namespace, Name: etcdKey.Name + "-" + etcdKey.Name + "-0"}
	f := features.New("nightly on kind: a one-member tenant that loses its volume is restored from the store through delta snapshots").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			startStore(ctx, t, k8s)
			create(ctx, t, k8s, &lodiav1alpha1.DatastoreClass{
				ObjectMeta: metav1.ObjectMeta{Name: deltasClass},
				Spec: lodiav1alpha1.DatastoreClassSpec{
					Storage: lodiav1alpha1.EtcdStorage{Size: resource.MustParse("8Gi"), ReclaimPolicy: "Retain"},
					Backup: &lodiav1alpha1.EtcdBackup{
						Store: lodiav1alpha1.BackupStore{
							Provider: "S3", Bucket: s3Bucket, Prefix: "e2e-deltas", Endpoint: s3Endpoint,
							SecretRef: lodiav1alpha1.SecretReference{Name: s3Secret, Namespace: operatorNamespace},
						},
						DeltaSnapshotPeriod: &metav1.Duration{Duration: 10 * time.Second},
					},
				},
			})
			create(ctx, t, k8s, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: key.Namespace}})
			spec := restoreSpec(t, nil)
			spec.Datastore.ClassName = deltasClass
			create(ctx, t, k8s, &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}, Spec: spec})
			return ctx
		}).
		Assess("1. the tenant is Ready with one etcd member and BackupReady", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 8*time.Minute, "Ready=True and BackupReady=True, BackupSucceeded", func(ctx context.Context) error {
				cp := &lodiav1alpha1.ControlPlane{}
				if err := controlPlaneReady(ctx, k8s, key, cp); err != nil {
					return err
				}
				if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionBackupReady); c == nil || c.Status != metav1.ConditionTrue || c.Reason != lodiav1alpha1.ReasonBackupSucceeded {
					return fmt.Errorf("BackupReady is %+v", c)
				}
				return nil
			})
			etcd := &druidv1alpha1.Etcd{}
			if err := k8s.Get(ctx, etcdKey, etcd); err != nil {
				t.Fatal(err)
			}
			if etcd.Spec.Replicas != 1 {
				t.Fatalf("Etcd %s asks for %d members, want 1", etcd.Name, etcd.Spec.Replicas)
			}
			if period := etcd.Spec.Backup.DeltaSnapshotPeriod; period == nil || period.Duration != 10*time.Second || etcd.Spec.Backup.FullSnapshotSchedule == nil || *etcd.Spec.Backup.FullSnapshotSchedule != "0 0 * * *" {
				t.Fatalf("Etcd %s snapshots with a delta period of %v and the full schedule %v, want 10s and the class's default", etcd.Name, period, etcd.Spec.Backup.FullSnapshotSchedule)
			}
			access = openTenant(ctx, t, k8s, key)
			return ctx
		}).
		Assess("2. an Event, then a full snapshot at or past the current revision, then a marker, then a delta that holds it and one more", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			event = &corev1.Event{
				ObjectMeta:     metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "e2e-event"},
				InvolvedObject: corev1.ObjectReference{APIVersion: "v1", Kind: "Namespace", Name: metav1.NamespaceDefault},
				Reason:         "E2EDeltas", Message: "an Event, whose key etcd holds on a lease", Type: corev1.EventTypeNormal,
				Source:         corev1.EventSource{Component: "lodia-e2e"},
				FirstTimestamp: metav1.Now(), LastTimestamp: metav1.Now(), Count: 1,
			}
			create(ctx, t, access.client, event)
			current := currentRevision(ctx, t, access.client)
			t.Logf("Event %s is at revision %s, the tenant's etcd at %d", event.Name, event.ResourceVersion, current)
			create(ctx, t, k8s, &druidv1alpha1.EtcdOpsTask{
				ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: "full-snapshot"},
				Spec: druidv1alpha1.EtcdOpsTaskSpec{
					Config:   druidv1alpha1.EtcdOpsTaskConfig{OnDemandSnapshot: &druidv1alpha1.OnDemandSnapshotConfig{Type: druidv1alpha1.OnDemandSnapshotTypeFull}},
					EtcdName: new(etcdKey.Name),
				},
			})
			until(ctx, t, 3*time.Minute, "the full snapshot Lease holds a revision at or past "+strconv.FormatInt(current, 10), func(ctx context.Context) error {
				var err error
				if full, err = snapshotLease(ctx, k8s, key.Namespace, druidv1alpha1.GetFullSnapshotLeaseName(metav1.ObjectMeta{Name: etcdKey.Name})); err != nil {
					return err
				}
				if full < current {
					return fmt.Errorf("the full snapshot Lease holds %d", full)
				}
				return nil
			})
			marker = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceDefault, Name: "e2e-marker"}, Data: map[string]string{"after": "the full snapshot"}}
			create(ctx, t, access.client, marker)
			revision, err := strconv.ParseInt(marker.ResourceVersion, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			if revision <= full {
				t.Fatalf("the marker is at revision %d, not past the full snapshot's %d", revision, full)
			}
			until(ctx, t, 3*time.Minute, "the delta snapshot Lease holds a revision past the marker's "+marker.ResourceVersion, func(ctx context.Context) error {
				var err error
				if delta, err = snapshotLease(ctx, k8s, key.Namespace, druidv1alpha1.GetDeltaSnapshotLeaseName(metav1.ObjectMeta{Name: etcdKey.Name})); err != nil {
					return err
				}
				if delta < revision {
					return fmt.Errorf("the delta snapshot Lease holds %d", delta)
				}
				return nil
			})
			until(ctx, t, 3*time.Minute, "a delta snapshot after the one that holds the marker", func(ctx context.Context) error {
				var err error
				if next, err = snapshotLease(ctx, k8s, key.Namespace, druidv1alpha1.GetDeltaSnapshotLeaseName(metav1.ObjectMeta{Name: etcdKey.Name})); err != nil {
					return err
				}
				if next <= delta {
					return fmt.Errorf("the delta snapshot Lease holds %d", next)
				}
				return nil
			})
			t.Logf("the full snapshot is at revision %d, the marker at %d, %d revisions after it, the delta snapshot that holds it reaches %d and the next %d", full, revision, revision-full, delta, next)
			return ctx
		}).
		Assess("3. the member's claim and then its pod are deleted", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			pvc := &corev1.PersistentVolumeClaim{}
			if err := k8s.Get(ctx, claimKey, pvc); err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{}
			if err := k8s.Get(ctx, client.ObjectKey{Namespace: etcdKey.Namespace, Name: etcdKey.Name + "-0"}, pod); err != nil {
				t.Fatal(err)
			}
			claim, member, deletion = pvc.UID, pod.UID, time.Now()
			if err := k8s.Delete(ctx, pvc, client.Preconditions{UID: &claim}); err != nil {
				t.Fatal(err)
			}
			if err := k8s.Delete(ctx, pod, client.Preconditions{UID: &member}); err != nil {
				t.Fatal(err)
			}
			t.Logf("claim %s (%s) and pod %s (%s) deleted", claimKey.Name, claim, pod.Name, member)
			return ctx
		}).
		Assess("4. a new member on a new claim is Ready again, restored through at least two deltas, and the tenant serves the Event and the marker", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			var pod *corev1.Pod
			until(ctx, t, 10*time.Minute, "a new member pod on a new claim is Ready", func(ctx context.Context) error {
				pod = &corev1.Pod{}
				if err := k8s.Get(ctx, client.ObjectKey{Namespace: etcdKey.Namespace, Name: etcdKey.Name + "-0"}, pod); err != nil {
					return err
				}
				if pod.UID == member {
					return fmt.Errorf("the member pod is still %s", member)
				}
				if err := running(pod); err != nil {
					return err
				}
				pvc := &corev1.PersistentVolumeClaim{}
				if err := k8s.Get(ctx, claimKey, pvc); err != nil {
					return err
				}
				if pvc.UID == claim || pvc.Status.Phase != corev1.ClaimBound {
					return fmt.Errorf("claim %s is %s, %s", pvc.Name, pvc.UID, pvc.Status.Phase)
				}
				return nil
			})
			started := pod.Status.StartTime.Time
			var ready time.Time
			for _, c := range pod.Status.Conditions {
				if c.Type == corev1.PodReady {
					ready = c.LastTransitionTime.Time
				}
			}
			until(ctx, t, 5*time.Minute, "the tenant is Ready", func(ctx context.Context) error {
				return controlPlaneReady(ctx, k8s, key, &lodiav1alpha1.ControlPlane{})
			})
			tenantReady := time.Now()
			t.Logf("the member pod started %s after the deletion and was Ready %s after it started; the tenant was Ready %s after it started", started.Sub(deletion).Round(time.Second), ready.Sub(started).Round(time.Second), tenantReady.Sub(started).Round(time.Second))
			logs, err := kubernetes.NewForConfigOrDie(cfg.Client().RESTConfig()).CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: "backup-restore", Timestamps: true}).DoRaw(ctx)
			if err != nil {
				t.Fatal(err)
			}
			deltas, base, took, err := restoreOf(string(logs))
			if err != nil {
				t.Errorf("the log of backup-restore in %s: %v", pod.Name, err)
				for line := range strings.Lines(string(logs)) {
					if strings.Contains(line, "estor") || strings.Contains(line, "snapshot") {
						t.Log(strings.TrimSpace(line))
					}
				}
			} else {
				t.Logf("backup-restore restored %s's etcd from a full snapshot that ends at revision %d through %d delta snapshots in %s", key.Name, base, deltas, took)
				markerRevision, err := strconv.ParseInt(marker.ResourceVersion, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				if base >= markerRevision {
					t.Errorf("the restore started from a full snapshot that ends at revision %d, not before the marker's %d: a scheduled full snapshot was taken after the marker, and the deltas did not bring it back", base, markerRevision)
				}
			}
			access = openTenant(ctx, t, k8s, key)
			until(ctx, t, 2*time.Minute, "the tenant serves the marker and the Event it had", func(ctx context.Context) error {
				restoredMarker := &corev1.ConfigMap{}
				if err := access.client.Get(ctx, client.ObjectKeyFromObject(marker), restoredMarker); err != nil {
					return err
				}
				restoredEvent := &corev1.Event{}
				if err := access.client.Get(ctx, client.ObjectKeyFromObject(event), restoredEvent); err != nil {
					return err
				}
				if restoredMarker.UID != marker.UID || restoredMarker.Data["after"] != marker.Data["after"] || restoredEvent.UID != event.UID || restoredEvent.Message != event.Message {
					return fmt.Errorf("the marker is %s %v and the Event %s %q, want %s %v and %s %q", restoredMarker.UID, restoredMarker.Data, restoredEvent.UID, restoredEvent.Message, marker.UID, marker.Data, event.UID, event.Message)
				}
				return nil
			})
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			closeTenant(ctx, t, k8s, key, deltasClass)
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func currentRevision(ctx context.Context, t *testing.T, tenant client.Client) int64 {
	t.Helper()
	list := &corev1.ConfigMapList{}
	if err := tenant.List(ctx, list, client.InNamespace(metav1.NamespaceDefault)); err != nil {
		t.Fatal(err)
	}
	revision, err := strconv.ParseInt(list.ResourceVersion, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func snapshotLease(ctx context.Context, k8s client.Client, namespace, name string) (int64, error) {
	lease := &coordinationv1.Lease{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, lease); err != nil {
		return 0, err
	}
	return strconv.ParseInt(ptr.Deref(lease.Spec.HolderIdentity, ""), 10, 64)
}

func restoreOf(logs string) (int, int64, time.Duration, error) {
	var started, ended time.Time
	var deltas int
	var base int64
	var based bool
	for line := range strings.Lines(logs) {
		stamp, message, _ := strings.Cut(line, " ")
		at, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil {
			continue
		}
		switch {
		case strings.Contains(message, restoreStarts):
			started = at
		case strings.Contains(message, restoresBase):
			base, based = baseRevision(message)
		case strings.Contains(message, restoreEnds):
			ended = at
		}
		if m := attemptsDeltas.FindStringSubmatch(message); m != nil {
			deltas, _ = strconv.Atoi(m[1])
		}
	}
	switch {
	case started.IsZero() || ended.IsZero() || !based:
		return 0, 0, 0, fmt.Errorf("it holds no complete restore from a named base snapshot: started %v, from a base snapshot %t, ended %v", started, based, ended)
	case deltas < 2:
		return 0, 0, 0, fmt.Errorf("the restore went through %d delta snapshots, want the one that holds the marker and the next", deltas)
	}
	return deltas, base, ended.Sub(started), nil
}
