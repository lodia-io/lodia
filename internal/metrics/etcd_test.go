// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package metrics_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	datastorectrl "go.lodia.io/lodia/internal/controller/datastore"
)

func etcdOf(cp *lodiav1alpha1.ControlPlane, quota string, period time.Duration, store, formed bool) *druidv1alpha1.Etcd {
	etcd := &druidv1alpha1.Etcd{
		ObjectMeta: metav1.ObjectMeta{
			Name: lodiav1alpha1.EtcdName(cp.Name), Namespace: cp.Namespace, UID: types.UID("etcd-of-" + string(cp.UID)),
			Labels:          map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cp, lodiav1alpha1.GroupVersion.WithKind("ControlPlane"))},
		},
		Spec: druidv1alpha1.EtcdSpec{
			Replicas: 1,
			Labels:   map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name},
			Etcd:     druidv1alpha1.EtcdConfig{Quota: new(resource.MustParse(quota))},
			Backup:   druidv1alpha1.BackupSpec{DeltaSnapshotPeriod: &metav1.Duration{Duration: period}},
		},
	}
	if store {
		etcd.Spec.Backup.Store = &druidv1alpha1.StoreSpec{Provider: new(druidv1alpha1.StorageProvider("S3")), Container: new("lodia-backups"), Prefix: "prod/" + string(cp.UID)}
	}
	if formed {
		etcd.Annotations = map[string]string{lodiav1alpha1.EtcdFormedAnnotation: string(etcd.UID)}
	}
	return etcd
}

func goldenEtcds(tenants []client.Object) []client.Object {
	var out []client.Object
	for _, obj := range tenants {
		cp := obj.(*lodiav1alpha1.ControlPlane)
		var etcd *druidv1alpha1.Etcd
		switch cp.Name {
		case "edge":
			etcd = etcdOf(cp, "8Gi", 5*time.Minute, true, true)
		case "restored":
			etcd = etcdOf(cp, "4Gi", time.Hour, true, true)
		case "failed":
			etcd = etcdOf(cp, "2Gi", 5*time.Minute, false, true)
		case "queued":
			etcd = etcdOf(cp, "4Gi", 500*time.Millisecond, true, true)
		case "upgrading":
			etcd = etcdOf(cp, "4Gi", 0, true, true)
		case "paused", "deleting", "waiting":
			etcd = etcdOf(cp, "4Gi", 5*time.Minute, true, true)
		case "rolling":
			etcd = etcdOf(cp, "4Gi", 30*time.Second, true, true)
		case "starting":
			etcd = etcdOf(cp, "4Gi", 5*time.Minute, true, false)
		case "overdue":
			etcd = etcdOf(cp, "4Gi", 5*time.Minute, true, true)
			etcd.OwnerReferences[0].UID = "a-predecessor"
		default:
			continue
		}
		out = append(out, etcd)
	}
	return out
}

var goldenSamples = map[string]datastorectrl.EtcdSample{
	"edge":      {HasLeader: true, DBSize: new(int64(52428800))},
	"restored":  {HasLeader: true, DBSize: new(int64(1073741824))},
	"failed":    {HasLeader: true, DBSize: new(int64(2147487744)), Alarms: []string{lodiav1alpha1.MetricAlarmNoSpace}},
	"queued":    {HasLeader: true, DBSize: new(int64(31457280))},
	"upgrading": {HasLeader: true, DBSize: new(int64(41943040)), Alarms: []string{lodiav1alpha1.MetricAlarmCorrupt}},
	"rolling":   {DBSize: new(int64(62914560))},
	"waiting":   {},
}

func samplesOf(objs []client.Object) func(types.NamespacedName, types.UID) (datastorectrl.EtcdSample, bool) {
	uids := map[types.NamespacedName]types.UID{}
	for _, obj := range objs {
		if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
			uids[client.ObjectKeyFromObject(cp)] = cp.UID
		}
	}
	return func(key types.NamespacedName, uid types.UID) (datastorectrl.EtcdSample, bool) {
		sample, ok := goldenSamples[key.Name]
		return sample, ok && uids[key] == uid
	}
}

func TestTheQuotaAndThePeriodComeFromTheEtcdWithoutASample(t *testing.T) {
	young := controlPlane("young", 10)
	short := controlPlane("short", 11)
	storeless := controlPlane("storeless", 12)
	predecessor := controlPlane("successor", 13)
	leftover := etcdOf(predecessor, "4Gi", 5*time.Minute, true, true)
	leftover.OwnerReferences[0].UID = "a-predecessor"
	c := collector(young, etcdOf(young, "8Gi", 5*time.Minute, true, false),
		short, etcdOf(short, "2Gi", 999*time.Millisecond, true, true),
		storeless, etcdOf(storeless, "2Gi", time.Minute, false, true),
		predecessor, leftover)
	want := `
# HELP lodia_controlplane_datastore_delta_snapshot_period_seconds The Etcd's spec.backup.deltaSnapshotPeriod; absent without a backup store and below 1 s, where no delta snapshot is taken.
# TYPE lodia_controlplane_datastore_delta_snapshot_period_seconds gauge
lodia_controlplane_datastore_delta_snapshot_period_seconds{controlplane="young",namespace="tenants"} 300
# HELP lodia_controlplane_datastore_quota_bytes The Etcd's spec.etcd.quota: the database size at which etcd arms NOSPACE.
# TYPE lodia_controlplane_datastore_quota_bytes gauge
lodia_controlplane_datastore_quota_bytes{controlplane="short",namespace="tenants"} 2.147483648e+09
lodia_controlplane_datastore_quota_bytes{controlplane="storeless",namespace="tenants"} 2.147483648e+09
lodia_controlplane_datastore_quota_bytes{controlplane="young",namespace="tenants"} 8.589934592e+09
`
	names := []string{"lodia_controlplane_datastore_quota_bytes", "lodia_controlplane_datastore_delta_snapshot_period_seconds",
		"lodia_controlplane_datastore_db_size_bytes", "lodia_controlplane_datastore_has_leader", "lodia_controlplane_datastore_alarm"}
	if err := testutil.CollectAndCompare(c, strings.NewReader(want), names...); err != nil {
		t.Error(err)
	}
}

func TestACollectionWhoseEtcdListHangsEndsWithinTheDeadline(t *testing.T) {
	requireEnv(t)
	release := make(chan struct{})
	reader := startCache(t, func(req *http.Request, next http.RoundTripper) (*http.Response, error) {
		if strings.HasSuffix(req.URL.Path, "/etcds") {
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-release:
				return nil, errors.New("released")
			}
		}
		return next.RoundTrip(req)
	})
	t.Cleanup(func() { close(release) })
	create(t, namespace(t), gatewayHA("unlisted", 30))
	c := collectorOf(reader, elected())
	start := time.Now()
	collected := make(chan int, 1)
	go func() { collected <- testutil.CollectAndCount(c) }()
	select {
	case n := <-collected:
		if elapsed := time.Since(start); elapsed > 6*time.Second {
			t.Errorf("the collection took %s, want at most the 5 s deadline", elapsed)
		}
		if n != 0 {
			t.Errorf("a collection whose Etcd list does not answer emits %d series, want none", n)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the collection did not return in 15 s")
	}
}
