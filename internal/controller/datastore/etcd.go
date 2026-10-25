// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
)

// TLS is set on client, peer and backup-restore together: etcd-druid probes
// the members over HTTPS only with backup.tls, while they serve HTTPS whenever
// client TLS is on (docs/decisions/dedicated-etcd-by-default.md). The storage
// class and size come from the current Etcd once it exists: the first is
// immutable in the Etcd, the second a StatefulSet field that cannot change in
// place. A nil class is a recorded class that is gone.
func render(cp *bryggav1alpha1.ControlPlane, class *bryggav1alpha1.DatastoreClass, generation int, current *druidv1alpha1.Etcd) *druidv1alpha1.Etcd {
	secret := func(certificate bryggav1alpha1.EtcdCertificateRole) corev1.SecretReference {
		return corev1.SecretReference{Name: bryggav1alpha1.EtcdCertificateSecretName(cp.Name, certificate, generation)}
	}
	// etcd-druid reads the CA under ca.crt for the members and under
	// bundle.crt for its own calls to the sidecar unless the key is named
	// (etcd-druid@v0.39.0 internal/component/statefulset/builder.go#L468,
	// internal/controller/compaction/snapshot.go#L87).
	ca := druidv1alpha1.SecretReference{SecretReference: secret(bryggav1alpha1.EtcdCARole), DataKey: new(bryggav1alpha1.CACertKey)}
	labels := map[string]string{bryggav1alpha1.ControlPlaneLabel: cp.Name}
	etcd := &druidv1alpha1.Etcd{
		TypeMeta: metav1.TypeMeta{APIVersion: druidv1alpha1.SchemeGroupVersion.String(), Kind: "Etcd"},
		ObjectMeta: metav1.ObjectMeta{
			Name:            bryggav1alpha1.EtcdName(cp.Name),
			Namespace:       cp.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cp, bryggav1alpha1.GroupVersion.WithKind("ControlPlane"))},
		},
		Spec: druidv1alpha1.EtcdSpec{
			Replicas: 1,
			Labels:   labels,
			Etcd: druidv1alpha1.EtcdConfig{
				ClientUrlTLS: &druidv1alpha1.TLSConfig{TLSCASecretRef: ca, ServerTLSSecretRef: secret(bryggav1alpha1.EtcdServerRole), ClientTLSSecretRef: secret(bryggav1alpha1.EtcdClientRole)},
				PeerUrlTLS:   &druidv1alpha1.PeerTLSConfig{TLSConfig: druidv1alpha1.TLSConfig{TLSCASecretRef: ca, ServerTLSSecretRef: secret(bryggav1alpha1.EtcdPeerRole)}},
			},
			Backup: druidv1alpha1.BackupSpec{
				TLS: &druidv1alpha1.TLSConfig{TLSCASecretRef: ca, ServerTLSSecretRef: secret(bryggav1alpha1.EtcdBackupRestoreRole), ClientTLSSecretRef: secret(bryggav1alpha1.EtcdClientRole)},
			},
		},
	}
	if class == nil {
		keep(etcd, current)
		return etcd
	}
	spec := &class.Spec
	etcd.Annotations = map[string]string{
		bryggav1alpha1.DatastoreClassAnnotation:           class.Name,
		bryggav1alpha1.DatastoreClassGenerationAnnotation: strconv.FormatInt(class.Generation, 10),
	}
	etcd.Spec.StorageCapacity = new(spec.Storage.Size)
	if spec.Storage.ClassName != "" {
		etcd.Spec.StorageClass = new(spec.Storage.ClassName)
	}
	if current != nil {
		etcd.Spec.StorageClass = current.Spec.StorageClass
		etcd.Spec.StorageCapacity = current.Spec.StorageCapacity
	}
	if settings := spec.Etcd; settings != nil {
		etcd.Spec.Etcd.Quota = settings.QuotaBackendBytes
		etcd.Spec.Etcd.Resources = settings.Resources
		if settings.Defragmentation != nil {
			etcd.Spec.Etcd.DefragmentationSchedule = new(string(settings.Defragmentation.Schedule))
		}
		if compaction := settings.AutoCompaction; compaction != nil {
			if compaction.Mode != "" {
				etcd.Spec.Common.AutoCompactionMode = new(druidv1alpha1.CompactionMode(compaction.Mode))
			}
			if compaction.Retention != "" {
				etcd.Spec.Common.AutoCompactionRetention = new(compaction.Retention)
			}
		}
	}
	if backup := spec.Backup; backup != nil {
		etcd.Spec.Backup.Store = &druidv1alpha1.StoreSpec{
			Provider:  new(druidv1alpha1.StorageProvider(backup.Store.Provider)),
			Container: new(backup.Store.Bucket),
			Prefix:    path.Join(backup.Store.Prefix, cp.Namespace, cp.Name, string(cp.UID)),
			SecretRef: &corev1.SecretReference{Name: bryggav1alpha1.EtcdBackupSecret.Name(cp.Name)},
		}
		if backup.Store.Endpoint != "" {
			etcd.Spec.Backup.Store.EndpointOverride = new(backup.Store.Endpoint)
		}
		if backup.FullSnapshotSchedule != "" {
			etcd.Spec.Backup.FullSnapshotSchedule = new(string(backup.FullSnapshotSchedule))
		}
		etcd.Spec.Backup.DeltaSnapshotPeriod = backup.DeltaSnapshotPeriod
		if backup.Retention != nil {
			etcd.Spec.Backup.GarbageCollectionPolicy = new(druidv1alpha1.GarbageCollectionPolicy(druidv1alpha1.GarbageCollectionPolicyLimitBased))
			etcd.Spec.Backup.MaxBackupsLimitBasedGC = new(backup.Retention.Days)
		}
	}
	if placement := spec.Placement; placement != nil {
		etcd.Spec.SchedulingConstraints.Tolerations = placement.Tolerations
		if placement.PriorityClassName != "" {
			etcd.Spec.PriorityClassName = new(placement.PriorityClassName)
		}
		if len(placement.NodeSelector) > 0 {
			etcd.Spec.SchedulingConstraints.Affinity = nodeAffinity(placement.NodeSelector)
		}
	}
	return etcd
}

// keep must copy every field render takes from the class.
func keep(etcd, current *druidv1alpha1.Etcd) {
	for _, key := range []string{bryggav1alpha1.DatastoreClassAnnotation, bryggav1alpha1.DatastoreClassGenerationAnnotation} {
		if value, ok := current.Annotations[key]; ok {
			metav1.SetMetaDataAnnotation(&etcd.ObjectMeta, key, value)
		}
	}
	etcd.Spec.StorageClass = current.Spec.StorageClass
	etcd.Spec.StorageCapacity = current.Spec.StorageCapacity
	etcd.Spec.Etcd.Quota = current.Spec.Etcd.Quota
	etcd.Spec.Etcd.Resources = current.Spec.Etcd.Resources
	etcd.Spec.Etcd.DefragmentationSchedule = current.Spec.Etcd.DefragmentationSchedule
	etcd.Spec.Common.AutoCompactionMode = current.Spec.Common.AutoCompactionMode
	etcd.Spec.Common.AutoCompactionRetention = current.Spec.Common.AutoCompactionRetention
	etcd.Spec.Backup.Store = current.Spec.Backup.Store
	etcd.Spec.Backup.FullSnapshotSchedule = current.Spec.Backup.FullSnapshotSchedule
	etcd.Spec.Backup.DeltaSnapshotPeriod = current.Spec.Backup.DeltaSnapshotPeriod
	etcd.Spec.Backup.GarbageCollectionPolicy = current.Spec.Backup.GarbageCollectionPolicy
	etcd.Spec.Backup.MaxBackupsLimitBasedGC = current.Spec.Backup.MaxBackupsLimitBasedGC
	etcd.Spec.SchedulingConstraints.Affinity = current.Spec.SchedulingConstraints.Affinity
	etcd.Spec.SchedulingConstraints.Tolerations = current.Spec.SchedulingConstraints.Tolerations
	etcd.Spec.PriorityClassName = current.Spec.PriorityClassName
}

// The Etcd has no node selector; a required node affinity says the same.
func nodeAffinity(nodeSelector map[string]string) *corev1.Affinity {
	var requirements []corev1.NodeSelectorRequirement
	for _, key := range slices.Sorted(maps.Keys(nodeSelector)) {
		requirements = append(requirements, corev1.NodeSelectorRequirement{Key: key, Operator: corev1.NodeSelectorOpIn, Values: []string{nodeSelector[key]}})
	}
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: requirements}},
		},
	}}
}

func (r *Reconciler) apply(ctx context.Context, etcd *druidv1alpha1.Etcd) (*druidv1alpha1.Etcd, error) {
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(etcd)
	if err != nil {
		return nil, err
	}
	// The typed Etcd marshals its deprecated selector as null and its status
	// as an empty object; neither is a field to apply.
	unstructured.RemoveNestedField(content, "spec", "selector")
	unstructured.RemoveNestedField(content, "status")
	applied := &unstructured.Unstructured{Object: content}
	if err := r.Apply(ctx, client.ApplyConfigurationFromUnstructured(applied), client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		return nil, err
	}
	result := &druidv1alpha1.Etcd{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(applied.Object, result); err != nil {
		return nil, err
	}
	return result, nil
}

// etcd-druid's report is read with the checks Gardener makes before it counts
// an Etcd ready (gardener/gardener@v1.154.0
// pkg/component/etcd/etcd/waiter.go#L49-L104) but the last errors: Gardener's
// one-shot wait retries on them, while etcd-druid clears them at the start of
// every spec reconcile, every 15 seconds, and sets them again on a failure
// (etcd-druid@v0.39.0 internal/controller/utils/etcdstatus.go#L45-L93), so a
// condition would flip with them; a failed sync shows anyway, since etcd-druid
// stamps the observed generation only after it. Then the StatefulSet, read
// from the API server: etcd-druid computes ready from its cached StatefulSet
// before it stamps the observed generation. The second result is a
// StatefulSet that lags a complete report, which no watch brings back.
func readiness(cp string, etcd *druidv1alpha1.Etcd, sts *appsv1.StatefulSet, generation int, missing []string) (metav1.Condition, bool) {
	not := func(message string) (metav1.Condition, bool) {
		return newCondition(metav1.ConditionFalse, bryggav1alpha1.ReasonProvisioning, message), false
	}
	if !etcd.DeletionTimestamp.IsZero() {
		return not(fmt.Sprintf("Etcd %s is being deleted", etcd.Name))
	}
	if len(missing) > 0 {
		return not(fmt.Sprintf("Secrets Etcd %s mounts do not exist: %s", etcd.Name, strings.Join(missing, ", ")))
	}
	if observed := ptr.Deref(etcd.Status.ObservedGeneration, 0); observed != etcd.Generation {
		return not(fmt.Sprintf("etcd-druid has not observed generation %d of Etcd %s, observedGeneration %d", etcd.Generation, etcd.Name, observed))
	}
	updated := slices.IndexFunc(etcd.Status.Conditions, func(c druidv1alpha1.Condition) bool { return c.Type == druidv1alpha1.ConditionTypeAllMembersUpdated })
	switch {
	case updated < 0:
		return not(fmt.Sprintf("etcd-druid reports no %s condition on Etcd %s", druidv1alpha1.ConditionTypeAllMembersUpdated, etcd.Name))
	case etcd.Status.Conditions[updated].Status != druidv1alpha1.ConditionTrue:
		c := etcd.Status.Conditions[updated]
		return not(fmt.Sprintf("etcd-druid reports %s=%s on Etcd %s: %s", c.Type, c.Status, etcd.Name, c.Message))
	}
	if !ptr.Deref(etcd.Status.Ready, false) || etcd.Status.ReadyReplicas != etcd.Spec.Replicas {
		return not(fmt.Sprintf("etcd-druid reports Etcd %s not ready, %d of %d replicas", etcd.Name, etcd.Status.ReadyReplicas, etcd.Spec.Replicas))
	}
	if sts == nil {
		condition, _ := not(fmt.Sprintf("StatefulSet %s does not exist", etcd.Name))
		return condition, true
	}
	if !mounts(sts, cp, generation) {
		condition, _ := not(fmt.Sprintf("StatefulSet %s does not mount certificate generation %d yet", sts.Name, generation))
		return condition, true
	}
	if sts.Status.ObservedGeneration < sts.Generation {
		condition, _ := not(fmt.Sprintf("the StatefulSet controller has not observed generation %d of StatefulSet %s, observedGeneration %d",
			sts.Generation, sts.Name, sts.Status.ObservedGeneration))
		return condition, true
	}
	replicas := ptr.Deref(sts.Spec.Replicas, 1)
	if sts.Status.UpdatedReplicas != replicas || sts.Status.ReadyReplicas != replicas {
		condition, _ := not(fmt.Sprintf("StatefulSet %s: %d of %d replicas updated, %d ready", sts.Name, sts.Status.UpdatedReplicas, replicas, sts.Status.ReadyReplicas))
		return condition, true
	}
	return newCondition(metav1.ConditionTrue, bryggav1alpha1.ReasonReady,
		fmt.Sprintf("etcd-druid reports Etcd %s ready on certificate generation %d, %d of %d replicas", etcd.Name, generation, etcd.Status.ReadyReplicas, etcd.Spec.Replicas)), false
}

func mounts(sts *appsv1.StatefulSet, cp string, generation int) bool {
	var mounted []string
	for _, volume := range sts.Spec.Template.Spec.Volumes {
		if volume.Secret != nil {
			mounted = append(mounted, volume.Secret.SecretName)
		}
	}
	for _, certificate := range bryggav1alpha1.EtcdCertificateRoles() {
		if !slices.Contains(mounted, bryggav1alpha1.EtcdCertificateSecretName(cp, certificate, generation)) {
			return false
		}
	}
	return true
}

// etcd-druid refreshes every Etcd's status every 15 seconds
// (etcd-druid@v0.39.0 api/config/v1alpha1/defaults.go#L104, charts/values.yaml#L63)
// with new timestamps alone; only a change readiness or status.datastore reads
// gets a pass.
func etcdChanged() predicate.Funcs {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		before, ok := e.ObjectOld.(*druidv1alpha1.Etcd)
		after, ok2 := e.ObjectNew.(*druidv1alpha1.Etcd)
		return !ok || !ok2 || readinessInput(before) != readinessInput(after)
	}}
}

func readinessInput(etcd *druidv1alpha1.Etcd) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d %t %d %t %d/%d %d;", etcd.Generation, !etcd.DeletionTimestamp.IsZero(), ptr.Deref(etcd.Status.ObservedGeneration, 0),
		ptr.Deref(etcd.Status.Ready, false), etcd.Status.ReadyReplicas, etcd.Spec.Replicas, len(etcd.Status.Members))
	for _, c := range etcd.Status.Conditions {
		fmt.Fprintf(&b, "%s=%s/%s;", c.Type, c.Status, c.Reason)
	}
	return b.String()
}
