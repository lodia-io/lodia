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

// Certificates are the five Secrets of a generation of the etcd certificates,
// <cp>-etcd-<certificate>-<generation>: the CA under ca.crt, and the server,
// client, peer and backup-restore certificates under tls.crt and tls.key.
var Certificates = []string{"ca", "server", "client", "peer", "backup-restore"}

// SecretName names one Secret of a certificate generation.
func SecretName(cp, certificate string, generation int) string {
	return fmt.Sprintf("%s-etcd-%s-%d", cp, certificate, generation)
}

func etcdName(cp string) string { return cp + "-etcd" }

func backupSecretName(cp string) string { return cp + "-etcd-backup" }

// render builds the Etcd from the class and the certificate generation, with
// etcd-druid's defaults for everything the class does not set. TLS is set on
// client, peer and backup-restore together: etcd-druid probes the members
// over HTTPS only with backup.tls, while they serve HTTPS whenever client TLS
// is on (docs/decisions/dedicated-etcd-by-default.md). The storage class and
// size come from the current Etcd once it exists: the first is immutable in
// the Etcd, the second a StatefulSet field that cannot change in place. A nil
// class is a recorded class that is gone: the current Etcd keeps every field
// the class gave it.
func render(cp *bryggav1alpha1.ControlPlane, class *bryggav1alpha1.DatastoreClass, generation int, current *druidv1alpha1.Etcd) *druidv1alpha1.Etcd {
	secret := func(certificate string) corev1.SecretReference {
		return corev1.SecretReference{Name: SecretName(cp.Name, certificate, generation)}
	}
	// etcd-druid reads the CA under ca.crt for the members and under
	// bundle.crt for its own calls to the sidecar unless the key is named
	// (etcd-druid@v0.39.0 internal/component/statefulset/builder.go#L468,
	// internal/controller/compaction/snapshot.go#L87).
	ca := druidv1alpha1.SecretReference{SecretReference: secret("ca"), DataKey: new("ca.crt")}
	labels := map[string]string{bryggav1alpha1.ControlPlaneLabel: cp.Name}
	etcd := &druidv1alpha1.Etcd{
		TypeMeta: metav1.TypeMeta{APIVersion: druidv1alpha1.SchemeGroupVersion.String(), Kind: "Etcd"},
		ObjectMeta: metav1.ObjectMeta{
			Name:            etcdName(cp.Name),
			Namespace:       cp.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cp, bryggav1alpha1.GroupVersion.WithKind("ControlPlane"))},
		},
		Spec: druidv1alpha1.EtcdSpec{
			Replicas: 1,
			Labels:   labels,
			Etcd: druidv1alpha1.EtcdConfig{
				ClientUrlTLS: &druidv1alpha1.TLSConfig{TLSCASecretRef: ca, ServerTLSSecretRef: secret("server"), ClientTLSSecretRef: secret("client")},
				PeerUrlTLS:   &druidv1alpha1.PeerTLSConfig{TLSConfig: druidv1alpha1.TLSConfig{TLSCASecretRef: ca, ServerTLSSecretRef: secret("peer")}},
			},
			Backup: druidv1alpha1.BackupSpec{
				TLS: &druidv1alpha1.TLSConfig{TLSCASecretRef: ca, ServerTLSSecretRef: secret("backup-restore"), ClientTLSSecretRef: secret("client")},
			},
		},
	}
	if class == nil {
		keep(etcd, current)
		return etcd
	}
	spec := &class.Spec
	etcd.Annotations = map[string]string{
		ClassAnnotation:           class.Name,
		ClassGenerationAnnotation: strconv.FormatInt(class.Generation, 10),
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
			etcd.Spec.Etcd.DefragmentationSchedule = new(settings.Defragmentation.Schedule)
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
			SecretRef: &corev1.SecretReference{Name: backupSecretName(cp.Name)},
		}
		if backup.Store.Endpoint != "" {
			etcd.Spec.Backup.Store.EndpointOverride = new(backup.Store.Endpoint)
		}
		if backup.FullSnapshotSchedule != "" {
			etcd.Spec.Backup.FullSnapshotSchedule = new(backup.FullSnapshotSchedule)
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

// keep copies from the current Etcd every field render takes from the class,
// the recorded class and its generation included.
func keep(etcd, current *druidv1alpha1.Etcd) {
	for _, key := range []string{ClassAnnotation, ClassGenerationAnnotation} {
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

// nodeAffinity is the node selector as the required node affinity the Etcd
// takes instead.
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

// apply writes the Etcd with server-side apply and returns it as the API
// server holds it, status included.
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

// readiness is DatastoreReady from etcd-druid's report and the StatefulSet it
// runs. The report is read with the checks Gardener makes before it counts an
// Etcd ready (gardener/gardener@v1.154.0
// pkg/component/etcd/etcd/waiter.go#L49-L104): no last errors, no deletion,
// the generation observed, every member updated, ready; Gardener checks the
// last errors first, a deletion is reported first here. Then the StatefulSet
// itself, read from the API server, must mount the certificate generation on
// every replica, since etcd-druid computes ready from its cached StatefulSet
// before it stamps the observed generation. The second result says the
// Etcd's report is complete and only the StatefulSet lags, which nothing
// watched reports.
func readiness(cp string, etcd *druidv1alpha1.Etcd, sts *appsv1.StatefulSet, generation int) (metav1.Condition, bool) {
	not := func(message string) (metav1.Condition, bool) {
		return newCondition(metav1.ConditionFalse, ReasonProvisioning, message), false
	}
	if !etcd.DeletionTimestamp.IsZero() {
		return not(fmt.Sprintf("Etcd %s is being deleted", etcd.Name))
	}
	if len(etcd.Status.LastErrors) > 0 {
		errs := make([]string, 0, len(etcd.Status.LastErrors))
		for _, lastError := range etcd.Status.LastErrors {
			errs = append(errs, string(lastError.Code)+": "+lastError.Description)
		}
		return not(fmt.Sprintf("etcd-druid reports errors on Etcd %s: %s", etcd.Name, strings.Join(errs, "; ")))
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
	return newCondition(metav1.ConditionTrue, ReasonReady,
		fmt.Sprintf("etcd-druid reports Etcd %s ready on certificate generation %d, %d of %d replicas", etcd.Name, generation, etcd.Status.ReadyReplicas, etcd.Spec.Replicas)), false
}

// mounts says whether the StatefulSet's pod template names every Secret of
// the certificate generation.
func mounts(sts *appsv1.StatefulSet, cp string, generation int) bool {
	var mounted []string
	for _, volume := range sts.Spec.Template.Spec.Volumes {
		if volume.Secret != nil {
			mounted = append(mounted, volume.Secret.SecretName)
		}
	}
	for _, certificate := range Certificates {
		if !slices.Contains(mounted, SecretName(cp, certificate, generation)) {
			return false
		}
	}
	return true
}

// etcdChanged passes the Etcd events that can move DatastoreReady or
// status.datastore and drops etcd-druid's status refresh every 15 seconds,
// which moves timestamps alone.
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
	for _, lastError := range etcd.Status.LastErrors {
		fmt.Fprintf(&b, "%s:%s;", lastError.Code, lastError.Description)
	}
	for _, c := range etcd.Status.Conditions {
		fmt.Fprintf(&b, "%s=%s/%s;", c.Type, c.Status, c.Reason)
	}
	return b.String()
}
