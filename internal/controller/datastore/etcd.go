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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

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
// the Etcd, the second a StatefulSet field that cannot change in place.
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
	spec := &class.Spec
	etcd := &druidv1alpha1.Etcd{
		TypeMeta: metav1.TypeMeta{APIVersion: druidv1alpha1.SchemeGroupVersion.String(), Kind: "Etcd"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      etcdName(cp.Name),
			Namespace: cp.Namespace,
			Labels:    labels,
			Annotations: map[string]string{
				ClassAnnotation:           class.Name,
				ClassGenerationAnnotation: strconv.FormatInt(class.Generation, 10),
			},
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
			StorageCapacity: new(spec.Storage.Size),
		},
	}
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

// readiness is DatastoreReady as etcd-druid reports the Etcd: True once it has
// observed the current generation and every replica is ready.
func readiness(etcd *druidv1alpha1.Etcd) metav1.Condition {
	observed := ptr.Deref(etcd.Status.ObservedGeneration, 0)
	ready := ptr.Deref(etcd.Status.Ready, false)
	if observed == etcd.Generation && ready && etcd.Status.ReadyReplicas == etcd.Spec.Replicas {
		return newCondition(metav1.ConditionTrue, ReasonReady,
			fmt.Sprintf("etcd-druid reports Etcd %s ready, %d of %d replicas", etcd.Name, etcd.Status.ReadyReplicas, etcd.Spec.Replicas))
	}
	message := fmt.Sprintf("etcd-druid has not reported Etcd %s ready for generation %d: observedGeneration %d, ready %t, %d of %d replicas ready",
		etcd.Name, etcd.Generation, observed, ready, etcd.Status.ReadyReplicas, etcd.Spec.Replicas)
	if len(etcd.Status.LastErrors) > 0 {
		errs := make([]string, 0, len(etcd.Status.LastErrors))
		for _, lastError := range etcd.Status.LastErrors {
			errs = append(errs, string(lastError.Code)+": "+lastError.Description)
		}
		message += "; last errors: " + strings.Join(errs, "; ")
	}
	return newCondition(metav1.ConditionFalse, ReasonProvisioning, message)
}
