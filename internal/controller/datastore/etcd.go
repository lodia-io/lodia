// Copyright 2026 The Lodia Authors.
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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// allowUnhealthyPodEviction on an Etcd lets its PodDisruptionBudget evict an
// unhealthy member always
// (etcd-druid@v0.39.0 internal/component/poddistruptionbudget/poddisruptionbudget.go#L36-L38).
const allowUnhealthyPodEviction = "resources.druid.gardener.cloud/allow-unhealthy-pod-eviction"

// TLS is set on client, peer and backup-restore together: etcd-druid probes
// the members over HTTPS only with backup.tls, while they serve HTTPS whenever
// client TLS is on (docs/decisions/dedicated-etcd-by-default.md). The storage
// class and size come from the current Etcd once it exists: the first is
// immutable in the Etcd, the second a StatefulSet field that cannot change in
// place. Without one, the size is that of the smallest claim left, which the
// StatefulSet binds again by name. The quota is capped at half the size. A nil
// class is a recorded class that is gone. The backup prefix holds UIDs and no
// name: etcd-backup-restore takes the last v1/ anywhere in a key for its
// layout and ignores the snapshots it misreads
// (etcd-backup-restore@v0.45.1 pkg/snapstore/snapshot.go#L39-L59).
func render(cp *lodiav1alpha1.ControlPlane, class *lodiav1alpha1.DatastoreClass, namespaceUID types.UID, generation int, current *druidv1alpha1.Etcd, claims []corev1.PersistentVolumeClaim, sts *appsv1.StatefulSet) (*druidv1alpha1.Etcd, string) {
	secret := func(certificate lodiav1alpha1.EtcdCertificateRole) corev1.SecretReference {
		return corev1.SecretReference{Name: lodiav1alpha1.EtcdCertificateSecretName(cp.Name, certificate, generation)}
	}
	// etcd-druid reads the CA under ca.crt for the members and under
	// bundle.crt for its own calls to the sidecar unless the key is named
	// (etcd-druid@v0.39.0 internal/component/statefulset/builder.go#L468,
	// internal/controller/compaction/snapshot.go#L87).
	ca := druidv1alpha1.SecretReference{SecretReference: secret(lodiav1alpha1.EtcdCARole), DataKey: new(lodiav1alpha1.CACertKey)}
	labels := map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name}
	etcd := &druidv1alpha1.Etcd{
		TypeMeta: metav1.TypeMeta{APIVersion: druidv1alpha1.SchemeGroupVersion.String(), Kind: "Etcd"},
		ObjectMeta: metav1.ObjectMeta{
			Name:            lodiav1alpha1.EtcdName(cp.Name),
			Namespace:       cp.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cp, lodiav1alpha1.GroupVersion.WithKind("ControlPlane"))},
		},
		Spec: druidv1alpha1.EtcdSpec{
			Replicas: memberCount(cp),
			Labels:   labels,
			Etcd: druidv1alpha1.EtcdConfig{
				ClientUrlTLS: &druidv1alpha1.TLSConfig{TLSCASecretRef: ca, ServerTLSSecretRef: secret(lodiav1alpha1.EtcdServerRole), ClientTLSSecretRef: secret(lodiav1alpha1.EtcdClientRole)},
				PeerUrlTLS:   &druidv1alpha1.PeerTLSConfig{TLSConfig: druidv1alpha1.TLSConfig{TLSCASecretRef: ca, ServerTLSSecretRef: secret(lodiav1alpha1.EtcdPeerRole)}},
			},
			Backup: druidv1alpha1.BackupSpec{
				TLS: &druidv1alpha1.TLSConfig{TLSCASecretRef: ca, ServerTLSSecretRef: secret(lodiav1alpha1.EtcdBackupRestoreRole), ClientTLSSecretRef: secret(lodiav1alpha1.EtcdClientRole)},
			},
		},
	}
	// The apply names the UID it was rendered from, so a pass whose cache
	// still holds a deleted Etcd creates nothing from it
	// (k8s.io/apiserver@v0.37.1 pkg/endpoints/handlers/patch.go#L608-L618).
	if current != nil {
		etcd.UID = current.UID
	}
	switch ha := cp.Spec.HighAvailability; {
	case membersHeld(cp, current, sts):
		etcd.Spec.Replicas = current.Spec.Replicas
		etcd.Spec.SchedulingConstraints.TopologySpreadConstraints = current.Spec.SchedulingConstraints.TopologySpreadConstraints
		if value, ok := current.Annotations[allowUnhealthyPodEviction]; ok {
			metav1.SetMetaDataAnnotation(&etcd.ObjectMeta, allowUnhealthyPodEviction, value)
		}
	case ha != nil:
		metav1.SetMetaDataAnnotation(&etcd.ObjectMeta, allowUnhealthyPodEviction, "")
		etcd.Spec.SchedulingConstraints.TopologySpreadConstraints = spread(ha.FailureTolerance, memberLabels(etcd))
	}
	if current != nil && (formed(current) || current.Status.ReadyReplicas > 0) {
		metav1.SetMetaDataAnnotation(&etcd.ObjectMeta, lodiav1alpha1.EtcdFormedAnnotation, string(current.UID))
	}
	if class == nil {
		keep(etcd, current)
		return etcd, ""
	}
	spec := &class.Spec
	metav1.SetMetaDataAnnotation(&etcd.ObjectMeta, lodiav1alpha1.DatastoreClassAnnotation, class.Name)
	metav1.SetMetaDataAnnotation(&etcd.ObjectMeta, lodiav1alpha1.DatastoreClassGenerationAnnotation, strconv.FormatInt(class.Generation, 10))
	if spec.Storage.ClassName != "" {
		etcd.Spec.StorageClass = new(spec.Storage.ClassName)
	}
	volume := spec.Storage.Size
	if current != nil {
		etcd.Spec.StorageClass = current.Spec.StorageClass
		volume = ptr.Deref(current.Spec.StorageCapacity, volume)
	} else {
		var smallest *resource.Quantity
		for _, claim := range claims {
			// The StatefulSet never attaches a claim being deleted: it creates
			// a new one from its template once the old one is gone
			// (kubernetes/kubernetes@v1.35.0 pkg/controller/statefulset/stateful_pod_control.go#L337-L352).
			if claim.DeletionTimestamp != nil {
				continue
			}
			if size := claimSize(claim); smallest == nil || size.Cmp(*smallest) < 0 {
				smallest = &size
			}
		}
		if smallest != nil {
			volume = *smallest
		}
	}
	etcd.Spec.StorageCapacity = &volume
	quota := resource.MustParse(lodiav1alpha1.DefaultQuotaBackendBytes)
	if spec.Etcd != nil && spec.Etcd.QuotaBackendBytes != nil {
		quota = spec.Etcd.QuotaBackendBytes.DeepCopy()
	}
	var capped string
	twice := quota.DeepCopy()
	twice.Add(quota)
	if twice.Cmp(volume) > 0 {
		half := resource.NewQuantity(volume.Value()/2, volume.Format)
		capped = fmt.Sprintf("DatastoreClass %s's quota %s is capped at %s: it needs %s of volume and Etcd %s's is %s",
			class.Name, quota.String(), half.String(), twice.String(), etcd.Name, volume.String())
		quota = *half
	}
	etcd.Spec.Etcd.Quota = &quota
	if settings := spec.Etcd; settings != nil {
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
			Prefix:    path.Join(backup.Store.Prefix, string(namespaceUID), "controlplanes", string(cp.UID)),
			SecretRef: &corev1.SecretReference{Name: lodiav1alpha1.EtcdBackupSecret.Name(cp.Name)},
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
			etcd.Spec.Backup.MaxBackupsLimitBasedGC = new(backup.Retention.FullSnapshots)
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
	return etcd, capped
}

func claimSize(claim corev1.PersistentVolumeClaim) resource.Quantity {
	if size, ok := claim.Status.Capacity[corev1.ResourceStorage]; ok {
		return size
	}
	return claim.Spec.Resources.Requests[corev1.ResourceStorage]
}

func memberCount(cp *lodiav1alpha1.ControlPlane) int32 {
	if cp.Spec.HighAvailability != nil {
		return 3
	}
	return 1
}

// membersHeld keeps the Etcd's members, spread and eviction annotation as they
// are (docs/README.md, the highAvailability row of the Etcd's fields).
func membersHeld(cp *lodiav1alpha1.ControlPlane, current *druidv1alpha1.Etcd, sts *appsv1.StatefulSet) bool {
	if current == nil {
		return false
	}
	if !druidv1alpha1.HasScaleOperationCompleted(current) {
		return true
	}
	if memberCount(cp) < current.Spec.Replicas {
		_, ready := keptMember(current)
		return !ready
	}
	updated := updatedMembers(current, sts)
	return memberCount(cp) > current.Spec.Replicas && (updated == nil || *updated < current.Spec.Replicas)
}

// keptMember names the member a removal of highAvailability keeps and says
// whether etcd-druid lists it ready.
func keptMember(etcd *druidv1alpha1.Etcd) (string, bool) {
	name := druidv1alpha1.GetOrdinalPodName(etcd.ObjectMeta, 0)
	return name, slices.ContainsFunc(etcd.Status.Members, func(member druidv1alpha1.EtcdMemberStatus) bool {
		return member.Name == name && member.Status == druidv1alpha1.EtcdMemberStatusReady
	})
}

// spread puts the members on three nodes, and with Zone in three zones too;
// a member replaced in place keeps its volume's domain.
func spread(tolerance lodiav1alpha1.FailureTolerance, members map[string]string) []corev1.TopologySpreadConstraint {
	keys := []string{corev1.LabelHostname}
	if tolerance == "Zone" {
		keys = append(keys, corev1.LabelTopologyZone)
	}
	var out []corev1.TopologySpreadConstraint
	for _, key := range keys {
		out = append(out, corev1.TopologySpreadConstraint{
			MaxSkew:           1,
			TopologyKey:       key,
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector:     &metav1.LabelSelector{MatchLabels: members},
			MinDomains:        new(int32(3)),
			NodeTaintsPolicy:  new(corev1.NodeInclusionPolicyHonor),
		})
	}
	return out
}

// memberLabels select the Etcd's members, not its compaction Job's pods.
func memberLabels(etcd *druidv1alpha1.Etcd) map[string]string {
	labels := druidv1alpha1.GetDefaultLabels(etcd.ObjectMeta)
	labels[druidv1alpha1.LabelAppNameKey] = etcd.Name
	return labels
}

// keep must copy every field render takes from the class.
func keep(etcd, current *druidv1alpha1.Etcd) {
	for _, key := range []string{lodiav1alpha1.DatastoreClassAnnotation, lodiav1alpha1.DatastoreClassGenerationAnnotation} {
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
	// as an empty object; neither is a field to apply. Nor are empty
	// scheduling constraints: applied as an empty object where the apply set
	// some before, they come back null, which the schema refuses.
	unstructured.RemoveNestedField(content, "spec", "selector")
	unstructured.RemoveNestedField(content, "status")
	if constraints, _, _ := unstructured.NestedMap(content, "spec", "schedulingConstraints"); len(constraints) == 0 {
		unstructured.RemoveNestedField(content, "spec", "schedulingConstraints")
	}
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

// readiness reports quorum (docs/README.md, A tenant namespace), which one
// ready member shows, since a member is ready only while it answers a
// linearizable read. Without one, an etcd marked formed has lost it, and any
// other is on its way.
func readiness(etcd *druidv1alpha1.Etcd, missing []string) metav1.Condition {
	not := func(message string) metav1.Condition {
		return newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonProvisioning, message)
	}
	if !etcd.DeletionTimestamp.IsZero() {
		return not(fmt.Sprintf("Etcd %s is being deleted", etcd.Name))
	}
	if len(missing) > 0 {
		return not(fmt.Sprintf("Secrets Etcd %s mounts do not exist: %s", etcd.Name, strings.Join(missing, ", ")))
	}
	observed := ptr.Deref(etcd.Status.ObservedGeneration, 0)
	ready := min(etcd.Status.ReadyReplicas, etcd.Spec.Replicas)
	switch {
	case ready > 0 && !druidv1alpha1.HasScaleOperationCompleted(etcd):
		return newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonReady,
			fmt.Sprintf("Etcd %s has quorum, %d of %d members ready, while etcd-druid scales it", etcd.Name, ready, etcd.Spec.Replicas))
	case ready > 0:
		return newCondition(metav1.ConditionTrue, lodiav1alpha1.ReasonReady, fmt.Sprintf("Etcd %s has quorum, %d of %d members ready", etcd.Name, ready, etcd.Spec.Replicas))
	case formed(etcd):
		return newCondition(metav1.ConditionFalse, lodiav1alpha1.ReasonQuorumLost, fmt.Sprintf("Etcd %s has %d of %d members ready", etcd.Name, ready, etcd.Spec.Replicas))
	case observed != etcd.Generation:
		return not(fmt.Sprintf("etcd-druid has not observed generation %d of Etcd %s, observedGeneration %d", etcd.Generation, etcd.Name, observed))
	}
	return not(fmt.Sprintf("Etcd %s has %d of %d members ready", etcd.Name, ready, etcd.Spec.Replicas))
}

// updatedMembers counts, once etcd-druid and the StatefulSet controller have
// observed the Etcd's spec, its members that are available: all available
// ones less those of older revisions.
func updatedMembers(etcd *druidv1alpha1.Etcd, sts *appsv1.StatefulSet) *int32 {
	if sts == nil || ptr.Deref(etcd.Status.ObservedGeneration, 0) != etcd.Generation || sts.Status.ObservedGeneration != sts.Generation {
		return nil
	}
	s := sts.Status
	return new(max(0, min(s.UpdatedReplicas, s.AvailableReplicas-(s.Replicas-s.UpdatedReplicas))))
}

// etcd-druid refreshes every Etcd's status every 15 seconds
// (etcd-druid@v0.39.0 api/config/v1alpha1/defaults.go#L104, charts/values.yaml#L63)
// with new timestamps alone; only a change readiness, status.datastore or
// membersHeld reads gets a pass.
func etcdChanged() predicate.Funcs {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		before, ok := e.ObjectOld.(*druidv1alpha1.Etcd)
		after, ok2 := e.ObjectNew.(*druidv1alpha1.Etcd)
		return !ok || !ok2 || readinessInput(before) != readinessInput(after)
	}}
}

func readinessInput(etcd *druidv1alpha1.Etcd) string {
	var b strings.Builder
	_, kept := keptMember(etcd)
	fmt.Fprintf(&b, "%d %t %d %t %d/%d %d %t;", etcd.Generation, !etcd.DeletionTimestamp.IsZero(), ptr.Deref(etcd.Status.ObservedGeneration, 0),
		ptr.Deref(etcd.Status.Ready, false), etcd.Status.ReadyReplicas, etcd.Spec.Replicas, len(etcd.Status.Members), kept)
	for _, c := range etcd.Status.Conditions {
		fmt.Fprintf(&b, "%s=%s/%s;", c.Type, c.Status, c.Reason)
	}
	return b.String()
}

func formed(etcd *druidv1alpha1.Etcd) bool {
	mark, ok := etcd.Annotations[lodiav1alpha1.EtcdFormedAnnotation]
	return ok && mark == string(etcd.UID)
}
