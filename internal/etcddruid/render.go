// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/contract"
)

// allowUnhealthyPodEviction on an Etcd lets its PodDisruptionBudget evict an
// unhealthy member always
// (etcd-druid@v0.39.2 internal/component/poddistruptionbudget/poddisruptionbudget.go#L36-L38).
const allowUnhealthyPodEviction = "resources.druid.gardener.cloud/allow-unhealthy-pod-eviction"

// Render is the Etcd of class for cp on the certificate generation given, and
// the note of a quota capped; render says how.
func (e *Engine) Render(cp *lodiav1alpha1.ControlPlane, class *lodiav1alpha1.DatastoreClass, namespaceUID types.UID, generation int, current *Store, claims []corev1.PersistentVolumeClaim, rollout *Rollout, members int32) (*Store, string) {
	etcd, capped := render(cp, class, namespaceUID, generation, current.object(), claims, rollout.statefulSet(), members)
	return &Store{etcd: etcd}, capped
}

// TLS is set on client, peer and backup-restore together: etcd-druid probes
// the members over HTTPS only with backup.tls, while they serve HTTPS whenever
// client TLS is on (docs/decisions/dedicated-etcd-by-default.md). The storage
// class and size come from the current Etcd once it exists: the first is
// immutable in the Etcd, the second a StatefulSet field that cannot change in
// place. Without one, the size is that of the smallest claim left, which the
// StatefulSet binds again by name. The quota is capped at half the size. A nil
// class keeps the fields current's class gave it, while that class waits for
// its grant, is held for its credentials or is gone, under current's
// resourceVersion, so that a stale cache conflicts instead of writing an older
// class back outside the window. The backup prefix holds UIDs and no
// name: etcd-backup-restore takes the last v1/ anywhere in a key for its
// layout and ignores the snapshots it misreads
// (etcd-backup-restore@v0.45.3 pkg/snapstore/snapshot.go#L39-L59).
func render(cp *lodiav1alpha1.ControlPlane, class *lodiav1alpha1.DatastoreClass, namespaceUID types.UID, generation int, current *druidv1alpha1.Etcd, claims []corev1.PersistentVolumeClaim, sts *appsv1.StatefulSet, members int32) (*druidv1alpha1.Etcd, string) {
	secret := func(certificate contract.EtcdCertificateRole) corev1.SecretReference {
		return corev1.SecretReference{Name: contract.EtcdCertificateSecretName(cp.Name, certificate, generation)}
	}
	// etcd-druid reads the CA under ca.crt for the members and under
	// bundle.crt for its own calls to the sidecar unless the key is named
	// (etcd-druid@v0.39.2 internal/component/statefulset/builder.go#L468,
	// internal/controller/compaction/snapshot.go#L87).
	ca := druidv1alpha1.SecretReference{SecretReference: secret(contract.EtcdCARole), DataKey: new(lodiav1alpha1.CACertKey)}
	etcd := &druidv1alpha1.Etcd{
		TypeMeta:   metav1.TypeMeta{APIVersion: druidv1alpha1.SchemeGroupVersion.String(), Kind: "Etcd"},
		ObjectMeta: etcdMeta(cp),
		Spec: druidv1alpha1.EtcdSpec{
			Replicas: members,
			Labels:   map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name},
			Etcd: druidv1alpha1.EtcdConfig{
				ClientUrlTLS: &druidv1alpha1.TLSConfig{TLSCASecretRef: ca, ServerTLSSecretRef: secret(contract.EtcdServerRole), ClientTLSSecretRef: secret(contract.EtcdClientRole)},
				PeerUrlTLS:   &druidv1alpha1.PeerTLSConfig{TLSConfig: druidv1alpha1.TLSConfig{TLSCASecretRef: ca, ServerTLSSecretRef: secret(contract.EtcdPeerRole)}},
			},
			Backup: druidv1alpha1.BackupSpec{
				TLS: &druidv1alpha1.TLSConfig{TLSCASecretRef: ca, ServerTLSSecretRef: secret(contract.EtcdBackupRestoreRole), ClientTLSSecretRef: secret(contract.EtcdClientRole)},
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
	case membersHeld(members, current, sts):
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
		etcd.ResourceVersion = current.ResourceVersion
		return etcd, ""
	}
	spec := &class.Spec
	metav1.SetMetaDataAnnotation(&etcd.ObjectMeta, lodiav1alpha1.DatastoreClassAnnotation, class.Name)
	metav1.SetMetaDataAnnotation(&etcd.ObjectMeta, lodiav1alpha1.DatastoreClassUIDAnnotation, string(class.UID))
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
	quota, capped := cappedQuota(class, volume, etcd.Name)
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
		etcd.Spec.Backup.Store = store(cp, backup, namespaceUID)
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

// cappedQuota is the class's quota, capped at half the volume, and the note
// of a cap.
func cappedQuota(class *lodiav1alpha1.DatastoreClass, volume resource.Quantity, etcd string) (resource.Quantity, string) {
	quota := resource.MustParse(lodiav1alpha1.DefaultQuotaBackendBytes)
	if settings := class.Spec.Etcd; settings != nil && settings.QuotaBackendBytes != nil {
		quota = settings.QuotaBackendBytes.DeepCopy()
	}
	twice := quota.DeepCopy()
	twice.Add(quota)
	if twice.Cmp(volume) <= 0 {
		return quota, ""
	}
	half := resource.NewQuantity(volume.Value()/2, volume.Format)
	return *half, fmt.Sprintf("DatastoreClass %s's quota %s is capped at %s: it needs %s of volume and Etcd %s's is %s",
		class.Name, quota.String(), half.String(), twice.String(), etcd, volume.String())
}

func store(cp *lodiav1alpha1.ControlPlane, backup *lodiav1alpha1.EtcdBackup, namespaceUID types.UID) *druidv1alpha1.StoreSpec {
	spec := &druidv1alpha1.StoreSpec{
		Provider:  new(druidv1alpha1.StorageProvider(backup.Store.Provider)),
		Container: new(backup.Store.Bucket),
		Prefix:    path.Join(backup.Store.Prefix, string(namespaceUID), "controlplanes", string(cp.UID)),
		SecretRef: &corev1.SecretReference{Name: lodiav1alpha1.EtcdBackupSecret.Name(cp.Name)},
	}
	if backup.Store.Endpoint != "" {
		spec.EndpointOverride = new(backup.Store.Endpoint)
	}
	return spec
}

func etcdMeta(cp *lodiav1alpha1.ControlPlane) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            contract.EtcdName(cp.Name),
		Namespace:       cp.Namespace,
		Labels:          map[string]string{lodiav1alpha1.ControlPlaneLabel: cp.Name},
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cp, lodiav1alpha1.GroupVersion.WithKind("ControlPlane"))},
	}
}

// Copy is the Etcd a held pass applies: current's spec on the certificate
// generation given, those of current's annotations that an apply leaving them
// out would remove, and the formed mark as render sets it. It names current's
// resourceVersion, so that a copy from a stale cache conflicts instead of
// writing an older spec back. etcd-druid's annotation and the record of a
// request stay out of it, as out of every apply: one that carried them would
// ask for ever, or take a request back before etcd-druid has run.
func (e *Engine) Copy(cp *lodiav1alpha1.ControlPlane, current *Store, generation int) *Store {
	return &Store{etcd: copied(cp, current.etcd, generation)}
}

func copied(cp *lodiav1alpha1.ControlPlane, current *druidv1alpha1.Etcd, generation int) *druidv1alpha1.Etcd {
	etcd := &druidv1alpha1.Etcd{
		TypeMeta:   metav1.TypeMeta{APIVersion: druidv1alpha1.SchemeGroupVersion.String(), Kind: "Etcd"},
		ObjectMeta: etcdMeta(cp),
		Spec:       *current.Spec.DeepCopy(),
	}
	etcd.UID, etcd.ResourceVersion = current.UID, current.ResourceVersion
	for _, key := range []string{lodiav1alpha1.OperatorRevisionAnnotation, lodiav1alpha1.DatastoreClassAnnotation, lodiav1alpha1.DatastoreClassUIDAnnotation, lodiav1alpha1.DatastoreClassGenerationAnnotation, allowUnhealthyPodEviction} {
		if value, ok := current.Annotations[key]; ok {
			metav1.SetMetaDataAnnotation(&etcd.ObjectMeta, key, value)
		}
	}
	if formed(current) || current.Status.ReadyReplicas > 0 {
		metav1.SetMetaDataAnnotation(&etcd.ObjectMeta, lodiav1alpha1.EtcdFormedAnnotation, string(current.UID))
	}
	name := func(certificate contract.EtcdCertificateRole) string {
		return contract.EtcdCertificateSecretName(cp.Name, certificate, generation)
	}
	if tls := etcd.Spec.Etcd.ClientUrlTLS; tls != nil {
		tls.TLSCASecretRef.Name, tls.ServerTLSSecretRef.Name, tls.ClientTLSSecretRef.Name = name(contract.EtcdCARole), name(contract.EtcdServerRole), name(contract.EtcdClientRole)
	}
	if peer := etcd.Spec.Etcd.PeerUrlTLS; peer != nil {
		peer.TLSCASecretRef.Name, peer.ServerTLSSecretRef.Name = name(contract.EtcdCARole), name(contract.EtcdPeerRole)
	}
	if tls := etcd.Spec.Backup.TLS; tls != nil {
		tls.TLSCASecretRef.Name, tls.ServerTLSSecretRef.Name, tls.ClientTLSSecretRef.Name = name(contract.EtcdCARole), name(contract.EtcdBackupRestoreRole), name(contract.EtcdClientRole)
	}
	return etcd
}

func claimSize(claim corev1.PersistentVolumeClaim) resource.Quantity {
	if size, ok := claim.Status.Capacity[corev1.ResourceStorage]; ok {
		return size
	}
	return claim.Spec.Resources.Requests[corev1.ResourceStorage]
}

// membersHeld keeps the Etcd's members, spread and eviction annotation as they
// are (docs/README.md, the highAvailability row of the Etcd's fields).
func membersHeld(members int32, current *druidv1alpha1.Etcd, sts *appsv1.StatefulSet) bool {
	if current == nil {
		return false
	}
	if !druidv1alpha1.HasScaleOperationCompleted(current) {
		return true
	}
	if members < current.Spec.Replicas {
		_, ready := keptMember(current)
		return !ready
	}
	updated := updatedMembers(current, sts)
	return members > current.Spec.Replicas && (updated == nil || *updated < current.Spec.Replicas)
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
	for _, key := range []string{lodiav1alpha1.DatastoreClassAnnotation, lodiav1alpha1.DatastoreClassUIDAnnotation, lodiav1alpha1.DatastoreClassGenerationAnnotation} {
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

// Apply applies the rendered Etcd with c as manager and returns the Etcd the
// API server answers with.
func (e *Engine) Apply(ctx context.Context, c client.Client, manager string, rendered *Store) (*Store, error) {
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rendered.etcd)
	if err != nil {
		return nil, err
	}
	// The typed Etcd marshals its deprecated selector as null and its status
	// as an empty object; neither is a field to apply. Nor are the objects
	// render leaves empty without the class or spec fields they come from:
	// applied as an empty object where the apply set fields in it before, an
	// object comes back null, which the schema refuses
	// (sigs.k8s.io/structured-merge-diff/v6@v6.4.2 typed/remove.go#L171-L173).
	unstructured.RemoveNestedField(content, "spec", "selector")
	unstructured.RemoveNestedField(content, "status")
	// @todo: the drop goes once every minor a management cluster runs keeps an object its removal empties (todo.md).
	for _, object := range [][]string{{"spec", "schedulingConstraints"}, {"spec", "sharedConfig"}, {"spec", "etcd", "resources"}} {
		if fields, _, _ := unstructured.NestedMap(content, object...); len(fields) == 0 {
			unstructured.RemoveNestedField(content, object...)
		}
	}
	applied := &unstructured.Unstructured{Object: content}
	if err := c.Apply(ctx, client.ApplyConfigurationFromUnstructured(applied), client.FieldOwner(manager), client.ForceOwnership); err != nil {
		return nil, err
	}
	result := &druidv1alpha1.Etcd{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(applied.Object, result); err != nil {
		return nil, err
	}
	return &Store{etcd: result}, nil
}

// @todo: goes with the drop in Apply once every minor a management cluster runs keeps an object its removal empties (todo.md).

// HeldEmpty names each object that refused, the API server's refusal of
// manager's apply of rendered, finds null because the apply removed manager's
// fields in it while other field managers hold it as an empty object, as Apply
// says, with those managers and what releases it; explained says that the
// refusal has no other cause. It reads the Etcd's managedFields through
// reader, which must not be the cache: the cache strips them.
func (e *Engine) HeldEmpty(ctx context.Context, reader client.Reader, manager string, rendered *Store, refused error) (held []string, explained bool, err error) {
	var status apierrors.APIStatus
	if !errors.As(refused, &status) || status.Status().Details == nil {
		return nil, false, nil
	}
	var nulls []string
	explained = true
	for _, cause := range status.Status().Details.Causes {
		switch {
		case cause.Type == metav1.CauseTypeTypeInvalid && strings.HasPrefix(cause.Message, `Invalid value: "null"`):
			nulls = append(nulls, cause.Field)
		case cause.Field != skippedRules:
			explained = false
		}
	}
	if len(nulls) == 0 {
		return nil, false, nil
	}
	current := &druidv1alpha1.Etcd{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(rendered.etcd), current); err != nil {
		return nil, false, client.IgnoreNotFound(err)
	}
	for _, field := range nulls {
		names := strings.Split(field, ".")
		path := make(fieldpath.Path, 0, len(names))
		for _, name := range names {
			path = append(path, fieldpath.PathElement{FieldName: &name})
		}
		ours, others := false, false
		var holders []holder
		for _, entry := range current.ManagedFields {
			if entry.FieldsV1 == nil {
				continue
			}
			set := &fieldpath.Set{}
			if err := set.FromJSON(entry.FieldsV1.GetRawReader()); err != nil {
				return nil, false, fmt.Errorf("read the managedFields of Etcd %s: %w", current.Name, err)
			}
			inside := set
			for _, element := range path {
				inside = inside.WithPrefix(element)
			}
			switch {
			case entry.Manager == manager && entry.Operation == metav1.ManagedFieldsOperationApply:
				ours = !inside.Empty()
			case !inside.Empty():
				others = true
			case set.Has(path):
				holders = append(holders, holder{entry.Operation, entry.Manager})
			}
		}
		if !ours || others || len(holders) == 0 {
			explained = false
			continue
		}
		held = append(held, field+", "+holding(field, holders))
	}
	return held, explained, nil
}

// skippedRules is the field of the cause the API server adds when a schema
// error kept it from evaluating the validation rules
// (k8s.io/apiextensions-apiserver@v0.37.2 pkg/registry/customresource/strategy.go#L405-L412).
const skippedRules = "<nil>"

type holder struct {
	operation metav1.ManagedFieldsOperationType
	manager   string
}

// holding says which managers hold the object at field and what releases
// it. The API server keys a manager's fields by its operation too
// (k8s.io/apimachinery@v0.37.2 pkg/util/managedfields/internal/managedfields.go#L134-L153),
// so an apply releases only what the manager applied, and an object held by an
// update goes only when it is removed, which takes it from every manager.
func holding(field string, holders []holder) string {
	slices.SortFunc(holders, func(a, b holder) int {
		return cmp.Or(strings.Compare(a.manager, b.manager), strings.Compare(string(a.operation), string(b.operation)))
	})
	holders = slices.Compact(holders)
	which := make([]string, len(holders))
	update := false
	for i, h := range holders {
		which[i] = "the " + strings.ToLower(string(h.operation)) + " of field manager " + h.manager
		update = update || h.operation == metav1.ManagedFieldsOperationUpdate
	}
	verb := " holds "
	if len(holders) > 1 {
		verb = " hold "
	}
	release := "until " + holders[0].manager + " applies without it"
	switch {
	case update:
		release = `until the JSON patch [{"op":"remove","path":"/` + strings.ReplaceAll(field, ".", "/") + `"}] removes it`
	case len(holders) > 1:
		release = "until each applies without it"
	}
	return "which " + strings.Join(which, " and ") + verb + release
}

// Delete deletes the Etcd under its UID.
func (e *Engine) Delete(ctx context.Context, c client.Client, s *Store) error {
	return c.Delete(ctx, s.etcd, client.Preconditions{UID: &s.etcd.UID})
}

// ClaimOrdinal is the ordinal of the member whose volume claim is named claim,
// false for a claim of no member of cp's Etcd. A StatefulSet names a claim
// <template>-<StatefulSet>-<ordinal>, and etcd-druid names the template after
// the Etcd
// (kubernetes/kubernetes@v1.35.0 pkg/controller/statefulset/stateful_set_utils.go#L116-L119,
// etcd-druid@v0.39.2 internal/component/statefulset/builder.go#L234).
func (e *Engine) ClaimOrdinal(cp, claim string) (int, bool) {
	ordinal, named := strings.CutPrefix(claim, contract.EtcdName(cp)+"-"+contract.EtcdName(cp)+"-")
	n, err := strconv.Atoi(ordinal)
	return n, named && err == nil && n >= 0 && strconv.Itoa(n) == ordinal
}
