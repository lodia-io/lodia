// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package datastore is the Datastore controller for the dedicated case: one
// etcd-druid Etcd per ControlPlane, built from its DatastoreClass and the
// newest complete generation of the etcd certificates, held by the
// ControlPlane's finalizer and reported through DatastoreReady.
package datastore

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
)

const (
	// Name is the controller's name; its field manager is brygga/datastore.
	Name = "datastore"
	// FieldManager owns every field this controller writes.
	FieldManager = "brygga/" + Name
	// Finalizer is the ControlPlane's one finalizer, which this controller
	// holds until the Etcd is gone and the class's reclaim policy applied.
	Finalizer = "brygga.segla.io/controlplane"
	// ConditionType is the one condition this controller writes.
	ConditionType = "DatastoreReady"
	// DefaultClassAnnotation marks the DatastoreClass that serves ControlPlanes
	// naming none.
	DefaultClassAnnotation = "brygga.segla.io/is-default-class"
	// CertificatesLabel carries the generation of a Secret of the etcd
	// certificates; the Etcd names the newest generation whose five Secrets
	// all carry it.
	CertificatesLabel = "brygga.segla.io/etcd-certificates"
	// ControlPlaneUIDLabel marks the claims of a ControlPlane's etcd with its
	// UID: a ControlPlane recreated under the same name never starts from them.
	ControlPlaneUIDLabel = "brygga.segla.io/control-plane-uid"
	// ClassAnnotation records on the Etcd, and on its claims, the
	// DatastoreClass it was created under.
	ClassAnnotation = "brygga.segla.io/datastore-class"
	// ClassGenerationAnnotation records on the Etcd the generation of the
	// class it was rendered from.
	ClassGenerationAnnotation = "brygga.segla.io/datastore-class-generation"

	// ReasonReconciling is DatastoreReady's reason, Unknown, until the first
	// pass has finished.
	ReasonReconciling = "Reconciling"
	// ReasonProvisioning says the certificates or etcd-druid's report are awaited.
	ReasonProvisioning = "Provisioning"
	// ReasonDatastoreClassNotFound says no class serves the ControlPlane.
	ReasonDatastoreClassNotFound = "DatastoreClassNotFound"
	// ReasonVolumesExist says claims of another ControlPlane stand where this
	// one's etcd would be created.
	ReasonVolumesExist = "VolumesExist"
	// ReasonDeleting says the ControlPlane is being deleted.
	ReasonDeleting = "Deleting"
	// ReasonReady says etcd-druid reports the Etcd ready for its current generation.
	ReasonReady = "Ready"
)

// Reconciler keeps the Etcd of every ControlPlane with a dedicated datastore.
type Reconciler struct {
	client.Client
	// Reader reads uncached what someone else created for a tenant: the
	// class's credentials Secret and the claims of etcd-druid's StatefulSet.
	Reader client.Reader
}

// SetupWithManager registers the controller with workers concurrent reconciles.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	certificates, err := predicate.LabelSelectorPredicate(metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: CertificatesLabel, Operator: metav1.LabelSelectorOpExists}},
	})
	if err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers}).
		For(&bryggav1alpha1.ControlPlane{}).
		Owns(&druidv1alpha1.Etcd{}, builder.WithPredicates(etcdChanged())).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(controlPlaneOf), builder.WithPredicates(certificates)).
		Watches(&bryggav1alpha1.DatastoreClass{}, handler.EnqueueRequestsFromMapFunc(r.controlPlanesUsing)).
		Complete(r)
}

func controlPlaneOf(_ context.Context, secret client.Object) []reconcile.Request {
	name, ok := secret.GetLabels()[bryggav1alpha1.ControlPlaneLabel]
	if !ok {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: secret.GetNamespace(), Name: name}}}
}

// controlPlanesUsing enqueues the ControlPlanes that name the class and those
// that name none, since the default may have moved.
func (r *Reconciler) controlPlanesUsing(ctx context.Context, class client.Object) []reconcile.Request {
	controlPlanes := &bryggav1alpha1.ControlPlaneList{}
	if err := r.List(ctx, controlPlanes); err != nil {
		logf.FromContext(ctx).Error(err, "list the ControlPlanes of a DatastoreClass", "class", class.GetName())
		return nil
	}
	var requests []reconcile.Request
	for i := range controlPlanes.Items {
		cp := &controlPlanes.Items[i]
		if d := cp.Spec.Datastore.Dedicated; d != nil && (d.ClassName == "" || d.ClassName == class.GetName()) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
		}
	}
	return requests
}

// Reconcile holds the finalizer, keeps the Etcd and reports DatastoreReady.
// A paused ControlPlane is left as it is, its deletion included, until it is
// unpaused.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &bryggav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cp.Spec.Datastore.Dedicated == nil || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	if !cp.DeletionTimestamp.IsZero() {
		requeueAfter, err := r.finalize(ctx, cp)
		return ctrl.Result{RequeueAfter: requeueAfter}, err
	}
	if !controllerutil.ContainsFinalizer(cp, Finalizer) {
		err := r.setFinalizer(ctx, cp, true)
		switch {
		case apierrors.IsNotFound(err):
			return ctrl.Result{}, nil
		case apierrors.IsConflict(err):
			return ctrl.Result{RequeueAfter: time.Second}, nil
		case err != nil:
			return ctrl.Result{}, err
		}
	}
	if meta.FindStatusCondition(cp.Status.Conditions, ConditionType) == nil {
		first := newCondition(metav1.ConditionUnknown, ReasonReconciling, "the first pass has not finished")
		if err := r.applyStatus(ctx, cp, cp.Status.Datastore, first); err != nil {
			return ctrl.Result{}, err
		}
	}
	etcd, condition, requeueAfter, err := r.reconcileEtcd(ctx, cp)
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.applyStatus(ctx, cp, datastoreStatus(etcd), condition); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// reconcileEtcd applies the Etcd once the class and a complete certificate
// generation exist and no other ControlPlane's claims stand in its way, and
// says what DatastoreReady reports meanwhile. An error is the API server's:
// the pass ends with it and leaves the status as it is.
func (r *Reconciler) reconcileEtcd(ctx context.Context, cp *bryggav1alpha1.ControlPlane) (*druidv1alpha1.Etcd, metav1.Condition, time.Duration, error) {
	current, err := r.currentEtcd(ctx, cp)
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	class, missing, err := r.class(ctx, cp, current)
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	if class == nil {
		return current, newCondition(metav1.ConditionFalse, ReasonDatastoreClassNotFound, missing), 0, nil
	}
	generation, found, err := r.newestCompleteGeneration(ctx, cp)
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	if !found {
		message := fmt.Sprintf("waiting for a complete generation of the etcd certificates: the Secrets %s-etcd-{%s}-<g> labelled %s=<g>",
			cp.Name, strings.Join(Certificates, ","), CertificatesLabel)
		return current, newCondition(metav1.ConditionFalse, ReasonProvisioning, message), 0, nil
	}
	if current == nil {
		foreign, err := r.foreignClaims(ctx, cp)
		if err != nil {
			return nil, metav1.Condition{}, 0, err
		}
		if len(foreign) > 0 {
			message := fmt.Sprintf("claims of another ControlPlane stand where the etcd of %s would be created: %s; delete them to start empty, or restore from them on purpose",
				cp.Name, strings.Join(foreign, ", "))
			return nil, newCondition(metav1.ConditionFalse, ReasonVolumesExist, message), time.Minute, nil
		}
	}
	if backup := class.Spec.Backup; backup != nil {
		copied, err := r.copyCredentials(ctx, cp, class)
		if err != nil {
			return nil, metav1.Condition{}, 0, err
		}
		if !copied {
			ref := backup.Store.SecretRef
			message := fmt.Sprintf("waiting for the credentials Secret %s/%s of DatastoreClass %s", ref.Namespace, ref.Name, class.Name)
			return current, newCondition(metav1.ConditionFalse, ReasonProvisioning, message), time.Minute, nil
		}
	}
	etcd, err := r.apply(ctx, render(cp, class, generation, current))
	if err != nil {
		return nil, metav1.Condition{}, 0, fmt.Errorf("apply Etcd %s: %w", etcdName(cp.Name), err)
	}
	if err := r.labelClaims(ctx, cp, class.Name); err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	sts := &appsv1.StatefulSet{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: etcdName(cp.Name)}, sts); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, metav1.Condition{}, 0, err
		}
		sts = nil
	}
	condition, statefulSetLags := readiness(cp.Name, etcd, sts, generation)
	if statefulSetLags {
		return etcd, condition, 15 * time.Second, nil
	}
	return etcd, condition, 0, nil
}

// currentEtcd reads the Etcd from the cache, and from the API server when the
// cache has none: an Etcd that fell out of the label-filtered cache still runs.
func (r *Reconciler) currentEtcd(ctx context.Context, cp *bryggav1alpha1.ControlPlane) (*druidv1alpha1.Etcd, error) {
	etcd := &druidv1alpha1.Etcd{}
	key := client.ObjectKey{Namespace: cp.Namespace, Name: etcdName(cp.Name)}
	err := r.Get(ctx, key, etcd)
	if apierrors.IsNotFound(err) {
		err = r.Reader.Get(ctx, key, etcd)
	}
	switch {
	case apierrors.IsNotFound(err):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return etcd, nil
}

// class is the DatastoreClass recorded on the Etcd or, before the Etcd
// exists, the one the ControlPlane names or the one default class. A class is
// resolved once, as a default StorageClass is written into a claim when the
// claim is created, so a default that moves changes no running etcd. When no
// class serves, the class is nil and the message says what is missing.
func (r *Reconciler) class(ctx context.Context, cp *bryggav1alpha1.ControlPlane, current *druidv1alpha1.Etcd) (*bryggav1alpha1.DatastoreClass, string, error) {
	name := cp.Spec.Datastore.Dedicated.ClassName
	if current != nil {
		if recorded, ok := current.Annotations[ClassAnnotation]; ok {
			name = recorded
		}
	}
	if name != "" {
		class := &bryggav1alpha1.DatastoreClass{}
		if err := r.Get(ctx, client.ObjectKey{Name: name}, class); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, "DatastoreClass " + name + " does not exist", nil
			}
			return nil, "", err
		}
		return class, "", nil
	}
	classes := &bryggav1alpha1.DatastoreClassList{}
	if err := r.List(ctx, classes); err != nil {
		return nil, "", err
	}
	var defaults []*bryggav1alpha1.DatastoreClass
	for i := range classes.Items {
		if classes.Items[i].Annotations[DefaultClassAnnotation] == "true" {
			defaults = append(defaults, &classes.Items[i])
		}
	}
	switch len(defaults) {
	case 1:
		return defaults[0], "", nil
	case 0:
		return nil, "no DatastoreClass is annotated " + DefaultClassAnnotation + `: "true"`, nil
	default:
		names := make([]string, 0, len(defaults))
		for _, class := range defaults {
			names = append(names, class.Name)
		}
		slices.Sort(names)
		return nil, fmt.Sprintf("%d DatastoreClasses are annotated %s: %q, exactly one may be: %s",
			len(defaults), DefaultClassAnnotation, "true", strings.Join(names, ", ")), nil
	}
}

// newestCompleteGeneration lists the etcd certificate Secrets, metadata only,
// and picks the newest generation whose five Secrets all carry its label and
// none is being deleted.
func (r *Reconciler) newestCompleteGeneration(ctx context.Context, cp *bryggav1alpha1.ControlPlane) (int, bool, error) {
	secrets := &metav1.PartialObjectMetadataList{}
	secrets.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("SecretList"))
	if err := r.List(ctx, secrets, client.InNamespace(cp.Namespace), client.MatchingLabels{bryggav1alpha1.ControlPlaneLabel: cp.Name}); err != nil {
		return 0, false, err
	}
	labelled := make(map[string]string, len(secrets.Items))
	for _, secret := range secrets.Items {
		if generation, ok := secret.Labels[CertificatesLabel]; ok && secret.DeletionTimestamp.IsZero() {
			labelled[secret.Name] = generation
		}
	}
	newest, found := 0, false
	for _, label := range labelled {
		generation, err := strconv.Atoi(label)
		if err != nil || generation < 0 || (found && generation <= newest) {
			continue
		}
		complete := true
		for _, certificate := range Certificates {
			if labelled[SecretName(cp.Name, certificate, generation)] != label {
				complete = false
				break
			}
		}
		if complete {
			newest, found = generation, true
		}
	}
	return newest, found, nil
}

// claims lists the PersistentVolumeClaims of the Etcd's StatefulSet, which
// etcd-druid keeps past the Etcd, by the labels the StatefulSet controller
// copies from the selector etcd-druid gives it
// (kubernetes/kubernetes@v1.35.0 pkg/controller/statefulset/stateful_set_utils.go#L389-L407,
// etcd-druid@v0.39.0 internal/component/statefulset/builder.go#L149-L151).
func (r *Reconciler) claims(ctx context.Context, cp *bryggav1alpha1.ControlPlane, uid bool) (*corev1.PersistentVolumeClaimList, error) {
	selector := druidv1alpha1.GetDefaultLabels(metav1.ObjectMeta{Name: etcdName(cp.Name)})
	if uid {
		selector[ControlPlaneUIDLabel] = string(cp.UID)
	}
	claims := &corev1.PersistentVolumeClaimList{}
	err := r.Reader.List(ctx, claims, client.InNamespace(cp.Namespace), client.MatchingLabels(selector))
	return claims, err
}

// foreignClaims names the claims of <cp>-etcd that are not this
// ControlPlane's: unlabelled, or labelled with another UID. Starting an etcd
// on them would be a restore of another tenant's data by name.
func (r *Reconciler) foreignClaims(ctx context.Context, cp *bryggav1alpha1.ControlPlane) ([]string, error) {
	claims, err := r.claims(ctx, cp, false)
	if err != nil {
		return nil, err
	}
	var foreign []string
	for _, claim := range claims.Items {
		if claim.Labels[ControlPlaneUIDLabel] != string(cp.UID) {
			foreign = append(foreign, claim.Name)
		}
	}
	slices.Sort(foreign)
	return foreign, nil
}

// labelClaims marks the claims the StatefulSet created for this Etcd with the
// ControlPlane's UID and the class it was created under, since they outlive
// the Etcd: deletion reclaims only these, under that class's policy.
func (r *Reconciler) labelClaims(ctx context.Context, cp *bryggav1alpha1.ControlPlane, class string) error {
	claims, err := r.claims(ctx, cp, false)
	if err != nil {
		return err
	}
	for i := range claims.Items {
		claim := &claims.Items[i]
		if _, labelled := claim.Labels[ControlPlaneUIDLabel]; labelled {
			continue
		}
		patch := client.MergeFrom(claim.DeepCopy())
		metav1.SetMetaDataLabel(&claim.ObjectMeta, ControlPlaneUIDLabel, string(cp.UID))
		metav1.SetMetaDataAnnotation(&claim.ObjectMeta, ClassAnnotation, class)
		if err := client.IgnoreNotFound(r.Patch(ctx, claim, patch, client.FieldOwner(FieldManager))); err != nil {
			return err
		}
	}
	return nil
}

// copyCredentials copies the class's credentials Secret into the tenant's
// namespace, where etcd-druid mounts it, with the class's region under the key
// the backup-restore sidecar reads. It copies nothing while the class's
// Secret does not exist.
func (r *Reconciler) copyCredentials(ctx context.Context, cp *bryggav1alpha1.ControlPlane, class *bryggav1alpha1.DatastoreClass) (bool, error) {
	store := class.Spec.Backup.Store
	source := &corev1.Secret{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: store.SecretRef.Namespace, Name: store.SecretRef.Name}, source); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read the credentials Secret %s/%s of DatastoreClass %s: %w", store.SecretRef.Namespace, store.SecretRef.Name, class.Name, err)
	}
	data := maps.Clone(source.Data)
	if store.Region != "" {
		if data == nil {
			data = map[string][]byte{}
		}
		data["region"] = []byte(store.Region)
	}
	secret := corev1ac.Secret(backupSecretName(cp.Name), cp.Namespace).
		WithLabels(map[string]string{bryggav1alpha1.ControlPlaneLabel: cp.Name}).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(bryggav1alpha1.GroupVersion.String()).
			WithKind("ControlPlane").
			WithName(cp.Name).
			WithUID(cp.UID).
			WithController(true).
			WithBlockOwnerDeletion(true)).
		WithData(data)
	return true, r.Apply(ctx, secret, client.FieldOwner(FieldManager), client.ForceOwnership)
}

// finalize deletes the Etcd, waits for etcd-druid to let it go, reclaims the
// claims it left behind and releases the finalizer.
func (r *Reconciler) finalize(ctx context.Context, cp *bryggav1alpha1.ControlPlane) (time.Duration, error) {
	if !controllerutil.ContainsFinalizer(cp, Finalizer) {
		return 0, nil
	}
	etcd, err := r.currentEtcd(ctx, cp)
	if err != nil {
		return 0, err
	}
	deleting := newCondition(metav1.ConditionFalse, ReasonDeleting, "the ControlPlane is being deleted: the Etcd goes first, then its claims as the class says")
	if err := r.applyStatus(ctx, cp, datastoreStatus(etcd), deleting); err != nil {
		return 0, err
	}
	switch {
	case etcd != nil && etcd.DeletionTimestamp.IsZero():
		logf.FromContext(ctx).Info("deleting the Etcd", "etcd", etcd.Name)
		return 0, client.IgnoreNotFound(r.Delete(ctx, etcd))
	case etcd != nil:
		return 10 * time.Second, nil
	}
	if err := r.reclaimClaims(ctx, cp); err != nil {
		return 0, err
	}
	if err := r.setFinalizer(ctx, cp, false); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return 0, err
	}
	return 0, nil
}

// reclaimClaims deletes the claims labelled with this ControlPlane's UID
// whose recorded class says Delete; Retain, and a class that is gone, keep
// them.
func (r *Reconciler) reclaimClaims(ctx context.Context, cp *bryggav1alpha1.ControlPlane) error {
	log := logf.FromContext(ctx)
	claims, err := r.claims(ctx, cp, true)
	if err != nil {
		return err
	}
	for i := range claims.Items {
		claim := &claims.Items[i]
		class := &bryggav1alpha1.DatastoreClass{}
		name := claim.Annotations[ClassAnnotation]
		if name == "" {
			log.Info("keeping the claim, it records no class", "claim", claim.Name)
			continue
		}
		err := r.Get(ctx, client.ObjectKey{Name: name}, class)
		switch {
		case apierrors.IsNotFound(err):
			log.Info("keeping the claim, its class is gone", "claim", claim.Name, "class", name)
			continue
		case err != nil:
			return err
		case class.Spec.Storage.ReclaimPolicy != "Delete":
			continue
		}
		log.Info("deleting the claim", "claim", claim.Name, "class", name)
		if err := client.IgnoreNotFound(r.Delete(ctx, claim)); err != nil {
			return err
		}
	}
	return nil
}

// setFinalizer adds or removes the finalizer with a patch locked on the
// ControlPlane's resourceVersion, never with an apply: an apply would create
// a ControlPlane the cache still showed after its deletion. A conflict is a
// ControlPlane that changed since it was read, and that change reconciles it
// again.
func (r *Reconciler) setFinalizer(ctx context.Context, cp *bryggav1alpha1.ControlPlane, held bool) error {
	patch := client.MergeFromWithOptions(cp.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if held {
		controllerutil.AddFinalizer(cp, Finalizer)
	} else {
		controllerutil.RemoveFinalizer(cp, Finalizer)
	}
	return r.Patch(ctx, cp, patch, client.FieldOwner(FieldManager))
}

// datastoreStatus is status.datastore for the Etcd, nil while there is none.
func datastoreStatus(etcd *druidv1alpha1.Etcd) *bryggav1alpha1.DatastoreStatus {
	if etcd == nil {
		return nil
	}
	return &bryggav1alpha1.DatastoreStatus{Kind: "Dedicated", Name: etcd.Name, Members: int32(len(etcd.Status.Members))}
}

// applyStatus writes the condition and status.datastore through the status
// subresource, the only fields of the ControlPlane's status this controller
// owns, and only when one of them changed.
func (r *Reconciler) applyStatus(ctx context.Context, cp *bryggav1alpha1.ControlPlane, datastore *bryggav1alpha1.DatastoreStatus, condition metav1.Condition) error {
	condition.ObservedGeneration = cp.Generation
	previous := meta.FindStatusCondition(cp.Status.Conditions, ConditionType)
	unchanged := previous != nil && previous.Status == condition.Status && previous.Reason == condition.Reason &&
		previous.Message == condition.Message && previous.ObservedGeneration == condition.ObservedGeneration
	if unchanged && sameDatastore(cp.Status.Datastore, datastore) {
		return nil
	}
	conditions := slices.Clone(cp.Status.Conditions)
	meta.SetStatusCondition(&conditions, condition)
	current, err := runtime.DefaultUnstructuredConverter.ToUnstructured(meta.FindStatusCondition(conditions, ConditionType))
	if err != nil {
		return err
	}
	status := map[string]any{"conditions": []any{current}}
	if datastore != nil {
		status["datastore"] = map[string]any{"kind": datastore.Kind, "name": datastore.Name, "members": int64(datastore.Members)}
	}
	controlPlane := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": bryggav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     status,
	}}
	return r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(controlPlane), client.FieldOwner(FieldManager), client.ForceOwnership)
}

func sameDatastore(a, b *bryggav1alpha1.DatastoreStatus) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Kind == b.Kind && a.Name == b.Name && a.Members == b.Members
}

func newCondition(status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: ConditionType, Status: status, Reason: reason, Message: message}
}
