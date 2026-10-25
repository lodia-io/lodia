// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package datastore is the Datastore controller for the dedicated case: one
// etcd-druid Etcd per ControlPlane, built from its DatastoreClass and the
// newest complete generation of the etcd certificates, held by the
// ControlPlane's finalizer and reported through DatastoreReady.
package datastore

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
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

	// ReasonProvisioning says the certificates or etcd-druid's report are awaited.
	ReasonProvisioning = "Provisioning"
	// ReasonDatastoreClassNotFound says no class serves the ControlPlane.
	ReasonDatastoreClassNotFound = "DatastoreClassNotFound"
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
		Owns(&druidv1alpha1.Etcd{}).
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
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &bryggav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cp.Spec.Datastore.Dedicated == nil {
		return ctrl.Result{}, nil
	}
	if !cp.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, cp)
	}
	if !controllerutil.ContainsFinalizer(cp, Finalizer) {
		if err := r.setFinalizer(ctx, cp, true); err != nil {
			return ctrl.Result{}, err
		}
	}
	etcd, condition, requeueAfter, err := r.reconcileEtcd(ctx, cp)
	if err != nil {
		condition = newCondition(metav1.ConditionFalse, ReasonProvisioning, err.Error())
	}
	if statusErr := r.applyStatus(ctx, cp, etcd, condition); statusErr != nil {
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// reconcileEtcd applies the Etcd once the class and a complete certificate
// generation exist, and says what DatastoreReady reports meanwhile.
func (r *Reconciler) reconcileEtcd(ctx context.Context, cp *bryggav1alpha1.ControlPlane) (*druidv1alpha1.Etcd, metav1.Condition, time.Duration, error) {
	current := &druidv1alpha1.Etcd{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: etcdName(cp.Name)}, current); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, metav1.Condition{}, 0, err
		}
		current = nil
	}
	class, err := r.class(ctx, cp)
	if _, missing := errors.AsType[classNotFound](err); missing {
		return current, newCondition(metav1.ConditionFalse, ReasonDatastoreClassNotFound, err.Error()), 0, nil
	}
	if err != nil {
		return current, metav1.Condition{}, 0, err
	}
	generation, found, err := r.newestCompleteGeneration(ctx, cp)
	if err != nil {
		return current, metav1.Condition{}, 0, err
	}
	if !found {
		message := fmt.Sprintf("waiting for a complete generation of the etcd certificates: the Secrets %s-etcd-{%s}-<g> labelled %s=<g>",
			cp.Name, strings.Join(Certificates, ","), CertificatesLabel)
		return current, newCondition(metav1.ConditionFalse, ReasonProvisioning, message), 0, nil
	}
	if class.Spec.Backup != nil {
		if err := r.copyCredentials(ctx, cp, class); err != nil {
			if apierrors.IsNotFound(err) {
				return current, newCondition(metav1.ConditionFalse, ReasonProvisioning, err.Error()), time.Minute, nil
			}
			return current, metav1.Condition{}, 0, err
		}
	}
	etcd, err := r.apply(ctx, render(cp, class, generation))
	if err != nil {
		return current, metav1.Condition{}, 0, fmt.Errorf("apply Etcd %s: %w", etcdName(cp.Name), err)
	}
	return etcd, readiness(etcd), 0, nil
}

type classNotFound string

func (e classNotFound) Error() string { return string(e) }

// class is the DatastoreClass the ControlPlane names, or the one default class.
func (r *Reconciler) class(ctx context.Context, cp *bryggav1alpha1.ControlPlane) (*bryggav1alpha1.DatastoreClass, error) {
	if name := cp.Spec.Datastore.Dedicated.ClassName; name != "" {
		class := &bryggav1alpha1.DatastoreClass{}
		if err := r.Get(ctx, client.ObjectKey{Name: name}, class); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, classNotFound("DatastoreClass " + name + " does not exist")
			}
			return nil, err
		}
		return class, nil
	}
	classes := &bryggav1alpha1.DatastoreClassList{}
	if err := r.List(ctx, classes); err != nil {
		return nil, err
	}
	var defaults []*bryggav1alpha1.DatastoreClass
	for i := range classes.Items {
		if classes.Items[i].Annotations[DefaultClassAnnotation] == "true" {
			defaults = append(defaults, &classes.Items[i])
		}
	}
	switch len(defaults) {
	case 1:
		return defaults[0], nil
	case 0:
		return nil, classNotFound("no DatastoreClass is annotated " + DefaultClassAnnotation + `: "true"`)
	default:
		names := make([]string, 0, len(defaults))
		for _, class := range defaults {
			names = append(names, class.Name)
		}
		slices.Sort(names)
		return nil, classNotFound(fmt.Sprintf("%d DatastoreClasses are annotated %s: %q, exactly one may be: %s",
			len(defaults), DefaultClassAnnotation, "true", strings.Join(names, ", ")))
	}
}

// newestCompleteGeneration lists the etcd certificate Secrets, metadata only,
// and picks the newest generation whose five Secrets all carry its label.
func (r *Reconciler) newestCompleteGeneration(ctx context.Context, cp *bryggav1alpha1.ControlPlane) (int, bool, error) {
	secrets := &metav1.PartialObjectMetadataList{}
	secrets.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("SecretList"))
	if err := r.List(ctx, secrets, client.InNamespace(cp.Namespace), client.MatchingLabels{bryggav1alpha1.ControlPlaneLabel: cp.Name}); err != nil {
		return 0, false, err
	}
	labelled := make(map[string]string, len(secrets.Items))
	for _, secret := range secrets.Items {
		if generation, ok := secret.Labels[CertificatesLabel]; ok {
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

// copyCredentials copies the class's credentials Secret into the tenant's
// namespace, where etcd-druid mounts it, with the class's region under the key
// the backup-restore sidecar reads.
func (r *Reconciler) copyCredentials(ctx context.Context, cp *bryggav1alpha1.ControlPlane, class *bryggav1alpha1.DatastoreClass) error {
	store := class.Spec.Backup.Store
	source := &corev1.Secret{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: store.SecretRef.Namespace, Name: store.SecretRef.Name}, source); err != nil {
		return fmt.Errorf("read the credentials Secret %s/%s of DatastoreClass %s: %w", store.SecretRef.Namespace, store.SecretRef.Name, class.Name, err)
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
	return r.Apply(ctx, secret, client.FieldOwner(FieldManager), client.ForceOwnership)
}

// finalize deletes the Etcd, waits for etcd-druid to let it go, applies the
// class's reclaim policy to the claims its StatefulSet left behind and
// releases the finalizer.
func (r *Reconciler) finalize(ctx context.Context, cp *bryggav1alpha1.ControlPlane) error {
	if !controllerutil.ContainsFinalizer(cp, Finalizer) {
		return nil
	}
	log := logf.FromContext(ctx)
	etcd := &druidv1alpha1.Etcd{}
	err := r.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: etcdName(cp.Name)}, etcd)
	switch {
	case err == nil && etcd.DeletionTimestamp.IsZero():
		log.Info("deleting the Etcd", "etcd", etcd.Name)
		return client.IgnoreNotFound(r.Delete(ctx, etcd))
	case err == nil:
		return nil
	case !apierrors.IsNotFound(err):
		return err
	}
	class, err := r.class(ctx, cp)
	_, classMissing := errors.AsType[classNotFound](err)
	switch {
	case classMissing:
		log.Info("keeping the claims, the class is gone", "reason", err.Error())
	case err != nil:
		return err
	case class.Spec.Storage.ReclaimPolicy == "Delete":
		if err := r.deleteClaims(ctx, cp); err != nil {
			return err
		}
	}
	return r.setFinalizer(ctx, cp, false)
}

// deleteClaims removes the PersistentVolumeClaims of the Etcd's StatefulSet,
// which etcd-druid keeps past the Etcd. The StatefulSet controller labels
// them with the selector etcd-druid gives the StatefulSet
// (kubernetes/kubernetes@v1.35.0 pkg/controller/statefulset/stateful_set_utils.go#L389-L407,
// etcd-druid@v0.39.0 internal/component/statefulset/builder.go#L149-L151).
func (r *Reconciler) deleteClaims(ctx context.Context, cp *bryggav1alpha1.ControlPlane) error {
	log := logf.FromContext(ctx)
	claims := &corev1.PersistentVolumeClaimList{}
	selector := druidv1alpha1.GetDefaultLabels(metav1.ObjectMeta{Name: etcdName(cp.Name)})
	if err := r.Reader.List(ctx, claims, client.InNamespace(cp.Namespace), client.MatchingLabels(selector)); err != nil {
		return err
	}
	for i := range claims.Items {
		log.Info("deleting the claim", "claim", claims.Items[i].Name)
		if err := client.IgnoreNotFound(r.Delete(ctx, &claims.Items[i])); err != nil {
			return err
		}
	}
	return nil
}

// setFinalizer holds or releases the finalizer with a patch locked on the
// ControlPlane's resourceVersion, never with an apply: an apply would create
// a ControlPlane the cache still showed after its deletion. A conflict means
// the ControlPlane changed since it was read, and that change reconciles it
// again; a ControlPlane that is gone stays gone.
func (r *Reconciler) setFinalizer(ctx context.Context, cp *bryggav1alpha1.ControlPlane, held bool) error {
	patch := client.MergeFromWithOptions(cp.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if held {
		controllerutil.AddFinalizer(cp, Finalizer)
	} else {
		controllerutil.RemoveFinalizer(cp, Finalizer)
	}
	err := r.Patch(ctx, cp, patch, client.FieldOwner(FieldManager))
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return err
}

// applyStatus writes the condition and status.datastore through the status
// subresource, the only fields of the ControlPlane's status this controller owns.
func (r *Reconciler) applyStatus(ctx context.Context, cp *bryggav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd, condition metav1.Condition) error {
	condition.ObservedGeneration = cp.Generation
	conditions := slices.Clone(cp.Status.Conditions)
	meta.SetStatusCondition(&conditions, condition)
	current, err := runtime.DefaultUnstructuredConverter.ToUnstructured(meta.FindStatusCondition(conditions, ConditionType))
	if err != nil {
		return err
	}
	status := map[string]any{"conditions": []any{current}}
	if etcd != nil {
		status["datastore"] = map[string]any{"kind": "Dedicated", "name": etcd.Name, "members": int64(len(etcd.Status.Members))}
	}
	controlPlane := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": bryggav1alpha1.GroupVersion.String(),
		"kind":       "ControlPlane",
		"metadata":   map[string]any{"name": cp.Name, "namespace": cp.Namespace},
		"status":     status,
	}}
	return r.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(controlPlane), client.FieldOwner(FieldManager), client.ForceOwnership)
}

func newCondition(status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: ConditionType, Status: status, Reason: reason, Message: message}
}
