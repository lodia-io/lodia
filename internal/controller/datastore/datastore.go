// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package datastore is the Datastore controller.
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
	"k8s.io/utils/ptr"
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
	Name                   = "datastore"
	FieldManager           = "brygga/" + Name
	Finalizer              = "brygga.segla.io/controlplane"
	DefaultClassAnnotation = "brygga.segla.io/is-default-class"
	// ControlPlaneUIDLabel on a claim keeps a ControlPlane recreated under the
	// same name from starting on the old one's data.
	ControlPlaneUIDLabel = "brygga.segla.io/control-plane-uid"
	ClassAnnotation      = "brygga.segla.io/datastore-class"
	// ClassGenerationAnnotation is for Lifecycle, which will hold a class
	// edit for each tenant's maintenance window.
	ClassGenerationAnnotation = "brygga.segla.io/datastore-class-generation"
)

// Each message depends on the state alone, so a pass that changes nothing
// writes nothing; an error of the API server goes to the log and leaves the
// status as it was.
const (
	ReasonReconciling            = "Reconciling"
	ReasonProvisioning           = "Provisioning"
	ReasonDatastoreClassNotFound = "DatastoreClassNotFound"
	ReasonVolumesExist           = "VolumesExist"
	ReasonEtcdRefused            = "EtcdRefused"
	ReasonDeleting               = "Deleting"
	ReasonReady                  = "Ready"
)

type Reconciler struct {
	client.Client
	// Reader reads what the cache does not hold: the class's credentials
	// Secret, the claims, the StatefulSet, and an Etcd without the
	// control-plane label.
	Reader client.Reader
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	certificates, err := predicate.LabelSelectorPredicate(metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: bryggav1alpha1.EtcdCertificatesLabel, Operator: metav1.LabelSelectorOpExists}},
	})
	if err != nil {
		return err
	}
	backupCopy := predicate.NewPredicateFuncs(func(secret client.Object) bool {
		cp, ok := secret.GetLabels()[bryggav1alpha1.ControlPlaneLabel]
		return ok && secret.GetName() == backupSecretName(cp)
	})
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&bryggav1alpha1.ControlPlane{}).
		Owns(&druidv1alpha1.Etcd{}, builder.WithPredicates(etcdChanged())).
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(controlPlaneOf), builder.WithPredicates(predicate.Or(certificates, backupCopy))).
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

// The ControlPlanes that name no class are enqueued too: the default may have
// moved.
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
	if meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionDatastoreReady) == nil {
		first := newCondition(metav1.ConditionUnknown, ReasonReconciling, "the first pass has not finished")
		if err := r.applyStatus(ctx, cp, cp.Status.Datastore, first); err != nil {
			return ctrl.Result{}, err
		}
	}
	etcd, condition, requeueAfter, err := r.reconcileEtcd(ctx, cp)
	if condition.Type == "" {
		return ctrl.Result{}, err
	}
	if statusErr := r.applyStatus(ctx, cp, datastoreStatus(etcd), condition); statusErr != nil || err != nil {
		return ctrl.Result{}, errors.Join(err, statusErr)
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *Reconciler) reconcileEtcd(ctx context.Context, cp *bryggav1alpha1.ControlPlane) (*druidv1alpha1.Etcd, metav1.Condition, time.Duration, error) {
	current, err := r.currentEtcd(ctx, cp)
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	// An Etcd another ControlPlane or nobody controls, an orphan or a
	// predecessor's the garbage collector has not removed yet, is never
	// applied: the apply would take it over, and this ControlPlane would start
	// on another tenant's data.
	if current != nil && !metav1.IsControlledBy(current, cp) {
		return nil, newCondition(metav1.ConditionFalse, ReasonVolumesExist, foreignEtcd(current)), time.Minute, nil
	}
	var claims []corev1.PersistentVolumeClaim
	if current == nil {
		if claims, err = r.claims(ctx, cp); err != nil {
			return nil, metav1.Condition{}, 0, err
		}
	}
	class, missing, err := r.class(ctx, cp, current, claims)
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	// A recorded class that is gone stops only its own edits: the Etcd keeps
	// what the class gave it and still takes new certificate generations.
	if class == nil && current == nil {
		return nil, newCondition(metav1.ConditionFalse, ReasonDatastoreClassNotFound, missing), 0, nil
	}
	secrets, err := r.secrets(ctx, cp)
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	generation, found := newestCompleteGeneration(cp, secrets)
	if named, ok := namedGeneration(cp.Name, current); ok && (!found || named > generation) {
		generation, found = named, true
	}
	if !found {
		message := fmt.Sprintf("waiting for a complete generation of the etcd certificates: the Secrets %s-etcd-{%s}-<g> labelled %s=<g>",
			cp.Name, strings.Join(Certificates, ","), bryggav1alpha1.EtcdCertificatesLabel)
		return current, newCondition(metav1.ConditionFalse, ReasonProvisioning, message), 0, nil
	}
	if current == nil {
		if foreign := foreignClaims(cp, claims); len(foreign) > 0 {
			message := fmt.Sprintf("the claims %s of another ControlPlane stand where the etcd of %s would be created: delete them to start empty, or keep them, and this ControlPlane waits",
				strings.Join(foreign, ", "), cp.Name)
			return nil, newCondition(metav1.ConditionFalse, ReasonVolumesExist, message), time.Minute, nil
		}
	}
	var held string
	var heldFor time.Duration
	var refusal error
	if class != nil && class.Spec.Backup != nil {
		blocked, err := r.copyCredentials(ctx, cp, class, secrets)
		if blocked == "" && err != nil {
			return nil, metav1.Condition{}, 0, err
		}
		refusal = err
		if blocked != "" && current == nil {
			message := fmt.Sprintf("DatastoreClass %s cannot be applied yet: %s", class.Name, blocked)
			return nil, newCondition(metav1.ConditionFalse, ReasonProvisioning, message), time.Minute, refusal
		}
		// A class is held whole: applying the rest of an edit could add a store
		// whose copy cannot be made, and the certificate generations still move.
		if blocked != "" {
			held = fmt.Sprintf("DatastoreClass %s is held: %s", class.Name, blocked)
			class, heldFor = nil, time.Minute
		}
	}
	rendered := render(cp, class, generation, current)
	etcd, err := r.apply(ctx, rendered)
	if err != nil {
		err = fmt.Errorf("apply Etcd %s: %w", rendered.Name, err)
		if refused(err) {
			message := note(fmt.Sprintf("the API server refuses Etcd %s on certificate generation %d with the fields of DatastoreClass %s generation %s; the reason is in Brygga's log",
				rendered.Name, generation, rendered.Annotations[ClassAnnotation], rendered.Annotations[ClassGenerationAnnotation]), held)
			return current, newCondition(metav1.ConditionFalse, ReasonEtcdRefused, message), 0, errors.Join(err, refusal)
		}
		return nil, metav1.Condition{}, 0, err
	}
	if err := r.labelClaims(ctx, cp, etcd.Annotations[ClassAnnotation]); err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	absent, err := r.missingSecrets(ctx, cp, etcd, secrets)
	if err != nil {
		return nil, metav1.Condition{}, 0, err
	}
	sts := &appsv1.StatefulSet{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: etcdName(cp.Name)}, sts); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, metav1.Condition{}, 0, err
		}
		sts = nil
	}
	condition, statefulSetLags := readiness(cp.Name, etcd, sts, generation, absent)
	condition.Message = note(condition.Message, held)
	if statefulSetLags {
		return etcd, condition, 15 * time.Second, refusal
	}
	return etcd, condition, heldFor, refusal
}

func note(message, held string) string {
	if held == "" {
		return message
	}
	return message + "; " + held
}

func foreignEtcd(etcd *druidv1alpha1.Etcd) string {
	owner := "it has no controller"
	if ref := metav1.GetControllerOfNoCopy(etcd); ref != nil {
		owner = fmt.Sprintf("its controller is %s %s with UID %s", ref.Kind, ref.Name, ref.UID)
	}
	return fmt.Sprintf("Etcd %s is not this ControlPlane's, %s: delete it and its claims to start empty, or keep them, and this ControlPlane waits",
		etcd.Name, owner)
}

// refused tells the API server's verdict on an object, which sending it again
// cannot change, from a failure to answer: every 4xx is one but a missing
// object, a timeout, a conflict, throttling and a namespace being deleted.
// An admission webhook's denial without a code arrives as 400
// (k8s.io/apiserver@v0.37.1 pkg/admission/plugin/webhook/errors/statuserror.go#L37-L39).
func refused(err error) bool {
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}
	switch code := status.Status().Code; {
	case code < 400 || code > 499, code == 404, code == 408, code == 409, code == 429:
		return false
	}
	return !apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause)
}

// An Etcd without the control-plane label is outside the cache and still
// runs.
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

// A class is resolved once and recorded on the Etcd, so a default that moves
// changes no running etcd; this ControlPlane's claims record it too, so an
// Etcd created again over them keeps it.
func (r *Reconciler) class(ctx context.Context, cp *bryggav1alpha1.ControlPlane, current *druidv1alpha1.Etcd, claims []corev1.PersistentVolumeClaim) (*bryggav1alpha1.DatastoreClass, string, error) {
	name, recordedBy := cp.Spec.Datastore.Dedicated.ClassName, ""
	if current != nil {
		if recorded, ok := current.Annotations[ClassAnnotation]; ok {
			name = recorded
		}
	} else if i := slices.IndexFunc(claims, func(claim corev1.PersistentVolumeClaim) bool {
		return claim.Labels[ControlPlaneUIDLabel] == string(cp.UID) && claim.Annotations[ClassAnnotation] != ""
	}); i >= 0 {
		name, recordedBy = claims[i].Annotations[ClassAnnotation], ", which the claims of this ControlPlane record,"
	}
	if name != "" {
		class := &bryggav1alpha1.DatastoreClass{}
		if err := r.Get(ctx, client.ObjectKey{Name: name}, class); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, "DatastoreClass " + name + recordedBy + " does not exist", nil
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

func (r *Reconciler) secrets(ctx context.Context, cp *bryggav1alpha1.ControlPlane) ([]metav1.PartialObjectMetadata, error) {
	secrets := &metav1.PartialObjectMetadataList{}
	secrets.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("SecretList"))
	if err := r.List(ctx, secrets, client.InNamespace(cp.Namespace), client.MatchingLabels{bryggav1alpha1.ControlPlaneLabel: cp.Name}); err != nil {
		return nil, err
	}
	return secrets.Items, nil
}

// secret finds a Secret's metadata in the cache, and on the API server when
// the cache does not hold it; nil when it does not exist.
func (r *Reconciler) secret(ctx context.Context, cp *bryggav1alpha1.ControlPlane, name string, secrets []metav1.PartialObjectMetadata) (*metav1.PartialObjectMetadata, error) {
	if i := slices.IndexFunc(secrets, func(s metav1.PartialObjectMetadata) bool { return s.Name == name }); i >= 0 {
		return &secrets[i], nil
	}
	secret := &metav1.PartialObjectMetadata{}
	secret.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: cp.Namespace, Name: name}, secret); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return secret, nil
}

// controlledElsewhere returns the owner reference that makes an object
// another owner's: a controller that is not this ControlPlane, or any
// reference to another ControlPlane.
func controlledElsewhere(obj metav1.Object, cp *bryggav1alpha1.ControlPlane) *metav1.OwnerReference {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID != cp.UID && (ptr.Deref(ref.Controller, false) ||
			ref.Kind == "ControlPlane" && strings.HasPrefix(ref.APIVersion, bryggav1alpha1.GroupVersion.Group+"/")) {
			return &ref
		}
	}
	return nil
}

// A terminating Secret is still mounted: etcd-druid holds its finalizer on
// every Secret an Etcd references until none does
// (etcd-druid@v0.39.0 internal/controller/secret/reconciler.go#L44-L101).
func (r *Reconciler) missingSecrets(ctx context.Context, cp *bryggav1alpha1.ControlPlane, etcd *druidv1alpha1.Etcd, secrets []metav1.PartialObjectMetadata) ([]string, error) {
	configs := []*druidv1alpha1.TLSConfig{etcd.Spec.Etcd.ClientUrlTLS, etcd.Spec.Backup.TLS}
	if peer := etcd.Spec.Etcd.PeerUrlTLS; peer != nil {
		configs = append(configs, &peer.TLSConfig)
	}
	var names []string
	for _, tls := range configs {
		if tls != nil {
			names = append(names, tls.TLSCASecretRef.Name, tls.ServerTLSSecretRef.Name, tls.ClientTLSSecretRef.Name)
		}
	}
	if store := etcd.Spec.Backup.Store; store != nil && store.SecretRef != nil {
		names = append(names, store.SecretRef.Name)
	}
	slices.Sort(names)
	var missing []string
	for _, name := range slices.Compact(names) {
		if name == "" {
			continue
		}
		secret, err := r.secret(ctx, cp, name, secrets)
		if err != nil {
			return nil, err
		}
		if secret == nil {
			missing = append(missing, name)
		}
	}
	return missing, nil
}

func newestCompleteGeneration(cp *bryggav1alpha1.ControlPlane, secrets []metav1.PartialObjectMetadata) (int, bool) {
	labelled := make(map[string]string, len(secrets))
	for _, secret := range secrets {
		if generation, ok := secret.Labels[bryggav1alpha1.EtcdCertificatesLabel]; ok && secret.DeletionTimestamp.IsZero() && metav1.IsControlledBy(&secret, cp) {
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
	return newest, found
}

// The Etcd never moves to a generation below the one it names: the newest
// complete one is older while a Secret of the named one is being deleted, and
// the older generation PKI keeps can have expired, as it has for the last
// four months of its life with one-year leaves.
func namedGeneration(cp string, etcd *druidv1alpha1.Etcd) (int, bool) {
	if etcd == nil || etcd.Spec.Etcd.ClientUrlTLS == nil {
		return 0, false
	}
	suffix, ok := strings.CutPrefix(etcd.Spec.Etcd.ClientUrlTLS.ServerTLSSecretRef.Name, etcdName(cp)+"-server-")
	generation, err := strconv.Atoi(suffix)
	return generation, ok && err == nil
}

// Claims are matched by the name the StatefulSet attaches them by,
// <template>-<StatefulSet>-<ordinal> with the template named after the Etcd
// (kubernetes/kubernetes@v1.35.0 pkg/controller/statefulset/stateful_set_utils.go#L116-L119,
// etcd-druid@v0.39.0 internal/component/statefulset/builder.go#L233), not by
// labels, which a claim made by hand lacks.
func (r *Reconciler) claims(ctx context.Context, cp *bryggav1alpha1.ControlPlane) ([]corev1.PersistentVolumeClaim, error) {
	list := &corev1.PersistentVolumeClaimList{}
	if err := r.Reader.List(ctx, list, client.InNamespace(cp.Namespace)); err != nil {
		return nil, err
	}
	prefix := etcdName(cp.Name) + "-" + etcdName(cp.Name) + "-"
	var claims []corev1.PersistentVolumeClaim
	for _, claim := range list.Items {
		ordinal, named := strings.CutPrefix(claim.Name, prefix)
		if n, err := strconv.Atoi(ordinal); named && err == nil && n >= 0 && strconv.Itoa(n) == ordinal {
			claims = append(claims, claim)
		}
	}
	return claims, nil
}

// Starting an etcd on a claim without this ControlPlane's UID would be a
// restore of another tenant's data by name.
func foreignClaims(cp *bryggav1alpha1.ControlPlane, claims []corev1.PersistentVolumeClaim) []string {
	var foreign []string
	for _, claim := range claims {
		if claim.Labels[ControlPlaneUIDLabel] != string(cp.UID) {
			foreign = append(foreign, claim.Name)
		}
	}
	slices.Sort(foreign)
	return foreign
}

// The claims outlive the Etcd, so they carry the UID and the class their
// reclaim goes by.
func (r *Reconciler) labelClaims(ctx context.Context, cp *bryggav1alpha1.ControlPlane, class string) error {
	claims, err := r.claims(ctx, cp)
	if err != nil {
		return err
	}
	for i := range claims {
		claim := &claims[i]
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

// etcd-druid mounts the store's Secret from the Etcd's namespace, and the
// backup-restore sidecar reads the region from it. The first result says why
// the copy cannot be made, the second carries a refusal for the log.
func (r *Reconciler) copyCredentials(ctx context.Context, cp *bryggav1alpha1.ControlPlane, class *bryggav1alpha1.DatastoreClass, secrets []metav1.PartialObjectMetadata) (string, error) {
	store := class.Spec.Backup.Store
	source := &corev1.Secret{}
	if err := r.Reader.Get(ctx, client.ObjectKey{Namespace: store.SecretRef.Namespace, Name: store.SecretRef.Name}, source); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("its credentials Secret %s/%s is missing", store.SecretRef.Namespace, store.SecretRef.Name), nil
		}
		return "", fmt.Errorf("read the credentials Secret %s/%s of DatastoreClass %s: %w", store.SecretRef.Namespace, store.SecretRef.Name, class.Name, err)
	}
	name := backupSecretName(cp.Name)
	existing, err := r.secret(ctx, cp, name, secrets)
	if err != nil {
		return "", err
	}
	if existing != nil {
		if ref := controlledElsewhere(existing, cp); ref != nil {
			return fmt.Sprintf("%s is controlled by %s %s with UID %s", name, ref.Kind, ref.Name, ref.UID), nil
		}
	}
	data := maps.Clone(source.Data)
	if store.Region != "" {
		if data == nil {
			data = map[string][]byte{}
		}
		data["region"] = []byte(store.Region)
	}
	secret := corev1ac.Secret(name, cp.Namespace).
		WithLabels(map[string]string{bryggav1alpha1.ControlPlaneLabel: cp.Name}).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(bryggav1alpha1.GroupVersion.String()).
			WithKind("ControlPlane").
			WithName(cp.Name).
			WithUID(cp.UID).
			WithController(true).
			WithBlockOwnerDeletion(true)).
		WithData(data)
	if err := r.Apply(ctx, secret, client.FieldOwner(FieldManager), client.ForceOwnership); err != nil {
		err = fmt.Errorf("apply Secret %s: %w", name, err)
		if refused(err) {
			return fmt.Sprintf("the API server refuses %s; the reason is in Brygga's log", name), err
		}
		return "", err
	}
	return "", nil
}

func (r *Reconciler) finalize(ctx context.Context, cp *bryggav1alpha1.ControlPlane) (time.Duration, error) {
	if !controllerutil.ContainsFinalizer(cp, Finalizer) {
		return 0, nil
	}
	etcd, err := r.currentEtcd(ctx, cp)
	if err != nil {
		return 0, err
	}
	foreign := etcd != nil && !metav1.IsControlledBy(etcd, cp)
	if foreign {
		etcd = nil
	}
	deleting := newCondition(metav1.ConditionFalse, ReasonDeleting, "the ControlPlane is being deleted: the Etcd goes first, then its claims as the class says")
	if err := r.applyStatus(ctx, cp, datastoreStatus(etcd), deleting); err != nil {
		return 0, err
	}
	// A claim no pass has labelled yet can only be this Etcd's: VolumesExist
	// kept it from being created while anything else stood there.
	if etcd != nil {
		if err := r.labelClaims(ctx, cp, etcd.Annotations[ClassAnnotation]); err != nil {
			return 0, err
		}
	}
	switch {
	case etcd != nil && etcd.DeletionTimestamp.IsZero():
		logf.FromContext(ctx).Info("deleting the Etcd", "etcd", etcd.Name)
		return 0, client.IgnoreNotFound(r.Delete(ctx, etcd))
	case etcd != nil:
		return 10 * time.Second, nil
	case foreign:
		logf.FromContext(ctx).Info("leaving the Etcd and the claims its StatefulSet uses alone, the ControlPlane does not control it", "etcd", etcdName(cp.Name))
	default:
		if err := r.reclaimClaims(ctx, cp); err != nil {
			return 0, err
		}
	}
	if err := r.setFinalizer(ctx, cp, false); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return 0, err
	}
	return 0, nil
}

func (r *Reconciler) reclaimClaims(ctx context.Context, cp *bryggav1alpha1.ControlPlane) error {
	log := logf.FromContext(ctx)
	claims, err := r.claims(ctx, cp)
	if err != nil {
		return err
	}
	for i := range claims {
		claim := &claims[i]
		if claim.Labels[ControlPlaneUIDLabel] != string(cp.UID) || !claim.DeletionTimestamp.IsZero() {
			continue
		}
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

// The finalizer goes through a patch locked on the resourceVersion, never an
// apply, which would create a ControlPlane the cache still showed after its
// deletion. A conflict is a change since the read, which reconciles it again.
func (r *Reconciler) setFinalizer(ctx context.Context, cp *bryggav1alpha1.ControlPlane, held bool) error {
	patch := client.MergeFromWithOptions(cp.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if held {
		controllerutil.AddFinalizer(cp, Finalizer)
	} else {
		controllerutil.RemoveFinalizer(cp, Finalizer)
	}
	return r.Patch(ctx, cp, patch, client.FieldOwner(FieldManager))
}

func datastoreStatus(etcd *druidv1alpha1.Etcd) *bryggav1alpha1.DatastoreStatus {
	if etcd == nil {
		return nil
	}
	return &bryggav1alpha1.DatastoreStatus{Kind: "Dedicated", Name: etcd.Name, Members: int32(len(etcd.Status.Members))}
}

func (r *Reconciler) applyStatus(ctx context.Context, cp *bryggav1alpha1.ControlPlane, datastore *bryggav1alpha1.DatastoreStatus, condition metav1.Condition) error {
	condition.ObservedGeneration = cp.Generation
	previous := meta.FindStatusCondition(cp.Status.Conditions, bryggav1alpha1.ConditionDatastoreReady)
	unchanged := previous != nil && previous.Status == condition.Status && previous.Reason == condition.Reason &&
		previous.Message == condition.Message && previous.ObservedGeneration == condition.ObservedGeneration
	if unchanged && sameDatastore(cp.Status.Datastore, datastore) {
		return nil
	}
	conditions := slices.Clone(cp.Status.Conditions)
	meta.SetStatusCondition(&conditions, condition)
	current, err := runtime.DefaultUnstructuredConverter.ToUnstructured(meta.FindStatusCondition(conditions, bryggav1alpha1.ConditionDatastoreReady))
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
	return metav1.Condition{Type: bryggav1alpha1.ConditionDatastoreReady, Status: status, Reason: reason, Message: message}
}
