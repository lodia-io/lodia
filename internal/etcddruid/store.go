// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/contract"
)

// maxClassName and maxClassUID are the MaxLength of status.datastore.class's
// name and uid, which the API server counts in characters.
const maxClassName, maxClassUID = 253, 36

// Store is a ControlPlane's etcd, its Etcd, as the engine read or rendered it.
type Store struct {
	etcd *druidv1alpha1.Etcd
}

// Rollout is etcd-druid's StatefulSet of a ControlPlane's Etcd as read.
type Rollout struct {
	sts *appsv1.StatefulSet
}

func (s *Store) object() *druidv1alpha1.Etcd {
	if s == nil {
		return nil
	}
	return s.etcd
}

func (r *Rollout) statefulSet() *appsv1.StatefulSet {
	if r == nil {
		return nil
	}
	return r.sts
}

func etcdKey(cp *lodiav1alpha1.ControlPlane) client.ObjectKey {
	return client.ObjectKey{Namespace: cp.Namespace, Name: contract.EtcdName(cp.Name)}
}

// Current reads cp's Etcd from cache and, while cache does not hold it, from
// live: an Etcd without the control-plane label is outside the cache and
// still runs. It is nil when there is none.
func (e *Engine) Current(ctx context.Context, cache, live client.Reader, cp *lodiav1alpha1.ControlPlane) (*Store, error) {
	etcd := &druidv1alpha1.Etcd{}
	key := etcdKey(cp)
	err := cache.Get(ctx, key, etcd)
	if apierrors.IsNotFound(err) {
		err = live.Get(ctx, key, etcd)
	}
	switch {
	case apierrors.IsNotFound(err):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return &Store{etcd: etcd}, nil
}

// Get reads cp's Etcd from reader, nil when there is none.
func (e *Engine) Get(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane) (*Store, error) {
	etcd := &druidv1alpha1.Etcd{}
	if err := reader.Get(ctx, etcdKey(cp), etcd); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return &Store{etcd: etcd}, nil
}

// Reread reads s's Etcd again from reader.
func (e *Engine) Reread(ctx context.Context, reader client.Reader, s *Store) (*Store, error) {
	etcd := &druidv1alpha1.Etcd{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(s.etcd), etcd); err != nil {
		return nil, fmt.Errorf("read Etcd %s: %w", s.etcd.Name, err)
	}
	return &Store{etcd: etcd}, nil
}

// Raised says whether the Etcd asks for more than one member. The cache can
// lag Datastore's own scale-up, so an Etcd it holds at one is read again from
// reader.
func (e *Engine) Raised(ctx context.Context, reader client.Reader, current *Store) (bool, error) {
	if current.etcd.Spec.Replicas > 1 {
		return true, nil
	}
	etcd := &druidv1alpha1.Etcd{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(current.etcd), etcd); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read Etcd %s: %w", current.etcd.Name, err)
	}
	return etcd.Spec.Replicas > 1, nil
}

// Rollout reads etcd-druid's StatefulSet of cp's Etcd from reader, nil when
// there is none. etcd-druid syncs the StatefulSet before it removes its
// annotation (etcd-druid@v0.39.2 internal/controller/etcd/reconciler.go#L119-L142),
// so a rollout read before an Etcd shows its request served can predate it.
func (e *Engine) Rollout(ctx context.Context, reader client.Reader, cp *lodiav1alpha1.ControlPlane) (*Rollout, error) {
	sts := &appsv1.StatefulSet{}
	if err := reader.Get(ctx, etcdKey(cp), sts); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &Rollout{sts: sts}, nil
}

// NamedGeneration is the certificate generation the Etcd names, false for no
// Etcd. The Etcd never moves to a generation below the one it names: the
// newest complete one is older while a Secret of the named one is being
// deleted, and the older generation PKI keeps can have expired, as it has for
// the last four months of its life with one-year leaves.
func (e *Engine) NamedGeneration(cp string, s *Store) (int, bool) {
	etcd := s.object()
	if etcd == nil || etcd.Spec.Etcd.ClientUrlTLS == nil {
		return 0, false
	}
	role, generation, ok := contract.ParseEtcdCertificateSecretName(cp, etcd.Spec.Etcd.ClientUrlTLS.ServerTLSSecretRef.Name)
	return generation, ok && role == contract.EtcdServerRole
}

// BackupReady reports etcd-druid's BackupReady of the Etcd the pass applied,
// which reads the snapshot Leases' renewals
// (etcd-druid@v0.39.2 internal/health/condition/check_backup_ready.go#L34-L125).
func (e *Engine) BackupReady(s *Store) metav1.Condition {
	etcd := s.object()
	condition := func(status metav1.ConditionStatus, reason, message string) metav1.Condition {
		return metav1.Condition{Type: lodiav1alpha1.ConditionBackupReady, Status: status, Reason: reason, Message: message}
	}
	if etcd == nil {
		return condition(metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForDatastore, "this ControlPlane has no Etcd yet")
	}
	if etcd.Spec.Backup.Store == nil {
		return condition(metav1.ConditionFalse, lodiav1alpha1.ReasonNoBackupStore, fmt.Sprintf("Etcd %s has no backup store: its class has none", etcd.Name))
	}
	druid := druidv1alpha1.GetCondition(etcd, druidv1alpha1.ConditionTypeBackupReady)
	switch {
	case druid == nil:
		return condition(metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotsPending, fmt.Sprintf("etcd-druid reports no BackupReady of Etcd %s yet", etcd.Name))
	case druid.Status == druidv1alpha1.ConditionTrue:
		return condition(metav1.ConditionTrue, lodiav1alpha1.ReasonBackupSucceeded, fmt.Sprintf("etcd-druid reports of Etcd %s: %s", etcd.Name, druid.Message))
	case druid.Status == druidv1alpha1.ConditionFalse:
		return condition(metav1.ConditionFalse, lodiav1alpha1.ReasonBackupFailed, fmt.Sprintf("etcd-druid reports of Etcd %s: %s", etcd.Name, druid.Message))
	}
	return condition(metav1.ConditionUnknown, lodiav1alpha1.ReasonSnapshotsPending, fmt.Sprintf("etcd-druid reports BackupReady %s (%s) of Etcd %s: %s", druid.Status, druid.Reason, etcd.Name, druid.Message))
}

func (s *Store) Name() string { return s.etcd.Name }

func (s *Store) Namespace() string { return s.etcd.Namespace }

// ControlledBy says whether owner is the Etcd's controller.
func (s *Store) ControlledBy(owner metav1.Object) bool { return metav1.IsControlledBy(s.etcd, owner) }

// Foreign is the message of an Etcd another ControlPlane or nobody controls.
func (s *Store) Foreign() string {
	owner := "it has no controller"
	if ref := metav1.GetControllerOfNoCopy(s.etcd); ref != nil {
		owner = fmt.Sprintf("its controller is %s %s with UID %s", ref.Kind, ref.Name, ref.UID)
	}
	return fmt.Sprintf("Etcd %s is not this ControlPlane's, %s: delete it and its claims to start empty, or keep them, and this ControlPlane waits",
		s.etcd.Name, owner)
}

func (s *Store) Deleting() bool { return !s.etcd.DeletionTimestamp.IsZero() }

// Class is the name of the class the Etcd records, and whether it records one.
func (s *Store) Class() (string, bool) {
	name, ok := s.etcd.Annotations[lodiav1alpha1.DatastoreClassAnnotation]
	return name, ok
}

// ClassGeneration is the class generation the Etcd records, as written.
func (s *Store) ClassGeneration() string {
	return s.etcd.Annotations[lodiav1alpha1.DatastoreClassGenerationAnnotation]
}

// RecordedClass is the class the Etcd was rendered from, as its annotations
// record it, and whether they record one status.datastore.class takes: a
// person can edit them to any value.
func (s *Store) RecordedClass() (lodiav1alpha1.DatastoreClassStatus, bool) {
	return recordedClass(s.etcd)
}

func recordedClass(etcd *druidv1alpha1.Etcd) (lodiav1alpha1.DatastoreClassStatus, bool) {
	generation, err := strconv.ParseInt(etcd.Annotations[lodiav1alpha1.DatastoreClassGenerationAnnotation], 10, 64)
	class := lodiav1alpha1.DatastoreClassStatus{
		Name:                     etcd.Annotations[lodiav1alpha1.DatastoreClassAnnotation],
		DatastoreClassGeneration: lodiav1alpha1.DatastoreClassGeneration{UID: types.UID(etcd.Annotations[lodiav1alpha1.DatastoreClassUIDAnnotation]), Generation: generation},
	}
	return class, class.Name != "" && utf8.RuneCountInString(class.Name) <= maxClassName &&
		class.UID != "" && utf8.RuneCountInString(string(class.UID)) <= maxClassUID &&
		err == nil && generation >= 1
}

// ClassStatus is status.datastore.class as the Etcd's annotations record it,
// nil while they record none.
func (s *Store) ClassStatus() *lodiav1alpha1.DatastoreClassStatus {
	if recorded, ok := recordedClass(s.etcd); ok {
		return &recorded
	}
	return nil
}

// Revision is the operator revision the Etcd is stamped with.
func (s *Store) Revision() string {
	return s.etcd.Annotations[lodiav1alpha1.OperatorRevisionAnnotation]
}

// Stamp stamps the rendered Etcd with the operator revision.
func (s *Store) Stamp(revision string) {
	metav1.SetMetaDataAnnotation(&s.etcd.ObjectMeta, lodiav1alpha1.OperatorRevisionAnnotation, revision)
}

// Members is how many members the Etcd asks for, and ReadyMembers how many of
// them etcd-druid reports ready.
func (s *Store) Members() int32 { return s.etcd.Spec.Replicas }

func (s *Store) ReadyMembers() int32 { return min(s.etcd.Status.ReadyReplicas, s.etcd.Spec.Replicas) }

// UpdatedMembers counts, once etcd-druid and the StatefulSet controller have
// observed the Etcd's spec, its members that are available: all available
// ones less those of older revisions.
func (s *Store) UpdatedMembers(rollout *Rollout) *int32 {
	return updatedMembers(s.etcd, rollout.statefulSet())
}

func updatedMembers(etcd *druidv1alpha1.Etcd, sts *appsv1.StatefulSet) *int32 {
	if sts == nil || ptr.Deref(etcd.Status.ObservedGeneration, 0) != etcd.Generation || sts.Status.ObservedGeneration != sts.Generation {
		return nil
	}
	s := sts.Status
	return new(max(0, min(s.UpdatedReplicas, s.AvailableReplicas-(s.Replicas-s.UpdatedReplicas))))
}

// RolledOut says whether etcd-druid has served the request for the Etcd's
// generation and stamp and every member runs that spec: a revision that
// changes no field of the spec rolls a new etcd-druid's render with the
// generation unchanged, and etcd-druid can mark as observed a generation it
// did not reconcile (reconcile_complete.go#L33-L36), so a class edit would
// otherwise count as rolled out, and free its slot, before etcd-druid rolls
// it.
func (s *Store) RolledOut(rollout *Rollout) bool {
	updated := updatedMembers(s.etcd, rollout.statefulSet())
	return served(s.etcd) && updated != nil && *updated == s.etcd.Spec.Replicas
}

// Served says whether etcd-druid has reconciled the request for the Etcd's
// generation and stamp.
func (s *Store) Served() bool { return served(s.etcd) }

func (s *Store) Formed() bool { return formed(s.etcd) }

func formed(etcd *druidv1alpha1.Etcd) bool {
	mark, ok := etcd.Annotations[lodiav1alpha1.EtcdFormedAnnotation]
	return ok && mark == string(etcd.UID)
}

// Readiness is DatastoreReady of the Etcd, whose Secrets missing do not exist.
func (s *Store) Readiness(missing []string) metav1.Condition { return readiness(s.etcd, missing) }

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

func newCondition(status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: status, Reason: reason, Message: message}
}

// MemberNote says why the applied Etcd does not have the members asked for,
// from the applied Etcd: its status can be newer than the cached one
// membersHeld read.
func (s *Store) MemberNote(members int32) string {
	etcd := s.etcd
	if etcd.Spec.Replicas < members {
		return fmt.Sprintf("highAvailability waits for Etcd %s to settle: every member on its current spec and available, etcd-druid's last scale completed", etcd.Name)
	}
	if kept, ready := keptMember(etcd); etcd.Spec.Replicas > members && !ready {
		return fmt.Sprintf("removing highAvailability waits for etcd-druid to list member %s, the one it keeps, as ready", kept)
	}
	return ""
}

// keptMember names the member a removal of highAvailability keeps and says
// whether etcd-druid lists it ready.
func keptMember(etcd *druidv1alpha1.Etcd) (string, bool) {
	name := druidv1alpha1.GetOrdinalPodName(etcd.ObjectMeta, 0)
	return name, slices.ContainsFunc(etcd.Status.Members, func(member druidv1alpha1.EtcdMemberStatus) bool {
		return member.Name == name && member.Status == druidv1alpha1.EtcdMemberStatusReady
	})
}

// RequestRefused is the message of a request that etcd-druid reconcile the
// Etcd which the API server refuses.
func (s *Store) RequestRefused() string {
	return fmt.Sprintf("the API server refuses the request that etcd-druid reconcile Etcd %s, the annotations %s and %s; the reason is in Lodia's log",
		s.etcd.Name, druidv1alpha1.DruidOperationAnnotation, lodiav1alpha1.EtcdReconcileRequestedAnnotation)
}

// Secrets are the names of the Secrets the Etcd mounts, sorted, each once. A
// terminating Secret is still mounted: etcd-druid holds its finalizer on
// every Secret an Etcd references until none does
// (etcd-druid@v0.39.2 internal/controller/secret/reconciler.go#L44-L101).
func (s *Store) Secrets() []string {
	etcd := s.etcd
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
	var mounted []string
	for _, name := range slices.Compact(names) {
		if name != "" {
			mounted = append(mounted, name)
		}
	}
	return mounted
}

// KeptCap notes the cap of class's quota on an Etcd that keeps its own quota,
// under the operator hold or while its class waits or is held, as long as
// that quota is the cap.
func (s *Store) KeptCap(class *lodiav1alpha1.DatastoreClass) string {
	etcd := s.etcd
	quota := etcd.Spec.Etcd.Quota
	if class == nil || quota == nil {
		return ""
	}
	capped, note := cappedQuota(class, ptr.Deref(etcd.Spec.StorageCapacity, class.Spec.Storage.Size), etcd.Name)
	if capped.Cmp(*quota) != 0 {
		return ""
	}
	return note
}

// OnStore says whether the Etcd names the store backup renders for cp.
func (s *Store) OnStore(cp *lodiav1alpha1.ControlPlane, backup *lodiav1alpha1.EtcdBackup, namespaceUID types.UID) bool {
	return equality.Semantic.DeepEqual(store(cp, backup, namespaceUID), s.etcd.Spec.Backup.Store)
}

// Endpoint is etcd-druid's client Service of the Etcd
// (etcd-druid@v0.39.2 api/core/v1alpha1/helper.go#L27-L30), which a member
// without a leader leaves about 30 s later, by the readiness probe etcd-druid
// gives it (etcd-druid@v0.39.2 internal/component/statefulset/builder.go#L603-L610).
func (s *Store) Endpoint() (string, int32) { return clientEndpoint(s.etcd) }

func clientEndpoint(etcd *druidv1alpha1.Etcd) (string, int32) {
	return druidv1alpha1.GetClientServiceName(etcd.ObjectMeta) + "." + etcd.Namespace + ".svc", druidv1alpha1.GetClientPort(etcd)
}

// Snapshots is where the Etcd's store puts its snapshots, nil without a
// store.
func (s *Store) Snapshots() *lodiav1alpha1.SnapshotLocation {
	store := s.etcd.Spec.Backup.Store
	if store == nil {
		return nil
	}
	return &lodiav1alpha1.SnapshotLocation{
		Provider: lodiav1alpha1.StoreProvider(ptr.Deref(store.Provider, "")),
		Endpoint: ptr.Deref(store.EndpointOverride, ""),
		Bucket:   ptr.Deref(store.Container, ""),
		Prefix:   store.Prefix,
	}
}

// Quota is the Etcd's quota.
func (s *Store) Quota() *resource.Quantity { return s.etcd.Spec.Etcd.Quota }

// DeltaSnapshotPeriod is the Etcd's delta snapshot period, false without a
// store or a period.
func (s *Store) DeltaSnapshotPeriod() (time.Duration, bool) {
	period := s.etcd.Spec.Backup.DeltaSnapshotPeriod
	if s.etcd.Spec.Backup.Store == nil || period == nil {
		return 0, false
	}
	return period.Duration, true
}

// etcd-druid refreshes every Etcd's status every 15 seconds
// (etcd-druid@v0.39.2 api/config/v1alpha1/defaults.go#L104, charts/values.yaml#L63)
// with new timestamps alone; only a change readiness, status.datastore,
// membersHeld or a request to etcd-druid reads gets a pass.
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
	fmt.Fprintf(&b, "%d %t %d %t %d/%d %d %t %q %t;", etcd.Generation, !etcd.DeletionTimestamp.IsZero(), ptr.Deref(etcd.Status.ObservedGeneration, 0),
		ptr.Deref(etcd.Status.Ready, false), etcd.Status.ReadyReplicas, etcd.Spec.Replicas, len(etcd.Status.Members), kept,
		etcd.Annotations[lodiav1alpha1.EtcdReconcileRequestedAnnotation], druidv1alpha1.HasReconcileOperationAnnotation(etcd.ObjectMeta))
	if last := etcd.Status.LastOperation; last != nil {
		fmt.Fprintf(&b, "%s %s/%s;", last.RunID, last.Type, last.State)
	}
	for _, c := range etcd.Status.Conditions {
		fmt.Fprintf(&b, "%s=%s/%s;", c.Type, c.Status, c.Reason)
	}
	return b.String()
}
