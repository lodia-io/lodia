// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/contract"
)

// sightingsMu guards the sightings of every Engine, which sighted makes on
// first use: an Engine is a struct literal, and a copy shares its map but
// would not share a lock of its own.
var sightingsMu sync.Mutex

type sighting struct {
	run string
	at  time.Time
}

// RequestReconcile asks etcd-druid, which reconciles a new Etcd's spec
// unasked, past the wait below, and any other's only on request
// (etcd-druid@v0.39.2 internal/controller/etcd/reconcile_spec.go#L200-L224),
// to reconcile s until the request for its generation and stamp is served:
// its annotation and Lodia's record of the request go together, under the
// resourceVersion of the apply's response, so that a pass on an older
// response conflicts and the next one asks again, and the record names the
// run ID that lock makes current when the annotation lands. A request waits
// while etcd-druid's annotation is there or a spec reconcile is in flight,
// and the watch brings the pass after the annotation's removal or the
// reconcile's last write of lastOperation. A request under a revision the
// record does not name waits for etcd-druid's Deployment, read from live, to
// run the version the operator was given in every replica: an older
// etcd-druid would take it and roll the members to its own render, and the
// second result notes that wait. A request the API server refuses comes back
// with s and the error.
func (e *Engine) RequestReconcile(ctx context.Context, c client.Client, live client.Reader, clk clock.PassiveClock, manager string, s *Store) (*Store, string, error) {
	etcd := s.etcd
	// inFlight goes first: it records and drops the sightings.
	if e.inFlight(clk, etcd) || druidv1alpha1.HasReconcileOperationAnnotation(etcd.ObjectMeta) || served(etcd) {
		return s, "", nil
	}
	recorded, ok := etcd.Annotations[lodiav1alpha1.EtcdReconcileRequestedAnnotation]
	asked, _, _ := cutLast(recorded, "/")
	stamp := etcd.Annotations[lodiav1alpha1.OperatorRevisionAnnotation]
	if _, revision, _ := strings.Cut(asked, "/"); !ok || revision != stamp {
		pending, err := e.druidPending(ctx, live)
		if err != nil {
			return nil, "", err
		}
		if pending != "" {
			return s, fmt.Sprintf("etcd-druid is asked to reconcile Etcd %s under operator revision %s once %s", etcd.Name, stamp, pending), nil
		}
	}
	requested := etcd.DeepCopy()
	metav1.SetMetaDataAnnotation(&requested.ObjectMeta, druidv1alpha1.DruidOperationAnnotation, druidv1alpha1.DruidOperationReconcile)
	metav1.SetMetaDataAnnotation(&requested.ObjectMeta, lodiav1alpha1.EtcdReconcileRequestedAnnotation, requestOf(etcd)+"/"+runID(etcd))
	err := c.Patch(ctx, requested, client.MergeFromWithOptions(etcd, client.MergeFromWithOptimisticLock{}), client.FieldOwner(manager))
	switch {
	case apierrors.IsConflict(err):
		return s, "", nil
	case apistatus.Refused(err):
		return s, "", fmt.Errorf("ask etcd-druid to reconcile Etcd %s: %w", etcd.Name, err)
	case err != nil:
		return nil, "", fmt.Errorf("ask etcd-druid to reconcile Etcd %s: %w", etcd.Name, err)
	}
	return &Store{etcd: requested}, "", nil
}

// requestOf is the generation and stamp a request to reconcile etcd asks for.
func requestOf(etcd *druidv1alpha1.Etcd) string {
	return fmt.Sprintf("%d/%s", etcd.Generation, etcd.Annotations[lodiav1alpha1.OperatorRevisionAnnotation])
}

func runID(etcd *druidv1alpha1.Etcd) string {
	if last := etcd.Status.LastOperation; last != nil {
		return last.RunID
	}
	return ""
}

// served says whether etcd-druid has reconciled the request for etcd's
// generation and stamp: its annotation is gone, and a spec reconcile
// succeeded under a run ID other than the recorded one. etcd-druid writes a
// new run ID and the type Reconcile in a status patch at the start of every
// spec reconcile, a new Etcd's first included, and syncs the Etcd that patch
// returns (reconcile_spec.go#L33-L42, reconcile_spec.go#L112-L118,
// internal/controller/utils/etcdstatus.go#L83-L93), so such a reconcile read
// the request's spec; one under the recorded run ID started before the request
// landed, and may have removed the annotation without syncing that spec.
func served(etcd *druidv1alpha1.Etcd) bool {
	asked, run, ok := cutLast(etcd.Annotations[lodiav1alpha1.EtcdReconcileRequestedAnnotation], "/")
	last := etcd.Status.LastOperation
	return ok && asked == requestOf(etcd) && !druidv1alpha1.HasReconcileOperationAnnotation(etcd.ObjectMeta) && last != nil &&
		last.Type == druidv1alpha1.LastOperationTypeReconcile && last.State == druidv1alpha1.LastOperationStateSucceeded && last.RunID != run
}

// specReconcileBound outlasts one spec reconcile of etcd-druid, which writes
// status.lastOperation at its start and at its end only
// (reconcile_spec.go#L33-L49).
const specReconcileBound = 3 * time.Minute

// inFlight says whether etcd-druid runs a spec reconcile under a run ID other
// than the recorded one, which started after the last request landed: a
// request made meanwhile would record that run ID, which then removes the
// annotation, and etcd-druid's next pass from its cache would repeat that. A
// reconcile seen in flight for specReconcileBound is one a stopped etcd-druid
// left. The bound runs on the operator's clock from this operator's first
// sight of that run: lastUpdateTime is etcd-druid's.
func (e *Engine) inFlight(clk clock.PassiveClock, etcd *druidv1alpha1.Etcd) bool {
	_, run, _ := cutLast(etcd.Annotations[lodiav1alpha1.EtcdReconcileRequestedAnnotation], "/")
	seen, processing := e.sighted(clk, etcd)
	return processing && etcd.Status.LastOperation.RunID != run && clk.Since(seen) < specReconcileBound
}

// sighted is when this operator first saw etcd Processing under its run ID,
// and false while etcd is not Processing, whose sighting it drops.
func (e *Engine) sighted(clk clock.PassiveClock, etcd *druidv1alpha1.Etcd) (time.Time, bool) {
	sightingsMu.Lock()
	defer sightingsMu.Unlock()
	if e.sightings == nil {
		e.sightings = map[client.ObjectKey]sighting{}
	}
	key, last := client.ObjectKeyFromObject(etcd), etcd.Status.LastOperation
	if last == nil || last.State != druidv1alpha1.LastOperationStateProcessing {
		delete(e.sightings, key)
		return time.Time{}, false
	}
	if s, ok := e.sightings[key]; ok && s.run == last.RunID {
		return s.at, true
	}
	e.sightings[key] = sighting{run: last.RunID, at: clk.Now()}
	return e.sightings[key].at, true
}

// Forget drops what this operator saw of the reconciles of the etcd of the
// ControlPlane cp names.
func (e *Engine) Forget(cp types.NamespacedName) {
	sightingsMu.Lock()
	defer sightingsMu.Unlock()
	delete(e.sightings, client.ObjectKey{Namespace: cp.Namespace, Name: contract.EtcdName(cp.Name)})
}

func cutLast(s, sep string) (string, string, bool) {
	i := strings.LastIndex(s, sep)
	if i < 0 {
		return s, "", false
	}
	return s[:i], s[i+len(sep):], true
}

// druidPending says why etcd-druid's Deployment does not yet run the version
// the operator was given in every replica, as kubectl rollout status counts
// them (kubernetes/kubernetes@v1.37.1 staging/src/k8s.io/kubectl/pkg/polymorphichelpers/rollout_status.go#L75-L91),
// with no terminating pod left, which kubectl does not count and which
// reconciles until it stops; empty once it does. It is read from live: the
// cache holds only what Lodia labels.
func (e *Engine) druidPending(ctx context.Context, live client.Reader) (string, error) {
	key := client.ObjectKey{Namespace: e.Namespace, Name: e.Deployment}
	d := &appsv1.Deployment{}
	if err := live.Get(ctx, key, d); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("Deployment %s exists", key), nil
		}
		return "", fmt.Errorf("read Deployment %s: %w", key, err)
	}
	if !slices.ContainsFunc(d.Spec.Template.Spec.Containers, func(c corev1.Container) bool { return strings.HasSuffix(c.Image, ":"+e.Version) }) {
		return fmt.Sprintf("Deployment %s runs etcd-druid %s", key, e.Version), nil
	}
	s := d.Status
	if s.ObservedGeneration < d.Generation || s.UpdatedReplicas < ptr.Deref(d.Spec.Replicas, 1) || s.Replicas > s.UpdatedReplicas || s.AvailableReplicas < s.UpdatedReplicas {
		return fmt.Sprintf("Deployment %s has rolled out etcd-druid %s to every replica", key, e.Version), nil
	}
	if ptr.Deref(s.TerminatingReplicas, 0) > 0 {
		return fmt.Sprintf("no pod of Deployment %s terminates", key), nil
	}
	return "", nil
}
