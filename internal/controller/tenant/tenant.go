// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package tenant is the Tenant controller.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/contract"
	"go.lodia.io/lodia/internal/live"
	"go.lodia.io/lodia/internal/ownership"
	"go.lodia.io/lodia/internal/statusapply"
	"go.lodia.io/lodia/internal/tenantclient"
)

const (
	Name         = "tenant"
	FieldManager = "lodia/" + Name
)

// waitPeriod is how often a pass runs while it waits on what no event
// reports: the addons' pods on the nodes, and a Secret that is missing or that
// this ControlPlane does not control.
const waitPeriod = time.Minute

// deliveryTimeout and applyTimeout bound each request of a tenant client, the
// delivery's and the apply pass's, and deliveryTimeout the dial and the
// handshake of every request; Delivery's is short of the 30 s dial of a tenant
// that drops packets. A pass sends its requests one after another and ends at
// its minute, ReconciliationTimeout, and statusapply gives the status write 10 s
// of its own after it.
// A pass completes while the tenant answers each request in under 60 s over
// the requests it sends. At rest those are 39 for the defaults where the pods
// dial an IP host or a node port (the inventory's read, 35 applies, 3 reads of
// workloads), 1.54 s each, 42 where apiserver-proxy runs without the tunnel (1,
// 37 and 4), 1.43 s, and 45 for a gateway tenant (the delivery's 2, then 1, 38
// and 4), 1.33 s. A fresh tenant's first pass writes the inventory with its
// request 37, 39 or 41, and its second is a pass at rest, so it converges in
// two passes within the same figures.
const (
	deliveryTimeout = 10 * time.Second
	applyTimeout    = 30 * time.Second
)

type Reconciler struct {
	client.Client
	// Reader reads AddonsApplied and AddonsAvailable when Tenant waits for
	// access or keeps them, from the API server, and a Secret again when its
	// copy makes a pass wait.
	Reader client.Reader
	// Secrets reads the Secrets the cache holds the metadata of only.
	Secrets client.Reader
	// Events records the transitions of the conditions it writes;
	// SetupWithManager sets it.
	Events events.EventRecorder
	// ApplyPeriod is how often the tenant gets every object again, so one
	// deleted or edited there comes back.
	ApplyPeriod time.Duration
	// TunnelImage is lodia-tunnel's, which apiserver-proxy runs.
	TunnelImage string
	// Revision is the running operator revision, which Tenant renders with
	// once Lifecycle grants it in status.target.operatorRevision.
	Revision string
	// Dial, when set, opens every connection to a tenant's API server; tests
	// set it, since envtest has no DNS for the tenant's Service.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)

	mu      sync.Mutex
	clients *tenantclient.Clients
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	r.Events = mgr.GetEventRecorder("lodia.io/" + Name)
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.ControlPlane{}, builder.WithPredicates(inputsChanged())).
		Complete(r)
}

// The other controllers' conditions change often; a pass runs only for what
// the render or the wait reads.
func inputsChanged() predicate.Funcs {
	return predicate.Funcs{UpdateFunc: func(e event.UpdateEvent) bool {
		before, ok := e.ObjectOld.(*lodiav1alpha1.ControlPlane)
		after, ok2 := e.ObjectNew.(*lodiav1alpha1.ControlPlane)
		return !ok || !ok2 || inputs(before) != inputs(after)
	}}
}

func inputs(cp *lodiav1alpha1.ControlPlane) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d %t", cp.Generation, cp.DeletionTimestamp.IsZero())
	if t := cp.Status.Target; t != nil {
		fmt.Fprintf(&b, " target=%s %s", t.Version, t.OperatorRevision)
	}
	fmt.Fprintf(&b, " components=%q", reported(cp))
	if e := cp.Status.Endpoint; e != nil {
		fmt.Fprintf(&b, " %s:%d %s:%d %s:%d", e.APIServer.Host, e.APIServer.Port, e.Konnectivity.Host, e.Konnectivity.Port, e.InCluster.Host, e.InCluster.Port)
		if e.Tunnel != nil {
			fmt.Fprintf(&b, " %s:%d", e.Tunnel.Host, e.Tunnel.Port)
		}
	}
	if c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAccessReady); c != nil {
		fmt.Fprintf(&b, " %s/%d", c.Status, c.ObservedGeneration)
	}
	return b.String()
}

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		if apierrors.IsNotFound(err) {
			r.forget(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !cp.DeletionTimestamp.IsZero() || cp.Spec.Paused {
		return ctrl.Result{}, nil
	}
	if cp.Spec.Endpoint.Gateway != nil {
		r.deliverTunnelClient(ctx, cp)
	}
	if err := live.Addons(ctx, r.Reader, cp, lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ConditionAddonsAvailable); err != nil {
		return ctrl.Result{}, err
	}
	applied, available := previous(cp, lodiav1alpha1.ConditionAddonsApplied), previous(cp, lodiav1alpha1.ConditionAddonsAvailable)
	if meta.FindStatusCondition(cp.Status.Conditions, applied.Type) == nil || meta.FindStatusCondition(cp.Status.Conditions, available.Type) == nil {
		if err := r.applyStatus(ctx, cp, cp.Status.Addons, applied, available); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !statusapply.TrueAt(cp, lodiav1alpha1.ConditionAccessReady) {
		if err := r.wait(ctx, cp, lodiav1alpha1.ReasonWaitingForAccess, "AccessReady is not True for this generation"); err != nil {
			return ctrl.Result{}, err
		}
		if cp.Spec.Endpoint.Gateway != nil {
			return ctrl.Result{RequeueAfter: r.ApplyPeriod}, nil
		}
		return ctrl.Result{}, nil
	}
	clusterCA, kubeconfig, tunnelClient, unusable, err := r.credentials(ctx, cp)
	if err != nil {
		return ctrl.Result{}, err
	}
	if unusable != "" {
		if err := r.wait(ctx, cp, lodiav1alpha1.ReasonWaitingForAccess, unusable); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: min(waitPeriod, r.ApplyPeriod)}, nil
	}
	if reason, message := waitsFor(cp); message != "" {
		return ctrl.Result{}, r.wait(ctx, cp, reason, message)
	}
	held := cp.Status.Target.OperatorRevision != r.Revision
	v, outside, err := newValues(cp, clusterCA)
	if err != nil {
		return ctrl.Result{}, err
	}
	if outside != "" && !held {
		return ctrl.Result{}, r.wait(ctx, cp, lodiav1alpha1.ReasonWaitingForTarget, outside)
	}
	var objects []*unstructured.Unstructured
	if outside == "" {
		v.TunnelImage, v.tunnelCert, v.tunnelKey = r.TunnelImage, tunnelClient.Data[corev1.TLSCertKey], tunnelClient.Data[corev1.TLSPrivateKeyKey]
		if objects, err = render(v); err != nil {
			return ctrl.Result{}, err
		}
	}
	tenant, err := r.reach(client.ObjectKeyFromObject(cp), kubeconfig, applyTimeout)
	if err != nil {
		return ctrl.Result{}, err
	}
	if held {
		return r.hold(ctx, cp, tenant, objects, applied, available)
	}

	others, refused, applyErr := applyInventory(ctx, tenant, objects)
	var rolled metav1.Condition
	var rolloutErr error
	_, _, answered := outcome(applyErr)
	read := applyErr == nil || answered
	if read {
		rolled, rolloutErr = rollout(ctx, tenant, cp, entriesOf(objects), others)
	}
	syncErr := errors.Join(joined(refused), applyErr, rolloutErr)
	switch reason, message, answered := outcome(applyErr); {
	case len(refused) > 0:
		var readErr error
		applied, readErr = refusing(ctx, tenant, applied, refusal(cp, applied.Type, refused, others, nil), refused)
		syncErr = errors.Join(syncErr, readErr)
	case applyErr == nil && len(others) > 0:
		applied = standing(applied, refusal(cp, applied.Type, nil, others, nil))
	case applyErr == nil:
		applied = newCondition(cp, applied.Type, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, fmt.Sprintf("%d objects are applied and the inventory is pruned", len(objects)))
	case answered || applied.Status != metav1.ConditionTrue && !slices.Contains([]string{lodiav1alpha1.ReasonRefused, lodiav1alpha1.ReasonObjectsControlledElsewhere}, applied.Reason):
		// No answer is no observation: a True, a refused object and another
		// owner's stay, since the object does; anything else says so.
		applied = newCondition(cp, applied.Type, metav1.ConditionFalse, reason, message)
	}
	addons := cp.Status.Addons
	if applyErr == nil && len(refused) == 0 && len(others) == 0 {
		addons = &lodiav1alpha1.Rendered{Version: v.Version, OperatorRevision: r.Revision}
	}
	switch {
	case read && rolloutErr == nil:
		available = rolled
	case available.Reason != lodiav1alpha1.ReasonAvailable && available.Reason != lodiav1alpha1.ReasonRolloutPending:
		// A rollout read before stays while none can be read; until one is,
		// what stopped the read says why.
		stopped := applyErr
		if read {
			stopped = rolloutErr
		}
		reason, message, _ := outcome(stopped)
		available = newCondition(cp, available.Type, metav1.ConditionFalse, reason, message)
	}
	if err := r.applyStatus(ctx, cp, addons, applied, available); err != nil || syncErr != nil {
		return ctrl.Result{}, errors.Join(syncErr, err)
	}
	return r.next(available), nil
}

// hold is a pass while Lifecycle has not granted the running revision: it
// creates what the stored inventory lists and the tenant lacks, with an apply
// since appliedByLodia counts only that, writes nothing else, and reports what
// runs, each condition at the generation it describes: the refusals it meets,
// a creation or another owner's object, beside the rollout, which it returns.
// While AddonsApplied holds a refusal or is not True it also sends every write
// of the grant's first pass, the inventory's among them, by dry run, and
// reports what that finds, True while no object it lists is missing and the
// condition was True or the inventory is whole, or, with every write accepted
// over a refusal, that the grant waits; a dry run or a read that gets no answer
// changes nothing. objects is nil when the running operator cannot render.
func (r *Reconciler) hold(ctx context.Context, cp *lodiav1alpha1.ControlPlane, tenant tenantAPI, objects []*unstructured.Unstructured, applied, available metav1.Condition) (ctrl.Result, error) {
	listed, missing, others, inventoried, err := heldInventory(ctx, tenant, objects)
	if err != nil {
		return ctrl.Result{}, err
	}
	var failures []*failure
	for _, obj := range missing {
		err := apply(ctx, tenant, obj)
		if f, ok := errors.AsType[*failure](err); ok && f.reason == lodiav1alpha1.ReasonRefused {
			failures = append(failures, f)
		} else if err != nil {
			return ctrl.Result{}, errors.Join(joined(failures), err)
		}
	}
	refusing := slices.Contains(contract.Refusals(), applied.Reason)
	trying := objects != nil && (refusing || applied.Status != metav1.ConditionTrue)
	var tried []*failure
	if trying {
		_, refused, err := applyInventory(ctx, tenant.dryRun(), objects)
		if f, ok := errors.AsType[*failure](err); ok && f.reason == lodiav1alpha1.ReasonRefused {
			refused, err = append(refused, f), nil
		}
		if err != nil {
			return ctrl.Result{}, errors.Join(joined(failures), err)
		}
		tried = refused
	}
	whole := inventoried && cp.Status.Addons != nil && cp.Status.Addons.Version != ""
	switch {
	case len(failures) > 0 || len(others) > 0 || len(tried) > 0:
		met := at(refusal(cp, applied.Type, failures, others, tried), applied.ObservedGeneration)
		if len(failures) == 0 && (applied.Status == metav1.ConditionTrue || whole) {
			met.Status = metav1.ConditionTrue
		}
		applied = met
	// A wait over a healthy render, as an API server restart leaves, or a
	// refusal the dry run no longer finds, would otherwise outlast it until
	// the grant.
	case whole && (refusing || applied.Status == metav1.ConditionFalse && strings.HasPrefix(applied.Reason, "WaitingFor")):
		applied = at(newCondition(cp, applied.Type, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied,
			fmt.Sprintf("the %d objects the inventory lists are applied; waiting for Lifecycle to grant the running operator revision %s", len(listed), r.Revision)), applied.ObservedGeneration)
	case trying && refusing:
		accepted := "the tenant accepts by dry run every write of the running operator revision " + r.Revision + "; waiting for Lifecycle to grant it"
		if applied.Status == metav1.ConditionTrue {
			applied = at(newCondition(cp, applied.Type, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, accepted), applied.ObservedGeneration)
		} else {
			applied = at(newCondition(cp, applied.Type, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, accepted), applied.ObservedGeneration)
		}
	}
	refused := errors.Join(joined(failures), joined(tried))
	if slices.ContainsFunc(listed, isWorkload) {
		rolled, err := rollout(ctx, tenant, cp, listed, nil)
		switch {
		case err == nil:
			available = at(rolled, available.ObservedGeneration)
		case refused == nil:
			return ctrl.Result{}, err
		}
		refused = errors.Join(refused, err)
	}
	if err := r.applyStatus(ctx, cp, cp.Status.Addons, applied, available); err != nil || refused != nil {
		return ctrl.Result{}, errors.Join(refused, err)
	}
	return r.next(available), nil
}

// next looks again every minute while the addons are not rolled out.
func (r *Reconciler) next(available metav1.Condition) ctrl.Result {
	if available.Status != metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: min(waitPeriod, r.ApplyPeriod)}
	}
	return ctrl.Result{RequeueAfter: r.ApplyPeriod}
}

func at(c metav1.Condition, generation int64) metav1.Condition {
	c.ObservedGeneration = generation
	return c
}

// standing is refused while what it names exists: True under its reason, at
// the generation previous describes, while previous is True, and refused
// otherwise.
func standing(previous, refused metav1.Condition) metav1.Condition {
	if previous.Status == metav1.ConditionTrue {
		refused.Status = metav1.ConditionTrue
		return at(refused, previous.ObservedGeneration)
	}
	return refused
}

// refusing is AddonsApplied while the refusals of failures stand, which
// refused names: standing while the tenant holds every object refused, which
// a refused creation leaves missing, refused while it lacks one or refuses its
// read, and previous while a read gets no answer, which tells nothing.
func refusing(ctx context.Context, a tenantAPI, previous, refused metav1.Condition, failures []*failure) (metav1.Condition, error) {
	for _, f := range failures {
		existing, err := readEntry(ctx, a, f.entry)
		switch _, _, answered := outcome(err); {
		case err != nil && !answered:
			return previous, err
		case existing == nil:
			return refused, err
		}
	}
	return standing(previous, refused), nil
}

// maxRefused is how many refused objects, how many of another owner and how
// many writes of the grant a message names: the prunes of an inventory the
// tenant's administrator wrote, up to maxEntries of them, would pass a
// condition's message bound.
const maxRefused = 10

// refusal is the condition of conditionType that names the refusals of
// failures, the objects of another owner and the grant's writes whose dry run
// tried refuses, the first maxRefused of each and the count of the rest, under
// the reason of the first refusal: an object refused, another owner's, then a
// dry run's, so that one the pass meets itself keeps its own.
func refusal(cp *lodiav1alpha1.ControlPlane, conditionType string, failures []*failure, others []foreignObject, tried []*failure) metav1.Condition {
	var parts []string
	if len(failures) > 0 {
		named := make([]string, 0, maxRefused+1)
		for _, f := range failures[:min(len(failures), maxRefused)] {
			named = append(named, f.entry.String()+" "+f.verdict)
		}
		if more := len(failures) - maxRefused; more > 0 {
			named = append(named, fmt.Sprintf("%d more objects are refused", more))
		}
		parts = append(parts, strings.Join(named, "; ")+"; the reason is in Lodia's log")
	}
	if len(others) > 0 {
		named := make([]string, 0, maxRefused+1)
		for _, o := range others[:min(len(others), maxRefused)] {
			named = append(named, o.String()+" exists and is not Lodia's: "+o.owners())
		}
		ending := "; delete it or disable the addon"
		if len(others) > 1 {
			ending = "; delete them or disable their addons"
		}
		if more := len(others) - maxRefused; more > 0 {
			named = append(named, fmt.Sprintf("%d more objects are another owner's", more))
		}
		parts = append(parts, strings.Join(named, "; ")+ending)
	}
	if len(tried) > 0 {
		named := make([]string, 0, maxRefused+1)
		for _, f := range tried[:min(len(tried), maxRefused)] {
			named = append(named, "the grant's write of "+f.entry.String()+" would be refused: the dry run "+f.verdict)
		}
		if more := len(tried) - maxRefused; more > 0 {
			named = append(named, fmt.Sprintf("%d more writes of the grant would be refused", more))
		}
		parts = append(parts, strings.Join(named, "; ")+"; the reason is in Lodia's log")
	}
	reason := lodiav1alpha1.ReasonRefused
	if len(failures) == 0 && len(others) > 0 {
		reason = lodiav1alpha1.ReasonObjectsControlledElsewhere
	}
	return newCondition(cp, conditionType, metav1.ConditionFalse, reason, strings.Join(parts, "; "))
}

func joined(failures []*failure) error {
	errs := make([]error, 0, len(failures))
	for _, f := range failures {
		errs = append(errs, f)
	}
	return errors.Join(errs...)
}

// outcome is the reason and the message a pass that stopped on err reports,
// and whether the tenant answered.
func outcome(err error) (string, string, bool) {
	if f, ok := errors.AsType[*failure](err); ok && f.verdict != "" {
		return f.reason, f.entry.String() + " " + f.verdict + "; the reason is in Lodia's log", true
	}
	return lodiav1alpha1.ReasonApplying, "the tenant's API server does not answer; the error is in Lodia's log", false
}

// wait says what both conditions wait for, but leaves one at the earlier
// generation it describes: Tenant has not acted on this one, and a held pass
// writes True over a wait at the generation the wait names.
func (r *Reconciler) wait(ctx context.Context, cp *lodiav1alpha1.ControlPlane, reason, message string) error {
	conditions := make([]metav1.Condition, 0, 2)
	for _, conditionType := range []string{lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ConditionAddonsAvailable} {
		c := previous(cp, conditionType)
		conditions = append(conditions, statusapply.Wait(cp, &c, waiting(c, newCondition(cp, conditionType, metav1.ConditionFalse, reason, message))))
	}
	return r.applyStatus(ctx, cp, cp.Status.Addons, conditions...)
}

// waiting is what a wait reports instead of c. An AddonsApplied that names a
// refused object, another owner's or an unfinished apply stays: a lost access
// undoes no observation, and a held pass writes True over a wait.
func waiting(c, wait metav1.Condition) metav1.Condition {
	if c.Type == lodiav1alpha1.ConditionAddonsApplied && slices.Contains([]string{lodiav1alpha1.ReasonRefused, lodiav1alpha1.ReasonObjectsControlledElsewhere, lodiav1alpha1.ReasonApplying}, c.Reason) {
		return c
	}
	return wait
}

type secretRead struct {
	secret lodiav1alpha1.Secret
	obj    *corev1.Secret
}

// credentials also returns, when one of the Secrets cannot be used, what to
// report about it; its content is not used.
func (r *Reconciler) credentials(ctx context.Context, cp *lodiav1alpha1.ControlPlane) ([]byte, []byte, *corev1.Secret, string, error) {
	bundle, kubeconfig, tunnelClient := &corev1.Secret{}, &corev1.Secret{}, &corev1.Secret{}
	reads := []secretRead{{lodiav1alpha1.CABundleSecret, bundle}, {lodiav1alpha1.LodiaKubeconfigSecret, kubeconfig}}
	if cp.Spec.Endpoint.Gateway != nil {
		reads = append(reads, secretRead{lodiav1alpha1.TunnelClientSecret, tunnelClient})
	}
	unusable, err := r.readSecrets(ctx, cp, reads)
	if err != nil || unusable != "" {
		return nil, nil, nil, unusable, err
	}
	return bundle.Data[lodiav1alpha1.CACertKey], kubeconfig.Data[lodiav1alpha1.KubeconfigKey], tunnelClient, "", nil
}

// readSecrets stops at the first Secret that is missing, that another owner
// keeps or that this ControlPlane does not control, and returns what to report
// about it. AccessReady does not cover the last: PKI grants access while it
// holds a Secret it has not adopted, and a restore adopts none. A copy that
// shows one of the last two is read again from the API server, and the pass
// decides on that: a wait is never decided on a copy.
func (r *Reconciler) readSecrets(ctx context.Context, cp *lodiav1alpha1.ControlPlane, reads []secretRead) (string, error) {
	for _, read := range reads {
		obj, name := read.obj, read.secret.Name(cp.Name)
		key := client.ObjectKey{Namespace: cp.Namespace, Name: name}
		err := r.Secrets.Get(ctx, key, obj)
		if err == nil && keptApart(obj, cp) != "" {
			*obj = corev1.Secret{}
			err = r.Reader.Get(ctx, key, obj)
		}
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("Secret %s does not exist", name), nil
		}
		if err != nil {
			return "", err
		}
		if message := keptApart(obj, cp); message != "" {
			return message, nil
		}
	}
	return "", nil
}

func keptApart(obj *corev1.Secret, cp *lodiav1alpha1.ControlPlane) string {
	if ref := ownership.ControlledElsewhere(obj, cp); ref != nil {
		return fmt.Sprintf("Secret %s is kept by %s %s with UID %s", obj.Name, ref.Kind, ref.Name, ref.UID)
	}
	if !metav1.IsControlledBy(obj, cp) {
		return fmt.Sprintf("Secret %s is not controlled by this ControlPlane", obj.Name)
	}
	return ""
}

// deliverTunnelClient brings a renewed pair to the tenant before any condition
// is read: none proves the API server cannot take it, and an expired pair takes
// every pod's API. Failures stop nothing, and neither a Secret that does not
// exist yet, the inventory's to write, nor another owner's gets a write.
func (r *Reconciler) deliverTunnelClient(ctx context.Context, cp *lodiav1alpha1.ControlPlane) {
	kubeconfig, pair := &corev1.Secret{}, &corev1.Secret{}
	unusable, err := r.readSecrets(ctx, cp, []secretRead{{lodiav1alpha1.LodiaKubeconfigSecret, kubeconfig}, {lodiav1alpha1.TunnelClientSecret, pair}})
	if unusable != "" {
		return
	}
	if err == nil {
		err = r.applyTunnelClient(ctx, client.ObjectKeyFromObject(cp), kubeconfig.Data[lodiav1alpha1.KubeconfigKey], pair)
	}
	if err != nil {
		logf.FromContext(ctx).Error(err, "deliver the tunnel's client certificate")
	}
}

func (r *Reconciler) applyTunnelClient(ctx context.Context, key types.NamespacedName, kubeconfig []byte, pair *corev1.Secret) error {
	tenant, err := r.reach(key, kubeconfig, deliveryTimeout)
	if err != nil {
		return err
	}
	e := entry{Version: "v1", Kind: "Secret", Namespace: metav1.NamespaceSystem, Name: apiserverProxyClientSecret}
	existing, err := tenant.readObject(ctx, e)
	if err != nil || existing == nil || !existing.applied {
		return err
	}
	body, err := apiserverProxyClient(pair.Data[corev1.TLSCertKey], pair.Data[corev1.TLSPrivateKeyKey]).MarshalJSON()
	if err != nil {
		return err
	}
	return tenant.apply(ctx, e, body)
}

// reach returns the client of the ControlPlane under key with each request
// within timeout; a pass sends its requests one after another.
func (r *Reconciler) reach(key types.NamespacedName, kubeconfig []byte, timeout time.Duration) (tenantAPI, error) {
	r.mu.Lock()
	if r.clients == nil {
		r.clients = tenantclient.NewClients(tenantclient.Options{Connections: 1, Timeout: deliveryTimeout, Dial: r.Dial})
	}
	clients := r.clients
	r.mu.Unlock()
	c, err := clients.Of(key, kubeconfig)
	return tenantAPI{client: c, timeout: timeout}, err
}

func (r *Reconciler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.clients != nil {
		r.clients.Forget(key)
	}
}

func isWorkload(e entry) bool {
	return e.Group == "apps" && (e.Kind == "DaemonSet" || e.Kind == "Deployment")
}

// rollout reads the DaemonSets and Deployments among entries but others. One
// that is missing is pending: a held pass leaves a listed one it no longer
// renders as it is.
func rollout(ctx context.Context, a tenantAPI, cp *lodiav1alpha1.ControlPlane, entries []entry, others []foreignObject) (metav1.Condition, error) {
	var names, pending []string
	for _, e := range entries {
		if !isWorkload(e) || isForeign(others, e) {
			continue
		}
		names = append(names, e.Name)
		body, err := a.read(ctx, e)
		if err == nil && body != nil {
			var w *workload
			if w, err = tenantclient.Decode(body, nil, workloadOf(e.Kind)); err == nil {
				if want, available, done := rolledOut(e.Kind, w); !done {
					pending = append(pending, fmt.Sprintf("%s %d of %d available", e.Name, available, want))
				}
			}
		}
		switch {
		case err != nil:
			return metav1.Condition{}, failed(e, err)
		case body == nil:
			pending = append(pending, e.Name+" is missing")
		}
	}
	if len(pending) > 0 {
		slices.Sort(pending)
		return newCondition(cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending, strings.Join(pending, ", ")), nil
	}
	slices.Sort(names)
	return newCondition(cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable, strings.Join(names, ", ")+" are rolled out"), nil
}

func rolledOut(kind string, w *workload) (int32, int32, bool) {
	if kind == "DaemonSet" {
		return w.desiredNumberScheduled, w.available, w.observedGeneration >= w.generation && w.updated == w.desiredNumberScheduled && w.available == w.desiredNumberScheduled
	}
	want := int32(1)
	if w.replicas != nil {
		want = *w.replicas
	}
	return want, w.available, w.observedGeneration >= w.generation && w.updated == want && w.available == want
}

func previous(cp *lodiav1alpha1.ControlPlane, conditionType string) metav1.Condition {
	if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c != nil {
		return *c
	}
	return statusapply.FirstPass(cp, conditionType)
}

func newCondition(cp *lodiav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: message, ObservedGeneration: cp.Generation}
}

// applyStatus writes only on a change, and everything Tenant owns every time:
// both conditions and addons, since an apply that leaves one out removes it.
// A path that changes no addons passes cp's, which Reconcile read live.
func (r *Reconciler) applyStatus(ctx context.Context, cp *lodiav1alpha1.ControlPlane, addons *lodiav1alpha1.Rendered, conditions ...metav1.Condition) error {
	return statusapply.Writer{Client: r.Client, Reader: r.Reader, Manager: FieldManager, Events: r.Events}.ApplyInto(ctx, cp, lodiav1alpha1.ControlPlaneStatus{Conditions: conditions, Addons: addons}, !equality.Semantic.DeepEqual(cp.Status.Addons, addons))
}
