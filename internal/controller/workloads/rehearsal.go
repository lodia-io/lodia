// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/fence"
	"go.lodia.io/lodia/internal/live"
	"go.lodia.io/lodia/internal/ownership"
	"go.lodia.io/lodia/internal/statusapply"
	"go.lodia.io/lodia/internal/tenantclient"
	"go.lodia.io/lodia/internal/versions"
)

const (
	RehearsalName         = "rehearsal"
	RehearsalFieldManager = "lodia/" + RehearsalName

	// readyWithin is how long after the copy's creation its API server may
	// run without being ready.
	readyWithin = 5 * time.Minute
	// rehearseAgain is how often a failed verdict runs again at the same
	// generation.
	rehearseAgain = 10 * time.Minute
	// placeAgain is how often a rehearsal looks for a free place.
	placeAgain = 30 * time.Second
	// maxQuoted bounds what a verdict quotes of a container or of an answer.
	maxQuoted = 1024
)

// RehearsalReconciler rehearses a minor step on a copy of the API server
// (docs/decisions/rehearse-a-minor-step.md). It is Workloads' second
// reconciler, with a queue of its own, and writes Rehearsed and
// status.rehearsal alone.
type RehearsalReconciler struct {
	client.Client
	// Reader reads Rehearsed and status.rehearsal as the API server holds
	// them, the copies while a create under the fence has not shown in the
	// cache, and the objects under the copy's names the cache lacks.
	Reader client.Reader
	// Secrets reads <cp>-lodia-kubeconfig, whose data the cache does not hold.
	Secrets client.Reader
	// Events records the transitions of Rehearsed; SetupWithManager sets it.
	Events events.EventRecorder
	Clock  clock.PassiveClock
	// SignerImage is lodia-signer's image, the copy's sidecar as the API
	// server's.
	SignerImage string
	// OperatorNamespace is the namespace the copy's NetworkPolicy admits.
	OperatorNamespace string
	// MaxConcurrent is fleet.maxConcurrentRehearsals: how many copies run at
	// once, 0 for none.
	MaxConcurrent int
	// Timeout bounds a dial and a TLS handshake to a copy or a tenant.
	Timeout time.Duration
	// Dial, when set, opens every connection to a copy and a tenant; tests
	// set it.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)

	places fence.Fence

	mu      sync.Mutex
	tenants *tenantclient.Clients
	sweeps  map[types.UID]*sweep
	// verdicts is when this operator last concluded a verdict for a
	// ControlPlane, or first saw its failed one.
	verdicts map[types.UID]time.Time
}

func (r *RehearsalReconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	r.Events = mgr.GetEventRecorder("lodia.io/" + RehearsalName)
	return ctrl.NewControllerManagedBy(mgr).
		Named(RehearsalName).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.ControlPlane{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch
// +kubebuilder:rbac:groups=lodia.io,resources=datastoreclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile runs a copy while status.kubernetes.upgrade.step is Rehearsal and
// Rehearsed has no verdict for the step's version at the object's
// generation, or a False one due to run again, and removes any copy otherwise.
// A paused ControlPlane starts no copy, and one already running ends at its
// deadline.
func (r *RehearsalReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
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
	pod, err := r.copyOf(ctx, cp)
	if err != nil {
		return ctrl.Result{}, err
	}
	running := types.UID("")
	if pod != nil {
		running = pod.UID
	}
	r.endSweeps(cp.UID, running)
	version, wanted := r.rehearsing(cp)
	if !wanted {
		return ctrl.Result{}, r.remove(ctx, cp, pod)
	}
	if err := live.Rehearsal(ctx, r.Reader, cp, lodiav1alpha1.ConditionRehearsed); err != nil {
		return ctrl.Result{}, err
	}
	verdict := verdictOf(cp, version)
	if verdict != nil && verdict.Status == metav1.ConditionTrue {
		return ctrl.Result{}, r.remove(ctx, cp, pod)
	}
	if pod != nil {
		if pod.Annotations[versionAnnotation] != version || pod.Annotations[generationAnnotation] != strconv.FormatInt(cp.Generation, 10) {
			return ctrl.Result{RequeueAfter: copyGrace}, r.deleteCopy(ctx, pod)
		}
		return r.judge(ctx, cp, version, verdict, pod)
	}
	if verdict != nil && !slices.Contains(soonerAgain, verdict.Reason) {
		if wait := r.againAt(cp.UID, verdict).Sub(r.Clock.Now()); wait > 0 {
			return ctrl.Result{RequeueAfter: wait}, nil
		}
	}
	return r.start(ctx, cp, version, verdict)
}

// rehearsing is the version of the minor step Lifecycle rehearses, while it
// does.
func (r *RehearsalReconciler) rehearsing(cp *lodiav1alpha1.ControlPlane) (string, bool) {
	k := cp.Status.Kubernetes
	if r.MaxConcurrent == 0 || k == nil || k.Upgrade == nil || k.Upgrade.Step != lodiav1alpha1.UpgradeStepRehearsal || cp.Status.Target == nil {
		return "", false
	}
	version, _, inTable := versions.Step(cp.Spec.Version, cp.Status.Target.Version)
	return version, inTable
}

// verdictOf is Rehearsed when it is True or False for version at cp's
// generation, and nil otherwise.
func verdictOf(cp *lodiav1alpha1.ControlPlane, version string) *metav1.Condition {
	c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionRehearsed)
	if c == nil || c.Status == metav1.ConditionUnknown || c.ObservedGeneration != cp.Generation || cp.Status.Rehearsal == nil || cp.Status.Rehearsal.Version != version {
		return nil
	}
	return c
}

// copyOf is cp's copy that is not being deleted, from the cache: one being
// deleted, as on a node that is gone, stands in the way of nothing. A copy
// the cache lacks is found by place under the fence.
func (r *RehearsalReconciler) copyOf(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (*corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := r.List(ctx, list, client.InNamespace(cp.Namespace), client.MatchingLabels(podLabels(cp.Name, lodiav1alpha1.RehearsalComponent))); err != nil {
		return nil, err
	}
	for i := range list.Items {
		if pod := &list.Items[i]; pod.DeletionTimestamp.IsZero() && metav1.IsControlledBy(pod, cp) {
			return pod, nil
		}
	}
	return nil, nil
}

// start creates the copy's NetworkPolicy and, while fewer than MaxConcurrent
// copies run, its Pod, and says so unless a failed verdict stands for the
// step, which stays while the rehearsal runs again.
func (r *RehearsalReconciler) start(ctx context.Context, cp *lodiav1alpha1.ControlPlane, version string, verdict *metav1.Condition) (ctrl.Result, error) {
	if cp.Status.Endpoint == nil || cp.Status.Datastore == nil || len(cp.Status.Datastore.Endpoints) == 0 {
		return ctrl.Result{RequeueAfter: placeAgain}, nil
	}
	images, ok := versions.For(version)
	if !ok {
		return ctrl.Result{}, nil
	}
	quota, etcdResources, err := r.class(ctx, cp)
	if err != nil {
		return ctrl.Result{}, err
	}
	rendered := rendering{cp: cp, images: images, signerImage: r.SignerImage, endpoint: cp.Status.Endpoint, etcd: cp.Status.Datastore.Endpoints}
	pod := rendered.rehearsalCopy(version, cp.Generation, quota, etcdResources)
	policy := copyPolicy(cp, r.OperatorNamespace)
	if held, err := r.policyHeld(ctx, cp, policy); err != nil || held != "" {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Minute}, r.report(ctx, cp, version, metav1.ConditionFalse, lodiav1alpha1.ReasonObjectsControlledElsewhere, held)
	}
	if err := r.apply(ctx, policy); err != nil {
		return ctrl.Result{}, r.refused(ctx, cp, version, "NetworkPolicy", policy.Name, err)
	}
	running, err := r.place(ctx, cp, pod)
	switch {
	case errors.Is(err, errCopied) || apierrors.IsAlreadyExists(err):
		return ctrl.Result{RequeueAfter: copyGrace}, nil
	case err != nil:
		return ctrl.Result{}, r.refused(ctx, cp, version, "Pod", pod.Name, err)
	case running >= r.MaxConcurrent:
		if verdict != nil {
			return ctrl.Result{RequeueAfter: placeAgain}, nil
		}
		message := fmt.Sprintf("fleet.maxConcurrentRehearsals, %d, copies run; the step to %s waits for a place", running, version)
		return ctrl.Result{RequeueAfter: placeAgain}, r.report(ctx, cp, version, metav1.ConditionUnknown, lodiav1alpha1.ReasonRehearsalLimit, message)
	}
	return ctrl.Result{}, r.rehearsingNow(ctx, cp, version, verdict)
}

func (r *RehearsalReconciler) rehearsingNow(ctx context.Context, cp *lodiav1alpha1.ControlPlane, version string, verdict *metav1.Condition) error {
	if verdict != nil {
		return nil
	}
	return r.report(ctx, cp, version, metav1.ConditionUnknown, lodiav1alpha1.ReasonRehearsing,
		fmt.Sprintf("a copy of the API server at %s runs on a fresh snapshot of the etcd", version))
}

// errCopied is place's answer when cp has a copy the cache did not show.
var errCopied = errors.New("the ControlPlane has a copy")

// place creates pod unless cp has a copy or MaxConcurrent copies run, counted
// under the fence without those being deleted, and returns how many ran before
// it.
func (r *RehearsalReconciler) place(ctx context.Context, cp *lodiav1alpha1.ControlPlane, pod *corev1.Pod) (int, error) {
	if err := r.places.Acquire(ctx); err != nil {
		return 0, err
	}
	defer r.places.Release()
	copies := r.Reader
	if r.places.Cached() {
		copies = r.Client
	}
	list := &corev1.PodList{}
	if err := copies.List(ctx, list, client.MatchingLabels{lodiav1alpha1.ComponentLabel: lodiav1alpha1.RehearsalComponent}, client.HasLabels{lodiav1alpha1.ControlPlaneLabel}); err != nil {
		return 0, err
	}
	running := 0
	for i := range list.Items {
		switch copied := &list.Items[i]; {
		case !copied.DeletionTimestamp.IsZero():
		case metav1.IsControlledBy(copied, cp):
			return 0, errCopied
		default:
			running++
		}
	}
	if running >= r.MaxConcurrent {
		return running, nil
	}
	err := r.Create(ctx, pod, client.FieldOwner(RehearsalFieldManager))
	return running, r.places.Wrote(ctx, r.Client, pod, err)
}

// class is the quota and the member resources of the DatastoreClass the etcd
// runs, the default quota while it names none or is gone.
func (r *RehearsalReconciler) class(ctx context.Context, cp *lodiav1alpha1.ControlPlane) (resource.Quantity, *corev1.ResourceRequirements, error) {
	quota := resource.MustParse(lodiav1alpha1.DefaultQuotaBackendBytes)
	running := cp.Status.Datastore.Class
	if running == nil {
		return quota, nil, nil
	}
	class := &lodiav1alpha1.DatastoreClass{}
	if err := r.Get(ctx, client.ObjectKey{Name: running.Name}, class); err != nil {
		return quota, nil, client.IgnoreNotFound(err)
	}
	settings := class.Spec.Etcd
	if settings == nil {
		return quota, nil, nil
	}
	if settings.QuotaBackendBytes != nil {
		quota = settings.QuotaBackendBytes.DeepCopy()
	}
	return quota, settings.Resources, nil
}

// judge reads the copy's verdict from its pod, or sweeps it once its API
// server is ready.
func (r *RehearsalReconciler) judge(ctx context.Context, cp *lodiav1alpha1.ControlPlane, version string, verdict *metav1.Condition, pod *corev1.Pod) (ctrl.Result, error) {
	age := r.Clock.Since(pod.CreationTimestamp.Time)
	api := containerStatus(pod.Status.ContainerStatuses, apiServer)
	started := api != nil && (api.State.Running != nil || api.State.Terminated != nil || api.LastTerminationState.Terminated != nil)
	switch exit := exited(api); {
	case exit != "":
		return r.conclude(ctx, cp, version, pod, metav1.ConditionFalse, lodiav1alpha1.ReasonAPIServerFailed,
			fmt.Sprintf("the copy's kube-apiserver at %s exited %s", version, exit))
	case pod.Status.Phase == corev1.PodFailed || !started && age >= copyDeadline:
		return r.conclude(ctx, cp, version, pod, metav1.ConditionFalse, lodiav1alpha1.ReasonCopyFailed,
			fmt.Sprintf("the copy did not run its API server at %s within %s of its creation: %s", version, copyDeadline, lastReason(pod)))
	case age >= copyDeadline:
		return r.conclude(ctx, cp, version, pod, metav1.ConditionFalse, lodiav1alpha1.ReasonCopyFailed,
			fmt.Sprintf("the copy at %s gave no verdict within %s of its creation%s", version, copyDeadline, r.lastRead(pod)))
	case api != nil && api.Ready:
		return r.sweep(ctx, cp, version, verdict, pod)
	case started && age >= readyWithin:
		return r.conclude(ctx, cp, version, pod, metav1.ConditionFalse, lodiav1alpha1.ReasonAPIServerFailed,
			fmt.Sprintf("the copy's kube-apiserver at %s runs and is not ready %s after the pod's creation", version, readyWithin))
	}
	if err := r.rehearsingNow(ctx, cp, version, verdict); err != nil {
		return ctrl.Result{}, err
	}
	if started && age < readyWithin {
		return ctrl.Result{RequeueAfter: readyWithin - age}, nil
	}
	return ctrl.Result{RequeueAfter: copyDeadline - age}, nil
}

func containerStatus(statuses []corev1.ContainerStatus, name string) *corev1.ContainerStatus {
	for i := range statuses {
		if statuses[i].Name == name {
			return &statuses[i]
		}
	}
	return nil
}

// exited says how the container ended, its exit code, reason and termination
// message, empty while it has never ended: the copy's API server never exits
// on its own.
func exited(c *corev1.ContainerStatus) string {
	if c == nil {
		return ""
	}
	t := c.State.Terminated
	if t == nil {
		t = c.LastTerminationState.Terminated
	}
	if t == nil {
		return ""
	}
	out := fmt.Sprintf("%d (%s)", t.ExitCode, t.Reason)
	if m := strings.TrimSpace(t.Message); m != "" {
		out += ": " + quoted(m)
	}
	return out
}

// lastReason is why the copy did not run its API server: the last init
// container that has not finished, with its state, or the pod's reason.
func lastReason(pod *corev1.Pod) string {
	for _, c := range pod.Status.InitContainerStatuses {
		if c.State.Terminated != nil && c.State.Terminated.ExitCode == 0 || c.Started != nil && *c.Started && c.Ready {
			continue
		}
		switch {
		case c.State.Waiting != nil && c.State.Waiting.Reason != "":
			out := "init container " + c.Name + " " + c.State.Waiting.Reason
			if exit := exited(&c); exit != "" {
				out += ", last exited " + exit
			} else if m := strings.TrimSpace(c.State.Waiting.Message); m != "" {
				out += ": " + quoted(m)
			}
			return out
		case exited(&c) != "":
			return "init container " + c.Name + " exited " + exited(&c)
		}
		return "init container " + c.Name + " has not finished"
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status != corev1.ConditionTrue {
			return "not scheduled: " + c.Reason + ": " + quoted(c.Message)
		}
	}
	if pod.Status.Reason != "" {
		return "the pod is " + pod.Status.Reason + ": " + quoted(pod.Status.Message)
	}
	return "the pod is " + string(pod.Status.Phase)
}

// quoted is s cut to maxQuoted bytes before the rune that crosses it.
func quoted(s string) string {
	if len(s) <= maxQuoted {
		return s
	}
	cut := maxQuoted
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// conclude writes the verdict, then deletes the copy: a copy whose deletion
// fails is judged again and deleted on the next pass.
func (r *RehearsalReconciler) conclude(ctx context.Context, cp *lodiav1alpha1.ControlPlane, version string, pod *corev1.Pod, status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {
	if err := r.report(ctx, cp, version, status, reason, message); err != nil {
		return ctrl.Result{}, err
	}
	r.mu.Lock()
	if r.verdicts == nil {
		r.verdicts = map[types.UID]time.Time{}
	}
	r.verdicts[cp.UID] = r.Clock.Now()
	r.mu.Unlock()
	if err := r.remove(ctx, cp, pod); err != nil {
		return ctrl.Result{}, err
	}
	if status == metav1.ConditionFalse {
		return ctrl.Result{RequeueAfter: rehearseAgain}, nil
	}
	return ctrl.Result{}, nil
}

// soonerAgain are the failed verdicts about the copy's objects on the
// management cluster, which run again on their own requeue and not 10
// minutes on.
var soonerAgain = []string{lodiav1alpha1.ReasonRefused, lodiav1alpha1.ReasonObjectsControlledElsewhere}

// againAt is when a failed verdict runs again: at the first of every 10
// minutes from Rehearsed's last transition after this operator last
// concluded a verdict for cp, or first saw this one. A failed verdict stays
// through its runs again, so the transition does not move, and a restarted
// operator waits for the next mark instead of running at once.
func (r *RehearsalReconciler) againAt(uid types.UID, failed *metav1.Condition) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.verdicts == nil {
		r.verdicts = map[types.UID]time.Time{}
	}
	since, ok := r.verdicts[uid]
	if !ok {
		since = r.Clock.Now()
		r.verdicts[uid] = since
	}
	from := failed.LastTransitionTime.Time
	if since.Before(from) {
		return from.Add(rehearseAgain)
	}
	return from.Add(since.Sub(from).Truncate(rehearseAgain) + rehearseAgain)
}

// remove deletes the copy and its NetworkPolicy, those cp controls, and
// forgets its sweep.
func (r *RehearsalReconciler) remove(ctx context.Context, cp *lodiav1alpha1.ControlPlane, pod *corev1.Pod) error {
	if pod != nil {
		r.endSweeps(cp.UID, "")
		if err := r.deleteCopy(ctx, pod); err != nil {
			return err
		}
	}
	policy := &networkingv1.NetworkPolicy{}
	key := client.ObjectKey{Namespace: cp.Namespace, Name: copyName(cp.Name)}
	if err := r.Get(ctx, key, policy); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(policy, cp) {
		return nil
	}
	if err := client.IgnoreNotFound(r.Delete(ctx, policy, client.Preconditions{UID: &policy.UID})); err != nil {
		return fmt.Errorf("delete NetworkPolicy %s: %w", policy.Name, err)
	}
	return nil
}

func (r *RehearsalReconciler) deleteCopy(ctx context.Context, pod *corev1.Pod) error {
	if !pod.DeletionTimestamp.IsZero() {
		return nil
	}
	if err := client.IgnoreNotFound(r.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}, client.GracePeriodSeconds(int64(copyGrace/time.Second)))); err != nil {
		return fmt.Errorf("delete Pod %s: %w", pod.Name, err)
	}
	return nil
}

// policyHeld names the NetworkPolicy under the copy's name when another owner
// keeps it.
func (r *RehearsalReconciler) policyHeld(ctx context.Context, cp *lodiav1alpha1.ControlPlane, policy *networkingv1.NetworkPolicy) (string, error) {
	current := &networkingv1.NetworkPolicy{}
	err := r.Get(ctx, client.ObjectKeyFromObject(policy), current)
	if apierrors.IsNotFound(err) {
		err = r.Reader.Get(ctx, client.ObjectKeyFromObject(policy), current)
	}
	switch {
	case apierrors.IsNotFound(err):
		return "", nil
	case err != nil:
		return "", err
	}
	if owner := ownership.ControlledElsewhere(current, cp); owner != nil {
		return elsewhere("NetworkPolicy", policy.Name, owner), nil
	}
	return "", nil
}

func elsewhere(kind, name string, owner *metav1.OwnerReference) string {
	return fmt.Sprintf("another owner keeps %s %s by %s %s (UID %s); the rehearsal writes it once it is gone: the garbage collector deletes a previous ControlPlane's, any other owner's needs a person",
		kind, name, owner.Kind, owner.Name, owner.UID)
}

func (r *RehearsalReconciler) apply(ctx context.Context, policy *networkingv1.NetworkPolicy) error {
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(policy)
	if err != nil {
		return err
	}
	dropNulls(content)
	if err := r.Apply(ctx, client.ApplyConfigurationFromUnstructured(&unstructured.Unstructured{Object: content}), client.FieldOwner(RehearsalFieldManager), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply NetworkPolicy %s: %w", policy.Name, err)
	}
	return nil
}

// refused reports a write the API server refused and returns its error for
// the backoff; any other error is returned alone.
func (r *RehearsalReconciler) refused(ctx context.Context, cp *lodiav1alpha1.ControlPlane, version, kind, name string, err error) error {
	if !apistatus.Refused(err) {
		return err
	}
	message := fmt.Sprintf("the API server refuses %s %s; the reason is in Lodia's log", kind, name)
	if reportErr := r.report(ctx, cp, version, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, message); reportErr != nil {
		return reportErr
	}
	return err
}

// report applies Rehearsed at cp's generation and status.rehearsal, compared
// with cp's, which holds both as the API server does.
func (r *RehearsalReconciler) report(ctx context.Context, cp *lodiav1alpha1.ControlPlane, version string, status metav1.ConditionStatus, reason, message string) error {
	condition := metav1.Condition{Type: lodiav1alpha1.ConditionRehearsed, Status: status, Reason: reason, Message: message, ObservedGeneration: cp.Generation}
	changed := cp.Status.Rehearsal == nil || cp.Status.Rehearsal.Version != version
	desired := lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{condition}, Rehearsal: &lodiav1alpha1.RehearsalStatus{Version: version}}
	return statusapply.Writer{Client: r.Client, Reader: r.Reader, Manager: RehearsalFieldManager, Events: r.Events}.ApplyInto(ctx, cp, desired, changed)
}

func (r *RehearsalReconciler) forget(key types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tenants != nil {
		r.tenants.Forget(key)
	}
}
