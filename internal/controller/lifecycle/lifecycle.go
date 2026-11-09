// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package lifecycle is the Lifecycle controller.
package lifecycle

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/statusapply"
	"go.lodia.io/lodia/internal/versions"
)

const (
	Name         = "lifecycle"
	FieldManager = "lodia/" + Name
)

// readiness is what Ready is made of, in the order its reason is taken from.
// In gateway mode RoutesReady comes first: what stops the routes causes what
// the others report then, a wait for the endpoint or a failed probe.
func readiness(cp *lodiav1alpha1.ControlPlane) []string {
	conditions := []string{
		lodiav1alpha1.ConditionCertificatesReady,
		lodiav1alpha1.ConditionDatastoreReady,
		lodiav1alpha1.ConditionAPIServerReady,
		lodiav1alpha1.ConditionAccessReady,
		lodiav1alpha1.ConditionAddonsApplied,
		lodiav1alpha1.ConditionReachable,
		lodiav1alpha1.ConditionControllersLeading,
	}
	if cp.Spec.Endpoint.Gateway != nil {
		return append([]string{lodiav1alpha1.ConditionRoutesReady}, conditions...)
	}
	return conditions
}

type Reconciler struct {
	client.Client
	// Reader reads the ControlPlane as the API server holds it, before a write.
	Reader client.Reader
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, workers int) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WithOptions(controller.Options{MaxConcurrentReconciles: workers, ReconciliationTimeout: time.Minute}).
		For(&lodiav1alpha1.ControlPlane{}).
		Complete(r)
}

// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups=lodia.io,resources=controlplanes/status,verbs=patch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cp := &lodiav1alpha1.ControlPlane{}
	if err := r.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	next := desired(cp)
	changed := next.ObservedGeneration != cp.Status.ObservedGeneration || !equality.Semantic.DeepEqual(next.Target, cp.Status.Target)
	return ctrl.Result{}, statusapply.Apply(ctx, r.Client, r.Reader, FieldManager, cp, next, changed)
}

// desired holds every status field Lifecycle owns and no other: an apply that
// leaves one out removes it.
func desired(cp *lodiav1alpha1.ControlPlane) lodiav1alpha1.ControlPlaneStatus {
	progressing := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionProgressing)
	readyNow := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionReady)
	observedGeneration := cp.Status.ObservedGeneration
	version := ""
	if cp.Status.Target != nil {
		version = cp.Status.Target.Version
	}
	switch {
	case !cp.DeletionTimestamp.IsZero():
		readyNow = newCondition(cp, lodiav1alpha1.ConditionReady, metav1.ConditionFalse, lodiav1alpha1.ReasonDeleting, "the ControlPlane is being deleted")
	case cp.Spec.Paused:
		progressing = newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonPaused, "spec.paused is true: Lodia reconciles nothing")
	default:
		var next lodiav1alpha1.Target
		next, progressing = target(cp, version)
		version = next.Version
		readyNow = ready(cp)
		if observed(cp) {
			observedGeneration = cp.Generation
		}
	}
	status := lodiav1alpha1.ControlPlaneStatus{ObservedGeneration: observedGeneration}
	if version != "" {
		status.Target = &lodiav1alpha1.Target{Version: version}
	}
	for _, c := range []*metav1.Condition{progressing, readyNow} {
		if c != nil {
			status.Conditions = append(status.Conditions, *c)
		}
	}
	return status
}

// target moves within a minor only: a minor upgrade needs the documented
// order, the API server before the controller manager and the scheduler,
// which phase 2 builds (docs/decisions/upgrades-and-maintenance-windows.md).
func target(cp *lodiav1alpha1.ControlPlane, current string) (lodiav1alpha1.Target, *metav1.Condition) {
	want := cp.Spec.Version
	if _, ok := versions.For(want); !ok {
		message := fmt.Sprintf("spec.version %s is not of a minor Lodia runs: %s", want, strings.Join(versions.Minors(), ", "))
		return lodiav1alpha1.Target{Version: current},
			newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported, message)
	}
	wantMinor, _ := versions.Minor(want)
	if currentMinor, ok := versions.Minor(current); ok && currentMinor != wantMinor {
		message := fmt.Sprintf("spec.version %s is another minor than the target %s, which stays until minor upgrades are supported", want, current)
		return lodiav1alpha1.Target{Version: current},
			newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonUnsupported, message)
	}
	if lag := lagging(cp); len(lag) > 0 {
		return lodiav1alpha1.Target{Version: want},
			newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionTrue, lodiav1alpha1.ReasonLatestGenerationNotObserved,
				fmt.Sprintf("generation %d is not taken up yet: %s", cp.Generation, strings.Join(lag, ", ")))
	}
	if rolling := rollingOut(cp); len(rolling) > 0 {
		return lodiav1alpha1.Target{Version: want},
			newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionTrue, lodiav1alpha1.ReasonRollingOut, strings.Join(rolling, "; "))
	}
	return lodiav1alpha1.Target{Version: want},
		newCondition(cp, lodiav1alpha1.ConditionProgressing, metav1.ConditionFalse, lodiav1alpha1.ReasonIdle, "no upgrade, rotation or hibernation is running")
}

// rollingOut names what status.components and status.datastore.members count
// as not updated; a count of updated not reported yet is a spec its writer's
// controller has not observed.
func rollingOut(cp *lodiav1alpha1.ControlPlane) []string {
	type rollout struct {
		name, unit string
		count      lodiav1alpha1.RolloutCount
	}
	var counts []rollout
	if c := cp.Status.Components; c != nil {
		for _, component := range []struct {
			name   string
			status *lodiav1alpha1.ComponentStatus
		}{{"kube-apiserver", c.APIServer}, {"kube-controller-manager", c.ControllerManager}, {"kube-scheduler", c.Scheduler}} {
			if component.status != nil {
				counts = append(counts, rollout{component.name, "replicas", component.status.RolloutCount})
			}
		}
	}
	if d := cp.Status.Datastore; d != nil {
		counts = append(counts, rollout{"etcd", "members", d.Members})
	}
	var rolling []string
	for _, c := range counts {
		switch {
		case c.count.Updated == nil:
			rolling = append(rolling, fmt.Sprintf("%s: its updated %s are not reported yet", c.name, c.unit))
		case *c.count.Updated < c.count.Total:
			rolling = append(rolling, fmt.Sprintf("%s: %d of %d %s updated", c.name, *c.count.Updated, c.count.Total, c.unit))
		}
	}
	return rolling
}

func lagging(cp *lodiav1alpha1.ControlPlane) []string {
	var lag []string
	for _, conditionType := range readiness(cp) {
		if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c != nil && c.ObservedGeneration < cp.Generation {
			lag = append(lag, fmt.Sprintf("%s at %d", conditionType, c.ObservedGeneration))
		}
	}
	return lag
}

func ready(cp *lodiav1alpha1.ControlPlane) *metav1.Condition {
	inputs := readiness(cp)
	next := newCondition(cp, lodiav1alpha1.ConditionReady, metav1.ConditionTrue, lodiav1alpha1.ReasonReady,
		strings.Join(inputs[:len(inputs)-1], ", ")+" and "+inputs[len(inputs)-1]+" are True")
	for _, conditionType := range inputs {
		c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
		if c == nil {
			next = newCondition(cp, lodiav1alpha1.ConditionReady, metav1.ConditionUnknown, lodiav1alpha1.ReasonLatestGenerationNotObserved,
				fmt.Sprintf("%s is not reported for generation %d", conditionType, cp.Generation))
			break
		}
		if c.Status != metav1.ConditionTrue {
			message := conditionType
			if c.Message != "" {
				message += ": " + c.Message
			}
			next = newCondition(cp, lodiav1alpha1.ConditionReady, c.Status, c.Reason, message)
			break
		}
	}
	next.ObservedGeneration = oldest(cp)
	return next
}

// oldest is what Ready describes, so that Ready True at the object's
// generation means every change is taken up.
func oldest(cp *lodiav1alpha1.ControlPlane) int64 {
	generation := cp.Generation
	for _, conditionType := range readiness(cp) {
		c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
		if c == nil {
			return cp.Generation
		}
		generation = min(generation, c.ObservedGeneration)
	}
	return generation
}

func observed(cp *lodiav1alpha1.ControlPlane) bool {
	return !slices.ContainsFunc(readiness(cp), func(conditionType string) bool {
		c := meta.FindStatusCondition(cp.Status.Conditions, conditionType)
		return c == nil || c.ObservedGeneration != cp.Generation
	})
}

func newCondition(cp *lodiav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason, message string) *metav1.Condition {
	return &metav1.Condition{Type: conditionType, Status: status, Reason: reason, Message: message, ObservedGeneration: cp.Generation}
}
