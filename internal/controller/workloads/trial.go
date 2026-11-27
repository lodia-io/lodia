// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/contract"
	"go.lodia.io/lodia/internal/statusapply"
)

// trial is what a held pass finds of the writes the grant's first pass would
// send, which it tries by dry run: the objects whose write the API server
// refuses, with the errors for the log, and those an admission webhook keeps
// from any verdict, with those webhooks.
type trial struct {
	refused, untried, webhooks []string
	refusals                   error
}

// trying says whether a held pass tries the grant's writes: while a condition
// Workloads writes holds a refusal, which only what the pass sees now ends,
// or is not True, which may hide one in its message
// (docs/decisions/upgrades-and-maintenance-windows.md).
func (r *Reconciler) trying(cp *lodiav1alpha1.ControlPlane) bool {
	if !r.held(cp) {
		return false
	}
	for _, conditionType := range []string{lodiav1alpha1.ConditionAPIServerReady, lodiav1alpha1.ConditionRoutesReady, lodiav1alpha1.ConditionLoadBalancerReady} {
		if c := meta.FindStatusCondition(cp.Status.Conditions, conditionType); c != nil && (c.Status != metav1.ConditionTrue || slices.Contains(contract.Refusals(), c.Reason)) {
			return true
		}
	}
	return false
}

// triesPast says whether a held wait for CertificatesReady or DatastoreReady
// renders past it and tries the grant's writes as a held pass does: while it
// tries, over an APIServerReady a wait counts, one that reports availability
// or holds a refusal, whose refusal the trial stands in for. Over any other
// condition the wait reports itself, which is not True, so the first pass past
// it tries.
func (r *Reconciler) triesPast(cp *lodiav1alpha1.ControlPlane) bool {
	c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionAPIServerReady)
	return r.trying(cp) && c != nil && (reportsAvailability(*c) || slices.Contains(contract.Refusals(), c.Reason))
}

// try sends obj, the running revision's render of an object, as the grant's
// first pass would, by dry run, and records the API server's answer in t; an
// error that is no answer it returns.
func (r *Reconciler) try(ctx context.Context, t *trial, obj client.Object) error {
	applied, err := applyConfiguration(obj)
	if err != nil {
		return err
	}
	err = r.Apply(ctx, client.ApplyConfigurationFromUnstructured(applied), client.FieldOwner(FieldManager), client.ForceOwnership, client.DryRunAll)
	switch webhook := apistatus.DryRunUnsupported(err); {
	case err == nil:
	case webhook != "":
		t.untried = append(t.untried, kindAndName(obj))
		if !slices.Contains(t.webhooks, webhook) {
			t.webhooks = append(t.webhooks, webhook)
		}
	case apistatus.Refused(err):
		t.refused = append(t.refused, kindAndName(obj))
		t.refusals = errors.Join(t.refusals, fmt.Errorf("apply %s by dry run: %w", kindAndName(obj), err))
	default:
		return fmt.Errorf("apply %s by dry run: %w", kindAndName(obj), err)
	}
	return nil
}

// on is c with what t found after its message, as statusapply.Tried gives
// it.
func (t *trial) on(c metav1.Condition) metav1.Condition {
	if t == nil {
		return c
	}
	var refused, untried string
	if len(t.refused) > 0 {
		refused = "the grant's write of " + strings.Join(t.refused, ", ") + " would be refused: the API server refuses the dry run" + inTheLog
	}
	if len(t.untried) > 0 {
		untried = "the grant's write of " + strings.Join(t.untried, ", ") + " cannot be tried: " + unsupported(t.webhooks)
	}
	return statusapply.Tried(c, refused, untried)
}

// over is c, a condition a held pass or a held wait builds anew, ending with
// refused, the authentication configuration's refusal, then with what t
// found. Over the grant's wait, which Lifecycle grants at once, a refusal
// takes its own reason under False too, so that it holds the grant as every
// refusal does; a write no dry run can try alone leaves the wait.
func (t *trial) over(c metav1.Condition, refused string) metav1.Condition {
	if c.Reason == lodiav1alpha1.ReasonWaitingForTarget {
		switch {
		case refused != "":
			c.Reason = lodiav1alpha1.ReasonAuthenticationInvalid
		case t != nil && len(t.refused) > 0:
			c.Reason = lodiav1alpha1.ReasonRefused
		}
	}
	return t.on(authenticated(c, refused))
}

// unsupported is the API server's answer of each webhook that does not
// support dry run.
func unsupported(webhooks []string) string {
	answers := make([]string, 0, len(webhooks))
	for _, w := range webhooks {
		answers = append(answers, apistatus.Unsupported(w))
	}
	return strings.Join(answers, ", ")
}
