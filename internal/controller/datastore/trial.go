// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/contract"
	"go.lodia.io/lodia/internal/etcddruid"
	"go.lodia.io/lodia/internal/statusapply"
)

const inTheLog = "; the reason is in Lodia's log"

// trial is what a held pass finds of the writes the grant's first pass would
// send, which it tries by dry run: each refusal, with the errors for the log,
// and the objects an admission webhook keeps from any verdict, with those
// webhooks.
type trial struct {
	refused, untried, webhooks []string
	refusals                   error
}

// trying says whether a pass tries the writes of the grant's first pass it
// does not send, which only a held pass leaves: while DatastoreReady holds a
// refusal, which only what the pass sees now ends, or is not True, which may
// hide one in its message (docs/decisions/upgrades-and-maintenance-windows.md).
func trying(cp *lodiav1alpha1.ControlPlane) bool {
	c := meta.FindStatusCondition(cp.Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
	return c != nil && (c.Status != metav1.ConditionTrue || slices.Contains(contract.Refusals(), c.Reason))
}

// answer records the API server's answer to the dry run of named in t: a
// refusal, with why, or a webhook that keeps it from any verdict. An error
// that is no answer it returns.
func (t *trial) answer(named string, err error, why string) error {
	switch webhook := apistatus.DryRunUnsupported(err); {
	case err == nil:
	case webhook != "":
		t.untried = append(t.untried, named)
		if !slices.Contains(t.webhooks, webhook) {
			t.webhooks = append(t.webhooks, webhook)
		}
	case apistatus.Refused(err):
		t.refused = append(t.refused, "the grant's write of "+named+" would be refused: the API server refuses the dry run"+why)
		t.refusals = errors.Join(t.refusals, fmt.Errorf("apply %s by dry run: %w", named, err))
	default:
		return fmt.Errorf("apply %s by dry run: %w", named, err)
	}
	return nil
}

// tryEtcd sends the running revision's render of applied, the Etcd a held
// pass has just copied, as the grant's first pass would render it from there,
// by dry run.
func (r *Reconciler) tryEtcd(ctx context.Context, p *pass, applied *etcddruid.Store) error {
	class := p.class
	if p.keep {
		class = nil
	}
	render, _ := r.Druid.Render(p.cp, class, p.namespaceUID, p.generation, applied, p.claims, p.rollout, p.members)
	render.Stamp(r.Revision)
	err := r.Druid.Try(ctx, r.Client, FieldManager, render)
	if replaced(err) {
		return err
	}
	var why string
	var heldErr error
	if apistatus.Refused(err) {
		why, heldErr = r.whyRefused(ctx, render, err)
	}
	return errors.Join(p.tried.answer(sent(p, render), err, why), heldErr)
}

// on is c with what t found after its message, as statusapply.Tried gives
// it.
func (t *trial) on(c metav1.Condition) metav1.Condition {
	if t == nil {
		return c
	}
	var untried string
	if len(t.untried) > 0 {
		answers := make([]string, 0, len(t.webhooks))
		for _, w := range t.webhooks {
			answers = append(answers, apistatus.Unsupported(w))
		}
		untried = "the grant's write of " + strings.Join(t.untried, ", ") + " cannot be tried: " + strings.Join(answers, ", ")
	}
	return statusapply.Tried(c, strings.Join(t.refused, "; "), untried)
}
