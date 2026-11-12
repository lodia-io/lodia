// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
)

const (
	// heldLeaseSeconds is the duration of the Lease Lodia holds, so that a Lodia
	// that stops renewing gives it back after that long.
	heldLeaseSeconds int32 = 60

	// releasedLeaseSeconds is the duration client-go leaves on a Lease its holder
	// releases (https://github.com/kubernetes/client-go/blob/v0.37.1/tools/leaderelection/leaderelection.go#L341-L347).
	releasedLeaseSeconds int32 = 1

	// maxQuoted bounds what a message quotes from the tenant: a Lease's holder
	// and a refusal have no limit, and a message over the schema's 32768 fails
	// the whole status apply.
	maxQuoted = 1024
)

type leaseWrite int

const (
	noWrite leaseWrite = iota
	takeLease
	renewLease
	releaseLease
)

// row is what selectRow decides, a row of the table in
// docs/decisions/controller-manager-hold.md: what the pass writes in the
// tenant, and what it reports once that is written. A nil condition applies
// nothing. silent and counted are status.health's, when counts says the row
// has a list to count from.
type row struct {
	write           leaseWrite
	condition       *metav1.Condition
	silent, counted int
	counts          bool
}

// holdOrRelease reads the controller manager's Lease, lists the node Leases
// and the Nodes, selects a row and makes its write, then reports. A conflict selects the row again from the Lease as it stands then,
// so that a retry never writes over a holder the table does not allow.
func (r *NodesReconciler) holdOrRelease(ctx context.Context, cp *lodiav1alpha1.ControlPlane, t *tenant, s *sampling, now time.Time) error {
	lease, readErr := r.readLease(ctx, t)
	listed := false
	if readErr == nil && access(cp) {
		items, err := r.list(ctx, t, s, now)
		if err != nil {
			logf.FromContext(ctx).Error(err, "list the node Leases")
		} else {
			s.observe(items, now)
			listed = true
		}
	}
	var chosen row
	first := true
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if !first {
			lease, readErr = r.readLease(ctx, t)
		}
		first = false
		var err error
		if chosen, err = r.selectRow(ctx, cp, s, lease, readErr, listed, now); err != nil {
			return err
		}
		written, err := r.writeLease(ctx, t, lease, chosen.write, now)
		if err == nil {
			lease = written
			return nil
		}
		if !apistatus.Refused(err) {
			return err
		}
		logf.FromContext(ctx).Error(err, "the API server refused a write of the controller manager's Lease")
		chosen = refusedRow(cp, chosen, err)
		return nil
	})
	if err != nil {
		return fmt.Errorf("hold the controller manager: %w", err)
	}
	if chosen.condition == nil {
		return nil
	}
	return r.report(ctx, cp, s.status(cp), holdStatus(chosen, lease), *chosen.condition)
}

// readLease reads the controller manager's Lease with a consistent GET; a
// Lease that does not exist reads as nil, with no error.
func (r *NodesReconciler) readLease(ctx context.Context, t *tenant) (*coordinationv1.Lease, error) {
	reading, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	lease, err := t.leases.Get(reading, lodiav1alpha1.ControllerManagerLease, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		logf.FromContext(ctx).Error(err, "read the controller manager's Lease")
		return nil, err
	}
	return lease, nil
}

// selectRow selects the first row of that table that matches. A pass that gets here with no
// list of the node Leases behind it, or with a list stopped at a bound,
// restarts the clock and clears the count towards the release, whichever row
// follows; a hold with no list goes on renewing, and one beyond a bound ends.
func (r *NodesReconciler) selectRow(ctx context.Context, cp *lodiav1alpha1.ControlPlane, s *sampling, lease *coordinationv1.Lease, readErr error, listed bool, now time.Time) (row, error) {
	stop := s.stopped()
	if readErr != nil || !listed || stop != "" {
		s.restart()
	}
	if readErr != nil {
		if !access(cp) {
			return row{condition: held(cp, lodiav1alpha1.ReasonWaitingForAccess, accessNotTrue)}, nil
		}
		return row{}, nil
	}
	if lease != nil && s.leader.resourceVersion != lease.ResourceVersion {
		s.leader = renewal{resourceVersion: lease.ResourceVersion, at: now}
	}
	holder := leaseHolder(lease)
	ours := holder == lodiav1alpha1.HoldHolderIdentity
	switch {
	case cp.Spec.Health != nil && !ptr.Deref(cp.Spec.Health.HoldControllerManager, true):
		disabled := held(cp, lodiav1alpha1.ReasonDisabled, "spec.health.holdControllerManager is false")
		if ours {
			return row{write: releaseLease, condition: disabled}, nil
		}
		return row{condition: disabled}, nil
	case !access(cp):
		if ours {
			return row{write: renewLease, condition: nodesSilent(cp, accessNotTrue+keepsHolding)}, nil
		}
		return row{condition: held(cp, lodiav1alpha1.ReasonWaitingForAccess, accessNotTrue)}, nil
	case !listed:
		const failed = "the Leases of " + corev1.NamespaceNodeLease + " could not be listed"
		if ours {
			return row{write: renewLease, condition: nodesSilent(cp, failed+keepsHolding)}, nil
		}
		return row{condition: held(cp, lodiav1alpha1.ReasonNotHeld, failed)}, nil
	case stop != "":
		beyond := held(cp, lodiav1alpha1.ReasonTooManyNodes, stop+": Lodia does not count this tenant's nodes")
		if ours {
			return row{write: releaseLease, condition: beyond}, nil
		}
		return row{condition: beyond}, nil
	case s.listedAt.IsZero():
		const notListed = "the nodes are not listed yet"
		if ours {
			return row{write: renewLease, condition: nodesSilent(cp, notListed+keepsHolding)}, nil
		}
		return row{condition: held(cp, lodiav1alpha1.ReasonNotHeld, notListed)}, nil
	}
	began := now
	if ours && lease.Spec.AcquireTime != nil {
		began = lease.Spec.AcquireTime.Time
	}
	silent, counted := r.silence(s, now, began)
	trusted := now.Sub(s.restarted) >= r.SilentAfter
	if trusted {
		s.count = &nodeCount{uid: s.uid, silent: silent, counted: counted}
	}
	decide := func(write leaseWrite, condition *metav1.Condition) (row, error) {
		return row{write: write, condition: condition, silent: silent, counted: counted, counts: true}, nil
	}
	share := strconv.Itoa(silent) + " of " + strconv.Itoa(counted) + " counted nodes are silent"
	percent := strconv.FormatFloat(float64(r.HoldFraction)/10, 'f', -1, 64) + "%"
	if counted >= 2 && silent*1000 >= r.HoldFraction*counted {
		s.below = time.Time{}
		share += ", at or above " + percent
		if ours {
			return decide(renewLease, nodesSilent(cp, share+holds))
		}
		if holder != "" && !s.expired(lease, now) {
			pod, err := r.podOfControlPlane(ctx, cp, holder)
			if err != nil {
				return row{}, fmt.Errorf("list the pods of the ControlPlane: %w", err)
			}
			if !pod {
				return row{condition: held(cp, lodiav1alpha1.ReasonHoldBlocked, share+", but "+quoted(holder)+" holds Lease "+lodiav1alpha1.LeaderElectionNamespace+"/"+lodiav1alpha1.ControllerManagerLease)}, nil
			}
		}
		return decide(takeLease, nodesSilent(cp, share+holds))
	}
	if counted < 2 {
		share += "; fewer than 2 are counted"
	}
	if !ours {
		return row{condition: held(cp, lodiav1alpha1.ReasonNotHeld, share)}, nil
	}
	if s.below.IsZero() && trusted {
		s.below = now
	}
	if !s.below.IsZero() && now.Sub(s.below) >= r.ReleaseAfter {
		return decide(releaseLease, held(cp, lodiav1alpha1.ReasonNotHeld, share))
	}
	return decide(renewLease, nodesSilent(cp, share+", below "+percent+holds+" until they have been back for "+r.ReleaseAfter.String()))
}

const (
	holds = "; Lodia holds the controller manager's leader Lease"
	// keepsHolding ends a message of the rows that renew a hold they cannot count.
	keepsHolding = "; Lodia keeps holding the controller manager's leader Lease"
)

func nodesSilent(cp *lodiav1alpha1.ControlPlane, message string) *metav1.Condition {
	return newCondition(cp, lodiav1alpha1.ConditionControllerManagerHeld, metav1.ConditionTrue, lodiav1alpha1.ReasonNodesSilent, message)
}

// refusedRow is what the pass reports of a write the API server refused: a take
// tells the refusal, and a renewal or a release keeps the Lease as it read it.
func refusedRow(cp *lodiav1alpha1.ControlPlane, chosen row, err error) row {
	verb := map[leaseWrite]string{takeLease: "take", renewLease: "renewal", releaseLease: "release"}[chosen.write]
	message := "the API server refused the " + verb + " of Lease " + lodiav1alpha1.LeaderElectionNamespace + "/" + lodiav1alpha1.ControllerManagerLease + ": " + quoted(err.Error())
	if chosen.write == takeLease {
		return row{condition: held(cp, lodiav1alpha1.ReasonHoldBlocked, message)}
	}
	chosen.condition = nodesSilent(cp, message)
	return chosen
}

// quoted cuts text from the tenant to maxQuoted characters.
func quoted(text string) string {
	if utf8.RuneCountInString(text) <= maxQuoted {
		return text
	}
	return string([]rune(text)[:maxQuoted])
}

func holdStatus(chosen row, lease *coordinationv1.Lease) *lodiav1alpha1.HealthStatus {
	if chosen.condition.Reason != lodiav1alpha1.ReasonNodesSilent {
		return nil
	}
	hold := &lodiav1alpha1.ControllerManagerHold{}
	if chosen.counts {
		hold.SilentNodes, hold.CountedNodes = int32(chosen.silent), int32(chosen.counted)
	}
	if lease.Spec.AcquireTime != nil {
		hold.Since = new(metav1.NewTime(lease.Spec.AcquireTime.Time).Rfc3339Copy())
	}
	return &lodiav1alpha1.HealthStatus{ControllerManagerHold: hold}
}

func leaseHolder(lease *coordinationv1.Lease) string {
	if lease == nil {
		return ""
	}
	return ptr.Deref(lease.Spec.HolderIdentity, "")
}

// expired says whether the Lease has not changed for its duration, by the
// operator's clock, as client-go's observed time works.
func (s *sampling) expired(lease *coordinationv1.Lease, now time.Time) bool {
	return now.Sub(s.leader.at) >= time.Duration(ptr.Deref(lease.Spec.LeaseDurationSeconds, 0))*time.Second
}

// podOfControlPlane says whether holder is a pod Lodia runs for cp: a leader's
// identity is <pod name>_<uuid>. The list goes through the API server, since no
// cache holds pods.
func (r *NodesReconciler) podOfControlPlane(ctx context.Context, cp *lodiav1alpha1.ControlPlane, holder string) (bool, error) {
	pods := &corev1.PodList{}
	if err := r.Reader.List(ctx, pods, client.InNamespace(cp.Namespace), client.MatchingLabels{lodiav1alpha1.ControlPlaneLabel: cp.Name}); err != nil {
		return false, err
	}
	for _, pod := range pods.Items {
		if strings.HasPrefix(holder, pod.Name+"_") {
			return true, nil
		}
	}
	return false, nil
}

// writeLease makes the row's write, over the Lease as it was read, and keeps
// the fields Lodia does not own; a Lease that does not exist is created.
func (r *NodesReconciler) writeLease(ctx context.Context, t *tenant, read *coordinationv1.Lease, write leaseWrite, now time.Time) (*coordinationv1.Lease, error) {
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: lodiav1alpha1.LeaderElectionNamespace, Name: lodiav1alpha1.ControllerManagerLease}}
	if read != nil {
		lease = read.DeepCopy()
	}
	at := new(metav1.NewMicroTime(now))
	spec := &lease.Spec
	switch write {
	case noWrite:
		return read, nil
	case takeLease:
		spec.HolderIdentity, spec.LeaseDurationSeconds = ptr.To(lodiav1alpha1.HoldHolderIdentity), new(heldLeaseSeconds)
		spec.AcquireTime, spec.RenewTime = at, at
		spec.LeaseTransitions = new(ptr.Deref(spec.LeaseTransitions, 0) + 1)
	case renewLease:
		spec.LeaseDurationSeconds, spec.RenewTime = new(heldLeaseSeconds), at
	case releaseLease:
		spec.HolderIdentity, spec.LeaseDurationSeconds = new(""), new(releasedLeaseSeconds)
		spec.AcquireTime, spec.RenewTime = at, at
	}
	writing, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	if read == nil {
		return t.leases.Create(writing, lease, metav1.CreateOptions{})
	}
	return t.leases.Update(writing, lease, metav1.UpdateOptions{})
}
