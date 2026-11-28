// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// Latches hold, per ControlPlane, the newest new data life of a member that
// the sampler told and the reconciler has not recorded yet in
// status.memberRestore, and the record the reconciler's first pass on it read
// live, its base. internal/operator hands one to both: they live in the
// operator's memory, and the next leader's sampler, which holds no answer of
// the life before, tells a latch lost with it again only for a restore from
// snapshots while the newest life the API server records is not one, as
// SampleReconciler.tell says.
type Latches struct {
	told chan event.GenericEvent
	mu   sync.Mutex
	held map[types.NamespacedName]*latch
}

// NewLatches returns Latches whose channel the reconciler's source reads.
func NewLatches() *Latches {
	return &Latches{told: make(chan event.GenericEvent), held: map[types.NamespacedName]*latch{}}
}

type latch struct {
	uid  types.UID
	told lodiav1alpha1.MemberRestoreStatus
	// based says that a pass took the latch and kept base, the record its
	// live read showed, none included.
	based bool
	base  lodiav1alpha1.MemberRestoreStatus
}

// is compares by identity, never by time: detectedAt is read from the clock
// of the replica that told the restore.
func (l *latch) is(other latch) bool {
	return l.uid == other.uid && same(l.told, other.told)
}

// record is the latch as it is written over base: one second past base's
// detectedAt when its own is not after it, so that every record that lands
// is after the one it replaces, whatever the replicas' clocks, and its
// annotation moves.
func (l latch) record(base lodiav1alpha1.MemberRestoreStatus) lodiav1alpha1.MemberRestoreStatus {
	record := l.told
	if base != (lodiav1alpha1.MemberRestoreStatus{}) && !record.DetectedAt.After(base.DetectedAt.Time) {
		record.DetectedAt = metav1.NewTime(base.DetectedAt.Add(time.Second))
	}
	return record
}

func (s *Latches) tell(key types.NamespacedName, uid types.UID, told lodiav1alpha1.MemberRestoreStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.held[key] = &latch{uid: uid, told: told}
}

func (s *Latches) newest(key types.NamespacedName, uid types.UID) (lodiav1alpha1.MemberRestoreFrom, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.held[key]
	if l == nil || l.uid != uid {
		return "", false
	}
	return l.told.From, true
}

func (s *Latches) untaken(key types.NamespacedName, uid types.UID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.held[key]
	return l != nil && l.uid == uid && !l.based
}

func (s *Latches) take(key types.NamespacedName) (latch, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.held[key]
	if l == nil {
		return latch{}, false
	}
	return *l, true
}

// setBase keeps base beside taken while taken is still the key's latch, so
// that a latch the sampler replaced since gets the base of the pass that
// takes it.
func (s *Latches) setBase(key types.NamespacedName, taken latch, base lodiav1alpha1.MemberRestoreStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l := s.held[key]; l != nil && l.is(taken) {
		l.based, l.base = true, base
	}
}

// drop removes taken once its record has landed, while it is still the key's
// latch: one the sampler replaced since is written over that record.
func (s *Latches) drop(key types.NamespacedName, taken latch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l := s.held[key]; l != nil && l.is(taken) {
		delete(s.held, key)
	}
}

func (s *Latches) forget(key types.NamespacedName) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.held, key)
}

// send enqueues cp for the reconciler, outside the lock: the reconciler's
// channel source reads only while its controller runs
// (https://github.com/kubernetes-sigs/controller-runtime/blob/v0.25.3/pkg/source/source.go#L175-L266),
// so a send that nobody takes ends with the pass, and the next pass sends
// again.
func (s *Latches) send(ctx context.Context, cp client.Object) {
	select {
	case s.told <- event.GenericEvent{Object: cp}:
	case <-ctx.Done():
	}
}

// same says whether two records name one restore: member, detectedAt and
// from, reportedAt aside.
func same(a, b lodiav1alpha1.MemberRestoreStatus) bool {
	return a.Member == b.Member && a.DetectedAt.Equal(&b.DetectedAt) && a.From == b.From
}
