// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	druidCluster    = 0xfc1e285c08de6c0c
	restoredCluster = 0xcdf818194e3a8c32
)

var toldAt = time.Date(2026, 11, 26, 10, 0, 0, 400_000_000, time.UTC)

func ruleControlPlane() *lodiav1alpha1.ControlPlane {
	return &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: "tenants", Name: "dev", UID: "uid-dev"}}
}

func ruleSampler(t *testing.T, live *lodiav1alpha1.ControlPlane, funcs interceptor.Funcs) *SampleReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := interceptor.NewClient(fake.NewClientBuilder().WithScheme(scheme).WithObjects(live).Build(), funcs)
	return &SampleReconciler{Reader: reader, Latches: NewLatches()}
}

func told(t *testing.T, r *SampleReconciler, cp *lodiav1alpha1.ControlPlane) lodiav1alpha1.MemberRestoreStatus {
	t.Helper()
	r.Latches.mu.Lock()
	defer r.Latches.mu.Unlock()
	l := r.Latches.held[client.ObjectKeyFromObject(cp)]
	if l == nil {
		return lodiav1alpha1.MemberRestoreStatus{}
	}
	return l.told
}

func answered(t *testing.T, r *SampleReconciler, cp *lodiav1alpha1.ControlPlane, listed []string, answers map[string]answer) {
	t.Helper()
	r.tell(t.Context(), cp, toldAt, listed, answers)
}

func TestEachSignalTellsANewLifeAndTheBoundsAtEqualityDoNot(t *testing.T) {
	kept := answer{cluster: druidCluster, term: 3, lower: -20, upper: -18}
	for _, c := range []struct {
		name string
		got  answer
		new  bool
	}{
		{"the same answer", kept, false},
		{"the lower bound at the kept upper bound", answer{cluster: druidCluster, term: 3, lower: -18, upper: -18}, false},
		{"the lower bound past the kept upper bound", answer{cluster: druidCluster, term: 3, lower: -17, upper: -17}, true},
		{"a higher term with an offset below", answer{cluster: druidCluster, term: 4, lower: -40, upper: -39}, false},
		{"a lower term", answer{cluster: druidCluster, term: 2, lower: -40, upper: -39}, true},
		{"another cluster ID", answer{cluster: restoredCluster, term: 3, lower: -40, upper: -39}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := ruleControlPlane()
			r := ruleSampler(t, cp, interceptor.Funcs{})
			answered(t, r, cp, []string{"dev-etcd-0"}, map[string]answer{"dev-etcd-0": kept})
			answered(t, r, cp, []string{"dev-etcd-0"}, map[string]answer{"dev-etcd-0": c.got})
			if got := told(t, r, cp); (got != (lodiav1alpha1.MemberRestoreStatus{})) != c.new {
				t.Errorf("%s told %+v, want a new life told: %t", c.name, got, c.new)
			}
		})
	}
}

func TestANewLifeIsLatchedAtTheSecondOfThePassForTheFirstMemberByNameWithItsSource(t *testing.T) {
	for _, c := range []struct {
		name    string
		cluster uint64
		from    lodiav1alpha1.MemberRestoreFrom
	}{
		{"restored from snapshots", restoredCluster, lodiav1alpha1.MemberRestoreFromSnapshots},
		{"started empty", 0x1234, lodiav1alpha1.MemberRestoreFromEmpty},
	} {
		t.Run(c.name, func(t *testing.T) {
			cp := ruleControlPlane()
			r := ruleSampler(t, cp, interceptor.Funcs{})
			members := []string{"dev-etcd-0", "dev-etcd-1", "dev-etcd-2"}
			kept := answer{cluster: druidCluster, term: 3, lower: -20, upper: -18}
			answered(t, r, cp, members, map[string]answer{"dev-etcd-0": kept, "dev-etcd-1": kept, "dev-etcd-2": kept})
			fresh := answer{cluster: c.cluster, term: 2, lower: -4, upper: -4}
			answered(t, r, cp, members, map[string]answer{"dev-etcd-2": fresh, "dev-etcd-1": fresh, "dev-etcd-0": kept})
			want := lodiav1alpha1.MemberRestoreStatus{Member: "dev-etcd-1", DetectedAt: metav1.NewTime(time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)), From: c.from}
			if got := told(t, r, cp); !same(got, want) {
				t.Errorf("the latch is %+v, want %+v", got, want)
			}
		})
	}
}

func TestAMemberNoLongerListedStartsAfresh(t *testing.T) {
	cp := ruleControlPlane()
	r := ruleSampler(t, cp, interceptor.Funcs{})
	answered(t, r, cp, []string{"dev-etcd-0", "dev-etcd-1"}, map[string]answer{
		"dev-etcd-0": {cluster: druidCluster, term: 3, lower: -20, upper: -18},
		"dev-etcd-1": {cluster: druidCluster, term: 3, lower: -20, upper: -18},
	})
	answered(t, r, cp, []string{"dev-etcd-0"}, map[string]answer{"dev-etcd-0": {cluster: druidCluster, term: 3, lower: -21, upper: -20}})
	answered(t, r, cp, []string{"dev-etcd-0", "dev-etcd-1"}, map[string]answer{
		"dev-etcd-0": {cluster: druidCluster, term: 3, lower: -22, upper: -21},
		"dev-etcd-1": {cluster: druidCluster, term: 3, lower: -2, upper: -1},
	})
	if got := told(t, r, cp); got != (lodiav1alpha1.MemberRestoreStatus{}) {
		t.Errorf("a member added back under its name told %+v, want nothing told", got)
	}
}

func TestAMemberListedThatDoesNotAnswerKeepsItsAnswer(t *testing.T) {
	cp := ruleControlPlane()
	r := ruleSampler(t, cp, interceptor.Funcs{})
	members := []string{"dev-etcd-0", "dev-etcd-1"}
	answered(t, r, cp, members, map[string]answer{
		"dev-etcd-0": {cluster: druidCluster, term: 3, lower: -20, upper: -18},
		"dev-etcd-1": {cluster: druidCluster, term: 3, lower: -20, upper: -18},
	})
	answered(t, r, cp, members, map[string]answer{"dev-etcd-0": {cluster: druidCluster, term: 3, lower: -21, upper: -20}})
	answered(t, r, cp, members, map[string]answer{"dev-etcd-1": {cluster: druidCluster, term: 2, lower: -40, upper: -40}})
	if got := told(t, r, cp); got == (lodiav1alpha1.MemberRestoreStatus{}) || got.Member != "dev-etcd-1" {
		t.Errorf("a member that answered again under a lower term told %+v, want it told", got)
	}
}

func TestAFireDropsTheAnswersOfTheMembersThatGaveNone(t *testing.T) {
	cp := ruleControlPlane()
	r := ruleSampler(t, cp, interceptor.Funcs{})
	members := []string{"dev-etcd-0", "dev-etcd-1", "dev-etcd-2"}
	kept := answer{cluster: druidCluster, term: 3, lower: -20, upper: -18}
	answered(t, r, cp, members, map[string]answer{"dev-etcd-0": kept, "dev-etcd-1": kept, "dev-etcd-2": kept})
	restored := answer{cluster: restoredCluster, term: 3, lower: 90, upper: 90}
	answered(t, r, cp, members, map[string]answer{"dev-etcd-0": restored})
	first := told(t, r, cp)
	if first == (lodiav1alpha1.MemberRestoreStatus{}) {
		t.Fatal("the restore told nothing")
	}
	answered(t, r, cp, members, map[string]answer{"dev-etcd-0": restored, "dev-etcd-1": restored, "dev-etcd-2": restored})
	if got := told(t, r, cp); !same(got, first) {
		t.Errorf("the members that joined the restored etcd told %+v, want the first latch %+v alone", got, first)
	}
}

func TestTheFirstAnswerUnderTheClusterIDOfARestoreIsANewLifeUnlessTheNewestKnownIsOne(t *testing.T) {
	snapshots := lodiav1alpha1.MemberRestoreStatus{Member: "dev-etcd-0", DetectedAt: metav1.NewTime(toldAt.Add(-time.Hour).Truncate(time.Second)), From: lodiav1alpha1.MemberRestoreFromSnapshots}
	empty := lodiav1alpha1.MemberRestoreStatus{Member: "dev-etcd-0", DetectedAt: snapshots.DetectedAt, From: lodiav1alpha1.MemberRestoreFromEmpty}
	for _, c := range []struct {
		name        string
		restoreFrom bool
		record      lodiav1alpha1.MemberRestoreStatus
		cached      lodiav1alpha1.MemberRestoreStatus
		latch       lodiav1alpha1.MemberRestoreFrom
		fires       bool
	}{
		{"no record and no restoreFrom", false, lodiav1alpha1.MemberRestoreStatus{}, lodiav1alpha1.MemberRestoreStatus{}, "", true},
		{"restoreFrom and nothing newer", true, lodiav1alpha1.MemberRestoreStatus{}, lodiav1alpha1.MemberRestoreStatus{}, "", false},
		{"a record from snapshots", false, snapshots, snapshots, "", false},
		{"a record of an empty start", false, empty, empty, "", true},
		{"a record of an empty start after restoreFrom", true, empty, empty, "", true},
		{"a latch from snapshots over a record of an empty start", false, empty, empty, lodiav1alpha1.MemberRestoreFromSnapshots, false},
		{"a latch of an empty start over a record from snapshots", true, snapshots, snapshots, lodiav1alpha1.MemberRestoreFromEmpty, true},
		{"a record from snapshots the cache lacks", false, snapshots, lodiav1alpha1.MemberRestoreStatus{}, "", false},
		{"a record of an empty start where the cache shows one from snapshots", true, empty, snapshots, "", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			live := ruleControlPlane()
			live.Status.MemberRestore = c.record
			cp := ruleControlPlane()
			cp.Status.MemberRestore = c.cached
			if c.restoreFrom {
				live.Spec.RestoreFrom = lodiav1alpha1.RestoreFrom{BackupRef: lodiav1alpha1.LocalReference{Name: "nightly"}}
				cp.Spec.RestoreFrom = live.Spec.RestoreFrom
			}
			r := ruleSampler(t, live, interceptor.Funcs{})
			if c.latch != "" {
				r.Latches.tell(client.ObjectKeyFromObject(cp), cp.UID, lodiav1alpha1.MemberRestoreStatus{Member: "dev-etcd-0", DetectedAt: metav1.NewTime(toldAt.Add(-time.Minute).Truncate(time.Second)), From: c.latch})
			}
			before := told(t, r, cp)
			answered(t, r, cp, []string{"dev-etcd-0"}, map[string]answer{"dev-etcd-0": {cluster: restoredCluster, term: 3, lower: 90, upper: 90}})
			fired := !same(told(t, r, cp), before)
			if fired != c.fires {
				t.Errorf("with %s the first answer of a restored member fired: %t, want %t", c.name, fired, c.fires)
			}
		})
	}
}

func TestAReadOfTheRecordThatFailsKeepsNoAnswerAndTellsNothingUntilOneReads(t *testing.T) {
	cp := ruleControlPlane()
	failing := true
	r := ruleSampler(t, cp, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if failing {
			return errors.New("the API server does not answer")
		}
		return c.Get(ctx, key, obj, opts...)
	}})
	restored := map[string]answer{"dev-etcd-0": {cluster: restoredCluster, term: 3, lower: 90, upper: 90}}
	answered(t, r, cp, []string{"dev-etcd-0"}, restored)
	if got := told(t, r, cp); got != (lodiav1alpha1.MemberRestoreStatus{}) {
		t.Fatalf("a pass whose read failed told %+v, want nothing", got)
	}
	if kept := r.answers(client.ObjectKeyFromObject(cp), cp.UID); len(kept) > 0 {
		t.Fatalf("a pass whose read failed kept %v, want no answer", kept)
	}
	failing = false
	answered(t, r, cp, []string{"dev-etcd-0"}, restored)
	if got := told(t, r, cp); got == (lodiav1alpha1.MemberRestoreStatus{}) {
		t.Error("the next pass told nothing, want the restore told once the read answers")
	}
}

func TestTheRecordIsReadOnlyForAFirstAnswerUnderTheClusterIDOfARestoreWithNoLatch(t *testing.T) {
	cp := ruleControlPlane()
	cp.Status.MemberRestore = lodiav1alpha1.MemberRestoreStatus{Member: "dev-etcd-0", DetectedAt: metav1.NewTime(toldAt.Add(-time.Hour).Truncate(time.Second)), From: lodiav1alpha1.MemberRestoreFromSnapshots}
	reads := 0
	r := ruleSampler(t, cp, interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		reads++
		return c.Get(ctx, key, obj, opts...)
	}})
	bootstrapped := answer{cluster: druidCluster, term: 3, lower: -20, upper: -18}
	restored := answer{cluster: restoredCluster, term: 3, lower: 90, upper: 90}
	for _, step := range []struct {
		name    string
		latch   bool
		answers map[string]answer
		reads   int
	}{
		{"a first answer under etcd-druid's cluster ID", false, map[string]answer{"dev-etcd-0": bootstrapped}, 0},
		{"every answer kept", false, map[string]answer{"dev-etcd-0": bootstrapped}, 0},
		{"a first answer under the cluster ID of a restore", false, map[string]answer{"dev-etcd-0": bootstrapped, "dev-etcd-1": restored}, 1},
		{"every answer kept, one under the cluster ID of a restore", false, map[string]answer{"dev-etcd-0": bootstrapped, "dev-etcd-1": restored}, 0},
		{"a first answer under the cluster ID of a restore with a latch held", true, map[string]answer{"dev-etcd-0": bootstrapped, "dev-etcd-1": restored, "dev-etcd-2": restored}, 0},
	} {
		if step.latch {
			r.Latches.tell(client.ObjectKeyFromObject(cp), cp.UID, cp.Status.MemberRestore)
		}
		before := reads
		answered(t, r, cp, []string{"dev-etcd-0", "dev-etcd-1", "dev-etcd-2"}, step.answers)
		if got := reads - before; got != step.reads {
			t.Errorf("a pass with %s read the ControlPlane %d times, want %d", step.name, got, step.reads)
		}
	}
}

type scripted struct {
	etcdserverpb.UnimplementedMaintenanceServer
	mu      sync.Mutex
	answers []*etcdserverpb.StatusResponse
}

func (s *scripted) Status(context.Context, *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.answers[0]
	s.answers = s.answers[1:]
	return next, nil
}

func TestTheBoundsTakeTheSecondCallsCommittedIndexAndTheFirstCallsAppliedIndex(t *testing.T) {
	server := grpc.NewServer()
	etcdserverpb.RegisterMaintenanceServer(server, &scripted{answers: []*etcdserverpb.StatusResponse{
		{Header: &etcdserverpb.ResponseHeader{ClusterId: druidCluster, Revision: 180}, RaftIndex: 182, RaftAppliedIndex: 181, RaftTerm: 3},
		{Header: &etcdserverpb.ResponseHeader{ClusterId: druidCluster, Revision: 200}, RaftIndex: 203, RaftAppliedIndex: 201, RaftTerm: 3},
	}})
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	url := "http://" + listener.Addr().String()
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{url}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	b, got, err := status(t.Context(), c, url)
	if err != nil {
		t.Fatal(err)
	}
	if want := (answer{cluster: druidCluster, term: 3, lower: 200 - 203, upper: 200 - 181}); got != want || b.Header.GetRevision() != 200 {
		t.Errorf("two calls give %+v with the second's revision %d, want %+v with 200", got, b.Header.GetRevision(), want)
	}
}
