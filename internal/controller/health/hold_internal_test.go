// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

type sent struct {
	method, path, contentType, accept string
	body                              map[string]any
}

type recorder struct {
	mu   sync.Mutex
	sent []sent
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	var decoded map[string]any
	_ = json.Unmarshal(body, &decoded)
	r.mu.Lock()
	r.sent = append(r.sent, sent{req.Method, req.URL.Path, req.Header.Get("Content-Type"), req.Header.Get("Accept"), decoded})
	r.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"metadata":{"resourceVersion":"8"},"spec":{"holderIdentity":"lodia","leaseTransitions":5,"renewTime":"2026-11-08T12:00:00.000000Z"}}`)
}

func TestAWriteSendsAMergePatchOfTheSpecItWritesUnderTheResourceVersionItRead(t *testing.T) {
	const (
		leases = "/apis/coordination.k8s.io/v1/namespaces/kube-system/leases"
		at     = "2026-11-08T12:00:00.000000Z"
	)
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	read := &leaderLease{}
	read.Metadata.ResourceVersion = "7"
	read.Spec.LeaseTransitions = new(int32(4))
	for _, c := range []struct {
		name  string
		write leaseWrite
		spec  map[string]any
	}{
		{"a take", takeLease, map[string]any{"holderIdentity": lodiav1alpha1.HoldHolderIdentity, "leaseDurationSeconds": 60.0, "acquireTime": at, "renewTime": at, "leaseTransitions": 5.0}},
		{"a renewal", renewLease, map[string]any{"leaseDurationSeconds": 60.0, "renewTime": at}},
		{"a release", releaseLease, map[string]any{"holderIdentity": "", "leaseDurationSeconds": 1.0, "acquireTime": at, "renewTime": at}},
	} {
		t.Run(c.name, func(t *testing.T) {
			var requests recorder
			server := httptest.NewServer(&requests)
			t.Cleanup(server.Close)
			r := &NodesReconciler{Timeout: time.Second}
			tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
			if err != nil {
				t.Fatal(err)
			}
			written, err := r.writeLease(t.Context(), tenant, read, c.write, now)
			if err != nil {
				t.Fatal(err)
			}
			if len(requests.sent) != 1 {
				t.Fatalf("the write sent %d requests, want 1", len(requests.sent))
			}
			got := requests.sent[0]
			if got.method != http.MethodPatch || got.path != leases+"/kube-controller-manager" || got.contentType != "application/merge-patch+json" || got.accept != "application/json" {
				t.Errorf("the write sent %s %s as %q accepting %q, want a merge patch of the Lease accepting JSON", got.method, got.path, got.contentType, got.accept)
			}
			want := map[string]any{"metadata": map[string]any{"resourceVersion": "7"}, "spec": c.spec}
			if wantJSON, _ := json.Marshal(want); string(wantJSON) != mustMarshal(t, got.body) {
				t.Errorf("the patch is %s, want %s", mustMarshal(t, got.body), wantJSON)
			}
			if written.Metadata.ResourceVersion != "8" {
				t.Errorf("the write returned resourceVersion %q, want the one the answer carries", written.Metadata.ResourceVersion)
			}
		})
	}
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestAFirstWriteCreatesTheLeaseWhole(t *testing.T) {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	var requests recorder
	server := httptest.NewServer(&requests)
	t.Cleanup(server.Close)
	r := &NodesReconciler{Timeout: time.Second}
	tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.writeLease(t.Context(), tenant, nil, takeLease, now); err != nil {
		t.Fatal(err)
	}
	if len(requests.sent) != 1 {
		t.Fatalf("the write sent %d requests, want 1", len(requests.sent))
	}
	got, at := requests.sent[0], "2026-11-08T12:00:00.000000Z"
	if got.method != http.MethodPost || got.path != "/apis/coordination.k8s.io/v1/namespaces/kube-system/leases" || got.contentType != "application/json" {
		t.Errorf("the write sent %s %s as %q, want a POST of the Lease as JSON", got.method, got.path, got.contentType)
	}
	want := map[string]any{
		"apiVersion": "coordination.k8s.io/v1",
		"kind":       "Lease",
		"metadata":   map[string]any{"name": lodiav1alpha1.ControllerManagerLease, "namespace": lodiav1alpha1.LeaderElectionNamespace},
		"spec":       map[string]any{"holderIdentity": lodiav1alpha1.HoldHolderIdentity, "leaseDurationSeconds": 60.0, "acquireTime": at, "renewTime": at, "leaseTransitions": 1.0},
	}
	if wantJSON, _ := json.Marshal(want); string(wantJSON) != mustMarshal(t, got.body) {
		t.Errorf("the Lease created is %s, want %s", mustMarshal(t, got.body), wantJSON)
	}
}
