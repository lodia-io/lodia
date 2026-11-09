// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package live_test

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/live"
)

var at = metav1.Date(2026, 11, 2, 18, 0, 0, 0, time.UTC)

func held(t *testing.T) (client.Reader, *lodiav1alpha1.ControlPlane) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	server := &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a", UID: "5e1f7c3a-0000-4000-8000-000000000002"},
		Status: lodiav1alpha1.ControlPlaneStatus{
			Conditions: []metav1.Condition{
				{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, Message: "the API server's", LastTransitionTime: at},
				{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, Message: "the API server's", LastTransitionTime: at},
			},
			Datastore: &lodiav1alpha1.DatastoreStatus{
				Endpoints: []lodiav1alpha1.HostPort{{Host: "dev-etcd-client.team-a.svc", Port: 2379}},
				Members:   lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: 1, Ready: 1}},
			},
			Restore: &lodiav1alpha1.RestoreStatus{
				Backup:      lodiav1alpha1.UIDReference{Name: "a-1", UID: "5e1f7c3a-0000-4000-8000-000000000003"},
				Source:      lodiav1alpha1.UIDReference{Name: "a", UID: "5e1f7c3a-0000-4000-8000-000000000004"},
				CompletedAt: at,
			},
			Endpoint: &lodiav1alpha1.EndpointStatus{
				APIServer:    lodiav1alpha1.HostPort{Host: "api.dev.example.com", Port: 443},
				Konnectivity: lodiav1alpha1.HostPort{Host: "konnectivity.dev.example.com", Port: 443},
				InCluster:    lodiav1alpha1.HostPort{Host: lodiav1alpha1.DefaultAdvertiseAddress, Port: 6443},
				Tunnel:       &lodiav1alpha1.HostPort{Host: "tunnel.dev.example.com", Port: 443},
			},
			Components: &lodiav1alpha1.ComponentsStatus{
				APIServer: &lodiav1alpha1.ComponentStatus{
					RolloutCount: lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: 2, Ready: 2}, Updated: new(int32(2))},
					Rendered:     lodiav1alpha1.Rendered{Version: "v1.37.1"},
				},
			},
			Nodes: &lodiav1alpha1.NodesStatus{Count: lodiav1alpha1.Count{Total: 3, Ready: 2}, OldestKubelet: "v1.34.2"},
			Credentials: &lodiav1alpha1.CredentialsStatus{
				AdminKubeconfigSecretRef: &lodiav1alpha1.LocalReference{Name: "dev-admin-kubeconfig"},
				CABundleSecretRef:        &lodiav1alpha1.LocalReference{Name: "dev-ca-bundle"},
			},
		},
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(server.DeepCopy()).Build(), server
}

func cachedAs(uid types.UID) *lodiav1alpha1.ControlPlane {
	return &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a", UID: uid},
		Status: lodiav1alpha1.ControlPlaneStatus{Conditions: []metav1.Condition{
			{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRestoring, Message: "the cache's", LastTransitionTime: at},
			{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForDatastore, Message: "the cache's", LastTransitionTime: at},
		}},
	}
}

func TestDatastoreTakesTheAPIServersStatus(t *testing.T) {
	reader, server := held(t)
	cp := cachedAs(server.UID)
	if err := live.Datastore(t.Context(), reader, cp, lodiav1alpha1.ConditionDatastoreReady); err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(cp.Status.Datastore, server.Status.Datastore) || !equality.Semantic.DeepEqual(cp.Status.Restore, server.Status.Restore) {
		t.Errorf("status.datastore %+v and status.restore %+v, want the API server's %+v and %+v", cp.Status.Datastore, cp.Status.Restore, server.Status.Datastore, server.Status.Restore)
	}
	for _, c := range []struct {
		conditionType string
		from          *lodiav1alpha1.ControlPlane
	}{{lodiav1alpha1.ConditionDatastoreReady, server}, {lodiav1alpha1.ConditionAPIServerReady, cachedAs(server.UID)}} {
		got, want := meta.FindStatusCondition(cp.Status.Conditions, c.conditionType), meta.FindStatusCondition(c.from.Status.Conditions, c.conditionType)
		if !equality.Semantic.DeepEqual(got, want) {
			t.Errorf("%s is %+v, want %+v", c.conditionType, got, want)
		}
	}
}

func TestComponentsTakesTheAPIServersStatus(t *testing.T) {
	reader, server := held(t)
	cp := cachedAs(server.UID)
	cp.Status.Components = &lodiav1alpha1.ComponentsStatus{APIServer: &lodiav1alpha1.ComponentStatus{Rendered: lodiav1alpha1.Rendered{Version: "v1.36.9"}}}
	if err := live.Components(t.Context(), reader, cp, lodiav1alpha1.ConditionAPIServerReady); err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(cp.Status.Components, server.Status.Components) || !equality.Semantic.DeepEqual(cp.Status.Endpoint, server.Status.Endpoint) {
		t.Errorf("status.components %+v and status.endpoint %+v, want the API server's %+v and %+v", cp.Status.Components, cp.Status.Endpoint, server.Status.Components, server.Status.Endpoint)
	}
	for _, c := range []struct {
		conditionType string
		from          *lodiav1alpha1.ControlPlane
	}{{lodiav1alpha1.ConditionAPIServerReady, server}, {lodiav1alpha1.ConditionDatastoreReady, cachedAs(server.UID)}} {
		got, want := meta.FindStatusCondition(cp.Status.Conditions, c.conditionType), meta.FindStatusCondition(c.from.Status.Conditions, c.conditionType)
		if !equality.Semantic.DeepEqual(got, want) {
			t.Errorf("%s is %+v, want %+v", c.conditionType, got, want)
		}
	}
	if cp.Status.Datastore != nil || cp.Status.Restore != nil {
		t.Errorf("status.datastore %+v and status.restore %+v, want the cache's, none", cp.Status.Datastore, cp.Status.Restore)
	}
}

func TestCredentialsTakesTheAPIServersStatus(t *testing.T) {
	reader, server := held(t)
	cp := cachedAs(server.UID)
	if err := live.Credentials(t.Context(), reader, cp, lodiav1alpha1.ConditionDatastoreReady); err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(cp.Status.Credentials, server.Status.Credentials) {
		t.Errorf("status.credentials %+v, want the API server's %+v", cp.Status.Credentials, server.Status.Credentials)
	}
	for _, c := range []struct {
		conditionType string
		from          *lodiav1alpha1.ControlPlane
	}{{lodiav1alpha1.ConditionDatastoreReady, server}, {lodiav1alpha1.ConditionAPIServerReady, cachedAs(server.UID)}} {
		got, want := meta.FindStatusCondition(cp.Status.Conditions, c.conditionType), meta.FindStatusCondition(c.from.Status.Conditions, c.conditionType)
		if !equality.Semantic.DeepEqual(got, want) {
			t.Errorf("%s is %+v, want %+v", c.conditionType, got, want)
		}
	}
	if cp.Status.Datastore != nil || cp.Status.Endpoint != nil {
		t.Errorf("status.datastore %+v and status.endpoint %+v, want the cache's, none", cp.Status.Datastore, cp.Status.Endpoint)
	}
}

func TestNodesTakesTheAPIServersStatus(t *testing.T) {
	reader, server := held(t)
	cp := cachedAs(server.UID)
	if err := live.Nodes(t.Context(), reader, cp, lodiav1alpha1.ConditionDatastoreReady); err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(cp.Status.Nodes, server.Status.Nodes) {
		t.Errorf("status.nodes %+v, want the API server's %+v", cp.Status.Nodes, server.Status.Nodes)
	}
	for _, c := range []struct {
		conditionType string
		from          *lodiav1alpha1.ControlPlane
	}{{lodiav1alpha1.ConditionDatastoreReady, server}, {lodiav1alpha1.ConditionAPIServerReady, cachedAs(server.UID)}} {
		got, want := meta.FindStatusCondition(cp.Status.Conditions, c.conditionType), meta.FindStatusCondition(c.from.Status.Conditions, c.conditionType)
		if !equality.Semantic.DeepEqual(got, want) {
			t.Errorf("%s is %+v, want %+v", c.conditionType, got, want)
		}
	}
	if cp.Status.Datastore != nil || cp.Status.Endpoint != nil {
		t.Errorf("status.datastore %+v and status.endpoint %+v, want the cache's, none", cp.Status.Datastore, cp.Status.Endpoint)
	}
}

func TestAReadOfAnotherObjectFails(t *testing.T) {
	for _, c := range []struct {
		name string
		read func(client.Reader, *lodiav1alpha1.ControlPlane) error
	}{
		{"Conditions", func(reader client.Reader, cp *lodiav1alpha1.ControlPlane) error {
			return live.Conditions(t.Context(), reader, cp, lodiav1alpha1.ConditionDatastoreReady)
		}},
		{"Datastore", func(reader client.Reader, cp *lodiav1alpha1.ControlPlane) error {
			return live.Datastore(t.Context(), reader, cp, lodiav1alpha1.ConditionDatastoreReady)
		}},
		{"Components", func(reader client.Reader, cp *lodiav1alpha1.ControlPlane) error {
			return live.Components(t.Context(), reader, cp, lodiav1alpha1.ConditionAPIServerReady)
		}},
		{"Nodes", func(reader client.Reader, cp *lodiav1alpha1.ControlPlane) error {
			return live.Nodes(t.Context(), reader, cp, lodiav1alpha1.ConditionControllerManagerHeld)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			reader, _ := held(t)
			predecessor := cachedAs("5e1f7c3a-0000-4000-8000-0000000000ff")
			before := predecessor.DeepCopy()
			if err := c.read(reader, predecessor); err == nil {
				t.Error("a read of a ControlPlane with another UID succeeded")
			}
			if !equality.Semantic.DeepEqual(predecessor, before) {
				t.Errorf("a read of a ControlPlane with another UID left %+v, want %+v", predecessor.Status, before.Status)
			}
		})
	}
}
