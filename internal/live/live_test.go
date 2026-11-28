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
			Datastore: lodiav1alpha1.DatastoreStatus{
				Endpoints: []lodiav1alpha1.HostPort{{Host: "dev-etcd-client.team-a.svc", Port: 2379}},
				Members:   lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: new(int32(1)), Ready: new(int32(1))}},
			},
			Restore: lodiav1alpha1.RestoreStatus{
				Backup:      lodiav1alpha1.UIDReference{Name: "a-1", UID: "5e1f7c3a-0000-4000-8000-000000000003"},
				Source:      lodiav1alpha1.UIDReference{Name: "a", UID: "5e1f7c3a-0000-4000-8000-000000000004"},
				CompletedAt: at,
			},
			MemberRestore: lodiav1alpha1.MemberRestoreStatus{Member: "dev-etcd-0", DetectedAt: at, From: lodiav1alpha1.MemberRestoreFromSnapshots},
			Endpoint: lodiav1alpha1.EndpointStatus{
				APIServer:    lodiav1alpha1.HostPort{Host: "api.dev.example.com", Port: 443},
				Konnectivity: lodiav1alpha1.HostPort{Host: "konnectivity.dev.example.com", Port: 443},
				InCluster:    lodiav1alpha1.HostPort{Host: lodiav1alpha1.DefaultAdvertiseAddress, Port: 6443},
				Tunnel:       lodiav1alpha1.HostPort{Host: "tunnel.dev.example.com", Port: 443},
			},
			Addons: &lodiav1alpha1.Rendered{Version: "v1.37.1"},
			Components: &lodiav1alpha1.ComponentsStatus{
				APIServer: lodiav1alpha1.ComponentStatus{
					RolloutCount: lodiav1alpha1.RolloutCount{Count: lodiav1alpha1.Count{Total: new(int32(2)), Ready: new(int32(2))}, Updated: new(int32(2))},
					Rendered:     lodiav1alpha1.Rendered{Version: "v1.37.1"},
				},
			},
			Nodes:  lodiav1alpha1.NodesStatus{Count: lodiav1alpha1.Count{Total: new(int32(3)), Ready: new(int32(2))}, OldestKubelet: "v1.34.2"},
			Health: &lodiav1alpha1.HealthStatus{ControllerManagerHold: &lodiav1alpha1.ControllerManagerHold{Since: &at, SilentNodes: new(int32(2)), CountedNodes: new(int32(3))}},
			Credentials: &lodiav1alpha1.CredentialsStatus{
				AdminKubeconfigSecretRef: lodiav1alpha1.LocalReference{Name: "dev-admin-kubeconfig"},
				CABundleSecretRef:        lodiav1alpha1.LocalReference{Name: "dev-ca-bundle"},
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

func TestDatastoreTakesTheAPIServersStatusAndReturnsItsResourceVersionApart(t *testing.T) {
	reader, server := held(t)
	stored := &lodiav1alpha1.ControlPlane{}
	if err := reader.Get(t.Context(), client.ObjectKeyFromObject(server), stored); err != nil {
		t.Fatal(err)
	}
	cp := cachedAs(server.UID)
	cp.ResourceVersion = "1"
	cp.Status.Components = &lodiav1alpha1.ComponentsStatus{APIServer: lodiav1alpha1.ComponentStatus{MemberRestoreDetectedAt: new(metav1.NewTime(at.Add(-time.Hour)))}}
	version, err := live.Datastore(t.Context(), reader, cp, lodiav1alpha1.ConditionDatastoreReady)
	if err != nil {
		t.Fatal(err)
	}
	if version != stored.ResourceVersion || cp.ResourceVersion != "1" {
		t.Errorf("the read returned resourceVersion %q and left %q on the cached copy, want the API server's %q returned and the cache's 1 left", version, cp.ResourceVersion, stored.ResourceVersion)
	}
	if !equality.Semantic.DeepEqual(cp.Status.Datastore, server.Status.Datastore) || !equality.Semantic.DeepEqual(cp.Status.Restore, server.Status.Restore) || !equality.Semantic.DeepEqual(cp.Status.MemberRestore, server.Status.MemberRestore) || !equality.Semantic.DeepEqual(cp.Status.Components, server.Status.Components) {
		t.Errorf("status.datastore %+v, status.restore %+v, status.memberRestore %+v and status.components %+v, want the API server's %+v, %+v, %+v and %+v", cp.Status.Datastore, cp.Status.Restore, cp.Status.MemberRestore, cp.Status.Components, server.Status.Datastore, server.Status.Restore, server.Status.MemberRestore, server.Status.Components)
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
	cp.Status.Components = &lodiav1alpha1.ComponentsStatus{APIServer: lodiav1alpha1.ComponentStatus{Rendered: lodiav1alpha1.Rendered{Version: "v1.36.9"}}}
	if err := live.Components(t.Context(), reader, cp, lodiav1alpha1.ConditionAPIServerReady); err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(cp.Status.Components, server.Status.Components) || !equality.Semantic.DeepEqual(cp.Status.Endpoint, server.Status.Endpoint) || !equality.Semantic.DeepEqual(cp.Status.MemberRestore, server.Status.MemberRestore) {
		t.Errorf("status.components %+v, status.endpoint %+v and status.memberRestore %+v, want the API server's %+v, %+v and %+v", cp.Status.Components, cp.Status.Endpoint, cp.Status.MemberRestore, server.Status.Components, server.Status.Endpoint, server.Status.MemberRestore)
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
	if len(cp.Status.Datastore.Endpoints) > 0 || cp.Status.Restore != (lodiav1alpha1.RestoreStatus{}) {
		t.Errorf("status.datastore %+v and status.restore %+v, want the cache's, none", cp.Status.Datastore, cp.Status.Restore)
	}
}

func TestAddonsTakesTheAPIServersStatus(t *testing.T) {
	reader, server := held(t)
	cp := cachedAs(server.UID)
	cp.Status.Addons = &lodiav1alpha1.Rendered{Version: "v1.36.9"}
	cp.Status.Components = &lodiav1alpha1.ComponentsStatus{APIServer: lodiav1alpha1.ComponentStatus{Rendered: lodiav1alpha1.Rendered{Version: "v1.36.9"}}}
	if err := live.Addons(t.Context(), reader, cp, lodiav1alpha1.ConditionAPIServerReady); err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(cp.Status.Addons, server.Status.Addons) || !equality.Semantic.DeepEqual(cp.Status.Components, server.Status.Components) {
		t.Errorf("status.addons %+v and status.components %+v, want the API server's %+v and %+v", cp.Status.Addons, cp.Status.Components, server.Status.Addons, server.Status.Components)
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
	if cp.Status.Endpoint != (lodiav1alpha1.EndpointStatus{}) {
		t.Errorf("status.endpoint %+v, want the cache's, none", cp.Status.Endpoint)
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
	if len(cp.Status.Datastore.Endpoints) > 0 || cp.Status.Endpoint != (lodiav1alpha1.EndpointStatus{}) {
		t.Errorf("status.datastore %+v and status.endpoint %+v, want the cache's, none", cp.Status.Datastore, cp.Status.Endpoint)
	}
}

func TestNodesTakesTheAPIServersStatus(t *testing.T) {
	reader, server := held(t)
	cp := cachedAs(server.UID)
	if err := live.Nodes(t.Context(), reader, cp, lodiav1alpha1.ConditionDatastoreReady); err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(cp.Status.Nodes, server.Status.Nodes) || !equality.Semantic.DeepEqual(cp.Status.Health, server.Status.Health) {
		t.Errorf("status.nodes %+v and status.health %+v, want the API server's %+v and %+v", cp.Status.Nodes, cp.Status.Health, server.Status.Nodes, server.Status.Health)
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
	if len(cp.Status.Datastore.Endpoints) > 0 || cp.Status.Endpoint != (lodiav1alpha1.EndpointStatus{}) {
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
			_, err := live.Datastore(t.Context(), reader, cp, lodiav1alpha1.ConditionDatastoreReady)
			return err
		}},
		{"Components", func(reader client.Reader, cp *lodiav1alpha1.ControlPlane) error {
			return live.Components(t.Context(), reader, cp, lodiav1alpha1.ConditionAPIServerReady)
		}},
		{"Addons", func(reader client.Reader, cp *lodiav1alpha1.ControlPlane) error {
			return live.Addons(t.Context(), reader, cp, lodiav1alpha1.ConditionAPIServerReady)
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
