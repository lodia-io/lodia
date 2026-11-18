// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package statusapply_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/statusapply"
)

type request struct {
	method, path, contentType string
	query                     url.Values
	body                      []byte
}

func bodyScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

// sent is the request write sends through a client of a server that answers
// with obj at resourceVersion 42.
func sent(t *testing.T, obj client.Object, write func(c client.Client) error) request {
	t.Helper()
	scheme := bodyScheme(t)
	gvk, err := apiutil.GVKForObject(obj, scheme)
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		requests <- request{method: r.Method, path: r.URL.Path, contentType: r.Header.Get("Content-Type"), query: r.URL.Query(), body: body}
		stored := obj.DeepCopyObject().(client.Object)
		stored.GetObjectKind().SetGroupVersionKind(gvk)
		stored.SetResourceVersion("42")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stored); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(gvk, meta.RESTScopeNamespace)
	c, err := client.New(&rest.Config{Host: server.URL}, client.Options{Scheme: scheme, Mapper: mapper})
	if err != nil {
		t.Fatal(err)
	}
	if err := write(c); err != nil {
		t.Fatal(err)
	}
	got := <-requests
	resource := strings.ToLower(gvk.Kind) + "s"
	if path := "/apis/lodia.io/v1alpha1/namespaces/" + obj.GetNamespace() + "/" + resource + "/" + obj.GetName() + "/status"; got.method != http.MethodPatch || got.path != path || got.contentType != "application/apply-patch+yaml" {
		t.Errorf("%s %s as %s, want PATCH %s as application/apply-patch+yaml", got.method, got.path, got.contentType, path)
	}
	if want := (url.Values{"fieldManager": {manager}, "force": {"true"}}); !reflect.DeepEqual(got.query, want) {
		t.Errorf("query %v, want %v", got.query, want)
	}
	return got
}

func bodyIs(t *testing.T, got request, golden string) {
	t.Helper()
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v; the body sent is %s", err, got.body)
	}
	var sentBody, wantBody any
	if err := json.Unmarshal(got.body, &sentBody); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(want, &wantBody); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sentBody, wantBody) {
		t.Errorf("the body sent is %s, want %s", got.body, golden)
	}
}

func TestTheRequestOfAStatusApplyIsTheOneInTestdata(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a", UID: "5e1f7c3a-0000-4000-8000-000000000001", Generation: 3, ResourceVersion: "41"}}
	var version string
	got := sent(t, cp, func(c client.Client) error {
		w := statusapply.Writer{Client: c, Reader: fake.NewClientBuilder().WithScheme(bodyScheme(t)).WithObjects(cp.DeepCopy()).Build(), Manager: manager}
		var err error
		version, err = w.ApplyVersion(t.Context(), cp, lodiav1alpha1.ControlPlaneStatus{
			Conditions: []metav1.Condition{
				{Type: lodiav1alpha1.ConditionCertificatesReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonIssued, Message: "issued", ObservedGeneration: 3, LastTransitionTime: earlier},
				{Type: lodiav1alpha1.ConditionAccessReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForAPIServer, Message: "", ObservedGeneration: 2, LastTransitionTime: later},
			},
			Components: &lodiav1alpha1.ComponentsStatus{APIServer: &lodiav1alpha1.ComponentStatus{Rendered: lodiav1alpha1.Rendered{Version: "v1.35.0"}}},
			Credentials: &lodiav1alpha1.CredentialsStatus{
				AdminKubeconfigSecretRef: &lodiav1alpha1.LocalReference{Name: "dev-admin-kubeconfig"},
				CABundleSecretRef:        &lodiav1alpha1.LocalReference{Name: "dev-ca-bundle"},
			},
		}, true)
		return err
	})
	if version != "42" {
		t.Errorf("resourceVersion %q, want the response's 42", version)
	}
	bodyIs(t, got, "testdata/status-apply.json")
}

func TestTheRequestOfALockedApplyIsTheOneInTestdata(t *testing.T) {
	ready := metav1.Condition{Type: lodiav1alpha1.ConditionReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, Message: "ready", ObservedGeneration: 1, LastTransitionTime: earlier}
	request := &lodiav1alpha1.KubeconfigRequest{ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "team-a", ResourceVersion: "41"}}
	got := sent(t, request, func(c client.Client) error {
		return statusapply.Writer{Client: c, Manager: manager}.ApplyLocked(t.Context(), "KubeconfigRequest", request, nil, map[string]any{
			"kubeconfig":          "a2luZDogQ29uZmlnCg==",
			"expirationTimestamp": time.Date(2026, 11, 4, 18, 0, 0, 0, time.UTC).Format(time.RFC3339),
		}, ready)
	})
	bodyIs(t, got, "testdata/locked-kubeconfigrequest.json")

	backup := &lodiav1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "dev-1", Namespace: "team-a", ResourceVersion: "41"}}
	status, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&lodiav1alpha1.BackupStatus{
		Source:            &lodiav1alpha1.UIDReference{Name: "dev", UID: "5e1f7c3a-0000-4000-8000-000000000001"},
		KubernetesVersion: "v1.35.0",
		Seal:              &lodiav1alpha1.BackupSeal{QuotaBackendBytes: new(resource.MustParse("8Gi"))},
		StartedAt:         &earlier,
	})
	if err != nil {
		t.Fatal(err)
	}
	got = sent(t, backup, func(c client.Client) error {
		return statusapply.Writer{Client: c, Manager: manager}.ApplyLocked(t.Context(), "Backup", backup, nil, status, ready)
	})
	bodyIs(t, got, "testdata/locked-backup.json")
}
