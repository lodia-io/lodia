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
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/statusapply"
)

type request struct {
	method, path, contentType string
	query                     url.Values
	body                      []byte
}

func TestTheRequestOfAStatusApplyIsTheOneInTestdata(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := lodiav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a", UID: "5e1f7c3a-0000-4000-8000-000000000001", Generation: 3, ResourceVersion: "41"}}
	sent := make(chan request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		sent <- request{method: r.Method, path: r.URL.Path, contentType: r.Header.Get("Content-Type"), query: r.URL.Query(), body: body}
		stored := cp.DeepCopy()
		stored.APIVersion, stored.Kind, stored.ResourceVersion = lodiav1alpha1.GroupVersion.String(), "ControlPlane", "42"
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stored); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	mapper := meta.NewDefaultRESTMapper(nil)
	mapper.Add(lodiav1alpha1.GroupVersion.WithKind("ControlPlane"), meta.RESTScopeNamespace)
	c, err := client.New(&rest.Config{Host: server.URL}, client.Options{Scheme: scheme, Mapper: mapper})
	if err != nil {
		t.Fatal(err)
	}
	w := statusapply.Writer{Client: c, Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cp.DeepCopy()).Build(), Manager: manager}
	version, err := w.ApplyVersion(t.Context(), cp, lodiav1alpha1.ControlPlaneStatus{
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
	if err != nil {
		t.Fatal(err)
	}
	if version != "42" {
		t.Errorf("resourceVersion %q, want the response's 42", version)
	}
	got := <-sent
	if got.method != http.MethodPatch || got.path != "/apis/lodia.io/v1alpha1/namespaces/team-a/controlplanes/dev/status" || got.contentType != "application/apply-patch+yaml" {
		t.Errorf("%s %s as %s, want PATCH of dev's status as application/apply-patch+yaml", got.method, got.path, got.contentType)
	}
	if want := (url.Values{"fieldManager": {manager}, "force": {"true"}}); !reflect.DeepEqual(got.query, want) {
		t.Errorf("query %v, want %v", got.query, want)
	}
	golden, err := os.ReadFile("testdata/status-apply.json")
	if err != nil {
		t.Fatalf("%v; the body sent is %s", err, got.body)
	}
	var sentBody, wantBody any
	if err := json.Unmarshal(got.body, &sentBody); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(golden, &wantBody); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sentBody, wantBody) {
		t.Errorf("the body sent is %s, want testdata/status-apply.json", got.body)
	}
}
