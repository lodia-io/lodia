// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// A read of a refused binding that gets no answer keeps AccessReady as it was; one the tenant answers, every binding held, one missing or its read refused, decides it.
func TestTheReadOfARefusedBindingDecidesOnlyOnAnAnswer(t *testing.T) {
	kept := metav1.Condition{Type: lodiav1alpha1.ConditionAccessReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonBound, Message: "lodia:operator, lodia:cluster-admins and lodia:kubelet-api-admins are bound", ObservedGeneration: 1}
	const refused = "ClusterRoleBinding lodia:operator is refused by the tenant's API server; ClusterRoleBinding lodia:kubelet-api-admins is refused by the tenant's API server; the reason is in Lodia's log"
	status := func(code int, reason metav1.StatusReason) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: int32(code), Reason: reason, Message: "the test says so"})
		}
	}
	held := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ClusterRoleBinding","apiVersion":"rbac.authorization.k8s.io/v1","metadata":{"name":"refused"}}`))
	}
	unanswered := func(w http.ResponseWriter) {
		if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
			_ = conn.Close()
		}
	}
	missing := metav1.Condition{Type: kept.Type, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRefused, Message: refused}
	for _, c := range []struct {
		name             string
		operator, second func(http.ResponseWriter)
		want             metav1.Condition
	}{
		{"no answer", unanswered, held, kept},
		{"missing", status(http.StatusNotFound, metav1.StatusReasonNotFound), held, missing},
		{"the read refused", status(http.StatusForbidden, metav1.StatusReasonForbidden), held, missing},
		{"the second missing", held, status(http.StatusNotFound, metav1.StatusReasonNotFound), missing},
		{"every one held", held, held, metav1.Condition{Type: kept.Type, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonRefused, Message: kept.Message + "; " + refused, ObservedGeneration: 1}},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				refusedOne := strings.HasSuffix(r.URL.Path, "/lodia:operator") || strings.HasSuffix(r.URL.Path, "/lodia:kubelet-api-admins")
				switch {
				case r.Method == http.MethodPatch && refusedOne:
					status(http.StatusUnprocessableEntity, metav1.StatusReasonInvalid)(w)
				case r.Method == http.MethodPatch:
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{}`))
				case strings.HasSuffix(r.URL.Path, "/lodia:operator"):
					c.operator(w)
				default:
					c.second(w)
				}
			}))
			t.Cleanup(server.Close)
			cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "dev", Generation: 2}}
			cp.Status.Conditions = []metav1.Condition{kept, {Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, ObservedGeneration: 2}}
			kubeconfig := lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name)
			tn := &tenant{cp: cp, now: time.Now(), existing: map[string]*corev1.Secret{kubeconfig: {Data: map[string][]byte{lodiav1alpha1.KubeconfigKey: kubeconfigFor(t, server.URL)}}}}
			got, err := (&Reconciler{}).access(t.Context(), tn)
			if err == nil {
				t.Error("a pass whose bindings the tenant refused returned no error")
			}
			if !equality.Semantic.DeepEqual(got, c.want) {
				t.Errorf("AccessReady is %+v, want %+v", got, c.want)
			}
		})
	}
}

func kubeconfigFor(t *testing.T, server string) []byte {
	t.Helper()
	kubeconfig, err := clientcmd.Write(clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"tenant": {Server: server}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"lodia": {}},
		Contexts:       map[string]*clientcmdapi.Context{"tenant": {Cluster: "tenant", AuthInfo: "lodia"}},
		CurrentContext: "tenant",
	})
	if err != nil {
		t.Fatal(err)
	}
	return kubeconfig
}
