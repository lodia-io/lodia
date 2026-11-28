// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package testenv

import (
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	admissionv1 "k8s.io/api/admission/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Webhook is the name of the webhook RefusingWebhook configures.
const Webhook = "probe.lodia.io"

// Uncallable is a webhook at an address that refuses every connection.
func Uncallable() Hook {
	return Hook{URL: "https://127.0.0.1:1/validate"}
}

// Hook is where a webhook is called, and, for one Denying serves, how many
// of the reviews it denied were of a dry run.
type Hook struct {
	URL      string
	CABundle []byte
	DryRuns  *atomic.Int32
}

// Denying serves a webhook over TLS that denies every review with code.
func Denying(t *testing.T, code int32) Hook {
	t.Helper()
	dryRuns := new(atomic.Int32)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		review := &admissionv1.AdmissionReview{}
		if body, err := io.ReadAll(r.Body); err != nil || json.Unmarshal(body, review) != nil || review.Request == nil {
			http.Error(w, "no review", http.StatusBadRequest)
			return
		}
		if review.Request.DryRun != nil && *review.Request.DryRun {
			dryRuns.Add(1)
		}
		review.Response = &admissionv1.AdmissionResponse{UID: review.Request.UID, Result: &metav1.Status{Code: code, Message: "kept out by the tenant's policy"}}
		review.Request = nil
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(review)
	}))
	t.Cleanup(srv.Close)
	return Hook{URL: srv.URL + "/validate", CABundle: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), DryRuns: dryRuns}
}

// RefusingWebhook is a configuration whose webhook Webhook fails closed at
// hook on the writes rule matches, in the namespaces namespaces selects, all
// of them when it is nil, under conditions.
func RefusingWebhook(hook Hook, rule admissionregistrationv1.RuleWithOperations, namespaces *metav1.LabelSelector, conditions ...admissionregistrationv1.MatchCondition) *admissionregistrationv1.ValidatingWebhookConfiguration {
	fail, none, timeout := admissionregistrationv1.Fail, admissionregistrationv1.SideEffectClassNone, int32(2)
	return &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "probe-"},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name:                    Webhook,
			ClientConfig:            admissionregistrationv1.WebhookClientConfig{URL: &hook.URL, CABundle: hook.CABundle},
			Rules:                   []admissionregistrationv1.RuleWithOperations{rule},
			NamespaceSelector:       namespaces,
			MatchConditions:         conditions,
			FailurePolicy:           &fail,
			SideEffects:             &none,
			AdmissionReviewVersions: []string{"v1"},
			TimeoutSeconds:          &timeout,
		}},
	}
}

// StoreWithSideEffects stores configuration, as the API server holds it, in
// the etcd at endpoint with every webhook's sideEffects set to sideEffects,
// past the validation that takes None or NoneOnDryRun alone from a
// configuration that has no other
// (k8s.io/kubernetes@v1.37.3 pkg/apis/admissionregistration/validation/validation.go#L219-L228,
// #L728-L738): as one stored before that rule keeps any other.
func StoreWithSideEffects(t *testing.T, endpoint string, configuration *admissionregistrationv1.ValidatingWebhookConfiguration, sideEffects admissionregistrationv1.SideEffectClass) {
	t.Helper()
	stored := configuration.DeepCopy()
	stored.APIVersion, stored.Kind, stored.ResourceVersion = admissionregistrationv1.SchemeGroupVersion.String(), "ValidatingWebhookConfiguration", ""
	for i := range stored.Webhooks {
		stored.Webhooks[i].SideEffects = &sideEffects
	}
	data, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = etcd.Close() }()
	if _, err := etcd.Put(t.Context(), "/registry/validatingwebhookconfigurations/"+stored.Name, string(data)); err != nil {
		t.Fatal(err)
	}
}
