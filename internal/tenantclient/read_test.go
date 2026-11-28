// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantclient_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/tenantclient"
)

// A webhook's denial with 500 and a message of 2 KiB keeps the first 1,024 bytes of the message, and is still the webhook's verdict.
func TestALongDenialWithA5xxIsStillTheWebhooksVerdict(t *testing.T) {
	denial := `admission webhook "probe.lodia.io" denied the request: ` + strings.Repeat("é", 1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: http.StatusInternalServerError, Message: denial})
	}))
	t.Cleanup(server.Close)
	c, err := tenantclient.New(&rest.Config{Host: server.URL}, tenantclient.Options{Connections: 1, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Do(t.Context(), c.Get().AbsPath("/api/v1/namespaces/kube-system/configmaps/x"))
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		t.Fatalf("the denial returned %v, want a Status", err)
	}
	if message := status.Status().Message; len(message) > tenantclient.MaxMessage || len(message) < tenantclient.MaxMessage-utf8.UTFMax || !utf8.ValidString(message) || !strings.HasPrefix(denial, message) {
		t.Errorf("the Status says %d bytes, want the denial's first %d cut on a rune", len(message), tenantclient.MaxMessage)
	}
	if name, denied := apistatus.Webhook(err); !apistatus.Refused(err) || name != "probe.lodia.io" || !denied {
		t.Errorf("the denial is refused %t by webhook %q, denied %t", apistatus.Refused(err), name, denied)
	}
}
