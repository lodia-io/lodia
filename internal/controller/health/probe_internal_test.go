// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/tenantclient"
)

func TestAProbeCutsTheHeadOfItsAnswerAtTheBoundOfTheTenantClients(t *testing.T) {
	for _, c := range []struct {
		name    string
		padding int
		code    int
	}{
		{"a head under the bound", tenantclient.MaxResponseHeaderBytes - 1024, http.StatusOK},
		{"a head over it", tenantclient.MaxResponseHeaderBytes + 1024, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Padding", strings.Repeat("a", c.padding))
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(server.Close)
			bundle := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			r := &Reconciler{Timeout: 5 * time.Second}
			p := r.prober(bundle)
			code, err := r.probe(t.Context(), p.direct, server.URL+"/readyz")
			if code != c.code || (c.code == 0) != (err != nil && strings.Contains(err.Error(), "headers exceeded")) {
				t.Errorf("the probe answered %d, %v, want %d", code, err, c.code)
			}
		})
	}
}

func TestEveryProbeTransportCutsAHeadAtTheBoundOfTheTenantClients(t *testing.T) {
	server := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	r := &Reconciler{Timeout: 5 * time.Second}
	p := r.prober(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
	for name, client := range map[string]*http.Client{"direct": p.direct, "through the tunnel": r.throughTunnel(&prober{roots: p.roots, client: &server.TLS.Certificates[0]}, lodiav1alpha1.HostPort{Host: "tunnel.example.com", Port: 443})} {
		if got := client.Transport.(*http.Transport).MaxResponseHeaderBytes; got != tenantclient.MaxResponseHeaderBytes {
			t.Errorf("the probe %s reads a head of up to %d bytes, want %d", name, got, tenantclient.MaxResponseHeaderBytes)
		}
	}
}
