// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package apistatus_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.lodia.io/lodia/internal/apistatus"
)

func unsupportedBy(name string) error {
	return apierrors.NewBadRequest(fmt.Sprintf("admission webhook %q does not support dry run", name))
}

// DryRunUnsupported names the webhook of the API server's answer to a dry run that webhook cannot take, and of nothing else, and that answer is no verdict.
func TestDryRunUnsupported(t *testing.T) {
	for _, row := range []struct {
		name string
		err  error
		want string
	}{
		{"the API server's answer", unsupportedBy("probe.lodia.io"), "probe.lodia.io"},
		{"wrapped", fmt.Errorf("apply ConfigMap x: %w", unsupportedBy("probe.lodia.io")), "probe.lodia.io"},
		{"on another code", &apierrors.StatusError{ErrStatus: metav1.Status{Code: http.StatusForbidden, Message: `admission webhook "probe.lodia.io" does not support dry run`}}, ""},
		{"with text after it", apierrors.NewBadRequest(`admission webhook "probe.lodia.io" does not support dry run: no`), ""},
		{"a webhook's denial", &apierrors.StatusError{ErrStatus: metav1.Status{Code: http.StatusBadRequest, Message: `admission webhook "probe.lodia.io" denied the request: no`}}, ""},
		{"a denial whose message reads unsupported", apierrors.NewBadRequest(`admission webhook "probe.lodia.io" denied the request: admission webhook "probe.lodia.io" does not support dry run`), ""},
		{"a name that is no DNS subdomain", unsupportedBy("Probe_Lodia"), ""},
		{"no answer", &url.Error{Op: "Patch", URL: "https://dev-kube-apiserver:443", Err: errors.New("connection refused")}, ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := apistatus.DryRunUnsupported(row.err); got != row.want {
				t.Errorf("DryRunUnsupported(%v) = %q, want %q", row.err, got, row.want)
			}
			if row.want != "" && apistatus.Refused(row.err) {
				t.Errorf("Refused(%v) is true for an answer that is no verdict", row.err)
			}
		})
	}
}
