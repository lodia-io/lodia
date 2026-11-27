// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package apistatus_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"go.lodia.io/lodia/internal/apistatus"
)

func TestRefused(t *testing.T) {
	etcds := schema.GroupResource{Group: "druid.gardener.cloud", Resource: "etcds"}
	bindings := schema.GroupResource{Group: "rbac.authorization.k8s.io", Resource: "clusterrolebindings"}
	invalid := apierrors.NewInvalid(schema.GroupKind{Group: etcds.Group, Kind: "Etcd"}, "dev-etcd",
		field.ErrorList{field.Invalid(field.NewPath("spec", "etcd", "defragmentationSchedule"), "0 3 * * 0", "does not match")})
	terminating := apierrors.NewForbidden(etcds, "dev-etcd", errors.New("unable to create new content in namespace team-a because it is being terminated"))
	terminating.ErrStatus.Details.Causes = []metav1.StatusCause{{Type: corev1.NamespaceTerminatingCause}}
	for _, row := range []struct {
		name string
		err  error
		want bool
	}{
		{"invalid", invalid, true},
		{"wrapped", fmt.Errorf("apply Etcd dev-etcd: %w", invalid), true},
		{"a webhook's denial without a code", apierrors.NewBadRequest("admission webhook denied the request"), true},
		{"forbidden by a policy", apierrors.NewForbidden(etcds, "dev-etcd", errors.New("denied by a policy")), true},
		{"forbidden to lodia:operator", apierrors.NewForbidden(bindings, "lodia:operator", errors.New("denied")), true},
		{"unauthorized", apierrors.NewUnauthorized("denied by a policy"), true},
		{"too large", apierrors.NewRequestEntityTooLargeError("limit is 3145728"), true},
		{"not found", apierrors.NewNotFound(etcds, "dev-etcd"), false},
		{"request timeout", apierrors.NewGenericServerResponse(http.StatusRequestTimeout, "PATCH", etcds, "dev-etcd", "", 0, true), false},
		{"conflict", apierrors.NewConflict(etcds, "dev-etcd", errors.New("the object has been modified")), false},
		{"throttled", apierrors.NewTooManyRequests("slow down", 1), false},
		{"a namespace being deleted", terminating, false},
		{"internal", apierrors.NewInternalError(errors.New(".spec.foo: field not declared in schema")), false},
		{"service unavailable", apierrors.NewServiceUnavailable("starting"), false},
		{"gateway timeout", apierrors.NewTimeoutError("the server was unable to return a response in the time allotted", 1), false},
		{"no answer", &url.Error{Op: "Post", URL: "https://dev-kube-apiserver:443", Err: errors.New("connection refused")}, false},
		{"a webhook that cannot be called", uncallable("probe.lodia.io"), true},
		{"a webhook that cannot be called, with no further details", apierrors.NewInternalError(errors.New(`failed calling webhook "probe.lodia.io"; no further details available`)), true},
		{"a webhook's denial with 500", denial(http.StatusInternalServerError, "probe.lodia.io"), true},
		{"a webhook's denial with 503", denial(http.StatusServiceUnavailable, "probe.lodia.io"), true},
		{"an internal error that names no webhook", apierrors.NewInternalError(errors.New("etcdserver: request timed out")), false},
		{"the webhook's text past the start of a 500", apierrors.NewInternalError(errors.New(`apply: failed calling webhook "probe.lodia.io": connection refused`)), false},
		{"the webhook's text past the start of a 503", apierrors.NewServiceUnavailable(`retry: admission webhook "probe.lodia.io" denied the request: no`), false},
		{"a 500 that names a webhook that is no DNS subdomain", uncallable("Probe_Lodia"), false},
		{"admission not ready yet", apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "probe", errors.New("not yet ready to handle request")), false},
		{"admission not ready yet, nameless", apierrors.NewForbidden(schema.GroupResource{}, "", errors.New("not yet ready to handle request")), false},
		{"a webhook's denial with 403 that reads not ready", denialAs(http.StatusForbidden, "probe.lodia.io", "not yet ready to handle request"), true},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := apistatus.Refused(row.err); got != row.want {
				t.Errorf("Refused(%v) = %t, want %t", row.err, got, row.want)
			}
		})
	}
}

func TestWebhook(t *testing.T) {
	long := strings.Repeat("a", 250) + ".io"
	for _, row := range []struct {
		name   string
		err    error
		want   string
		denied bool
	}{
		{"cannot be called", uncallable("probe.lodia.io"), "probe.lodia.io", false},
		{"cannot be called, wrapped", fmt.Errorf("apply ConfigMap x: %w", uncallable("probe.lodia.io")), "probe.lodia.io", false},
		{"denies without a code", denial(http.StatusBadRequest, "probe.lodia.io"), "probe.lodia.io", true},
		{"denies with 500", denial(http.StatusInternalServerError, "probe.lodia.io"), "probe.lodia.io", true},
		{"denies without an explanation", &apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: http.StatusBadRequest, Message: `admission webhook "probe.lodia.io" denied the request without explanation`}}, "probe.lodia.io", true},
		{"a name of 253 bytes", uncallable(long), long, false},
		{"a name past 253 bytes", uncallable("b" + long), "", false},
		{"a name that is no DNS subdomain", denial(http.StatusBadRequest, "Probe.Lodia.io"), "", false},
		{"a 500 that is no webhook's", apierrors.NewInternalError(errors.New("etcdserver: request timed out")), "", false},
		{"a denial's text with a code below 400", &apierrors.StatusError{ErrStatus: metav1.Status{Code: http.StatusOK, Message: `admission webhook "probe.lodia.io" denied the request: no`}}, "", false},
		{"the uncalled text on another code", &apierrors.StatusError{ErrStatus: metav1.Status{Code: http.StatusBadGateway, Message: `Internal error occurred: failed calling webhook "probe.lodia.io": no`}}, "", false},
		{"no answer", &url.Error{Op: "Post", URL: "https://dev-kube-apiserver:443", Err: errors.New("connection refused")}, "", false},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got, denied := apistatus.Webhook(row.err); got != row.want || denied != row.denied {
				t.Errorf("Webhook(%v) = %q, %t, want %q, %t", row.err, got, denied, row.want, row.denied)
			}
		})
	}
}

func uncallable(name string) error {
	return apierrors.NewInternalError(fmt.Errorf("failed calling webhook %q: failed to call webhook: Post \"https://127.0.0.1:1/validate?timeout=2s\": dial tcp 127.0.0.1:1: connect: connection refused", name))
}

func denial(code int32, name string) error {
	return denialAs(code, name, "kept out by the tenant's policy")
}

func denialAs(code int32, name, message string) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: code, Message: fmt.Sprintf("admission webhook %q denied the request: %s", name, message)}}
}
