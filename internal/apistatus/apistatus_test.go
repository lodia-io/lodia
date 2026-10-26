// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package apistatus_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
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
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := apistatus.Refused(row.err); got != row.want {
				t.Errorf("Refused(%v) = %t, want %t", row.err, got, row.want)
			}
		})
	}
}
