// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"errors"
	"fmt"
	"net/url"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestARefusalIsTheTenantsAnswer(t *testing.T) {
	bindings := schema.GroupResource{Group: "rbac.authorization.k8s.io", Resource: "clusterrolebindings"}
	for _, c := range []struct {
		err     error
		refused bool
	}{
		{apierrors.NewForbidden(bindings, "brygga:operator", errors.New("denied")), true},
		{apierrors.NewBadRequest("an admission webhook denied the request"), true},
		{apierrors.NewInvalid(schema.GroupKind{Group: "rbac.authorization.k8s.io", Kind: "ClusterRoleBinding"}, "brygga:operator", nil), true},
		{fmt.Errorf("apply ClusterRoleBinding brygga:operator: %w", apierrors.NewForbidden(bindings, "brygga:operator", errors.New("denied"))), true},
		{apierrors.NewConflict(bindings, "brygga:operator", errors.New("conflict")), false},
		{apierrors.NewTooManyRequests("slow down", 1), false},
		{apierrors.NewTimeoutError("timed out", 1), false},
		{apierrors.NewInternalError(errors.New("etcd")), false},
		{apierrors.NewServiceUnavailable("starting"), false},
		{&url.Error{Op: "Post", URL: "https://dev-kube-apiserver:443", Err: errors.New("connection refused")}, false},
	} {
		if got := refusal(c.err); got != c.refused {
			t.Errorf("%v: refused %t, want %t", c.err, got, c.refused)
		}
	}
}
