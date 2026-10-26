// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package apistatus tells an API server's answer from no answer.
package apistatus

import (
	"errors"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// Refused says whether err is the API server's verdict on a request, which
// sending it again cannot change: every 4xx but a missing object, a
// timeout, a conflict, throttling and a namespace being deleted. Anything
// else is no answer. An admission webhook's denial without a code arrives as
// 400 (k8s.io/apiserver@v0.37.1
// pkg/admission/plugin/webhook/errors/statuserror.go#L37-L39).
func Refused(err error) bool {
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return false
	}
	switch code := status.Status().Code; {
	case code < 400 || code > 499, code == http.StatusNotFound, code == http.StatusRequestTimeout, code == http.StatusConflict, code == http.StatusTooManyRequests:
		return false
	}
	return !apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause)
}
