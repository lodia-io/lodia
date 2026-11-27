// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Package apistatus tells an API server's answer from no answer.
package apistatus

import (
	"errors"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"
)

// The texts kube-apiserver answers with for an admission webhook, the same at
// the patches of 1.35, 1.36 and 1.37 Lodia runs: a webhook it cannot call
// that should admit the request is a 500 (k8s.io/apiserver@v0.37.3 pkg/util/webhook/error.go#L34-L39,
// k8s.io/apimachinery@v0.37.3 pkg/api/errors/errors.go#L387-L397), and a
// webhook's own denial keeps the webhook's code from 400 up
// (k8s.io/apiserver@v0.37.3 pkg/admission/plugin/webhook/errors/statuserror.go#L28-L57).
// An admission plugin whose sources have not synced answers 403 with
// notReady (k8s.io/apiserver@v0.37.3 pkg/admission/plugin/policy/generic/plugin.go#L323-L331).
const (
	uncalled = `Internal error occurred: failed calling webhook "`
	denying  = `admission webhook "`
	refusing = `" denied the request`
	notReady = "forbidden: not yet ready to handle request"
)

// Refused says whether err is the API server's verdict on a request, which
// sending it again cannot change: every 4xx but a missing object, a
// timeout, a conflict, throttling, a namespace being deleted, an admission
// plugin not ready yet and a dry run a webhook cannot take, and an admission
// webhook's answer whatever its code. Anything else is no answer.
func Refused(err error) bool {
	var status apierrors.APIStatus
	if !errors.As(err, &status) || DryRunUnsupported(err) != "" {
		return false
	}
	if name, _ := Webhook(err); name != "" {
		return true
	}
	s := status.Status()
	switch code := s.Code; {
	case code < 400 || code > 499, code == http.StatusNotFound, code == http.StatusRequestTimeout, code == http.StatusConflict, code == http.StatusTooManyRequests:
		return false
	case code == http.StatusForbidden && strings.HasSuffix(s.Message, notReady):
		return false
	}
	return !apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause)
}

// Webhook names the admission webhook err's answer comes from, anchored at
// the start of its message: one kube-apiserver could not call, a 500, or one
// that denied the request, from 400 up, which denied says. The name is empty
// for any other answer, and for one that is no DNS subdomain.
func Webhook(err error) (name string, denied bool) {
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return "", false
	}
	s := status.Status()
	switch {
	case s.Code == http.StatusInternalServerError && strings.HasPrefix(s.Message, uncalled):
		return named(strings.TrimPrefix(s.Message, uncalled), `"`), false
	case s.Code >= http.StatusBadRequest && strings.HasPrefix(s.Message, denying):
		name := named(strings.TrimPrefix(s.Message, denying), refusing)
		return name, name != ""
	}
	return "", false
}

// named is the DNS subdomain rest starts with up to end, empty when there is
// none.
func named(rest, end string) string {
	i := strings.Index(rest[:min(len(rest), validation.DNS1123SubdomainMaxLength+len(end))], end)
	if i < 0 || len(validation.IsDNS1123Subdomain(rest[:i])) > 0 {
		return ""
	}
	return rest[:i]
}
