// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package apistatus

import (
	"errors"
	"net/http"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"
)

// A dry run that reaches an admission webhook whose sideEffects are neither
// None nor NoneOnDryRun is a 400 that names it, whatever its failurePolicy,
// the same at the patches of 1.35, 1.36 and 1.37 Lodia runs
// (k8s.io/apiserver@v0.37.3 pkg/admission/plugin/webhook/validating/dispatcher.go#L249-L255,
// pkg/admission/plugin/webhook/mutating/dispatcher.go#L248-L254,
// pkg/admission/plugin/webhook/errors/statuserror.go#L60-L63).
const (
	unsupportedBy = `admission webhook "`
	unsupported   = `" does not support dry run`
)

// DryRunUnsupported names the admission webhook that kept a dry run from the
// API server's verdict, since it does not support dry run, and is empty for
// any other answer: the request is neither admitted nor refused.
func DryRunUnsupported(err error) string {
	var status apierrors.APIStatus
	if !errors.As(err, &status) {
		return ""
	}
	s := status.Status()
	if s.Code != http.StatusBadRequest || !strings.HasPrefix(s.Message, unsupportedBy) || !strings.HasSuffix(s.Message, unsupported) {
		return ""
	}
	name := strings.TrimSuffix(strings.TrimPrefix(s.Message, unsupportedBy), unsupported)
	if len(validation.IsDNS1123Subdomain(name)) > 0 {
		return ""
	}
	return name
}

// Unsupported is the API server's answer to a dry run that webhook keeps
// from any verdict.
func Unsupported(webhook string) string {
	return unsupportedBy + webhook + unsupported
}
