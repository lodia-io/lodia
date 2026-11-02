// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
)

func TestAWebhookActsOutsideWhenItCallsAURLOrFailsClosed(t *testing.T) {
	url := "https://hooks.example.com/validate"
	service := admissionregistrationv1.WebhookClientConfig{Service: &admissionregistrationv1.ServiceReference{Namespace: "shop", Name: "hooks"}}
	for _, row := range []struct {
		name   string
		config admissionregistrationv1.WebhookClientConfig
		policy *admissionregistrationv1.FailurePolicyType
		want   bool
	}{
		{"a Service that fails open", service, new(admissionregistrationv1.Ignore), false},
		{"a Service that fails closed", service, new(admissionregistrationv1.Fail), true},
		{"a Service without a failure policy", service, nil, true},
		{"a URL that fails open", admissionregistrationv1.WebhookClientConfig{URL: &url}, new(admissionregistrationv1.Ignore), true},
	} {
		if got := actsOutside(row.config, row.policy); got != row.want {
			t.Errorf("%s acts outside: %t, want %t", row.name, got, row.want)
		}
	}
}
