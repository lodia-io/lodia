// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"encoding/json"
	"testing"
)

func TestAWebhookActsOutsideWhenItCallsAURLOrFailsClosed(t *testing.T) {
	service := `"clientConfig":{"service":{"namespace":"shop","name":"hooks"}}`
	for _, row := range []struct {
		name    string
		webhook string
		want    bool
	}{
		{"a Service that fails open", `{` + service + `,"failurePolicy":"Ignore"}`, false},
		{"a Service that fails closed", `{` + service + `,"failurePolicy":"Fail"}`, true},
		{"a Service without a failure policy", `{` + service + `}`, true},
		{"a URL that fails open", `{"clientConfig":{"url":"https://hooks.example.com/validate"},"failurePolicy":"Ignore"}`, true},
		{"a null URL that fails open", `{"clientConfig":{"url":null,"service":{"name":"hooks"}},"failurePolicy":"Ignore"}`, false},
	} {
		var w webhook
		if err := json.Unmarshal([]byte(row.webhook), &w); err != nil {
			t.Fatal(err)
		}
		if got := w.actsOutside(); got != row.want {
			t.Errorf("%s acts outside: %t, want %t", row.name, got, row.want)
		}
	}
}
