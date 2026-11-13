// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"encoding/json"
	"path"
	"strings"
	"testing"
	"time"
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

func TestEveryKindOfAHeldRestoreSaysOrMoreWhenItsPageHasANextAndOnlyThen(t *testing.T) {
	const (
		nothing = "0 LoadBalancer Services; 0 PersistentVolumes with reclaimPolicy Delete; 0 pending PersistentVolumeClaims" +
			"; 0 webhooks that call a URL or fail closed; 0 APIServices served by a Service; 0 CronJobs not suspended"
		configuration = `{"metadata":{"name":"c"},"webhooks":[{"name":"w"}]}`
		webhooks      = "webhooks that call a URL or fail closed"
	)
	for _, c := range []struct{ path, item, kind, name string }{
		{servicesPath, `{"metadata":{"namespace":"n","name":"s"},"spec":{"type":"LoadBalancer"}}`, "LoadBalancer Services", "n/s"},
		{volumesPath, `{"metadata":{"name":"v"},"spec":{"persistentVolumeReclaimPolicy":"Delete"}}`, "PersistentVolumes with reclaimPolicy Delete", "v"},
		{claimsPath, `{"metadata":{"namespace":"n","name":"c"},"status":{"phase":"Pending"}}`, "pending PersistentVolumeClaims", "n/c"},
		{validatingPath, configuration, webhooks, "c/w"},
		{mutatingPath, configuration, webhooks, "c/w"},
		{apiServicePath, `{"metadata":{"name":"a"},"spec":{"service":{}}}`, "APIServices served by a Service", "a"},
		{cronJobsPath, `{"metadata":{"namespace":"n","name":"j"},"spec":{}}`, "CronJobs not suspended", "n/j"},
	} {
		for _, page := range []struct{ name, next, count, ellipsis string }{
			{"a last page", "", "1", ""},
			{"a page with a next", "next", "1 or more", "…"},
		} {
			t.Run(path.Base(c.path)+", "+page.name, func(t *testing.T) {
				body := `{"metadata":{"continue":"` + page.next + `"},"items":[` + c.item + `]}`
				tenant := servingPaths(t, map[string][]byte{c.path: []byte(body)})
				want := strings.Replace(nothing, "0 "+c.kind, page.count+" "+c.kind+" ("+c.name+page.ellipsis+")", 1)
				if got := (&Reconciler{Timeout: time.Minute}).actedOn(t.Context(), tenant); got != want {
					t.Errorf("the message says %q, want %q", got, want)
				}
			})
		}
	}
}
