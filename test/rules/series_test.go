// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package rules

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

const (
	conditionMetric  = "lodia_controlplane_condition"
	reasonMetric     = conditionMetric + "_reason"
	transitionMetric = conditionMetric + "_last_transition_timestamp_seconds"
	slotMetric       = "lodia_controlplane_maintenance_slot_since_timestamp_seconds"
	upgradeMetric    = "lodia_controlplane_upgrade_started_timestamp_seconds"
)

type pod struct{ name, instance string }

var (
	podA = pod{"lodia-a", "10.0.0.1:8443"}
	podB = pod{"lodia-b", "10.0.0.2:8443"}
)

type label struct{ name, value string }

var targetLabels = []label{{"service", "lodia-metrics"}, {"endpoint", "metrics"}, {"container", "lodia"}}

type series struct {
	metric       string
	namespace    string
	controlplane string
	pod          pod
	labels       []label
	values       string
}

type batch []series

func (s series) String() string {
	parts := []label{{"job", "lodia"}, {"namespace", s.namespace}}
	if s.controlplane != "" {
		parts = append(parts, label{"controlplane", s.controlplane})
	}
	parts = append(parts, targetLabels...)
	parts = append(parts, label{"pod", s.pod.name}, label{"instance", s.pod.instance})
	parts = append(parts, s.labels...)
	rendered := make([]string, len(parts))
	for i, l := range parts {
		rendered[i] = fmt.Sprintf("%s=%q", l.name, l.value)
	}
	return s.metric + "{" + strings.Join(rendered, ",") + "}"
}

func (s series) label(name string) string {
	for _, l := range s.labels {
		if l.name == name {
			return l.value
		}
	}
	return ""
}

func (b batch) on(p pod) batch {
	moved := slices.Clone(b)
	for i := range moved {
		moved[i].pod = p
	}
	return moved
}

func (b batch) in(namespace string) batch {
	moved := slices.Clone(b)
	for i := range moved {
		moved[i].namespace = namespace
	}
	return moved
}

func join(parts ...batch) batch {
	return slices.Concat(parts...)
}

func pairs(kv []string) []label {
	if len(kv)%2 != 0 {
		panic(fmt.Sprintf("labels %q are not name, value pairs", kv))
	}
	labels := make([]label, 0, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		labels = append(labels, label{kv[i], kv[i+1]})
	}
	return labels
}

func tenant(metric, controlplane, values string, labels ...string) batch {
	return batch{{metric: metric, namespace: "tenants", controlplane: controlplane, pod: podA, labels: pairs(labels), values: values}}
}

func operator(metric, values string, labels ...string) batch {
	return batch{{metric: metric, namespace: "lodia-system", pod: podA, labels: pairs(labels), values: values}}
}

func cond(controlplane, typ, status, values string) batch {
	return tenant(conditionMetric, controlplane, values, "type", typ, "status", status)
}

func reason(controlplane, typ, cause, values string) batch {
	return tenant(reasonMetric, controlplane, values, "type", typ, "reason", cause)
}

type alertLabels map[string]string

func alertOf(labels ...string) alertLabels {
	l := alertLabels{"job": "lodia"}
	for _, t := range targetLabels {
		l[t.name] = t.value
	}
	for _, p := range pairs(labels) {
		l[p.name] = p.value
	}
	return l
}

func tenantAlert(controlplane string, labels ...string) alertLabels {
	return alertOf(append([]string{"namespace", "tenants", "controlplane", controlplane}, labels...)...)
}

func operatorAlert(labels ...string) alertLabels {
	return alertOf(append([]string{"namespace", "lodia-system"}, labels...)...)
}

type check struct {
	at    int
	alert string
	want  []alertLabels
}

type testCase struct {
	name   string
	series batch
	checks []check
	asIs   bool
}

func throughout(alert string, want alertLabels, times ...int) []check {
	checks := make([]check, len(times))
	for i, at := range times {
		checks[i] = check{at, alert, []alertLabels{want}}
	}
	return checks
}

func silent(alert string, times ...int) []check {
	checks := make([]check, len(times))
	for i, at := range times {
		checks[i] = check{at, alert, nil}
	}
	return checks
}

func failover(alert string, want alertLabels, before int, firing ...batch) testCase {
	n := before / 30
	stale := (n + 1) * 30
	back := stale + 210
	var onA, onB batch
	for _, s := range join(firing...) {
		a, b := s, s
		a.values = fmt.Sprintf("%sx%d stale", s.values, n)
		b.pod = podB
		b.values = fmt.Sprintf("_x%d %sx20", back/30, s.values)
		onA, onB = append(onA, a), append(onB, b)
	}
	return testCase{
		name:   alert + " fires throughout a failover",
		series: join(onA, onB),
		checks: throughout(alert, want, before, stale, back-30, back),
		asIs:   true,
	}
}

type healthyType struct{ typ, status, cause string }

var healthy = []healthyType{
	{"CertificatesReady", "True", "Issued"}, {"AccessReady", "True", "Bound"},
	{"CAValidityAcceptable", "True", "Valid"}, {"DatastoreReady", "True", "Ready"},
	{"APIServerReady", "True", "Ready"}, {"AddonsApplied", "True", "Applied"},
	{"AddonsAvailable", "True", "Available"}, {"Reachable", "True", "Reachable"},
	{"ControllersLeading", "True", "Leading"}, {"ControllerManagerHeld", "False", "NotHeld"},
	{"BackupReady", "True", "BackupSucceeded"}, {"Progressing", "False", "Idle"}, {"Ready", "True", "Ready"},
}

var (
	trueReasons = map[string][]string{
		"Ready": {"Ready"}, "ControllersLeading": {"Leading", "Held"},
		"Progressing":    {"Maintaining", "Upgrading", "RollingOut", "LatestGenerationNotObserved"},
		"APIServerReady": {"Ready", "ProgressDeadlineExceeded"}, "ControllerManagerHeld": {"NodesSilent"},
		"CAValidityAcceptable": {"Valid"},
	}
	unknownReasons = []string{"Reconciling", "LatestGenerationNotObserved", "SnapshotsPending"}
	reasonOf       = map[[2]string]string{
		{"ControllerManagerHeld", "True"}: "NodesSilent", {"ControllerManagerHeld", "False"}: "NotHeld",
		{"CAValidityAcceptable", "False"}: "CAExpiring", {"CAValidityAcceptable", "True"}: "Valid",
		{"Ready", "True"}: "Ready",
	}
	readyFrom = map[string]string{
		"CAExpired": "CertificatesReady", "WaitingForBackup": "CertificatesReady",
		"Restoring": "DatastoreReady", "Unavailable": "APIServerReady", "HostnameConflict": "RoutesReady",
	}
	statuses = []string{"True", "False", "Unknown"}
)

const completedFor = 520

var constantValues = regexp.MustCompile(`^(-?\d+)x(\d+)$`)

func statusOfReason(typ, cause string) string {
	switch {
	case slices.Contains(unknownReasons, cause) && typ != "Progressing":
		return "Unknown"
	case typ == "Progressing" && cause == "LatestGenerationNotObserved":
		return "True"
	case slices.Contains(trueReasons[typ], cause):
		return "True"
	}
	return "False"
}

func constant(values string) (value, length string, err error) {
	m := constantValues.FindStringSubmatch(values)
	if m == nil {
		return "", "", fmt.Errorf("a condition series of a case varies: %s", values)
	}
	return m[1], m[2], nil
}

func bit(on bool) int {
	if on {
		return 1
	}
	return 0
}

type conditionSeries struct {
	statuses   []series
	reason     *series
	transition bool
}

type tenantKey struct{ namespace, controlplane string }

type tenantSeries struct {
	pod   pod
	types map[string]*conditionSeries
}

func complete(in batch) (batch, error) {
	tenants := map[tenantKey]*tenantSeries{}
	var order []tenantKey
	for _, s := range in {
		if !strings.HasPrefix(s.metric, "lodia_controlplane_") {
			continue
		}
		key := tenantKey{s.namespace, s.controlplane}
		t, ok := tenants[key]
		if !ok {
			t = &tenantSeries{pod: s.pod, types: map[string]*conditionSeries{}}
			tenants[key] = t
			order = append(order, key)
		}
		if !strings.HasPrefix(s.metric, conditionMetric) {
			continue
		}
		c, ok := t.types[s.label("type")]
		if !ok {
			c = &conditionSeries{}
			t.types[s.label("type")] = c
		}
		switch s.metric {
		case conditionMetric:
			c.statuses = append(c.statuses, s)
		case reasonMetric:
			c.reason = &s
		default:
			c.transition = true
		}
	}
	var extra batch
	for _, key := range order {
		t, cp := tenants[key], key.controlplane
		add := func(b batch) { extra = append(extra, b.on(t.pod).in(key.namespace)...) }
		want := slices.Clone(healthy)
		if ready := t.types["Ready"]; ready != nil && ready.reason != nil {
			cause := ready.reason.label("reason")
			if from, ok := readyFrom[cause]; ok && t.types[from] == nil {
				h := healthyType{from, statusOfReason("Ready", cause), cause}
				if i := slices.IndexFunc(want, func(w healthyType) bool { return w.typ == from }); i >= 0 {
					want[i] = h
				} else {
					want = append(want, h)
				}
			}
		}
		for _, h := range want {
			c := t.types[h.typ]
			if c == nil {
				for _, st := range statuses {
					add(cond(cp, h.typ, st, fmt.Sprintf("%dx%d", bit(st == h.status), completedFor)))
				}
				add(reason(cp, h.typ, h.cause, fmt.Sprintf("1x%d", completedFor)))
				transition := fmt.Sprintf("-86400x%d", completedFor)
				if h.typ == "Progressing" {
					transition = fmt.Sprintf("0x20 600x%d", completedFor-21)
				}
				add(tenant(transitionMetric, cp, transition, "type", h.typ))
				continue
			}
			var status, n string
			if c.reason != nil {
				status = statusOfReason(h.typ, c.reason.label("reason"))
				var err error
				if _, n, err = constant(c.reason.values); err != nil {
					return nil, err
				}
			}
			for _, s := range c.statuses {
				v, length, err := constant(s.values)
				if err != nil {
					return nil, err
				}
				n = length
				if v == "1" {
					status = s.label("status")
				}
			}
			has := func(st string) bool {
				return slices.ContainsFunc(c.statuses, func(s series) bool { return s.label("status") == st })
			}
			if status == "" {
				status = h.status
				if has(h.status) {
					status = "False"
				}
			}
			for _, st := range statuses {
				if !has(st) {
					add(cond(cp, h.typ, st, fmt.Sprintf("%dx%s", bit(st == status), n)))
				}
			}
			if c.reason == nil {
				cause, ok := reasonOf[[2]string{h.typ, status}]
				if !ok {
					return nil, fmt.Errorf("%s/%s: no reason for %s %s", key.namespace, cp, h.typ, status)
				}
				add(reason(cp, h.typ, cause, "1x"+n))
			}
			if !c.transition {
				add(tenant(transitionMetric, cp, "-86400x"+n, "type", h.typ))
			}
		}
	}
	return join(in, extra), nil
}
