// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	authenticationv1 "k8s.io/api/authentication/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	eventsv1 "k8s.io/api/events/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	metricsService = release + "-metrics"
	leaderLease    = "lodia.io"
)

var scraper = &http.Client{
	Timeout:   10 * time.Second,
	Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, DisableKeepAlives: true},
}

func leaderServesMetrics(ctx context.Context, t *testing.T, cfg *envconf.Config, k8s client.Client, key client.ObjectKey) {
	t.Helper()
	reader := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: "metrics-reader"}}
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: key.Namespace + "-metrics-reader"},
		Rules:      []rbacv1.PolicyRule{{NonResourceURLs: []string{"/metrics"}, Verbs: []string{"get"}}},
	}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: role.Name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Namespace: reader.Namespace, Name: reader.Name}},
	}
	objects := []client.Object{reader, role, binding}
	defer func() {
		for _, obj := range objects {
			if err := client.IgnoreNotFound(k8s.Delete(context.Background(), obj)); err != nil {
				t.Error(err)
			}
		}
	}()
	for _, obj := range objects {
		create(ctx, t, k8s, obj)
	}
	bound := serviceAccountToken(ctx, t, k8s, reader)
	unbound := serviceAccountToken(ctx, t, k8s, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: "default"}})
	var leader string
	var size float64
	until(ctx, t, 2*time.Minute, fmt.Sprintf("the leader of %s serves %s's Ready, its etcd's leader and size behind the metrics filter, and the standby no lodia_ series", leaderLease, key), func(ctx context.Context) error {
		var err error
		leader, size, err = scrapeOperator(ctx, t, cfg, k8s, key, bound, unbound)
		return err
	})
	t.Logf("pod %s leads and serves %s's etcd at %.0f bytes", leader, key, size)
}

func readyRecorded(ctx context.Context, t *testing.T, k8s client.Client, key client.ObjectKey) {
	t.Helper()
	cp := &lodiav1alpha1.ControlPlane{}
	if err := k8s.Get(ctx, key, cp); err != nil {
		t.Fatal(err)
	}
	until(ctx, t, 2*time.Minute, fmt.Sprintf("an event regarding %s records its Ready, Normal, from lodia.io/lifecycle", key), func(ctx context.Context) error {
		events := &eventsv1.EventList{}
		if err := k8s.List(ctx, events, client.InNamespace(key.Namespace)); err != nil {
			return err
		}
		var regarding []string
		for _, e := range events.Items {
			if e.Regarding.Kind != "ControlPlane" || e.Regarding.Name != key.Name || e.Regarding.UID != cp.UID {
				continue
			}
			if e.Action == lodiav1alpha1.ConditionReady && e.Reason == lodiav1alpha1.ReasonReady && e.Type == corev1.EventTypeNormal && e.ReportingController == "lodia.io/lifecycle" {
				return nil
			}
			regarding = append(regarding, fmt.Sprintf("%s %s %s from %s", e.Type, e.Action, e.Reason, e.ReportingController))
		}
		return fmt.Errorf("the events regarding it are %v", regarding)
	})
}

func serviceAccountToken(ctx context.Context, t *testing.T, k8s client.Client, sa *corev1.ServiceAccount) string {
	t.Helper()
	request := &authenticationv1.TokenRequest{}
	if err := k8s.SubResource("token").Create(ctx, sa, request); err != nil {
		t.Fatal(err)
	}
	return request.Status.Token
}

func scrapeOperator(ctx context.Context, t *testing.T, cfg *envconf.Config, k8s client.Client, key client.ObjectKey, bound, unbound string) (string, float64, error) {
	before, err := leaseHolder(ctx, k8s)
	if err != nil {
		return "", 0, err
	}
	pods, port, err := metricsEndpoints(ctx, k8s)
	if err != nil {
		return "", 0, err
	}
	scraped := make(map[string]model.Vector, len(pods))
	for _, pod := range pods {
		if scraped[pod], err = scrapePod(ctx, t, cfg, pod, port, bound, unbound); err != nil {
			return "", 0, err
		}
	}
	if len(scraped) != 2 {
		return "", 0, fmt.Errorf("the Service %s lists the pods %v, want two distinct ones", metricsService, pods)
	}
	after, err := leaseHolder(ctx, k8s)
	if err != nil {
		return "", 0, err
	}
	if after != before {
		return "", 0, fmt.Errorf("the Lease %s moved from %q to %q between the scrapes", leaderLease, before, after)
	}
	leader, _, _ := strings.Cut(before, "_")
	samples, ok := scraped[leader]
	if !ok {
		return "", 0, fmt.Errorf("the Lease %s is held by %q, none of the endpoints %v of the Service %s", leaderLease, before, slices.Sorted(maps.Keys(scraped)), metricsService)
	}
	tenant := model.LabelSet{"namespace": model.LabelValue(key.Namespace), "controlplane": model.LabelValue(key.Name)}
	for _, want := range []struct {
		name   string
		labels model.LabelSet
		ok     func(float64) bool
		what   string
	}{
		{"leader_election_master_status", model.LabelSet{"name": leaderLease}, func(v float64) bool { return v == 1 }, "1"},
		{"lodia_controlplane_condition", tenant.Merge(model.LabelSet{"type": "Ready", "status": "True"}), func(v float64) bool { return v == 1 }, "1"},
		{"lodia_controlplane_datastore_has_leader", tenant, func(v float64) bool { return v == 1 }, "1"},
		{"lodia_controlplane_datastore_db_size_bytes", tenant, func(v float64) bool { return v > 0 }, "above 0"},
	} {
		if v, ok := value(samples, want.name, want.labels); !ok || !want.ok(v) {
			return "", 0, fmt.Errorf("the leader %s serves %s%s %v (present %t), want %s", leader, want.name, want.labels, v, ok, want.what)
		}
	}
	for pod, samples := range scraped {
		if pod == leader {
			continue
		}
		if lodia := slices.IndexFunc(samples, func(s *model.Sample) bool {
			return strings.HasPrefix(string(s.Metric[model.MetricNameLabel]), "lodia_")
		}); lodia >= 0 {
			return "", 0, fmt.Errorf("the standby %s serves %s", pod, samples[lodia])
		}
		if v, ok := value(samples, "leader_election_master_status", model.LabelSet{"name": leaderLease}); !ok || v != 0 {
			return "", 0, fmt.Errorf("the standby %s serves leader_election_master_status{name=%q} %v (present %t), want 0", pod, leaderLease, v, ok)
		}
	}
	size, _ := value(samples, "lodia_controlplane_datastore_db_size_bytes", tenant)
	return leader, size, nil
}

func leaseHolder(ctx context.Context, k8s client.Client) (string, error) {
	lease := &coordinationv1.Lease{}
	if err := k8s.Get(ctx, client.ObjectKey{Namespace: operatorNamespace, Name: leaderLease}, lease); err != nil {
		return "", err
	}
	return ptr.Deref(lease.Spec.HolderIdentity, ""), nil
}

func metricsEndpoints(ctx context.Context, k8s client.Client) ([]string, int32, error) {
	list := &discoveryv1.EndpointSliceList{}
	if err := k8s.List(ctx, list, client.InNamespace(operatorNamespace), client.MatchingLabels{discoveryv1.LabelServiceName: metricsService}); err != nil {
		return nil, 0, err
	}
	var pods []string
	var port int32
	for _, slice := range list.Items {
		for _, p := range slice.Ports {
			if ptr.Deref(p.Name, "") == "metrics" {
				port = ptr.Deref(p.Port, 0)
			}
		}
		for _, e := range slice.Endpoints {
			if ptr.Deref(e.Conditions.Ready, false) && e.TargetRef != nil && e.TargetRef.Kind == "Pod" {
				pods = append(pods, e.TargetRef.Name)
			}
		}
	}
	if len(pods) != 2 || port == 0 {
		return nil, 0, fmt.Errorf("the Service %s has the ready endpoints %v on port %d, want the operator's two pods on its port metrics", metricsService, pods, port)
	}
	return pods, port, nil
}

func scrapePod(ctx context.Context, t *testing.T, cfg *envconf.Config, pod string, port int32, bound, unbound string) (model.Vector, error) {
	address, stop := forward(t, cfg.Client().RESTConfig(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: operatorNamespace, Name: pod}}, port)
	defer stop()
	url := "https://" + address + "/metrics"
	for _, refused := range []struct {
		who, token string
		want       int
	}{
		{"no token", "", http.StatusUnauthorized},
		{"the token of a ServiceAccount without get on /metrics", unbound, http.StatusForbidden},
	} {
		status, body, err := getMetrics(ctx, url, refused.token)
		if err != nil {
			return nil, fmt.Errorf("scrape pod %s with %s: %w", pod, refused.who, err)
		}
		if status != refused.want {
			return nil, fmt.Errorf("pod %s answers %s with %d, want %d: %.200s", pod, refused.who, status, refused.want, body)
		}
	}
	status, body, err := getMetrics(ctx, url, bound)
	if err != nil {
		return nil, fmt.Errorf("scrape pod %s: %w", pod, err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("pod %s answers the token of a ServiceAccount with get on /metrics with %d: %.200s", pod, status, body)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("pod %s's metrics: %w", pod, err)
	}
	return expfmt.ExtractSamples(&expfmt.DecodeOptions{Timestamp: model.Now()}, slices.Collect(maps.Values(families))...)
}

func getMetrics(ctx context.Context, url, token string) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, "", err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := scraper.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), err
}

func value(samples model.Vector, name string, labels model.LabelSet) (float64, bool) {
	for _, s := range samples {
		if string(s.Metric[model.MetricNameLabel]) != name {
			continue
		}
		matched := true
		for label, want := range labels {
			matched = matched && s.Metric[label] == want
		}
		if matched {
			return float64(s.Value), true
		}
	}
	return 0, false
}
