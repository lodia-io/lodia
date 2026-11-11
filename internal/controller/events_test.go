// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package controller_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

var recorders = map[string]string{
	"lodia.io/backup":            "Backup",
	"lodia.io/kubeconfigrequest": "KubeconfigRequest",
}

func TestEachControllerRecordsItsTransitionsOnceUnderItsName(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run make test")
	}
	cfg, k8s := serve(t)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "events-"}}
	if err := k8s.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	for _, role := range generatedRoles(t) {
		user := role.Name
		bind(t, k8s, role, user)
		for _, verb := range []string{"create", "patch"} {
			allowed(t, k8s, user, ns.Name, verb)
		}
	}
	operatorUser := asTheOperator(t, k8s, cfg)
	stop := run(t, operatorUser)

	class, gateway, cp := gatewayTenant(ns.Name)
	backup := &lodiav1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "dev-backup", Namespace: ns.Name},
		Spec:       lodiav1alpha1.BackupSpec{ControlPlaneRef: lodiav1alpha1.LocalReference{Name: cp.Name}},
	}
	request := &lodiav1alpha1.KubeconfigRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: ns.Name},
		Spec: lodiav1alpha1.KubeconfigRequestSpec{
			ControlPlaneRef: lodiav1alpha1.LocalReference{Name: cp.Name},
			User:            "alice",
			Request:         []byte("-----BEGIN CERTIFICATE REQUEST-----\nA\n-----END CERTIFICATE REQUEST-----\n"),
		},
	}
	for _, obj := range []client.Object{class, gateway, cp, backup, request} {
		if err := k8s.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	uids := map[string]string{"ControlPlane": string(cp.UID), "Backup": string(backup.UID), "KubeconfigRequest": string(request.UID)}
	want := map[string]bool{}
	for _, writer := range conditionWriters {
		want["lodia.io/"+strings.TrimPrefix(writer, "lodia/")] = true
	}
	for controller := range recorders {
		want[controller] = true
	}

	poll(t, time.Minute, func() error {
		var list eventsv1.EventList
		if err := k8s.List(t.Context(), &list, client.InNamespace(ns.Name)); err != nil {
			return err
		}
		recorded := map[string]bool{}
		for _, e := range list.Items {
			recorded[e.ReportingController] = true
		}
		if !maps.Equal(recorded, want) {
			return fmt.Errorf("recorded by %v, want %v", slices.Sorted(maps.Keys(recorded)), slices.Sorted(maps.Keys(want)))
		}
		return nil
	})
	before := settled(t, k8s, ns.Name)
	stop()
	var list eventsv1.EventList
	if err := k8s.List(t.Context(), &list, client.InNamespace(ns.Name)); err != nil {
		t.Fatal(err)
	}
	for _, e := range list.Items {
		kind, own := recorders[e.ReportingController]
		if !own {
			kind = "ControlPlane"
		}
		switch {
		case e.Regarding.Kind != kind || string(e.Regarding.UID) != uids[kind] || e.Regarding.ResourceVersion != "":
			t.Errorf("%s records %s %s regarding %+v", e.ReportingController, e.Action, e.Reason, e.Regarding)
		case kind == "ControlPlane" && "lodia.io/"+strings.TrimPrefix(conditionWriters[e.Action], "lodia/") != e.ReportingController:
			t.Errorf("%s records %s, which %s writes", e.ReportingController, e.Action, conditionWriters[e.Action])
		case kind != "ControlPlane" && e.Action != lodiav1alpha1.ConditionReady:
			t.Errorf("%s records %s", e.ReportingController, e.Action)
		}
	}

	passes := reconciles(t)
	run(t, operatorUser)
	poll(t, time.Minute, func() error {
		now := reconciles(t)
		var behind []string
		for controller := range want {
			name := strings.TrimPrefix(controller, "lodia.io/")
			if now[name] <= passes[name] {
				behind = append(behind, name)
			}
		}
		if len(behind) > 0 {
			return fmt.Errorf("the restarted manager has not passed in %v", behind)
		}
		return nil
	})
	if after := settled(t, k8s, ns.Name); !maps.Equal(after, before) {
		t.Errorf("the restarted manager recorded: events and their counts went from %v to %v", before, after)
	}
}

func generatedRoles(t *testing.T) []*rbacv1.ClusterRole {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "charts", "lodia", "files", "rbac", "*.yaml"))
	if err != nil || len(files) != 6 {
		t.Fatalf("the generated ClusterRoles are %v: %v", files, err)
	}
	var roles []*rbacv1.ClusterRole
	for _, file := range files {
		manifest, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		role := &rbacv1.ClusterRole{}
		if err := yaml.UnmarshalStrict(manifest, role); err != nil {
			t.Fatal(err)
		}
		roles = append(roles, role)
	}
	return roles
}

func asTheOperator(t *testing.T, k8s client.Client, cfg *rest.Config) *rest.Config {
	t.Helper()
	const user = "lodia"
	for _, role := range generatedRoles(t) {
		role.Name = "operator-" + role.Name
		bind(t, k8s, role, user)
	}
	druid := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "etcd-druid"}}
	deployment := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "lodia-etcd-druid", Namespace: druid.Name},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"apps"}, Resources: []string{"deployments"}, ResourceNames: []string{"etcd-druid"}, Verbs: []string{"get"}}},
	}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "lodia-etcd-druid", Namespace: druid.Name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: deployment.Name},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}
	for _, obj := range []client.Object{druid, deployment, binding} {
		if err := k8s.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
	allowed(t, k8s, user, "", "create")
	operator := rest.CopyConfig(cfg)
	operator.Impersonate = rest.ImpersonationConfig{UserName: user}
	return operator
}

func bind(t *testing.T, k8s client.Client, role *rbacv1.ClusterRole, user string) {
	t.Helper()
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: role.Name},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}
	for _, obj := range []client.Object{role, binding} {
		if err := k8s.Create(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}
}

func allowed(t *testing.T, k8s client.Client, user, ns, verb string) {
	t.Helper()
	poll(t, 30*time.Second, func() error {
		review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
			User:               user,
			ResourceAttributes: &authorizationv1.ResourceAttributes{Group: eventsv1.GroupName, Resource: "events", Namespace: ns, Verb: verb},
		}}
		if err := k8s.Create(t.Context(), review); err != nil {
			return err
		}
		if !review.Status.Allowed {
			return fmt.Errorf("%s may not %s events.k8s.io events in %q", user, verb, ns)
		}
		return nil
	})
}

func settled(t *testing.T, k8s client.Client, ns string) map[string]int32 {
	t.Helper()
	var last map[string]int32
	since := time.Now()
	poll(t, 2*time.Minute, func() error {
		var list eventsv1.EventList
		if err := k8s.List(t.Context(), &list, client.InNamespace(ns)); err != nil {
			return err
		}
		current := map[string]int32{}
		for _, e := range list.Items {
			current[e.ReportingController+" "+e.Action+" "+e.Reason+" "+e.Type] += 1
			if e.Series != nil {
				current[e.ReportingController+" "+e.Action+" "+e.Reason+" "+e.Type] += e.Series.Count - 1
			}
		}
		if !maps.Equal(current, last) {
			last, since = current, time.Now()
		}
		if time.Since(since) < 5*time.Second {
			return errors.New("the events still change")
		}
		return nil
	})
	return last
}

func reconciles(t *testing.T) map[string]float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "controller_runtime_reconcile_total" {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "controller" {
					counts[label.GetValue()] += m.GetCounter().GetValue()
				}
			}
		}
	}
	return counts
}

func poll(t *testing.T, timeout time.Duration, check func() error) {
	t.Helper()
	var last error
	if err := wait.PollUntilContextTimeout(t.Context(), 250*time.Millisecond, timeout, true, func(context.Context) (bool, error) {
		last = check()
		return last == nil, nil
	}); err != nil {
		t.Fatalf("%v: %v", err, last)
	}
}
