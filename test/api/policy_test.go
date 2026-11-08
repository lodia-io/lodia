// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestKubeconfigRequestPolicy(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "team-a", "team-b")
	ctx := context.Background()
	user := func(name string) rbacv1.Subject {
		return rbacv1.Subject{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: name}
	}
	role := func(name string) rbacv1.RoleRef {
		return rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: name}
	}
	for _, obj := range []client.Object{
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: "kubeconfig-requesters", Namespace: "team-a"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{lodiav1alpha1.GroupVersion.Group}, Resources: []string{"kubeconfigrequests"}, Verbs: []string{"create"}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "kubeconfig-requesters", Namespace: "team-a"},
			RoleRef:    role("kubeconfig-requesters"),
			Subjects:   []rbacv1.Subject{user("alice"), user("carol")},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: "kubeconfig-requesters", Namespace: "team-b"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{lodiav1alpha1.GroupVersion.Group}, Resources: []string{"kubeconfigrequests"}, Verbs: []string{"create"}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "kubeconfig-requesters", Namespace: "team-b"},
			RoleRef:    role("kubeconfig-requesters"),
			Subjects:   []rbacv1.Subject{user("carol")},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: "prod-eu-admins", Namespace: "team-a"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{lodiav1alpha1.GroupVersion.Group}, Resources: []string{"controlplanes/adminkubeconfig"}, ResourceNames: []string{"prod-eu"}, Verbs: []string{"create"}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "prod-eu-admins", Namespace: "team-a"},
			RoleRef:    role("prod-eu-admins"),
			Subjects:   []rbacv1.Subject{user("carol")},
		},
	} {
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = k8s.Delete(context.Background(), obj) })
	}

	clients := map[string]client.Client{}
	for _, name := range []string{"alice", "carol", "dave"} {
		authenticated, err := env.AddUser(envtest.User{Name: name}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if clients[name], err = client.New(authenticated.Config(), client.Options{}); err != nil {
			t.Fatal(err)
		}
	}
	request := func(namespace, user, controlPlane, access string) *unstructured.Unstructured {
		obj := example(t, "kubeconfigrequest.yaml", "policy")
		obj.SetNamespace(namespace)
		set(t, obj, user, "spec", "user")
		set(t, obj, controlPlane, "spec", "controlPlaneRef", "name")
		unstructured.RemoveNestedField(obj.Object, "spec", "access")
		if access != "" {
			set(t, obj, access, "spec", "access")
		}
		return obj
	}

	for _, obj := range policy(t) {
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		ctx := context.Background()
		for _, obj := range policy(t) {
			if err := k8s.Delete(ctx, obj); err != nil {
				t.Error(err)
			}
		}
		err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			return k8s.Create(ctx, example(t, "kubeconfigrequest.yaml", "policy-removed"), client.DryRunAll) == nil, nil
		})
		if err != nil {
			t.Errorf("the envtest admin's request is still refused after the policy was deleted: %v", err)
		}
	})
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		err := clients["alice"].Create(ctx, request("team-a", "bob", "prod-eu", ""), client.DryRunAll)
		return err != nil && strings.Contains(err.Error(), "spec.user must be the requester, alice"), nil
	})
	if err != nil {
		t.Fatalf("the policy never refused alice asking for bob: %v", err)
	}
	for _, attributes := range []authorizationv1.ResourceAttributes{
		{Namespace: "team-a", Group: lodiav1alpha1.GroupVersion.Group, Resource: "controlplanes", Subresource: "adminkubeconfig", Name: "prod-eu", Verb: "create"},
		{Namespace: "team-b", Group: lodiav1alpha1.GroupVersion.Group, Resource: "kubeconfigrequests", Verb: "create"},
	} {
		err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{User: "carol", ResourceAttributes: &attributes}}
			if err := k8s.Create(ctx, review); err != nil {
				return false, err
			}
			return review.Status.Allowed, nil
		})
		if err != nil {
			t.Fatalf("carol's Role in %s never took effect: %v", attributes.Namespace, err)
		}
	}

	cases := []struct {
		name, as, namespace, user, controlPlane, access, want string
	}{
		{"alice for alice", "alice", "team-a", "alice", "prod-eu", "", ""},
		{"alice for bob", "alice", "team-a", "bob", "prod-eu", "", "spec.user must be the requester, alice"},
		{"alice for Alice", "alice", "team-a", "Alice", "prod-eu", "", "spec.user must be the requester, alice"},
		{"alice for alice2", "alice", "team-a", "alice2", "prod-eu", "", "spec.user must be the requester, alice"},
		{"alice for ali", "alice", "team-a", "ali", "prod-eu", "", "spec.user must be the requester, alice"},
		{"alice as a cluster admin of prod-eu", "alice", "team-a", "alice", "prod-eu", "ClusterAdmin", "alice may not create controlplanes/adminkubeconfig for prod-eu"},
		{"carol as a cluster admin of prod-eu", "carol", "team-a", "carol", "prod-eu", "ClusterAdmin", ""},
		{"carol as a cluster admin of prod-us", "carol", "team-a", "carol", "prod-us", "ClusterAdmin", "carol may not create controlplanes/adminkubeconfig for prod-us"},
		{"carol as a cluster admin of team-b's prod-eu", "carol", "team-b", "carol", "prod-eu", "ClusterAdmin", "carol may not create controlplanes/adminkubeconfig for prod-eu"},
		{"dave for dave", "dave", "team-a", "dave", "prod-eu", "", `User "dave" cannot create resource "kubeconfigrequests" in API group "lodia.io" in the namespace "team-a"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := clients[c.as].Create(ctx, request(c.namespace, c.user, c.controlPlane, c.access), client.DryRunAll)
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("want admitted, got %v", err)
			case c.want != "" && !apierrors.IsForbidden(err):
				t.Fatalf("want Forbidden with %q, got %v", c.want, err)
			case c.want != "" && !strings.Contains(err.Error(), c.want):
				t.Fatalf("want %q in the error, got %v", c.want, err)
			}
		})
	}
}

func policy(t *testing.T) []*unstructured.Unstructured {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "lodia", "files", "policies", "kubeconfigrequests.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var out []*unstructured.Unstructured
	docs := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
	for {
		doc, err := docs.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		obj := decode(t, string(doc))
		if obj.GetName() != lodiav1alpha1.KubeconfigRequestPolicy {
			t.Fatalf("the %s is %s, want %s", obj.GetKind(), obj.GetName(), lodiav1alpha1.KubeconfigRequestPolicy)
		}
		out = append(out, obj)
	}
	if len(out) != 2 {
		t.Fatalf("want a policy and its binding, got %d objects", len(out))
	}
	if name, _, _ := unstructured.NestedString(out[1].Object, "spec", "policyName"); name != lodiav1alpha1.KubeconfigRequestPolicy {
		t.Fatalf("the binding names the policy %s", name)
	}
	return out
}

func TestTheHostnamePolicyOfOperationsBindsHostnamesToTheirNamespace(t *testing.T) {
	needsEnvtest(t)
	ensureNamespaces(t, "lodia-system", "team-a")
	ctx := context.Background()
	objects := hostnamePolicy(t)
	zone, _, _ := unstructured.NestedString(objects[2].Object, "data", "zone")
	controlPlane := func(api, konnectivity, tunnel string) *unstructured.Unstructured {
		obj := example(t, "controlplane-ha.yaml", "hostnames")
		set(t, obj, map[string]any{"apiServer": api, "konnectivity": konnectivity, "tunnel": tunnel}, "spec", "endpoint", "gateway", "hostnames")
		return obj
	}
	own := ".prod-eu.team-a." + zone
	stored := controlPlane("api"+own, "konnectivity"+own, "tunnel.prod-eu.team-b."+zone)
	stored.SetName("stored")
	if err := k8s.Create(ctx, stored); err != nil {
		t.Fatal(err)
	}
	for _, obj := range objects {
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		ctx := context.Background()
		if err := k8s.Delete(ctx, stored); err != nil {
			t.Error(err)
		}
		for _, i := range []int{1, 0, 2} {
			if err := k8s.Delete(ctx, objects[i]); err != nil {
				t.Error(err)
			}
		}
		err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			return k8s.Create(ctx, controlPlane("api.elsewhere.example.org", "konnectivity.elsewhere.example.org", "tunnel.elsewhere.example.org"), client.DryRunAll) == nil, nil
		})
		if err != nil {
			t.Errorf("a ControlPlane is still refused after the policy was deleted: %v", err)
		}
	})
	refused := "every hostname of endpoint.gateway must end in .team-a." + zone
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		err := k8s.Create(ctx, controlPlane("api"+own, "konnectivity"+own, "tunnel.prod-eu.team-b."+zone), client.DryRunAll)
		return err != nil && strings.Contains(err.Error(), refused), nil
	})
	if err != nil {
		t.Fatalf("the policy never refused a hostname of team-b: %v", err)
	}
	if err := k8s.Get(ctx, client.ObjectKeyFromObject(stored), stored); err != nil {
		t.Fatal(err)
	}
	stored.SetLabels(map[string]string{"example.com/team": "a"})
	if err := k8s.Update(ctx, stored); err != nil {
		t.Errorf("an update of a ControlPlane stored before the policy: %v, want admitted", err)
	}
	loadBalancer := example(t, "controlplane-ha.yaml", "hostnames")
	set(t, loadBalancer, map[string]any{"loadBalancer": map[string]any{}}, "spec", "endpoint")
	for _, c := range []struct {
		name string
		obj  *unstructured.Unstructured
		want string
	}{
		{"every hostname under its namespace", controlPlane("api"+own, "konnectivity"+own, "tunnel"+own), ""},
		{"a hostname under another namespace", controlPlane("api"+own, "konnectivity.prod-eu.team-b."+zone, "tunnel"+own), refused},
		{"the namespace's own name", controlPlane("team-a."+zone, "konnectivity"+own, "tunnel"+own), refused},
		{"the zone inside another domain", controlPlane("api"+own+".example.org", "konnectivity"+own, "tunnel"+own), refused},
		{"another mode", loadBalancer, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := k8s.Create(ctx, c.obj, client.DryRunAll)
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("want admitted, got %v", err)
			case c.want != "" && (!apierrors.IsInvalid(err) || !strings.Contains(err.Error(), c.want)):
				t.Fatalf("want Invalid with %q, got %v", c.want, err)
			}
		})
	}
}

func hostnamePolicy(t *testing.T) []*unstructured.Unstructured {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "operations.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, section, ok := strings.Cut(string(raw), "### Hostnames on a shared Gateway\n")
	_, block, found := strings.Cut(section, "```yaml\n")
	block, _, closed := strings.Cut(block, "```")
	if !ok || !found || !closed {
		t.Fatal("docs/operations.md has no YAML block under Hostnames on a shared Gateway")
	}
	var out []*unstructured.Unstructured
	docs := utilyaml.NewYAMLReader(bufio.NewReader(strings.NewReader(block)))
	for {
		doc, err := docs.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, decode(t, string(doc)))
	}
	var kinds []string
	for _, obj := range out {
		kinds = append(kinds, obj.GetKind())
	}
	if strings.Join(kinds, " ") != "ValidatingAdmissionPolicy ValidatingAdmissionPolicyBinding ConfigMap" {
		t.Fatalf("the block holds %v, want a policy, its binding and its ConfigMap", kinds)
	}
	return out
}
