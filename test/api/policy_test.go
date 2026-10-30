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
	ensureNamespaces(t, "team-a")
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
	request := func(user, controlPlane, access string) *unstructured.Unstructured {
		obj := example(t, "kubeconfigrequest.yaml", "policy")
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
		err := clients["alice"].Create(ctx, request("bob", "prod-eu", ""), client.DryRunAll)
		return err != nil && strings.Contains(err.Error(), "spec.user must be the requester, alice"), nil
	})
	if err != nil {
		t.Fatalf("the policy never refused alice asking for bob: %v", err)
	}
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
			User: "carol",
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace: "team-a", Group: lodiav1alpha1.GroupVersion.Group, Resource: "controlplanes", Subresource: "adminkubeconfig", Name: "prod-eu", Verb: "create",
			},
		}}
		if err := k8s.Create(ctx, review); err != nil {
			return false, err
		}
		return review.Status.Allowed, nil
	})
	if err != nil {
		t.Fatalf("carol's Role never took effect: %v", err)
	}

	cases := []struct {
		name, as, user, controlPlane, access, want string
	}{
		{"alice for alice", "alice", "alice", "prod-eu", "", ""},
		{"alice for bob", "alice", "bob", "prod-eu", "", "spec.user must be the requester, alice"},
		{"alice as a cluster admin of prod-eu", "alice", "alice", "prod-eu", "ClusterAdmin", "alice may not create controlplanes/adminkubeconfig for prod-eu"},
		{"carol as a cluster admin of prod-eu", "carol", "carol", "prod-eu", "ClusterAdmin", ""},
		{"carol as a cluster admin of prod-us", "carol", "carol", "prod-us", "ClusterAdmin", "carol may not create controlplanes/adminkubeconfig for prod-us"},
		{"dave for dave", "dave", "dave", "prod-eu", "", `User "dave" cannot create resource "kubeconfigrequests" in API group "lodia.io" in the namespace "team-a"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := clients[c.as].Create(ctx, request(c.user, c.controlPlane, c.access), client.DryRunAll)
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
