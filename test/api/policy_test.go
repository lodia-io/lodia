// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
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

	for _, obj := range policy(t, "kubeconfigrequests.yaml", lodiav1alpha1.KubeconfigRequestPolicy) {
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		ctx := context.Background()
		for _, obj := range policy(t, "kubeconfigrequests.yaml", lodiav1alpha1.KubeconfigRequestPolicy) {
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

func policy(t *testing.T, file, want string) []*unstructured.Unstructured {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "lodia", "files", "policies", file))
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
		if obj.GetName() != want {
			t.Fatalf("the %s is %s, want %s", obj.GetKind(), obj.GetName(), want)
		}
		out = append(out, obj)
	}
	if len(out) != 2 {
		t.Fatalf("want a policy and its binding, got %d objects", len(out))
	}
	if name, _, _ := unstructured.NestedString(out[1].Object, "spec", "policyName"); name != want {
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
	out := recipe(t, "### Hostnames on a shared Gateway\n")
	var kinds []string
	for _, obj := range out {
		kinds = append(kinds, obj.GetKind())
	}
	if strings.Join(kinds, " ") != "ValidatingAdmissionPolicy ValidatingAdmissionPolicyBinding ConfigMap" {
		t.Fatalf("the block holds %v, want a policy, its binding and its ConfigMap", kinds)
	}
	return out
}

func recipe(t *testing.T, heading string) []*unstructured.Unstructured {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "operations.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, section, ok := strings.Cut(string(raw), heading)
	_, block, found := strings.Cut(section, "```yaml\n")
	block, _, closed := strings.Cut(block, "```")
	if !ok || !found || !closed {
		t.Fatalf("docs/operations.md has no YAML block under %s", strings.TrimSpace(heading))
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
	return out
}

func TestAControlPlaneIsDeletedOnlyOnceConfirmed(t *testing.T) {
	needsEnvtest(t)
	const live, ending, collection = "deletion-live", "deletion-ending", "deletion-collection"
	ensureNamespaces(t, live, collection)
	ctx := context.Background()
	confirmed := lodiav1alpha1.DeletionConfirmedAnnotation
	controlPlane := func(namespace, name string, annotations map[string]string) *unstructured.Unstructured {
		obj := example(t, "controlplane-single.yaml", name)
		obj.SetNamespace(namespace)
		obj.SetAnnotations(annotations)
		return obj
	}
	message := func(obj *unstructured.Unstructured) string {
		return "ControlPlane " + obj.GetNamespace() + "/" + obj.GetName() + " is deleted only once confirmed: kubectl annotate controlplane " + obj.GetName() + " --namespace " + obj.GetNamespace() + " " + confirmed + "=true"
	}
	refused := func(t *testing.T, err error, obj *unstructured.Unstructured) {
		t.Helper()
		if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), message(obj)) {
			t.Fatalf("want Forbidden with %q, got %v", message(obj), err)
		}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(obj), obj.DeepCopy()); err != nil {
			t.Fatalf("the refused ControlPlane: %v", err)
		}
	}
	deleted := func(t *testing.T, err error, obj *unstructured.Unstructured) {
		t.Helper()
		if err != nil {
			t.Fatalf("want admitted, got %v", err)
		}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(obj), obj.DeepCopy()); !apierrors.IsNotFound(err) {
			t.Fatalf("the deleted ControlPlane: %v", err)
		}
	}
	probe := controlPlane(live, "probe", nil)
	if err := k8s.Create(ctx, probe); err != nil {
		t.Fatal(err)
	}
	for _, obj := range policy(t, "controlplanes.yaml", "controlplanes.lodia.io") {
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		ctx := context.Background()
		for _, obj := range policy(t, "controlplanes.yaml", "controlplanes.lodia.io") {
			if err := k8s.Delete(ctx, obj); err != nil {
				t.Error(err)
			}
		}
		err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			return k8s.Delete(ctx, probe, client.DryRunAll) == nil, nil
		})
		if err != nil {
			t.Errorf("a ControlPlane is still refused after the policy was deleted: %v", err)
		}
		for _, namespace := range []string{live, ending, collection} {
			if err := k8s.DeleteAllOf(ctx, probe, client.InNamespace(namespace)); err != nil {
				t.Error(err)
			}
		}
	})
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		return apierrors.IsForbidden(k8s.Delete(ctx, probe, client.DryRunAll)), nil
	})
	if err != nil {
		t.Fatalf("the policy never refused a delete: %v", err)
	}

	for i, c := range []struct {
		name               string
		annotations, label map[string]string
		admitted           bool
	}{
		{name: "no annotation"},
		{name: "false", annotations: map[string]string{confirmed: "false"}},
		{name: "True", annotations: map[string]string{confirmed: "True"}},
		{name: "an empty value", annotations: map[string]string{confirmed: ""}},
		{name: "the key as a label", label: map[string]string{confirmed: "true"}},
		{name: "true", annotations: map[string]string{confirmed: "true"}, admitted: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			obj := controlPlane(live, fmt.Sprintf("delete-%d", i), c.annotations)
			obj.SetLabels(c.label)
			if err := k8s.Create(ctx, obj); err != nil {
				t.Fatal(err)
			}
			if c.admitted {
				deleted(t, k8s.Delete(ctx, obj), obj)
			} else {
				refused(t, k8s.Delete(ctx, obj), obj)
			}
		})
	}

	t.Run("a dry run", func(t *testing.T) {
		open, sealed := controlPlane(live, "dry-open", nil), controlPlane(live, "dry-confirmed", map[string]string{confirmed: "true"})
		for _, obj := range []*unstructured.Unstructured{open, sealed} {
			if err := k8s.Create(ctx, obj); err != nil {
				t.Fatal(err)
			}
		}
		refused(t, k8s.Delete(ctx, open, client.DryRunAll), open)
		if err := k8s.Delete(ctx, sealed, client.DryRunAll); err != nil {
			t.Fatalf("want admitted, got %v", err)
		}
		if err := k8s.Get(ctx, client.ObjectKeyFromObject(sealed), sealed.DeepCopy()); err != nil {
			t.Fatalf("the ControlPlane a dry run deleted: %v", err)
		}
	})

	t.Run("a namespace being deleted", func(t *testing.T) {
		if err := k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ending}}); err != nil {
			t.Fatal(err)
		}
		obj := controlPlane(ending, "left", nil)
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
		refused(t, k8s.Delete(ctx, obj, client.DryRunAll), obj)
		if err := k8s.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ending}}); err != nil {
			t.Fatal(err)
		}
		namespace := &corev1.Namespace{}
		if err := k8s.Get(ctx, client.ObjectKey{Name: ending}, namespace); err != nil || namespace.DeletionTimestamp.IsZero() {
			t.Fatalf("the namespace is not being deleted: %v", err)
		}
		err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			return k8s.Delete(ctx, obj) == nil, nil
		})
		if err != nil {
			t.Fatalf("the policy refused a delete in a namespace being deleted: %v", err)
		}
		deleted(t, nil, obj)
	})

	t.Run("a collection", func(t *testing.T) {
		first, second := controlPlane(collection, "first", nil), controlPlane(collection, "second", nil)
		for _, obj := range []*unstructured.Unstructured{first, second} {
			if err := k8s.Create(ctx, obj); err != nil {
				t.Fatal(err)
			}
		}
		if err := k8s.DeleteAllOf(ctx, first, client.InNamespace(collection)); !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), message(first)) {
			t.Fatalf("want Forbidden with %q, got %v", message(first), err)
		}
		for _, obj := range []*unstructured.Unstructured{first, second} {
			obj.SetAnnotations(map[string]string{confirmed: "true"})
			if err := k8s.Update(ctx, obj); err != nil {
				t.Fatal(err)
			}
		}
		if err := k8s.DeleteAllOf(ctx, first, client.InNamespace(collection)); err != nil {
			t.Fatalf("want admitted, got %v", err)
		}
		for _, obj := range []*unstructured.Unstructured{first, second} {
			deleted(t, nil, obj)
		}
	})

	t.Run("a create and an update", func(t *testing.T) {
		obj := controlPlane(live, "written", nil)
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatalf("a create: %v", err)
		}
		for _, value := range []string{"true", "false", ""} {
			obj.SetAnnotations(map[string]string{confirmed: value})
			if err := k8s.Update(ctx, obj); err != nil {
				t.Fatalf("an update to %q: %v", value, err)
			}
		}
		obj.SetAnnotations(nil)
		if err := k8s.Update(ctx, obj); err != nil {
			t.Fatalf("an update that removes the annotation: %v", err)
		}
		refused(t, k8s.Delete(ctx, obj), obj)
	})
}

func TestTheNamespacePolicyOfOperationsRefusesTheDeletionOfATenantNamespaceUnlessConfirmed(t *testing.T) {
	needsEnvtest(t)
	ctx := context.Background()
	const tenant = "example.com/tenant"
	confirmed := lodiav1alpha1.DeletionConfirmedAnnotation
	objects := recipe(t, "### Deleting a namespace that holds tenants\n")
	namespace := func(name string, labels, annotations map[string]string) *corev1.Namespace {
		return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, Annotations: annotations}}
	}
	message := func(name string) string {
		return "namespace " + name + " holds tenants: kubectl annotate namespace " + name + " " + confirmed + "=true"
	}
	probe := namespace("recipe-probe", map[string]string{tenant: "probe"}, nil)
	if err := k8s.Create(ctx, probe); err != nil {
		t.Fatal(err)
	}
	for _, obj := range objects {
		if err := k8s.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		ctx := context.Background()
		for _, i := range []int{1, 0} {
			if err := k8s.Delete(ctx, objects[i]); err != nil {
				t.Error(err)
			}
		}
		err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			return k8s.Delete(ctx, probe, client.DryRunAll) == nil, nil
		})
		if err != nil {
			t.Errorf("a tenant namespace is still refused after the policy was deleted: %v", err)
		}
	})
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		return apierrors.IsForbidden(k8s.Delete(ctx, probe, client.DryRunAll)), nil
	})
	if err != nil {
		t.Fatalf("the policy never refused the deletion of a tenant namespace: %v", err)
	}

	for _, c := range []struct {
		name                string
		labels, annotations map[string]string
		admitted            bool
	}{
		{name: "a tenant namespace", labels: map[string]string{tenant: "team-a"}},
		{name: "a tenant namespace confirmed false", labels: map[string]string{tenant: "team-a"}, annotations: map[string]string{confirmed: "false"}},
		{name: "a tenant namespace confirmed", labels: map[string]string{tenant: "team-a"}, annotations: map[string]string{confirmed: "true"}, admitted: true},
		{name: "a namespace without the label", admitted: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			name := "recipe-" + strings.ReplaceAll(c.name, " ", "-")
			ns := namespace(name, c.labels, c.annotations)
			if err := k8s.Create(ctx, ns); err != nil {
				t.Fatal(err)
			}
			if c.admitted {
				if err := k8s.Delete(ctx, ns, client.DryRunAll); err != nil {
					t.Fatalf("a dry run: want admitted, got %v", err)
				}
				if err := k8s.Delete(ctx, ns); err != nil {
					t.Fatalf("want admitted, got %v", err)
				}
				return
			}
			for _, opts := range [][]client.DeleteOption{{client.DryRunAll}, nil} {
				if err := k8s.Delete(ctx, ns, opts...); !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), message(name)) {
					t.Fatalf("want Forbidden with %q, got %v", message(name), err)
				}
			}
			if err := k8s.Get(ctx, client.ObjectKey{Name: name}, ns); err != nil || !ns.DeletionTimestamp.IsZero() {
				t.Fatalf("the namespace the policy refused: %v", err)
			}
		})
	}
}
