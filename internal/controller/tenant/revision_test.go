// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/pki"
	"go.lodia.io/lodia/internal/controller/tenant"
	"go.lodia.io/lodia/internal/testenv"
)

type unpaused struct {
	client.Client
}

func (c unpaused) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := c.Client.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if cp, ok := obj.(*lodiav1alpha1.ControlPlane); ok {
		cp.Spec.Paused = false
	}
	return nil
}

func runningOperator() *tenant.Reconciler {
	return &tenant.Reconciler{Client: unpaused{k8s}, Reader: k8s, ApplyPeriod: time.Hour, TunnelImage: tunnelImage, Revision: revision, Dial: *dial.Load()}
}

func passWith(t *testing.T, r *tenant.Reconciler, cp *lodiav1alpha1.ControlPlane) error {
	t.Helper()
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	return err
}

func heldPass(t *testing.T, cp *lodiav1alpha1.ControlPlane) {
	t.Helper()
	if err := passWith(t, runningOperator(), cp); err != nil {
		t.Fatal(err)
	}
}

func heldTenant(t *testing.T, namespace string) (*lodiav1alpha1.ControlPlane, *tenantClient) {
	t.Helper()
	cp, admin := servedTenant(t, namespace)
	applied(t, cp)
	pause(t, cp)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	setTarget(t, cp, version, previous)
	refresh(t, cp)
	return cp, admin
}

func setAddonsApplied(t *testing.T, cp *lodiav1alpha1.ControlPlane, status metav1.ConditionStatus, reason string, generation int64) {
	t.Helper()
	applyStatus(t, cp, "test/"+lodiav1alpha1.ConditionAddonsApplied, map[string]any{"conditions": []any{conditionAt(lodiav1alpha1.ConditionAddonsApplied, status, reason, generation)}})
}

func expectAt(t *testing.T, cp *lodiav1alpha1.ControlPlane, conditionType string, status metav1.ConditionStatus, reason string, generation int64) {
	t.Helper()
	if c := condition(t, cp, conditionType); c.Status != status || c.Reason != reason || c.ObservedGeneration != generation {
		t.Errorf("%s is %s %s at generation %d (%s), want %s %s at %d", conditionType, c.Status, c.Reason, c.ObservedGeneration, c.Message, status, reason, generation)
	}
}

func expectSame(t *testing.T, cp *lodiav1alpha1.ControlPlane, before metav1.Condition) {
	t.Helper()
	if after := *condition(t, cp, before.Type); !equality.Semantic.DeepEqual(after, before) {
		t.Errorf("%s went from %+v to %+v", before.Type, before, after)
	}
}

func resourceVersions(t *testing.T, admin *tenantClient, entries []string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, entry := range append(slices.Clone(entries), "ConfigMap kube-system/lodia-inventory") {
		obj, err := lookup(t, admin, entry)
		switch {
		case apierrors.IsNotFound(err):
			out[entry] = "missing"
		case err != nil:
			t.Fatal(err)
		default:
			out[entry] = obj.GetResourceVersion()
		}
	}
	return out
}

func editByHand(t *testing.T, admin *tenantClient, entries []string) {
	t.Helper()
	for _, entry := range entries {
		patch := client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"labels":{"app.kubernetes.io/managed-by":"someone"}}}`))
		if err := admin.Patch(t.Context(), objectOf(t, admin, entry), patch, client.FieldOwner("test/hand")); err != nil {
			t.Fatal(err)
		}
	}
}

func appliedByLodia(obj client.Object) bool {
	return slices.ContainsFunc(obj.GetManagedFields(), func(f metav1.ManagedFieldsEntry) bool {
		return f.Manager == tenant.FieldManager && f.Operation == metav1.ManagedFieldsOperationApply
	})
}

type inventoryEntry = map[string]string

func storedInventory(t *testing.T, admin *tenantClient) []inventoryEntry {
	t.Helper()
	var entries []inventoryEntry
	if err := json.Unmarshal([]byte(get(t, admin, &corev1.ConfigMap{}, "kube-system", "lodia-inventory").Data["objects"]), &entries); err != nil {
		t.Fatal(err)
	}
	return entries
}

func writeInventory(t *testing.T, admin *tenantClient, entries []inventoryEntry) {
	t.Helper()
	objects, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	setInventory(t, admin, string(objects))
}

func withoutCoreDNS(entries []inventoryEntry, kind string) []inventoryEntry {
	return slices.DeleteFunc(slices.Clone(entries), func(e inventoryEntry) bool { return e["kind"] == kind && e["name"] == "coredns" })
}

func deleteObject(t *testing.T, admin *tenantClient, entry string) {
	t.Helper()
	if err := admin.Delete(t.Context(), objectOf(t, admin, entry)); err != nil {
		t.Fatal(err)
	}
}

var leftTheRender = inventoryEntry{"group": "rbac.authorization.k8s.io", "version": "v1", "kind": "ClusterRole", "name": "left-the-render"}

func rollOut(t *testing.T, admin *tenantClient, names ...string) {
	t.Helper()
	for _, name := range names {
		ds := get(t, admin, &appsv1.DaemonSet{}, "kube-system", name)
		ds.Status = appsv1.DaemonSetStatus{ObservedGeneration: ds.Generation}
		if err := admin.Status().Update(t.Context(), ds); err != nil {
			t.Fatal(err)
		}
	}
}

func image(t *testing.T, admin *tenantClient, daemonSet string) string {
	t.Helper()
	return get(t, admin, &appsv1.DaemonSet{}, "kube-system", daemonSet).Spec.Template.Spec.Containers[0].Image
}

func TestAHeldPassWritesNothingThatExistsAndTheGrantAppliesTheRender(t *testing.T) {
	ctx := t.Context()
	cp, admin := heldTenant(t, "tenant-held-writes")
	generation := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied).ObservedGeneration
	if generation == cp.Generation {
		t.Fatalf("AddonsApplied describes generation %d, which the pause should have moved past", generation)
	}
	if err := admin.Apply(ctx, rbacv1ac.ClusterRole("left-the-render").WithLabels(map[string]string{lodiav1alpha1.ManagedByLabel: lodiav1alpha1.ManagedBy}).WithRules(rbacv1ac.PolicyRule().WithAPIGroups("").WithResources("configmaps").WithVerbs("get")), client.FieldOwner(tenant.FieldManager)); err != nil {
		t.Fatal(err)
	}
	writeInventory(t, admin, append(withoutCoreDNS(storedInventory(t, admin), "Deployment"), leftTheRender))
	deleteObject(t, admin, "Deployment kube-system/coredns")
	setTarget(t, cp, "v1.35.9", previous)
	listed := inventoryOf(t, admin)
	editByHand(t, admin, listed)
	before := resourceVersions(t, admin, listed)
	addons := *storedStatus(t, cp).Addons

	heldPass(t, cp)
	heldPass(t, cp)
	if after := resourceVersions(t, admin, listed); !equality.Semantic.DeepEqual(after, before) {
		for entry, rv := range before {
			if after[entry] != rv {
				t.Errorf("a held pass wrote %s: resourceVersion %s, then %s", entry, rv, after[entry])
			}
		}
	}
	if exists(t, admin, "Deployment kube-system/coredns") {
		t.Error("a held pass created the Deployment the render adds and the inventory does not list")
	}
	if !exists(t, admin, "ClusterRole left-the-render") {
		t.Error("a held pass pruned the stale entry")
	}
	if got := image(t, admin, "kube-proxy"); got != "registry.k8s.io/kube-proxy:"+version {
		t.Errorf("kube-proxy runs %s under the hold, want the stored render's %s", got, version)
	}
	expectAt(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, generation)
	if got := *storedStatus(t, cp).Addons; got != addons {
		t.Errorf("status.addons went from %+v to %+v on a held pass", addons, got)
	}

	setTarget(t, cp, "v1.35.9", revision)
	heldPass(t, cp)
	if got := image(t, admin, "kube-proxy"); got != "registry.k8s.io/kube-proxy:v1.35.9" {
		t.Errorf("kube-proxy runs %s after the grant, want v1.35.9", got)
	}
	if !exists(t, admin, "Deployment kube-system/coredns") {
		t.Error("the grant did not create the Deployment the render adds")
	}
	if exists(t, admin, "ClusterRole left-the-render") {
		t.Error("the grant did not prune the stale entry")
	}
	inventory := inventoryOf(t, admin)
	if !slices.Contains(inventory, "Deployment kube-system/coredns") || slices.Contains(inventory, "ClusterRole left-the-render") {
		t.Errorf("after the grant the inventory lists %v", inventory)
	}
	after := resourceVersions(t, admin, inventory)
	for _, entry := range inventory {
		if before[entry] != "" && after[entry] == before[entry] {
			t.Errorf("%s kept resourceVersion %s through the grant, so the hold proved nothing about it", entry, after[entry])
		}
	}
	expectAt(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation)
	if got := *storedStatus(t, cp).Addons; got != (lodiav1alpha1.Rendered{Version: "v1.35.9", OperatorRevision: revision}) {
		t.Errorf("status.addons is %+v after the grant, want v1.35.9 at %s", got, revision)
	}
}

func storedStatus(t *testing.T, cp *lodiav1alpha1.ControlPlane) *lodiav1alpha1.ControlPlaneStatus {
	t.Helper()
	refresh(t, cp)
	if cp.Status.Addons == nil {
		t.Fatal("status.addons is absent")
	}
	return &cp.Status
}

func TestAHeldPassCreatesOnlyWhatTheInventoryListsAndTheTenantLacks(t *testing.T) {
	cp, admin := heldTenant(t, "tenant-held-creates")
	applied, available := *condition(t, cp, lodiav1alpha1.ConditionAddonsApplied), *condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable)
	setTarget(t, cp, "v1.35.9", previous)
	listed := inventoryOf(t, admin)
	deleteObject(t, admin, "Deployment kube-system/coredns")
	before := resourceVersions(t, admin, listed)

	heldPass(t, cp)
	coreDNS := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns")
	if !appliedByLodia(coreDNS) {
		t.Errorf("the missing Deployment was created without an apply as %s: %+v", tenant.FieldManager, coreDNS.ManagedFields)
	}
	after := resourceVersions(t, admin, listed)
	for entry, rv := range before {
		if entry != "Deployment kube-system/coredns" && after[entry] != rv {
			t.Errorf("creating the missing Deployment also wrote %s", entry)
		}
	}

	deleteObject(t, admin, "ConfigMap kube-system/lodia-inventory")
	deleteObject(t, admin, "ConfigMap kube-system/kube-proxy")
	heldPass(t, cp)
	for _, entry := range []string{"ConfigMap kube-system/lodia-inventory", "ConfigMap kube-system/kube-proxy"} {
		if exists(t, admin, entry) {
			t.Errorf("a held pass without an inventory created %s", entry)
		}
	}
	expectSame(t, cp, applied)
	expectSame(t, cp, available)
}

func TestAHeldCreateTheTenantRefusesIsRefusedAndTheNextPassFinishesIt(t *testing.T) {
	ctx := t.Context()
	cp, admin := heldTenant(t, "tenant-held-refused")
	applied := *condition(t, cp, lodiav1alpha1.ConditionAddonsApplied)
	available := *condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable)
	deleteObject(t, admin, "ConfigMap kube-system/coredns")
	deleteObject(t, admin, "Deployment kube-system/coredns")
	binding := get(t, admin, &rbacv1.ClusterRoleBinding{}, "", "lodia:operator")
	denied := authorizationv1.ResourceAttributes{Group: appsv1.GroupName, Resource: "deployments", Verb: "patch", Namespace: "kube-system"}
	rebindOperator(t, admin, []rbacv1.PolicyRule{
		{APIGroups: []string{"", "discovery.k8s.io", rbacv1.GroupName}, Resources: []string{"*"}, Verbs: []string{"*"}},
		{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"get", "list", "watch"}},
		{NonResourceURLs: []string{"*"}, Verbs: []string{"*"}},
	}, denied)

	if err := passWith(t, runningOperator(), cp); !apierrors.IsForbidden(err) {
		t.Fatalf("a held pass whose create the tenant refuses returned %v, want the refusal", err)
	}
	expectAt(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, applied.ObservedGeneration)
	if c := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); !strings.HasPrefix(c.Message, "Deployment kube-system/coredns is refused by the tenant's API server") {
		t.Errorf("AddonsApplied says %q", c.Message)
	}
	expectSame(t, cp, available)
	created := get(t, admin, &corev1.ConfigMap{}, "kube-system", "coredns")
	if !appliedByLodia(created) {
		t.Error("the ConfigMap created before the refused Deployment is not Lodia's apply")
	}

	if err := admin.Delete(ctx, binding.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if err := admin.Create(ctx, &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: binding.Name}, RoleRef: binding.RoleRef, Subjects: binding.Subjects}); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{User: "lodia:operator", ResourceAttributes: &denied}}
		if err := admin.Create(ctx, review); err != nil {
			return err
		}
		if !review.Status.Allowed {
			return fmt.Errorf("lodia:operator may not %s %s yet", denied.Verb, denied.Resource)
		}
		return nil
	})
	heldPass(t, cp)
	if !exists(t, admin, "Deployment kube-system/coredns") {
		t.Error("the next held pass did not create the Deployment")
	}
	if got := get(t, admin, &corev1.ConfigMap{}, "kube-system", "coredns").ResourceVersion; got != created.ResourceVersion {
		t.Errorf("the next held pass wrote the ConfigMap the refused pass created: resourceVersion %s, then %s", created.ResourceVersion, got)
	}
	expectAt(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, applied.ObservedGeneration)
}

func TestAHeldCreateTheTenantFailsWithoutARefusalChangesNothing(t *testing.T) {
	ctx := t.Context()
	cp, admin := heldTenant(t, "tenant-held-create-fails")
	applied, available := *condition(t, cp, lodiav1alpha1.ConditionAddonsApplied), *condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable)
	deleteObject(t, admin, "Deployment kube-system/coredns")
	if err := admin.Create(ctx, &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "unreachable"},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name:         "unreachable.example.test",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{URL: new("https://127.0.0.1:1/")},
			Rules: []admissionregistrationv1.RuleWithOperations{{
				Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
				Rule:       admissionregistrationv1.Rule{APIGroups: []string{appsv1.GroupName}, APIVersions: []string{"v1"}, Resources: []string{"deployments"}},
			}},
			FailurePolicy:           new(admissionregistrationv1.Fail),
			SideEffects:             new(admissionregistrationv1.SideEffectClassNone),
			AdmissionReviewVersions: []string{"v1"},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	testenv.Eventually(t, func() error {
		err := admin.Create(ctx, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "probe"},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "probe"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "probe"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "probe", Image: "probe"}}},
				},
			},
		}, client.DryRunAll)
		if err == nil {
			return errors.New("the webhook does not fail a Deployment's create yet")
		}
		if !apierrors.IsInternalError(err) {
			return err
		}
		return nil
	})
	refresh(t, cp)
	written := cp.ResourceVersion

	if err := passWith(t, runningOperator(), cp); !apierrors.IsInternalError(err) {
		t.Errorf("a held pass whose create the tenant fails without a refusal returned %v, want the failure", err)
	}
	expectSame(t, cp, applied)
	expectSame(t, cp, available)
	if cp.ResourceVersion != written {
		t.Errorf("a held pass whose create failed without a refusal wrote the ControlPlane: resourceVersion %s, then %s", written, cp.ResourceVersion)
	}
	if exists(t, admin, "Deployment kube-system/coredns") {
		t.Error("the Deployment exists although its create failed")
	}
}

func TestAHeldPassKeepsAddonsAppliedUnlessAWaitCoversWhatRuns(t *testing.T) {
	cp, admin := heldTenant(t, "tenant-held-applied")
	generation := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied).ObservedGeneration
	setReplicas(t, cp, 3)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	heldPass(t, cp)
	expectAt(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, generation)
	if replicas := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns").Spec.Replicas; replicas == nil || *replicas != 2 {
		t.Errorf("a held pass rendered the owner's edit: CoreDNS has %v replicas", replicas)
	}

	for _, kept := range []struct {
		status metav1.ConditionStatus
		reason string
	}{
		{metav1.ConditionFalse, lodiav1alpha1.ReasonRefused},
		{metav1.ConditionFalse, lodiav1alpha1.ReasonApplying},
		{metav1.ConditionUnknown, lodiav1alpha1.ReasonReconciling},
	} {
		setAddonsApplied(t, cp, kept.status, kept.reason, generation)
		heldPass(t, cp)
		expectAt(t, cp, lodiav1alpha1.ConditionAddonsApplied, kept.status, kept.reason, generation)
	}

	inventory, listed := storedInventory(t, admin), inventoryOf(t, admin)
	for _, c := range []struct {
		name            string
		breaks, repairs func()
	}{
		{"the running operator cannot render", func() {
			setComponents(t, cp, running(version), running("v1.34.2"), running(version))
		}, func() { setComponents(t, cp, nil, nil, nil) }},
		{"status.addons has no version", func() {
			applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
				conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, generation),
				conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending, generation),
			}})
			if got := storedAddons(t, cp); got != "absent" {
				t.Fatalf("status.addons is %s", got)
			}
		}, func() {
			applyStatus(t, cp, tenant.FieldManager, map[string]any{
				"conditions": []any{
					conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, generation),
					conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending, generation),
				},
				"addons": map[string]any{"version": version, "operatorRevision": revision},
			})
		}},
		{"the inventory is empty", func() { writeInventory(t, admin, []inventoryEntry{}) }, func() { writeInventory(t, admin, inventory) }},
		{"the inventory is gone", func() { deleteObject(t, admin, "ConfigMap kube-system/lodia-inventory") }, func() {
			objects, err := json.Marshal(inventory)
			if err != nil {
				t.Fatal(err)
			}
			if err := admin.Create(t.Context(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "lodia-inventory"}, Data: map[string]string{"objects": string(objects)}}); err != nil {
				t.Fatal(err)
			}
		}},
		{"a listed object is a person's", func() {
			deleteObject(t, admin, "ConfigMap kube-system/kube-proxy")
			if err := admin.Create(t.Context(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "kube-proxy"}, Data: map[string]string{"config.conf": "a person's"}}); err != nil {
				t.Fatal(err)
			}
		}, func() { deleteObject(t, admin, "ConfigMap kube-system/kube-proxy") }},
		{"a listed object the render no longer has is gone", func() { writeInventory(t, admin, append(slices.Clone(inventory), leftTheRender)) }, func() { writeInventory(t, admin, inventory) }},
		{"an object of the render the inventory does not list is a person's", func() {
			writeInventory(t, admin, withoutCoreDNS(inventory, "ServiceAccount"))
			deleteObject(t, admin, "ServiceAccount kube-system/coredns")
			if err := admin.Create(t.Context(), &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns"}}); err != nil {
				t.Fatal(err)
			}
		}, func() {
			deleteObject(t, admin, "ServiceAccount kube-system/coredns")
			writeInventory(t, admin, inventory)
		}},
	} {
		setAddonsApplied(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, generation)
		c.breaks()
		before := resourceVersions(t, admin, listed)
		heldPass(t, cp)
		if got := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); got.Status != metav1.ConditionFalse || got.Reason != lodiav1alpha1.ReasonWaitingForAccess || got.ObservedGeneration != generation {
			t.Errorf("when %s, AddonsApplied is %s %s at generation %d (%s), want the wait at %d", c.name, got.Status, got.Reason, got.ObservedGeneration, got.Message, generation)
		}
		for entry, rv := range resourceVersions(t, admin, listed) {
			if before[entry] != "missing" && rv != before[entry] {
				t.Errorf("when %s, the held pass wrote %s", c.name, entry)
			}
		}
		c.repairs()
	}

	want := fmt.Sprintf("the %d objects the inventory lists are applied; waiting for Lifecycle to grant the running operator revision %s", len(inventory), revision)
	for _, reason := range []string{lodiav1alpha1.ReasonWaitingForAccess, lodiav1alpha1.ReasonWaitingForTarget, lodiav1alpha1.ReasonWaitingForEndpoint} {
		setAddonsApplied(t, cp, metav1.ConditionFalse, reason, generation)
		heldPass(t, cp)
		expectAt(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, generation)
		if c := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied); c.Message != want {
			t.Errorf("over %s AddonsApplied says %q, want %q", reason, c.Message, want)
		}
	}
	if !appliedByLodia(get(t, admin, &corev1.ConfigMap{}, "kube-system", "kube-proxy")) {
		t.Error("the held pass did not create the listed ConfigMap a person's deletion left missing")
	}
}

func TestAHeldPassReadsTheRolloutOfWhatTheInventoryListsAndChangesNothingItCannotRead(t *testing.T) {
	ctx := t.Context()
	cp, admin := heldTenant(t, "tenant-held-rollout")
	generation := condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable).ObservedGeneration
	inventory := storedInventory(t, admin)
	writeInventory(t, admin, withoutCoreDNS(inventory, "Deployment"))
	deleteObject(t, admin, "Deployment kube-system/coredns")
	rollOut(t, admin, "kube-proxy", "konnectivity-agent")

	heldPass(t, cp)
	expectAt(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable, generation)
	if c := condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable); c.Message != "konnectivity-agent, kube-proxy are rolled out" {
		t.Errorf("AddonsAvailable says %q", c.Message)
	}

	writeInventory(t, admin, append(withoutCoreDNS(inventory, "Deployment"), inventoryEntry{"group": "apps", "version": "v1", "kind": "DaemonSet", "namespace": "kube-system", "name": "old-proxy"}))
	heldPass(t, cp)
	expectAt(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending, generation)
	if c := condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable); c.Message != "old-proxy is missing" {
		t.Errorf("AddonsAvailable says %q", c.Message)
	}

	missing := *condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable)
	writeInventory(t, admin, []inventoryEntry{})
	heldPass(t, cp)
	expectSame(t, cp, missing)

	writeInventory(t, admin, withoutCoreDNS(inventory, "Deployment"))
	setAddonsApplied(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, generation)
	applied := *condition(t, cp, lodiav1alpha1.ConditionAddonsApplied)
	written := cp.ResourceVersion
	reaches := *dial.Load()
	late := func(ctx context.Context, network, address string) (net.Conn, error) {
		time.Sleep(3 * time.Second)
		return reaches(ctx, network, address)
	}
	for name, c := range map[string]struct {
		dial    dialFunc
		timeout time.Duration
		failed  func(error) bool
	}{
		"the tenant does not answer":         {unreachable, time.Minute, func(err error) bool { return err != nil }},
		"the pass's deadline passes mid-way": {late, 2 * time.Second, func(err error) bool { return errors.Is(err, context.DeadlineExceeded) }},
	} {
		t.Run(name, func(t *testing.T) {
			r := runningOperator()
			r.Dial = c.dial
			ctx, cancel := context.WithTimeout(ctx, c.timeout)
			defer cancel()
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !c.failed(err) {
				t.Errorf("the pass returned %v", err)
			}
			expectSame(t, cp, applied)
			expectSame(t, cp, missing)
			if cp.ResourceVersion != written {
				t.Errorf("the pass wrote the ControlPlane: resourceVersion %s, then %s", written, cp.ResourceVersion)
			}
		})
	}

	rebindOperator(t, admin, []rbacv1.PolicyRule{
		{APIGroups: []string{"", "discovery.k8s.io", rbacv1.GroupName}, Resources: []string{"*"}, Verbs: []string{"*"}},
		{APIGroups: []string{appsv1.GroupName}, Resources: []string{"deployments"}, Verbs: []string{"*"}},
		{APIGroups: []string{appsv1.GroupName}, Resources: []string{"daemonsets"}, Verbs: []string{"create", "patch", "update", "delete", "list", "watch"}},
		{NonResourceURLs: []string{"*"}, Verbs: []string{"*"}},
	}, authorizationv1.ResourceAttributes{Group: appsv1.GroupName, Resource: "daemonsets", Verb: "get", Namespace: "kube-system"})
	listed := inventoryOf(t, admin)
	editByHand(t, admin, listed)
	before := resourceVersions(t, admin, listed)
	if err := passWith(t, runningOperator(), cp); !apierrors.IsForbidden(err) {
		t.Errorf("a held pass whose read of a listed DaemonSet is refused returned %v, want the refusal", err)
	}
	expectSame(t, cp, applied)
	expectSame(t, cp, missing)
	if cp.ResourceVersion != written {
		t.Errorf("a held pass that could not read a listed DaemonSet wrote the ControlPlane: resourceVersion %s, then %s", written, cp.ResourceVersion)
	}
	if after := resourceVersions(t, admin, listed); !equality.Semantic.DeepEqual(after, before) {
		t.Errorf("a held pass that could not read a listed DaemonSet wrote the tenant: %v, then %v", before, after)
	}

	setComponents(t, cp, running(version), running("v1.34.2"), running(version))
	refresh(t, cp)
	written = cp.ResourceVersion
	if err := passWith(t, runningOperator(), cp); !apierrors.IsForbidden(err) {
		t.Errorf("a held pass that cannot render, whose rollout read of a listed DaemonSet is refused, returned %v, want the refusal", err)
	}
	expectSame(t, cp, applied)
	expectSame(t, cp, missing)
	if cp.ResourceVersion != written {
		t.Errorf("a held pass that cannot render and could not read a listed DaemonSet wrote the ControlPlane: resourceVersion %s, then %s", written, cp.ResourceVersion)
	}
}

func TestAHeldPassOutsideTheRunningTableReportsWhatRuns(t *testing.T) {
	cp, admin := heldTenant(t, "tenant-held-outside")
	generation := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied).ObservedGeneration
	rollOut(t, admin, "kube-proxy", "konnectivity-agent")
	for name, c := range map[string]struct{ outside, back func() }{
		"a report": {
			func() { setComponents(t, cp, running(version), running("v1.34.2"), running(version)) },
			func() { setComponents(t, cp, nil, nil, nil) },
		},
		"the target": {
			func() { setTarget(t, cp, "v1.38.0", previous) },
			func() { setTarget(t, cp, version, previous) },
		},
	} {
		t.Run(name, func(t *testing.T) {
			setAddonsApplied(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, generation)
			deleteObject(t, admin, "Deployment kube-system/coredns")
			c.outside()
			heldPass(t, cp)
			expectAt(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, generation)
			expectAt(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonRolloutPending, generation)
			if got := condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable).Message; got != "coredns is missing" {
				t.Errorf("AddonsAvailable says %q", got)
			}
			if exists(t, admin, "Deployment kube-system/coredns") {
				t.Error("a pass that cannot render created the missing Deployment")
			}
			c.back()
			heldPass(t, cp)
			if !exists(t, admin, "Deployment kube-system/coredns") {
				t.Error("a held pass that can render again did not create the missing Deployment")
			}
		})
	}
}

func TestWithoutAGrantedRevisionTenantWaitsForTheTarget(t *testing.T) {
	cp := controlPlane(t, "tenant-no-revision")
	publish(t, cp)
	reconcilePKI(t, &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour, Clock: clock.RealClock{}}, cp)
	testenv.EventuallyWithin(t, 10*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	})
	pause(t, cp)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	applyStatus(t, cp, "test/target", map[string]any{"target": map[string]any{"version": version}})
	appliedAtTheGeneration(t, cp)
	r := runningOperator()
	r.Dial = unreachable
	if err := passWith(t, r, cp); err != nil {
		t.Fatalf("the pass returned %v, which a wait does not", err)
	}
	for _, conditionType := range []string{lodiav1alpha1.ConditionAddonsApplied, lodiav1alpha1.ConditionAddonsAvailable} {
		expectAt(t, cp, conditionType, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForTarget, cp.Generation)
		if got := condition(t, cp, conditionType).Message; got != "status.target has no operatorRevision" {
			t.Errorf("%s says %q", conditionType, got)
		}
	}
}

func TestAWaitKeepsARefusalAndAnUnfinishedApply(t *testing.T) {
	cp := controlPlane(t, "tenant-wait-keeps", paused)
	publish(t, cp)
	for _, granted := range []string{revision, previous} {
		for wait, c := range map[string]struct {
			access          metav1.ConditionStatus
			reason, message string
		}{
			"for access":                     {metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAPIServer, "AccessReady is not True for this generation"},
			"for a Secret Tenant cannot use": {metav1.ConditionTrue, lodiav1alpha1.ReasonBound, "Secret dev-ca-bundle does not exist"},
		} {
			t.Run(fmt.Sprintf("%s, %s granted", wait, granted), func(t *testing.T) {
				setTarget(t, cp, version, granted)
				refresh(t, cp)
				applyStatus(t, cp, "test/"+lodiav1alpha1.ConditionAccessReady, map[string]any{"conditions": []any{
					conditionAt(lodiav1alpha1.ConditionAccessReady, c.access, c.reason, cp.Generation),
				}})
				for _, before := range []struct {
					status metav1.ConditionStatus
					reason string
					kept   bool
				}{
					{metav1.ConditionFalse, lodiav1alpha1.ReasonRefused, true},
					{metav1.ConditionFalse, lodiav1alpha1.ReasonApplying, true},
					{metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, false},
				} {
					setAddonsApplied(t, cp, before.status, before.reason, cp.Generation)
					applyStatus(t, cp, "test/"+lodiav1alpha1.ConditionAddonsAvailable, map[string]any{"conditions": []any{
						conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable, cp.Generation),
					}})
					if err := passWith(t, runningOperator(), cp); err != nil {
						t.Fatal(err)
					}
					if before.kept {
						expectAt(t, cp, lodiav1alpha1.ConditionAddonsApplied, before.status, before.reason, cp.Generation)
					} else {
						expectAt(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, cp.Generation)
						if got := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied).Message; got != c.message {
							t.Errorf("over %s AddonsApplied says %q, want %q", before.reason, got, c.message)
						}
					}
					expectAt(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess, cp.Generation)
				}
			})
		}
	}
}

type hidingSecret struct {
	client.Reader
	name string
}

func (r hidingSecret) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok && key.Name == r.name {
		return apierrors.NewNotFound(corev1.Resource("secrets"), key.Name)
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

func TestAWaitAtANewGenerationKeepsBothConditions(t *testing.T) {
	cp := controlPlane(t, "tenant-wait-generation")
	publish(t, cp)
	reconcilePKI(t, &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour, Clock: clock.RealClock{}}, cp)
	testenv.EventuallyWithin(t, 10*time.Second, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
	})
	pause(t, cp)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
		conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation-1),
		conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable, cp.Generation-1),
	}})
	applied, available := *condition(t, cp, lodiav1alpha1.ConditionAddonsApplied), *condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable)
	for _, c := range []struct {
		name            string
		reader          client.Reader
		breaks, repairs func()
	}{
		{"AccessReady is not True", k8s, func() {
			applyStatus(t, cp, "test/"+lodiav1alpha1.ConditionAccessReady, map[string]any{"conditions": []any{
				conditionAt(lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAPIServer, cp.Generation),
			}})
		}, func() { setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound) }},
		{"a Secret Tenant cannot use", hidingSecret{Reader: k8s, name: "dev-ca-bundle"}, func() {}, func() {}},
		{"status.target has no version", k8s, func() { applyStatus(t, cp, "test/target", map[string]any{}) }, func() { setTarget(t, cp, version, revision) }},
		{"status.target has no operatorRevision", k8s, func() {
			applyStatus(t, cp, "test/target", map[string]any{"target": map[string]any{"version": version}})
		}, func() { setTarget(t, cp, version, revision) }},
		{"status.endpoint is not published", k8s, func() { applyStatus(t, cp, "test/endpoint", map[string]any{}) }, func() { publish(t, cp) }},
		{"a component runs a version outside the table", k8s, func() {
			setComponents(t, cp, running(version), running("v1.34.2"), running(version))
		}, func() { setComponents(t, cp, nil, nil, nil) }},
	} {
		c.breaks()
		refresh(t, cp)
		written := cp.ResourceVersion
		r := runningOperator()
		r.Reader, r.Dial = c.reader, unreachable
		if err := passWith(t, r, cp); err != nil {
			t.Errorf("when %s, the pass returned %v, which a wait does not", c.name, err)
		}
		expectSame(t, cp, applied)
		expectSame(t, cp, available)
		if cp.ResourceVersion != written {
			t.Errorf("when %s, a wait at generation %d wrote the ControlPlane: resourceVersion %s, then %s", c.name, cp.Generation, written, cp.ResourceVersion)
		}
		c.repairs()
	}
}

func TestAWaitAtANewGenerationLeavesAHeldPassNoTrueToWriteThere(t *testing.T) {
	cp, admin := heldTenant(t, "tenant-held-wait-generation")
	generation := condition(t, cp, lodiav1alpha1.ConditionAddonsApplied).ObservedGeneration
	setReplicas(t, cp, 3)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	heldPass(t, cp)
	expectAt(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, generation)
	for _, granted := range []string{previous, revision} {
		setTarget(t, cp, version, granted)
		applied, available := *condition(t, cp, lodiav1alpha1.ConditionAddonsApplied), *condition(t, cp, lodiav1alpha1.ConditionAddonsAvailable)
		r := runningOperator()
		r.Reader = hidingSecret{Reader: k8s, name: "dev-ca-bundle"}
		if err := passWith(t, r, cp); err != nil {
			t.Fatal(err)
		}
		expectSame(t, cp, applied)
		expectSame(t, cp, available)

		setTarget(t, cp, version, previous)
		heldPass(t, cp)
		expectAt(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, generation)
		if replicas := get(t, admin, &appsv1.Deployment{}, "kube-system", "coredns").Spec.Replicas; replicas == nil || *replicas != 2 {
			t.Errorf("after a wait with %s granted, a held pass rendered the owner's edit: CoreDNS has %v replicas", granted, replicas)
		}
	}
}

func TestAStaleCacheWithAnOlderRevisionHoldsThePass(t *testing.T) {
	cp, admin := heldTenant(t, "tenant-held-stale")
	setTarget(t, cp, "v1.35.9", revision)
	refresh(t, cp)
	stale := cp.DeepCopy()
	stale.Status.Target.OperatorRevision = previous
	listed := inventoryOf(t, admin)
	editByHand(t, admin, listed)
	before := resourceVersions(t, admin, listed)
	r := runningOperator()
	r.Client = snapshot{Client: k8s, cp: stale}
	if err := passWith(t, r, cp); err != nil {
		t.Fatal(err)
	}
	if after := resourceVersions(t, admin, listed); !equality.Semantic.DeepEqual(after, before) {
		t.Errorf("a pass on a cache that holds the previous revision wrote the tenant: %v, then %v", before, after)
	}
	if got := storedStatus(t, cp).Addons.Version; got != version {
		t.Errorf("status.addons.version is %s after a held pass, want %s", got, version)
	}

	heldPass(t, cp)
	if got := image(t, admin, "kube-proxy"); got != "registry.k8s.io/kube-proxy:v1.35.9" {
		t.Errorf("kube-proxy runs %s once the cache holds the grant, want v1.35.9", got)
	}
	if got := *storedStatus(t, cp).Addons; got != (lodiav1alpha1.Rendered{Version: "v1.35.9", OperatorRevision: revision}) {
		t.Errorf("status.addons is %+v once the cache holds the grant", got)
	}
}

func TestARenewalReachesAHeldTenant(t *testing.T) {
	cp, admin, _ := gatewayTenant(t, "tenant-held-delivery")
	applied(t, cp)
	pause(t, cp)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	setTarget(t, cp, version, previous)
	before := get(t, admin, &corev1.ConfigMap{}, "kube-system", "lodia-inventory").ResourceVersion
	renew(t, cp, "under a hold")
	heldPass(t, cp)
	if err := holds(t, admin, "under a hold"); err != nil {
		t.Error(err)
	}
	if after := get(t, admin, &corev1.ConfigMap{}, "kube-system", "lodia-inventory").ResourceVersion; after != before {
		t.Errorf("the held pass wrote the inventory: resourceVersion %s, then %s", before, after)
	}
}
