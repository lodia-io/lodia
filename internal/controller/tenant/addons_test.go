// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/pki"
	"go.lodia.io/lodia/internal/controller/tenant"
	"go.lodia.io/lodia/internal/testenv"
)

const older = "v1.35.7"

type recorder struct {
	client.Client
	made *[]lodiav1alpha1.ControlPlaneStatus
	fail error
}

func (c recorder) Status() client.SubResourceWriter {
	return recording{SubResourceWriter: c.Client.Status(), made: c.made, fail: c.fail}
}

type recording struct {
	client.SubResourceWriter
	made *[]lodiav1alpha1.ControlPlaneStatus
	fail error
}

func (w recording) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	raw, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	cp := &lodiav1alpha1.ControlPlane{}
	if err := json.Unmarshal(raw, cp); err != nil {
		return err
	}
	*w.made = append(*w.made, cp.Status)
	if w.fail != nil {
		return w.fail
	}
	return w.SubResourceWriter.Apply(ctx, obj, opts...)
}

func pass(t *testing.T, cp, cached *lodiav1alpha1.ControlPlane, dial dialFunc, fail error) ([]lodiav1alpha1.ControlPlaneStatus, error) {
	t.Helper()
	var made []lodiav1alpha1.ControlPlaneStatus
	r := &tenant.Reconciler{Client: recorder{Client: snapshot{Client: k8s, cp: cached}, made: &made, fail: fail}, Reader: k8s, Revision: revision, ApplyPeriod: time.Hour, Dial: dial}
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	return made, err
}

func paused(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true }

func setAddons(t *testing.T, cp *lodiav1alpha1.ControlPlane, version string) {
	t.Helper()
	applyStatus(t, cp, "test/addons", map[string]any{"addons": map[string]any{"version": version, "operatorRevision": previous}})
}

func storedAddons(t *testing.T, cp *lodiav1alpha1.ControlPlane) string {
	t.Helper()
	refresh(t, cp)
	if cp.Status.Addons == nil {
		return "absent"
	}
	return cp.Status.Addons.Version
}

func expectAddons(t *testing.T, made []lodiav1alpha1.ControlPlaneStatus, version, rev string) {
	t.Helper()
	for i, s := range made {
		if s.Addons == nil || s.Addons.Version != version || s.Addons.OperatorRevision != rev {
			t.Errorf("status apply %d of the pass carries the addons %+v, want version %s and operator revision %s", i+1, s.Addons, version, rev)
		}
	}
}

func appliedWith(made []lodiav1alpha1.ControlPlaneStatus, reason, message string) bool {
	for _, s := range made {
		if c := meta.FindStatusCondition(s.Conditions, lodiav1alpha1.ConditionAddonsApplied); c != nil && c.Reason == reason && strings.Contains(c.Message, message) {
			return true
		}
	}
	return false
}

func TestTheAddonsVersionIsTheRenderOfAPassThatAppliedInFull(t *testing.T) {
	cp, _ := servedTenant(t, "tenant-addons-full")
	testenv.Eventually(t, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
			return err
		}
		if got := storedAddons(t, cp); got != version {
			return fmt.Errorf("status.addons.version is %s, want %s", got, version)
		}
		return nil
	})
	if cp.Status.Addons.OperatorRevision != revision {
		t.Errorf("status.addons.operatorRevision is %q, want the running %s", cp.Status.Addons.OperatorRevision, revision)
	}
	owned := false
	for _, e := range cp.ManagedFields {
		if e.Subresource == "status" && e.FieldsV1 != nil && strings.Contains(e.FieldsV1.GetRawString(), `"f:addons"`) {
			owned = true
			if e.Manager != tenant.FieldManager {
				t.Errorf("status.addons is also owned by %s", e.Manager)
			}
		}
	}
	if !owned {
		t.Errorf("no manager owns status.addons: %+v", cp.ManagedFields)
	}

	setTargetVersion(t, cp, "v1.35.9")
	testenv.Eventually(t, func() error {
		if got := storedAddons(t, cp); got != "v1.35.9" {
			return fmt.Errorf("status.addons.version is %s, want the render's v1.35.9", got)
		}
		return nil
	})
}

func TestAPassThatAppliedInFullReportsEvenWhenTheRolloutCannotBeRead(t *testing.T) {
	cp, admin := servedTenant(t, "tenant-addons-rollout")
	testenv.Eventually(t, func() error {
		return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
	})
	update(t, cp, paused)
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	rebindOperator(t, admin, []rbacv1.PolicyRule{
		{APIGroups: []string{"", "discovery.k8s.io", "rbac.authorization.k8s.io"}, Resources: []string{"*"}, Verbs: []string{"*"}},
		{APIGroups: []string{"apps"}, Resources: []string{"*"}, Verbs: []string{"patch"}},
		{NonResourceURLs: []string{"*"}, Verbs: []string{"*"}},
	}, authorizationv1.ResourceAttributes{Group: "apps", Resource: "deployments", Verb: "get", Namespace: "kube-system"})
	setAddons(t, cp, older)
	refresh(t, cp)

	made, err := pass(t, cp, cp.DeepCopy(), *dial.Load(), nil)
	if err == nil {
		t.Fatal("a pass that cannot read the rollout succeeded")
	}
	if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
		t.Error(err)
	}
	expectAddons(t, made, version, revision)
	if got := storedAddons(t, cp); got != version {
		t.Errorf("status.addons.version is %s after a full apply, want %s", got, version)
	}
}

func TestAPassThatDidNotApplyInFullKeepsTheAddonsVersion(t *testing.T) {
	t.Run("the tenant refuses an object", func(t *testing.T) {
		cp, admin := servedTenant(t, "tenant-addons-refused")
		testenv.Eventually(t, func() error {
			return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
		})
		update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
		setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
		rebindOperator(t, admin, []rbacv1.PolicyRule{
			{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"get", "list", "watch"}},
			{NonResourceURLs: []string{"*"}, Verbs: []string{"*"}},
		}, authorizationv1.ResourceAttributes{Resource: "configmaps", Verb: "patch", Namespace: "kube-system"})
		setAddons(t, cp, older)
		refresh(t, cp)

		made, err := pass(t, cp, cp.DeepCopy(), *dial.Load(), nil)
		if err == nil {
			t.Fatal("a pass whose apply the tenant refuses succeeded")
		}
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonRefused); err != nil {
			t.Error(err)
		}
		expectAddons(t, made, older, previous)
		if got := storedAddons(t, cp); got != older {
			t.Errorf("status.addons.version is %s after a refused apply, want %s", got, older)
		}
	})

	t.Run("an object is another owner's", func(t *testing.T) {
		cp, admin := servedTenant(t, "tenant-addons-owners")
		setCoreDNS(t, cp, false)
		testenv.Eventually(t, func() error {
			return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied)
		})
		foreign := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns"}, Data: map[string]string{"Corefile": ".:53 { forward . 9.9.9.9 }"}}
		if err := admin.Create(t.Context(), foreign); err != nil {
			t.Fatal(err)
		}
		setCoreDNS(t, cp, true)
		testenv.Eventually(t, func() error {
			return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionFalse, lodiav1alpha1.ReasonApplying)
		})
		update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
		setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
		setAddons(t, cp, older)
		refresh(t, cp)

		made, err := pass(t, cp, cp.DeepCopy(), *dial.Load(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if !appliedWith(made, lodiav1alpha1.ReasonApplying, "ConfigMap kube-system/coredns exists and is not Lodia's") {
			t.Fatalf("the pass applied %+v, want AddonsApplied Applying for the other owner's object", made)
		}
		expectAddons(t, made, older, previous)
		if got := storedAddons(t, cp); got != older {
			t.Errorf("status.addons.version is %s beside another owner's object, want %s", got, older)
		}
	})
}

func TestAFailedStatusWriteLeavesTheAddonsVersionForTheNextPass(t *testing.T) {
	cp, _ := servedTenant(t, "tenant-addons-write")
	testenv.Eventually(t, func() error {
		if err := expectCondition(t, cp, lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied); err != nil {
			return err
		}
		if got := storedAddons(t, cp); got != version {
			return fmt.Errorf("status.addons.version is %s", got)
		}
		return nil
	})
	update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
	setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
	setAddons(t, cp, older)
	refresh(t, cp)

	injected := errors.New("injected: the API server did not answer")
	made, err := pass(t, cp, cp.DeepCopy(), *dial.Load(), injected)
	if !errors.Is(err, injected) {
		t.Fatalf("a pass whose status write fails returned %v, want the write's error", err)
	}
	if len(made) == 0 || made[len(made)-1].Addons == nil || made[len(made)-1].Addons.Version != version {
		t.Fatalf("the failed write carried %+v, want the addons at %s", made, version)
	}
	if got := storedAddons(t, cp); got != older {
		t.Errorf("status.addons.version is %s after a failed write, want %s", got, older)
	}

	if _, err := pass(t, cp, cp.DeepCopy(), *dial.Load(), nil); err != nil {
		t.Fatal(err)
	}
	if got := storedAddons(t, cp); got != version {
		t.Errorf("status.addons.version is %s after the next pass, want %s", got, version)
	}
}

func TestEveryPathThatAppliesStatusKeepsTheAddons(t *testing.T) {
	t.Run("the first pass", func(t *testing.T) {
		cp := controlPlane(t, "tenant-addons-first", paused)
		setAddons(t, cp, older)
		refresh(t, cp)
		made, err := pass(t, cp, cp.DeepCopy(), unreachable, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(made) == 0 || made[0].Conditions[0].Reason != lodiav1alpha1.ReasonReconciling {
			t.Fatalf("the pass applied %+v first, want Reconciling", made)
		}
		expectAddons(t, made, older, previous)
	})

	t.Run("AccessReady is not True, and the cache is older than the API server", func(t *testing.T) {
		cp := controlPlane(t, "tenant-addons-access", paused)
		setAddons(t, cp, older)
		refresh(t, cp)
		stale := cp.DeepCopy()
		setAddons(t, cp, version)
		refresh(t, cp)
		applyStatus(t, cp, tenant.FieldManager, map[string]any{"conditions": []any{
			conditionAt(lodiav1alpha1.ConditionAddonsApplied, metav1.ConditionTrue, lodiav1alpha1.ReasonApplied, cp.Generation),
			conditionAt(lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionTrue, lodiav1alpha1.ReasonAvailable, cp.Generation),
		}})
		applyStatus(t, cp, "test/"+lodiav1alpha1.ConditionAccessReady, map[string]any{"conditions": []any{
			conditionAt(lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAPIServer, cp.Generation),
		}})
		made, err := pass(t, cp, stale, unreachable, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(made) != 1 || !appliedWith(made, lodiav1alpha1.ReasonWaitingForAccess, "AccessReady is not True for this generation") {
			t.Fatalf("the pass applied %+v, want one apply that waits for access", made)
		}
		expectAddons(t, made, version, previous)
	})

	t.Run("a Secret is missing", func(t *testing.T) {
		cp := controlPlane(t, "tenant-addons-secret", paused)
		setAddons(t, cp, older)
		setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
		refresh(t, cp)
		made, err := pass(t, cp, cp.DeepCopy(), unreachable, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !appliedWith(made, lodiav1alpha1.ReasonWaitingForAccess, "Secret dev-ca-bundle does not exist") {
			t.Fatalf("the pass applied %+v, want a wait for the missing CA bundle", made)
		}
		expectAddons(t, made, older, previous)
	})

	t.Run("the render waits", func(t *testing.T) {
		cp := controlPlane(t, "tenant-addons-render")
		publish(t, cp)
		reconcilePKI(t, &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour, Clock: clock.RealClock{}}, cp)
		testenv.EventuallyWithin(t, 10*time.Second, func() error {
			return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
		})
		update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
		setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
		setAddons(t, cp, older)
		applyStatus(t, cp, "test/target", map[string]any{})
		appliedAtTheGeneration(t, cp)
		if cp.Status.Target != nil && cp.Status.Target.Version != "" {
			t.Fatalf("status.target is %+v, want none", cp.Status.Target)
		}
		made, err := pass(t, cp, cp.DeepCopy(), unreachable, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !appliedWith(made, lodiav1alpha1.ReasonWaitingForTarget, "status.target has no version") {
			t.Fatalf("the pass applied %+v, want a wait for the target", made)
		}
		expectAddons(t, made, older, previous)
	})

	t.Run("a component runs a version outside the table", func(t *testing.T) {
		cp := controlPlane(t, "tenant-addons-outside")
		publish(t, cp)
		reconcilePKI(t, &pki.Reconciler{Client: k8s, Reader: k8s, LeafLifetime: time.Hour, CALifetime: 24 * time.Hour, TrustAnchorLifetime: 48 * time.Hour, CAExpiryNotice: time.Hour, Clock: clock.RealClock{}}, cp)
		testenv.EventuallyWithin(t, 10*time.Second, func() error {
			return expectCondition(t, cp, lodiav1alpha1.ConditionAddonsAvailable, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForAccess)
		})
		update(t, cp, func(cp *lodiav1alpha1.ControlPlane) { cp.Spec.Paused = true })
		setCondition(t, cp, lodiav1alpha1.ConditionAccessReady, lodiav1alpha1.ReasonBound)
		setAddons(t, cp, older)
		setComponents(t, cp, running(version), running("v1.34.2"), running(version))
		appliedAtTheGeneration(t, cp)
		made, err := pass(t, cp, cp.DeepCopy(), unreachable, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !appliedWith(made, lodiav1alpha1.ReasonWaitingForTarget, `"v1.34.2", is not a version Lodia runs`) {
			t.Fatalf("the pass applied %+v, want a wait that names v1.34.2", made)
		}
		expectAddons(t, made, older, previous)
	})
}

func TestAPassThatCannotReadTheAddonsLiveWritesNothing(t *testing.T) {
	cp := controlPlane(t, "tenant-addons-expired", paused)
	setAddons(t, cp, older)
	refresh(t, cp)
	expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	var made []lodiav1alpha1.ControlPlaneStatus
	r := &tenant.Reconciler{Client: recorder{Client: snapshot{Client: k8s, cp: cp.DeepCopy()}, made: &made}, Reader: k8s, Revision: revision, ApplyPeriod: time.Hour, Dial: unreachable}
	if _, err := r.Reconcile(expired, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a pass whose deadline is spent returned %v", err)
	}
	if len(made) != 0 {
		t.Errorf("a pass whose deadline is spent applied %+v", made)
	}
}
