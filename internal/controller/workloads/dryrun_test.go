// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/testenv"
)

func probe(ns string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "probe"}, Data: map[string]string{"a": "b"}}
}

func quotaUsed(t *testing.T, quota *corev1.ResourceQuota, used string) {
	t.Helper()
	testenv.Eventually(t, func() error {
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(quota), quota); err != nil {
			return err
		}
		quota.Status.Hard = quota.Spec.Hard
		quota.Status.Used = corev1.ResourceList{"count/configmaps": resource.MustParse(used)}
		return k8s.Status().Update(t.Context(), quota)
	})
}

func configMapsWebhook(t *testing.T, hook testenv.Hook, ns string, failure admissionregistrationv1.FailurePolicyType) *admissionregistrationv1.ValidatingWebhookConfiguration {
	t.Helper()
	configuration := testenv.RefusingWebhook(hook, admissionregistrationv1.RuleWithOperations{
		Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
		Rule:       admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"configmaps"}},
	}, &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: ns}})
	configuration.Webhooks[0].FailurePolicy = &failure
	if err := k8s.Create(t.Context(), configuration); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.IgnoreNotFound(k8s.Delete(t.Context(), configuration)) })
	return configuration
}

// The API server answers a dry run as it answers the write, which a held pass's trial of the grant relies on: a ResourceQuota refuses it without recording usage, a ValidatingAdmissionPolicy evaluates it, a webhook whose sideEffects are None or NoneOnDryRun is called, and one whose sideEffects are any other keeps it from a verdict, whatever its failurePolicy, while it still answers the write.
func TestTheAPIServerAnswersADryRunAsTheWrite(t *testing.T) {
	t.Run("a ResourceQuota", func(t *testing.T) {
		ns := namespace(t)
		quota := &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "configmaps"}, Spec: corev1.ResourceQuotaSpec{Hard: corev1.ResourceList{"count/configmaps": resource.MustParse("2")}}}
		if err := k8s.Create(t.Context(), quota); err != nil {
			t.Fatal(err)
		}
		quotaUsed(t, quota, "2")
		testenv.Eventually(t, func() error {
			err := k8s.Create(t.Context(), probe(ns), client.DryRunAll)
			if !apistatus.Refused(err) || !strings.Contains(err.Error(), "exceeded quota") {
				return fmt.Errorf("a dry run over a full quota answers %w", err)
			}
			return nil
		})
		quotaUsed(t, quota, "1")
		testenv.Eventually(t, func() error { return k8s.Create(t.Context(), probe(ns), client.DryRunAll) })
		if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(quota), quota); err != nil {
			t.Fatal(err)
		}
		if used := quota.Status.Used["count/configmaps"]; used.String() != "1" {
			t.Errorf("a dry run moved the quota's usage to %s", used.String())
		}
		if err := k8s.Create(t.Context(), probe(ns)); err != nil {
			t.Fatal(err)
		}
		testenv.Eventually(t, func() error {
			if err := k8s.Get(t.Context(), client.ObjectKeyFromObject(quota), quota); err != nil {
				return err
			}
			if used := quota.Status.Used["count/configmaps"]; used.String() != "2" {
				return fmt.Errorf("the write left the quota's usage at %s", used.String())
			}
			return nil
		})
	})
	t.Run("a ValidatingAdmissionPolicy", func(t *testing.T) {
		ns := namespace(t)
		cm := probe(ns)
		if err := k8s.Create(t.Context(), cm); err != nil {
			t.Fatal(err)
		}
		policy := &admissionregistrationv1.ValidatingAdmissionPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "keep-configmaps-in-" + ns},
			Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
				FailurePolicy: new(admissionregistrationv1.Fail),
				MatchConstraints: &admissionregistrationv1.MatchResources{ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
					RuleWithOperations: admissionregistrationv1.RuleWithOperations{
						Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
						Rule:       admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"configmaps"}},
					},
				}}},
				Validations: []admissionregistrationv1.Validation{{Expression: "object.metadata.namespace != '" + ns + "'", Message: "no update in " + ns}},
			},
		}
		binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
			ObjectMeta: metav1.ObjectMeta{Name: policy.Name},
			Spec:       admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{PolicyName: policy.Name, ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny}},
		}
		for _, obj := range []client.Object{policy, binding} {
			if err := k8s.Create(t.Context(), obj); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.IgnoreNotFound(k8s.Delete(t.Context(), obj)) })
		}
		testenv.Eventually(t, func() error {
			cm.Data["a"] = "c"
			if err := k8s.Update(t.Context(), cm, client.DryRunAll); !apistatus.Refused(err) || !strings.Contains(err.Error(), "no update in "+ns) {
				return fmt.Errorf("a dry run the policy refuses answers %w", err)
			}
			return nil
		})
	})
	for _, sideEffects := range []admissionregistrationv1.SideEffectClass{admissionregistrationv1.SideEffectClassNone, admissionregistrationv1.SideEffectClassNoneOnDryRun} {
		t.Run("a webhook with sideEffects "+string(sideEffects), func(t *testing.T) {
			ns := namespace(t)
			hook := testenv.Denying(t, http.StatusForbidden)
			configuration := configMapsWebhook(t, hook, ns, admissionregistrationv1.Fail)
			configuration.Webhooks[0].SideEffects = &sideEffects
			if err := k8s.Update(t.Context(), configuration); err != nil {
				t.Fatal(err)
			}
			testenv.Eventually(t, func() error {
				err := k8s.Create(t.Context(), probe(ns), client.DryRunAll)
				if webhook, denied := apistatus.Webhook(err); !denied || webhook != testenv.Webhook || !apistatus.Refused(err) || hook.DryRuns.Load() == 0 {
					return fmt.Errorf("a dry run answers %w after %d dry runs reached the webhook", err, hook.DryRuns.Load())
				}
				return nil
			})
		})
	}
	for _, failure := range []admissionregistrationv1.FailurePolicyType{admissionregistrationv1.Fail, admissionregistrationv1.Ignore} {
		for _, sideEffects := range []admissionregistrationv1.SideEffectClass{admissionregistrationv1.SideEffectClassSome, admissionregistrationv1.SideEffectClassUnknown} {
			t.Run(fmt.Sprintf("a webhook with sideEffects %s that fails %s", sideEffects, failure), func(t *testing.T) {
				ns := namespace(t)
				hook := testenv.Denying(t, http.StatusForbidden)
				testenv.StoreWithSideEffects(t, etcd, configMapsWebhook(t, hook, ns, failure), sideEffects)
				unsupported := func() error {
					err := k8s.Create(t.Context(), probe(ns), client.DryRunAll)
					if webhook := apistatus.DryRunUnsupported(err); webhook != testenv.Webhook || apistatus.Refused(err) || err.Error() != apistatus.Unsupported(testenv.Webhook) {
						return fmt.Errorf("a dry run answers %w", err)
					}
					return nil
				}
				testenv.Eventually(t, unsupported)
				hook.DryRuns.Store(0)
				if err := unsupported(); err != nil {
					t.Error(err)
				}
				if n := hook.DryRuns.Load(); n != 0 {
					t.Errorf("%d dry runs reached the webhook once the API server held the configuration", n)
				}
				err := k8s.Create(t.Context(), probe(ns))
				if webhook, denied := apistatus.Webhook(err); !denied || webhook != testenv.Webhook {
					t.Errorf("the write answers %v, want the webhook's denial", err)
				}
			})
		}
	}
}
