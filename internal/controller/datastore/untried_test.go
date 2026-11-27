// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/testenv"
)

// A held pass whose dry run of the grant's write a webhook does not support reports RefusedBeforeHold naming the webhook, as the API server answers, which Lifecycle grants in the window.
func TestAHeldPassReportsAWriteNoDryRunCanTry(t *testing.T) {
	t.Parallel()
	ns := namespace(t)
	const name = "untried-held"
	const quorum = "Etcd untried-held-etcd has quorum, 1 of 1 members ready"
	createClass(t, newClass("untrying-held", "Retain"))
	heldTenant(t, ns, name, "untrying-held", nil)
	waitPolicy(t, ns, name, nil)
	configuration := testenv.RefusingWebhook(testenv.Denying(t, http.StatusForbidden), admissionregistrationv1.RuleWithOperations{
		Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
		Rule:       admissionregistrationv1.Rule{APIGroups: []string{networkingv1.GroupName}, APIVersions: []string{"v1"}, Resources: []string{"networkpolicies"}},
	}, &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: ns}})
	create(t, configuration)
	t.Cleanup(func() { _ = client.IgnoreNotFound(k8s.Delete(context.Background(), configuration)) })
	granted, held := operatorAt(unpaused(k8s), previousRevision), operatorAt(unpaused(k8s), revision)
	testenv.Eventually(t, func() error {
		if _, err := passOf(t, granted, ns, name); !apistatus.Refused(err) {
			return fmt.Errorf("a granted pass under the webhook returned %w", err)
		}
		return nil
	})
	described := getControlPlane(t, ns, name).Generation
	testenv.StoreWithSideEffects(t, etcd, configuration, admissionregistrationv1.SideEffectClassSome)
	testenv.Eventually(t, func() error {
		if _, err := passOf(t, held, ns, name); err != nil {
			return err
		}
		c := meta.FindStatusCondition(getControlPlane(t, ns, name).Status.Conditions, lodiav1alpha1.ConditionDatastoreReady)
		if want := quorum + "; the grant's write of NetworkPolicy untried-held-etcd cannot be tried: " + apistatus.Unsupported(testenv.Webhook); c.Status != metav1.ConditionTrue || c.Reason != lodiav1alpha1.ReasonRefusedBeforeHold || c.Message != want || c.ObservedGeneration != described {
			return fmt.Errorf("DatastoreReady is %+v, want True RefusedBeforeHold %q", c, want)
		}
		return nil
	})
}
