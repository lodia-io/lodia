// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package workloads_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/apistatus"
	"go.lodia.io/lodia/internal/testenv"
)

// A held pass whose dry run of the grant's write a webhook does not support reports RefusedBeforeHold naming it, as the API server answers, which Lifecycle grants in a slot.
func TestAHeldPassReportsAWriteNoDryRunCanTry(t *testing.T) {
	cp := running(t)
	taken := get(t, cp).Generation
	pause(t, cp)
	configuration := testenv.RefusingWebhook(testenv.Denying(t, http.StatusForbidden), admissionregistrationv1.RuleWithOperations{
		Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
		Rule:       admissionregistrationv1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1"}, Resources: []string{"deployments"}},
	}, &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: cp.Namespace}},
		admissionregistrationv1.MatchCondition{Name: "scheduler", Expression: "object.metadata.name == 'dev-kube-scheduler'"})
	if err := k8s.Create(t.Context(), configuration); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.IgnoreNotFound(k8s.Delete(context.Background(), configuration)) })
	testenv.Eventually(t, func() error {
		if err := heldPass(t, operatorAt(unpaused(), revision), cp); !apistatus.Refused(err) {
			return errors.New("the webhook does not refuse the scheduler yet")
		}
		return nil
	})
	testenv.StoreWithSideEffects(t, etcd, configuration, admissionregistrationv1.SideEffectClassSome)
	held := operatorAt(unpaused(), next)
	testenv.Eventually(t, func() error {
		_ = heldPass(t, held, cp)
		return expectAt(cp, taken, metav1.ConditionTrue, lodiav1alpha1.ReasonRefusedBeforeHold, allReady+"the grant's write of Deployment dev-kube-scheduler cannot be tried: "+apistatus.Unsupported(testenv.Webhook))
	})
	granted := lifecyclePassThrough(t, unpaused(), cp, next)
	if granted.Status.Target.OperatorRevision != next || granted.Status.Maintenance == nil || granted.Status.Maintenance.SlotSince == nil {
		t.Errorf("Lifecycle wrote target %+v and maintenance %+v over a write no dry run can try, want %s granted in a slot", granted.Status.Target, granted.Status.Maintenance, next)
	}
}
