// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/testenv"
)

func TestReadyTakesAnInputsFirstPassOverAnotherReason(t *testing.T) {
	cp := controlPlane(t, "v1.37.1")
	setCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "waiting for Workloads to publish status.endpoint")
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonWaitingForEndpoint, "CertificatesReady: waiting for Workloads to publish status.endpoint", 1, 0)
	})
	setCondition(t, cp, lodiav1alpha1.ConditionDatastoreReady, metav1.ConditionUnknown, lodiav1alpha1.ReasonReconciling, "the first pass has not finished")
	setCondition(t, cp, lodiav1alpha1.ConditionCertificatesReady, metav1.ConditionTrue, lodiav1alpha1.ReasonIssued, "")
	testenv.Eventually(t, func() error {
		return expectReady(t, cp, metav1.ConditionUnknown, lodiav1alpha1.ReasonReconciling, "DatastoreReady: the first pass has not finished", 1, 0)
	})
}
