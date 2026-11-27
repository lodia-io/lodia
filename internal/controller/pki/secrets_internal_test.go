// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/contract"
)

// The etcd generation a lost etcd CA is reported against is the newest one this ControlPlane controls, deleted or not, in any order of the Secrets.
func TestTheControlledEtcdGenerationIsTheNewestWhateverTheOrder(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "ns", UID: "cp-uid"}}
	cp.APIVersion, cp.Kind = lodiav1alpha1.GroupVersion.String(), "ControlPlane"
	mine := []metav1.OwnerReference{{APIVersion: cp.APIVersion, Kind: cp.Kind, Name: cp.Name, UID: cp.UID, Controller: new(true)}}
	now := metav1.NewTime(time.Now())
	tn := newTenant(cp, now.Time)
	for g := 1; g <= 4; g++ {
		for _, role := range contract.EtcdCertificateRoles() {
			s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Name: contract.EtcdCertificateSecretName(cp.Name, role, g), Namespace: cp.Namespace,
				Labels: map[string]string{lodiav1alpha1.EtcdCertificatesLabel: strconv.Itoa(g)},
			}}
			switch g {
			case 3:
				s.OwnerReferences, s.DeletionTimestamp = mine, &now
			case 4:
			default:
				s.OwnerReferences = mine
			}
			tn.existing[s.Name] = s
		}
	}
	for range 64 {
		if g := tn.controlledEtcdGeneration(); g != 3 {
			t.Fatalf("controlledEtcdGeneration is %d, want 3", g)
		}
	}
}
