// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestDerivedNamesFitTheLongestControlPlaneName(t *testing.T) {
	longestRevisionHash := strconv.FormatUint(math.MaxUint32, 10)
	for length, fits := range map[int]bool{32: true, 33: false} {
		controlPlane := strings.Repeat("a", length)
		etcd := metav1.ObjectMeta{Name: lodiav1alpha1.EtcdName(controlPlane)}
		errs := slices.Concat(
			validation.IsDNS1035Label(lodiav1alpha1.APIServerServiceName(controlPlane)),
			validation.IsDNS1035Label(druidv1alpha1.GetClientServiceName(etcd)),
			validation.IsDNS1035Label(druidv1alpha1.GetPeerServiceName(etcd)),
			validation.IsValidLabelValue(druidv1alpha1.GetStatefulSetName(etcd)+"-"+longestRevisionHash),
			validation.IsValidLabelValue(druidv1alpha1.GetCompactionJobName(etcd)),
			validation.IsValidLabelValue(druidv1alpha1.GetDeltaSnapshotLeaseName(etcd)),
			validation.IsValidLabelValue(druidv1alpha1.GetFullSnapshotLeaseName(etcd)),
			validation.IsValidLabelValue(strings.ReplaceAll(druidv1alpha1.GetRoleName(etcd), ":", "-")),
			validation.IsValidLabelValue(strings.ReplaceAll(druidv1alpha1.GetRoleBindingName(etcd), ":", "-")),
		)
		if (len(errs) == 0) != fits {
			t.Errorf("a ControlPlane name of %d characters: fits %t, want %t: %v", length, len(errs) == 0, fits, errs)
		}
	}
}

func TestTheLongestBackupNameLeavesRoomForItsEncryptionSecret(t *testing.T) {
	for length, fits := range map[int]bool{235: true, 236: false} {
		name := lodiav1alpha1.BackupEncryptionConfigSecretName(strings.Repeat("b", length))
		if errs := validation.IsDNS1123Subdomain(name); (len(errs) == 0) != fits {
			t.Errorf("the encryption Secret of a Backup name of %d characters: fits %t, want %t: %v", length, len(errs) == 0, fits, errs)
		}
	}
}
