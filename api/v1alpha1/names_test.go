// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1_test

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

func TestEtcdCertificateSecretNamesParseBack(t *testing.T) {
	for _, role := range lodiav1alpha1.EtcdCertificateRoles() {
		for _, generation := range []int{1, 12} {
			name := lodiav1alpha1.EtcdCertificateSecretName("dev", role, generation)
			if r, g, ok := lodiav1alpha1.ParseEtcdCertificateSecretName("dev", name); !ok || r != role || g != generation {
				t.Errorf("%s parses as %q %d %t", name, r, g, ok)
			}
		}
	}
	for _, name := range []string{"dev-etcd-ca", "dev-etcd-backup", "dev-etcd-ca-0", "dev-etcd-ca-01", "dev-etcd-ca-x", "dev-etcd-ca-1-1", "prod-etcd-ca-1", "dev-etcd-ca-"} {
		if r, g, ok := lodiav1alpha1.ParseEtcdCertificateSecretName("dev", name); ok {
			t.Errorf("%s parses as %q %d", name, r, g)
		}
	}
	roles := lodiav1alpha1.EtcdCertificateRoles()
	roles[0] = "changed"
	if lodiav1alpha1.EtcdCertificateRoles()[0] != lodiav1alpha1.EtcdCARole {
		t.Error("an importer changed the etcd certificate roles")
	}
}

func TestEveryDerivedNameFitsTheLongestControlPlaneName(t *testing.T) {
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
			validation.IsValidLabelValue(strings.ReplaceAll(druidv1alpha1.GetRoleName(etcd), ":", "-")),
			validation.IsValidLabelValue(strings.ReplaceAll(druidv1alpha1.GetRoleBindingName(etcd), ":", "-")),
		)
		if (len(errs) == 0) != fits {
			t.Errorf("a ControlPlane name of %d characters: fits %t, want %t: %v", length, len(errs) == 0, fits, errs)
		}
	}
}
