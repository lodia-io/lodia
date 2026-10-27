// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

func TestAPreUpgradeBackupNameIsABackupName(t *testing.T) {
	manifest, err := os.ReadFile(filepath.Join("..", "..", "api", "crds", "lodia.io_controlplanes.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	digits := int(*at(t, crdSchema(t, manifest), "status.target.version").MaxLength) - len("v1..")
	version := "v1." + strings.Repeat("9", digits/2) + "." + strings.Repeat("9", digits-digits/2)
	uid := types.UID(strings.Repeat("f", 8) + "-ffff-ffff-ffff-" + strings.Repeat("f", 12))
	name := lodiav1alpha1.PreUpgradeBackupName(strings.Repeat("a", 32), version, uid)
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 || len(name) > 235 {
		t.Errorf("%s, %d characters, is no Backup name: %v", name, len(name), errs)
	}
	if len(name) != 32+len("-pre-upgrade-")+len(version)+1+len(uid) {
		t.Errorf("%s does not carry the ControlPlane's name, the version and the UID", name)
	}
}
