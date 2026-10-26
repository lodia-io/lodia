// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1_test

import (
	"testing"

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
