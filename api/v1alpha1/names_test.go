// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package v1alpha1_test

import (
	"testing"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
)

func TestEtcdCertificateSecretNamesParseBack(t *testing.T) {
	for _, role := range bryggav1alpha1.EtcdCertificateRoles() {
		for _, generation := range []int{1, 12} {
			name := bryggav1alpha1.EtcdCertificateSecretName("dev", role, generation)
			if r, g, ok := bryggav1alpha1.ParseEtcdCertificateSecretName("dev", name); !ok || r != role || g != generation {
				t.Errorf("%s parses as %q %d %t", name, r, g, ok)
			}
		}
	}
	for _, name := range []string{"dev-etcd-ca", "dev-etcd-backup", "dev-etcd-ca-0", "dev-etcd-ca-01", "dev-etcd-ca-x", "dev-etcd-ca-1-1", "prod-etcd-ca-1", "dev-etcd-ca-"} {
		if r, g, ok := bryggav1alpha1.ParseEtcdCertificateSecretName("dev", name); ok {
			t.Errorf("%s parses as %q %d", name, r, g)
		}
	}
	roles := bryggav1alpha1.EtcdCertificateRoles()
	roles[0] = "changed"
	if bryggav1alpha1.EtcdCertificateRoles()[0] != bryggav1alpha1.EtcdCARole {
		t.Error("an importer changed the etcd certificate roles")
	}
}
