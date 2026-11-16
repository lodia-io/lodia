// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package contract_test

import (
	"testing"

	"go.lodia.io/lodia/internal/contract"
)

func TestEtcdCertificateSecretNamesParseBack(t *testing.T) {
	for _, role := range contract.EtcdCertificateRoles() {
		for _, generation := range []int{1, 12} {
			name := contract.EtcdCertificateSecretName("dev", role, generation)
			if r, g, ok := contract.ParseEtcdCertificateSecretName("dev", name); !ok || r != role || g != generation {
				t.Errorf("%s parses as %q %d %t", name, r, g, ok)
			}
		}
	}
	for _, name := range []string{"dev-etcd-ca", "dev-etcd-backup", "dev-etcd-ca-0", "dev-etcd-ca-01", "dev-etcd-ca-x", "dev-etcd-ca-1-1", "prod-etcd-ca-1", "dev-etcd-ca-"} {
		if r, g, ok := contract.ParseEtcdCertificateSecretName("dev", name); ok {
			t.Errorf("%s parses as %q %d", name, r, g)
		}
	}
	roles := contract.EtcdCertificateRoles()
	roles[0] = "changed"
	if contract.EtcdCertificateRoles()[0] != contract.EtcdCARole {
		t.Error("an importer changed the etcd certificate roles")
	}
}
