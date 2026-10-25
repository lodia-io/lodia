// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bryggav1alpha1 "github.com/segla-io/brygga/api/v1alpha1"
)

func TestNoSecretNameOfOneControlPlaneIsAnothers(t *testing.T) {
	suffixes := append([]string{string(bryggav1alpha1.EtcdBackupSecret)}, names...)
	for _, role := range bryggav1alpha1.EtcdCertificateRoles() {
		suffixes = append(suffixes, "etcd-"+role+"-1", "etcd-"+role+"-12")
	}
	for _, a := range suffixes {
		for _, b := range suffixes {
			if prefix, ok := strings.CutSuffix(a, "-"+b); ok {
				t.Errorf("<n>-%s of a ControlPlane <n> is also <n>-%s-%s of a ControlPlane <n>-%s", a, prefix, b, prefix)
			}
		}
	}
}

func TestReservedNames(t *testing.T) {
	tn := &tenant{cp: &bryggav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev"}}}
	for _, c := range []struct {
		name       string
		reserved   bool
		generation int
	}{
		{"dev-client-ca", true, 0},
		{"dev-encryption-config", true, 0},
		{"dev-etcd-ca", true, 0},
		{"dev-etcd-ca-1", true, 1},
		{"dev-etcd-backup-restore-12", true, 12},
		{"dev-etcd-ca-01", false, 0},
		{"dev-etcd-ca-0", false, 0},
		{"dev-etcd-ca-x", false, 0},
		{"dev-etcd-backup", false, 0},
		{"dev-client-ca-bundle", false, 0},
		{"prod-client-ca", false, 0},
	} {
		if g, ok := tn.reserved(c.name); ok != c.reserved || g != c.generation {
			t.Errorf("%s: reserved %t in generation %d, want %t in %d", c.name, ok, g, c.reserved, c.generation)
		}
	}
}
