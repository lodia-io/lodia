// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	"k8s.io/apiserver/pkg/server/options/encryptionconfig"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// PKI reads exactly the encryption configurations of its shape, one resource entry for secrets, that kube-apiserver's loader loads with reload on.
func TestPKIReadsTheConfigurationsOfItsShapeThatKubeAPIServerLoads(t *testing.T) {
	key := func(n int) string { return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'k'}, n)) }
	head := "apiVersion: apiserver.config.k8s.io/v1\nkind: EncryptionConfiguration\n"
	secrets := func(providers ...string) string {
		return "resources:\n- resources: [secrets]\n  providers:\n" + strings.Join(providers, "")
	}
	secretbox := func(name, secret string) string {
		return "  - secretbox:\n      keys:\n      - name: " + name + "\n        secret: " + secret + "\n"
	}
	kms := "  - kms:\n      apiVersion: v1\n      name: plugin\n      endpoint: unix:///nonexistent/kms.sock\n      timeout: 1s\n      cachesize: 10\n"
	for _, c := range []struct {
		name         string
		content      string
		shape, loads bool
	}{
		{"secretbox as PKI writes it", asPKIWrites(t, providerSecretbox), true, true},
		{"aescbc as PKI writes it", asPKIWrites(t, providerAESCBC), true, true},
		{"identity as PKI writes it", asPKIWrites(t, providerNone), true, true},
		{"a Backup's keys as the backup reconciler copies them, after two changes of provider", asPKIWrites(t, providerAESCBC, providerSecretbox, providerNone), true, true},
		{"a restore's first configuration", asPKIWrites(t, providerSecretbox, providerAESCBC, providerSecretbox, providerNone), true, true},
		{"an unknown field", head + secrets(secretbox("key1", key(32))) + "unknown: true\n", true, false},
		{"an unknown field in a key", head + secrets(secretbox("key1", key(32))+"        unknown: true\n"), true, false},
		{"a repeated field", head + secrets(secretbox("key1", key(32))+"        secret: "+key(32)+"\n"), true, false},
		{"an unknown kind", "apiVersion: apiserver.config.k8s.io/v1\nkind: EncryptionConfig\n" + secrets(secretbox("key1", key(32))), true, false},
		{"another kind of the group", "apiVersion: apiserver.config.k8s.io/v1\nkind: AdmissionConfiguration\nplugins: []\n", true, false},
		{"the kind of core v1", "apiVersion: v1\nkind: EncryptionConfig\n" + secrets(secretbox("key1", key(32))), true, true},
		{"an unknown version", "apiVersion: apiserver.config.k8s.io/v2\nkind: EncryptionConfiguration\n" + secrets(secretbox("key1", key(32))), true, false},
		{"no kind", secrets(secretbox("key1", key(32))), true, false},
		{"nothing", "", true, false},
		{"a key that is not base64", head + secrets(secretbox("key1", "not base64")), true, false},
		{"a secretbox key of 16 bytes", head + secrets(secretbox("key1", key(16))), true, false},
		{"an aescbc key of 16 bytes", head + secrets("  - aescbc:\n      keys:\n      - name: key1\n        secret: "+key(16)+"\n"), true, true},
		{"an aesgcm key", head + secrets("  - aesgcm:\n      keys:\n      - name: key1\n        secret: "+key(32)+"\n"), true, true},
		{"a provider without keys", head + secrets("  - secretbox:\n      keys: []\n"), true, false},
		{"two providers in one element", head + secrets("  - identity: {}\n    secretbox:\n      keys:\n      - name: key1\n        secret: "+key(32)+"\n"), true, false},
		{"a KMS v2 provider without a timeout", head + secrets("  - kms:\n      apiVersion: v2\n      name: plugin\n      endpoint: unix:///nonexistent/kms.sock\n", "  - identity: {}\n"), true, true},
		{"a KMS v1 provider", head + secrets(kms), true, false},
		{"two KMS v1 providers of one name", head + secrets(kms, kms), true, false},
		{"a KMS v1 provider behind secretbox", head + secrets(secretbox("key1", key(32)), kms), true, false},
		{"configmaps", head + "resources:\n- resources: [configmaps]\n  providers:\n  - identity: {}\n", false, true},
		{"secrets and configmaps in one entry", head + "resources:\n- resources: [secrets, configmaps]\n  providers:\n" + secretbox("key1", key(32)), false, true},
		{"two resource entries", head + secrets(secretbox("key1", key(32))) + "- resources: [configmaps]\n  providers:\n  - identity: {}\n", false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), lodiav1alpha1.EncryptionConfigKey)
			if err := os.WriteFile(path, []byte(c.content), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := encryptionconfig.LoadEncryptionConfig(t.Context(), path, true, "lodia")
			if loads := err == nil; loads != c.loads {
				t.Fatalf("kube-apiserver loads it: %v (%v), want %v", loads, err, c.loads)
			}
			if _, read := readEncryptionConfig([]byte(c.content)); read != (c.shape && c.loads) {
				t.Errorf("PKI reads it: %v, want %v", read, c.shape && c.loads)
			}
		})
	}
}

func asPKIWrites(t *testing.T, providers ...lodiav1alpha1.EncryptionProvider) string {
	t.Helper()
	cfg := apiserverv1.EncryptionConfiguration{
		TypeMeta:  metav1.TypeMeta{APIVersion: apiserverv1.SchemeGroupVersion.String(), Kind: "EncryptionConfiguration"},
		Resources: []apiserverv1.ResourceConfiguration{{Resources: []string{"secrets"}}},
	}
	for i, provider := range providers {
		p, err := newProvider(provider, time.Unix(0, int64(i)))
		if err != nil {
			t.Fatal(err)
		}
		cfg.Resources[0].Providers = append(cfg.Resources[0].Providers, p)
	}
	rendered, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return string(rendered)
}
