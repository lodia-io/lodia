// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

var newKeyName = regexp.MustCompile(`^key-[0-9]+$`)

type encryptionKey struct {
	provider, name, secret string
}

func (k encryptionKey) String() string {
	return k.provider + "/" + k.name
}

func encryptionKeys(t *testing.T, cp *lodiav1alpha1.ControlPlane) []encryptionKey {
	t.Helper()
	var cfg apiserverv1.EncryptionConfiguration
	if err := yaml.Unmarshal(secretsOf(t, cp)[cp.Name+"-encryption-config"].Data[lodiav1alpha1.EncryptionConfigKey], &cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Resources) != 1 {
		t.Fatalf("encryption configuration: %+v", cfg)
	}
	var keys []encryptionKey
	for _, p := range cfg.Resources[0].Providers {
		switch {
		case p.Secretbox != nil:
			for _, k := range p.Secretbox.Keys {
				keys = append(keys, encryptionKey{"secretbox", k.Name, k.Secret})
			}
		case p.AESCBC != nil:
			for _, k := range p.AESCBC.Keys {
				keys = append(keys, encryptionKey{"aescbc", k.Name, k.Secret})
			}
		}
	}
	return keys
}

func setEncryption(t *testing.T, cp *lodiav1alpha1.ControlPlane, provider lodiav1alpha1.EncryptionProvider) {
	t.Helper()
	patch := client.MergeFrom(cp.DeepCopy())
	cp.Spec.APIServer = &lodiav1alpha1.APIServer{Encryption: &lodiav1alpha1.Encryption{Provider: provider}}
	if err := k8s.Patch(t.Context(), cp, patch); err != nil {
		t.Fatal(err)
	}
}

func TestANewEncryptionKeyIsNamedAfterItsNanosecond(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-key-name")
	before := time.Now().UnixNano()
	reconcile(t, r, cp)
	after := time.Now().UnixNano()
	keys := encryptionKeys(t, cp)
	if len(keys) != 1 {
		t.Fatalf("keys %v, want one", keys)
	}
	nanosecond, err := strconv.ParseInt(strings.TrimPrefix(keys[0].name, "key-"), 10, 64)
	if !newKeyName.MatchString(keys[0].name) || err != nil || nanosecond < before || nanosecond > after {
		t.Errorf("the new key is named %q, want key-<unix nanoseconds> of the pass, between %d and %d", keys[0].name, before, after)
	}
}

func TestAProviderChangedBackGetsAKeyNameOfItsOwn(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-provider-back")
	reconcile(t, r, cp)
	for _, provider := range []lodiav1alpha1.EncryptionProvider{"aescbc", "secretbox"} {
		setEncryption(t, cp, provider)
		reconcile(t, r, cp)
	}
	keys := encryptionKeys(t, cp)
	if got := fmt.Sprint(keys); len(keys) != 3 || keys[0].provider != "secretbox" || keys[1].provider != "aescbc" || keys[2].provider != "secretbox" {
		t.Fatalf("keys %s, want the second secretbox key, the aescbc key, the first secretbox key", got)
	}
	names := map[string]bool{}
	for _, k := range keys {
		names[k.name] = true
	}
	if len(names) != len(keys) {
		t.Errorf("the keys %v share a name: the API server tries each key whose provider and name prefix a stored value", keys)
	}
}

func TestAnExistingKeyKeepsItsName(t *testing.T) {
	r := newReconciler(time.Hour)
	cp := controlPlane(t, "pki-existing-key")
	reconcile(t, r, cp)
	secret := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))
	existing := []byte("apiVersion: apiserver.config.k8s.io/v1\nkind: EncryptionConfiguration\nresources:\n- resources: [secrets]\n  providers:\n  - secretbox:\n      keys:\n      - name: key1\n        secret: " + secret + "\n")
	s := secretsOf(t, cp)[cp.Name+"-encryption-config"]
	s.Data = map[string][]byte{lodiav1alpha1.EncryptionConfigKey: existing}
	if err := k8s.Update(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, cp)
	if got := secretsOf(t, cp)[cp.Name+"-encryption-config"].Data[lodiav1alpha1.EncryptionConfigKey]; !bytes.Equal(got, existing) {
		t.Fatalf("a pass on the same provider rewrote the configuration:\n%s", got)
	}

	setEncryption(t, cp, "aescbc")
	reconcile(t, r, cp)
	keys := encryptionKeys(t, cp)
	if len(keys) != 2 || keys[0].provider != "aescbc" || !newKeyName.MatchString(keys[0].name) {
		t.Fatalf("keys %v, want a new aescbc key named key-<unix nanoseconds> first", keys)
	}
	if keys[1] != (encryptionKey{"secretbox", "key1", secret}) {
		t.Errorf("the existing key became %s, want secretbox/key1 with its secret unchanged", keys[1])
	}
}
