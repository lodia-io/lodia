// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"

	"go.lodia.io/lodia/internal/certs"
)

func TestTheLargestRequestKubeconfigFitsItsStatus(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "api", "crds", "lodia.io_kubeconfigrequests.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(raw, crd); err != nil {
		t.Fatal(err)
	}
	bound := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["status"].Properties["kubeconfig"].MaxLength
	if bound == nil {
		t.Fatal("status.kubeconfig has no maxLength")
	}

	now := time.Now()
	stamp := func(name string) string { return name + "@" + strconv.FormatInt(now.UnixNano(), 10) }
	longest := func(issue func() (*x509.Certificate, error)) *x509.Certificate {
		t.Helper()
		var out *x509.Certificate
		for range 256 {
			c, err := issue()
			if err != nil {
				t.Fatal(err)
			}
			if out == nil || len(c.Raw) > len(out.Raw) {
				out = c
			}
		}
		return out
	}
	clusterCA := func() (*x509.Certificate, error) {
		c, err := certs.NewCA(stamp(clusterCA+"-ca"), now, time.Date(2060, 1, 1, 0, 0, 0, 0, time.UTC).Sub(now))
		if err != nil {
			return nil, err
		}
		return c.Cert, nil
	}
	bundle, err := certs.Bundle(longest(clusterCA), longest(clusterCA))
	if err != nil {
		t.Fatal(err)
	}
	signer, err := certs.NewCA(stamp(requestCA+"-ca"), now, 365*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	modulus, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 11022))
	if err != nil {
		t.Fatal(err)
	}
	key := &rsa.PublicKey{N: modulus.SetBit(modulus, 11022, 1).SetBit(modulus, 0, 1), E: 3}
	user := userPrefix + strings.Repeat("\U0001F600", 512)
	cert := longest(func() (*x509.Certificate, error) {
		return signer.Sign(certs.Profile{CommonName: user, Organizations: []string{clusterAdmins}, Usages: clientAuth}, key, now, certs.KubeconfigRequestLifetime)
	})
	kubeconfig, err := requestKubeconfig(strings.Repeat("a", 32), &endpoint{host: strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61), port: 65535}, bundle, user, cert)
	if err != nil {
		t.Fatal(err)
	}
	if encoded := int64(base64.StdEncoding.EncodedLen(len(kubeconfig))); encoded > *bound {
		t.Errorf("the largest kubeconfig is %d characters of base64, above status.kubeconfig's %d", encoded, *bound)
	} else {
		t.Logf("the largest kubeconfig is %d characters of base64, status.kubeconfig holds %d", encoded, *bound)
	}
}
