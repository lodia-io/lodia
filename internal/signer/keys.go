// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package signer

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v4"
	externaljwtv1 "k8s.io/externaljwt/apis/v1"
)

const (
	// signingKeyFile is api/v1alpha1's SigningKeyKey, which a test pins: the
	// sidecar does not import the API package.
	signingKeyFile    = "signing.key"
	verifyKeyPattern  = "verify-*.pub"
	kubeletDataLink   = "..data"
	minRSAKeySizeBits = 2048
)

type keySet struct {
	signer     jose.Signer
	signingKID string
	keys       []*externaljwtv1.Key
	loadedAt   time.Time
}

func (s *keySet) kids() []string {
	kids := make([]string, len(s.keys))
	for i, k := range s.keys {
		kids[i] = k.GetKeyId()
	}
	return kids
}

func loadKeySet(dir string) (*keySet, error) {
	// The kubelet swaps a Secret volume by pointing ..data at a new directory;
	// reading every file through one resolved ..data keeps a swap from mixing
	// two versions of the Secret.
	if resolved, err := filepath.EvalSymlinks(filepath.Join(dir, kubeletDataLink)); err == nil {
		dir = resolved
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	raw, err := os.ReadFile(filepath.Join(dir, signingKeyFile))
	if err != nil {
		return nil, err
	}
	private, err := parsePrivateKey(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", signingKeyFile, err)
	}
	public := private.Public()
	alg, err := algorithm(public)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", signingKeyFile, err)
	}
	signingKey, err := newKey(public)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", signingKeyFile, err)
	}
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: alg, Key: jose.JSONWebKey{Key: private, KeyID: signingKey.GetKeyId()}},
		(&jose.SignerOptions{}).WithType("JWT"),
	)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", signingKeyFile, err)
	}

	keys := []*externaljwtv1.Key{signingKey}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "..") {
			continue
		}
		if ok, _ := filepath.Match(verifyKeyPattern, e.Name()); !ok {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		key, err := parsePublicKey(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		if keys, err = appendKey(keys, key); err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
	}
	slices.SortFunc(keys, func(a, b *externaljwtv1.Key) int { return strings.Compare(a.GetKeyId(), b.GetKeyId()) })

	return &keySet{signer: signer, signingKID: signingKey.GetKeyId(), keys: keys}, nil
}

func appendKey(keys []*externaljwtv1.Key, public crypto.PublicKey) ([]*externaljwtv1.Key, error) {
	if _, err := algorithm(public); err != nil {
		return nil, err
	}
	key, err := newKey(public)
	if err != nil {
		return nil, err
	}
	if slices.ContainsFunc(keys, func(k *externaljwtv1.Key) bool { return k.GetKeyId() == key.GetKeyId() }) {
		return keys, nil
	}
	return append(keys, key), nil
}

// newKey derives the kid as kube-apiserver's keyIDFromPublicKey does, so
// tokens signed by a static --service-account-signing-key-file keep verifying.
func newKey(public crypto.PublicKey) (*externaljwtv1.Key, error) {
	der, err := x509.MarshalPKIXPublicKey(public)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(der)
	return &externaljwtv1.Key{KeyId: base64.RawURLEncoding.EncodeToString(sum[:]), Key: der}, nil
}

func algorithm(public crypto.PublicKey) (jose.SignatureAlgorithm, error) {
	switch k := public.(type) {
	case *rsa.PublicKey:
		if k.N.BitLen() < minRSAKeySizeBits {
			return "", fmt.Errorf("RSA key of %d bits, want at least %d", k.N.BitLen(), minRSAKeySizeBits)
		}
		return jose.RS256, nil
	case *ecdsa.PublicKey:
		switch k.Curve {
		case elliptic.P256():
			return jose.ES256, nil
		case elliptic.P384():
			return jose.ES384, nil
		case elliptic.P521():
			return jose.ES512, nil
		default:
			return "", fmt.Errorf("unsupported ECDSA curve %s", k.Params().Name)
		}
	default:
		return "", fmt.Errorf("unsupported key type %T", public)
	}
}

func parsePrivateKey(raw []byte) (crypto.Signer, error) {
	block, err := onePEMBlock(raw)
	if err != nil {
		return nil, err
	}
	var key any
	switch block.Type {
	case "PRIVATE KEY":
		key, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	case "EC PRIVATE KEY":
		key, err = x509.ParseECPrivateKey(block.Bytes)
	default:
		return nil, fmt.Errorf("PEM block %q is not a private key", block.Type)
	}
	if err != nil {
		return nil, err
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, fmt.Errorf("unsupported key type %T", key)
	}
	return signer, nil
}

func parsePublicKey(raw []byte) (crypto.PublicKey, error) {
	block, err := onePEMBlock(raw)
	if err != nil {
		return nil, err
	}
	if block.Type != "PUBLIC KEY" {
		return nil, fmt.Errorf("PEM block %q is not a PKIX public key", block.Type)
	}
	return x509.ParsePKIXPublicKey(block.Bytes)
}

func onePEMBlock(raw []byte) (*pem.Block, error) {
	block, rest := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("no PEM block")
	}
	if next, _ := pem.Decode(rest); next != nil {
		return nil, errors.New("more than one PEM block")
	}
	return block, nil
}
