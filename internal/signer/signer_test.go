// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package signer_test

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	externaljwtv1 "k8s.io/externaljwt/apis/v1"

	"github.com/segla-io/brygga/internal/signer"
)

var (
	rsaKey  = mustRSA(2048)
	p256Key = mustECDSA(elliptic.P256())
	p384Key = mustECDSA(elliptic.P384())
	p521Key = mustECDSA(elliptic.P521())
	otherEC = mustECDSA(elliptic.P256())
)

var supportedKeys = []struct {
	name string
	key  crypto.Signer
	alg  string
}{
	{"RSA", rsaKey, "RS256"},
	{"P-256", p256Key, "ES256"},
	{"P-384", p384Key, "ES384"},
	{"P-521", p521Key, "ES512"},
}

func mustRSA(bits int) *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		panic(err)
	}
	return k
}

func mustECDSA(c elliptic.Curve) *ecdsa.PrivateKey {
	k, err := ecdsa.GenerateKey(c, rand.Reader)
	if err != nil {
		panic(err)
	}
	return k
}

func privatePEM(t *testing.T, key crypto.Signer) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

func publicPEM(t *testing.T, key crypto.Signer) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pkix(t, key)})
}

func pkix(t *testing.T, key crypto.Signer) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(key.Public())
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func kid(t *testing.T, key crypto.Signer) string {
	t.Helper()
	sum := sha256.Sum256(pkix(t, key))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func writeFiles(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func writeSecretVolume(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	version := fmt.Sprintf("..%d", time.Now().UnixNano())
	if err := os.Mkdir(filepath.Join(dir, version), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, version, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old, _ := os.Readlink(filepath.Join(dir, "..data"))
	if err := os.Symlink(version, filepath.Join(dir, "..data_tmp")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "..data_tmp"), filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, ok := files[e.Name()]; !ok && !strings.HasPrefix(e.Name(), "..") {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}
	for name := range files {
		link := filepath.Join(dir, name)
		if _, err := os.Lstat(link); err == nil {
			continue
		}
		if err := os.Symlink(filepath.Join("..data", name), link); err != nil {
			t.Fatal(err)
		}
	}
	if old != "" {
		if err := os.RemoveAll(filepath.Join(dir, old)); err != nil {
			t.Fatal(err)
		}
	}
}

func newSigner(t *testing.T, dir string) *signer.Signer {
	t.Helper()
	return signer.New(dir, 8760*time.Hour, time.Minute, slog.New(slog.NewTextHandler(t.Output(), nil)))
}

func loaded(t *testing.T, files map[string][]byte) (*signer.Signer, string) {
	t.Helper()
	dir := t.TempDir()
	writeFiles(t, dir, files)
	s := newSigner(t, dir)
	s.Reload()
	if !s.Ready() {
		t.Fatal("no key set loaded")
	}
	return s, dir
}

func fetch(t *testing.T, s *signer.Signer) *externaljwtv1.FetchKeysResponse {
	t.Helper()
	resp, err := s.FetchKeys(context.Background(), &externaljwtv1.FetchKeysRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func kids(resp *externaljwtv1.FetchKeysResponse) []string {
	var out []string
	for _, k := range resp.GetKeys() {
		out = append(out, k.GetKeyId())
	}
	return out
}

type header struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	Typ string `json:"typ"`
}

func sign(t *testing.T, s *signer.Signer, claims string) (header, *externaljwtv1.SignJWTResponse) {
	t.Helper()
	resp, err := s.Sign(context.Background(), &externaljwtv1.SignJWTRequest{Claims: claims})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(resp.GetHeader())
	if err != nil {
		t.Fatalf("header is not unpadded base64url: %v", err)
	}
	var h header
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&h); err != nil {
		t.Fatalf("header %s: %v", raw, err)
	}
	return h, resp
}

func sorted(ids ...string) []string {
	slices.Sort(ids)
	return ids
}

func TestSignHeaderAndSignature(t *testing.T) {
	claims := base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"https://kubernetes.default.svc","sub":"system:serviceaccount:default:x","x":"é<>&","n":1}`))
	for _, c := range supportedKeys {
		t.Run(c.name, func(t *testing.T) {
			s, _ := loaded(t, map[string][]byte{"signing.key": privatePEM(t, c.key)})
			h, resp := sign(t, s, claims)
			if h != (header{Alg: c.alg, Kid: kid(t, c.key), Typ: "JWT"}) {
				t.Fatalf("header = %+v", h)
			}
			raw, _ := base64.RawURLEncoding.DecodeString(resp.GetHeader())
			var fields map[string]any
			if err := json.Unmarshal(raw, &fields); err != nil || len(fields) != 3 {
				t.Fatalf("header %s has fields other than alg, kid and typ", raw)
			}
			sig, err := base64.RawURLEncoding.Strict().DecodeString(resp.GetSignature())
			if err != nil {
				t.Fatalf("signature is not unpadded base64url: %v", err)
			}
			input := []byte(resp.GetHeader() + "." + claims)
			switch pub := c.key.Public().(type) {
			case *rsa.PublicKey:
				sum := sha256.Sum256(input)
				if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig); err != nil {
					t.Fatal(err)
				}
			case *ecdsa.PublicKey:
				size := (pub.Curve.Params().BitSize + 7) / 8
				if len(sig) != 2*size {
					t.Fatalf("signature of %d bytes, want R||S of %d", len(sig), 2*size)
				}
				var digest []byte
				switch c.alg {
				case "ES256":
					sum := sha256.Sum256(input)
					digest = sum[:]
				case "ES384":
					sum := sha512.Sum384(input)
					digest = sum[:]
				default:
					sum := sha512.Sum512(input)
					digest = sum[:]
				}
				r, s := new(big.Int).SetBytes(sig[:size]), new(big.Int).SetBytes(sig[size:])
				if !ecdsa.Verify(pub, digest, r, s) {
					t.Fatal("signature does not verify over header.claims")
				}
			}
		})
	}
}

func TestSignRejectsClaimsNotUnpaddedBase64URL(t *testing.T) {
	s, _ := loaded(t, map[string][]byte{"signing.key": privatePEM(t, rsaKey)})
	for _, claims := range []string{"eyJhIjoxfQ==", "QR", "eyJhIjoxfQ+/"} {
		_, err := s.Sign(context.Background(), &externaljwtv1.SignJWTRequest{Claims: claims})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("claims %q: got %v, want InvalidArgument", claims, err)
		}
	}
}

func TestKeyIDIsKubeAPIServers(t *testing.T) {
	for _, c := range supportedKeys {
		t.Run(c.name, func(t *testing.T) {
			s, _ := loaded(t, map[string][]byte{"signing.key": privatePEM(t, c.key)})
			keys := fetch(t, s).GetKeys()
			if len(keys) != 1 || keys[0].GetKeyId() != kid(t, c.key) || !bytes.Equal(keys[0].GetKey(), pkix(t, c.key)) {
				t.Fatalf("keys = %v", keys)
			}
		})
	}
}

func TestFetchKeysIsStableUntilTheSetChanges(t *testing.T) {
	files := map[string][]byte{"signing.key": privatePEM(t, rsaKey), "verify-b.pub": publicPEM(t, p256Key)}
	s, dir := loaded(t, files)
	first := fetch(t, s)
	if first.GetRefreshHintSeconds() <= 0 || len(first.GetKeys()) == 0 {
		t.Fatalf("refresh hint %d, %d keys", first.GetRefreshHintSeconds(), len(first.GetKeys()))
	}
	if !slices.Equal(kids(first), sorted(kid(t, rsaKey), kid(t, p256Key))) {
		t.Fatalf("keys = %v", kids(first))
	}

	time.Sleep(10 * time.Millisecond)
	writeFiles(t, dir, files)
	s.Reload()
	if again := fetch(t, s); !proto.Equal(first, again) {
		t.Fatalf("unchanged set, changed response:\n%v\n%v", first, again)
	}

	writeFiles(t, dir, map[string][]byte{"signing.key": privatePEM(t, p256Key), "verify-a.pub": publicPEM(t, rsaKey)})
	s.Reload()
	if swapped := fetch(t, s); !proto.Equal(first, swapped) {
		t.Fatalf("same published keys under another signing key, changed response:\n%v\n%v", first, swapped)
	}

	time.Sleep(10 * time.Millisecond)
	files["verify-c.pub"] = publicPEM(t, p384Key)
	writeFiles(t, dir, files)
	s.Reload()
	changed := fetch(t, s)
	if !changed.GetDataTimestamp().AsTime().After(first.GetDataTimestamp().AsTime()) {
		t.Fatalf("timestamp %v did not move past %v", changed.GetDataTimestamp().AsTime(), first.GetDataTimestamp().AsTime())
	}
	if !slices.Equal(kids(changed), sorted(kid(t, rsaKey), kid(t, p256Key), kid(t, p384Key))) {
		t.Fatalf("keys = %v", kids(changed))
	}
}

func TestBadUpdateKeepsTheLastGoodSet(t *testing.T) {
	ed, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	edDER, err := x509.MarshalPKIXPublicKey(ed)
	if err != nil {
		t.Fatal(err)
	}
	good := map[string][]byte{"signing.key": privatePEM(t, rsaKey), "verify-k2.pub": publicPEM(t, p256Key)}
	cases := []struct {
		name  string
		files map[string][]byte
	}{
		{"no signing key", map[string][]byte{"verify-k2.pub": publicPEM(t, p256Key)}},
		{"garbage signing key", map[string][]byte{"signing.key": []byte("not a key")}},
		{"two private keys", map[string][]byte{"signing.key": append(privatePEM(t, rsaKey), privatePEM(t, p256Key)...)}},
		{"public key as signing key", map[string][]byte{"signing.key": publicPEM(t, rsaKey)}},
		{"RSA 1024", map[string][]byte{"signing.key": privatePEM(t, mustRSA(1024))}},
		{"P-224", map[string][]byte{"signing.key": privatePEM(t, mustECDSA(elliptic.P224()))}},
		{"Ed25519", map[string][]byte{"signing.key": func() []byte {
			_, priv, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			return privatePEM(t, priv)
		}()}},
		{"garbage verification key", map[string][]byte{"signing.key": privatePEM(t, p256Key), "verify-k1.pub": []byte("-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n")}},
		{"private key as verification key", map[string][]byte{"signing.key": privatePEM(t, p256Key), "verify-k1.pub": privatePEM(t, rsaKey)}},
		{"Ed25519 verification key", map[string][]byte{"signing.key": privatePEM(t, p256Key), "verify-k1.pub": pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: edDER})}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, dir := loaded(t, good)
			before := fetch(t, s)
			writeFiles(t, dir, c.files)
			s.Reload()
			if !s.Ready() {
				t.Fatal("not ready after a bad update")
			}
			if after := fetch(t, s); !proto.Equal(before, after) {
				t.Fatalf("bad update replaced the set:\n%v\n%v", before, after)
			}
			if h, _ := sign(t, s, "e30"); h.Kid != kid(t, rsaKey) {
				t.Fatalf("signs with %s after a bad update", h.Kid)
			}
		})
	}
}

func TestNotReadyBeforeASetLoads(t *testing.T) {
	s := newSigner(t, t.TempDir())
	s.Reload()
	if s.Ready() {
		t.Fatal("ready without a key set")
	}
	if _, err := s.FetchKeys(context.Background(), &externaljwtv1.FetchKeysRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("FetchKeys: %v, want Unavailable", err)
	}
	if _, err := s.Sign(context.Background(), &externaljwtv1.SignJWTRequest{Claims: "e30"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Sign: %v, want Unavailable", err)
	}
}

func TestPrivateKeyFormats(t *testing.T) {
	sec1, err := x509.MarshalECPrivateKey(p384Key)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		pem  []byte
		key  crypto.Signer
	}{
		{"PKCS#8 RSA", privatePEM(t, rsaKey), rsaKey},
		{"PKCS#8 ECDSA", privatePEM(t, p256Key), p256Key},
		{"PKCS#1 RSA", pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(rsaKey)}), rsaKey},
		{"SEC1 ECDSA", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: sec1}), p384Key},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _ := loaded(t, map[string][]byte{"signing.key": c.pem})
			if h, _ := sign(t, s, "e30"); h.Kid != kid(t, c.key) {
				t.Fatalf("kid = %s", h.Kid)
			}
		})
	}
}

func TestDirectoryContract(t *testing.T) {
	k1, k2 := rsaKey, p256Key
	cases := []struct {
		name    string
		files   map[string][]byte
		signing crypto.Signer
		keys    []string
	}{
		{"before", map[string][]byte{"signing.key": privatePEM(t, k1)}, k1, sorted(kid(t, k1))},
		{"stage 1: K2 published", map[string][]byte{"signing.key": privatePEM(t, k1), "verify-k2.pub": publicPEM(t, k2)}, k1, sorted(kid(t, k1), kid(t, k2))},
		{"stage 2: K2 signs", map[string][]byte{"signing.key": privatePEM(t, k2), "verify-k1.pub": publicPEM(t, k1)}, k2, sorted(kid(t, k1), kid(t, k2))},
		{"stage 3: K1 dropped", map[string][]byte{"signing.key": privatePEM(t, k2)}, k2, sorted(kid(t, k2))},
		{"duplicates and other files", map[string][]byte{
			"signing.key":    privatePEM(t, k1),
			"verify-k1.pub":  publicPEM(t, k1),
			"verify-k2.pub":  publicPEM(t, k2),
			"verify-dup.pub": publicPEM(t, k2),
			"verify-k3.pem":  []byte("ignored"),
			"k4.pub":         []byte("ignored"),
			"README":         []byte("ignored"),
		}, k1, sorted(kid(t, k1), kid(t, k2))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _ := loaded(t, c.files)
			if got := kids(fetch(t, s)); !slices.Equal(got, c.keys) {
				t.Fatalf("keys = %v, want %v", got, c.keys)
			}
			for _, k := range fetch(t, s).GetKeys() {
				if k.GetExcludeFromOidcDiscovery() {
					t.Fatalf("key %s excluded from OIDC discovery", k.GetKeyId())
				}
			}
			if h, _ := sign(t, s, "e30"); h.Kid != kid(t, c.signing) {
				t.Fatalf("signs with %s", h.Kid)
			}
		})
	}
}

func TestSecretVolumeIsReadThroughData(t *testing.T) {
	dir := t.TempDir()
	writeSecretVolume(t, dir, map[string][]byte{"signing.key": privatePEM(t, rsaKey), "verify-k2.pub": publicPEM(t, otherEC)})
	if err := os.Mkdir(filepath.Join(dir, "..stale"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "..stale", "verify-x.pub"), publicPEM(t, p384Key), 0o600); err != nil {
		t.Fatal(err)
	}
	s := newSigner(t, dir)
	s.Reload()
	if got := kids(fetch(t, s)); !slices.Equal(got, sorted(kid(t, rsaKey), kid(t, otherEC))) {
		t.Fatalf("keys = %v", got)
	}
}

func TestWatchPicksUpASecretUpdate(t *testing.T) {
	dir := t.TempDir()
	writeSecretVolume(t, dir, map[string][]byte{"signing.key": privatePEM(t, rsaKey)})
	s := newSigner(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- s.Watch(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})

	waitFor(t, 5*time.Second, func() bool { return s.Ready() })
	writeSecretVolume(t, dir, map[string][]byte{"signing.key": privatePEM(t, p256Key), "verify-k1.pub": publicPEM(t, rsaKey)})
	waitFor(t, 5*time.Second, func() bool {
		h, _ := sign(t, s, "e30")
		return h.Kid == kid(t, p256Key)
	})
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("not true after %s", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestListenReplacesAStaleSocketWithMode0600(t *testing.T) {
	dir, err := os.MkdirTemp("", "signer")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "s.sock")
	if err := os.WriteFile(path, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	lis, err := signer.Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Type() != os.ModeSocket || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v, want a socket with 0600", info.Mode())
	}
}

func TestMetadata(t *testing.T) {
	s := newSigner(t, t.TempDir())
	resp, err := s.Metadata(context.Background(), &externaljwtv1.MetadataRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetMaxTokenExpirationSeconds() != 365*24*3600 {
		t.Fatalf("max_token_expiration_seconds = %d", resp.GetMaxTokenExpirationSeconds())
	}
}
