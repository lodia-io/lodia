// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package certs issues a tenant's certificates and kubeconfigs: ECDSA P-256
// CAs and leaves from crypto/x509, PEM in and out.
package certs

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"slices"
	"strings"
	"time"

	"k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/keyutil"
)

// Backdate is how far before its issuance a certificate is valid, so a
// verifier whose clock runs behind accepts it; kubeadm's offset.
const Backdate = 5 * time.Minute

// Certificate is a certificate with its private key.
type Certificate struct {
	Cert *x509.Certificate
	Key  *ecdsa.PrivateKey
}

// Profile is what a certificate says about its holder.
type Profile struct {
	CommonName    string
	Organizations []string
	DNSNames      []string
	IPAddresses   []net.IP
	Usages        []x509.ExtKeyUsage
	// CA makes an intermediate that signs leaves and no further CA.
	CA bool
}

// NewCA returns a self-signed CA valid for lifetime.
func NewCA(commonName string, lifetime time.Duration) (*Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl, err := template(Profile{CommonName: commonName, CA: true}, lifetime)
	if err != nil {
		return nil, err
	}
	return create(tmpl, tmpl, key, key)
}

// Issue signs a certificate for p under c, valid for lifetime or until c
// expires, whichever is sooner.
func (c *Certificate) Issue(p Profile, lifetime time.Duration) (*Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl, err := template(p, lifetime)
	if err != nil {
		return nil, err
	}
	if tmpl.NotAfter.After(c.Cert.NotAfter) {
		tmpl.NotAfter = c.Cert.NotAfter
	}
	tmpl.MaxPathLenZero = p.CA
	return create(tmpl, c.Cert, key, c.Key)
}

func template(p Profile, lifetime time.Duration) (*x509.Certificate, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: p.CommonName, Organization: p.Organizations},
		DNSNames:              p.DNSNames,
		IPAddresses:           p.IPAddresses,
		NotBefore:             now.Add(-Backdate),
		NotAfter:              now.Add(lifetime),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           p.Usages,
		BasicConstraintsValid: true,
		IsCA:                  p.CA,
	}
	if p.CA {
		tmpl.KeyUsage |= x509.KeyUsageCertSign | x509.KeyUsageCRLSign
	}
	return tmpl, nil
}

func create(tmpl, parent *x509.Certificate, key, signer *ecdsa.PrivateKey) (*Certificate, error) {
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, signer)
	if err != nil {
		return nil, err
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &Certificate{Cert: parsed, Key: key}, nil
}

// Matches reports whether cert says what p says: subject, names, usages and
// whether it is a CA.
func Matches(c *x509.Certificate, p Profile) bool {
	return c.Subject.CommonName == p.CommonName &&
		slices.Equal(c.Subject.Organization, p.Organizations) &&
		sameSet(c.DNSNames, p.DNSNames) &&
		sameSet(ipStrings(c.IPAddresses), ipStrings(p.IPAddresses)) &&
		slices.Equal(c.ExtKeyUsage, p.Usages) &&
		c.IsCA == p.CA
}

func sameSet(a, b []string) bool {
	return slices.Equal(slices.Sorted(slices.Values(a)), slices.Sorted(slices.Values(b)))
}

func ipStrings(ips []net.IP) []string {
	out := make([]string, len(ips))
	for i, ip := range ips {
		out[i] = ip.String()
	}
	return out
}

// RenewalTime is two thirds of the way from a certificate's issuance (its
// NotBefore plus the Backdate) to its NotAfter.
func RenewalTime(c *x509.Certificate) time.Time {
	issued := c.NotBefore.Add(Backdate)
	if issued.After(c.NotAfter) {
		issued = c.NotBefore
	}
	return issued.Add(c.NotAfter.Sub(issued) * 2 / 3)
}

// Verify checks that c chains to ca and is valid now.
func Verify(c, ca *x509.Certificate) error {
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	_, err := c.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
	return err
}

// CertPEM encodes one certificate.
func CertPEM(c *x509.Certificate) ([]byte, error) {
	return cert.EncodeCertificates(c)
}

// Bundle encodes a set of certificates: each once, in DER order, so the
// bytes do not depend on the order given.
func Bundle(certs ...*x509.Certificate) ([]byte, error) {
	set := slices.Clone(certs)
	slices.SortFunc(set, func(a, b *x509.Certificate) int { return bytes.Compare(a.Raw, b.Raw) })
	return cert.EncodeCertificates(slices.CompactFunc(set, (*x509.Certificate).Equal)...)
}

// KeyPEM encodes key as PKCS#8.
func KeyPEM(key crypto.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: keyutil.PrivateKeyBlockType, Bytes: der}), nil
}

// ParseCerts decodes every certificate in data.
func ParseCerts(data []byte) ([]*x509.Certificate, error) {
	return cert.ParseCertsPEM(data)
}

// ParseKey decodes a PEM private key.
func ParseKey(data []byte) (crypto.PrivateKey, error) {
	return keyutil.ParsePrivateKeyPEM(data)
}

// Parse decodes one certificate and its P-256 key.
func Parse(certPEM, keyPEM []byte) (*Certificate, error) {
	parsed, err := ParseCerts(certPEM)
	if err != nil {
		return nil, err
	}
	if len(parsed) != 1 {
		return nil, fmt.Errorf("%d certificates, want one", len(parsed))
	}
	key, err := ParseKey(keyPEM)
	if err != nil {
		return nil, err
	}
	ec, ok := key.(*ecdsa.PrivateKey)
	if !ok || ec.Curve != elliptic.P256() || !ec.PublicKey.Equal(parsed[0].PublicKey) {
		return nil, errors.New("the key is not the certificate's P-256 key")
	}
	return &Certificate{Cert: parsed[0], Key: ec}, nil
}

// SAN appends host to p as an IP address or as a DNS name, once.
func (p *Profile) SAN(host string) {
	if ip := net.ParseIP(host); ip != nil {
		if !slices.ContainsFunc(p.IPAddresses, ip.Equal) {
			p.IPAddresses = append(p.IPAddresses, ip)
		}
		return
	}
	host = strings.ToLower(host)
	if host != "" && !slices.Contains(p.DNSNames, host) {
		p.DNSNames = append(p.DNSNames, host)
	}
}
