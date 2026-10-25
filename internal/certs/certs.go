// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

// Package certs issues a tenant's certificates and kubeconfigs: ECDSA P-256
// CAs and leaves from crypto/x509, PEM in and out.
package certs

import (
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
)

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

// NewCA returns a self-signed CA valid from now for lifetime.
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

// Issue signs a certificate for p under c, valid from now for lifetime.
func (c *Certificate) Issue(p Profile, lifetime time.Duration) (*Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl, err := template(p, lifetime)
	if err != nil {
		return nil, err
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
		NotBefore:             now,
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
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &Certificate{Cert: cert, Key: key}, nil
}

// Matches reports whether cert says what p says: subject, names, usages and
// whether it is a CA.
func Matches(cert *x509.Certificate, p Profile) bool {
	return cert.Subject.CommonName == p.CommonName &&
		slices.Equal(cert.Subject.Organization, p.Organizations) &&
		sameSet(cert.DNSNames, p.DNSNames) &&
		sameSet(ipStrings(cert.IPAddresses), ipStrings(p.IPAddresses)) &&
		slices.Equal(cert.ExtKeyUsage, p.Usages) &&
		cert.IsCA == p.CA
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

// RenewalTime is two thirds into cert's lifetime.
func RenewalTime(cert *x509.Certificate) time.Time {
	return cert.NotBefore.Add(cert.NotAfter.Sub(cert.NotBefore) * 2 / 3)
}

// Verify checks that cert chains to ca and is valid now.
func Verify(cert, ca *x509.Certificate) error {
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	_, err := cert.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
	return err
}

// CertPEM encodes certs, one PEM block each.
func CertPEM(certs ...*x509.Certificate) []byte {
	var out []byte
	for _, c := range certs {
		out = append(out, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw})...)
	}
	return out
}

// KeyPEM encodes key as PKCS#8.
func KeyPEM(key crypto.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

// ParseCerts decodes every certificate in data.
func ParseCerts(data []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	for block, rest := pem.Decode(data); block != nil; block, rest = pem.Decode(rest) {
		if block.Type != "CERTIFICATE" {
			return nil, fmt.Errorf("PEM block %q is not a certificate", block.Type)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, errors.New("no certificate")
	}
	return certs, nil
}

// ParseKey decodes a PKCS#8 private key.
func ParseKey(data []byte) (crypto.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, errors.New("no PKCS#8 private key")
	}
	return x509.ParsePKCS8PrivateKey(block.Bytes)
}

// Parse decodes one certificate and its P-256 key.
func Parse(certPEM, keyPEM []byte) (*Certificate, error) {
	certs, err := ParseCerts(certPEM)
	if err != nil {
		return nil, err
	}
	if len(certs) != 1 {
		return nil, fmt.Errorf("%d certificates, want one", len(certs))
	}
	key, err := ParseKey(keyPEM)
	if err != nil {
		return nil, err
	}
	ec, ok := key.(*ecdsa.PrivateKey)
	if !ok || ec.Curve != elliptic.P256() || !ec.PublicKey.Equal(certs[0].PublicKey) {
		return nil, errors.New("the key is not the certificate's P-256 key")
	}
	return &Certificate{Cert: certs[0], Key: ec}, nil
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
