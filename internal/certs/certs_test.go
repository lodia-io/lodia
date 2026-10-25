// Copyright 2026 The Brygga Authors.
// SPDX-License-Identifier: Apache-2.0

package certs_test

import (
	"bytes"
	"crypto/x509"
	"testing"
	"time"

	"github.com/segla-io/brygga/internal/certs"
)

func TestALeafLivesWithinItsCAAndIsBackdated(t *testing.T) {
	before := time.Now()
	ca, err := certs.NewCA("cluster-ca@1", before, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue(certs.Profile{CommonName: "kube-apiserver", Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, before, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]*x509.Certificate{"the CA": ca.Cert, "the leaf": leaf.Cert} {
		if issued := c.NotBefore.Add(certs.Backdate); issued.After(time.Now()) || issued.Before(before.Add(-time.Second)) {
			t.Errorf("%s is valid from %s, want five minutes before its issuance", name, c.NotBefore)
		}
	}
	if leaf.Cert.NotAfter.After(ca.Cert.NotAfter) {
		t.Errorf("the leaf expires at %s, after its CA at %s", leaf.Cert.NotAfter, ca.Cert.NotAfter)
	}
	if !leaf.Cert.NotAfter.Equal(ca.Cert.NotAfter) {
		t.Errorf("the leaf expires at %s, want the CA's %s", leaf.Cert.NotAfter, ca.Cert.NotAfter)
	}
	issued := leaf.Cert.NotBefore.Add(certs.Backdate)
	if due := certs.RenewalTime(leaf.Cert); due.Sub(issued) < 39*time.Minute || due.Sub(issued) > 41*time.Minute {
		t.Errorf("renewal %s after issuance, want two thirds of the hour the CA leaves", due.Sub(issued))
	}
	if err := certs.Verify(leaf.Cert, ca.Cert, before); err != nil {
		t.Fatal(err)
	}
}

func TestOnePassIssuesFromOneClock(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 999_999_999, time.UTC)
	ca, err := certs.NewCA("cluster-ca@1", now, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := ca.Issue(certs.Profile{CommonName: "kube-apiserver", Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, now, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	notBefore := now.Add(-certs.Backdate).Truncate(time.Second)
	if !ca.Cert.NotBefore.Equal(notBefore) || !leaf.Cert.NotBefore.Equal(notBefore) {
		t.Errorf("the CA is valid from %s and the leaf from %s, want both from %s", ca.Cert.NotBefore, leaf.Cert.NotBefore, notBefore)
	}
	if want := now.Add(30 * time.Minute).Truncate(time.Second); !leaf.Cert.NotAfter.Equal(want) {
		t.Errorf("the leaf ends at %s, want %s", leaf.Cert.NotAfter, want)
	}
	if err := certs.Verify(leaf.Cert, ca.Cert, now); err != nil {
		t.Errorf("the leaf does not verify at the time it was issued: %v", err)
	}
	if err := certs.Verify(leaf.Cert, ca.Cert, leaf.Cert.NotAfter.Add(time.Second)); err == nil {
		t.Error("the leaf verifies after it expired")
	}
	if _, err := ca.Issue(certs.Profile{CommonName: "kube-apiserver"}, ca.Cert.NotAfter, time.Minute); err == nil {
		t.Error("the CA issued at its NotAfter")
	}
}

func TestAnExpiredCASignsNothing(t *testing.T) {
	now := time.Now()
	ca, err := certs.NewCA("cluster-ca@1", now, -time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ca.Issue(certs.Profile{CommonName: "kube-apiserver", Usages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, now, time.Hour); err == nil {
		t.Fatal("an expired CA issued a certificate")
	}
}

func TestABundleDoesNotDependOnOrder(t *testing.T) {
	var all []*x509.Certificate
	for _, name := range []string{"a", "b", "c"} {
		ca, err := certs.NewCA(name, time.Now(), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, ca.Cert)
	}
	a, b, c := all[0], all[1], all[2]
	want, err := certs.Bundle(a, b, c)
	if err != nil {
		t.Fatal(err)
	}
	for _, order := range [][]*x509.Certificate{{c, b, a}, {b, a, c, a}, {c, c, a, b, b}} {
		got, err := certs.Bundle(order...)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("the bundle of %d certificates differs from the sorted set", len(order))
		}
	}
	parsed, err := certs.ParseCerts(want)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 3 {
		t.Errorf("the bundle holds %d certificates, want 3", len(parsed))
	}
	for i := 1; i < len(parsed); i++ {
		if bytes.Compare(parsed[i-1].Raw, parsed[i].Raw) >= 0 {
			t.Error("the bundle is not in DER order")
		}
	}
}
