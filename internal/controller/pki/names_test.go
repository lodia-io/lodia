// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki

import (
	"crypto/x509"
	"slices"
	"strings"
	"testing"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestNoSecretNameOfOneControlPlaneIsAnothers(t *testing.T) {
	suffixes := append([]string{string(lodiav1alpha1.EtcdBackupSecret)}, names(gatewayControlPlane())...)
	for _, role := range lodiav1alpha1.EtcdCertificateRoles() {
		suffixes = append(suffixes, "etcd-"+string(role)+"-1", "etcd-"+string(role)+"-12")
	}
	for _, a := range suffixes {
		for _, b := range suffixes {
			if prefix, ok := strings.CutSuffix(a, "-"+b); ok {
				t.Errorf("<n>-%s of a ControlPlane <n> is also <n>-%s-%s of a ControlPlane <n>-%s", a, prefix, b, prefix)
			}
		}
	}
}

func gatewayControlPlane() *lodiav1alpha1.ControlPlane {
	return &lodiav1alpha1.ControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "dev"},
		Spec:       lodiav1alpha1.ControlPlaneSpec{Endpoint: lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{}}},
	}
}

func TestTheTunnelsNamesAndCABelongToGatewayModeOnly(t *testing.T) {
	tunnel := []string{"cakey-tunnel", "tunnel-ca", "tunnel-serving", "tunnel-client"}
	for mode, endpoint := range map[string]lodiav1alpha1.Endpoint{
		"nodePort":     {NodePort: &lodiav1alpha1.NodePortEndpoint{}},
		"loadBalancer": {LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}},
	} {
		cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev"}, Spec: lodiav1alpha1.ControlPlaneSpec{Endpoint: endpoint}}
		if want, got := slices.Concat(names(cp), tunnel), names(gatewayControlPlane()); !slices.Equal(want, got) {
			t.Errorf("%s has the names %v, gateway %v: they differ by more than the tunnel's", mode, names(cp), got)
		}
		if bundles := caBundles(cp); slices.ContainsFunc(bundles, func(b caBundle) bool { return b.ca == tunnelCA }) || len(bundles) != 4 {
			t.Errorf("%s has the CAs %v", mode, bundles)
		}
		other := &tenant{cp: cp}
		for _, name := range tunnel {
			if _, ok := other.reserved("dev-" + name); ok {
				t.Errorf("%s reserves dev-%s", mode, name)
			}
		}
	}
	if bundles := caBundles(gatewayControlPlane()); !slices.Contains(bundles, caBundle{tunnelCA, lodiav1alpha1.TunnelCASecret}) || len(bundles) != 5 {
		t.Errorf("gateway has the CAs %v", bundles)
	}
	own := &tenant{cp: gatewayControlPlane()}
	for _, name := range tunnel {
		if _, ok := own.reserved("dev-" + name); !ok {
			t.Errorf("gateway does not reserve dev-%s", name)
		}
	}
	if _, ok := own.reserved("prod-tunnel-ca"); ok {
		t.Error("gateway reserves another ControlPlane's prod-tunnel-ca")
	}
}

func TestReservedNames(t *testing.T) {
	tn := &tenant{cp: &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev"}}}
	for _, c := range []struct {
		name       string
		reserved   bool
		generation int
	}{
		{"dev-client-ca", true, 0},
		{"dev-kubelet-signer-next", true, 0},
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

func TestTheEtcdCertificatesNameEtcdDruidsServices(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a"}}
	etcd := &druidv1alpha1.Etcd{
		ObjectMeta: metav1.ObjectMeta{Name: lodiav1alpha1.EtcdName(cp.Name), Namespace: cp.Namespace},
		Spec:       druidv1alpha1.EtcdSpec{Replicas: 3},
	}
	certificates := map[lodiav1alpha1.EtcdCertificateRole]*x509.Certificate{}
	for _, p := range (&tenant{cp: cp}).etcdProfiles() {
		certificates[p.role] = &x509.Certificate{DNSNames: p.profile.DNSNames, IPAddresses: p.profile.IPAddresses}
	}
	client := druidv1alpha1.GetClientHostname(etcd)
	var members []string
	for _, pod := range druidv1alpha1.GetAllPodNames(etcd.ObjectMeta, etcd.Spec.Replicas) {
		members = append(members, pod+"."+druidv1alpha1.GetPeerServiceName(etcd.ObjectMeta)+"."+etcd.Namespace+".svc")
	}
	for role, hosts := range map[lodiav1alpha1.EtcdCertificateRole][]string{
		lodiav1alpha1.EtcdServerRole:        append([]string{druidv1alpha1.GetClientServiceName(etcd.ObjectMeta), client}, members...),
		lodiav1alpha1.EtcdPeerRole:          members,
		lodiav1alpha1.EtcdBackupRestoreRole: {client},
	} {
		for _, host := range hosts {
			if err := certificates[role].VerifyHostname(host); err != nil {
				t.Errorf("the %s certificate does not name %s: %v", role, host, err)
			}
		}
	}
}
