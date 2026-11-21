// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid

import (
	"maps"
	"slices"
	"testing"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestTheOperatorsNamespaceIsAdmittedOnce(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a"}}
	etcd := &druidv1alpha1.Etcd{ObjectMeta: metav1.ObjectMeta{Name: "dev-etcd", Namespace: "team-a"}}
	for _, row := range []struct {
		operator string
		want     []string
	}{
		{"lodia-system", []string{"druid-system", "lodia-system"}},
		{"druid-system", []string{"druid-system"}},
	} {
		var namespaces []string
		for _, rule := range networkPolicy(cp, etcd, "druid-system", row.operator).Spec.Ingress {
			for _, peer := range rule.From {
				if peer.NamespaceSelector == nil {
					continue
				}
				namespaces = append(namespaces, peer.NamespaceSelector.MatchLabels[corev1.LabelMetadataName])
				if row.operator != "druid-system" && peer.NamespaceSelector.MatchLabels[corev1.LabelMetadataName] == row.operator &&
					(len(rule.Ports) != 1 || rule.Ports[0].Port.IntValue() != 2379) {
					t.Errorf("the operator's rule admits %+v, want the client port alone", rule.Ports)
				}
			}
		}
		if !slices.Equal(namespaces, row.want) {
			t.Errorf("with the operator in %s the policy admits the namespaces %v, want %v", row.operator, namespaces, row.want)
		}
	}
}

func TestTheRehearsalsCopyReachesTheClientPortAlone(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "team-a"}}
	etcd := &druidv1alpha1.Etcd{
		ObjectMeta: metav1.ObjectMeta{Name: "dev-etcd", Namespace: "team-a"},
		Spec:       druidv1alpha1.EtcdSpec{Backup: druidv1alpha1.BackupSpec{Store: &druidv1alpha1.StoreSpec{}}},
	}
	copyPods := map[string]string{lodiav1alpha1.ControlPlaneLabel: "dev", lodiav1alpha1.ComponentLabel: lodiav1alpha1.RehearsalComponent}
	var rules int
	for _, rule := range networkPolicy(cp, etcd, "druid-system", "lodia-system").Spec.Ingress {
		for _, peer := range rule.From {
			if peer.PodSelector == nil || !maps.Equal(peer.PodSelector.MatchLabels, copyPods) {
				continue
			}
			rules++
			if len(rule.From) != 1 || len(rule.Ports) != 1 || rule.Ports[0].Port.IntValue() != 2379 || *rule.Ports[0].Protocol != corev1.ProtocolTCP {
				t.Errorf("the copy's rule admits %d peers to %+v, want it alone to the client port", len(rule.From), rule.Ports)
			}
		}
	}
	if rules != 1 {
		t.Errorf("%d rules admit the copy's pods, want 1", rules)
	}
}
