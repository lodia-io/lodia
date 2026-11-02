// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
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
