// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestReadinessTakesTheConditionOfTheWayOutWhereItStandsInTheChainOfCauses(t *testing.T) {
	const build, reach = "CertificatesReady DatastoreReady APIServerReady AccessReady AddonsApplied", "Reachable ControllersLeading"
	for _, c := range []struct {
		name     string
		endpoint lodiav1alpha1.Endpoint
		want     string
	}{
		{"gateway", lodiav1alpha1.Endpoint{Gateway: &lodiav1alpha1.GatewayEndpoint{}}, "RoutesReady " + build + " " + reach},
		{"a load balancer without a host", lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{}}, "LoadBalancerReady " + build + " " + reach},
		{"a load balancer at an IP address", lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{Host: "203.0.113.10"}}, build + " LoadBalancerReady " + reach},
		{"a load balancer at a name", lodiav1alpha1.Endpoint{LoadBalancer: &lodiav1alpha1.LoadBalancerEndpoint{Host: "api.dev.example.com"}}, build + " LoadBalancerReady " + reach},
		{"a node port", lodiav1alpha1.Endpoint{NodePort: &lodiav1alpha1.NodePortEndpoint{}}, build + " " + reach},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := strings.Join(readiness(&lodiav1alpha1.ControlPlane{Spec: lodiav1alpha1.ControlPlaneSpec{Endpoint: c.endpoint}}), " "); got != c.want {
				t.Errorf("Ready is made of %s, want %s", got, c.want)
			}
		})
	}
}

// Ready True takes the first refusal's reason among its inputs, one that blocks before RefusedBeforeHold, which keeps no revision from its grant, and the oldest generation, and a False input still comes first.
func TestReadyTrueTakesTheFirstRefusalAmongItsInputs(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Generation: 3}}
	for _, conditionType := range readiness(cp) {
		cp.Status.Conditions = append(cp.Status.Conditions, metav1.Condition{Type: conditionType, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, ObservedGeneration: 3})
	}
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionAccessReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonRefused, Message: "ClusterRoleBinding lodia:operator is refused", ObservedGeneration: 3})
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonAuthenticationInvalid, Message: "every replica is ready; not written", ObservedGeneration: 2})
	got := ready(cp)
	if got.Status != metav1.ConditionTrue || got.Reason != lodiav1alpha1.ReasonAuthenticationInvalid || got.Message != "APIServerReady: every replica is ready; not written; also AccessReady (Refused)" || got.ObservedGeneration != 2 {
		t.Errorf("Ready is %+v, want True AuthenticationInvalid naming APIServerReady at generation 2", got)
	}
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionAPIServerReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonRefusedBeforeHold, Message: "every replica is ready; cannot be tried", ObservedGeneration: 2})
	if got := ready(cp); got.Status != metav1.ConditionTrue || got.Reason != lodiav1alpha1.ReasonRefused || got.Message != "AccessReady: ClusterRoleBinding lodia:operator is refused; also APIServerReady (RefusedBeforeHold)" {
		t.Errorf("Ready is %+v, want True Refused naming AccessReady, then APIServerReady", got)
	}
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionAccessReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, ObservedGeneration: 3})
	if got := ready(cp); got.Status != metav1.ConditionTrue || got.Reason != lodiav1alpha1.ReasonRefusedBeforeHold || got.Message != "APIServerReady: every replica is ready; cannot be tried" {
		t.Errorf("Ready is %+v, want True RefusedBeforeHold naming APIServerReady", got)
	}
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionReachable, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonExternalProbeFailed, Message: "api: refused", ObservedGeneration: 3})
	if got := ready(cp); got.Status != metav1.ConditionFalse || got.Reason != lodiav1alpha1.ReasonExternalProbeFailed {
		t.Errorf("Ready is %+v, want False ExternalProbeFailed", got)
	}
}

// Ready keeps the first input's status and reason and ends its message with every other input in trouble, so a restore whose Backup was replaced names DatastoreReady's RestoreFailed beside PKI's wait.
func TestReadyNamesEveryOtherInputInTrouble(t *testing.T) {
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Generation: 3}}
	for _, conditionType := range readiness(cp) {
		cp.Status.Conditions = append(cp.Status.Conditions, metav1.Condition{Type: conditionType, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonReady, ObservedGeneration: 3})
	}
	const wait = "dev-encryption-config holds the keys of Backup a-1 with UID 1, and the Backup of that name now has UID 2; PKI checks the Backup's UID and not its keys, and waits while the Backup of that name exists, until status.restore names UID 1; DatastoreReady says how the restore goes"
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionCertificatesReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonWaitingForBackup, Message: wait, ObservedGeneration: 3})
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionFalse, Reason: lodiav1alpha1.ReasonRestoreFailed, Message: "the restored etcd lacks the seal of Backup a-1", ObservedGeneration: 3})
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionAccessReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonRefused, Message: "ClusterRoleBinding lodia:operator is refused", ObservedGeneration: 3})
	if got := ready(cp); got.Status != metav1.ConditionFalse || got.Reason != lodiav1alpha1.ReasonWaitingForBackup || got.Message != "CertificatesReady: "+wait+"; also DatastoreReady (RestoreFailed), AccessReady (Refused)" {
		t.Errorf("Ready is %+v, want CertificatesReady's wait naming DatastoreReady and AccessReady after it", got)
	}
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionCertificatesReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonIssued, ObservedGeneration: 3})
	meta.SetStatusCondition(&cp.Status.Conditions, metav1.Condition{Type: lodiav1alpha1.ConditionDatastoreReady, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonSecretNotFound, Message: "DatastoreClass shared is held", ObservedGeneration: 3})
	if got := ready(cp); got.Status != metav1.ConditionTrue || got.Reason != lodiav1alpha1.ReasonSecretNotFound || got.Message != "DatastoreReady: DatastoreClass shared is held; also AccessReady (Refused)" {
		t.Errorf("Ready is %+v, want DatastoreReady's refusal naming AccessReady after it", got)
	}
}
