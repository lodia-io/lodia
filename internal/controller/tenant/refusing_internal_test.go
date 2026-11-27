// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

// The read back of the objects refused decides AddonsApplied only on an answer: every one held keeps a True, one missing or its read refused gives the refusal, and a read without an answer keeps the condition as it was.
func TestTheReadBackOfRefusedObjectsDecidesOnlyOnAnAnswer(t *testing.T) {
	previous := metav1.Condition{Type: lodiav1alpha1.ConditionAddonsApplied, Status: metav1.ConditionTrue, Reason: lodiav1alpha1.ReasonApplied, Message: "39 objects are applied and the inventory is pruned", ObservedGeneration: 1}
	failures := []*failure{
		{entry: configMap, reason: lodiav1alpha1.ReasonRefused, verdict: "is refused by the tenant's API server"},
		{entry: dns, reason: lodiav1alpha1.ReasonRefused, verdict: "is refused by the tenant's API server"},
	}
	cp := &lodiav1alpha1.ControlPlane{ObjectMeta: metav1.ObjectMeta{Generation: 2}}
	refused := refusal(cp, previous.Type, failures)
	if want := "ConfigMap kube-system/coredns is refused by the tenant's API server; Deployment kube-system/coredns is refused by the tenant's API server; the reason is in Lodia's log"; refused.Message != want {
		t.Errorf("the refusal says %q, want %q", refused.Message, want)
	}
	held := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metadata":{"uid":"7a3c","resourceVersion":"42"}}`))
	}
	status := func(code int, reason metav1.StatusReason) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}, Status: metav1.StatusFailure, Code: int32(code), Reason: reason, Message: "the test says so"})
		}
	}
	unanswered := func(w http.ResponseWriter) {
		if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
			_ = conn.Close()
		}
	}
	standing := refused
	standing.Status, standing.ObservedGeneration = metav1.ConditionTrue, previous.ObservedGeneration
	for _, c := range []struct {
		name                  string
		configMap, deployment func(http.ResponseWriter)
		want                  metav1.Condition
	}{
		{"both held", held, held, standing},
		{"the second missing", held, status(http.StatusNotFound, metav1.StatusReasonNotFound), refused},
		{"the second's read refused", held, status(http.StatusForbidden, metav1.StatusReasonForbidden), refused},
		{"the second's read unanswered", held, unanswered, previous},
		{"the first's read unanswered", unanswered, held, previous},
	} {
		t.Run(c.name, func(t *testing.T) {
			api := standingIn(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "/configmaps/") {
					c.configMap(w)
					return
				}
				c.deployment(w)
			}))
			got, _ := refusing(t.Context(), api, previous, refused, failures)
			if !equality.Semantic.DeepEqual(got, c.want) {
				t.Errorf("AddonsApplied is %+v, want %+v", got, c.want)
			}
		})
	}
}

// The refusal names the first ten objects refused and counts the rest, so that an inventory of the most entries a tenant may list fits a condition's message.
func TestTheRefusalNamesTenObjectsAndCountsTheRest(t *testing.T) {
	long := strings.Repeat("n", 253)
	verdict := "is refused by the tenant's API server: its admission webhook " + long + " cannot be called"
	failures := make([]*failure, 0, maxEntries)
	for i := range maxEntries {
		failures = append(failures, &failure{entry: entry{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRoleBinding", Name: fmt.Sprintf("%s-%03d", long[:249], i)}, reason: lodiav1alpha1.ReasonRefused, verdict: verdict})
	}
	got := refusal(&lodiav1alpha1.ControlPlane{}, lodiav1alpha1.ConditionAddonsApplied, failures).Message
	if !strings.HasPrefix(got, failures[0].entry.String()+" "+verdict+"; ") || !strings.HasSuffix(got, "; "+failures[9].entry.String()+" "+verdict+"; 246 more objects are refused; the reason is in Lodia's log") {
		t.Errorf("the refusal of %d objects says %q", len(failures), got)
	}
	if len(got) > 32768 {
		t.Errorf("the refusal of %d objects is %d bytes, past a condition's message bound of 32768", len(failures), len(got))
	}
}
