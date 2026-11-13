// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"go.lodia.io/lodia/internal/apistatus"
)

func TestAnArrayKeepsItsBoundAndSaysThereWasMore(t *testing.T) {
	elements := func(n int) string {
		return "[" + strings.TrimSuffix(strings.Repeat(`{"type":"Ready"},`, n), ",") + "]"
	}
	for _, c := range []struct {
		name string
		json string
		kept int
		over bool
		fail bool
	}{
		{"the bound", elements(maxConditions), maxConditions, false, false},
		{"one more", elements(maxConditions + 1), maxConditions, true, false},
		{"far more", elements(10 * maxConditions), maxConditions, true, false},
		{"none", `[]`, 0, false, false},
		{"null", `null`, 0, false, false},
		{"an object", `{"a":1}`, 0, false, true},
		{"a string", `"a"`, 0, false, true},
		{"an element of the wrong type", `[{"type":"Ready"},"x"]`, 1, false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := arrayOf("conditions", []byte(c.json), maxConditions, nodeConditionOf)
			if c.fail != (err != nil) || len(got.items) != c.kept || got.over != c.over {
				t.Errorf("decoded %d elements, over %t, error %v; want %d, %t, failing %t", len(got.items), got.over, err, c.kept, c.over, c.fail)
			}
		})
	}

	t.Run("a key twice", func(t *testing.T) {
		n, err := nodeOf([]byte(`{"status":{"conditions":` + elements(40) + `,"conditions":` + elements(70) + `}}`))
		if err != nil {
			t.Fatal(err)
		}
		if len(n.Status.Conditions.items) != maxConditions || !n.Status.Conditions.over {
			t.Errorf("decoded %d elements, over %t, want the last key's %d and true", len(n.Status.Conditions.items), n.Status.Conditions.over, maxConditions)
		}
	})
}

func TestAFieldTheTypeLacksIsSkippedAndKeepsNothing(t *testing.T) {
	body := `{"metadata":{"name":"n","annotations":{"a":"` + strings.Repeat("x", 1<<20) + `"},"managedFields":[{},{}]},"status":{"nodeInfo":{"kubeletVersion":"v1.35.0","machineID":"m"},"images":[{},{}],"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-11-13T10:00:00Z","message":"m"}]}}`
	n, err := nodeOf([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	since, _ := time.Parse(time.RFC3339, "2026-11-13T10:00:00Z")
	want := nodeObject{}
	want.Metadata.Name = "n"
	want.Status.NodeInfo.KubeletVersion = "v1.35.0"
	want.Status.Conditions.items = []nodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(since)}}
	if n.Metadata != want.Metadata || n.Status.NodeInfo != want.Status.NodeInfo || len(n.Status.Conditions.items) != 1 || !n.Status.Conditions.items[0].LastTransitionTime.Equal(&want.Status.Conditions.items[0].LastTransitionTime) || n.Status.Conditions.items[0].Type != corev1.NodeReady || n.Status.Conditions.items[0].Status != corev1.ConditionTrue {
		t.Errorf("decoded %+v, want %+v", n, want)
	}
}

func TestAFailedRequestIsTheStatusTheServerSentAndNeverMoreThanItsFields(t *testing.T) {
	failure := func(code int, reason, message string) string {
		return `{"kind":"Status","status":"Failure","reason":"` + reason + `","code":` + strconv.Itoa(code) + `,"message":"` + message + `"`
	}
	terminating := func(others int) string {
		causes := strings.Repeat(`{"reason":"FieldValueInvalid"},`, others) + `{"reason":"NamespaceTerminating"}`
		return failure(403, "Forbidden", "terminating") + `,"details":{"causes":[` + causes + `]}}`
	}
	for _, c := range []struct {
		name    string
		code    int
		body    string
		check   func(error) bool
		message string
		refused bool
	}{
		{"not found", http.StatusNotFound, failure(404, "NotFound", `leases x not found`) + `}`, apierrors.IsNotFound, "leases x not found", false},
		{"not found without a Status", http.StatusNotFound, `404 page not found`, apierrors.IsNotFound, "", false},
		{"a conflict", http.StatusConflict, failure(409, "Conflict", "the object has been modified") + `}`, apierrors.IsConflict, "the object has been modified", false},
		{"an existing Lease", http.StatusConflict, failure(409, "AlreadyExists", "exists") + `}`, apierrors.IsAlreadyExists, "exists", false},
		{"a refusal", http.StatusForbidden, failure(403, "Forbidden", "denied by a policy") + `}`, apierrors.IsForbidden, "denied by a policy", true},
		{"a namespace being deleted", http.StatusForbidden, terminating(0), apierrors.IsForbidden, "terminating", false},
		{"a namespace being deleted at the last cause it keeps", http.StatusForbidden, terminating(63), apierrors.IsForbidden, "terminating", false},
		{"a namespace being deleted past the causes it keeps", http.StatusForbidden, terminating(64), apierrors.IsForbidden, "terminating", true},
		{"a Status that is no failure", http.StatusInternalServerError, `{"kind":"Status","status":"Success","code":500,"message":"fine"}`, apierrors.IsInternalError, "an error on the server (\"unknown\") has prevented the request from succeeding", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(c.code)
				_, _ = w.Write([]byte(c.body))
			}))
			t.Cleanup(server.Close)
			r := &NodesReconciler{}
			tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
			if err != nil {
				t.Fatal(err)
			}
			_, err = tenant.getLease(t.Context(), "x")
			if err == nil || !c.check(err) {
				t.Fatalf("the error is %v, want one the API's own checks recognise", err)
			}
			if c.message != "" && err.Error() != c.message {
				t.Errorf("the error says %q, want %q", err.Error(), c.message)
			}
			if got := apistatus.Refused(err); got != c.refused {
				t.Errorf("Refused is %t, want %t", got, c.refused)
			}
		})
	}
}

func TestAFailedRequestKeepsNoMoreThan64OfAnyNumberOfCauses(t *testing.T) {
	for _, causes := range []int{1, 63, 64, 65, 1000, 100_000} {
		body := `{"kind":"Status","status":"Failure","reason":"Invalid","code":422,"details":{"causes":[` + strings.TrimSuffix(strings.Repeat(`{"reason":"FieldValueInvalid"},`, causes), ",") + `]}}`
		failure, ok := failureOf([]byte(body))
		if kept := len(failure.Details.Causes); !ok || kept != min(causes, 64) {
			t.Errorf("a Status of %d causes keeps %d, want %d", causes, kept, min(causes, 64))
		}
	}
}

func TestAnAnswerThatIsNoJSONIsAFailedRead(t *testing.T) {
	for _, c := range []struct {
		name string
		code int
		body string
	}{
		{"HTML", http.StatusOK, `<html></html>`},
		{"protobuf", http.StatusOK, "k8s\x00\n\x04\n\x02v1"},
		{"a list as a Lease", http.StatusOK, `[]`},
		{"a Lease cut in an object", http.StatusOK, `{"metadata":{"resourceVersion":"1"},"spec":{"holderIdentity":"h"`},
		{"a Lease cut in a string", http.StatusOK, `{"metadata":{"resourceVersion":"1"},"spec":{"holderIdentity":"h`},
		{"a Lease cut in an array of a list", http.StatusOK, `{"metadata":{},"items":[{"metadata":{"name":"n"}},`},
		{"a Status cut in an object", http.StatusInternalServerError, `{"kind":"Status","status":"Failure","message":"m","details":{"causes":[{"reason":"r"}`},
		{"a Status cut in a string", http.StatusInternalServerError, `{"kind":"Status","status":"Failure","message":"m`},
		{"two documents", http.StatusOK, `{"metadata":{"resourceVersion":"1"}}{"metadata":{"resourceVersion":"2"}}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(c.code)
				_, _ = w.Write([]byte(c.body))
			}))
			t.Cleanup(server.Close)
			r := &NodesReconciler{}
			tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tenant.getLease(t.Context(), "x"); err == nil {
				t.Error("an answer that is no Lease was read")
			}
			if _, err := tenant.nodePage(t.Context(), metav1.ListOptions{Limit: nodePage}); err == nil {
				t.Error("an answer that is no list was read")
			}
		})
	}
}
