// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestAFailedAnswerThatIsNoStatusCarriesNoByteOfItsBody(t *testing.T) {
	const marker = "BODYBYTES"
	for _, c := range []struct {
		code  int
		check func(error) bool
	}{
		{http.StatusInternalServerError, apierrors.IsInternalError},
		{http.StatusForbidden, apierrors.IsForbidden},
	} {
		for _, kind := range []struct {
			name string
			body string
			seen []string
		}{
			{"letters", strings.Repeat(marker, 4096/len(marker)), []string{marker}},
			{"0xff", strings.Repeat("\xff", 4096), []string{"\xff", `\xff`, `\\xff`, "�", `�`}},
		} {
			for _, request := range []struct {
				name string
				do   func(context.Context, *tenant) error
			}{
				{"a patch of the hold", func(ctx context.Context, t *tenant) error {
					_, err := t.patchLease(ctx, []byte(`{}`))
					return err
				}},
				{"a read of a Lease", func(ctx context.Context, t *tenant) error {
					_, err := t.getLease(ctx, "kube-controller-manager")
					return err
				}},
			} {
				t.Run(strconv.Itoa(c.code)+", "+kind.name+", "+request.name, func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						w.Header().Set("Content-Type", "text/plain")
						w.WriteHeader(c.code)
						_, _ = w.Write([]byte(kind.body))
					}))
					t.Cleanup(server.Close)
					tenant, err := (&NodesReconciler{}).tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
					if err != nil {
						t.Fatal(err)
					}
					err = request.do(t.Context(), tenant)
					if err == nil || !c.check(err) || err.Error() != noStatusMessage {
						t.Fatalf("the error is %v, want one of code %d and the message %q", err, c.code, noStatusMessage)
					}
					var logged bytes.Buffer
					crzap.New(crzap.WriteTo(&logged)).Error(err, "the API server refused a write of the controller manager's Lease")
					condition := refusedRow(&lodiav1alpha1.ControlPlane{}, row{write: renewLease}, err).condition
					var answered apierrors.APIStatus
					_ = errors.As(err, &answered)
					status := fmt.Sprintf("%+v", answered.Status())
					for what, text := range map[string]string{"error": err.Error(), "log line": logged.String(), "condition message": condition.Message, "status": status} {
						for _, seen := range kind.seen {
							if strings.Contains(text, seen) {
								t.Errorf("the %s carries the body: %.200q", what, text)
							}
						}
					}
					if logged.Len() > 1024 || len(condition.Message) > 512 {
						t.Errorf("the log line is %d bytes and the condition message %d", logged.Len(), len(condition.Message))
					}
				})
			}
		}
	}
}

func TestAFailedAnswerThatIsNoStatusKeepsItsCodeReasonAndRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("slow down"))
	}))
	t.Cleanup(server.Close)
	r := &NodesReconciler{Timeout: time.Minute}
	tenant, err := r.tenant(types.NamespacedName{Namespace: "team-a", Name: "dev"}, kubeconfigFor(t, server.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, err = tenant.getLease(t.Context(), "x")
	seconds, retry := apierrors.SuggestsClientDelay(err)
	if !apierrors.IsTooManyRequests(err) || !retry || seconds != 7 || err.Error() != noStatusMessage {
		t.Errorf("the error is %v, delay %d, %t; want a 429 that asks for 7 seconds and says %q", err, seconds, retry, noStatusMessage)
	}
}
