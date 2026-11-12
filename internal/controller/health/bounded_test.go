// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/health"
)

const eightMiB = 8 << 20

func writeOversizeList(w http.ResponseWriter, apiVersion, kind string) {
	const tail = `"}}}]}`
	head := `{"kind":"` + kind + `","apiVersion":"` + apiVersion + `","metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"big","annotations":{"pad":"`
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, head+strings.Repeat("x", eightMiB+1-len(head)-len(tail))+tail)
}

func TestAResponseLongerThanTheBoundIsAListThatCouldNotBeRead(t *testing.T) {
	cp := restoredControlPlane(t, lodiav1alpha1.RestoreHeld)
	setAccess(t, cp)
	writeKubeconfig(t, cp, throughProxy(t, func(w http.ResponseWriter, req *http.Request) bool {
		if req.URL.Path != "/api/v1/services" {
			return false
		}
		writeOversizeList(w, "v1", "ServiceList")
		return true
	}))
	reconcile(t, &health.Reconciler{Client: k8s, Reader: k8s, FailuresBeforeFalse: failuresBeforeFalse, Period: time.Hour, Timeout: 2 * time.Second, Clock: clock.RealClock{}}, cp)
	if err := expectLeading(t, cp, metav1.ConditionFalse, lodiav1alpha1.ReasonAwaitingRelease, actsOn+"Services could not be listed; 0 PersistentVolumes with reclaimPolicy Delete"+
		"; 0 pending PersistentVolumeClaims; 0 webhooks that call a URL or fail closed; 0 APIServices served by a Service; 0 CronJobs not suspended"); err != nil {
		t.Error(err)
	}
}
