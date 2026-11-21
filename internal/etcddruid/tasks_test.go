// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid_test

import (
	"testing"
	"time"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"go.lodia.io/lodia/internal/etcddruid"
)

func TestAFailedCopySaysWhyByItsMessageThenItsLastErrorThenItsReason(t *testing.T) {
	s := runtime.NewScheme()
	if err := druidv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	at := metav1.NewTime(time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC))
	const deleteFailed = "could not delete job team-a/restore-1-worker: the server is currently unable to handle the request"
	for _, c := range []struct {
		name, message string
		lastError     *string
		why           string
	}{
		{"a message and a lastError", "Job has reached the specified backoff limit", new(deleteFailed), "Job has reached the specified backoff limit"},
		{"a lastError alone", "", new(deleteFailed), deleteFailed},
		{"neither", "", nil, "BackoffLimitExceeded"},
	} {
		t.Run(c.name, func(t *testing.T) {
			task := &druidv1alpha1.EtcdCopyBackupsTask{
				ObjectMeta: metav1.ObjectMeta{Name: "restore-1", Namespace: "team-a"},
				Status: druidv1alpha1.EtcdCopyBackupsTaskStatus{
					Conditions: []druidv1alpha1.Condition{{
						Type: druidv1alpha1.EtcdCopyBackupsTaskFailed, Status: druidv1alpha1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: c.message,
						LastTransitionTime: at, LastUpdateTime: at,
					}},
					LastError: c.lastError,
				},
			}
			copied, err := (&etcddruid.Engine{}).CopyOf(t.Context(), fake.NewClientBuilder().WithScheme(s).WithObjects(task).Build(), client.ObjectKeyFromObject(task))
			if err != nil {
				t.Fatal(err)
			}
			if why, when, failed := copied.Failed(); !failed || why != c.why || !when.Equal(at.Time) {
				t.Errorf("the copy failed %t at %s because %q, want at %s because %q", failed, when, why, at.Time, c.why)
			}
		})
	}
}
