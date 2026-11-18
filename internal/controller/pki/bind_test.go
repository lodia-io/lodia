// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package pki_test

import (
	"context"
	"net"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestATenantThatNeverAnswersEndsTheBindWithinTenSecondsAndKeepsTheStatusWritable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var held []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			held = append(held, conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		for _, conn := range held {
			_ = conn.Close()
		}
	})
	r := newReconciler(time.Hour)
	r.Dial = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, listener.Addr().String())
	}
	cp := controlPlane(t, "pki-bind-silent")
	publishEndpoint(t, cp)
	reconcile(t, r, cp)
	setAPIServerReady(t, cp)

	start := time.Now()
	_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cp)})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a pass whose tenant answered nothing succeeded")
	}
	if elapsed < 9*time.Second || elapsed > 12*time.Second {
		t.Errorf("the bind took %s, want the bindTimeout of 10 s", elapsed)
	}
	expectCondition(t, cp, lodiav1alpha1.ConditionAccessReady, metav1.ConditionFalse, lodiav1alpha1.ReasonBinding)
}
