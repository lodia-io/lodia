// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package health_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/controller/health"
	"go.lodia.io/lodia/internal/testenv"
)

type copyShowing struct {
	client.Reader
	name  string
	shown func(*corev1.Secret)
}

func (l copyShowing) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := l.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if secret, ok := obj.(*corev1.Secret); ok && key.Name == l.name {
		l.shown(secret)
	}
	return nil
}

func TestACopyOfAnAccessSecretThatLagsMakesNoPassWaitAndAPassThatWaitsReadsItLive(t *testing.T) {
	stranger := func(s *corev1.Secret) {
		s.OwnerReferences = []metav1.OwnerReference{{APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret", Name: "stranger", UID: "abc", Controller: new(true)}}
	}
	for _, c := range []struct {
		what   string
		secret lodiav1alpha1.Secret
		shown  func(*corev1.Secret)
	}{
		{"the CA bundle kept by another owner", lodiav1alpha1.CABundleSecret, stranger},
		{"the CA bundle with no certificate", lodiav1alpha1.CABundleSecret, func(s *corev1.Secret) {
			s.Data = map[string][]byte{lodiav1alpha1.CACertKey: []byte("not a certificate")}
		}},
		{"the tunnel's client pair kept by another owner", lodiav1alpha1.TunnelClientSecret, stranger},
		{"the tunnel's client pair with no key pair", lodiav1alpha1.TunnelClientSecret, func(s *corev1.Secret) { s.Data = nil }},
		{"Lodia's kubeconfig kept by another owner", lodiav1alpha1.LodiaKubeconfigSecret, stranger},
	} {
		t.Run(c.what, func(t *testing.T) {
			f := newGateway(t)
			f.publishWorking(t)
			setAccess(t, f.cp)
			writeKubeconfig(t, f.cp, tenantKubeconfig)
			name := c.secret.Name(f.cp.Name)
			live, liveReads := readsOf(t)
			r := &health.Reconciler{Client: k8s, Reader: live, Secrets: copyShowing{Reader: k8s, name: name, shown: c.shown}, FailuresBeforeFalse: failuresBeforeFalse, DialContext: dialService, Period: time.Hour, Timeout: 2 * time.Second, Clock: clock.RealClock{}}
			reconcile(t, r, f.cp)
			if got := *liveReads; !slices.Equal(got, []string{name}) {
				t.Errorf("a pass whose copy of %s lags read the Secrets %v from the API server, want only it", name, got)
			}
			testenv.Eventually(t, func() error {
				return expect(t, f.cp, metav1.ConditionTrue, lodiav1alpha1.ReasonReachable, f.allAnswered())
			})
			if c := meta.FindStatusCondition(get(t, f.cp).Status.Conditions, lodiav1alpha1.ConditionControllersLeading); c != nil && strings.Contains(c.Message, "is kept by") {
				t.Errorf("ControllersLeading says %q after a pass whose copy lags", c.Message)
			}
		})
	}
}

func TestHealthNodesDecidesAWaitOnTheKubeconfigTheAPIServerHolds(t *testing.T) {
	cp := tenantControlPlane(t)
	joined(t, "a", corev1.ConditionTrue, time.Now())
	name := lodiav1alpha1.LodiaKubeconfigSecret.Name(cp.Name)
	live, liveReads := readsOf(t)
	r := nodesReconciler(clocktesting.NewFakePassiveClock(time.Now().Truncate(time.Second)))
	r.Reader = live
	r.Secrets = copyShowing{Reader: k8s, name: name, shown: func(s *corev1.Secret) {
		s.OwnerReferences = []metav1.OwnerReference{{APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret", Name: "stranger", UID: "abc", Controller: new(true)}}
	}}
	sample(t, r, cp)
	expectNodes(t, cp, nodesOf(1, 1, 0, "v1.35.0"))
	if got := *liveReads; !slices.Equal(got, []string{name}) {
		t.Errorf("a pass whose copy of %s lags read the Secrets %v from the API server, want only it", name, got)
	}
}
