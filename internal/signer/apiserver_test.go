// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package signer_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/signer"
)

func TestRotationUnderARunningAPIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run make test")
	}
	const refreshHint = time.Second
	k1, k2 := rsaKey, p256Key

	socketDir, err := os.MkdirTemp("", "signer")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "s.sock")
	keysDir := t.TempDir()
	writeSecretVolume(t, keysDir, map[string][]byte{lodiav1alpha1.SigningKeyKey: privatePEM(t, k1)})

	s := signer.New(keysDir, 8760*time.Hour, refreshHint, slog.New(slog.NewTextHandler(t.Output(), nil)))
	ctx, cancel := context.WithCancel(context.Background())
	lis, err := signer.Listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	watched, served := make(chan error, 1), make(chan error, 1)
	go func() { watched <- s.Watch(ctx) }()
	go func() { served <- s.Serve(ctx, lis) }()
	t.Cleanup(func() {
		cancel()
		for _, ch := range []chan error{watched, served} {
			if err := <-ch; err != nil {
				t.Error(err)
			}
		}
	})
	waitFor(t, 5*time.Second, s.Ready)

	env := &envtest.Environment{UseExistingCluster: new(false)}
	env.ControlPlane.GetAPIServer().Configure().
		Disable("service-account-key-file").
		Disable("service-account-signing-key-file").
		Set("service-account-signing-endpoint", socket)
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	k8s, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	identity := apiServerIdentity(t, k8s)

	sa, err := k8s.CoreV1().ServiceAccounts("default").Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "tenant"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	token := func() string {
		t.Helper()
		tr, err := k8s.CoreV1().ServiceAccounts(sa.Namespace).CreateToken(ctx, sa.Name,
			&authenticationv1.TokenRequest{Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: ptr.To[int64](600)}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return tr.Status.Token
	}
	authenticated := func(token string) bool {
		t.Helper()
		review, err := k8s.AuthenticationV1().TokenReviews().Create(ctx,
			&authenticationv1.TokenReview{Spec: authenticationv1.TokenReviewSpec{Token: token}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if review.Status.Authenticated && review.Status.User.Username != "system:serviceaccount:default:tenant" {
			t.Fatalf("authenticated as %q", review.Status.User.Username)
		}
		return review.Status.Authenticated
	}
	jwks := func() []string {
		t.Helper()
		raw, err := k8s.CoreV1().RESTClient().Get().AbsPath("/openid/v1/jwks").DoRaw(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var set struct {
			Keys []struct {
				Kid string `json:"kid"`
			} `json:"keys"`
		}
		if err := json.Unmarshal(raw, &set); err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, k := range set.Keys {
			out = append(out, k.Kid)
		}
		slices.Sort(out)
		return out
	}

	t1 := token()
	if got := tokenKID(t, t1); got != kid(t, k1) {
		t.Fatalf("token signed by %s, want K1 %s", got, kid(t, k1))
	}
	if !authenticated(t1) {
		t.Fatal("K1 token not authenticated")
	}

	writeSecretVolume(t, keysDir, map[string][]byte{lodiav1alpha1.SigningKeyKey: privatePEM(t, k1), "verify-k2.pub": publicPEM(t, k2)})
	waitFor(t, 30*time.Second, func() bool { return slices.Equal(jwks(), sorted(kid(t, k1), kid(t, k2))) })
	t1ForStage2, t1ForStage3 := token(), token()
	for _, tok := range []string{t1ForStage2, t1ForStage3} {
		if got := tokenKID(t, tok); got != kid(t, k1) {
			t.Fatalf("stage 1: token signed by %s, want K1", got)
		}
	}

	writeSecretVolume(t, keysDir, map[string][]byte{lodiav1alpha1.SigningKeyKey: privatePEM(t, k2), "verify-k1.pub": publicPEM(t, k1)})
	var t2 string
	waitFor(t, 30*time.Second, func() bool {
		t2 = token()
		return tokenKID(t, t2) == kid(t, k2)
	})
	t2ForStage3 := token()
	if !authenticated(t1ForStage2) {
		t.Fatal("stage 2: K1 token rejected")
	}
	if !authenticated(t1) {
		t.Fatal("stage 2: first K1 token rejected")
	}
	if !authenticated(t2) {
		t.Fatal("stage 2: K2 token rejected")
	}

	writeSecretVolume(t, keysDir, map[string][]byte{lodiav1alpha1.SigningKeyKey: privatePEM(t, k2)})
	waitFor(t, 5*time.Second, func() bool { return slices.Equal(kids(fetch(t, s)), []string{kid(t, k2)}) })
	time.Sleep(2 * refreshHint)
	if authenticated(t1ForStage3) {
		t.Fatal("stage 3: K1 token authenticated after one refresh hint")
	}
	if !authenticated(t2ForStage3) {
		t.Fatal("stage 3: K2 token rejected")
	}
	if got := jwks(); !slices.Equal(got, []string{kid(t, k2)}) {
		t.Fatalf("stage 3: jwks lists %v", got)
	}
	start := time.Now()
	waitFor(t, 15*time.Second, func() bool { return !authenticated(t1) })
	t.Logf("stage 3: the first K1 token, authenticated within the token cache's 10s, rejected after %s more", time.Since(start).Round(100*time.Millisecond))

	if got := apiServerIdentity(t, k8s); got != identity {
		t.Fatalf("API server identity changed from %q to %q", identity, got)
	}
}

func tokenKID(t *testing.T, token string) string {
	t.Helper()
	encoded, _, _ := strings.Cut(token, ".")
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var h header
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatal(err)
	}
	return h.Kid
}

func apiServerIdentity(t *testing.T, k8s kubernetes.Interface) string {
	t.Helper()
	var holder string
	waitFor(t, 30*time.Second, func() bool {
		leases, err := k8s.CoordinationV1().Leases("kube-system").List(context.Background(),
			metav1.ListOptions{LabelSelector: "apiserver.kubernetes.io/identity=kube-apiserver"})
		if err != nil {
			t.Fatal(err)
		}
		if len(leases.Items) != 1 || leases.Items[0].Spec.HolderIdentity == nil {
			return false
		}
		holder = *leases.Items[0].Spec.HolderIdentity
		return true
	})
	return holder
}
