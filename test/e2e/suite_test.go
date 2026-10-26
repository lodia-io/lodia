// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/e2e-framework/klient/decoder"
	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

var (
	testenv  env.Environment
	crds     = filepath.Join("..", "..", "api", "crds")
	examples = filepath.Join("..", "..", "docs", "api", "examples")
)

func TestMain(m *testing.M) {
	path := os.Getenv("E2E_KUBECONFIG")
	if err := RequireNamedCluster(path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	testenv = env.NewWithConfig(envconf.NewWithKubeConfig(path))
	testenv.Setup(func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
		_, err := envtest.InstallCRDs(cfg.Client().RESTConfig(), envtest.CRDInstallOptions{Paths: []string{crds}, ErrorIfPathMissing: true})
		return ctx, err
	})
	os.Exit(testenv.Run(m))
}

func TestExamplesOnAKindCluster(t *testing.T) {
	f := features.New("the documented examples").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			for _, name := range []string{"team-a", "team-b"} {
				ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
				if err := cfg.Client().Resources().Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
					t.Fatal(err)
				}
			}
			return ctx
		}).
		Assess("every example is accepted by the installed CRDs", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if err := decoder.DecodeEachFile(ctx, os.DirFS(examples), "*.yaml", decoder.CreateHandler(cfg.Client().Resources())); err != nil {
				t.Fatal(err)
			}
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if err := decoder.DecodeEachFile(ctx, os.DirFS(examples), "*.yaml", decoder.DeleteIgnoreNotFound(cfg.Client().Resources())); err != nil {
				t.Fatal(err)
			}
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}
