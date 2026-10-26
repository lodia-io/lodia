// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/distribution/reference"
	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/klient/decoder"
	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
	"sigs.k8s.io/e2e-framework/third_party/helm"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

const (
	release           = "lodia"
	operatorNamespace = "lodia-system"
)

var (
	testenv  env.Environment
	root     = filepath.Join("..", "..")
	chart    = filepath.Join(root, "charts", "lodia")
	examples = filepath.Join(root, "docs", "api", "examples")
)

func TestMain(m *testing.M) {
	path := os.Getenv("E2E_KUBECONFIG")
	if err := RequireNamedCluster(path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	testenv = env.NewWithConfig(envconf.NewWithKubeConfig(path))
	testenv.Setup(installLodia)
	testenv.Finish(removeNodes)
	os.Exit(testenv.Run(m))
}

func installLodia(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
	start := time.Now()
	operator, err := build(ctx, cfg.KubeconfigFile(), "./cmd/lodia")
	if err != nil {
		return ctx, err
	}
	signer, err := build(ctx, cfg.KubeconfigFile(), "./cmd/lodia-signer")
	if err != nil {
		return ctx, err
	}
	if out, err := exec.CommandContext(ctx, "helm", "dependency", "build", chart).CombinedOutput(); err != nil {
		return ctx, fmt.Errorf("build the chart's dependencies: %w\n%s", err, out)
	}
	err = helm.New(cfg.KubeconfigFile()).RunUpgrade(
		helm.WithName(release), helm.WithChart(chart), helm.WithNamespace(operatorNamespace), helm.WithContext(ctx),
		helm.WithArgs("--install", "--create-namespace",
			"--set", "image.repository="+operator.Name(), "--set", "image.tag="+operator.Tag(),
			"--set", "signerImage.repository="+signer.Name(), "--set", "signerImage.tag="+signer.Tag()),
		helm.WithWait(), helm.WithTimeout("5m"))
	if err != nil {
		return ctx, fmt.Errorf("install the chart: %w", err)
	}
	fmt.Printf("the chart installed %s into %s with %s, %s and etcd-druid, after %s\n", release, operatorNamespace, operator, signer, time.Since(start).Round(time.Second))
	return ctx, nil
}

func build(ctx context.Context, kubeconfig, command string) (reference.NamedTagged, error) {
	cluster, err := kindCluster(kubeconfig)
	if err != nil {
		return nil, err
	}
	ko := exec.CommandContext(ctx, "go", "run", "github.com/google/ko@v0.19.1", "build",
		"--base-import-paths", "--platform=linux/"+goruntime.GOARCH, command)
	ko.Dir = root
	ko.Env = append(os.Environ(), "KO_DOCKER_REPO=kind.local", "KIND_CLUSTER_NAME="+cluster)
	var stderr bytes.Buffer
	ko.Stderr = &stderr
	out, err := ko.Output()
	if err != nil {
		return nil, fmt.Errorf("build %s into kind: %w\n%s", command, err, stderr.String())
	}
	named, err := reference.ParseNamed(strings.TrimSpace(string(out)))
	if err != nil {
		return nil, err
	}
	tagged, ok := named.(reference.NamedTagged)
	if !ok {
		return nil, fmt.Errorf("ko named %s without a tag", named)
	}
	return tagged, nil
}

func kindCluster(kubeconfig string) (string, error) {
	cfg, err := clientcmd.LoadFromFile(kubeconfig)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(cfg.CurrentContext, "kind-"), nil
}

func newClient(t *testing.T, cfg *envconf.Config) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, lodiav1alpha1.AddToScheme, druidv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	k8s, err := client.New(cfg.Client().RESTConfig(), client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return k8s
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
