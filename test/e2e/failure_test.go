// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/e2e-framework/pkg/env"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/types"
)

const (
	dumpTimeout = 3 * time.Minute
	dumpMargin  = dumpTimeout + time.Minute
)

var (
	dumpDir = os.Getenv("E2E_DUMPS")
	dumped  sync.Map
	armed   sync.Once
)

// dumpCluster writes a dump of the cluster into E2E_DUMPS/name, once for each
// name, where the variable is set.
func dumpCluster(ctx context.Context, cfg *envconf.Config, name string) {
	if dumpDir == "" {
		return
	}
	if _, done := dumped.LoadOrStore(name, true); done {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dumpTimeout)
	defer cancel()
	dir := filepath.Join(dumpDir, name)
	cluster, err := kindCluster(cfg.KubeconfigFile())
	if err == nil {
		err = Dump(ctx, cfg.Client().RESTConfig(), cluster, dir)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "the dump %s in %s is incomplete: %v\n", name, dir, err)
		return
	}
	fmt.Fprintf(os.Stderr, "the dump %s is in %s\n", name, dir)
}

// dumpBeforeTimeout dumps the cluster dumpMargin before go test's timeout,
// which kills the process with no chance to dump after it.
func dumpBeforeTimeout(ctx context.Context, cfg *envconf.Config, t *testing.T) (context.Context, error) {
	armed.Do(func() {
		if deadline, ok := t.Deadline(); ok {
			time.AfterFunc(time.Until(deadline)-dumpMargin, func() { dumpCluster(context.Background(), cfg, "timeout") })
		}
	})
	return ctx, nil
}

// dumping dumps the cluster at the first step that fails, before a teardown
// deletes what the failure left.
type dumping struct{ env.Environment }

func (e dumping) Test(t *testing.T, features ...types.Feature) context.Context {
	return e.Environment.Test(t, dumpOnFailure(features)...)
}

func (e dumping) TestInParallel(t *testing.T, features ...types.Feature) context.Context {
	return e.Environment.TestInParallel(t, dumpOnFailure(features)...)
}

func dumpOnFailure(features []types.Feature) []types.Feature {
	out := make([]types.Feature, len(features))
	for i, f := range features {
		out[i] = dumpingFeature{f}
	}
	return out
}

type dumpingFeature struct{ types.Feature }

func (f dumpingFeature) Steps() []types.Step {
	steps := f.Feature.Steps()
	out := make([]types.Step, len(steps))
	for i, s := range steps {
		out[i] = dumpingStep{s}
	}
	return out
}

type dumpingStep struct{ types.Step }

func (s dumpingStep) Func() types.StepFunc {
	inner := s.Step.Func()
	return func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		defer func() {
			if t.Failed() {
				dumpCluster(ctx, cfg, "failure")
			}
		}()
		return inner(ctx, t, cfg)
	}
}
