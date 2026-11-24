// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

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
)

// dumpCluster writes a dump of the cluster into E2E_DUMPS/name, once for each
// name, where the variable is set, in place of what an earlier run left there.
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
		err = os.RemoveAll(dir)
	}
	if err == nil {
		err = Dump(ctx, cfg.Client().RESTConfig(), cluster, dir)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "the dump %s in %s is incomplete: %v\n", name, dir, err)
		return
	}
	fmt.Fprintf(os.Stderr, "the dump %s is in %s\n", name, dir)
}

// dumpBeforeTimeout dumps the cluster dumpMargin before the timeout of go test,
// counted from now, which kills the process with no chance to dump after it.
func dumpBeforeTimeout(cfg *envconf.Config, timeout time.Duration) {
	if timeout > 0 {
		time.AfterFunc(timeout-dumpMargin, func() { dumpCluster(context.Background(), cfg, "timeout") })
	}
}

// dumping dumps the cluster at the first Setup func of the environment or the
// first step that fails, before a Finish func or a teardown deletes what the
// failure left.
type dumping struct{ env.Environment }

func (e dumping) Setup(funcs ...env.Func) env.Environment {
	e.Environment.Setup(dumpOnSetupFailure(funcs)...)
	return e
}

func (e dumping) Test(t *testing.T, features ...types.Feature) context.Context {
	return e.Environment.Test(t, dumpOnFailure(features)...)
}

func dumpOnSetupFailure(funcs []env.Func) []env.Func {
	out := make([]env.Func, len(funcs))
	for i, f := range funcs {
		out[i] = func(ctx context.Context, cfg *envconf.Config) (context.Context, error) {
			next, err := f(ctx, cfg)
			if err != nil {
				dumpCluster(ctx, cfg, "failure")
			}
			return next, err
		}
	}
	return out
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
