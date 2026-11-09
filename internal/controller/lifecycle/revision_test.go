// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
	"go.lodia.io/lodia/internal/testenv"
)

const otherRevision = "v1.1.0+fedcba9876543210"

func revisionOn(cp *lodiav1alpha1.ControlPlane) string {
	if cp.Status.Target == nil {
		return ""
	}
	return cp.Status.Target.OperatorRevision
}

func TestTheRunningRevisionIsGrantedToANewControlPlane(t *testing.T) {
	for _, version := range []string{"v1.35.0", "v1.34.2"} {
		cp := controlPlane(t, version)
		testenv.Eventually(t, func() error {
			if got := revisionOn(get(t, cp)); got != runningRevision {
				return fmt.Errorf("target.operatorRevision is %q, want %q", got, runningRevision)
			}
			return nil
		})
	}
}

func TestTheRevisionSurvivesTheEdgesOfAPass(t *testing.T) {
	injected := errors.New("injected")
	seed := func(t *testing.T) *lodiav1alpha1.ControlPlane {
		t.Helper()
		cp := quietControlPlane(t)
		seedQuiet(t, cp, "v1.37.1", "", all("v1.37.1"), "")
		return cp
	}

	t.Run("a status write that fails grants nothing, and the next pass grants it", func(t *testing.T) {
		cp := seed(t)
		failing := interceptor.NewClient(quiet, interceptor.Funcs{
			SubResourceApply: func(context.Context, client.Client, string, runtime.ApplyConfiguration, ...client.SubResourceApplyOption) error {
				return injected
			},
		})
		if err := reconcileQuiet(t, failing, quiet, later, cp); !errors.Is(err, injected) {
			t.Fatalf("the pass returned %v", err)
		}
		if got := revisionOn(getQuiet(t, cp)); got != "" {
			t.Errorf("a write that failed left the revision %q", got)
		}
		quietPass(t, cp)
		if got := revisionOn(getQuiet(t, cp)); got != runningRevision {
			t.Errorf("the next pass left the revision %q, want %q", got, runningRevision)
		}
	})

	t.Run("a live read that fails writes nothing", func(t *testing.T) {
		cp := seed(t)
		before := getQuiet(t, cp)
		unavailable := interceptor.NewClient(quiet, interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return injected
			},
		})
		if err := reconcileQuiet(t, quiet, unavailable, later, cp); !errors.Is(err, injected) {
			t.Fatalf("the pass returned %v", err)
		}
		if after := getQuiet(t, cp); after.ResourceVersion != before.ResourceVersion {
			t.Errorf("a pass that could not read the ControlPlane live wrote it: resourceVersion %s, then %s", before.ResourceVersion, after.ResourceVersion)
		}
	})

	t.Run("a pass whose deadline is spent still grants it", func(t *testing.T) {
		cp := seed(t)
		expired, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()
		cached := interceptor.NewClient(quiet, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return c.Get(context.Background(), key, obj, opts...)
			},
		})
		if err := reconcileAt(expired, cached, quiet, later, cp); err != nil {
			t.Fatal(err)
		}
		if got := revisionOn(getQuiet(t, cp)); got != runningRevision {
			t.Errorf("the revision is %q after a pass past its deadline, want %q", got, runningRevision)
		}
	})

	t.Run("a restart in the same release writes nothing, in another it replaces the revision, and a rollback is one more", func(t *testing.T) {
		cp := seed(t)
		quietPass(t, cp)
		settled := getQuiet(t, cp)
		for _, c := range []struct {
			revision string
			writes   bool
		}{
			{runningRevision, false},
			{otherRevision, true},
			{otherRevision, false},
			{runningRevision, true},
		} {
			before := getQuiet(t, cp)
			if err := reconcileAs(t.Context(), quiet, quiet, c.revision, later, cp); err != nil {
				t.Fatal(err)
			}
			after := getQuiet(t, cp)
			if got := revisionOn(after); got != c.revision {
				t.Errorf("a pass of %s left the revision %q", c.revision, got)
			}
			if wrote := after.ResourceVersion != before.ResourceVersion; wrote != c.writes {
				t.Errorf("a pass of %s wrote %t, want %t", c.revision, wrote, c.writes)
			}
		}
		if after := getQuiet(t, cp); after.Status.Target.Version != settled.Status.Target.Version || after.Status.ObservedGeneration != settled.Status.ObservedGeneration {
			t.Errorf("a revision changed more than itself: target %+v then %+v", settled.Status.Target, after.Status.Target)
		}
	})

	t.Run("a cache that has not seen the grant, or holds another release's, makes no write", func(t *testing.T) {
		for _, behind := range []string{"", "v0.9.0+0000000000000000"} {
			cp := seed(t)
			quietPass(t, cp)
			settled := getQuiet(t, cp)
			stale := settled.DeepCopy()
			stale.Status.Target.OperatorRevision = behind
			applies := 0
			if err := reconcileQuiet(t, staleClient(stale, &applies), quiet, later, cp); err != nil {
				t.Fatal(err)
			}
			if after := getQuiet(t, cp); applies != 0 || after.ResourceVersion != settled.ResourceVersion || revisionOn(after) != runningRevision {
				t.Errorf("a pass on a cache holding %q applied %d times and left %q, resourceVersion %s then %s", behind, applies, revisionOn(after), settled.ResourceVersion, after.ResourceVersion)
			}
		}
	})

	t.Run("a paused ControlPlane keeps the revision it holds until it is unpaused", func(t *testing.T) {
		cp := seed(t)
		quietPass(t, cp)
		paused := getQuiet(t, cp)
		paused.Spec.Paused = true
		if err := quiet.Update(t.Context(), paused); err != nil {
			t.Fatal(err)
		}
		if err := reconcileAs(t.Context(), quiet, quiet, otherRevision, later, cp); err != nil {
			t.Fatal(err)
		}
		if got := revisionOn(getQuiet(t, cp)); got != runningRevision {
			t.Errorf("a paused ControlPlane holds the revision %q, want %q", got, runningRevision)
		}
		unpaused := getQuiet(t, cp)
		unpaused.Spec.Paused = false
		if err := quiet.Update(t.Context(), unpaused); err != nil {
			t.Fatal(err)
		}
		if err := reconcileAs(t.Context(), quiet, quiet, otherRevision, later, cp); err != nil {
			t.Fatal(err)
		}
		if got := revisionOn(getQuiet(t, cp)); got != otherRevision {
			t.Errorf("an unpaused ControlPlane holds the revision %q, want %q", got, otherRevision)
		}
	})
}
