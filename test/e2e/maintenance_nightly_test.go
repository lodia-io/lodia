// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && maintenance

package e2e

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"

	lodiav1alpha1 "go.lodia.io/lodia/api/v1alpha1"
)

func TestFleetLimitInQueueOrder(t *testing.T) {
	var (
		k8s       client.WithWatch
		b         secondBuild
		revisionA string
		fleet     *fleetWatch
	)
	var keys []client.ObjectKey
	for i := range 4 {
		keys = append(keys, client.ObjectKey{Namespace: fmt.Sprintf("team-fleet-%d", i+1), Name: controlPlane})
	}
	f := features.New("nightly on kind: an operator upgrade at a fleet limit of 1 rolls four tenants one at a time, in the order of the queue").
		Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			k8s = newClient(t, cfg)
			for _, key := range keys {
				createTenant(ctx, t, k8s, key, "e2e-"+key.Namespace)
			}
			return ctx
		}).
		Assess("the four tenants are Ready, every part reporting revision A", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			for _, key := range keys {
				revision := readyAtOneRevision(ctx, t, k8s, key)
				if revisionA != "" && revision != revisionA {
					t.Fatalf("%s runs revision %q, the others %q", key, revision, revisionA)
				}
				revisionA = revision
			}
			fleet = watchFleet(ctx, t, k8s, keys...)
			return ctx
		}).
		Assess("the chart upgrades to build B with fleet.maxConcurrentRollouts 1", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			b = buildSecond(ctx, t, cfg)
			upgradeTo(ctx, t, cfg, b, "fleet.maxConcurrentRollouts=1")
			awaitLeader(ctx, t, k8s, b.operator.String())
			return ctx
		}).
		Assess("every tenant runs revision B, one in a slot at a time, in the order of the queue", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			until(ctx, t, 20*time.Minute, "the four tenants run revision B in every part and are Idle", func(ctx context.Context) error {
				for _, key := range keys {
					cp := &lodiav1alpha1.ControlPlane{}
					if err := k8s.Get(ctx, key, cp); err != nil {
						return err
					}
					o := observe(cp, time.Now())
					if o.revision == revisionA || !strings.HasPrefix(o.revision, b.version+"+") || o.progressing != lodiav1alpha1.ReasonIdle {
						return fmt.Errorf("%s: revision %q, Progressing %s", key.Namespace, o.revision, o.progressing)
					}
					if err := reportsRevision(cp, o.revision); err != nil {
						return fmt.Errorf("%s: %w", key.Namespace, err)
					}
				}
				return nil
			})
			fleet.stop()
			checkOneSlotInQueueOrder(t, fleet.all(), len(keys))
			return ctx
		}).
		Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			if fleet != nil {
				fleet.stop()
				fleet.log(t)
			}
			for _, key := range keys {
				closeTenant(ctx, t, k8s, key, "e2e-"+key.Namespace)
			}
			return ctx
		}).
		Feature()
	testenv.Test(t, f)
}

func checkOneSlotInQueueOrder(t *testing.T, seen []observation, tenants int) {
	t.Helper()
	inSlot := map[client.ObjectKey]bool{}
	queued := map[client.ObjectKey]time.Time{}
	type grant struct {
		key    client.ObjectKey
		queued time.Time
		waited bool
	}
	var grants []grant
	for _, o := range seen {
		slot := o.progressing == lodiav1alpha1.ReasonMaintaining || o.progressing == lodiav1alpha1.ReasonMaintenanceFailed
		if o.progressing == lodiav1alpha1.ReasonMaintenanceFailed {
			t.Errorf("%s at resourceVersion %s: MaintenanceFailed", o.key.Namespace, o.resourceVersion)
		}
		if o.progressing == lodiav1alpha1.ReasonFleetLimit && o.scheduledFor != nil {
			queued[o.key] = o.scheduledFor.Time
		}
		if slot && !inSlot[o.key] {
			at, waited := queued[o.key]
			grants = append(grants, grant{o.key, at, waited})
		}
		inSlot[o.key] = slot
		holders := 0
		for _, holds := range inSlot {
			if holds {
				holders++
			}
		}
		if holders > 1 {
			t.Errorf("at resourceVersion %s, %d tenants hold a slot of a fleet limited to 1", o.resourceVersion, holders)
		}
	}
	var order []string
	for _, g := range grants {
		order = append(order, fmt.Sprintf("%s (queued at %s, waited %t)", g.key.Namespace, g.queued.Format(time.RFC3339), g.waited))
	}
	t.Logf("the slot went to %s", strings.Join(order, ", "))
	if len(grants) != tenants {
		t.Errorf("%d grants of a slot, want one for each of the %d tenants", len(grants), tenants)
	}
	var waited []grant
	for _, g := range grants {
		if g.waited {
			waited = append(waited, g)
		}
	}
	if len(waited) < tenants-1 {
		t.Errorf("%d tenants waited for the slot, want at least %d", len(waited), tenants-1)
	}
	if !slices.IsSortedFunc(waited, func(a, b grant) int {
		switch {
		case !a.queued.Equal(b.queued):
			return a.queued.Compare(b.queued)
		case a.key.Namespace != b.key.Namespace:
			return strings.Compare(a.key.Namespace, b.key.Namespace)
		}
		return strings.Compare(a.key.Name, b.key.Name)
	}) {
		t.Errorf("the tenants that waited were granted out of the queue's order: %s", strings.Join(order, ", "))
	}
}
