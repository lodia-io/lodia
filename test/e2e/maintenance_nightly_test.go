// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e && maintenance

package e2e

import (
	"context"
	"fmt"
	"maps"
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
			if err := fleet.whole(); err != nil {
				t.Fatal(err)
			}
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
	waiting := map[client.ObjectKey]time.Time{}
	waited := map[client.ObjectKey]bool{}
	var order []string
	for _, o := range seen {
		if o.progressing == lodiav1alpha1.ReasonMaintenanceFailed {
			t.Errorf("%s at resourceVersion %s: MaintenanceFailed", o.key.Namespace, o.resourceVersion)
		}
		if o.slot && !inSlot[o.key] {
			queue := slices.SortedFunc(maps.Keys(waiting), func(a, b client.ObjectKey) int {
				switch {
				case !waiting[a].Equal(waiting[b]):
					return waiting[a].Compare(waiting[b])
				case a.Namespace != b.Namespace:
					return strings.Compare(a.Namespace, b.Namespace)
				}
				return strings.Compare(a.Name, b.Name)
			})
			switch {
			case len(queue) == 0 && len(order) > 0 && !waited[o.key]:
				t.Errorf("at resourceVersion %s %s, never seen waiting, took a slot with no tenant waiting", o.resourceVersion, o.key.Namespace)
			case len(queue) > 0 && queue[0] != o.key:
				t.Errorf("at resourceVersion %s the slot went to %s, not to %s, the first of those waiting: %v", o.resourceVersion, o.key.Namespace, queue[0].Namespace, waiting)
			}
			order = append(order, fmt.Sprintf("%s (waiting since %s, %d waiting)", o.key.Namespace, waiting[o.key].Format(time.RFC3339), len(queue)))
		}
		if o.progressing == lodiav1alpha1.ReasonFleetLimit && (o.scheduledFor == nil || o.scheduledFor.After(o.at)) {
			t.Errorf("at resourceVersion %s %s waits for a slot without a scheduledFor at or before %s", o.resourceVersion, o.key.Namespace, o.at.Format(time.RFC3339))
		}
		inSlot[o.key] = o.slot
		if o.scheduledFor != nil && !o.scheduledFor.After(o.at) {
			waiting[o.key], waited[o.key] = o.scheduledFor.Time, true
		} else {
			delete(waiting, o.key)
		}
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
	t.Logf("the slot went to %s", strings.Join(order, ", "))
	if len(order) != tenants {
		t.Errorf("%d grants of a slot, want one for each of the %d tenants", len(order), tenants)
	}
}
