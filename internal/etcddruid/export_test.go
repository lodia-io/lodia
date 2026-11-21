// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package etcddruid

import (
	"maps"
	"slices"

	druidv1alpha1 "github.com/gardener/etcd-druid/api/core/v1alpha1"
	"k8s.io/utils/clock"
)

const SpecReconcileBound = specReconcileBound

func (e *Engine) InFlight(clk clock.PassiveClock, etcd *druidv1alpha1.Etcd) bool {
	return e.inFlight(clk, etcd)
}

func (e *Engine) Sighted() []string {
	sightingsMu.Lock()
	defer sightingsMu.Unlock()
	var names []string
	for key := range maps.Keys(e.sightings) {
		names = append(names, key.Name)
	}
	slices.Sort(names)
	return names
}
