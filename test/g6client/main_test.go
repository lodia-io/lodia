// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"
)

func TestPercentileIsTheNearestRankFromBelow(t *testing.T) {
	sorted := make([]float64, 200)
	for i := range sorted {
		sorted[i] = float64(i + 1)
	}
	for _, c := range []struct {
		q    float64
		want float64
	}{{0.99, 198}, {0.50, 100}, {1, 200}} {
		if got := percentile(sorted, c.q); got != c.want {
			t.Errorf("percentile(%g) = %g, want %g", c.q, got, c.want)
		}
	}
	if got := percentile([]float64{7}, 0.99); got != 7 {
		t.Errorf("the only sample is %g", got)
	}
}

func TestBootstrapIntervalIsSeededAndCoversTheDifference(t *testing.T) {
	direct, other := make([]float64, 400), make([]float64, 400)
	for i := range direct {
		direct[i] = float64(i)
		other[i] = float64(i) + 2 + float64(i%13)*0.05
	}
	low, high := bootstrapInterval(direct, other, 42)
	if againLow, againHigh := bootstrapInterval(direct, other, 42); againLow != low || againHigh != high {
		t.Errorf("the interval moved between two runs with one seed: [%g, %g] then [%g, %g]", low, high, againLow, againHigh)
	}
	if low > 2 || high < 2 {
		t.Errorf("the interval [%g, %g] does not hold the difference 2", low, high)
	}
}

func TestStatisticsCompareEachPathWithTheDirectOne(t *testing.T) {
	sample := func(offset float64) []float64 {
		out := make([]float64, 300)
		for i := range out {
			out[i] = float64(i) + offset
		}
		return out
	}
	paths := []*path{{Name: "direct", samples: sample(0)}, {Name: "gateway", samples: sample(3)}, {Name: "tunnel", samples: sample(5)}}
	computeStatistics(paths, 1)
	if paths[0].P99Ms != 296 || paths[1].DeltaP99Ms != 3 || paths[2].DeltaP99Ms != 5 {
		t.Errorf("p99 %g, deltas %g and %g", paths[0].P99Ms, paths[1].DeltaP99Ms, paths[2].DeltaP99Ms)
	}
}
