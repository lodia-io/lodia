// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Command g6client measures what the Gateway's and the tunnel's paths to one
// API server add to a direct one (gate G6, docs/roadmap.md): authenticated GET
// /version, sequentially, interleaved over the three paths with the starting
// path rotated, on reused connections (M1) and on a new connection per request
// (M2). Everything it prints but its last line is meant for a person; the last
// line is the result as JSON, after "g6result ".
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

const (
	versionPath = "/version"
	resamples   = 1000
	timeout     = 30 * time.Second
)

// A path is how the client reaches the API server, and the result of one mix
// for it: the p99 latency and its difference from the direct path, in
// milliseconds, with the difference's 95% bootstrap interval; requests that
// failed; and dials.
type path struct {
	Name       string  `json:"name"`
	P50Ms      float64 `json:"p50Ms"`
	P99Ms      float64 `json:"p99Ms"`
	DeltaP99Ms float64 `json:"deltaP99Ms"`
	CILowMs    float64 `json:"ciLowMs"`
	CIHighMs   float64 `json:"ciHighMs"`
	Errors     int64   `json:"errors"`
	Dials      int64   `json:"dials"`

	url        string
	serverName string
	client     *http.Client
	samples    []float64
	dials      atomic.Int64
}

type mix struct {
	Name  string  `json:"name"`
	N     int     `json:"n"`
	Paths []*path `json:"paths"`
}

type options struct {
	direct, gateway, tunnel, serverName, tokenFile, caFile string
	warmupReused, reused, warmupNew, fresh                 int
	seed                                                   uint64
}

func main() {
	var o options
	flag.StringVar(&o.direct, "direct", "", "the direct path's URL, without path")
	flag.StringVar(&o.gateway, "gateway", "", "the Gateway path's URL")
	flag.StringVar(&o.tunnel, "tunnel", "", "the tunnel path's URL, with the kubernetes Service's ClusterIP")
	flag.StringVar(&o.serverName, "server-name", "", "the TLS server name of the direct and Gateway paths")
	flag.StringVar(&o.tokenFile, "token-file", "/var/run/secrets/kubernetes.io/serviceaccount/token", "the file of the bearer token")
	flag.StringVar(&o.caFile, "ca-file", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt", "the file of the CA certificates")
	flag.IntVar(&o.warmupReused, "warmup-reused", 1000, "M1's warm-up requests per path")
	flag.IntVar(&o.reused, "reused", 20000, "M1's measured requests per path")
	flag.IntVar(&o.warmupNew, "warmup-new", 200, "M2's warm-up requests per path")
	flag.IntVar(&o.fresh, "new", 3000, "M2's measured requests per path")
	flag.Uint64Var(&o.seed, "seed", 1, "the bootstrap's seed")
	flag.Parse()
	mixes, err := run(o)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	out, err := json.Marshal(mixes)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("g6result " + string(out))
}

func run(o options) ([]mix, error) {
	token, err := os.ReadFile(o.tokenFile)
	if err != nil {
		return nil, err
	}
	bundle, err := os.ReadFile(o.caFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle) {
		return nil, fmt.Errorf("%s holds no certificate", o.caFile)
	}
	bearer := "Bearer " + strings.TrimSpace(string(token))
	var mixes []mix
	for _, m := range []struct {
		name         string
		keepAlive    bool
		warmup, size int
	}{
		{"M1, a reused connection per path", true, o.warmupReused, o.reused},
		{"M2, a new connection per request", false, o.warmupNew, o.fresh},
	} {
		fmt.Printf("mix %q: %d warm-up requests per path, then %d\n", m.name, m.warmup, m.size)
		paths := []*path{
			{Name: "direct", url: o.direct, serverName: o.serverName},
			{Name: "gateway", url: o.gateway, serverName: o.serverName},
			{Name: "tunnel", url: o.tunnel},
		}
		for _, p := range paths {
			p.client = newClient(p, roots, m.keepAlive)
			p.samples = make([]float64, 0, m.size)
		}
		sample(paths, bearer, m.warmup, m.size)
		computeStatistics(paths, o.seed)
		for _, p := range paths {
			p.client.CloseIdleConnections()
			p.Dials = p.dials.Load()
		}
		mixes = append(mixes, mix{Name: m.name, N: m.size, Paths: paths})
	}
	return mixes, nil
}

func newClient(p *path, roots *x509.CertPool, keepAlive bool) *http.Client {
	dialer := &net.Dialer{Timeout: timeout}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{ServerName: p.serverName, RootCAs: roots, MinVersion: tls.VersionTLS12},
			DisableKeepAlives: !keepAlive,
			ForceAttemptHTTP2: true,
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				p.dials.Add(1)
				return dialer.DialContext(ctx, network, address)
			},
		},
	}
}

func sample(paths []*path, bearer string, warmup, size int) {
	for round := range warmup + size {
		for i := range paths {
			p := paths[(round+i)%len(paths)]
			latency, err := get(p, bearer)
			if err != nil {
				p.Errors++
				fmt.Printf("failed %s round %d: %v\n", p.Name, round, err)
				continue
			}
			if round >= warmup {
				p.samples = append(p.samples, latency)
			}
		}
	}
}

func get(p *path, bearer string) (float64, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, p.url+versionPath, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", bearer)
	start := time.Now()
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	return float64(time.Since(start)) / float64(time.Millisecond), nil
}

// percentile is the ⌈q·n⌉-th of the ascending sorted sample.
func percentile(sorted []float64, q float64) float64 {
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[max(rank, 0)]
}

func computeStatistics(paths []*path, seed uint64) {
	direct := paths[0]
	for _, p := range paths {
		s := slices.Sorted(slices.Values(p.samples))
		p.P50Ms, p.P99Ms = percentile(s, 0.50), percentile(s, 0.99)
	}
	for _, p := range paths[1:] {
		p.DeltaP99Ms = p.P99Ms - direct.P99Ms
		p.CILowMs, p.CIHighMs = bootstrapInterval(direct.samples, p.samples, seed)
	}
}

// bootstrapInterval is the 95% interval of the p99 difference of paired
// samples, from resamples of the pairs with a fixed seed.
func bootstrapInterval(direct, other []float64, seed uint64) (float64, float64) {
	source := rand.New(rand.NewPCG(seed, 2))
	n := min(len(direct), len(other))
	deltas := make([]float64, resamples)
	base, sampled := make([]float64, n), make([]float64, n)
	for i := range deltas {
		for j := range n {
			k := source.IntN(n)
			base[j], sampled[j] = direct[k], other[k]
		}
		slices.Sort(base)
		slices.Sort(sampled)
		deltas[i] = percentile(sampled, 0.99) - percentile(base, 0.99)
	}
	slices.Sort(deltas)
	return percentile(deltas, 0.025), percentile(deltas, 0.975)
}
