// Copyright 2026 The Lodia Authors.
// SPDX-License-Identifier: Apache-2.0

// Command apiloop reads the kubernetes EndpointSlice from an API server at a
// fixed rate on new connections and at the same rate on reused ones. It
// follows an answer's Retry-After once, as client-go does, and retries
// nothing else. It prints each request that fails, finds the slice empty or
// follows a Retry-After, and every second the counts so far, after "counts ",
// as JSON; the e2e suite reads them from its log.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const slicePath = "/apis/discovery.k8s.io/v1/namespaces/default/endpointslices/kubernetes"

// tally counts a loop's requests: sent, answered with the slice, failed,
// answered with an empty slice, and, of all of them, those that followed a
// Retry-After.
type tally struct {
	Sent    int64 `json:"sent"`
	OK      int64 `json:"ok"`
	Failed  int64 `json:"failed"`
	Empty   int64 `json:"empty"`
	Retried int64 `json:"retried"`
}

// summary is what the program prints every second.
type summary struct {
	Since  time.Time `json:"since"`
	Time   time.Time `json:"time"`
	Fresh  tally     `json:"fresh"`
	Reused tally     `json:"reused"`
}

type counter struct {
	sent, ok, failed, empty, retried atomic.Int64
}

func (c *counter) tally() tally {
	return tally{Sent: c.sent.Load(), OK: c.ok.Load(), Failed: c.failed.Load(), Empty: c.empty.Load(), Retried: c.retried.Load()}
}

func main() {
	kubeconfig := flag.String("kubeconfig", "", "a kubeconfig whose user and certificate authority the requests use")
	server := flag.String("server", "", "the API server's URL, in place of the kubeconfig's")
	rate := flag.Int("rate", 10, "requests per second on each kind of connection")
	timeout := flag.Duration("timeout", 5*time.Second, "how long one request may take")
	flag.Parse()
	if err := run(*kubeconfig, *server, *rate, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(kubeconfig, server string, rate int, timeout time.Duration) error {
	cfg, err := clientcmd.BuildConfigFromFlags(server, kubeconfig)
	if err != nil {
		return err
	}
	tlsConfig, err := rest.TLSConfigFor(cfg)
	if err != nil {
		return err
	}
	// Each transport gets a TLS configuration of its own, since an HTTP/2
	// transport adds its protocol to the one it is given.
	fresh := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig.Clone(), DisableKeepAlives: true}}
	reused := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig.Clone(), ForceAttemptHTTP2: true}}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	counts := summary{Since: time.Now()}
	var f, r counter
	go send(ctx, fresh, cfg.Host+slicePath, "fresh", rate, timeout, &f)
	go send(ctx, reused, cfg.Host+slicePath, "reused", rate, timeout, &r)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-tick.C:
			counts.Time, counts.Fresh, counts.Reused = now, f.tally(), r.tally()
			line, err := json.Marshal(counts)
			if err != nil {
				return err
			}
			fmt.Printf("counts %s\n", line)
		}
	}
}

func send(ctx context.Context, c *http.Client, url, name string, rate int, timeout time.Duration, n *counter) {
	tick := time.NewTicker(time.Second / time.Duration(rate))
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case start := <-tick.C:
			n.sent.Add(1)
			go func() {
				empty, retried, err := get(ctx, c, url, timeout)
				switch {
				case ctx.Err() != nil:
					return
				case err != nil:
					n.failed.Add(1)
					fmt.Printf("failed %s %s: %v\n", start.UTC().Format(time.RFC3339Nano), name, err)
				case empty:
					n.empty.Add(1)
					fmt.Printf("empty %s %s\n", start.UTC().Format(time.RFC3339Nano), name)
				default:
					n.ok.Add(1)
				}
				if retried {
					n.retried.Add(1)
					fmt.Printf("retried %s %s\n", start.UTC().Format(time.RFC3339Nano), name)
				}
			}()
		}
	}
}

// get follows a Retry-After on a 429 or a 5xx once, as client-go does
// (https://github.com/kubernetes/client-go/blob/v0.37.1/rest/with_retry.go#L308-L318).
func get(ctx context.Context, c *http.Client, url string, timeout time.Duration) (bool, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	empty, wait, err := getOnce(ctx, c, url)
	if wait < 0 {
		return empty, false, err
	}
	select {
	case <-ctx.Done():
		return false, true, ctx.Err()
	case <-time.After(wait):
	}
	empty, _, err = getOnce(ctx, c, url)
	return empty, true, err
}

// getOnce returns the wait an answer's Retry-After asks for, or -1.
func getOnce(ctx context.Context, c *http.Client, url string) (bool, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, -1, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return false, -1, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		wait := time.Duration(-1)
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError) {
			wait = time.Duration(seconds) * time.Second
		}
		return false, wait, fmt.Errorf("status %s", resp.Status)
	}
	var slice discoveryv1.EndpointSlice
	if err := json.NewDecoder(resp.Body).Decode(&slice); err != nil {
		return false, -1, err
	}
	return len(slice.Endpoints) == 0, -1, nil
}
